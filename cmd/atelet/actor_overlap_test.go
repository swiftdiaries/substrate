// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"go.ate.dev/substrate/cmd/atelet/internal/snapshottracker"
	"go.ate.dev/substrate/internal/apierror"
	"go.ate.dev/substrate/internal/proto/ateletpb"
	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"go.ate.dev/substrate/internal/resources"
	"google.golang.org/grpc/codes"
	"k8s.io/utils/clock"
)

const (
	busyActorUID  = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a70"
	otherActorUID = "0f6c2b0e-1b7e-4b3f-9a6a-1d2c3e4f5a71"
	busyPodUID    = "5f1c0d2e-3a4b-4c5d-8e9f-0a1b2c3d4e70"
)

var overlapRunsc = []byte("runsc binary")

// gatedWorker is a fakeWorker whose next RunWorkload or TerminateWorkload
// can be held.
type gatedWorker struct {
	*fakeWorker
	mu    sync.Mutex
	gates map[string]*workerGate
}

// workerGate holds one worker RPC: the RPC closes entered, then waits for
// release.
type workerGate struct {
	entered chan struct{}
	release chan struct{}
}

// holdNext makes the next call of the named RPC wait, once it has signaled
// entered, until release is called.
func (w *gatedWorker) holdNext(rpc string) (entered <-chan struct{}, release func()) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.gates == nil {
		w.gates = map[string]*workerGate{}
	}
	g := &workerGate{entered: make(chan struct{}), release: make(chan struct{})}
	w.gates[rpc] = g
	return g.entered, func() { close(g.release) }
}

// pass waits at the named RPC's gate, if one is set, and clears it.
func (w *gatedWorker) pass(ctx context.Context, rpc string) error {
	w.mu.Lock()
	g := w.gates[rpc]
	delete(w.gates, rpc)
	w.mu.Unlock()
	if g == nil {
		return nil
	}
	close(g.entered)
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *gatedWorker) RunWorkload(ctx context.Context, req *ateworkerpb.RunWorkloadRequest) (*ateworkerpb.RunWorkloadResponse, error) {
	if err := w.pass(ctx, "RunWorkload"); err != nil {
		return nil, err
	}
	return w.fakeWorker.RunWorkload(ctx, req)
}

func (w *gatedWorker) TerminateWorkload(ctx context.Context, req *ateworkerpb.TerminateWorkloadRequest) (*ateworkerpb.TerminateWorkloadResponse, error) {
	if err := w.pass(ctx, "TerminateWorkload"); err != nil {
		return nil, err
	}
	return w.fakeWorker.TerminateWorkload(ctx, req)
}

// overlapEnv is an atelet whose node advertises a gVisor sandbox, with a
// worker that runs actors in it.
type overlapEnv struct {
	s      *Atelet
	worker *gatedWorker
	assets *ateletpb.SandboxAssets
	spec   *ateletpb.WorkloadSpec
}

func newOverlapEnv(t *testing.T) *overlapEnv {
	t.Helper()
	useTempNodeDirs(t)
	worker := &gatedWorker{fakeWorker: &fakeWorker{}}
	serveFakeWorker(t, worker)
	host := imageVolumeTestRegistry(t)
	pinnedImage := pushTestImage(t, host+"/actor:v1", singleFileLayer(t, "bin/app", "app"))
	e := &overlapEnv{
		s: &Atelet{
			workerDialer:      newWorkerDialer(1),
			imageCache:        newImageVolumeStore(t),
			sandboxAssetCache: newTestAssetCache(t),
			anonGCSClient:     fakeObjectStorage{data: overlapRunsc},
			systemInfoVolumes: newSystemInfoVolumeRefresher(nil, nil),
			snapshots:         snapshottracker.New(t.Context(), nil, clock.RealClock{}),
		},
		worker: worker,
		assets: &ateletpb.SandboxAssets{
			SandboxClass: "gvisor",
			PauseImage:   pinnedImage,
			Assets: map[string]*ateletpb.ArchAssets{
				runtime.GOARCH: {Files: map[string]*ateletpb.AssetFile{
					runscAssetName: {Url: "gs://test-bucket/runsc", Sha256: fmt.Sprintf("%x", sha256.Sum256(overlapRunsc))},
				}},
			},
		},
		spec: &ateletpb.WorkloadSpec{
			Containers: []*ateletpb.Container{{Name: "app", Image: pinnedImage, Command: []string{"/bin/app"}}},
		},
	}
	advertiseRuntime(t, e.s, e.assets)
	return e
}

func (e *overlapEnv) run(t *testing.T, actorUID string) error {
	_, err := e.s.Run(t.Context(), &ateletpb.RunRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: busyPodUID, SandboxAssets: e.assets,
		SandboxRuntime: runtimeFor(t, e.assets), Spec: e.spec,
	})
	return err
}

func (e *overlapEnv) restore(t *testing.T, actorUID string) error {
	_, err := e.s.Restore(t.Context(), &ateletpb.RestoreRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: busyPodUID, SandboxAssets: e.assets,
		SandboxRuntime: runtimeFor(t, e.assets), Spec: e.spec,
		Snapshot: lifecycleSnapshot(actorUID, "snap-1", 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	})
	return err
}

func (e *overlapEnv) checkpoint(t *testing.T, actorUID string) error {
	_, err := e.s.Checkpoint(t.Context(), &ateletpb.CheckpointRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: busyPodUID, Spec: e.spec,
		Snapshot:     lifecycleSnapshot(actorUID, "snap-2", 2, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
		UploadConfig: deferredUploadConfig(),
	})
	return err
}

// terminate terminates the actor, through its worker when workerPodUID is set.
func (e *overlapEnv) terminate(t *testing.T, actorUID, workerPodUID string) error {
	_, err := e.s.Terminate(t.Context(), &ateletpb.TerminateRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		WorkerPodUid: workerPodUID, Spec: e.spec,
	})
	return err
}

func (e *overlapEnv) promote(t *testing.T, actorUID string) error {
	_, err := e.s.PromoteSnapshot(t.Context(), &ateletpb.PromoteSnapshotRequest{
		Atespace: "ate-demo", ActorName: "counter", ActorUid: actorUID,
		ActorTemplateAtespace: "default", ActorTemplateName: "counter",
		Snapshot: lifecycleSnapshot(actorUID, "snap-1", 1, ateletpb.SnapshotFidelity_SNAPSHOT_FIDELITY_MEMORY),
	})
	return err
}

// secondOps are the lifecycle RPCs atelet rejects while another operation
// holds the actor.
func (e *overlapEnv) secondOps() []struct {
	name string
	call func(t *testing.T, actorUID string) error
} {
	return []struct {
		name string
		call func(t *testing.T, actorUID string) error
	}{
		{"Run", e.run},
		{"Restore", e.restore},
		{"Checkpoint", e.checkpoint},
		{"Terminate", func(t *testing.T, uid string) error { return e.terminate(t, uid, "") }},
		{"PromoteSnapshot", e.promote},
	}
}

// occupyActor gives the actor node state a rejected call must leave alone: a
// file in its directory, which Run and Restore reset and Terminate removes,
// and a system-info registration, which Run, Restore, Checkpoint, and
// Terminate deregister. It keeps a registration the actor already has. It
// returns a check that the state is still there.
func occupyActor(t *testing.T, s *Atelet, actorUID string) func(after string) {
	t.Helper()
	if err := os.MkdirAll(ateletpath.ActorPath(actorUID), 0o700); err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(ateletpath.ActorPath(actorUID), "sentinel")
	if err := os.WriteFile(sentinel, []byte("in progress"), 0o600); err != nil {
		t.Fatal(err)
	}
	s.systemInfoVolumes.mu.Lock()
	reg := s.systemInfoVolumes.actors[actorUID]
	s.systemInfoVolumes.mu.Unlock()
	if reg == nil {
		var err error
		if reg, err = s.systemInfoVolumes.Register(actorUID, resources.ActorRef{Atespace: "ate-demo", Name: "counter"}, nil); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}
	return func(after string) {
		t.Helper()
		if _, err := os.Stat(sentinel); err != nil {
			t.Errorf("the actor's directory changed under %s: %v", after, err)
		}
		s.systemInfoVolumes.mu.Lock()
		got := s.systemInfoVolumes.actors[actorUID]
		s.systemInfoVolumes.mu.Unlock()
		if got != reg {
			t.Errorf("the actor's system-info registration did not survive %s", after)
		}
	}
}

// wantBusy fails the test unless err is the UNAVAILABLE atelet returns for an
// operation on an actor another operation holds.
func wantBusy(t *testing.T, what string, err error) {
	t.Helper()
	if apierror.Code(err) != codes.Unavailable || !errors.Is(err, snapshottracker.ErrActorBusy) {
		t.Errorf("%s = %v, want UNAVAILABLE for a busy actor", what, err)
	}
}

// While a Run, Checkpoint, or Terminate of an actor is in progress, each other
// lifecycle RPC on the actor is rejected without changing the actor's node
// state, a Run of another actor goes ahead, and once the first operation ends
// the actor runs again. The Run is in flight on the worker; the Checkpoint and
// Terminate take the hold their handlers take. Restore takes the same hold as
// Run.
func TestOverlappingRPCsOnOneActor(t *testing.T) {
	for _, first := range []struct {
		name  string
		start func(t *testing.T, e *overlapEnv) (end func())
	}{
		{"Run", func(t *testing.T, e *overlapEnv) func() {
			entered, release := e.worker.holdNext("RunWorkload")
			done := make(chan error, 1)
			go func() { done <- e.run(t, busyActorUID) }()
			select {
			case <-entered:
			case err := <-done:
				t.Fatalf("the first Run returned %v before reaching the worker", err)
			case <-time.After(time.Minute):
				t.Fatal("the first Run did not reach the worker")
			}
			return func() {
				release()
				if err := <-done; err != nil {
					t.Errorf("the first Run: %v", err)
				}
			}
		}},
		{"Checkpoint", func(t *testing.T, e *overlapEnv) func() {
			end, err := e.s.snapshots.BeginCheckpoint(t.Context(), busyActorUID)
			if err != nil {
				t.Fatalf("BeginCheckpoint: %v", err)
			}
			return end
		}},
		{"Terminate", func(t *testing.T, e *overlapEnv) func() {
			end, err := e.s.snapshots.BeginTerminate(busyActorUID)
			if err != nil {
				t.Fatalf("BeginTerminate: %v", err)
			}
			return end
		}},
	} {
		t.Run(first.name, func(t *testing.T) {
			e := newOverlapEnv(t)
			end := first.start(t, e)
			stillThere := occupyActor(t, e.s, busyActorUID)
			for _, second := range e.secondOps() {
				wantBusy(t, fmt.Sprintf("%s during a %s of the same actor", second.name, first.name), second.call(t, busyActorUID))
				stillThere(second.name)
			}
			if err := e.run(t, otherActorUID); err != nil {
				t.Errorf("Run of another actor during a %s = %v, want success", first.name, err)
			}
			end()
			if err := e.run(t, busyActorUID); err != nil {
				t.Errorf("Run once the %s ended = %v, want success", first.name, err)
			}
		})
	}
}

// A Terminate in flight on the worker holds the actor: the other lifecycle
// RPCs on the actor are rejected meanwhile, and once the Terminate returns the
// actor runs again.
func TestTerminateInFlightHoldsActor(t *testing.T) {
	e := newOverlapEnv(t)
	if err := e.run(t, busyActorUID); err != nil {
		t.Fatalf("Run: %v", err)
	}
	entered, release := e.worker.holdNext("TerminateWorkload")
	done := make(chan error, 1)
	go func() { done <- e.terminate(t, busyActorUID, busyPodUID) }()
	select {
	case <-entered:
	case err := <-done:
		t.Fatalf("the Terminate returned %v before reaching the worker", err)
	case <-time.After(time.Minute):
		t.Fatal("the Terminate did not reach the worker")
	}
	stillThere := occupyActor(t, e.s, busyActorUID)
	for _, second := range e.secondOps() {
		wantBusy(t, second.name+" during a Terminate of the same actor", second.call(t, busyActorUID))
	}
	stillThere("the rejected RPCs")

	release()
	if err := <-done; err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if err := e.run(t, busyActorUID); err != nil {
		t.Errorf("Run after the Terminate = %v, want success", err)
	}
}

// A Terminate keeps the actor held until it has removed the actor's files, so
// no Run or Restore can start on the actor between its Forget and the removal
// and have its files deleted.
func TestTerminateHoldsActorUntilFilesRemoved(t *testing.T) {
	e := newOverlapEnv(t)
	if err := e.run(t, busyActorUID); err != nil {
		t.Fatalf("Run: %v", err)
	}
	reached, release := make(chan struct{}), make(chan struct{})
	orig := terminateAfterForget
	terminateAfterForget = func() {
		close(reached)
		<-release
	}
	t.Cleanup(func() { terminateAfterForget = orig })

	done := make(chan error, 1)
	go func() { done <- e.terminate(t, busyActorUID, busyPodUID) }()
	select {
	case <-reached:
	case err := <-done:
		t.Fatalf("the Terminate returned %v before forgetting the actor", err)
	case <-time.After(time.Minute):
		t.Fatal("the Terminate did not forget the actor")
	}
	wantBusy(t, "Run between the Terminate's Forget and its file removal", e.run(t, busyActorUID))
	wantBusy(t, "Restore between the Terminate's Forget and its file removal", e.restore(t, busyActorUID))

	close(release)
	if err := <-done; err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if _, err := os.Stat(ateletpath.ActorPath(busyActorUID)); !os.IsNotExist(err) {
		t.Errorf("actor dir survived the Terminate: %v", err)
	}
	if err := e.run(t, busyActorUID); err != nil {
		t.Errorf("Run after the Terminate = %v, want success", err)
	}
}

// An operation that fails after taking its hold releases it, so the next
// operation on the actor goes ahead.
func TestFailedOperationReleasesActor(t *testing.T) {
	t.Run("Run failing to fetch its sandbox assets", func(t *testing.T) {
		e := newOverlapEnv(t)
		e.s.anonGCSClient = fakeObjectStorage{err: errors.New("bucket unavailable")}
		if err := e.run(t, busyActorUID); err == nil || errors.Is(err, snapshottracker.ErrActorBusy) {
			t.Fatalf("Run with its sandbox assets unavailable = %v, want an asset fetch error", err)
		}
		e.s.anonGCSClient = fakeObjectStorage{data: overlapRunsc}
		if err := e.run(t, busyActorUID); err != nil {
			t.Errorf("Run after the failed Run = %v, want success", err)
		}
	})
	t.Run("Terminate failing to read the sandbox record", func(t *testing.T) {
		e := newOverlapEnv(t)
		if err := e.terminate(t, busyActorUID, busyPodUID); err == nil || errors.Is(err, snapshottracker.ErrActorBusy) {
			t.Fatalf("Terminate of an actor with no sandbox record = %v, want a read error", err)
		}
		if err := e.run(t, busyActorUID); err != nil {
			t.Errorf("Run after the failed Terminate = %v, want success", err)
		}
	})
}
