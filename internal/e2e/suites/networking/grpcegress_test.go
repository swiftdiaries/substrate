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

package networking

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.ate.dev/substrate/internal/e2e"
	"go.ate.dev/substrate/internal/resources"
)

// grpcEcho is the origin this test deploys: a cleartext-HTTP/2 gRPC server, in
// its own namespace, so nothing here depends on the internet.
var grpcEcho = e2e.ServerPod{
	Name:       "grpcecho",
	ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
	Args:       []string{"grpc"},
	Port:       50051,
	// A gRPC server answers an HTTP GET with a protocol error, so readiness has
	// to go through the health service the fixture registers.
	GRPCProbe: true,
}

// grpcEchoResponse mirrors the egress demo Actor's /grpc answer. Kept as a
// local copy rather than imported: the demo is a separate module-internal
// command, and what this suite is really pinning is the wire shape between the
// two, which a shared struct would hide.
type grpcEchoResponse struct {
	Message          string                  `json:"message"`
	Stream           []grpcEchoStreamedMsg   `json:"stream"`
	Bidi             []grpcEchoStreamedMsg   `json:"bidi"`
	BidiStreams      [][]grpcEchoStreamedMsg `json:"bidiStreams"`
	ServerOutcome    string                  `json:"serverOutcome"`
	TLS              bool                    `json:"tls"`
	ServerCertIssuer string                  `json:"serverCertIssuer"`
	Code             string                  `json:"code"`
	Error            string                  `json:"error"`
}

type grpcEchoStreamedMsg struct {
	Message string `json:"message"`
	Index   int32  `json:"index"`
	Size    int    `json:"size"`
}

// TestActorEgressGRPC covers the egress path with gRPC, which fails in ways the
// HTTP tests cannot see. atenet-egress terminates the Actor's CONNECT and
// decides the cleartext requests inside it one by one, so HTTP/2 framing has
// to survive that parsing end to end and the gRPC status has to arrive in
// trailers, after the response body. An egress path that downgraded the
// traffic to HTTP/1.1, or dropped trailers, would still pass TestActorEgress
// and fail here.
//
// All three streaming shapes in one request, because each one fails
// differently: unary is a status in trailers, a server-stream is many frames
// over a held-open connection, and a bidirectional stream has both directions
// carrying frames at once and then half-closes one of them.
//
// The origin is an in-cluster cleartext-HTTP/2 server, deployed per test into
// its own namespace, so nothing in this test depends on the internet.
func TestActorEgressGRPC(t *testing.T) {
	ctx := context.Background()
	target := e2e.DeployServerPod(t, ctx, grpcEcho).Address()

	actorAtespace, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-grpc", e2e.EgressFixture(), e2e.EgressAllowAll()...)
	router := mustRouterClient(t, ctx)
	defer router.Close()

	// Bound the access-log scan below to lines this test could have produced.
	// The slack absorbs clock skew between here and the gateway's node.
	since := metav1.NewTime(time.Now().Add(-1 * time.Minute))

	const (
		message     = "hello over grpc"
		streamCount = 3
		bidiCount   = 3
	)
	payload, err := json.Marshal(map[string]any{
		"target":      target,
		"message":     message,
		"streamCount": streamCount,
		"bidiCount":   bidiCount,
	})
	if err != nil {
		t.Fatalf("marshaling the gRPC request for %s: %v", target, err)
	}

	actorRef := resources.ActorRef{Atespace: actorAtespace, Name: actorName}
	status, body := postThroughEgressActor(t, ctx, router, actorRef, "/grpc", payload)
	if status != http.StatusOK {
		t.Fatalf("Actor gRPC egress to %s returned HTTP %d, want 200; body: %s", target, status, body)
	}

	var got grpcEchoResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decoding the Actor's gRPC response: %v; body: %s", err, body)
	}
	if got.Message != message {
		t.Errorf("unary Echo returned %q, want %q", got.Message, message)
	}
	// The status is the trailer assertion: a gRPC status is sent after the
	// response body, so a path that dropped trailers could deliver the message
	// above and still not produce this.
	if got.Code != "OK" {
		t.Errorf("gRPC status = %q, want OK; error: %s", got.Code, got.Error)
	}
	if len(got.Stream) != streamCount {
		t.Fatalf("EchoStream returned %d responses, want %d: %+v", len(got.Stream), streamCount, got.Stream)
	}
	for i, response := range got.Stream {
		if response.Message != message {
			t.Errorf("stream response %d message = %q, want %q", i, response.Message, message)
		}
		if int(response.Index) != i {
			t.Errorf("stream response %d index = %d, want %d", i, response.Index, i)
		}
	}

	// The bidi leg is the one that needs frames moving in both directions at
	// once: the Actor sends each message only after reading the response to the
	// previous one, so a path that carried one direction at a time would not
	// return a short answer here, it would hang until the Actor's own 15s
	// timeout and come back as a 502.
	if len(got.Bidi) != bidiCount {
		t.Fatalf("EchoBidi returned %d responses, want %d: %+v", len(got.Bidi), bidiCount, got.Bidi)
	}
	for i, response := range got.Bidi {
		// The Actor numbers each message it sends, so this pins every response
		// to the request it answered.
		want := fmt.Sprintf("%s-%d", message, i)
		if response.Message != want {
			t.Errorf("bidi response %d message = %q, want %q", i, response.Message, want)
		}
		if int(response.Index) != i {
			t.Errorf("bidi response %d index = %d, want %d", i, response.Index, i)
		}
	}
	t.Logf("Actor gRPC egress to %s succeeded; body: %s", target, body)

	// Everything above would also pass if the Actor's traffic had been
	// masqueraded straight out instead of tunneled. This is what says it went
	// through the gateway, on this Actor's own certificate.
	assertEgressGatewayConnect(t, ctx, since, actorAtespace, actorName, strconv.Itoa(grpcEcho.Port))
}

// grpcEchoBodyLimit bounds one /grpc answer. The demo reports sizes and
// de-padded messages rather than the payloads, so answers stay small; the limit
// only guards against a runaway response.
const grpcEchoBodyLimit = 8 << 20

// postGRPCEgress posts input to the Actor's /grpc through the router, allowing
// timeout for the whole call, and requires the answer to be HTTP 200.
func postGRPCEgress(t *testing.T, ctx context.Context, router *e2e.RouterClient, ref resources.ActorRef, input map[string]any, timeout time.Duration) grpcEchoResponse {
	t.Helper()
	payload, err := json.Marshal(input)
	if err != nil {
		t.Fatalf("encoding the gRPC request: %v", err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	response, err := router.PostJSONWithin(ctx, ref, "/grpc", payload, timeout)
	if err != nil {
		t.Fatalf("gRPC egress transport failed: %v", err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, grpcEchoBodyLimit+1))
	if err != nil {
		t.Fatalf("reading the Actor's gRPC response: %v", err)
	}
	if len(raw) > grpcEchoBodyLimit {
		t.Fatalf("the Actor's gRPC response exceeds %d bytes", grpcEchoBodyLimit)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("Actor gRPC egress returned HTTP %d, want 200; body: %s", response.StatusCode, raw)
	}
	var got grpcEchoResponse
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decoding the Actor's gRPC response: %v; body: %s", err, raw)
	}
	return got
}

// assertGRPCMessages checks that got holds want in order, with each entry's
// index equal to its position.
func assertGRPCMessages(t *testing.T, got []grpcEchoStreamedMsg, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d messages, want %d: %+v", len(got), len(want), got)
	}
	for i, m := range got {
		if m.Message != want[i] {
			t.Errorf("message %d = %q, want %q", i, m.Message, want[i])
		}
		if int(m.Index) != i {
			t.Errorf("message %d index = %d, want %d", i, m.Index, i)
		}
	}
}

// TestActorEgressGRPCBidiStreaming drives the bidirectional leg of the gRPC
// demo through the egress gateway over cleartext HTTP/2. Each case shapes one
// stream behavior that the gateway must carry intact: concurrent sends,
// responses after the client half-closes, a status mid-stream, a client
// cancel, large frames, several streams on one connection, and an idle gap.
func TestActorEgressGRPCBidiStreaming(t *testing.T) {
	ctx := t.Context()
	target := e2e.DeployServerPod(t, ctx, grpcEcho).Address()

	actorAtespace, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-grpc-bidi", e2e.EgressFixture(), e2e.EgressAllowAll()...)
	router := mustRouterClient(t, ctx)
	defer router.Close()
	ref := resources.ActorRef{Atespace: actorAtespace, Name: actorName}

	// Bound the access-log scan below to lines this test could have produced.
	since := metav1.NewTime(time.Now().Add(-time.Minute))

	warmup, err := json.Marshal(map[string]any{"target": target, "message": "warmup"})
	if err != nil {
		t.Fatalf("marshaling the warmup request: %v", err)
	}
	if status, body := postThroughEgressActor(t, ctx, router, ref, "/grpc", warmup); status != http.StatusOK {
		t.Fatalf("warmup returned HTTP %d, want 200; body: %s", status, body)
	}

	// base is the request every case starts from; each case adds its fields.
	base := func(extra map[string]any) map[string]any {
		input := map[string]any{"target": target, "message": "m"}
		for k, v := range extra {
			input[k] = v
		}
		return input
	}
	const short = 30 * time.Second

	t.Run("concurrent", func(t *testing.T) {
		got := postGRPCEgress(t, ctx, router, ref, base(map[string]any{"bidiMode": "concurrent", "bidiCount": 50}), short)
		if got.Code != "OK" {
			t.Fatalf("gRPC status = %q, want OK; error: %s", got.Code, got.Error)
		}
		want := make([]string, 50)
		for i := range want {
			want[i] = fmt.Sprintf("m-%d", i)
		}
		assertGRPCMessages(t, got.Bidi, want)
	})

	t.Run("half-close while server sends", func(t *testing.T) {
		for _, mode := range []string{"lockstep", "concurrent"} {
			t.Run(mode, func(t *testing.T) {
				got := postGRPCEgress(t, ctx, router, ref, base(map[string]any{"bidiMode": mode, "bidiCount": 2, "afterCloseCount": 3}), short)
				if got.Code != "OK" {
					t.Fatalf("gRPC status = %q, want OK; error: %s", got.Code, got.Error)
				}
				assertGRPCMessages(t, got.Bidi, []string{"m-0", "m-1", "after-close-0", "after-close-1", "after-close-2"})
			})
		}
	})

	t.Run("status mid-stream", func(t *testing.T) {
		// FailedPrecondition is chosen because Envoy never synthesizes it, so
		// it can only have come from the origin.
		got := postGRPCEgress(t, ctx, router, ref, base(map[string]any{"bidiCount": 5, "failAfter": 2, "failCode": "FailedPrecondition", "expectCode": "FailedPrecondition"}), short)
		if got.Code != "FailedPrecondition" {
			t.Fatalf("gRPC status = %q, want FailedPrecondition; error: %s", got.Code, got.Error)
		}
		if len(got.Bidi) != 2 {
			t.Fatalf("got %d responses before the failure, want 2: %+v", len(got.Bidi), got.Bidi)
		}
		if !strings.Contains(got.Error, "failing after 2 responses as requested") {
			t.Errorf("error = %q, want the origin's failure message", got.Error)
		}
	})

	t.Run("client cancel reaches origin", func(t *testing.T) {
		got := postGRPCEgress(t, ctx, router, ref, base(map[string]any{"bidiCount": 5, "cancelAfter": 2, "expectCode": "Canceled"}), short)
		if got.Code != "Canceled" {
			t.Fatalf("gRPC status = %q, want Canceled; error: %s", got.Code, got.Error)
		}
		if got.ServerOutcome != "canceled" {
			t.Errorf("server outcome = %q, want canceled", got.ServerOutcome)
		}
		if len(got.Bidi) != 2 {
			t.Fatalf("got %d responses before the cancel, want 2: %+v", len(got.Bidi), got.Bidi)
		}
	})

	t.Run("large payload", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			size int
		}{
			{name: "96KiB", size: 96 << 10},
			{name: "1MiBPlus4KiB", size: 1<<20 + 4096},
		} {
			t.Run(tc.name, func(t *testing.T) {
				got := postGRPCEgress(t, ctx, router, ref, base(map[string]any{"bidiCount": 3, "payloadBytes": tc.size, "replyBytes": tc.size}), short)
				if got.Code != "OK" {
					t.Fatalf("gRPC status = %q, want OK; error: %s", got.Code, got.Error)
				}
				if len(got.Bidi) != 3 {
					t.Fatalf("got %d responses, want 3", len(got.Bidi))
				}
				for i, m := range got.Bidi {
					if m.Size != tc.size {
						t.Errorf("response %d size = %d, want %d", i, m.Size, tc.size)
					}
					if want := fmt.Sprintf("m-%d", i); m.Message != want {
						t.Errorf("response %d message = %q, want %q", i, m.Message, want)
					}
				}
			})
		}
	})

	t.Run("concurrent streams on one connection", func(t *testing.T) {
		const streams, bidiCount = 8, 20
		got := postGRPCEgress(t, ctx, router, ref, base(map[string]any{"streams": streams, "bidiMode": "concurrent", "bidiCount": bidiCount}), short)
		if got.Code != "OK" {
			t.Fatalf("gRPC status = %q, want OK; error: %s", got.Code, got.Error)
		}
		if len(got.BidiStreams) != streams {
			t.Fatalf("got %d streams, want %d", len(got.BidiStreams), streams)
		}
		for k, stream := range got.BidiStreams {
			want := make([]string, bidiCount)
			for i := range want {
				want[i] = fmt.Sprintf("m-s%d-%d", k, i)
			}
			assertGRPCMessages(t, stream, want)
		}
	})

	t.Run("idle then resume", func(t *testing.T) {
		// 40s outlasts every 30s connection-level idle timer on the path but not
		// the outer 300s stream_idle_timeout (pinned by
		// TestEgressManifestsPinTheStreamIdleTimeouts), which no request through
		// ingress can reach.
		got := postGRPCEgress(t, ctx, router, ref, base(map[string]any{"bidiCount": 3, "idleAfter": 1, "idleMs": 40000, "timeoutMs": 60000}), 90*time.Second)
		if got.Code != "OK" {
			t.Fatalf("gRPC status = %q, want OK; error: %s", got.Code, got.Error)
		}
		assertGRPCMessages(t, got.Bidi, []string{"m-0", "m-1", "m-2"})
	})

	assertEgressGatewayConnect(t, ctx, since, actorAtespace, actorName, strconv.Itoa(grpcEcho.Port))
}
