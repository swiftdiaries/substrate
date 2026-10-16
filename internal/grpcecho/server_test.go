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

package grpcecho

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/proto/grpcechopb"
)

// dialLocal starts the server on loopback and returns a connection to it. A
// real listener rather than an in-memory pipe, so the registrations and the
// HTTP/2 framing are the ones the pod serves.
func dialLocal(t *testing.T) *grpc.ClientConn {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	server := NewServer()
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("serving: %v", err)
		}
	}()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing %s: %v", listener.Addr(), err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// waitForOutcome polls Echo until the recorded outcome of stream id is want.
// The server records an outcome when its handler returns, which can be after the
// client has seen the stream end, so a single read is not enough.
func waitForOutcome(t *testing.T, client grpcechopb.EchoClient, id, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), OutcomeOfKey, id), 2*time.Second)
		response, err := client.Echo(ctx, &grpcechopb.EchoRequest{})
		cancel()
		if err != nil {
			t.Fatalf("Echo for outcome of %q: %v", id, err)
		}
		if response.GetMessage() == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("outcome of %q = %q, want %q", id, response.GetMessage(), want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestEcho(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	for _, message := range []string{"hello", "", "unicode: é世界", "with spaces and\nnewline"} {
		response, err := client.Echo(testContext(t), &grpcechopb.EchoRequest{Message: message})
		if err != nil {
			t.Fatalf("Echo(%q): %v", message, err)
		}
		if response.GetMessage() != message {
			t.Errorf("Echo(%q) = %q, want the message back unchanged", message, response.GetMessage())
		}
	}
}

func TestEchoStream(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	const (
		message = "streamed"
		count   = 3
	)
	stream, err := client.EchoStream(testContext(t), &grpcechopb.EchoStreamRequest{Message: message, Count: count})
	if err != nil {
		t.Fatalf("EchoStream: %v", err)
	}

	var got []*grpcechopb.EchoResponse
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("EchoStream Recv after %d responses: %v", len(got), err)
		}
		got = append(got, response)
	}

	if len(got) != count {
		t.Fatalf("EchoStream sent %d responses, want %d", len(got), count)
	}
	for i, response := range got {
		if response.GetMessage() != message {
			t.Errorf("response %d message = %q, want %q", i, response.GetMessage(), message)
		}
		if int(response.GetIndex()) != i {
			t.Errorf("response %d index = %d, want %d", i, response.GetIndex(), i)
		}
	}
}

// A non-positive count must fail loudly. Returning an empty stream instead
// would make a caller that forgot to set count look like a working one.
func TestEchoStreamRejectsBadCount(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	for _, count := range []int32{0, -1, MaxStreamCount + 1} {
		stream, err := client.EchoStream(testContext(t), &grpcechopb.EchoStreamRequest{Message: "x", Count: count})
		if err == nil {
			_, err = stream.Recv()
		}
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("EchoStream(count=%d) status = %s (%v), want InvalidArgument", count, got, err)
		}
	}
}

// The bidi handler must answer each request as it arrives. Sending everything
// and only then reading would pass against a server that drains first, which is
// exactly the implementation this test exists to rule out -- so this sends one
// message at a time and blocks on its response before sending the next.
func TestEchoBidi(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	stream, err := client.EchoBidi(testContext(t))
	if err != nil {
		t.Fatalf("EchoBidi: %v", err)
	}

	const count = 3
	for i := range count {
		message := fmt.Sprintf("message-%d", i)
		if err := stream.Send(&grpcechopb.EchoRequest{Message: message}); err != nil {
			t.Fatalf("EchoBidi Send %d: %v", i, err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("EchoBidi Recv %d: %v", i, err)
		}
		if response.GetMessage() != message {
			t.Errorf("response %d message = %q, want %q", i, response.GetMessage(), message)
		}
		if int(response.GetIndex()) != i {
			t.Errorf("response %d index = %d, want %d", i, response.GetIndex(), i)
		}
	}

	// Half-close the request direction and drain the response direction. The
	// server must end with OK here: a handler that treated the half-close as an
	// error would still have echoed everything above.
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("EchoBidi CloseSend: %v", err)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("EchoBidi Recv after CloseSend = %v, want io.EOF", err)
	}
}

func TestEchoBidiFailAfter(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	const id = "fail-after"
	ctx := metadata.AppendToOutgoingContext(testContext(t), FailAfterKey, "2", FailCodeKey, "9", StreamIDKey, id)
	stream, err := client.EchoBidi(ctx)
	if err != nil {
		t.Fatalf("EchoBidi: %v", err)
	}

	for i := range 2 {
		message := fmt.Sprintf("message-%d", i)
		if err := stream.Send(&grpcechopb.EchoRequest{Message: message}); err != nil {
			t.Fatalf("EchoBidi Send %d: %v", i, err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("EchoBidi Recv %d: %v", i, err)
		}
		if response.GetMessage() != message {
			t.Errorf("response %d message = %q, want %q", i, response.GetMessage(), message)
		}
	}

	_, err = stream.Recv()
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("Recv after 2 responses status = %s (%v), want FailedPrecondition", got, err)
	}
	if got, want := status.Convert(err).Message(), "failing after 2 responses as requested"; got != want {
		t.Errorf("Recv error message = %q, want %q", got, want)
	}
	waitForOutcome(t, client, id, OutcomeError)
}

func TestEchoBidiReplyBytes(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	const size = 100000
	ctx := metadata.AppendToOutgoingContext(testContext(t), ReplyBytesKey, fmt.Sprint(size))
	stream, err := client.EchoBidi(ctx)
	if err != nil {
		t.Fatalf("EchoBidi: %v", err)
	}

	for _, message := range []string{"first", "second"} {
		if err := stream.Send(&grpcechopb.EchoRequest{Message: message}); err != nil {
			t.Fatalf("EchoBidi Send %q: %v", message, err)
		}
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("EchoBidi Recv for %q: %v", message, err)
		}
		if len(response.GetMessage()) != size {
			t.Errorf("response for %q is %d bytes, want %d", message, len(response.GetMessage()), size)
		}
		if got := strings.TrimRight(response.GetMessage(), "."); got != message {
			t.Errorf("response for %q trimmed of padding = %q, want %q", message, got, message)
		}
	}
}

func TestEchoBidiAfterClose(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	ctx := metadata.AppendToOutgoingContext(testContext(t), AfterCloseCountKey, "3")
	stream, err := client.EchoBidi(ctx)
	if err != nil {
		t.Fatalf("EchoBidi: %v", err)
	}

	if err := stream.Send(&grpcechopb.EchoRequest{Message: "hello"}); err != nil {
		t.Fatalf("EchoBidi Send: %v", err)
	}
	if response, err := stream.Recv(); err != nil || response.GetIndex() != 0 {
		t.Fatalf("EchoBidi first response = %v, %v; want index 0", response, err)
	}

	if err := stream.CloseSend(); err != nil {
		t.Fatalf("EchoBidi CloseSend: %v", err)
	}
	// The first response took index 0, so the after-close ones continue at 1.
	for j := range 3 {
		response, err := stream.Recv()
		if err != nil {
			t.Fatalf("EchoBidi after-close Recv %d: %v", j, err)
		}
		if want := fmt.Sprintf("after-close-%d", j); response.GetMessage() != want {
			t.Errorf("after-close response %d message = %q, want %q", j, response.GetMessage(), want)
		}
		if want := int32(j + 1); response.GetIndex() != want {
			t.Errorf("after-close response %d index = %d, want %d", j, response.GetIndex(), want)
		}
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Errorf("EchoBidi Recv after after-close responses = %v, want io.EOF", err)
	}
}

// The pause must hold the second response back by the full idle duration. The
// clock starts before the first send: the server's pause begins after it sends
// the first response, which is after that send, so the bound holds without
// depending on how quickly the client sees that response.
func TestEchoBidiIdle(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	const idle = 300 * time.Millisecond
	ctx := metadata.AppendToOutgoingContext(testContext(t), IdleAfterKey, "1", IdleMsKey, fmt.Sprint(idle.Milliseconds()))
	stream, err := client.EchoBidi(ctx)
	if err != nil {
		t.Fatalf("EchoBidi: %v", err)
	}

	start := time.Now()
	for i := range 2 {
		if err := stream.Send(&grpcechopb.EchoRequest{Message: "x"}); err != nil {
			t.Fatalf("EchoBidi Send %d: %v", i, err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("EchoBidi Recv %d: %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed < idle {
		t.Errorf("second response arrived after %v, want at least %v", elapsed, idle)
	}
}

// A stream that ends by the client's context being canceled must be recorded
// as canceled, and one that the client half-closes as eof.
func TestEchoBidiOutcome(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	t.Run("canceled", func(t *testing.T) {
		const id = "outcome-canceled"
		base := metadata.AppendToOutgoingContext(context.Background(), StreamIDKey, id)
		ctx, cancel := context.WithCancel(base)
		defer cancel()

		stream, err := client.EchoBidi(ctx)
		if err != nil {
			t.Fatalf("EchoBidi: %v", err)
		}
		if err := stream.Send(&grpcechopb.EchoRequest{Message: "one"}); err != nil {
			t.Fatalf("EchoBidi Send: %v", err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("EchoBidi Recv: %v", err)
		}

		cancel()
		waitForOutcome(t, client, id, OutcomeCanceled)
	})

	t.Run("eof", func(t *testing.T) {
		const id = "outcome-eof"
		ctx := metadata.AppendToOutgoingContext(testContext(t), StreamIDKey, id)
		stream, err := client.EchoBidi(ctx)
		if err != nil {
			t.Fatalf("EchoBidi: %v", err)
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatalf("EchoBidi CloseSend: %v", err)
		}
		if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
			t.Fatalf("EchoBidi Recv after CloseSend = %v, want io.EOF", err)
		}
		waitForOutcome(t, client, id, OutcomeEOF)
	})
}

// Each of these must fail the stream with InvalidArgument rather than fall back
// to a default, so a typo in a test's metadata is visible.
func TestEchoBidiRejectsInvalidControls(t *testing.T) {
	client := grpcechopb.NewEchoClient(dialLocal(t))

	for _, tc := range []struct {
		name string
		kv   []string
	}{
		{"fail-after without fail-code", []string{FailAfterKey, "2"}},
		{"fail-code without fail-after", []string{FailCodeKey, "9"}},
		{"fail-code above 16", []string{FailAfterKey, "2", FailCodeKey, "17"}},
		{"fail-after not a number", []string{FailAfterKey, "two", FailCodeKey, "9"}},
		{"idle-after without idle-ms", []string{IdleAfterKey, "1"}},
		{"idle-ms of zero", []string{IdleAfterKey, "1", IdleMsKey, "0"}},
		{"reply-bytes over the limit", []string{ReplyBytesKey, fmt.Sprint(MaxPayloadBytes + 1)}},
		{"after-close-count over the limit", []string{AfterCloseCountKey, fmt.Sprint(MaxStreamCount + 1)}},
		{"reply-bytes given twice", []string{ReplyBytesKey, "10", ReplyBytesKey, "20"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := metadata.AppendToOutgoingContext(testContext(t), tc.kv...)
			stream, err := client.EchoBidi(ctx)
			if err == nil {
				_, err = stream.Recv()
			}
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("status = %s (%v), want InvalidArgument", got, err)
			}
		})
	}
}

func TestPad(t *testing.T) {
	for _, tc := range []struct {
		message string
		size    int
		want    string
	}{
		{"", 0, ""},
		{"ab", 5, "ab..."},
		{"", 3, "..."},
		{"abcde", 5, "abcde"},
		{"abcdef", 3, "abcdef"},
	} {
		if got := Pad(tc.message, tc.size); got != tc.want {
			t.Errorf("Pad(%q, %d) = %q, want %q", tc.message, tc.size, got, tc.want)
		}
	}
}
