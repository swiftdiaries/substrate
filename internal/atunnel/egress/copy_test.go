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

package egress

import (
	"context"
	"io"
	"net"
	"testing"
	"time"
)

// tcpPair returns two ends of a loopback TCP connection.
func tcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	dialed, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	accepted, err := listener.Accept()
	if err != nil {
		_ = dialed.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = dialed.Close()
		_ = accepted.Close()
	})
	deadline := time.Now().Add(5 * time.Second)
	if err := dialed.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	if err := accepted.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	return dialed.(*net.TCPConn), accepted.(*net.TCPConn)
}

// A client's half-close reaches the origin as EOF, and the origin's half-close
// reaches the client as EOF, while each direction keeps flowing.
func TestCopyBothWaysPropagatesHalfClose(t *testing.T) {
	actor, proxyDown := tcpPair(t)
	proxyUp, origin := tcpPair(t)

	done := make(chan struct{})
	go func() {
		copyBothWays(proxyDown, proxyUp)
		close(done)
	}()

	if _, err := io.WriteString(actor, "request"); err != nil {
		t.Fatal(err)
	}
	if err := actor.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	request, err := io.ReadAll(origin)
	if err != nil {
		t.Fatal(err)
	}
	if string(request) != "request" {
		t.Fatalf("origin received %q, want %q", request, "request")
	}

	if _, err := io.WriteString(origin, "after half-close"); err != nil {
		t.Fatal(err)
	}
	if err := origin.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(actor)
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != "after half-close" {
		t.Fatalf("actor received %q, want %q", reply, "after half-close")
	}
	receiveWithin(t, done, "copyBothWays to return")
}

// An upstream that cannot half-close is closed outright once the origin is
// done, which still ends the actor's read with EOF.
func TestCopyBothWaysWithoutCloseWrite(t *testing.T) {
	actor, proxyDown := tcpPair(t)
	proxyUp, origin := net.Pipe()
	t.Cleanup(func() { _ = origin.Close() })

	done := make(chan struct{})
	go func() {
		copyBothWays(proxyDown, proxyUp)
		close(done)
	}()

	if _, err := io.WriteString(actor, "request"); err != nil {
		t.Fatal(err)
	}
	if err := actor.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	// A pipe never reports EOF to the origin here, so read only the bytes sent.
	request := make([]byte, len("request"))
	if _, err := io.ReadFull(origin, request); err != nil {
		t.Fatal(err)
	}
	if string(request) != "request" {
		t.Fatalf("origin received %q, want %q", request, "request")
	}

	if _, err := io.WriteString(origin, "reply"); err != nil {
		t.Fatal(err)
	}
	if err := origin.Close(); err != nil {
		t.Fatal(err)
	}
	reply, err := io.ReadAll(actor)
	if err != nil {
		t.Fatal(err)
	}
	if string(reply) != "reply" {
		t.Fatalf("actor received %q, want %q", reply, "reply")
	}
	receiveWithin(t, done, "copyBothWays to return")
}

// Deactivate stops an in-flight tunnel: it waits for the forwarding goroutines
// to exit, and both ends of the tunnel are closed afterwards.
func TestEgressDeactivateStopsActiveTunnel(t *testing.T) {
	originListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = originListener.Close() })
	originAddr := originListener.Addr().String()

	// The origin echoes one message, then waits for the proxy to close the
	// tunnel and reports how that read ended.
	originDone := make(chan error, 1)
	go func() {
		conn, err := originListener.Accept()
		if err != nil {
			originDone <- err
			return
		}
		defer conn.Close()
		buf := make([]byte, len("ping"))
		if _, err := io.ReadFull(conn, buf); err != nil {
			originDone <- err
			return
		}
		if _, err := conn.Write(buf); err != nil {
			originDone <- err
			return
		}
		_, err = io.ReadAll(conn)
		originDone <- err
	}()

	egress, err := New(func(net.Conn) (string, error) { return "192.0.2.10:443", nil })
	if err != nil {
		t.Fatal(err)
	}
	dialer := tunnelDialerFunc(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "tcp", originAddr)
	})
	if err := egress.Activate(testActorUID, dialer, fakeActorCertificateSource{expiresAt: time.Now().Add(time.Hour)}, time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	actor, proxyDown := tcpPair(t)
	egress.handle(proxyDown, egress.active[testActorUID])

	if _, err := io.WriteString(actor, "ping"); err != nil {
		t.Fatal(err)
	}
	echo := make([]byte, len("ping"))
	if _, err := io.ReadFull(actor, echo); err != nil {
		t.Fatal(err)
	}
	if string(echo) != "ping" {
		t.Fatalf("actor received %q, want %q", echo, "ping")
	}

	// A nil error means the handler and copy goroutines have exited.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := egress.Deactivate(ctx, testActorUID); err != nil {
		t.Fatalf("Deactivate() = %v, want nil", err)
	}

	if _, err := actor.Read(make([]byte, 1)); err == nil {
		t.Fatal("actor connection remained open after Deactivate")
	}
	receiveWithin(t, originDone, "origin to observe the tunnel closing")
}
