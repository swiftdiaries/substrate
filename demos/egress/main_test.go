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
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"

	"go.ate.dev/substrate/internal/grpcecho"
	"go.ate.dev/substrate/internal/testcert"
)

func TestFetch(t *testing.T) {
	const traceparent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01"
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet {
			t.Errorf("upstream method = %s, want GET", r.Method)
		}
		if got := r.Header.Get("traceparent"); got != traceparent {
			t.Errorf("upstream traceparent = %q, want %q", got, traceparent)
		}
		return &http.Response{
			StatusCode: http.StatusTeapot,
			Body:       io.NopCloser(strings.NewReader("hello from upstream")),
			Header:     make(http.Header),
		}, nil
	})}

	payload, err := json.Marshal(fetchRequest{URL: "https://allowed.example/"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload)))
	request.Header.Set("traceparent", traceparent)
	newHandler(client).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusTeapot)
	}
	var got fetchResponse
	if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
		t.Fatalf("decoding response: %v", err)
	}
	if got.StatusCode != http.StatusTeapot || got.Body != "hello from upstream" {
		t.Errorf("response = %+v", got)
	}
}

func TestFetchHTTPAndHTTPS(t *testing.T) {
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "from http")
	}))
	defer httpSrv.Close()

	httpsSrv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "from https")
	}))
	defer httpsSrv.Close()

	// The demo verifies origins with the roots its client is given: in the
	// cluster the projected trust bundle, here the test server's own.
	httpsClient := httpsSrv.Client()
	httpsClient.Timeout = requestTimeout
	handler := newHandler(httpsClient)
	for _, tc := range []struct {
		name     string
		url      string
		want     string
		wantCert string
	}{
		{name: "http", url: httpSrv.URL, want: "from http"},
		{name: "https", url: httpsSrv.URL, want: "from https", wantCert: httpsSrv.Certificate().Issuer.String()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(fetchRequest{URL: tc.url})
			if err != nil {
				t.Fatal(err)
			}
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload)))
			handler.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
			}
			var got fetchResponse
			if err := json.NewDecoder(recorder.Body).Decode(&got); err != nil {
				t.Fatalf("decoding response: %v", err)
			}
			if got.StatusCode != http.StatusOK || got.Body != tc.want || got.ServerCert != tc.wantCert {
				t.Errorf("response = %+v, want status 200, body %q, serverCert %q", got, tc.want, tc.wantCert)
			}
		})
	}
}

func TestInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
		status int
	}{
		{name: "method", method: http.MethodGet, body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, body: `{`, status: http.StatusBadRequest},
		{name: "missing hostname", method: http.MethodPost, body: `{"url":"https:///path"}`, status: http.StatusBadRequest},
		{name: "unsupported scheme", method: http.MethodPost, body: `{"url":"file:///etc/passwd"}`, status: http.StatusBadRequest},
	}

	handler := newHandler(http.DefaultClient)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "/", strings.NewReader(test.body))
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d; body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}

func TestOutboundFailure(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("blocked")
	})}
	handler := newHandler(client)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"url":"https://example.com/"}`))

	handler.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

func TestFetchWithRequestScopedTLSAndHTTP1(t *testing.T) {
	server, rootCA := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Proto != "HTTP/1.1" {
			t.Errorf("request protocol = %s, want HTTP/1.1", r.Proto)
		}
		_, _ = io.WriteString(w, "trusted")
	}))

	recorder := postHandler(t, newHandler(http.DefaultClient), "/", fetchRequest{
		URL:    server.URL,
		RootCA: rootCA,
		HTTP1:  true,
	})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got fetchResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusOK || got.Body != "trusted" || got.Protocol != "HTTP/1.1" || !got.TLS {
		t.Errorf("response = %+v, want successful verified HTTP/1.1 TLS response", got)
	}
}

func TestFetchTLSVerificationFailure(t *testing.T) {
	server, _ := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "should not arrive")
	}))

	recorder := postHandler(t, newHandler(http.DefaultClient), "/", fetchRequest{URL: server.URL})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
	var got fetchResponse
	decodeRecorder(t, recorder, &got)
	if got.Error == "" || got.Body != "" || got.StatusCode != 0 {
		t.Errorf("response = %+v, want TLS failure without upstream response", got)
	}
}

func TestFetchTLSWrongRootFails(t *testing.T) {
	server, _ := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "should not arrive")
	}))
	_, wrongRoot := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "other server")
	}))

	recorder := postHandler(t, newHandler(http.DefaultClient), "/", fetchRequest{URL: server.URL, RootCA: wrongRoot})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

func TestFetchDoesNotFollowRedirects(t *testing.T) {
	redirected := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected = true
		_, _ = io.WriteString(w, "redirected")
	}))
	t.Cleanup(target.Close)
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(http.StatusFound)
	}))
	t.Cleanup(source.Close)

	recorder := postHandler(t, newHandler(http.DefaultClient), "/", fetchRequest{URL: source.URL, HTTP1: true})
	if recorder.Code != http.StatusFound {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusFound, recorder.Body.String())
	}
	if redirected {
		t.Fatal("redirect target received a request")
	}
	var got fetchResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusFound || got.Protocol == "" {
		t.Errorf("response = %+v, want observed redirect response", got)
	}
}

func TestWebSocket(t *testing.T) {
	server := startWebSocketServer(t, false)
	messages := []string{"one", "two", "three"}
	recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server), Messages: messages})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got websocketResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusSwitchingProtocols || got.Protocol != "HTTP/1.1" || got.TLS || got.Error != "" {
		t.Errorf("response = %+v, want successful plaintext handshake", got)
	}
	if len(got.Messages) != len(messages) {
		t.Fatalf("messages = %+v, want %d observations", got.Messages, len(messages))
	}
	for i, message := range got.Messages {
		if message.Type != websocket.TextMessage || message.Message != messages[i] {
			t.Errorf("messages[%d] = %+v, want text %q", i, message, messages[i])
		}
	}
}

func TestWebSocketTLSAndVerificationFailure(t *testing.T) {
	server, rootCA := startIPTLSServer(t, webSocketEchoHandler(false))
	messages := []string{"tls-one", "tls-two", "tls-three"}
	recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server.URL), RootCA: rootCA, Messages: messages})
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	var got websocketResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusSwitchingProtocols || got.Protocol != "HTTP/1.1" || !got.TLS || len(got.Messages) != len(messages) {
		t.Errorf("response = %+v, want verified TLS observations", got)
	}

	recorder = postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server.URL), Messages: messages})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("untrusted status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
}

func TestWebSocketRejectsNonUpgrade(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "not websocket")
	}))
	t.Cleanup(server.Close)

	recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server.URL), Messages: []string{"one", "two", "three"}})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
	var got websocketResponse
	decodeRecorder(t, recorder, &got)
	if got.StatusCode != http.StatusOK || got.Protocol != "HTTP/1.1" || got.TLS || got.Error == "" {
		t.Errorf("response = %+v, want preserved non-upgrade response and stage error", got)
	}
}

func TestWebSocketPreservesBoundedHandshakeBody(t *testing.T) {
	for _, tc := range []struct {
		name       string
		body       string
		want       string
		wantPrefix bool
	}{
		{name: "exact denial body", body: "WebSocket egress is not supported", want: "WebSocket egress is not supported"},
		{name: "oversized body", body: strings.Repeat("x", maxWebSocketHandshakeBody+1), wantPrefix: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = io.WriteString(w, tc.body)
			}))
			t.Cleanup(server.Close)

			recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server.URL), Messages: []string{"probe"}})
			if recorder.Code != http.StatusBadGateway {
				t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
			}
			var got websocketResponse
			decodeRecorder(t, recorder, &got)
			if got.StatusCode != http.StatusForbidden {
				t.Errorf("handshake status = %d, want %d", got.StatusCode, http.StatusForbidden)
			}
			if tc.wantPrefix {
				if len(got.HandshakeBody) == 0 || len(got.HandshakeBody) > maxWebSocketHandshakeBody || !strings.HasPrefix(tc.body, got.HandshakeBody) {
					t.Errorf("handshake body length = %d, want a prefix no longer than %d bytes", len(got.HandshakeBody), maxWebSocketHandshakeBody)
				}
			} else if got.HandshakeBody != tc.want {
				t.Errorf("handshake body = %q, want %q", got.HandshakeBody, tc.want)
			}
		})
	}
}

func TestWebSocketPreservesPartialObservationsOnBadResponse(t *testing.T) {
	server := startWebSocketServer(t, true)
	recorder := postHandler(t, newHandler(http.DefaultClient), "/websocket", websocketRequest{URL: toWebSocketURL(server), Messages: []string{"one", "two", "three"}})
	if recorder.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusBadGateway, recorder.Body.String())
	}
	var got websocketResponse
	decodeRecorder(t, recorder, &got)
	if len(got.Messages) != 1 || got.Messages[0].Message != "one" || got.Messages[0].Type != websocket.TextMessage || got.Error == "" {
		t.Errorf("response = %+v, want first observation and read failure", got)
	}
}

func postHandler(t *testing.T, handler http.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	handler.ServeHTTP(recorder, request)
	return recorder
}

func decodeRecorder(t *testing.T, recorder *httptest.ResponseRecorder, dst any) {
	t.Helper()
	if err := json.NewDecoder(recorder.Body).Decode(dst); err != nil {
		t.Fatalf("decoding response (HTTP %d): %v", recorder.Code, err)
	}
}

func startWebSocketServer(t *testing.T, badSecondResponse bool) string {
	t.Helper()
	server := httptest.NewServer(webSocketEchoHandler(badSecondResponse))
	t.Cleanup(server.Close)
	return server.URL
}

func webSocketEchoHandler(badSecondResponse bool) http.Handler {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for index := 0; ; index++ {
			messageType, message, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if badSecondResponse && index == 1 {
				_ = conn.Close()
				return
			}
			if err := conn.WriteMessage(messageType, message); err != nil {
				return
			}
		}
	})
}

func startIPTLSServer(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	material := testcert.NewServerTLS(t, net.ParseIP("127.0.0.1"))
	cert, err := tls.X509KeyPair(material.Certificate, material.PrivateKey)
	if err != nil {
		t.Fatalf("loading server certificate: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.Listener = listener
	server.TLS = &tls.Config{Certificates: []tls.Certificate{cert}}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server, string(material.RootCA)
}

func toWebSocketURL(raw string) string {
	if strings.HasPrefix(raw, "https://") {
		return strings.Replace(raw, "https://", "wss://", 1)
	}
	return strings.Replace(raw, "http://", "ws://", 1)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

// The gRPC endpoint below is what an e2e reads as evidence that gRPC crossed
// the egress tunnel, so these tests pin its answers against a loopback server,
// where there is no tunnel to blame.

// startEchoServer serves the grpcecho Echo service on loopback and returns its
// host:port. It is the same server the egress e2e origin runs.
func startEchoServer(t *testing.T) string {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	server := grpcecho.NewServer()
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("serving: %v", err)
		}
	}()
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

// postGRPC drives the /grpc endpoint and returns the recorder and the decoded
// body, which is what the e2e asserts on.
func postGRPC(t *testing.T, body string) (*httptest.ResponseRecorder, grpcResponse) {
	t.Helper()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/grpc", strings.NewReader(body))
	newHandler(http.DefaultClient).ServeHTTP(recorder, request)

	var decoded grpcResponse
	if err := json.NewDecoder(recorder.Body).Decode(&decoded); err != nil {
		t.Fatalf("decoding response (HTTP %d): %v", recorder.Code, err)
	}
	return recorder, decoded
}

func TestGRPCUnary(t *testing.T) {
	target := startEchoServer(t)

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"hello"}`, target))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if got.Message != "hello" {
		t.Errorf("message = %q, want %q", got.Message, "hello")
	}
	if got.Code != codes.OK.String() {
		t.Errorf("code = %q, want %q", got.Code, codes.OK.String())
	}
	// A request that did not ask for a stream must not report one, so the e2e
	// cannot pass its streaming assertion against leftover unary state.
	if got.Stream != nil {
		t.Errorf("stream = %+v, want none for a request with no streamCount", got.Stream)
	}
}

func TestGRPCStream(t *testing.T) {
	target := startEchoServer(t)
	const count = 3

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"streamed","streamCount":%d}`, target, count))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if len(got.Stream) != count {
		t.Fatalf("stream has %d messages, want %d: %+v", len(got.Stream), count, got.Stream)
	}
	for i, message := range got.Stream {
		if message.Message != "streamed" {
			t.Errorf("stream[%d].message = %q, want %q", i, message.Message, "streamed")
		}
		if int(message.Index) != i {
			t.Errorf("stream[%d].index = %d, want %d", i, message.Index, i)
		}
	}
}

func TestGRPCBidi(t *testing.T) {
	target := startEchoServer(t)
	const count = 3

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"duplex","bidiCount":%d}`, target, count))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if len(got.Bidi) != count {
		t.Fatalf("bidi has %d messages, want %d: %+v", len(got.Bidi), count, got.Bidi)
	}
	// The handler numbers its own messages, so the echoed text pins each
	// response to the request it answered rather than to any response.
	for i, message := range got.Bidi {
		want := fmt.Sprintf("duplex-%d", i)
		if message.Message != want {
			t.Errorf("bidi[%d].message = %q, want %q", i, message.Message, want)
		}
		if int(message.Index) != i {
			t.Errorf("bidi[%d].index = %d, want %d", i, message.Index, i)
		}
	}
	// Asking only for a bidi stream must not report a server-stream, so the
	// e2e's two assertions cannot pass on each other's data.
	if got.Stream != nil {
		t.Errorf("stream = %+v, want none for a request with no streamCount", got.Stream)
	}
}

func TestGRPCBidiConcurrent(t *testing.T) {
	target := startEchoServer(t)
	const count = 50

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"burst","bidiCount":%d,"bidiMode":"concurrent"}`, target, count))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if len(got.Bidi) != count {
		t.Fatalf("bidi has %d messages, want %d", len(got.Bidi), count)
	}
	for i, message := range got.Bidi {
		if want := fmt.Sprintf("burst-%d", i); message.Message != want {
			t.Errorf("bidi[%d].message = %q, want %q", i, message.Message, want)
		}
		if int(message.Index) != i {
			t.Errorf("bidi[%d].index = %d, want %d", i, message.Index, i)
		}
	}
}

// The after-close responses follow the bidi responses and continue the index.
func TestGRPCBidiAfterClose(t *testing.T) {
	target := startEchoServer(t)

	for _, mode := range []string{"lockstep", "concurrent"} {
		t.Run(mode, func(t *testing.T) {
			recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"ac","bidiCount":2,"afterCloseCount":3,"bidiMode":%q}`, target, mode))

			if recorder.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
			}
			if len(got.Bidi) != 5 {
				t.Fatalf("bidi has %d messages, want 5: %+v", len(got.Bidi), got.Bidi)
			}
			for i := range 2 {
				if want := fmt.Sprintf("ac-%d", i); got.Bidi[i].Message != want || int(got.Bidi[i].Index) != i {
					t.Errorf("bidi[%d] = %+v, want message %q index %d", i, got.Bidi[i], want, i)
				}
			}
			for j := range 3 {
				want := fmt.Sprintf("after-close-%d", j)
				if got.Bidi[2+j].Message != want || int(got.Bidi[2+j].Index) != 2+j {
					t.Errorf("bidi[%d] = %+v, want message %q index %d", 2+j, got.Bidi[2+j], want, 2+j)
				}
			}
		})
	}
}

// A failure the caller expects is a pass: 200, with the code and the partial
// results it got before the failure.
func TestGRPCBidiExpectedCode(t *testing.T) {
	target := startEchoServer(t)

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"fc","bidiCount":3,"failAfter":2,"failCode":"FailedPrecondition","expectCode":"FailedPrecondition"}`, target))

	if recorder.Code != http.StatusOK {
		t.Errorf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if got.Code != codes.FailedPrecondition.String() {
		t.Errorf("code = %q, want %q; error = %s", got.Code, codes.FailedPrecondition.String(), got.Error)
	}
	if len(got.Bidi) != 2 {
		t.Errorf("bidi has %d messages, want 2 partial results: %+v", len(got.Bidi), got.Bidi)
	}
	if !strings.Contains(got.Error, "failing after 2 responses as requested") {
		t.Errorf("error = %q, want it to report the server's failure", got.Error)
	}
}

// The same failure without an expectCode is an unexpected code, so 502.
func TestGRPCBidiUnexpectedCode(t *testing.T) {
	target := startEchoServer(t)

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"fc","bidiCount":3,"failAfter":2,"failCode":"FailedPrecondition"}`, target))

	if recorder.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
	if got.Code != codes.FailedPrecondition.String() {
		t.Errorf("code = %q, want %q", got.Code, codes.FailedPrecondition.String())
	}
}

// Canceling the client side must reach the server, which records the stream
// as canceled.
func TestGRPCBidiCancelReachesServer(t *testing.T) {
	target := startEchoServer(t)

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"cancel","bidiCount":3,"cancelAfter":1,"expectCode":"Canceled"}`, target))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if got.Code != codes.Canceled.String() {
		t.Errorf("code = %q, want %q", got.Code, codes.Canceled.String())
	}
	if got.ServerOutcome != "canceled" {
		t.Errorf("serverOutcome = %q, want %q", got.ServerOutcome, "canceled")
	}
}

// Payloads past the old 64 KiB frame size must arrive whole in both
// directions, and each message must report the size it arrived at.
func TestGRPCBidiLargePayload(t *testing.T) {
	target := startEchoServer(t)
	const size = 1<<20 + 4096

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"big","bidiCount":2,"payloadBytes":%d,"replyBytes":%d}`, target, size, size))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if len(got.Bidi) != 2 {
		t.Fatalf("bidi has %d messages, want 2", len(got.Bidi))
	}
	for i, message := range got.Bidi {
		if message.Size != size {
			t.Errorf("bidi[%d].size = %d, want %d", i, message.Size, size)
		}
		if want := fmt.Sprintf("big-%d", i); message.Message != want {
			t.Errorf("bidi[%d].message trimmed of padding = %q, want %q", i, message.Message, want)
		}
	}
}

func TestGRPCBidiMultipleStreams(t *testing.T) {
	target := startEchoServer(t)
	const streams, count = 4, 10

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"m","bidiCount":%d,"streams":%d}`, target, count, streams))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if len(got.BidiStreams) != streams {
		t.Fatalf("bidiStreams has %d streams, want %d", len(got.BidiStreams), streams)
	}
	for k, stream := range got.BidiStreams {
		if len(stream) != count {
			t.Errorf("bidiStreams[%d] has %d messages, want %d", k, len(stream), count)
			continue
		}
		for i, message := range stream {
			if want := fmt.Sprintf("m-s%d-%d", k, i); message.Message != want {
				t.Errorf("bidiStreams[%d][%d].message = %q, want %q", k, i, message.Message, want)
			}
			if int(message.Index) != i {
				t.Errorf("bidiStreams[%d][%d].index = %d, want %d", k, i, message.Index, i)
			}
		}
	}
}

func TestGRPCBidiIdle(t *testing.T) {
	target := startEchoServer(t)

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"idle","bidiCount":3,"idleAfter":1,"idleMs":300}`, target))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if len(got.Bidi) != 3 {
		t.Errorf("bidi has %d messages, want 3: %+v", len(got.Bidi), got.Bidi)
	}
}

// A failed RPC must still report its gRPC code. That code is the only part of
// the answer that proves trailers arrived, so collapsing it into the HTTP
// status would erase the thing the e2e is looking for.
func TestGRPCFailureReportsStatusCode(t *testing.T) {
	// A port with nothing behind it: the dial fails, and grpc-go reports that
	// as Unavailable.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	target := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("closing loopback listener: %v", err)
	}

	recorder, got := postGRPC(t, fmt.Sprintf(`{"target":%q,"message":"hello"}`, target))

	if recorder.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
	if got.Code != codes.Unavailable.String() {
		t.Errorf("code = %q, want %q; error = %s", got.Code, codes.Unavailable.String(), got.Error)
	}
}

// startTLSEchoServer serves the grpcecho Echo service over TLS on loopback,
// presenting material's certificate, and returns its host:port.
func startTLSEchoServer(t *testing.T, material testcert.ServerTLS) string {
	t.Helper()

	cert, err := tls.X509KeyPair(material.Certificate, material.PrivateKey)
	if err != nil {
		t.Fatalf("loading server certificate: %v", err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listening on loopback: %v", err)
	}
	server := grpcecho.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2"},
	})))
	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("serving: %v", err)
		}
	}()
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

func TestGRPCOverTLS(t *testing.T) {
	material := testcert.NewServerTLS(t, net.ParseIP("127.0.0.1"))
	target := startTLSEchoServer(t, material)
	const count = 3
	body, err := json.Marshal(map[string]any{
		"target":    target,
		"message":   "secure",
		"tls":       true,
		"rootCA":    string(material.RootCA),
		"bidiCount": count,
	})
	if err != nil {
		t.Fatal(err)
	}

	recorder, got := postGRPC(t, string(body))

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %+v", recorder.Code, http.StatusOK, got)
	}
	if !got.TLS {
		t.Error("tls = false, want true")
	}
	if got.ServerCertIssuer == "" {
		t.Error("serverCertIssuer is empty, want the issuer of the server certificate")
	}
	if len(got.Bidi) != count {
		t.Fatalf("bidi has %d messages, want %d: %+v", len(got.Bidi), count, got.Bidi)
	}
}

func TestGRPCOverTLSWrongRoot(t *testing.T) {
	material := testcert.NewServerTLS(t, net.ParseIP("127.0.0.1"))
	target := startTLSEchoServer(t, material)
	other := testcert.NewServerTLS(t, net.ParseIP("127.0.0.1"))
	body, err := json.Marshal(map[string]any{
		"target":  target,
		"message": "secure",
		"tls":     true,
		"rootCA":  string(other.RootCA),
	})
	if err != nil {
		t.Fatal(err)
	}

	recorder, got := postGRPC(t, string(body))

	if recorder.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want %d", recorder.Code, http.StatusBadGateway)
	}
	if got.Code != codes.Unavailable.String() {
		t.Errorf("code = %q, want %q; error = %s", got.Code, codes.Unavailable.String(), got.Error)
	}
}

func TestGRPCInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
		status int
	}{
		{name: "method", method: http.MethodGet, body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, body: `{`, status: http.StatusBadRequest},
		{name: "missing target", method: http.MethodPost, body: `{"message":"hi"}`, status: http.StatusBadRequest},
		{name: "no port", method: http.MethodPost, body: `{"target":"grpcecho"}`, status: http.StatusBadRequest},
		{name: "no host", method: http.MethodPost, body: `{"target":":50051"}`, status: http.StatusBadRequest},
		{name: "non-numeric port", method: http.MethodPost, body: `{"target":"grpcecho:grpc"}`, status: http.StatusBadRequest},
		{name: "URL not host:port", method: http.MethodPost, body: `{"target":"http://grpcecho:50051"}`, status: http.StatusBadRequest},
		{name: "bidiMode not a mode", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiMode":"sideways","bidiCount":1}`, status: http.StatusBadRequest},
		{name: "bidiCount over the limit", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1001}`, status: http.StatusBadRequest},
		{name: "bidiCount negative", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":-1}`, status: http.StatusBadRequest},
		{name: "streams without bidiCount", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","streams":2}`, status: http.StatusBadRequest},
		{name: "streams over the limit", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"streams":17}`, status: http.StatusBadRequest},
		{name: "payloadBytes without bidiCount", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","payloadBytes":1}`, status: http.StatusBadRequest},
		{name: "payloadBytes over the limit", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"payloadBytes":2097153}`, status: http.StatusBadRequest},
		{name: "replyBytes over the limit", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"replyBytes":2097153}`, status: http.StatusBadRequest},
		{name: "afterCloseCount without bidiCount", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","afterCloseCount":1}`, status: http.StatusBadRequest},
		{name: "afterCloseCount over the limit", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"afterCloseCount":1001}`, status: http.StatusBadRequest},
		{name: "failAfter without bidiCount", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","failAfter":1,"failCode":"Internal"}`, status: http.StatusBadRequest},
		{name: "failAfter negative", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"failAfter":-1,"failCode":"Internal"}`, status: http.StatusBadRequest},
		{name: "failAfter without failCode", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"failAfter":1}`, status: http.StatusBadRequest},
		{name: "failCode without failAfter", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"failCode":"Internal"}`, status: http.StatusBadRequest},
		{name: "failCode not a code", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"failAfter":1,"failCode":"Teapot"}`, status: http.StatusBadRequest},
		{name: "failCode OK", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"failAfter":1,"failCode":"OK"}`, status: http.StatusBadRequest},
		{name: "cancelAfter without bidiCount", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","cancelAfter":1}`, status: http.StatusBadRequest},
		{name: "cancelAfter zero", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"cancelAfter":0}`, status: http.StatusBadRequest},
		{name: "cancelAfter beyond the responses", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":2,"afterCloseCount":1,"cancelAfter":4}`, status: http.StatusBadRequest},
		{name: "cancelAfter with failAfter", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":2,"cancelAfter":1,"failAfter":1,"failCode":"Internal"}`, status: http.StatusBadRequest},
		{name: "cancelAfter with several streams", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":2,"cancelAfter":1,"streams":2}`, status: http.StatusBadRequest},
		{name: "expectCode without bidiCount", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","expectCode":"OK"}`, status: http.StatusBadRequest},
		{name: "expectCode not a code", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"expectCode":"Nope"}`, status: http.StatusBadRequest},
		{name: "idleAfter without bidiCount", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","idleAfter":1,"idleMs":1}`, status: http.StatusBadRequest},
		{name: "idleAfter zero", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"idleAfter":0,"idleMs":1}`, status: http.StatusBadRequest},
		{name: "idleAfter without idleMs", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"idleAfter":1}`, status: http.StatusBadRequest},
		{name: "idleMs without idleAfter", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"idleMs":100}`, status: http.StatusBadRequest},
		{name: "idleMs negative", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"idleAfter":1,"idleMs":-1}`, status: http.StatusBadRequest},
		{name: "idleMs not below the default timeout", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"idleAfter":1,"idleMs":15000}`, status: http.StatusBadRequest},
		{name: "idleMs not below timeoutMs", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiCount":1,"idleAfter":1,"idleMs":1000,"timeoutMs":1000}`, status: http.StatusBadRequest},
		{name: "timeoutMs over the limit", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","timeoutMs":240001}`, status: http.StatusBadRequest},
		{name: "timeoutMs negative", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","timeoutMs":-1}`, status: http.StatusBadRequest},
		{name: "bidiMode without bidiCount", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","bidiMode":"lockstep"}`, status: http.StatusBadRequest},
		{name: "dot in message with payloadBytes", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"a.b","bidiCount":1,"payloadBytes":10}`, status: http.StatusBadRequest},
		{name: "dot in message with replyBytes", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"a.b","bidiCount":1,"replyBytes":10}`, status: http.StatusBadRequest},
		{name: "rootCA without tls", method: http.MethodPost, body: `{"target":"grpcecho:50051","message":"hi","rootCA":"unused"}`, status: http.StatusBadRequest},
	}

	handler := newHandler(http.DefaultClient)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "/grpc", strings.NewReader(test.body))
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d; body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}

func TestMux(t *testing.T) {
	var completed atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/fetch" {
			t.Errorf("path = %s, want /fetch", r.URL.Path)
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body) != muxBodySize {
			t.Errorf("len(body) = %d, want %d", len(body), muxBodySize)
		}
		completed.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload, err := json.Marshal(fetchRequest{URL: srv.URL + "/fetch"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/mux", strings.NewReader(string(payload)))
	newHandler(http.DefaultClient).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if got := int(completed.Load()); got != muxRequestCount {
		t.Errorf("completed = %d, want %d", got, muxRequestCount)
	}
}

func TestMuxNon200Fails(t *testing.T) {
	var seen atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		if int(seen.Add(1)) == muxRequestCount {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	payload, err := json.Marshal(fetchRequest{URL: srv.URL + "/fetch"})
	if err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/mux", strings.NewReader(string(payload)))
	newHandler(http.DefaultClient).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d; body = %s", recorder.Code, http.StatusInternalServerError, recorder.Body.String())
	}
}

func TestMuxInvalidRequests(t *testing.T) {
	tests := []struct {
		name   string
		method string
		body   string
		status int
	}{
		{name: "method", method: http.MethodGet, body: `{}`, status: http.StatusMethodNotAllowed},
		{name: "malformed JSON", method: http.MethodPost, body: `{`, status: http.StatusBadRequest},
		{name: "missing hostname", method: http.MethodPost, body: `{"url":"http:///path"}`, status: http.StatusBadRequest},
		{name: "https scheme", method: http.MethodPost, body: `{"url":"https://example.com/fetch"}`, status: http.StatusBadRequest},
	}

	handler := newHandler(http.DefaultClient)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(test.method, "/mux", strings.NewReader(test.body))
			handler.ServeHTTP(recorder, request)
			if recorder.Code != test.status {
				t.Errorf("status = %d, want %d; body = %s", recorder.Code, test.status, recorder.Body.String())
			}
		})
	}
}

func TestPost(t *testing.T) {
	headersReceived := make(chan struct{})
	var bodyBytes atomic.Int32
	srv, rootCA := startIPTLSServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if r.URL.Path != "/fetch" {
			t.Errorf("path = %s, want /fetch", r.URL.Path)
		}
		close(headersReceived)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		bodyBytes.Store(int32(len(body)))
		w.WriteHeader(http.StatusOK)
	}))

	mux := http.NewServeMux()
	mux.HandleFunc("/post", handlePost(50*time.Millisecond))

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- postHandler(t, mux, "/post", fetchRequest{
			URL:    srv.URL + "/fetch",
			RootCA: rootCA,
		})
	}()

	<-headersReceived
	if got := bodyBytes.Load(); got != 0 {
		t.Fatalf("body bytes immediately after headers = %d, want 0", got)
	}

	rec := <-done
	if rec.Code != http.StatusOK {
		t.Fatalf("/post status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
	if got := int(bodyBytes.Load()); got != muxBodySize {
		t.Errorf("body bytes after /post completed = %d, want %d", got, muxBodySize)
	}
}

func TestPostInvalidRequests(t *testing.T) {
	handler := newHandler(http.DefaultClient)

	t.Run("method not allowed", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/post", nil)
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
		}
	})
	t.Run("invalid rootCA", func(t *testing.T) {
		recorder := postHandler(t, handler, "/post", fetchRequest{
			URL:    "https://example.com/fetch",
			RootCA: "not-a-cert",
		})
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})
}

func TestSlowHeaders(t *testing.T) {
	headersCompleted := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		close(headersCompleted)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	}))
	defer srv.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/slow-headers", handleSlowHeaders(80*time.Millisecond))

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		done <- postHandler(t, mux, "/slow-headers", fetchRequest{
			URL: srv.URL + "/",
		})
	}()

	select {
	case <-headersCompleted:
		t.Fatal("headers completed before headerDelay elapsed")
	case <-time.After(20 * time.Millisecond):
	}

	<-headersCompleted
	rec := <-done
	if rec.Code != http.StatusOK {
		t.Fatalf("/slow-headers status = %d, want %d; body = %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

func TestSlowHeadersInvalidRequests(t *testing.T) {
	handler := newHandler(http.DefaultClient)

	t.Run("method not allowed", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/slow-headers", nil)
		handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusMethodNotAllowed {
			t.Errorf("status = %d, want %d", recorder.Code, http.StatusMethodNotAllowed)
		}
	})
	t.Run("https rejected", func(t *testing.T) {
		recorder := postHandler(t, handler, "/slow-headers", fetchRequest{
			URL: "https://example.net/",
		})
		if recorder.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want %d", recorder.Code, http.StatusBadRequest)
		}
	})
}
