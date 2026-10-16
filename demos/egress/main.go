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

// Command egress is a small HTTP service for demonstrating per-Actor egress
// policy. It accepts a URL, fetches it, and returns the upstream response, and
// on a second endpoint it makes gRPC calls and returns what came back.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	grpcstatus "google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/grpcecho"
	"go.ate.dev/substrate/internal/proto/grpcechopb"
)

const (
	listenAddress             = ":80"
	maxRequestBody            = 64 << 10
	maxResponseBody           = 1 << 20
	requestTimeout            = 15 * time.Second
	maxWebSocketMessages      = 16
	maxWebSocketMessageSize   = 64 << 10
	maxWebSocketHandshakeBody = 4 << 10
	muxRequestCount           = 250
	muxBodySize               = 64
	maxGRPCStreams            = 16
	maxGRPCBidiCount          = 1000
	maxGRPCTimeout            = 4 * time.Minute
	// How long to wait for the server to record how a canceled stream ended.
	grpcOutcomeWait         = 5 * time.Second
	grpcOutcomePollInterval = 100 * time.Millisecond
	// Keep connection around longer than ext_proc cache freshness (10s)
	postBodyDelay   = 15 * time.Second
	slowHeaderDelay = 5 * time.Second
)

type fetchRequest struct {
	URL    string `json:"url"`
	RootCA string `json:"rootCA,omitempty"`
	HTTP1  bool   `json:"http1,omitempty"`
}

type fetchResponse struct {
	StatusCode int    `json:"statusCode,omitempty"`
	Body       string `json:"body,omitempty"`
	ServerCert string `json:"serverCert,omitempty"`
	Protocol   string `json:"protocol,omitempty"`
	TLS        bool   `json:"tls"`
	Error      string `json:"error,omitempty"`
}

type websocketRequest struct {
	URL      string   `json:"url"`
	RootCA   string   `json:"rootCA,omitempty"`
	Messages []string `json:"messages"`
}

type websocketMessage struct {
	Type    int    `json:"type"`
	Message string `json:"message"`
}

type websocketResponse struct {
	StatusCode    int                `json:"statusCode,omitempty"`
	Protocol      string             `json:"protocol,omitempty"`
	HandshakeBody string             `json:"handshakeBody,omitempty"`
	TLS           bool               `json:"tls"`
	Messages      []websocketMessage `json:"messages"`
	Error         string             `json:"error,omitempty"`
}

// grpcRequest asks for one unary Echo against target, and additionally for a
// server-stream or bidirectional streams when the matching count is positive.
// The bidi fields shape those streams and are rejected unless bidiCount is set.
type grpcRequest struct {
	// Target is the gRPC server to dial, as host:port. The dial is cleartext
	// HTTP/2 unless TLS is set.
	Target  string `json:"target"`
	Message string `json:"message"`
	// TLS dials with TLS 1.2 or later, verifying the server against the system
	// roots.
	TLS bool `json:"tls,omitempty"`
	// RootCA is a PEM bundle that replaces the system roots for the TLS dial.
	// It is for tests and needs TLS.
	RootCA      string `json:"rootCA,omitempty"`
	StreamCount int32  `json:"streamCount,omitempty"`
	// BidiCount is how many messages each bidirectional stream sends.
	BidiCount int32 `json:"bidiCount,omitempty"`
	// BidiMode is "lockstep" (the default), sending each message after the
	// previous response, or "concurrent", sending them all back to back.
	BidiMode string `json:"bidiMode,omitempty"`
	// Streams is how many bidirectional streams to run over the one connection.
	Streams int32 `json:"streams,omitempty"`
	// PayloadBytes pads each sent message with '.' to this size.
	PayloadBytes int32 `json:"payloadBytes,omitempty"`
	// ReplyBytes pads each response message to this size.
	ReplyBytes int32 `json:"replyBytes,omitempty"`
	// AfterCloseCount is how many responses the server sends after the client
	// half-closes, which this request expects to receive.
	AfterCloseCount int32 `json:"afterCloseCount,omitempty"`
	// FailAfter ends the stream with FailCode after this many responses.
	FailAfter *int32 `json:"failAfter,omitempty"`
	// FailCode is the codes.Code name to fail with, such as "FailedPrecondition".
	FailCode string `json:"failCode,omitempty"`
	// CancelAfter cancels the client side of the stream after this many
	// responses, then records how the server saw the stream end.
	CancelAfter *int32 `json:"cancelAfter,omitempty"`
	// ExpectCode is the gRPC code the bidi phase should end with. The response
	// is 200 when it does and 502 when it does not. Defaults to "OK".
	ExpectCode string `json:"expectCode,omitempty"`
	// IdleAfter pauses the server for IdleMs after this many responses.
	IdleAfter *int32 `json:"idleAfter,omitempty"`
	// IdleMs is the pause IdleAfter asks for, in milliseconds.
	IdleMs int32 `json:"idleMs,omitempty"`
	// TimeoutMs overrides the request timeout, in milliseconds.
	TimeoutMs int32 `json:"timeoutMs,omitempty"`
}

type grpcResponse struct {
	// Message is what the unary Echo returned.
	Message string `json:"message"`
	// TLS reports whether the unary Echo ran over TLS.
	TLS bool `json:"tls"`
	// ServerCertIssuer is the issuer of the leaf certificate the server presented
	// over TLS. Empty when TLS was not used.
	ServerCertIssuer string `json:"serverCertIssuer,omitempty"`
	// Stream is what EchoStream returned, in the order it arrived. Absent when
	// the request did not ask for a stream.
	Stream []streamedMessage `json:"stream,omitempty"`
	// Bidi is what EchoBidi returned, in the order it arrived. Absent when the
	// request did not ask for one, or asked for several streams.
	Bidi []streamedMessage `json:"bidi,omitempty"`
	// BidiStreams holds what each of several EchoBidi streams returned, indexed
	// by stream. Present only when the request asked for more than one stream.
	BidiStreams [][]streamedMessage `json:"bidiStreams,omitempty"`
	// ServerOutcome is how the server recorded a canceled stream as ending,
	// such as "canceled". Empty unless the request canceled a stream.
	ServerOutcome string `json:"serverOutcome,omitempty"`
	// Code is the gRPC status of the last RPC attempted, as a string. It is the
	// one field an HTTP status cannot stand in for: a gRPC status travels in
	// trailers, after the response body, so a path that drops trailers or
	// downgrades the connection to HTTP/1.1 cannot produce this at all.
	Code  string `json:"code,omitempty"`
	Error string `json:"error,omitempty"`
}

type streamedMessage struct {
	Message string `json:"message"`
	Index   int32  `json:"index"`
	// Size is the length of the message as received, before any trimming of
	// its padding.
	Size int `json:"size"`
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	client := &http.Client{Timeout: requestTimeout}
	slog.Info("starting egress demo", "address", listenAddress)
	if err := http.ListenAndServe(listenAddress, newHandler(client)); err != nil {
		slog.Error("egress demo stopped", "error", err)
		os.Exit(1)
	}
}

func newHandler(client *http.Client) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, fetchResponse{Error: "method must be POST"})
			return
		}

		var input fetchRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err := decoder.Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
			return
		}
		if err := validateURL(input.URL); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: err.Error()})
			return
		}

		outbound, err := http.NewRequestWithContext(r.Context(), http.MethodGet, input.URL, nil)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid URL: %v", err)})
			return
		}
		if traceparent := r.Header.Get("traceparent"); traceparent != "" {
			outbound.Header.Set("traceparent", traceparent)
		}
		requestClient, cleanup, err := fetchClient(client, input)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("configuring request: %v", err)})
			return
		}
		defer cleanup()
		response, err := requestClient.Do(outbound)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}
		defer response.Body.Close()

		body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBody))
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("reading response: %v", err)})
			return
		}
		var serverCert string
		verifiedTLS := response.TLS != nil && len(response.TLS.VerifiedChains) > 0
		if response.TLS != nil && len(response.TLS.PeerCertificates) > 0 {
			serverCert = response.TLS.PeerCertificates[0].Issuer.String()
		}
		writeJSON(w, response.StatusCode, fetchResponse{
			StatusCode: response.StatusCode,
			Body:       string(body),
			ServerCert: serverCert,
			Protocol:   response.Proto,
			TLS:        verifiedTLS,
		})
	})
	mux.HandleFunc("/post", handlePost(postBodyDelay))
	mux.HandleFunc("/slow-headers", handleSlowHeaders(slowHeaderDelay))
	mux.HandleFunc("/mux", handleMux)
	mux.HandleFunc("/websocket", handleWebSocket)
	mux.HandleFunc("/grpc", handleGRPC)
	return mux
}

func handleSlowHeaders(headerDelay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, fetchResponse{Error: "method must be POST"})
			return
		}

		var input fetchRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err := decoder.Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
			return
		}
		if err := validateURL(input.URL); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: err.Error()})
			return
		}
		parsed, err := url.Parse(input.URL)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid URL: %v", err)})
			return
		}
		if parsed.Scheme != "http" {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: "URL scheme must be http"})
			return
		}

		targetAddr := parsed.Host
		if parsed.Port() == "" {
			targetAddr = net.JoinHostPort(parsed.Hostname(), "80")
		}

		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout+headerDelay)
		defer cancel()

		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "tcp", targetAddr)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}
		defer conn.Close()

		if deadline, ok := ctx.Deadline(); ok {
			if err := conn.SetDeadline(deadline); err != nil {
				writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
				return
			}
		}

		// Send request line and headers without the final "\r\n" that terminates the header block.
		reqHeader := fmt.Sprintf(
			"GET %s HTTP/1.1\r\nHost: %s\r\nConnection: close\r\n",
			parsed.RequestURI(), parsed.Host,
		)
		if _, err := io.WriteString(conn, reqHeader); err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}

		select {
		case <-time.After(headerDelay):
		case <-ctx.Done():
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", ctx.Err())})
			return
		}

		if _, err := io.WriteString(conn, "\r\n"); err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}

		resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodGet})
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("reading response: %v", err)})
			return
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("reading response: %v", err)})
			return
		}
		writeJSON(w, resp.StatusCode, fetchResponse{
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
			Protocol:   resp.Proto,
			TLS:        false,
		})
	}
}

func handlePost(bodyDelay time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSON(w, http.StatusMethodNotAllowed, fetchResponse{Error: "method must be POST"})
			return
		}

		var input fetchRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
		if err := decoder.Decode(&input); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
			return
		}
		if err := validateURL(input.URL); err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: err.Error()})
			return
		}
		parsed, err := url.Parse(input.URL)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid URL: %v", err)})
			return
		}

		var tlsCfg *tls.Config
		if parsed.Scheme == "https" {
			tlsCfg = &tls.Config{
				ServerName: parsed.Hostname(),
				MinVersion: tls.VersionTLS12,
			}
			if input.RootCA != "" {
				roots := x509.NewCertPool()
				if !roots.AppendCertsFromPEM([]byte(input.RootCA)) {
					writeJSON(w, http.StatusBadRequest, fetchResponse{Error: "rootCA does not contain a valid certificate"})
					return
				}
				tlsCfg.RootCAs = roots
			}
		}

		targetAddr := parsed.Host
		if parsed.Port() == "" {
			defaultPort := "80"
			if parsed.Scheme == "https" {
				defaultPort = "443"
			}
			targetAddr = net.JoinHostPort(parsed.Hostname(), defaultPort)
		}

		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout+bodyDelay)
		defer cancel()

		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "tcp", targetAddr)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}
		defer conn.Close()

		if deadline, ok := ctx.Deadline(); ok {
			if err := conn.SetDeadline(deadline); err != nil {
				writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
				return
			}
		}

		if tlsCfg != nil {
			tlsConn := tls.Client(conn, tlsCfg)
			if err := tlsConn.HandshakeContext(ctx); err != nil {
				writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
				return
			}
			conn = tlsConn
		}

		reqHeader := fmt.Sprintf(
			"POST %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
			parsed.RequestURI(), parsed.Host, muxBodySize,
		)
		if _, err := io.WriteString(conn, reqHeader); err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}

		select {
		case <-time.After(bodyDelay):
		case <-ctx.Done():
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", ctx.Err())})
			return
		}

		body := bytes.Repeat([]byte("a"), muxBodySize)
		if _, err := conn.Write(body); err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request failed: %v", err)})
			return
		}

		resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("reading response: %v", err)})
			return
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
		if err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("reading response: %v", err)})
			return
		}
		writeJSON(w, resp.StatusCode, fetchResponse{
			StatusCode: resp.StatusCode,
			Body:       string(respBody),
			Protocol:   resp.Proto,
			TLS:        tlsCfg != nil,
		})
	}
}

// This handler is for testing mutiplexing of actor `muxRequestCount` connections over H/2
// CONNECT tunnels between an atunnel and egress gateway. This handler:
// 1. opens `muxRequestCount` connections to the origin
// 2. sends POST request with headers, but not bodies on all of them.
// 3. once headers were sent, it sends bodies
// 4. waits for all 200 responses.
// Tests can then validate the expected number of H/2 connections on egress gateways.
func handleMux(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, fetchResponse{Error: "method must be POST"})
		return
	}

	var input fetchRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, fetchResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if err := validateURL(input.URL); err != nil {
		writeJSON(w, http.StatusBadRequest, fetchResponse{Error: err.Error()})
		return
	}
	parsed, err := url.Parse(input.URL)
	if err != nil || parsed.Scheme != "http" {
		writeJSON(w, http.StatusBadRequest, fetchResponse{Error: "URL scheme must be http"})
		return
	}

	targetAddr := parsed.Host
	if parsed.Port() == "" {
		targetAddr = net.JoinHostPort(parsed.Hostname(), "80")
	}
	requestURI := parsed.RequestURI()

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()

	var headersWg sync.WaitGroup
	headersWg.Add(muxRequestCount)
	allHeadersSent := make(chan struct{})
	go func() {
		headersWg.Wait()
		close(allHeadersSent)
	}()

	type muxOutcome struct {
		status int
		body   string
		err    error
	}
	outcomes := make([]muxOutcome, muxRequestCount)

	var doneWg sync.WaitGroup
	doneWg.Add(muxRequestCount)
	for i := range muxRequestCount {
		go func(idx int) {
			defer doneWg.Done()
			status, body, reqErr := sendMuxRequest(ctx, cancel, targetAddr, parsed.Host, requestURI, &headersWg, allHeadersSent)
			outcomes[idx] = muxOutcome{status: status, body: body, err: reqErr}
		}(i)
	}
	doneWg.Wait()

	for i, out := range outcomes {
		if out.err != nil && !errors.Is(out.err, context.Canceled) {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request %d failed: %v", i, out.err)})
			return
		}
		if out.err == nil && out.status != http.StatusOK {
			writeJSON(w, out.status, fetchResponse{
				StatusCode: out.status,
				Body:       out.body,
				Error:      fmt.Sprintf("request %d returned status %d, want 200", i, out.status),
			})
			return
		}
	}
	for i, out := range outcomes {
		if out.err != nil {
			writeJSON(w, http.StatusBadGateway, fetchResponse{Error: fmt.Sprintf("request %d failed: %v", i, out.err)})
			return
		}
	}

	writeJSON(w, http.StatusOK, fetchResponse{StatusCode: http.StatusOK})
}

func sendMuxRequest(ctx context.Context, cancel context.CancelFunc, targetAddr, host, requestURI string, headersWg *sync.WaitGroup, allHeadersSent <-chan struct{}) (int, string, error) {
	headerDone := false
	defer func() {
		if !headerDone {
			cancel()
			headersWg.Done()
		}
	}()

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", targetAddr)
	if err != nil {
		return 0, "", fmt.Errorf("dialing %s: %w", targetAddr, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		if err := conn.SetDeadline(deadline); err != nil {
			return 0, "", fmt.Errorf("setting deadline: %w", err)
		}
	}

	reqHeader := fmt.Sprintf(
		"POST %s HTTP/1.1\r\nHost: %s\r\nContent-Length: %d\r\nContent-Type: application/octet-stream\r\nConnection: close\r\n\r\n",
		requestURI, host, muxBodySize,
	)
	if _, err := io.WriteString(conn, reqHeader); err != nil {
		return 0, "", fmt.Errorf("writing request headers: %w", err)
	}

	headerDone = true
	headersWg.Done()

	select {
	case <-allHeadersSent:
	case <-ctx.Done():
		return 0, "", ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return 0, "", err
	}

	body := bytes.Repeat([]byte("a"), muxBodySize)
	if _, err := conn.Write(body); err != nil {
		return 0, "", fmt.Errorf("writing request body: %w", err)
	}

	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodPost})
	if err != nil {
		return 0, "", fmt.Errorf("reading response: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return 0, "", fmt.Errorf("reading response body: %w", err)
	}
	return resp.StatusCode, string(respBody), nil
}

// fetchClient constructs a per-request client only when the request asks for
// request-scoped TLS or protocol behavior. The default path retains the
// caller's client, including its transport and redirect policy.
func fetchClient(base *http.Client, input fetchRequest) (*http.Client, func(), error) {
	if input.RootCA == "" && !input.HTTP1 {
		return base, func() {}, nil
	}
	if base == nil {
		base = http.DefaultClient
	}
	// Start from the standard transport so request-scoped trust cannot inherit
	// an insecure TLS callback or proxy from an injected/base client.
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	if input.RootCA != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(input.RootCA)) {
			return nil, nil, errors.New("rootCA does not contain a valid certificate")
		}
		transport.TLSClientConfig.RootCAs = roots
	}
	if input.HTTP1 {
		protocols := new(http.Protocols)
		protocols.SetHTTP1(true)
		transport.Protocols = protocols
		transport.ForceAttemptHTTP2 = false
		transport.TLSNextProto = nil
	}
	requestClient := *base
	requestClient.Transport = transport
	requestClient.Timeout = requestTimeout
	requestClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &requestClient, transport.CloseIdleConnections, nil
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, websocketResponse{Messages: []websocketMessage{}, Error: "method must be POST"})
		return
	}

	var input websocketRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, websocketResponse{Messages: []websocketMessage{}, Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if err := validateWebSocketRequest(input); err != nil {
		writeJSON(w, http.StatusBadRequest, websocketResponse{Messages: []websocketMessage{}, Error: err.Error()})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	response, err := performWebSocket(ctx, input)
	if err != nil {
		response.Error = err.Error()
		writeJSON(w, http.StatusBadGateway, response)
		return
	}
	writeJSON(w, http.StatusOK, response)
}

func validateWebSocketRequest(input websocketRequest) error {
	parsed, err := url.Parse(input.URL)
	if err != nil {
		return fmt.Errorf("invalid WebSocket URL: %w", err)
	}
	if parsed.Scheme != "ws" && parsed.Scheme != "wss" {
		return errors.New("WebSocket URL scheme must be ws or wss")
	}
	if parsed.Hostname() == "" {
		return errors.New("WebSocket URL must include a hostname")
	}
	if len(input.Messages) == 0 {
		return errors.New("messages must contain at least one message")
	}
	if len(input.Messages) > maxWebSocketMessages {
		return fmt.Errorf("messages contains %d items, maximum is %d", len(input.Messages), maxWebSocketMessages)
	}
	for index, message := range input.Messages {
		if len(message) > maxWebSocketMessageSize {
			return fmt.Errorf("message %d is %d bytes, maximum is %d", index, len(message), maxWebSocketMessageSize)
		}
	}
	return nil
}

func performWebSocket(ctx context.Context, input websocketRequest) (websocketResponse, error) {
	dialer := websocket.Dialer{HandshakeTimeout: requestTimeout, Proxy: nil}
	if input.RootCA != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(input.RootCA)) {
			return websocketResponse{Messages: []websocketMessage{}}, errors.New("configuring TLS: rootCA does not contain a valid certificate")
		}
		dialer.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	}
	response := websocketResponse{Messages: []websocketMessage{}}
	conn, handshakeResponse, err := dialer.DialContext(ctx, input.URL, nil)
	if handshakeResponse != nil {
		response.StatusCode = handshakeResponse.StatusCode
		response.Protocol = handshakeResponse.Proto
		if handshakeResponse.Body != nil {
			body, _ := io.ReadAll(io.LimitReader(handshakeResponse.Body, maxWebSocketHandshakeBody))
			_ = handshakeResponse.Body.Close()
			response.HandshakeBody = string(body)
		}
	}
	if err != nil {
		return response, fmt.Errorf("WebSocket handshake failed: %w", err)
	}
	if tlsConn, ok := conn.UnderlyingConn().(*tls.Conn); ok {
		response.TLS = tlsConn.ConnectionState().VerifiedChains != nil
	}
	defer conn.Close()
	conn.SetReadLimit(maxWebSocketMessageSize)

	stopCancellation := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-stopCancellation:
		}
	}()
	defer close(stopCancellation)

	for index, message := range input.Messages {
		if err := conn.SetWriteDeadline(time.Now().Add(requestTimeout)); err != nil {
			return response, fmt.Errorf("setting write deadline for message %d: %w", index, err)
		}
		if err := conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
			return response, fmt.Errorf("writing message %d: %w", index, err)
		}
		if err := conn.SetReadDeadline(time.Now().Add(requestTimeout)); err != nil {
			return response, fmt.Errorf("setting read deadline for message %d: %w", index, err)
		}
		messageType, received, err := conn.ReadMessage()
		if err != nil {
			return response, fmt.Errorf("reading response to message %d: %w", index, err)
		}
		response.Messages = append(response.Messages, websocketMessage{Type: messageType, Message: string(received)})
	}
	return response, nil
}

func handleGRPC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, grpcResponse{Error: "method must be POST"})
		return
	}

	var input grpcRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err := decoder.Decode(&input); err != nil {
		writeJSON(w, http.StatusBadRequest, grpcResponse{Error: fmt.Sprintf("invalid JSON payload: %v", err)})
		return
	}
	if err := validateTarget(input.Target); err != nil {
		writeJSON(w, http.StatusBadRequest, grpcResponse{Error: err.Error()})
		return
	}
	if err := validateGRPCRequest(input); err != nil {
		writeJSON(w, http.StatusBadRequest, grpcResponse{Error: err.Error()})
		return
	}

	creds := insecure.NewCredentials()
	if input.TLS {
		// A nil pool makes the dial verify against the system roots.
		var pool *x509.CertPool
		if input.RootCA != "" {
			pool = x509.NewCertPool()
			if !pool.AppendCertsFromPEM([]byte(input.RootCA)) {
				writeJSON(w, http.StatusBadRequest, grpcResponse{Error: "rootCA does not contain a valid certificate"})
				return
			}
		}
		creds = credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool})
	}

	timeout := requestTimeout
	if input.TimeoutMs > 0 {
		timeout = time.Duration(input.TimeoutMs) * time.Millisecond
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	// Dialed per request and closed with it. The actor this runs inside is
	// checkpointed and restored, and an HTTP/2 connection opened before a
	// snapshot does not survive one: the peer is long gone by the time the
	// actor resumes, and every RPC on it would fail in a way that looks like a
	// broken gateway.
	conn, err := grpc.NewClient(input.Target, grpc.WithTransportCredentials(creds))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, grpcResponse{Error: fmt.Sprintf("dialing %s: %v", input.Target, err)})
		return
	}
	defer conn.Close()

	client := grpcechopb.NewEchoClient(conn)
	var echoPeer peer.Peer
	echoed, err := client.Echo(ctx, &grpcechopb.EchoRequest{Message: input.Message}, grpc.Peer(&echoPeer))
	if err != nil {
		writeGRPCFailure(w, "Echo", err)
		return
	}

	response := grpcResponse{Message: echoed.GetMessage(), Code: codes.OK.String()}
	if info, ok := echoPeer.AuthInfo.(credentials.TLSInfo); ok && len(info.State.PeerCertificates) > 0 {
		response.TLS = true
		response.ServerCertIssuer = info.State.PeerCertificates[0].Issuer.String()
	}
	if input.StreamCount > 0 {
		response.Stream, err = echoStream(ctx, client, input)
		if err != nil {
			writeGRPCFailure(w, "EchoStream", err)
			return
		}
	}
	if input.BidiCount == 0 {
		writeJSON(w, http.StatusOK, response)
		return
	}

	// The bidi phase reports its own code and returns partial results with it,
	// so the status answers whether that code is the one the caller expected.
	single, multi, outcome, err := runBidi(ctx, client, input)
	response.Bidi = single
	response.BidiStreams = multi
	response.ServerOutcome = outcome
	response.Code = codes.OK.String()
	if err != nil {
		response.Code = grpcstatus.Code(err).String()
		response.Error = "EchoBidi failed: " + err.Error()
	}
	expected := input.ExpectCode
	if expected == "" {
		expected = codes.OK.String()
	}
	status := http.StatusBadGateway
	if response.Code == expected {
		status = http.StatusOK
	}
	writeJSON(w, status, response)
}

// echoStream drains a server-stream into the response's Stream field.
func echoStream(ctx context.Context, client grpcechopb.EchoClient, input grpcRequest) ([]streamedMessage, error) {
	stream, err := client.EchoStream(ctx, &grpcechopb.EchoStreamRequest{Message: input.Message, Count: input.StreamCount})
	if err != nil {
		return nil, err
	}
	var out []streamedMessage
	for {
		received, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		out = append(out, streamedMessage{Message: received.GetMessage(), Index: received.GetIndex(), Size: len(received.GetMessage())})
	}
}

// runBidi runs the bidirectional phase: one stream, or input.Streams streams
// over the same connection. It returns the single stream's messages, or one
// entry per stream in multi. A canceled stream also reports how the server
// recorded its end.
func runBidi(ctx context.Context, client grpcechopb.EchoClient, input grpcRequest) (single []streamedMessage, multi [][]streamedMessage, outcome string, err error) {
	var streamID string
	if input.CancelAfter != nil {
		streamID = strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	bidiCtx := bidiMetadata(ctx, input, streamID)

	if input.Streams <= 1 {
		single, err = bidiStream(bidiCtx, client, input, input.Message)
	} else {
		multi = make([][]streamedMessage, input.Streams)
		errs := make([]error, input.Streams)
		var wg sync.WaitGroup
		for k := range input.Streams {
			wg.Add(1)
			go func() {
				defer wg.Done()
				multi[k], errs[k] = bidiStream(bidiCtx, client, input, fmt.Sprintf("%s-s%d", input.Message, k))
			}()
		}
		wg.Wait()
		for _, streamErr := range errs {
			if streamErr != nil {
				err = streamErr
				break
			}
		}
	}

	if streamID == "" {
		return single, multi, "", err
	}
	// The server records the outcome when its handler returns, which can be
	// after the client has seen the cancel, so poll for it.
	outcome, outcomeErr := awaitOutcome(ctx, client, streamID)
	if err == nil {
		err = outcomeErr
	}
	return single, multi, outcome, err
}

// bidiMetadata returns ctx with the server controls for input's bidi streams
// attached as outgoing metadata. streamID is empty unless the stream is to be
// recorded under that ID.
func bidiMetadata(ctx context.Context, input grpcRequest, streamID string) context.Context {
	var kv []string
	if input.FailAfter != nil {
		// The code is validated by validateGRPCRequest.
		code, _ := parseCode(input.FailCode)
		kv = append(kv, grpcecho.FailAfterKey, strconv.Itoa(int(*input.FailAfter)),
			grpcecho.FailCodeKey, strconv.Itoa(int(code)))
	}
	if input.IdleAfter != nil {
		kv = append(kv, grpcecho.IdleAfterKey, strconv.Itoa(int(*input.IdleAfter)),
			grpcecho.IdleMsKey, strconv.Itoa(int(input.IdleMs)))
	}
	if input.ReplyBytes > 0 {
		kv = append(kv, grpcecho.ReplyBytesKey, strconv.Itoa(int(input.ReplyBytes)))
	}
	if input.AfterCloseCount > 0 {
		kv = append(kv, grpcecho.AfterCloseCountKey, strconv.Itoa(int(input.AfterCloseCount)))
	}
	if streamID != "" {
		kv = append(kv, grpcecho.StreamIDKey, streamID)
	}
	if len(kv) == 0 {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, kv...)
}

// awaitOutcome asks the server, by unary Echo, how the stream with streamID
// ended. It returns an empty string, and no error, if the server has not
// recorded an outcome within grpcOutcomeWait.
func awaitOutcome(ctx context.Context, client grpcechopb.EchoClient, streamID string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, grpcOutcomeWait)
	defer cancel()
	for {
		lookup := metadata.AppendToOutgoingContext(ctx, grpcecho.OutcomeOfKey, streamID)
		response, err := client.Echo(lookup, &grpcechopb.EchoRequest{})
		if err != nil {
			if ctx.Err() != nil {
				return "", nil
			}
			return "", fmt.Errorf("looking up outcome of stream %s: %w", streamID, err)
		}
		if message := response.GetMessage(); message != "" {
			return message, nil
		}
		select {
		case <-ctx.Done():
			return "", nil
		case <-time.After(grpcOutcomePollInterval):
		}
	}
}

// bidiStream runs one bidirectional stream whose messages begin with prefix.
// It returns the messages received so far along with any error, so a failed
// stream still reports what it got.
func bidiStream(ctx context.Context, client grpcechopb.EchoClient, input grpcRequest, prefix string) ([]streamedMessage, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.EchoBidi(streamCtx)
	if err != nil {
		return nil, err
	}

	want := int(input.BidiCount + input.AfterCloseCount)
	var out []streamedMessage
	send := func(i int32) error {
		// A distinct message per send, so a path that replays or holds on to a
		// buffered frame shows up as a mismatch rather than as a pass.
		message := grpcecho.Pad(fmt.Sprintf("%s-%d", prefix, i), int(input.PayloadBytes))
		return stream.Send(&grpcechopb.EchoRequest{Message: message})
	}
	// receive appends the next response to out. Once CancelAfter responses are
	// in, it cancels the stream and returns the error the cancel produces.
	receive := func() error {
		response, err := stream.Recv()
		if err != nil {
			return err
		}
		out = append(out, bidiMessage(response, input))
		if len(out) > want {
			return fmt.Errorf("server sent more responses than the %d requested", want)
		}
		if input.CancelAfter != nil && len(out) == int(*input.CancelAfter) {
			cancel()
			_, err := stream.Recv()
			return err
		}
		return nil
	}

	if input.BidiMode == "concurrent" {
		err := bidiConcurrent(stream, cancel, send, receive, input.BidiCount)
		return out, err
	}

	// Lockstep: each message goes out only after the previous response is in.
	for i := range input.BidiCount {
		if err := send(i); err != nil {
			// grpc-go reports a broken stream from Send as io.EOF and puts the
			// real status on Recv, which is the code worth reporting.
			if _, recvErr := stream.Recv(); recvErr != nil {
				return out, recvErr
			}
			return out, err
		}
		if err := receive(); err != nil {
			return out, err
		}
	}

	// Half-close the request direction while the response direction is still
	// open, then drain it. A path that reads a one-directional END_STREAM as a
	// teardown fails here and nowhere else.
	if err := stream.CloseSend(); err != nil {
		return out, err
	}
	for {
		err := receive()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
	}
}

// bidiConcurrent sends every message back to back while the caller receives.
// It waits for the sender before returning, so no goroutine outlives the
// stream.
func bidiConcurrent(stream grpc.BidiStreamingClient[grpcechopb.EchoRequest, grpcechopb.EchoResponse], cancel context.CancelFunc, send func(int32) error, receive func() error, bidiCount int32) error {
	sent := make(chan error, 1)
	go func() {
		for i := range bidiCount {
			if err := send(i); err != nil {
				sent <- err
				return
			}
		}
		sent <- stream.CloseSend()
	}()

	var err error
	for {
		if err = receive(); err != nil {
			break
		}
	}
	if errors.Is(err, io.EOF) {
		err = nil
	}
	if err != nil {
		// Unblock a sender that is waiting on flow control.
		cancel()
	}
	if sendErr := <-sent; err == nil {
		err = sendErr
	}
	return err
}

// bidiMessage records a response. The padding is trimmed when it was asked
// for, so the message reads as the one that was sent.
func bidiMessage(response *grpcechopb.EchoResponse, input grpcRequest) streamedMessage {
	message := response.GetMessage()
	size := len(message)
	if input.PayloadBytes > 0 || input.ReplyBytes > 0 {
		message = strings.TrimRight(message, ".")
	}
	return streamedMessage{Message: message, Index: response.GetIndex(), Size: size}
}

// writeGRPCFailure reports a failed RPC. The HTTP status is 502 so that a
// caller polling through the ingress router keeps retrying -- the origin may
// simply not be up yet -- while the gRPC code travels in the body, where an
// HTTP status cannot flatten it into a generic "bad gateway".
func writeGRPCFailure(w http.ResponseWriter, rpc string, err error) {
	writeJSON(w, http.StatusBadGateway, grpcResponse{
		Code:  grpcstatus.Code(err).String(),
		Error: fmt.Sprintf("%s failed: %v", rpc, err),
	})
}

func validateURL(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("URL scheme must be http or https")
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("URL must include a hostname")
	}
	return nil
}

// validateTarget checks that raw is the host:port a gRPC dial needs. Unlike a
// URL there is no scheme to reject, so a caller that passes one -- or passes a
// bare hostname and lets the dial default the port -- finds out here rather
// than in a connection error from somewhere along the egress path.
func validateTarget(raw string) error {
	host, port, err := net.SplitHostPort(raw)
	if err != nil {
		return fmt.Errorf("target must be host:port: %w", err)
	}
	if host == "" {
		return fmt.Errorf("target must include a host")
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return fmt.Errorf("target port must be a number between 1 and 65535, got %q", port)
	}
	return nil
}

// validateGRPCRequest checks the bidi and timeout knobs. Each rule fails the
// request with 400 rather than falling back to a default, so a caller that
// asks for something the endpoint cannot do sees why.
func validateGRPCRequest(input grpcRequest) error {
	if input.RootCA != "" && !input.TLS {
		return errors.New("rootCA requires tls")
	}
	switch input.BidiMode {
	case "", "lockstep", "concurrent":
	default:
		return fmt.Errorf("bidiMode must be lockstep or concurrent, got %q", input.BidiMode)
	}
	if input.BidiCount < 0 || input.BidiCount > maxGRPCBidiCount {
		return fmt.Errorf("bidiCount must be between 0 and %d, got %d", maxGRPCBidiCount, input.BidiCount)
	}

	bidiOnly := []struct {
		name string
		set  bool
	}{
		{"bidiMode", input.BidiMode != ""},
		{"streams", input.Streams != 0},
		{"payloadBytes", input.PayloadBytes != 0},
		{"replyBytes", input.ReplyBytes != 0},
		{"afterCloseCount", input.AfterCloseCount != 0},
		{"failAfter", input.FailAfter != nil},
		{"cancelAfter", input.CancelAfter != nil},
		{"expectCode", input.ExpectCode != ""},
		{"idleAfter", input.IdleAfter != nil},
	}
	for _, field := range bidiOnly {
		if field.set && input.BidiCount == 0 {
			return fmt.Errorf("%s requires bidiCount", field.name)
		}
	}

	if input.Streams < 0 || input.Streams > maxGRPCStreams {
		return fmt.Errorf("streams must be between 0 and %d, got %d", maxGRPCStreams, input.Streams)
	}
	if input.PayloadBytes < 0 || input.PayloadBytes > grpcecho.MaxPayloadBytes {
		return fmt.Errorf("payloadBytes must be between 0 and %d, got %d", grpcecho.MaxPayloadBytes, input.PayloadBytes)
	}
	if input.ReplyBytes < 0 || input.ReplyBytes > grpcecho.MaxPayloadBytes {
		return fmt.Errorf("replyBytes must be between 0 and %d, got %d", grpcecho.MaxPayloadBytes, input.ReplyBytes)
	}
	if input.AfterCloseCount < 0 || input.AfterCloseCount > grpcecho.MaxStreamCount {
		return fmt.Errorf("afterCloseCount must be between 0 and %d, got %d", grpcecho.MaxStreamCount, input.AfterCloseCount)
	}
	if input.TimeoutMs < 0 || time.Duration(input.TimeoutMs)*time.Millisecond > maxGRPCTimeout {
		return fmt.Errorf("timeoutMs must be between 0 and %d, got %d", maxGRPCTimeout.Milliseconds(), input.TimeoutMs)
	}
	timeout := requestTimeout
	if input.TimeoutMs > 0 {
		timeout = time.Duration(input.TimeoutMs) * time.Millisecond
	}

	if (input.FailAfter != nil) != (input.FailCode != "") {
		return errors.New("failAfter and failCode must be given together")
	}
	if input.FailAfter != nil {
		if *input.FailAfter < 0 {
			return fmt.Errorf("failAfter must be at least 0, got %d", *input.FailAfter)
		}
		code, ok := parseCode(input.FailCode)
		if !ok {
			return fmt.Errorf("failCode %q is not a gRPC code", input.FailCode)
		}
		if code == codes.OK {
			return errors.New("failCode must not be OK")
		}
	}

	if input.CancelAfter != nil {
		if limit := input.BidiCount + input.AfterCloseCount; *input.CancelAfter < 1 || *input.CancelAfter > limit {
			return fmt.Errorf("cancelAfter must be between 1 and %d, got %d", limit, *input.CancelAfter)
		}
		if input.FailAfter != nil {
			return errors.New("cancelAfter and failAfter are mutually exclusive")
		}
		if input.Streams > 1 {
			return errors.New("cancelAfter requires streams of at most 1")
		}
	}

	if input.IdleAfter != nil || input.IdleMs != 0 {
		if (input.IdleAfter != nil) != (input.IdleMs != 0) {
			return errors.New("idleAfter and idleMs must be given together")
		}
		if *input.IdleAfter < 1 {
			return fmt.Errorf("idleAfter must be at least 1, got %d", *input.IdleAfter)
		}
		if input.IdleMs < 1 {
			return fmt.Errorf("idleMs must be at least 1, got %d", input.IdleMs)
		}
		if time.Duration(input.IdleMs)*time.Millisecond >= timeout {
			return fmt.Errorf("idleMs must be below the %v timeout, got %d", timeout, input.IdleMs)
		}
	}

	if input.ExpectCode != "" {
		if _, ok := parseCode(input.ExpectCode); !ok {
			return fmt.Errorf("expectCode %q is not a gRPC code", input.ExpectCode)
		}
	}

	if (input.PayloadBytes > 0 || input.ReplyBytes > 0) && strings.Contains(input.Message, ".") {
		return errors.New("message must not contain '.' when payloadBytes or replyBytes is set, because padding is trimmed")
	}
	return nil
}

// parseCode returns the gRPC code whose String form is name.
func parseCode(name string) (codes.Code, bool) {
	for code := codes.Code(0); code <= codes.Code(16); code++ {
		if code.String() == name {
			return code, true
		}
	}
	return codes.Code(0), false
}

func writeJSON(w http.ResponseWriter, statusCode int, response any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(response)
}
