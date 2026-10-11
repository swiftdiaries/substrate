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

//go:build envoy_integration

package router

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/atenet/internal/router/extproc"
	"github.com/agent-substrate/substrate/internal/atunnel"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
	"sigs.k8s.io/yaml"
)

// TestEgressPoolSeparatesPolicies runs the production egress listener and
// dynamic policy module in Envoy. Only the outer H2 setting and test-only
// secrets, routes, and ext_proc endpoint are adapted from the shipped manifest.
func TestEgressPoolSeparatesPolicies(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	grpcListener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer grpcListener.Close()
	processor := &policyProcessor{}
	processor.SetPolicy("revoked.test", "control.test")
	grpcServer := grpc.NewServer()
	extprocv3.RegisterExternalProcessorServer(grpcServer, processor)
	go func() { _ = grpcServer.Serve(grpcListener) }()
	t.Cleanup(grpcServer.Stop)

	certPEM, keyPEM := integrationCertificate(t)
	config := egressPoolTestConfig(t, grpcListener.Addr().(*net.TCPAddr).Port, certPEM, keyPEM)
	configPath := filepath.Join(t.TempDir(), "envoy.yaml")
	if err := os.WriteFile(configPath, config, 0o600); err != nil {
		t.Fatal(err)
	}

	image := os.Getenv("SUBSTRATE_ENVOY_TEST_IMAGE")
	if image == "" {
		image = "substrate-factory-key-envoy:local"
	}
	const outerPort = "1443/tcp"
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image,
			Cmd:   []string{"-c", "/tmp/envoy.yaml", "--log-level", "error"},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      configPath,
				ContainerFilePath: "/tmp/envoy.yaml",
				FileMode:          0o444,
			}},
			ExposedPorts:    []string{outerPort},
			HostAccessPorts: []int{grpcListener.Addr().(*net.TCPAddr).Port},
			WaitingFor:      wait.ForListeningPort(outerPort).WithStartupTimeout(30 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start Envoy container using %s: %v", image, err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		if err := testcontainers.TerminateContainer(container, testcontainers.StopContext(stopCtx)); err != nil {
			t.Errorf("stop Envoy container: %v", err)
		}
	})

	endpoint, err := container.PortEndpoint(ctx, outerPort, "")
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		t.Fatal(err)
	}
	trustPath := filepath.Join(t.TempDir(), "gateway-ca.pem")
	if err := os.WriteFile(trustPath, certPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	var downstreamConnections atomic.Int32
	client, err := atunnel.NewClient(atunnel.ClientConfig{
		GatewayAddress:       endpoint,
		ServerName:           "gateway.test",
		GetClientCertificate: func(*tls.CertificateRequestInfo) (*tls.Certificate, error) { return &cert, nil },
		TrustBundlePath:      trustPath,
	}, atunnel.WithDialer(func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
		if err == nil {
			downstreamConnections.Add(1)
		}
		return conn, err
	}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Errorf("close atunnel client: %v", err)
		}
	})
	const authority = "192.0.2.1:8443"
	first, connected, handshook, err := openEgressTunnelResult(ctx, client, authority, "revoked.test")
	if err != nil || !connected || !handshook {
		logs, logErr := container.Logs(ctx)
		if logErr == nil {
			body, _ := io.ReadAll(logs)
			_ = logs.Close()
			t.Logf("Envoy logs:\n%s", body)
		}
		t.Fatalf("open first CONNECT / inner TLS: %v", err)
	}
	defer first.Close()

	processor.SetPolicy("control.test")
	second, secondConnected, secondHandshook, err := openEgressTunnelResult(ctx, client, authority, "revoked.test")
	if second != nil {
		_ = second.Close()
	}
	connectStage := "CONNECT returned 200 and inner TLS completed"
	if err != nil {
		var rejected *atunnel.ConnectRejectedError
		if errors.As(err, &rejected) {
			connectStage = fmt.Sprintf("CONNECT rejected with status %d", rejected.StatusCode)
		} else if secondConnected {
			connectStage = fmt.Sprintf("CONNECT returned 200; inner TLS failed: %v", err)
		} else {
			connectStage = fmt.Sprintf("CONNECT failed before a 2xx response: %v", err)
		}
	}
	events := processor.Events()
	connectionCount := downstreamConnections.Load()
	t.Logf("second SNI=revoked.test result=%s (connectAccepted=%t innerTLSCompleted=%t); downstream TLS connections=%d; ext_proc events=%+v", connectStage, secondConnected, secondHandshook, connectionCount, events)
	if connectionCount != 1 {
		t.Errorf("atunnel opened %d downstream TLS connections before denial, want one shared H2 connection", connectionCount)
	}
	if len(events) != 2 {
		t.Errorf("ext_proc observed %d CONNECT requests before the denial check, want two", len(events))
	} else {
		if events[0].Revision != 1 || events[1].Revision != 2 {
			t.Errorf("ext_proc policy revisions before denial = [%d %d], want [1 2]", events[0].Revision, events[1].Revision)
		}
		for i, event := range events {
			if event.Authority != authority {
				t.Errorf("ext_proc request %d authority = %q, want %q", i+1, event.Authority, authority)
			}
		}
		if !equalStrings(events[0].Allowed, []string{"revoked.test", "control.test"}) {
			t.Errorf("first ext_proc payload allowed = %v, want [revoked.test control.test]", events[0].Allowed)
		}
		if !equalStrings(events[1].Allowed, []string{"control.test"}) {
			t.Errorf("second ext_proc payload allowed = %v, want [control.test]", events[1].Allowed)
		}
	}
	var innerErr *innerTLSHandshakeError
	if err == nil || !secondConnected || secondHandshook || !errors.As(err, &innerErr) {
		t.Errorf("second CONNECT should succeed at the outer layer and fail during inner TLS for revoked.test; got connected=%t handshook=%t err=%v", secondConnected, secondHandshook, err)
	}

	// The policy update applies to new tunnels, while the already-open tunnel
	// and a new control tunnel continue to work over the same H2 socket.
	assertOriginResponse(t, first)
	control := openEgressTunnel(t, ctx, client, authority, "control.test")
	assertOriginResponse(t, control)
	_ = first.Close()
	_ = control.Close()
	accessLogs := readEgressAccessLogs(t, ctx, container)
	if len(accessLogs) == 0 {
		t.Errorf("Envoy emitted no CONNECT access log with the negotiated protocol and connection ID")
	} else {
		var connectionID string
		for i, line := range accessLogs {
			id := accessLogValue(line, "connection_id")
			if id == "" || accessLogValue(line, "protocol") != "HTTP/2" {
				t.Errorf("Envoy CONNECT access log %d does not identify an H2 connection: %s", i+1, line)
			}
			if i == 0 {
				connectionID = id
			} else if id != connectionID {
				t.Errorf("Envoy CONNECT access log %d connection id=%s, want shared id=%s", i+1, id, connectionID)
			}
		}
		t.Logf("atunnel TCP dials=%d; all %d ext_proc CONNECTs used Envoy H2 connection id=%s", downstreamConnections.Load(), len(events), connectionID)
	}

	if got := downstreamConnections.Load(); got != 1 {
		t.Fatalf("Envoy received %d downstream TLS connections, want one shared H2 connection", got)
	}
	if got := processor.RequestCount(); got != 3 {
		t.Fatalf("ext_proc received %d CONNECT requests, want three on the shared H2 connection", got)
	}
	for _, got := range processor.Authorities() {
		if got != authority {
			t.Fatalf("ext_proc saw CONNECT authority %q, want the same authority %q for every stream", got, authority)
		}
	}
}

type policyProcessor struct {
	extprocv3.UnimplementedExternalProcessorServer

	mu     sync.Mutex
	state  processorPolicy
	events []processorEvent
}

type processorPolicy struct {
	revision uint64
	allowed  []string
	metadata *structpb.Struct
}

type processorEvent struct {
	Authority string
	Revision  uint64
	Allowed   []string
}

func (p *policyProcessor) SetPolicy(allowed ...string) {
	metadata := policyMetadata(allowed...)
	p.mu.Lock()
	p.state.revision++
	p.state.allowed = append([]string(nil), allowed...)
	p.state.metadata = metadata
	p.mu.Unlock()
}

func (p *policyProcessor) RequestCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.events)
}

func (p *policyProcessor) Authorities() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	authorities := make([]string, len(p.events))
	for i, event := range p.events {
		authorities[i] = event.Authority
	}
	return authorities
}

func (p *policyProcessor) Events() []processorEvent {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := make([]processorEvent, len(p.events))
	for i, event := range p.events {
		events[i] = processorEvent{Authority: event.Authority, Revision: event.Revision, Allowed: append([]string(nil), event.Allowed...)}
	}
	return events
}

func (p *policyProcessor) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	for {
		request, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		if headers := request.GetRequestHeaders(); headers != nil {
			var authority string
			for _, header := range headers.GetHeaders().GetHeaders() {
				if header.GetKey() == ":authority" {
					authority = string(header.GetRawValue())
				}
			}
			p.mu.Lock()
			state := p.state
			p.events = append(p.events, processorEvent{Authority: authority, Revision: state.revision, Allowed: append([]string(nil), state.allowed...)})
			p.mu.Unlock()

			response := &extprocv3.ProcessingResponse{
				Response: &extprocv3.ProcessingResponse_RequestHeaders{
					RequestHeaders: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}},
				},
				DynamicMetadata: state.metadata,
			}
			if err := stream.Send(response); err != nil {
				return err
			}
		}
	}
}

func policyMetadata(allowed ...string) *structpb.Struct {
	rules := make([]any, 0, len(allowed))
	for _, hostname := range allowed {
		rules = append(rules, map[string]any{"pattern": hostname, "mode": "passthrough"})
	}
	policy, _ := structpb.NewStruct(map[string]any{"rules": rules})
	metadata, _ := structpb.NewStruct(map[string]any{
		"dev.ate.policy.egress": policy.AsMap(),
		"dev.ate.egress":        map[string]any{"dialed_port": "8443"},
	})
	return metadata
}

func openEgressTunnel(t *testing.T, ctx context.Context, client *atunnel.Client, authority, serverName string) *tls.Conn {
	t.Helper()
	tunnel, connected, handshook, err := openEgressTunnelResult(ctx, client, authority, serverName)
	if err != nil || !connected || !handshook {
		t.Fatalf("open CONNECT stream for SNI %s: %v", serverName, err)
	}
	return tunnel
}

func openEgressTunnelResult(ctx context.Context, client *atunnel.Client, authority, serverName string) (*tls.Conn, bool, bool, error) {
	tunnel, err := client.DialContext(ctx, authority)
	if err != nil {
		return nil, false, false, fmt.Errorf("CONNECT %s: %w", authority, err)
	}
	conn := tls.Client(tunnel, &tls.Config{ServerName: serverName, InsecureSkipVerify: true}) // test origin uses an inline self-signed certificate
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := conn.HandshakeContext(ctx); err != nil {
		_ = tunnel.Close()
		return nil, true, false, &innerTLSHandshakeError{serverName: serverName, err: err}
	}
	return conn, true, true, nil
}

type innerTLSHandshakeError struct {
	serverName string
	err        error
}

func (e *innerTLSHandshakeError) Error() string {
	return fmt.Sprintf("inner TLS handshake for %q: %v", e.serverName, e.err)
}

func (e *innerTLSHandshakeError) Unwrap() error { return e.err }

func equalStrings(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func readEgressAccessLogs(t *testing.T, ctx context.Context, container testcontainers.Container) []string {
	t.Helper()
	logs, err := container.Logs(ctx)
	if err != nil {
		t.Errorf("read Envoy access logs: %v", err)
		return nil
	}
	defer logs.Close()
	body, err := io.ReadAll(logs)
	if err != nil {
		t.Errorf("read Envoy access log output: %v", err)
		return nil
	}
	var accessLogs []string
	for _, line := range strings.Split(string(body), "\n") {
		if strings.Contains(line, "[egress]") {
			t.Logf("Envoy %s", line)
			accessLogs = append(accessLogs, line)
		}
	}
	return accessLogs
}

func accessLogValue(line, key string) string {
	for _, field := range strings.Fields(line) {
		name, value, ok := strings.Cut(field, "=")
		if ok && name == key {
			return value
		}
	}
	return ""
}

func assertOriginResponse(t *testing.T, tunnel *tls.Conn) {
	t.Helper()
	if _, err := io.WriteString(tunnel, "GET / HTTP/1.1\r\nHost: origin.test\r\nConnection: keep-alive\r\n\r\n"); err != nil {
		t.Fatalf("send request through existing tunnel: %v", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(tunnel), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read response through existing tunnel: %v", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read origin response body: %v", err)
	}
	if response.StatusCode != http.StatusOK || string(body) != "ok" {
		t.Fatalf("origin response = %d %q, want 200 %q", response.StatusCode, body, "ok")
	}
}

func egressPoolTestConfig(t *testing.T, extProcPort int, certPEM, keyPEM []byte) []byte {
	t.Helper()
	tree := bootstrapTree(t, egressManifest)
	resources := child(tree, "static_resources")

	listeners := listeners(tree)
	outer := byName(listeners, "egress")
	outer["address"].(node)["socket_address"].(node)["port_value"] = 1443
	chain := byName(list(outer, "filter_chains"), extproc.EgressFilterChainName)
	tlsConfig := child(child(chain, "transport_socket"), "typed_config")
	tlsConfig["require_client_certificate"] = true
	commonTLS := child(tlsConfig, "common_tls_context")
	delete(commonTLS, "tls_certificate_sds_secret_configs")
	delete(commonTLS, "validation_context")
	commonTLS["tls_certificates"] = []any{node{
		"certificate_chain": node{"inline_string": string(certPEM)},
		"private_key":       node{"inline_string": string(keyPEM)},
	}}
	commonTLS["validation_context"] = node{"trusted_ca": node{"inline_string": string(certPEM)}}
	commonTLS["alpn_protocols"] = []any{"h2"}
	outerHCM := hcm(chain)
	outerHCM["codec_type"] = "HTTP2"
	outerHCM["http2_protocol_options"] = node{}
	for _, accessLog := range list(outerHCM, "access_log") {
		format := child(child(accessLog, "typed_config"), "log_format")
		text := child(format, "text_format_source")
		text["inline_string"] = strings.TrimSuffix(str(text, "inline_string"), "\n") + " connection_id=%CONNECTION_ID% protocol=%PROTOCOL%\n"
	}

	inner := byName(listeners, "mitm_listener")
	chains := list(inner, "filter_chains")
	passthrough := byName(chains, "egress_passthrough")
	passthroughFilters := list(passthrough, "filters")
	passthrough["filters"] = []any{passthroughFilters[1]}
	child(inner, "filter_chain_matcher")["matcher_tree"].(node)["exact_match_map"].(node)["map"] = node{
		"passthrough": node{"action": node{
			"name": "egress_passthrough",
			"typed_config": node{
				"@type": "type.googleapis.com/google.protobuf.StringValue",
				"value": "egress_passthrough",
			},
		}},
	}
	inner["filter_chains"] = []any{passthrough}

	origin := testOriginListener(certPEM, keyPEM)
	resources["listeners"] = []any{outer, inner, origin}

	var selected []node
	for _, c := range clusters(tree) {
		switch str(c, "name") {
		case "mitm_internal", "ext_proc_server":
			selected = append(selected, c)
		case "egress_forward_proxy_passthrough":
			selected = append(selected, testOriginCluster())
		}
	}
	for _, c := range selected {
		if str(c, "name") == "ext_proc_server" {
			c["type"] = "STRICT_DNS"
			assignment := child(c, "load_assignment")
			lbEndpoints := list(list(assignment, "endpoints")[0], "lb_endpoints")
			address := child(child(lbEndpoints[0], "endpoint"), "address")
			socket := child(address, "socket_address")
			socket["address"] = testcontainers.HostInternal
			socket["port_value"] = extProcPort
		}
	}
	resources["clusters"] = selected

	encoded, err := yaml.Marshal(tree)
	if err != nil {
		t.Fatalf("marshal adapted production Envoy config: %v", err)
	}
	return encoded
}

func testOriginCluster() node {
	return node{
		"name":            "egress_forward_proxy_passthrough",
		"type":            "STATIC",
		"connect_timeout": "5s",
		"lb_policy":       "ROUND_ROBIN",
		"load_assignment": node{
			"cluster_name": "egress_forward_proxy_passthrough",
			"endpoints":    []any{node{"lb_endpoints": []any{node{"endpoint": node{"address": node{"socket_address": node{"address": "127.0.0.1", "port_value": 8443}}}}}}},
		},
	}
}

func testOriginListener(certPEM, keyPEM []byte) node {
	return node{
		"name":    "test_origin",
		"address": node{"socket_address": node{"address": "127.0.0.1", "port_value": 8443}},
		"filter_chains": []any{node{
			"transport_socket": node{
				"name": "envoy.transport_sockets.tls",
				"typed_config": node{
					"@type": "type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.DownstreamTlsContext",
					"common_tls_context": node{"tls_certificates": []any{node{
						"certificate_chain": node{"inline_string": string(certPEM)},
						"private_key":       node{"inline_string": string(keyPEM)},
					}}},
				},
			},
			"filters": []any{node{
				"name": "envoy.filters.network.http_connection_manager",
				"typed_config": node{
					"@type":       "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
					"stat_prefix": "test_origin",
					"route_config": node{
						"name": "test_origin",
						"virtual_hosts": []any{node{
							"name":    "origin",
							"domains": []any{"*"},
							"routes": []any{node{
								"match":           node{"prefix": "/"},
								"direct_response": node{"status": 200, "body": node{"inline_string": "ok"}},
							}},
						}},
					},
					"http_filters": []any{node{
						"name":         "envoy.filters.http.router",
						"typed_config": node{"@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router"},
					}},
				},
			}},
		}},
	}
}

func integrationCertificate(t *testing.T) ([]byte, []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber:          serial,
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		DNSNames:              []string{"gateway.test", "origin.test", "revoked.test", "control.test"},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, &x509.Certificate{SerialNumber: serial, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), DNSNames: []string{"gateway.test", "origin.test", "revoked.test", "control.test"}, IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}
