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
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"go.ate.dev/substrate/internal/e2e"
)

// TestActorEgressGRPCOverTLS covers gRPC over TLS on the egress MITM leg. The
// gateway terminates the Actor's TLS, decrypts the HTTP/2 stream, and opens its
// own TLS connection to the origin, so the Actor must see the gateway's minted
// leaf certificate, not the origin's. The origin's root is passed to the test
// only so the assertion can prove the Actor did not see the origin's chain.
func TestActorEgressGRPCOverTLS(t *testing.T) {
	ctx := t.Context()
	target, address, rootCA := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name:       "grpcs-origin",
		ImportPath: "go.ate.dev/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"grpc"},
		Port:       50051,
		TLSPort:    8443,
		GRPCProbe:  true,
	}, true)
	actorName, router, ref := prepareProtocolActor(t, ctx, "grpcs")
	since := metav1.NewTime(time.Now().Add(-time.Minute))

	const (
		message     = "m"
		bidiCount   = 3
		afterClose  = 2
		echoedCount = bidiCount + afterClose
	)
	// The request dials the origin's service-DNS name, which its certificate
	// covers. The Actor's system roots trust the gateway's leaf certificates,
	// so no rootCA is passed.
	payload, err := json.Marshal(map[string]any{
		"target":          address,
		"tls":             true,
		"message":         message,
		"bidiMode":        "concurrent",
		"bidiCount":       bidiCount,
		"afterCloseCount": afterClose,
	})
	if err != nil {
		t.Fatalf("marshaling the gRPC over TLS request: %v", err)
	}

	status, body := postThroughEgressActor(t, ctx, router, ref, "/grpc", payload)
	if status != http.StatusOK {
		t.Fatalf("Actor gRPC over TLS egress to %s returned HTTP %d, want 200; body: %s", address, status, body)
	}
	var got grpcEchoResponse
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("decoding the Actor's gRPC over TLS response: %v; body: %s", err, body)
	}

	if got.Code != "OK" {
		t.Errorf("gRPC status = %q, want OK; error: %s", got.Code, got.Error)
	}
	if !got.TLS {
		t.Errorf("Actor reported TLS = false, want true")
	}

	// The server echoes each request with its index, then after the client
	// half-closes it sends the after-close responses, numbered on from the
	// last index it used.
	if len(got.Bidi) != echoedCount {
		t.Fatalf("EchoBidi returned %d responses, want %d: %+v", len(got.Bidi), echoedCount, got.Bidi)
	}
	for i := range bidiCount {
		want := fmt.Sprintf("%s-%d", message, i)
		if got.Bidi[i].Message != want || int(got.Bidi[i].Index) != i {
			t.Errorf("bidi response %d = %+v, want message %q index %d", i, got.Bidi[i], want, i)
		}
	}
	for j := range afterClose {
		i := bidiCount + j
		want := fmt.Sprintf("after-close-%d", j)
		if got.Bidi[i].Message != want || int(got.Bidi[i].Index) != i {
			t.Errorf("after-close response %d = %+v, want message %q index %d", j, got.Bidi[i], want, i)
		}
	}

	// The gateway's leaf is issued by the gateway's CA. If the Actor had seen
	// the origin certificate directly, its issuer would be the origin's CA
	// subject, so inequality shows the gateway intercepted the connection.
	// Both strings come from pkix.Name.String, so they compare directly.
	block, _ := pem.Decode([]byte(rootCA))
	if block == nil {
		t.Fatal("origin root CA is not PEM")
	}
	originRoot, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parsing origin root CA: %v", err)
	}
	if got.ServerCertIssuer == "" {
		t.Error("Actor reported no server certificate issuer over TLS")
	}
	if got.ServerCertIssuer == originRoot.Subject.String() {
		t.Errorf("Actor saw issuer %q, the origin's CA; want the gateway's minted leaf", got.ServerCertIssuer)
	}

	assertProtocolGateway(t, ctx, since, actorName, target.TLSAddress())
}
