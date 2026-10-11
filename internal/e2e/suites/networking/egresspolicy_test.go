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
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The EgressPolicy half of egress: the other TestActorEgress* tests give their
// actors an allow-everything policy and prove traffic flows; these give theirs
// a narrow one and prove what does not. A request the gateway can read is
// decided per request on its Host, by the http rules in the clear and the
// https rules once decrypted.

// notTransient stops the retry loop on anything but the 503 a request sees
// while the actor's route is still propagating; a denial is a final answer.
func notTransient(status int, _ []byte) bool { return status != http.StatusServiceUnavailable }

// reached stops the retry loop only on success. A lane expecting the fetch to
// work sees more transients than the 503 above (the minted leaf fails
// verification until kubelet has propagated the CA pool, public origins
// hiccup), all of them 502s the actor cannot tell from a denial.
func reached(status int, _ []byte) bool { return status == http.StatusOK }

// TestActorEgressPolicyDeniesUnlistedHost: the policy names one hostname, so
// the origin is reachable by that name and refused by its address, which is
// the same server.
func TestActorEgressPolicyDeniesUnlistedHost(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	origin := egressHTTPTarget()
	target := e2e.DeployServerPod(t, ctx, origin)
	allowed := fmt.Sprintf("%s.%s.svc.cluster.local", origin.Name, target.Namespace)

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-policy", e2e.EgressFixture(), e2e.EgressAllowHTTP(allowed))
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	url := "http://" + allowed + "/healthz"
	status, body := fetchThroughEgressActor(t, ctx, router, actorRef, url)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host %s returned HTTP %d, want 200; body: %s", url, status, body)
	}

	url = fmt.Sprintf("http://%s/healthz", target.Address())
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of the same origin by address %s returned HTTP %d, want an egress-policy denial; body: %s", url, status, body)
	}
	t.Logf("fetch by address was denied as expected: %s", body)
}

// TestActorEgressPolicyRefreshesAcrossH2Connects uses one real Actor and two
// fresh TLS dials to the same service IP:port. atunnel keeps the Actor's H2
// connection open while the policy changes between CONNECT streams.
func TestActorEgressPolicyRefreshesAcrossH2Connects(t *testing.T) {
	if os.Getenv(e2e.AtenetDataplaneEnv) == "agentgateway" {
		t.Skip("TLS passthrough policy is not supported by this dataplane")
	}
	ctx := t.Context()
	const expectedBody = "policy pool origin"
	target, address, rootCA := prepareProtocolOrigin(t, ctx, e2e.ServerPod{
		Name:       "policy-pool-origin",
		ImportPath: "github.com/agent-substrate/substrate/internal/e2e/fixtures/testserver",
		Args:       []string{"http", "--body=" + expectedBody},
		Port:       8443,
	}, true)

	hostname, _, err := net.SplitHostPort(address)
	if err != nil {
		t.Fatalf("splitting TLS origin address %q: %v", address, err)
	}
	ports := []int32{8443}
	initialRules := []*ateapipb.EgressRule{
		e2e.EgressAllowPassthroughOnPorts(ports, hostname, "control.test"),
	}
	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-policy-pool", egressFixture(), initialRules...)
	router := mustRouterClient(t, ctx)
	t.Cleanup(func() { router.Close() })
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)
	actorObject := &ateapipb.ObjectRef{Atespace: actorRef.Atespace, Name: actorRef.Name}
	clients := e2e.GetClients()
	since := metav1.NewTime(time.Now().Add(-time.Minute))
	type fetchResult struct {
		StatusCode int    `json:"statusCode"`
		Body       string `json:"body"`
		Protocol   string `json:"protocol"`
		TLS        bool   `json:"tls"`
		Error      string `json:"error"`
	}
	decode := func(raw []byte) fetchResult {
		t.Helper()
		var result fetchResult
		if err := json.Unmarshal(raw, &result); err != nil {
			t.Fatalf("decoding Actor egress result %q: %v", raw, err)
		}
		return result
	}

	request := map[string]any{"url": "https://" + address + "/healthz", "rootCA": rootCA, "http1": true}
	firstRaw := postEgressWithStatus(t, ctx, router, actorRef, "/", request, http.StatusOK)
	first := decode(firstRaw)
	if first.StatusCode != http.StatusOK || first.Body != expectedBody || first.Protocol != "HTTP/1.1" || !first.TLS {
		t.Fatalf("first allowed TLS fetch: result %+v; want verified HTTP/1.1 response %q", first, expectedBody)
	}

	updatedRules := []*ateapipb.EgressRule{e2e.EgressAllowPassthroughOnPorts(ports, "control.test")}
	e2e.EnsureEgressPolicy(t, ctx, clients, actorObject, updatedRules...)
	policy, err := clients.SubstrateAPI.GetActorEgressPolicy(ctx, &ateapipb.GetActorEgressPolicyRequest{Actor: actorObject})
	if err != nil {
		t.Fatalf("reading updated EgressPolicy for %s: %v", actorName, err)
	}
	if rules := policy.GetRules(); len(rules) != 1 || rules[0].GetTlsPassthrough() == nil || len(rules[0].GetTlsPassthrough().GetHostnames()) != 1 || rules[0].GetTlsPassthrough().GetHostnames()[0] != "control.test" {
		t.Fatalf("updated EgressPolicy rules = %v, want only TLS passthrough control.test", rules)
	}

	// atenet-egress caches each Actor's policy for 10 seconds by default.
	time.Sleep(11 * time.Second)
	secondRaw := postEgressWithStatus(t, ctx, router, actorRef, "/", request, http.StatusBadGateway)
	second := decode(secondRaw)
	if second.Error == "" {
		t.Errorf("revoked SNI fetch result %+v; want inner TLS failure after an accepted CONNECT", second)
	} else {
		t.Logf("revoked SNI failed after policy update: %s", second.Error)
	}

	var observed []gatewayAccessLogLine
	wantAuthority := target.Address()
	wantIdentity := resources.AteomForActorSPIFFEID(actorRef).String()
	waitForAccessLog(t, ctx, since, "two H2 CONNECT streams for the same Actor and origin authority", func(lines []gatewayAccessLogLine) bool {
		observed = observed[:0]
		for _, line := range lines {
			if line.container != "envoy" {
				continue
			}
			authority, _ := accessLogField(line.text, "authority")
			peers, _ := accessLogField(line.text, "peer_san")
			if authority == wantAuthority && strings.Contains(peers, wantIdentity) {
				observed = append(observed, line)
			}
		}
		return len(observed) >= 2
	})
	if len(observed) != 2 {
		t.Fatalf("observed %d matching CONNECT records, want two: %+v", len(observed), observed)
	}
	connectionID := ""
	for i, line := range observed {
		protocol, _ := accessLogField(line.text, "protocol")
		id, _ := accessLogField(line.text, "connection_id")
		code, _ := accessLogField(line.text, "code")
		if protocol != "HTTP/2" || id == "" || code != "200" {
			t.Errorf("CONNECT record %d = %q; want HTTP/2, a connection ID, and accepted CONNECT code 200", i+1, line.text)
		}
		if i == 0 {
			connectionID = id
		} else if id != connectionID {
			t.Errorf("CONNECT records used connection IDs %q and %q, want one shared H2 connection", connectionID, id)
		}
		t.Logf("H2 CONNECT evidence: %s", line.text)
	}
}

// TestActorEgressRequiresPolicy: an actor with no EgressPolicy is denied.
func TestActorEgressRequiresPolicy(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	target := e2e.DeployServerPod(t, ctx, egressHTTPTarget())

	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-nopolicy", e2e.EgressFixture())
	router := mustRouterClient(t, ctx)
	defer router.Close()
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}

	// There is no allowed fetch to prove the route is up, so wait on the
	// demo's readiness endpoint; otherwise a 503 from a route not yet
	// propagated would look like the answer we want.
	waitForActorRoute(t, ctx, router, actorRef)

	url := fmt.Sprintf("http://%s/healthz", target.Address())
	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, url, notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch by an actor with no policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
	}
	t.Logf("egress was denied as expected: %s", body)
}

// hostnamePolicyActor creates an actor whose policy allows HTTPS to
// example.com and nothing else, and waits until it is routable.
func hostnamePolicyActor(t *testing.T, ctx context.Context) (*e2e.RouterClient, resources.ActorRef) {
	t.Helper()
	_, actorName, _ := createAndResumeActorWithEgress(t, ctx, "egress-sni", e2e.EgressFixture(), e2e.EgressAllowHTTPS("example.com"), e2e.EgressAllowPassthrough("example.edu"))
	router := mustRouterClient(t, ctx)
	t.Cleanup(func() { router.Close() })
	actorRef := resources.ActorRef{Atespace: networkingAtespace, Name: actorName}
	waitForActorRoute(t, ctx, router, actorRef)
	return router, actorRef
}

func isMitmCert(body string) bool {
	// example.* domains use certs signed by Google Trust Services
	// If this substring is not present it means dataplane used a minted cert
	return !strings.Contains(body, "O=Google Trust Services")
}

// TestActorEgressHTTPSByHostnameMITM: the gateway terminates the TLS and decides
// each request by name: example.com 200, example.org 403.
func TestActorEgressHTTPSByHostnameMITM(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	router, actorRef := hostnamePolicyActor(t, ctx)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.com/", reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host returned HTTP %d, want 200; body: %s", status, body)
	}
	if !isMitmCert(string(body)) {
		t.Fatalf("request did not use minted cert; body: %s", body)
	}
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.org/", notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of a host outside the policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
	}
	t.Logf("denied on the decrypted request: %s", body)
}

// TestActorEgressHTTPSByHostnamePassthrough: the gateway acts as TCP proxy fetching
// allowed SNI.
func TestActorEgressHTTPSByHostnamePassthrough(t *testing.T) {
	ctx := context.Background()
	dataplane := e2e.CurrentAtenetDataplane()
	router, actorRef := hostnamePolicyActor(t, ctx)

	status, body := fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.edu/", reached)
	if status != http.StatusOK {
		t.Fatalf("fetch of the allowed host returned HTTP %d, want 200; body: %s", status, body)
	}
	if isMitmCert(string(body)) {
		t.Fatalf("request used minted cert; body: %s", body)
	}
	status, body = fetchThroughEgressActorUntil(t, ctx, router, actorRef, "https://example.org/", notTransient)
	if !dataplane.IsEgressPolicyDenied(status, string(body)) {
		t.Fatalf("fetch of a host outside the policy returned HTTP %d, want an egress-policy denial; body: %s", status, body)
	}
}

// fetchThroughEgressActorUntil is fetchThroughEgressActor with the caller
// deciding which answer is final.
func fetchThroughEgressActorUntil(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef, url string, done func(status int, body []byte) bool) (int, []byte) {
	t.Helper()
	payload := []byte(fmt.Sprintf(`{"url":%q}`, url))
	return postThroughEgressActorUntil(t, ctx, router, actorRef, "/", payload, done)
}

// waitForActorRoute polls the demo app's readiness endpoint through the router
// until it answers, which is when the actor's route has reached the ingress
// dataplane.
func waitForActorRoute(t *testing.T, ctx context.Context, router *e2e.RouterClient, actorRef resources.ActorRef) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		response, err := router.Get(ctx, actorRef, "/readyz")
		if err != nil {
			t.Fatalf("GET /readyz on the egress Actor through ingress: %v", err)
		}
		response.Body.Close()
		if response.StatusCode == http.StatusOK {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the egress Actor's route did not come up: GET /readyz returned HTTP %d", response.StatusCode)
		}
		time.Sleep(time.Second)
	}
}
