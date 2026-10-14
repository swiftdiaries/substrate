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

package config

import (
	"os"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
)

// loadEnv isolates Load from the ambient environment.
//
// Tests isolate retained secret inputs and tool conventions from the ambient
// process environment, then set back only what they exercise.
func loadEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"ACTOR_JWT_ALGORITHM",
		"ACTOR_JWT_ISSUER",
		"ANTHROPIC_API_KEY",
		"ATE_ADDITIONAL_EGRESS_EXTPROC_SERVICE",
		"ATE_API_POSTGRES_CLOUDSQL_GSA",
		"ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH",
		"ATE_API_POSTGRES_CLOUDSQL_IP_TYPE",
		"ATE_API_POSTGRES_OWNER_CONNECTION_STRING",
		"ATE_API_POSTGRES_OWNER_ROLE",
		"ATE_API_POSTGRES_POOL_MAX_CONNS",
		"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING",
		"ATE_API_POSTGRES_READ_WRITE_ROLE",
		"ATE_API_POSTGRES_SCHEMA",
		"ATE_API_POSTGRES_SERVER_CA_FILE",
		"ATE_ATENET_DATAPLANE",
		"ATE_CREDENTIAL_PROVIDER",
		"ATE_IMAGE_REPO",
		"ATE_IMAGE_TAG",
		"ATE_INSTALL_CLUSTER_SIZE",
		"ATE_INSTALL_CORDON_CONTROL_PLANE",
		"ATE_INSTALL_KIND",
		"ATE_INSTALL_PODCERT_WORKERS_PER_SIGNER",
		"ATE_INSTALL_ROLLOUT_TIMEOUT",
		"ATE_OTLP_ENDPOINT",
		"BENCHMARK_ACTOR_MEMORY",
		"BUCKET_NAME",
		"CLUSTER_LOCATION",
		"CLUSTER_NAME",
		"DOCKER_BUILD_FLAGS",
		"EXPECTED_JWT_ISSUER",
		"KIND_CLUSTER_NAME",
		"KO_DEFAULTPLATFORMS",
		"KO_DOCKER_REPO",
		"KUBECONFIG",
		"KUBECTL_CONTEXT",
		"MEMORYSTORE_INSTANCE",
		"PROJECT_ID",
		"ATE_API_POSTGRES_CLOUDSQL_INSTANCE",
	} {
		// Unset, not blanked. An exported empty variable is a value a channel
		// supplied, which is what an operator writes to clear a setting; a
		// test that blanked these would be configuring every one of them.
		unsetEnv(t, name)
	}
}

func loadFileValues(t *testing.T, opts Options, values map[string]string) (*Config, error) {
	t.Helper()
	fs, err := optionsFlagSet(opts)
	if err != nil {
		return nil, err
	}
	env := environ()
	r, err := Resolve(fs, ResolveOptions{File: fileWith(values), Env: env})
	if err != nil {
		return nil, err
	}
	root, err := RepoRoot()
	if err != nil {
		return nil, err
	}
	return buildConfig(root, env, r)
}

// unsetEnv removes a variable for the duration of the test. t.Setenv first, so
// that its cleanup restores whatever the caller's environment had.
func unsetEnv(t *testing.T, name string) {
	t.Helper()
	t.Setenv(name, "")
	if err := os.Unsetenv(name); err != nil {
		t.Fatalf("os.Unsetenv(%s) = %v", name, err)
	}
}

func TestLoadDefaults(t *testing.T) {
	loadEnv(t)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Router != RouterEnvoy {
		t.Errorf("Router = %q, want %q", cfg.Router, RouterEnvoy)
	}
	if cfg.PostgresReadWriteConnectionString != "" || cfg.PostgresOwnerConnectionString != "" {
		t.Errorf("unexpected external PostgreSQL connections: %q, %q", cfg.PostgresReadWriteConnectionString, cfg.PostgresOwnerConnectionString)
	}
	if cfg.PostgresReadWriteRole != DefaultPostgresReadWriteRole || cfg.PostgresOwnerRole != DefaultPostgresOwnerRole {
		t.Errorf("unexpected default PostgreSQL roles: %q, %q", cfg.PostgresReadWriteRole, cfg.PostgresOwnerRole)
	}
	if cfg.RolloutTimeout != DefaultRolloutTimeout {
		t.Errorf("RolloutTimeout = %v, want %v", cfg.RolloutTimeout, DefaultRolloutTimeout)
	}
	if cfg.ClusterSize != ClusterSizeSize0 {
		t.Errorf("ClusterSize = %q, want %q", cfg.ClusterSize, ClusterSizeSize0)
	}
	if cfg.CordonControlPlane {
		t.Error("CordonControlPlane = true, want false")
	}
}

func TestLoadClusterSize(t *testing.T) {
	for _, tc := range []struct {
		name     string
		opts     Options
		file     map[string]string
		wantSize string
	}{
		{
			name:     "flag",
			opts:     Options{ClusterSize: ClusterSizeSize10},
			wantSize: ClusterSizeSize10,
		},
		{
			name:     "file",
			file:     map[string]string{"clusterSize": ClusterSizeSize10},
			wantSize: ClusterSizeSize10,
		},
		{
			name:     "flag beats the file",
			opts:     Options{ClusterSize: ClusterSizeSize0},
			file:     map[string]string{"clusterSize": ClusterSizeSize10},
			wantSize: ClusterSizeSize0,
		},
		{
			name:     "explicit connection string is untouched",
			opts:     Options{ClusterSize: ClusterSizeSize10},
			file:     nil,
			wantSize: ClusterSizeSize10,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			cfg, err := loadFileValues(t, tc.opts, tc.file)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.ClusterSize != tc.wantSize {
				t.Errorf("ClusterSize = %q, want %q", cfg.ClusterSize, tc.wantSize)
			}
			if cfg.Size10() != (tc.wantSize == ClusterSizeSize10) {
				t.Errorf("Size10() = %v, want %v", cfg.Size10(), tc.wantSize == ClusterSizeSize10)
			}
			env := scriptEnvMap(t, cfg)
			if tc.wantSize == ClusterSizeSize10 {
				if env["ATE_INSTALL_CLUSTER_SIZE"] != ClusterSizeSize10 {
					t.Errorf("ScriptEnv()[ATE_INSTALL_CLUSTER_SIZE] = %q, want size10", env["ATE_INSTALL_CLUSTER_SIZE"])
				}
			} else if _, ok := env["ATE_INSTALL_CLUSTER_SIZE"]; ok {
				t.Errorf("ScriptEnv() exports ATE_INSTALL_CLUSTER_SIZE = %q for the default profile, want it absent", env["ATE_INSTALL_CLUSTER_SIZE"])
			}
		})
	}
}

func TestLoadCordonControlPlane(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts Options
		file string
		want bool
	}{
		{name: "flag", opts: Options{CordonControlPlane: true}, want: true},
		{name: "file true", file: "true", want: true},
		{name: "file false", file: "false", want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			cfg, err := loadFileValues(t, tc.opts, map[string]string{"cordonControlPlane": tc.file})
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.CordonControlPlane != tc.want {
				t.Errorf("CordonControlPlane = %v, want %v", cfg.CordonControlPlane, tc.want)
			}
			_, exported := scriptEnvMap(t, cfg)["ATE_INSTALL_CORDON_CONTROL_PLANE"]
			if exported != tc.want {
				t.Errorf("ScriptEnv() exports ATE_INSTALL_CORDON_CONTROL_PLANE = %v, want %v", exported, tc.want)
			}
		})
	}
}

func TestLoadDockerBuildFlags(t *testing.T) {
	loadEnv(t)
	cfg, err := loadFileValues(t, Options{}, map[string]string{"docker.buildFlags": " --cache-from type=gha  --cache-to type=gha,mode=max "})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := []string{"--cache-from", "type=gha", "--cache-to", "type=gha,mode=max"}
	if !slices.Equal(cfg.DockerBuildFlags, want) {
		t.Errorf("DockerBuildFlags = %q, want %q", cfg.DockerBuildFlags, want)
	}
}

func TestLoadFlagsBeatFile(t *testing.T) {
	loadEnv(t)
	cfg, err := loadFileValues(t, Options{Router: RouterAgentgateway, RolloutTimeout: "120s"}, map[string]string{
		"atenet.dataplane": RouterEnvoy, "rolloutTimeout": "30s",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Router != RouterAgentgateway {
		t.Errorf("Router = %q, want %q", cfg.Router, RouterAgentgateway)
	}
	if want := 120 * time.Second; cfg.RolloutTimeout != want {
		t.Errorf("RolloutTimeout = %v, want %v", cfg.RolloutTimeout, want)
	}
}

func TestLoadPostgresConnectionStrings(t *testing.T) {
	loadEnv(t)
	const dsn = "postgresql://someone@db.example:5432/atepg?sslmode=disable"
	t.Setenv("ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING", dsn)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresReadWriteConnectionString != dsn || cfg.PostgresOwnerConnectionString != dsn {
		t.Errorf("PostgreSQL connections = %q, %q, want %q for both", cfg.PostgresReadWriteConnectionString, cfg.PostgresOwnerConnectionString, dsn)
	}

	t.Setenv("ATE_API_POSTGRES_OWNER_CONNECTION_STRING", "owner-dsn")
	cfg, err = Load(Options{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PostgresReadWriteConnectionString != dsn || cfg.PostgresOwnerConnectionString != "owner-dsn" {
		t.Errorf("separate PostgreSQL connections = %q, %q, want %q and owner-dsn", cfg.PostgresReadWriteConnectionString, cfg.PostgresOwnerConnectionString, dsn)
	}
}

func TestLoadPostgresIdentityOverrides(t *testing.T) {
	loadEnv(t)
	t.Setenv("ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING", "readwrite-dsn")
	t.Setenv("ATE_API_POSTGRES_OWNER_CONNECTION_STRING", "owner-dsn")
	cfg, err := loadFileValues(t, Options{}, map[string]string{
		"ateapi.postgres.readWrite.role": "tenant_readwrite",
		"ateapi.postgres.owner.role":     "tenant_owner",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PostgresReadWriteConnectionString != "readwrite-dsn" || cfg.PostgresOwnerConnectionString != "owner-dsn" || cfg.PostgresReadWriteRole != "tenant_readwrite" || cfg.PostgresOwnerRole != "tenant_owner" {
		t.Fatalf("PostgreSQL identity overrides not loaded: %+v", cfg)
	}
	if !cfg.PostgresReadWriteRoleSet || !cfg.PostgresOwnerRoleSet {
		t.Fatalf("PostgreSQL role overrides not marked as explicit: %+v", cfg)
	}
}

// ATE_API_POSTGRES_SCHEMA defaults to substrate, as in the apiserver, and
// an explicit value wins.
func TestLoadPostgresSchema(t *testing.T) {
	loadEnv(t)
	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresSchemaName() != "substrate" {
		t.Errorf("PostgresSchemaName() = %q, want %q", cfg.PostgresSchemaName(), "substrate")
	}

	cfg, err = loadFileValues(t, Options{}, map[string]string{"ateapi.postgres.schema": "tenant_schema"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresSchemaName() != "tenant_schema" {
		t.Errorf("PostgresSchemaName() = %q, want %q", cfg.PostgresSchemaName(), "tenant_schema")
	}
}

// The apiserver reads its pool size and its server CA out of the DSN and a
// mounted file respectively, neither of which the shell installer synthesizes.
func TestLoadPostgresTuning(t *testing.T) {
	loadEnv(t)
	cfg, err := loadFileValues(t, Options{}, map[string]string{
		"ateapi.postgres.poolMaxConns": "50",
		"ateapi.postgres.serverCAFile": "/etc/ssl/server-ca.pem",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.PostgresPoolMaxConns != "50" {
		t.Errorf("PostgresPoolMaxConns = %q, want 50", cfg.PostgresPoolMaxConns)
	}
	if want := "/etc/ssl/server-ca.pem"; cfg.PostgresServerCAFile != want {
		t.Errorf("PostgresServerCAFile = %q, want %q", cfg.PostgresServerCAFile, want)
	}
}

// ATE_API_POSTGRES_CLOUDSQL_INSTANCE is three-way, and Load is where the
// distinction is made: everything downstream sees only Instance and
// InstanceSet.
func TestLoadCloudSQL(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		loadEnv(t)
		cfg, err := Load(Options{})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.CloudSQL.InstanceSet {
			t.Errorf("CloudSQL = %+v, want InstanceSet false so the cluster's record is adopted", cfg.CloudSQL)
		}
	})

	t.Run("explicitly empty removes Cloud SQL", func(t *testing.T) {
		loadEnv(t)
		cfg, err := loadFileValues(t, Options{}, map[string]string{"ateapi.postgres.cloudsql.instance": ""})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg.CloudSQL.InstanceSet || cfg.CloudSQL.Instance != "" {
			t.Errorf("CloudSQL = %+v, want an explicitly empty instance", cfg.CloudSQL)
		}
	})

	t.Run("fully specified", func(t *testing.T) {
		loadEnv(t)
		cfg, err := loadFileValues(t, Options{}, map[string]string{
			"ateapi.postgres.cloudsql.instance": "p:r:i",
			"ateapi.postgres.cloudsql.gsa":      "ate@p.iam.gserviceaccount.com",
			"ateapi.postgres.cloudsql.iamAuth":  "false",
			"ateapi.postgres.cloudsql.ipType":   CloudSQLIPTypePSC,
		})
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		want := CloudSQLConfig{
			Instance:    "p:r:i",
			InstanceSet: true,
			GSA:         "ate@p.iam.gserviceaccount.com",
			IAMAuth:     "false",
			IPType:      CloudSQLIPTypePSC,
		}
		if cfg.CloudSQL != want {
			t.Errorf("CloudSQL = %+v, want %+v", cfg.CloudSQL, want)
		}
	})

	// An unrecognized IP type reaches the proxy as an unset flag, which silently
	// dials the public address instead of the private one that was meant.
	t.Run("rejects an unknown IP type", func(t *testing.T) {
		loadEnv(t)
		if _, err := loadFileValues(t, Options{}, map[string]string{"ateapi.postgres.cloudsql.ipType": "internal"}); err == nil || !strings.Contains(err.Error(), "config.ateapi.postgres.cloudsql.ipType") {
			t.Fatalf("Load() error = %v, want it to name the invalid IP type", err)
		}
	})
}

// EXPECTED_JWT_ISSUER overrides the issuer derived from the GKE coordinates,
// which is how a cluster authenticating against something other than its own
// OIDC discovery document is installed.
func TestLoadExpectedJWTIssuer(t *testing.T) {
	loadEnv(t)
	const issuer = "https://issuer.example.com"
	cfg, err := loadFileValues(t, Options{}, map[string]string{"ateapi.expectedJWTIssuer": issuer})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.ExpectedJWTIssuer != issuer {
		t.Errorf("ExpectedJWTIssuer = %q, want %q", cfg.ExpectedJWTIssuer, issuer)
	}
}

func TestLoadActorJWTAlgorithm(t *testing.T) {
	for _, tt := range []struct {
		name    string
		value   string
		want    string
		wantErr bool
	}{
		{name: "unset", want: "ES256"},
		{name: "RS256", value: "RS256", want: "RS256"},
		{name: "unsupported", value: "HS256", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			loadEnv(t)
			values := map[string]string{}
			if tt.value != "" {
				values["actorJWT.algorithm"] = tt.value
			}
			cfg, err := loadFileValues(t, Options{}, values)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Load() with actorJWT.algorithm=%q returned nil error", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.ActorJWTAlgorithm != tt.want {
				t.Errorf("ActorJWTAlgorithm = %q, want %q", cfg.ActorJWTAlgorithm, tt.want)
			}
		})
	}
}

func TestLoadActorJWTIssuer(t *testing.T) {
	for _, tt := range []struct {
		name      string
		value     string
		namespace string
		want      string
		wantErr   bool
	}{
		{name: "unset", want: "https://idp.ate-system.svc"},
		{name: "unset in a relocated install", namespace: "team-a", want: "https://idp.team-a.svc"},
		{name: "set is used as given", value: "https://idp.example.com/prod/", want: "https://idp.example.com/prod/"},
		{name: "not https", value: "http://idp.example.com", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			loadEnv(t)
			values := map[string]string{}
			if tt.value != "" {
				values["actorJWT.issuer"] = tt.value
			}
			if tt.namespace != "" {
				values["namespace"] = tt.namespace
			}
			t.Setenv("ACTOR_JWT_ISSUER", "http://ambient.invalid")
			cfg, err := loadFileValues(t, Options{}, values)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Load() with actorJWT.issuer=%q returned nil error", tt.value)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.ActorJWTIssuer != tt.want {
				t.Errorf("ActorJWTIssuer = %q, want %q", cfg.ActorJWTIssuer, tt.want)
			}
		})
	}
}

// The endpoint has to reach both the Go steps and the shell scripts ate-setup
// still delegates to, or the two halves of an install export different
// collectors.
func TestLoadOtlpEndpoint(t *testing.T) {
	loadEnv(t)
	cfg, err := loadFileValues(t, Options{}, map[string]string{"otlpEndpoint": "http://from-file:4317"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "http://from-file:4317"; cfg.OtlpEndpoint != want {
		t.Errorf("OtlpEndpoint = %q, want %q", cfg.OtlpEndpoint, want)
	}

	cfg, err = Load(Options{OtlpEndpoint: "http://from-flag:4317"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "http://from-flag:4317"; cfg.OtlpEndpoint != want {
		t.Errorf("OtlpEndpoint = %q, want %q", cfg.OtlpEndpoint, want)
	}
	if got := scriptEnvMap(t, cfg)["ATE_OTLP_ENDPOINT"]; got != "http://from-flag:4317" {
		t.Errorf("ScriptEnv()[ATE_OTLP_ENDPOINT] = %q, want the flag's value", got)
	}
}

// ate-setup's own client resolves $KUBECONFIG through the client-go loading
// rules, so the value has to reach ScriptEnv as well. Otherwise a developer who
// exports KUBECONFIG without passing --kubeconfig gets an install split across
// two clusters: ate-setup writes to the exported one while the shell scripts it
// delegates to fall back to ~/.kube/config.
func TestLoadKubeconfigFallsBackToEnvironment(t *testing.T) {
	loadEnv(t)
	t.Setenv("KUBECONFIG", "/home/dev/clusters/dev.yaml")

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "/home/dev/clusters/dev.yaml"; cfg.Kubeconfig != want {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Kubeconfig, want)
	}
	if !slices.Contains(cfg.ScriptEnv(), "KUBECONFIG=/home/dev/clusters/dev.yaml") {
		t.Errorf("ScriptEnv() does not carry KUBECONFIG: %v", cfg.ScriptEnv())
	}

	// The flag still wins over the environment.
	cfg, err = Load(Options{Kubeconfig: "/tmp/explicit.yaml"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "/tmp/explicit.yaml"; cfg.Kubeconfig != want {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Kubeconfig, want)
	}
}

// A developer who juggles clusters sets KUBECONFIG to a list. client-go's
// explicit path is one file, so forwarding the list makes every command fail
// with "stat /a.yaml:/b.yaml: no such file or directory"; the loading rules
// read and merge the list themselves when no explicit path is given.
func TestLoadKubeconfigListStaysWithTheLoadingRules(t *testing.T) {
	loadEnv(t)
	list := strings.Join([]string{"/home/dev/a.yaml", "/home/dev/b.yaml"}, string(os.PathListSeparator))
	t.Setenv("KUBECONFIG", list)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Kubeconfig != "" {
		t.Errorf("Kubeconfig = %q, want it empty so the client-go loading rules merge the list", cfg.Kubeconfig)
	}
	// kubectl understands the list, so the scripts still get it verbatim.
	if !slices.Contains(cfg.ScriptEnv(), "KUBECONFIG="+list) {
		t.Errorf("ScriptEnv() does not carry KUBECONFIG=%s: %v", list, cfg.ScriptEnv())
	}

	// An explicit --kubeconfig is one file by definition and still wins.
	cfg, err = Load(Options{Kubeconfig: "/tmp/explicit.yaml"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if want := "/tmp/explicit.yaml"; cfg.Kubeconfig != want {
		t.Errorf("Kubeconfig = %q, want %q", cfg.Kubeconfig, want)
	}
	if !slices.Contains(cfg.ScriptEnv(), "KUBECONFIG=/tmp/explicit.yaml") {
		t.Errorf("ScriptEnv() does not carry the --kubeconfig value: %v", cfg.ScriptEnv())
	}
}

// The shell installer left the podcertificate-controller and CSI waits at 120s
// and pointed --rollout-timeout only at the ate-system rollouts. WaitTimeout
// lets the flag reach every wait without its 60s default shortening the slow
// bootstrap ones.
func TestWaitTimeout(t *testing.T) {
	loadEnv(t)
	const historical = 120 * time.Second

	for _, tc := range []struct {
		name string
		opts Options
		file string
		want time.Duration
	}{
		{"default leaves the historical timeout alone", Options{}, "", historical},
		{"a longer flag raises it", Options{RolloutTimeout: "5m"}, "", 5 * time.Minute},
		{"a shorter flag lowers it", Options{RolloutTimeout: "30s"}, "", 30 * time.Second},
		{"the file counts as asking too", Options{}, "10m", 10 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Unset rather than blanked for the cases that do not exercise
			// the variable: an exported empty value is a duration the
			// operator supplied, and no empty string is one.
			values := map[string]string{}
			if tc.file != "" {
				values["rolloutTimeout"] = tc.file
			}
			cfg, err := loadFileValues(t, tc.opts, values)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got := cfg.WaitTimeout(historical); got != tc.want {
				t.Errorf("WaitTimeout(%v) = %v, want %v", historical, got, tc.want)
			}
		})
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	loadEnv(t)

	for _, tc := range []struct {
		name string
		opts Options
	}{
		{"router", Options{Router: "nginx"}},
		{"rollout timeout", Options{RolloutTimeout: "invalid"}},
		// Both parse. Accepted, they would turn every wait into a single probe
		// that fails against a workload which has not started yet.
		{"zero rollout timeout", Options{RolloutTimeout: "0s"}},
		{"negative rollout timeout", Options{RolloutTimeout: "-30s"}},
		{"podcert workers", Options{PodcertWorkersPerSigner: -1}},
		{"cluster size", Options{ClusterSize: "size5"}},
		{"extproc invalid format", Options{AdditionalEgressExtprocService: "extproc:50051"}},
		{"extproc agentgateway", Options{Router: RouterAgentgateway, AdditionalEgressExtprocService: "ate-system/extproc:50051"}},
		{"provider not JSON", Options{CredentialProvider: "k8s.io"}},
		{"provider as a JSON string", Options{CredentialProvider: `"off"`}},
		{"provider null", Options{CredentialProvider: `null`}},
		{"provider without a name", Options{CredentialProvider: `{"address":"vault.ate-system.svc:8200"}`}},
		{"provider with an unknown key", Options{CredentialProvider: `{"name":"k8s.io","adress":"x:1"}`}},
		{"provider with trailing data", Options{CredentialProvider: `{"enabled":false} {"name":"k8s.io"}`}},
		{"provider name with a scheme", Options{CredentialProvider: `{"name":"ate-secret://k8s.io"}`}},
		{"provider name not lowercase", Options{CredentialProvider: `{"name":"Vault.example.com","address":"vault.ate-system.svc:8200"}`}},
		{"provider name with a port", Options{CredentialProvider: `{"name":"k8s.io:443"}`}},
		{"provider name with a path", Options{CredentialProvider: `{"name":"k8s.io/default"}`}},
		{"provider name with a query", Options{CredentialProvider: `{"name":"k8s.io?x=1"}`}},
		{"disabled with a name", Options{CredentialProvider: `{"enabled":false,"name":"k8s.io"}`}},
		{"disabled with an address", Options{CredentialProvider: `{"enabled":false,"address":"stale.ate-system.svc:50051"}`}},
		{"enabled without a name", Options{CredentialProvider: `{"enabled":true}`}},
		{"kubernetes address without a port", Options{CredentialProvider: `{"name":"k8s.io","address":"secrets.ate-system.svc"}`}},
		{"other provider without an address", Options{CredentialProvider: `{"name":"vault.example.com"}`}},
		{"other provider address without a port", Options{CredentialProvider: `{"name":"vault.example.com","address":"vault.ate-system.svc"}`}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Load(tc.opts); err == nil {
				t.Fatal("Load() succeeded, want an error")
			}
		})
	}
}

// The provider selection is required only where the egress gateway is
// rendered, so Load accepts an absent value and CredentialProvider refuses it.
func TestCredentialProvider(t *testing.T) {
	loadEnv(t)

	for _, tc := range []struct {
		name    string
		opts    Options
		file    map[string]string
		want    CredentialProvider
		wantErr bool
	}{
		{name: "absent is an error", opts: Options{}, wantErr: true},
		{name: "disabled", opts: Options{CredentialProvider: `{"enabled":false}`}},
		{name: "disabled on agentgateway", opts: Options{Router: RouterAgentgateway, CredentialProvider: `{"enabled":false}`}},
		{
			name: "kubernetes on agentgateway",
			opts: Options{Router: RouterAgentgateway, CredentialProvider: `{"name":"k8s.io"}`},
			want: CredentialProvider{Name: K8sCredentialProviderName, Address: K8sCredentialProviderAddress},
		},
		{
			name: "another provider on agentgateway",
			opts: Options{Router: RouterAgentgateway, CredentialProvider: `{"name":"vault.example.com","address":"vault.ate-system.svc:50051"}`},
			want: CredentialProvider{Name: "vault.example.com", Address: "vault.ate-system.svc:50051"},
		},
		{
			name: "enabled true names a provider",
			opts: Options{CredentialProvider: `{"enabled":true,"name":"k8s.io"}`},
			want: CredentialProvider{Name: K8sCredentialProviderName, Address: K8sCredentialProviderAddress},
		},
		{
			name: "kubernetes gets the bundled address",
			opts: Options{CredentialProvider: `{"name":"k8s.io"}`},
			want: CredentialProvider{Name: K8sCredentialProviderName, Address: K8sCredentialProviderAddress},
		},
		{
			name: "kubernetes keeps a given address",
			opts: Options{CredentialProvider: `{"name":"k8s.io","address":"secrets.ate-system.svc:443"}`},
			want: CredentialProvider{Name: K8sCredentialProviderName, Address: "secrets.ate-system.svc:443"},
		},
		{
			name: "another provider",
			opts: Options{CredentialProvider: `{"name":"vault.example.com","address":"vault.ate-system.svc:50051"}`},
			want: CredentialProvider{Name: "vault.example.com", Address: "vault.ate-system.svc:50051"},
		},
		{
			name: "whitespace is allowed",
			opts: Options{CredentialProvider: ` { "name" : "k8s.io" } `},
			want: CredentialProvider{Name: K8sCredentialProviderName, Address: K8sCredentialProviderAddress},
		},
		{
			name: "file selects a provider",
			file: map[string]string{"atenet.egress.credentialProvider": `{"name":"k8s.io"}`},
			want: CredentialProvider{Name: K8sCredentialProviderName, Address: K8sCredentialProviderAddress},
		},
		{
			name: "the flag overrides the file",
			opts: Options{CredentialProvider: `{"enabled":false}`},
			file: map[string]string{"atenet.egress.credentialProvider": `{"name":"vault.example.com","address":"vault.ate-system.svc:50051"}`},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := loadFileValues(t, tc.opts, tc.file)
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			got, err := cfg.CredentialProvider()
			if tc.wantErr {
				if err == nil {
					t.Fatal("CredentialProvider() succeeded, want an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("CredentialProvider() error = %v", err)
			}
			if got != tc.want {
				t.Errorf("CredentialProvider() = %+v, want %+v", got, tc.want)
			}
			if got.Enabled() != (tc.want.Name != "") {
				t.Errorf("Enabled() = %v, want %v", got.Enabled(), tc.want.Name != "")
			}
		})
	}
}

func TestCredentialProviderServerName(t *testing.T) {
	for addr, want := range map[string]string{
		"k8s-credential-provider.ate-system.svc:50051": "k8s-credential-provider.ate-system.svc",
		"vault.example.com":                            "vault.example.com",
	} {
		if got := (CredentialProvider{Address: addr}).ServerName(); got != want {
			t.Errorf("ServerName(%q) = %q, want %q", addr, got, want)
		}
	}
}

func TestLoadRejectsInvalidRolloutTimeoutFile(t *testing.T) {
	loadEnv(t)
	for _, val := range []string{"0", "0s", "-1m", "forever"} {
		t.Run(val, func(t *testing.T) {
			if _, err := loadFileValues(t, Options{}, map[string]string{"rolloutTimeout": val}); err == nil {
				t.Fatalf("Load() with file value %q succeeded, want an error", val)
			}
		})
	}
}

func TestLoadRejectsInvalidPodcertWorkersFile(t *testing.T) {
	loadEnv(t)
	for _, val := range []string{"0", "-1", "abc"} {
		t.Run(val, func(t *testing.T) {
			if _, err := loadFileValues(t, Options{}, map[string]string{"podcert.workersPerSigner": val}); err == nil {
				t.Fatalf("Load() with file value %q succeeded, want an error", val)
			}
		})
	}
}

// --kind must reproduce the exports in the shell kind installer, including
// clearing the GKE coordinates and isolating the snapshot bucket name.
func TestLoadKindProfile(t *testing.T) {
	loadEnv(t)
	t.Setenv("PROJECT_ID", "some-gke-project")
	t.Setenv("CLUSTER_LOCATION", "us-central1-c")
	t.Setenv("BUCKET_NAME", "ambient-cloud-bucket")

	cfg, err := Load(Options{Kind: true})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if cfg.Context != "kind-kind" {
		t.Errorf("Context = %q, want kind-kind", cfg.Context)
	}
	if cfg.ProjectID != "" {
		t.Errorf("ProjectID = %q, want it cleared for kind", cfg.ProjectID)
	}
	if cfg.ClusterLocation != "" {
		t.Errorf("ClusterLocation = %q, want it cleared for kind", cfg.ClusterLocation)
	}
	if cfg.KODockerRepo != "localhost:5001" {
		t.Errorf("KODockerRepo = %q, want localhost:5001", cfg.KODockerRepo)
	}
	if want := "linux/" + runtime.GOARCH; cfg.KODefaultPlatforms != want {
		t.Errorf("KODefaultPlatforms = %q, want %q", cfg.KODefaultPlatforms, want)
	}
	if cfg.BucketName != "ate-snapshots" {
		t.Errorf("BucketName = %q, want ate-snapshots (must not inherit ambient)", cfg.BucketName)
	}
}

// scriptEnvMap turns ScriptEnv's KEY=VALUE slice back into a map. Absence and
// emptiness are distinct: the shell scripts use ${VAR:-default} fallbacks, so a
// variable that is set but empty behaves differently from an unset one.
func scriptEnvMap(t *testing.T, cfg *Config) map[string]string {
	t.Helper()
	env := make(map[string]string)
	for _, kv := range cfg.ScriptEnv() {
		name, value, ok := strings.Cut(kv, "=")
		if !ok {
			t.Fatalf("ScriptEnv() entry %q is not KEY=VALUE", kv)
		}
		env[name] = value
	}
	return env
}

func TestScriptEnvCarriesResolvedConfig(t *testing.T) {
	loadEnv(t)
	cfg, err := loadFileValues(t, Options{Context: "gke_demo", Kubeconfig: "/tmp/kubeconfig"}, map[string]string{
		"storage.bucketName": "from-file",
	})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	env := scriptEnvMap(t, cfg)
	for name, want := range map[string]string{
		"KUBECTL_CONTEXT": "gke_demo",
		"KUBECONFIG":      "/tmp/kubeconfig",
		"BUCKET_NAME":     "from-file",
	} {
		if env[name] != want {
			t.Errorf("ScriptEnv()[%s] = %q, want %q", name, env[name], want)
		}
	}
	// Nothing configured a project, so the variable must not be exported at
	// all rather than exported empty.
	if _, ok := env["PROJECT_ID"]; ok {
		t.Errorf("ScriptEnv() exports PROJECT_ID = %q, want it absent", env["PROJECT_ID"])
	}
}

func TestScriptEnvSanitizesInheritedInstallerSettings(t *testing.T) {
	loadEnv(t)
	for name, value := range map[string]string{
		"ATE_NAMESPACE":                                 "hostile",
		"ACTOR_JWT_ISSUER":                              "hostile",
		"STORAGE_CLASS":                                 "hostile",
		"SETUP_CSI":                                     "hostile",
		"ATE_DEMO_COUNTER_WITH_EXTERNAL_VOLUME":         "hostile",
		"ATE_BENCHMARK_ACTOR_MEMORY":                    "hostile",
		"ATE_BENCHMARK_RUNNER_CPU_COUNT":                "hostile",
		"ATE_BENCHMARK_WORKER_COUNT":                    "hostile",
		"ATE_BENCHMARK_SANDBOX_CLASS":                   "hostile",
		"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": "postgres://secret",
		"ANTHROPIC_API_KEY":                             "secret",
		"VERSION":                                       "v9.9.9",
		"KO_DOCKER_REPO":                                "ambient.example/hostile",
		"KO_DEFAULTPLATFORMS":                           "linux/hostile",
		"NO_DEV_ENV":                                    "ambient-control",
		"PATH":                                          os.Getenv("PATH"),
		"GITHUB_TOKEN":                                  "tool-credential",
	} {
		t.Setenv(name, value)
	}
	cfg, err := loadFileValues(t, Options{}, map[string]string{"ko.dockerRepo": "registry.example/resolved"})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	env := scriptEnvMap(t, cfg)
	for _, name := range []string{
		"ATE_NAMESPACE", "ACTOR_JWT_ISSUER", "STORAGE_CLASS", "SETUP_CSI", "ATE_DEMO_COUNTER_WITH_EXTERNAL_VOLUME",
		"ATE_BENCHMARK_ACTOR_MEMORY", "ATE_BENCHMARK_RUNNER_CPU_COUNT",
		"ATE_BENCHMARK_WORKER_COUNT", "ATE_BENCHMARK_SANDBOX_CLASS", "KO_DEFAULTPLATFORMS", "NO_DEV_ENV",
		"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING", "ANTHROPIC_API_KEY", "VERSION",
	} {
		if _, ok := env[name]; ok {
			t.Errorf("ScriptEnv() includes retired or secret input %s", name)
		}
	}
	for name, want := range map[string]string{
		"PATH":           os.Getenv("PATH"),
		"GITHUB_TOKEN":   "tool-credential",
		"KO_DOCKER_REPO": "registry.example/resolved",
	} {
		if env[name] != want {
			t.Errorf("ScriptEnv()[%s] = %q, want %q", name, env[name], want)
		}
	}
	if got := os.Getenv("ATE_NAMESPACE"); got != "hostile" {
		t.Errorf("parent ATE_NAMESPACE = %q, want hostile", got)
	}
}

func TestScriptEnvDoesNotDisableDevEnvGlobally(t *testing.T) {
	loadEnv(t)
	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if _, ok := scriptEnvMap(t, cfg)["NO_DEV_ENV"]; ok {
		t.Fatal("ScriptEnv() sets NO_DEV_ENV outside legacy helper invocations")
	}
}

func TestScriptEnvKindProfile(t *testing.T) {
	loadEnv(t)
	t.Setenv("PROJECT_ID", "some-gke-project")
	t.Setenv("CLUSTER_LOCATION", "us-central1-c")
	t.Setenv("MEMORYSTORE_INSTANCE", "some-instance")

	cfg, err := Load(Options{Kind: true})
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	env := scriptEnvMap(t, cfg)
	for _, name := range []string{"PROJECT_ID", "CLUSTER_LOCATION", "MEMORYSTORE_INSTANCE"} {
		if _, ok := env[name]; ok {
			t.Errorf("ScriptEnv() exports %s = %q for a kind install, want it absent", name, env[name])
		}
	}
	if env["ATE_INSTALL_KIND"] != "true" {
		t.Errorf("ScriptEnv()[ATE_INSTALL_KIND] = %q, want true", env["ATE_INSTALL_KIND"])
	}
	if env["KO_DOCKER_REPO"] != "localhost:5001" {
		t.Errorf("ScriptEnv()[KO_DOCKER_REPO] = %q, want localhost:5001", env["KO_DOCKER_REPO"])
	}
}

func TestScriptEnvKindNamePreservesContextDerivation(t *testing.T) {
	for _, tc := range []struct {
		name, explicit string
	}{
		{name: "context only"},
		{name: "explicit name", explicit: "named"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			t.Setenv("KIND_CLUSTER_NAME", "ambient")
			values := map[string]string{"kindCluster.enabled": "true", "context": "kind-selected"}
			if tc.explicit != "" {
				values["kindCluster.name"] = tc.explicit
			}
			cfg, err := loadFileValues(t, Options{}, values)
			if err != nil {
				t.Fatal(err)
			}
			env := scriptEnvMap(t, cfg)
			if got := env["KIND_CLUSTER_NAME"]; got != tc.explicit {
				t.Errorf("KIND_CLUSTER_NAME = %q, want %q (empty lets staging derive the context name)", got, tc.explicit)
			}
			if got := env["KUBECTL_CONTEXT"]; got != "kind-selected" {
				t.Errorf("KUBECTL_CONTEXT = %q, want kind-selected", got)
			}
		})
	}
}

func TestLoadImageSource(t *testing.T) {
	tests := []struct {
		name      string
		file      map[string]string
		opts      Options
		want      images.Source
		wantError string
	}{
		{
			name: "unset builds from source",
			want: images.Source{},
		},
		{
			name: "flags",
			opts: Options{ImageRepo: "example.com/substrate", ImageTag: "v1.2.3"},
			want: images.Source{Repo: "example.com/substrate", Tag: "v1.2.3"},
		},
		{
			name: "file",
			file: map[string]string{"images.repo": "example.com/file", "images.tag": "v9"},
			want: images.Source{Repo: "example.com/file", Tag: "v9"},
		},
		{
			name: "flags beat the file",
			file: map[string]string{"images.repo": "example.com/file", "images.tag": "v9"},
			opts: Options{ImageRepo: "example.com/flag", ImageTag: "v1"},
			want: images.Source{Repo: "example.com/flag", Tag: "v1"},
		},
		{
			// A repo pasted from a registry UI often carries one, and it would
			// otherwise produce a double slash in every reference.
			name: "trailing slash is trimmed",
			opts: Options{ImageRepo: "example.com/substrate/", ImageTag: "v1"},
			want: images.Source{Repo: "example.com/substrate", Tag: "v1"},
		},
		{
			name:      "a repo needs a tag",
			opts:      Options{ImageRepo: "example.com/substrate"},
			wantError: `images.repo="example.com/substrate" (from --image-repo) conflicts with images.tag="" (from default)`,
		},
		{
			name:      "a tag needs a repo",
			opts:      Options{ImageTag: "v1"},
			wantError: `images.tag="v1" (from --image-tag) conflicts with images.repo="" (from default)`,
		},
		{
			name:      "a tag from the file needs a repo",
			file:      map[string]string{"images.tag": "v1"},
			wantError: `images.tag="v1" (from config.images.tag) conflicts with images.repo="" (from default)`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			loadEnv(t)
			cfg, err := loadFileValues(t, tc.opts, tc.file)
			if tc.wantError != "" {
				if err == nil {
					t.Fatalf("Load() = nil error, want one containing %q", tc.wantError)
				}
				if !strings.Contains(err.Error(), tc.wantError) {
					t.Errorf("Load() error = %v, want it to contain %q", err, tc.wantError)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if cfg.Images.Repo != tc.want.Repo {
				t.Errorf("Images.Repo = %q, want %q", cfg.Images.Repo, tc.want.Repo)
			}
			if cfg.Images.Tag != tc.want.Tag {
				t.Errorf("Images.Tag = %q, want %q", cfg.Images.Tag, tc.want.Tag)
			}
			if cfg.Images.IsPrebuilt() != (tc.want.Repo != "") {
				t.Errorf("Images.IsPrebuilt() = %v, want %v", cfg.Images.IsPrebuilt(), tc.want.Repo != "")
			}
		})
	}
}

func TestLoadRejectsBuildVersionForPrebuiltImages(t *testing.T) {
	loadEnv(t)
	_, err := loadFileValues(t, Options{}, map[string]string{
		"images.repo":   "example.com/substrate",
		"images.tag":    "v1.2.3",
		"build.version": "v9.9.9",
	})
	if err == nil || !strings.Contains(err.Error(), "build.version must be unset when installing prebuilt images") {
		t.Fatalf("Load() error = %v, want rejection of build.version for prebuilt images", err)
	}
}
