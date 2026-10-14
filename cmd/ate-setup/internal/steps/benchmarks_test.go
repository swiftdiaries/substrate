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

package steps

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
	"github.com/spf13/pflag"
)

func TestLegacyHelperGetsNoDevEnv(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "helper.sh"), []byte("#!/bin/sh\ntest \"$NO_DEV_ENV\" = 1\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := (&Env{Cfg: &config.Config{Root: root}}).runScript(t.Context(), "helper.sh"); err != nil {
		t.Fatalf("legacy helper did not receive NO_DEV_ENV=1: %v", err)
	}
}

func TestLegacyBenchmarkScriptsHonorNoDevEnv(t *testing.T) {
	sourceRoot, err := config.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, ".ate-dev-env.sh"), []byte("exit 93\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"git":    "#!/bin/sh\necho \"$TEST_ROOT\"\n",
		"docker": "#!/bin/sh\nexit 0\n",
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(body), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	for _, rel := range []string{
		"benchmarking/workloads/deploy.sh",
		"benchmarking/locust/deploy.sh",
		"benchmarking/locust/build_and_push.sh",
	} {
		dst := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(filepath.Join(sourceRoot, rel))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, data, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	manifest := filepath.Join(root, "benchmarking/workloads/manifests/workloads.yaml.tmpl")
	if err := os.MkdirAll(filepath.Dir(manifest), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		path string
		args []string
	}{
		{path: "benchmarking/workloads/deploy.sh", args: []string{"--help"}},
		{path: "benchmarking/locust/deploy.sh", args: []string{"--help"}},
		{path: "benchmarking/locust/build_and_push.sh"},
	} {
		t.Run(tc.path, func(t *testing.T) {
			cmd := exec.Command("bash", append([]string{filepath.Join(root, tc.path)}, tc.args...)...)
			cmd.Dir = root
			cmd.Env = []string{
				"PATH=" + bin + ":" + os.Getenv("PATH"),
				"TEST_ROOT=" + root,
				"NO_DEV_ENV=1",
				"PROJECT_ID=test-project",
				"BUCKET_NAME=test-bucket",
			}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("%s sourced .ate-dev-env.sh or otherwise failed: %v\n%s", tc.path, err, out)
			}
		})
	}
}

// deploy_locust.sh takes these as flags only, so anything not forwarded here is
// silently lost. The expected lists are what deploy_benchmarks in the shell
// installer assembled.
func TestDeployLocustArgs(t *testing.T) {
	opts := BenchmarkOptions{WorkerCount: 4, SandboxClass: config.SandboxClassGvisor}

	for _, tc := range []struct {
		name         string
		otlpEndpoint string
		actorMemory  string
		want         []string
	}{
		{
			name: "neither configured",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor"},
		},
		{
			name:         "otlp endpoint only",
			otlpEndpoint: "http://otel-collector.ate-system.svc:4317",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--otlp-endpoint", "http://otel-collector.ate-system.svc:4317"},
		},
		{
			name:        "actor memory only",
			actorMemory: "256Mi",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--actor-memory", "256Mi"},
		},
		{
			name:         "both configured",
			otlpEndpoint: "http://otel-collector.ate-system.svc:4317",
			actorMemory:  "256Mi",
			want: []string{"--deploy", "--worker-count", "4", "--sandbox-class", "gvisor",
				"--otlp-endpoint", "http://otel-collector.ate-system.svc:4317",
				"--actor-memory", "256Mi"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := deployLocustArgs(opts, tc.otlpEndpoint, tc.actorMemory, "", "")
			if !slices.Equal(got, tc.want) {
				t.Errorf("deployLocustArgs() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestDeployLocustArgsSelectsRequestedCluster(t *testing.T) {
	got := deployLocustArgs(BenchmarkOptions{WorkerCount: 1, SandboxClass: config.SandboxClassGvisor}, "", "", "/tmp/requested.yaml", "requested-context")
	want := []string{"--deploy", "--worker-count", "1", "--sandbox-class", "gvisor", "--kubeconfig", "/tmp/requested.yaml", "--context", "requested-context"}
	if !slices.Equal(got, want) {
		t.Fatalf("deployLocustArgs() = %v, want %v", got, want)
	}
}

func TestKubeconfigListUsesTheEnvironment(t *testing.T) {
	list := strings.Join([]string{"/tmp/first.yaml", "/tmp/second.yaml"}, string(os.PathListSeparator))
	if got := kubeFlags(list, "requested-context"); !slices.Equal(got, []string{"--context", "requested-context"}) {
		t.Errorf("kubeFlags() = %v, want only context flag for path list", got)
	}
	got := deployLocustArgs(BenchmarkOptions{WorkerCount: 1, SandboxClass: config.SandboxClassGvisor}, "", "", list, "requested-context")
	want := []string{"--deploy", "--worker-count", "1", "--sandbox-class", "gvisor", "--context", "requested-context"}
	if !slices.Equal(got, want) {
		t.Errorf("deployLocustArgs() = %v, want %v", got, want)
	}
}

func TestMicrovmInstallScriptUsesResolvedEnvironment(t *testing.T) {
	for _, name := range []string{"ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING", "ATE_API_POSTGRES_OWNER_CONNECTION_STRING", "ANTHROPIC_API_KEY", "KUBECONFIG"} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	testRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(testRoot, "hack"), 0o755); err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile(filepath.Join(root, installMicrovmDepScript))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(testRoot, installMicrovmDepScript), script, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(testRoot, ".ate-dev-env.sh"), []byte("BUCKET_NAME=stale-bucket\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	if err := os.WriteFile(filepath.Join(bin, "git"), []byte("#!/usr/bin/env bash\nprintf '%s\\n' \"${TEST_ROOT}\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	commandLog := filepath.Join(t.TempDir(), "kubectl.log")
	kubectl := "#!/usr/bin/env bash\nprintf 'bucket=%s context=%s args=%s\\n' \"${BUCKET_NAME:-}\" \"${KUBECTL_CONTEXT:-}\" \"$*\" >> \"${COMMAND_LOG}\"\n"
	if err := os.WriteFile(filepath.Join(bin, "kubectl"), []byte(kubectl), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	t.Setenv("TEST_ROOT", testRoot)
	t.Setenv("COMMAND_LOG", commandLog)
	t.Setenv("BUCKET_NAME", "parent-bucket")

	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.BindFlags(flags)
	cfg, err := config.LoadFlags(flags, config.LoadOptions{})
	if err != nil {
		t.Fatalf("config.Load() error = %v", err)
	}
	cfg.Root = testRoot
	cfg.BucketName = "resolved-bucket"
	env := &Env{Cfg: cfg}
	if err := env.runScript(context.Background(), installMicrovmDepScript, "--delete", "--context", "resolved-context"); err != nil {
		t.Fatalf("runScript() error = %v", err)
	}

	commands, err := os.ReadFile(commandLog)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(commands); !strings.Contains(got, "bucket=resolved-bucket") || !strings.Contains(got, "args=--context resolved-context") {
		t.Errorf("kubectl received stale child environment: %s", got)
	}
}

func TestSetupMicroVMUsesResolvedSettings(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "hack"), 0o755); err != nil {
		t.Fatal(err)
	}
	capture := filepath.Join(t.TempDir(), "microvm.log")
	script := `#!/bin/sh
printf '%s\n' "$*" "$KUBECONFIG" "$KUBECTL_CONTEXT" "$BUCKET_NAME" "$KO_DEFAULTPLATFORMS" "$KIND_CLUSTER_NAME" "$NO_DEV_ENV" "$OUT" > "$MICROVM_TEST_LOG"
`
	if err := os.WriteFile(filepath.Join(root, installMicrovmDepScript), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("MICROVM_TEST_LOG", capture)
	t.Setenv("KIND_CLUSTER_NAME", "ambient-kind")
	t.Setenv("KUBECTL_CONTEXT", "ambient-context")
	t.Setenv("OUT", "asset-build-output")
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	config.BindFlags(flags)
	if err := flags.Parse([]string{"--kind", "--kind-cluster-name=selected-kind", "--context=selected-context", "--kubeconfig=/a.yaml:/b.yaml", "--storage-bucket-name=selected-bucket", "--ko-default-platforms=linux/arm64"}); err != nil {
		t.Fatal(err)
	}
	cfg, err := config.LoadFlags(flags, config.LoadOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Root = root
	e := &Env{Cfg: cfg}
	if err := e.SetupMicroVM(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	const want = "--install --context selected-context\n/a.yaml:/b.yaml\nselected-context\nselected-bucket\nlinux/arm64\nselected-kind\n1\nasset-build-output\n"
	if string(got) != want {
		t.Errorf("microVM helper saw %q, want %q", got, want)
	}
}

func TestDeployLocustDeleteUsesRequestedClusterForKubectlAndKubectlAte(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	log := filepath.Join(t.TempDir(), "commands")
	ko := filepath.Join(bin, "ko")
	if err := os.WriteFile(ko, []byte("#!/usr/bin/env bash\nprintf 'ko %s\\n' \"$*\" >> \"${COMMAND_LOG}\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	goStub := `#!/usr/bin/env bash
case "$1" in
  tool) echo "$MOCK_KO" ;;
  build)
    while [[ "$#" -gt 0 ]]; do
      if [[ "$1" == -o ]]; then shift; out="$1"; fi
      shift
    done
    cat > "$out" <<'EOF'
#!/usr/bin/env bash
printf 'kubectl-ate %s\n' "$*" >> "${COMMAND_LOG}"
EOF
    chmod +x "$out"
    ;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(goStub), 0o755); err != nil {
		t.Fatal(err)
	}
	kubectl := filepath.Join(bin, "kubectl")
	if err := os.WriteFile(kubectl, []byte("#!/usr/bin/env bash\nprintf 'kubectl %s\\n' \"$*\" >> \"${COMMAND_LOG}\"\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "benchmarking/deploy_locust.sh", "--delete", "--kubeconfig", "/tmp/requested.yaml", "--context", "requested-context")
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"COMMAND_LOG="+log,
		"MOCK_KO="+ko,
		"PROJECT_ID=test-project",
		"BUCKET_NAME=test-bucket",
		"KUBECONFIG=/tmp/ambient.yaml",
		"KUBECTL_CONTEXT=ambient-context",
	)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("benchmark delete: %v\n%s", err, out)
	}
	commands, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"kubectl ", "kubectl-ate ", "ko "} {
		found := false
		for _, line := range strings.Split(string(commands), "\n") {
			if strings.HasPrefix(line, name) {
				found = true
				if !strings.Contains(line, "--kubeconfig /tmp/requested.yaml") || !strings.Contains(line, "--context requested-context") {
					t.Errorf("%s command did not select requested cluster: %s", strings.TrimSpace(name), line)
				}
				if strings.Contains(line, "ambient-context") || strings.Contains(line, "/tmp/ambient.yaml") {
					t.Errorf("%s command retained ambient cluster: %s", strings.TrimSpace(name), line)
				}
			}
		}
		if !found {
			t.Errorf("no captured %s command in %s", strings.TrimSpace(name), commands)
		}
	}
}

func TestWorkloadDeployReadsOTLPEndpointFromRequestedCluster(t *testing.T) {
	root, err := config.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	log := filepath.Join(t.TempDir(), "commands")
	goStub := `#!/usr/bin/env bash
while [[ "$#" -gt 0 ]]; do
  if [[ "$1" == -o ]]; then shift; out="$1"; fi
  shift
done
cat > "$out" <<'EOF'
#!/usr/bin/env bash
printf 'kubectl-ate %s\n' "$*" >> "${COMMAND_LOG}"
EOF
chmod +x "$out"
`
	if err := os.WriteFile(filepath.Join(bin, "go"), []byte(goStub), 0o755); err != nil {
		t.Fatal(err)
	}
	kubectl := filepath.Join(bin, "kubectl")
	kubectlStub := `#!/usr/bin/env bash
printf 'kubectl %s\n' "$*" >> "${COMMAND_LOG}"
`
	if err := os.WriteFile(kubectl, []byte(kubectlStub), 0o755); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("bash", "benchmarking/workloads/deploy.sh", "--deploy", "--kubeconfig", "/tmp/requested.yaml", "--context", "requested-context", "--wait-timeout", "1")
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"PATH="+bin+":"+os.Getenv("PATH"),
		"COMMAND_LOG="+log,
		"PROJECT_ID=test-project",
		"BUCKET_NAME=test-bucket",
		"KUBECONFIG=/tmp/ambient.yaml",
		"KUBECTL_CONTEXT=ambient-context",
	)
	out, runErr := cmd.CombinedOutput()
	if runErr == nil || !strings.Contains(string(out), "cannot read OTEL_EXPORTER_OTLP_ENDPOINT") {
		t.Fatalf("benchmark workload deploy should stop after the captured endpoint lookup, err=%v\n%s", runErr, out)
	}
	commands, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(commands), "\n") {
		if strings.HasPrefix(line, "kubectl ") && strings.Contains(line, " get configmap ate-otel-config") {
			if !strings.Contains(line, "--kubeconfig /tmp/requested.yaml") || !strings.Contains(line, "--context requested-context") {
				t.Fatalf("OTLP endpoint lookup did not select requested cluster: %s", line)
			}
			if strings.Contains(line, "ambient-context") || strings.Contains(line, "/tmp/ambient.yaml") {
				t.Fatalf("OTLP endpoint lookup retained ambient cluster: %s", line)
			}
			return
		}
	}
	t.Fatalf("no OTLP endpoint lookup captured in %s", commands)
}
