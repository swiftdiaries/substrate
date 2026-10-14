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
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/config"
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
			got := deployLocustArgs(opts, tc.otlpEndpoint, tc.actorMemory)
			if !slices.Equal(got, tc.want) {
				t.Errorf("deployLocustArgs() = %v, want %v", got, tc.want)
			}
		})
	}
}
