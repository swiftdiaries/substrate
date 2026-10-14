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

package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

func isolateValidationCommand(t *testing.T) {
	t.Helper()
	oldEnv, oldResolved := env, resolved
	t.Cleanup(func() {
		env, resolved = oldEnv, oldResolved
	})
	env, resolved = nil, nil
	seen := map[*pflag.Flag]bool{}
	for _, fs := range []*pflag.FlagSet{rootCmd.PersistentFlags(), deployAteSystemCmd.Flags(), validateCmd.Flags()} {
		fs.VisitAll(func(f *pflag.Flag) {
			if seen[f] {
				return
			}
			seen[f] = true
			value, changed := f.Value.String(), f.Changed
			t.Cleanup(func() {
				_ = f.Value.Set(value)
				f.Changed = changed
			})
			if err := f.Value.Set(f.DefValue); err != nil {
				t.Fatal(err)
			}
			f.Changed = false
		})
	}
}

func TestValidateUsesDeploymentChecksWithoutClusterInitialization(t *testing.T) {
	// A complete GKE target would fetch credentials if the validation path
	// reached normal initialization. Capture tools as well as the shared Env.
	bin := t.TempDir()
	toolLog := filepath.Join(t.TempDir(), "tools.log")
	t.Setenv("VALIDATION_TOOL_LOG", toolLog)
	for _, tool := range []string{"gcloud", "kubectl", "kubectl-ate", "ko"} {
		if err := os.WriteFile(filepath.Join(bin, tool), []byte("#!/bin/sh\necho invoked >> \"$VALIDATION_TOOL_LOG\"\nexit 97\n"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, tc := range []struct {
		name, body, wantError string
		args                  []string
	}{
		{name: "valid without credentials", body: "gcp.projectID: project\ngcp.clusterName: cluster\ngcp.clusterLocation: zone\nkubeconfig: /does-not-exist\n"},
		{name: "valid NFS from file", body: "csi.setup: nfs\n"},
		{name: "valid Kind CSI from file", body: "kindCluster.enabled: true\ncsi.setup: both\n"},
		{name: "valid CSI flag overrides invalid file", body: "csi.setup: bogus\n", args: []string{"--setup-csi=nfs"}},
		{name: "unknown setting", body: "contxt: selected\n", wantError: `unknown setting "contxt"`},
		{name: "invalid ordinary setting", body: "atenet.dataplane: invalid\n", wantError: "atenet.dataplane"},
		{name: "invalid CSI from file", body: "csi.setup: bogus\n", wantError: "csi.setup"},
		{name: "invalid CSI from flag", args: []string{"--setup-csi=bogus"}, wantError: `unknown CSI driver "bogus"`},
		{name: "hostpath without Kind", body: "csi.setup: hostpath\n", wantError: "only supported for Kind"},
		{name: "relocated namespace", body: "namespace: relocated\n", wantError: "cannot be used with --namespace=relocated"},
		{name: "invalid source version", body: "build.version: invalid/version\n", wantError: "not a valid label value"},
		{name: "invalid image version", body: "images.repo: example.com/substrate\nimages.tag: invalid:tag\n", wantError: "not a valid label value"},
		{name: "missing provider", args: []string{"--credential-provider="}, wantError: "a credential provider is required"},
		{name: "incomplete GKE target", body: "gcp.projectID: project\n", wantError: "cluster name and location are missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "install.yaml")
			if err := os.WriteFile(path, []byte("apiVersion: install.ate.dev/v1alpha1\nkind: SubstrateInstall\n"+tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			commands := [][]string{{"validate"}}
			if tc.wantError != "" {
				commands = append(commands, []string{"deploy", "ate-system"})
			}
			var validationError string
			for _, command := range commands {
				t.Run(strings.Join(command, " "), func(t *testing.T) {
					isolateValidationCommand(t)
					args := append(append([]string{}, command...), "--config", path, "--kubeconfig=/does-not-exist", `--credential-provider={"enabled":false}`)
					err := runRoot(t, append(args, tc.args...)...)
					if tc.wantError == "" && err != nil {
						t.Fatalf("validate failed: %v", err)
					}
					if tc.wantError != "" {
						if err == nil || !strings.Contains(err.Error(), tc.wantError) {
							t.Fatalf("error = %v, want %s", err, tc.wantError)
						}
						if command[0] == "validate" {
							validationError = err.Error()
						} else if err.Error() != validationError {
							t.Errorf("deploy error = %q, validate error = %q", err, validationError)
						}
					}
					if env != nil || (command[0] == "validate" && resolved != nil) {
						t.Error("command initialized the cluster or validation recording")
					}
				})
			}
		})
	}
	if contents, err := os.ReadFile(toolLog); !os.IsNotExist(err) {
		t.Fatalf("validation invoked a cluster tool: %s, error: %v", contents, err)
	}
}
