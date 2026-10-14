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

	"github.com/spf13/cobra"
)

// An upgrade or a rollback moves the control plane one component at a time,
// so the components that only deploy ate-system used to install have their own
// subcommands.
func TestComponentDeploySubcommands(t *testing.T) {
	for _, tc := range []struct {
		arg  string
		want string
	}{
		{"podcertificate-controller", "podcertificate-controller"},
		{"podcert", "podcertificate-controller"},
		{"sandboxconfig", "sandboxconfig"},
	} {
		t.Run(tc.arg, func(t *testing.T) {
			sub, _, err := deployCmd.Find([]string{tc.arg})
			if err != nil {
				t.Fatalf("deploy %s: %v", tc.arg, err)
			}
			if sub.Name() != tc.want {
				t.Fatalf("deploy %s resolved to %q, want %q", tc.arg, sub.Name(), tc.want)
			}
			if err := sub.Args(sub, []string{"extra"}); err == nil {
				t.Errorf("deploy %s accepted a stray argument", tc.arg)
			}
		})
	}
}

func TestDeployReusesPreflightVersion(t *testing.T) {
	isolateValidationCommand(t)
	bin := t.TempDir()
	discoveries := filepath.Join(t.TempDir(), "discoveries")
	koArgs := filepath.Join(t.TempDir(), "ko-args")
	t.Setenv("DEPLOY_TEST_DISCOVERIES", discoveries)
	t.Setenv("DEPLOY_TEST_KO_ARGS", koArgs)
	t.Setenv("DEPLOY_TEST_KO_BINARY", filepath.Join(bin, "ko"))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for name, script := range map[string]string{
		"git": `#!/bin/sh
if [ -f "$DEPLOY_TEST_DISCOVERIES" ]; then echo v2.0.0; else echo v1.0.0; fi
echo discovery >> "$DEPLOY_TEST_DISCOVERIES"
`,
		"go": `#!/bin/sh
[ "$*" = "tool -n ko" ] || exit 1
echo "$DEPLOY_TEST_KO_BINARY"
`,
		"ko": `#!/bin/sh
printf '%s\n' "$@" > "$DEPLOY_TEST_KO_ARGS"
cat
`,
	} {
		if err := os.WriteFile(filepath.Join(bin, name), []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(kubeconfig, []byte(`apiVersion: v1
kind: Config
current-context: test
clusters:
- name: test
  cluster:
    server: https://127.0.0.1:1
contexts:
- name: test
  context:
    cluster: test
    user: test
users:
- name: test
  user:
    token: test
`), 0o600); err != nil {
		t.Fatal(err)
	}

	// Keep normal command initialization, then exercise the deployment's
	// preflight, ko and rendering consumers without applying cluster resources.
	run := deployAteSystemCmd.RunE
	t.Cleanup(func() { deployAteSystemCmd.RunE = run })
	deployAteSystemCmd.RunE = func(cmd *cobra.Command, _ []string) error {
		if env == nil || env.Kube == nil {
			t.Fatal("deployment execution did not receive initialized clients")
		}
		opts, err := deployOptions(env.Cfg)
		if err != nil {
			return err
		}
		if _, err := env.ValidateDeployAteSystem(opts); err != nil {
			return err
		}
		manifest, err := env.ResolveManifestBytes(cmd.Context(), []byte("version: ${SUBSTRATE_VERSION}\n"))
		if err != nil {
			return err
		}
		manifest, err = env.SubstituteVersion(manifest)
		if err != nil {
			return err
		}
		if got := string(manifest); got != "version: \"v1.0.0\"\n" {
			t.Errorf("rendered version = %q, want the version resolved in preflight", got)
		}
		return nil
	}
	if err := runRoot(t, "deploy", "ate-system", "--kubeconfig", kubeconfig,
		`--credential-provider={"enabled":false}`, "--record-dir", t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(discoveries); err != nil || string(got) != "discovery\n" {
		t.Errorf("version discovery = %q, error = %v; want exactly one call", got, err)
	}
	if got, err := os.ReadFile(koArgs); err != nil || !strings.Contains(string(got), ".Version=v1.0.0\n") {
		t.Errorf("ko args = %q, error = %v; want the version resolved in preflight", got, err)
	}
}
