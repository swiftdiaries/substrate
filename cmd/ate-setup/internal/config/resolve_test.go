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
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
)

// flagsWith returns a bound flag set with the named flags marked as supplied.
func flagsWith(t *testing.T, set map[string]string) *pflag.FlagSet {
	t.Helper()
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	BindFlags(fs)
	for name, val := range set {
		if err := fs.Set(name, val); err != nil {
			t.Fatalf("setting --%s=%s: %v", name, val, err)
		}
	}
	return fs
}

// fileWith returns a File carrying the given already-flattened keys, bypassing
// YAML so precedence is tested independently of parsing.
func fileWith(values map[string]string) *File {
	return &File{Path: "test.yaml", values: values}
}

// The precedence rule: environment < file < flag. The file outranking the
// environment reverses what the dotfile does today, so it is asserted directly
// rather than inferred.
func TestResolvePrecedence(t *testing.T) {
	useFixtures(t)
	const key, env, flag = "fixture.mode", "FIXTURE_MODE", "fixture-mode"

	for _, tc := range []struct {
		name     string
		env      map[string]string
		file     map[string]string
		flags    map[string]string
		wantRaw  string
		wantFrom Origin
	}{
		{
			name:    "nothing supplied falls back to the default",
			wantRaw: "alpha", wantFrom: OriginDefault,
		},
		{
			name:    "ordinary environment is ignored",
			env:     map[string]string{env: "beta"},
			wantRaw: "alpha", wantFrom: OriginDefault,
		},
		{
			name:    "file alone",
			file:    map[string]string{key: "beta"},
			wantRaw: "beta", wantFrom: OriginFile,
		},
		{
			name:    "flag alone",
			flags:   map[string]string{flag: "beta"},
			wantRaw: "beta", wantFrom: OriginFlag,
		},
		{
			name:    "file beats environment",
			env:     map[string]string{env: "beta"},
			file:    map[string]string{key: "alpha"},
			wantRaw: "alpha", wantFrom: OriginFile,
		},
		{
			name:    "flag beats file",
			file:    map[string]string{key: "alpha"},
			flags:   map[string]string{flag: "beta"},
			wantRaw: "beta", wantFrom: OriginFlag,
		},
		{
			name:    "flag beats environment and file together",
			env:     map[string]string{env: "alpha"},
			file:    map[string]string{key: "alpha"},
			flags:   map[string]string{flag: "beta"},
			wantRaw: "beta", wantFrom: OriginFlag,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var f *File
			if tc.file != nil {
				f = fileWith(tc.file)
			}
			r, err := Resolve(flagsWith(t, tc.flags), ResolveOptions{File: f, Env: tc.env})
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			v, ok := r.Value(key)
			if !ok {
				t.Fatalf("Value(%q) missing", key)
			}
			if v.Raw != tc.wantRaw {
				t.Errorf("Raw = %q, want %q", v.Raw, tc.wantRaw)
			}
			if v.From != tc.wantFrom {
				t.Errorf("From = %v, want %v", v.From, tc.wantFrom)
			}
		})
	}
}

func TestResolveSettingsAndSecretEnvironment(t *testing.T) {
	for _, tc := range []struct {
		name     string
		flags    map[string]string
		file     map[string]string
		env      map[string]string
		key      string
		wantRaw  string
		wantFrom Origin
	}{
		{name: "ordinary environment is ignored", env: map[string]string{"SCRATCH_VALUE": "ambient"}, key: "scratch.value", wantRaw: "default", wantFrom: OriginDefault},
		{name: "file beats default", file: map[string]string{"scratch.value": "file"}, key: "scratch.value", wantRaw: "file", wantFrom: OriginFile},
		{name: "flag beats file", file: map[string]string{"scratch.value": "file"}, flags: map[string]string{"scratch-value": "flag"}, key: "scratch.value", wantRaw: "flag", wantFrom: OriginFlag},
		{name: "explicit false remains supplied", flags: map[string]string{"scratch-enabled": "false"}, key: "scratch.enabled", wantRaw: "false", wantFrom: OriginFlag},
		{name: "explicit empty remains supplied", file: map[string]string{"scratch.empty": ""}, key: "scratch.empty", wantRaw: "", wantFrom: OriginFile},
		{name: "secret environment is retained", env: map[string]string{"SCRATCH_SECRET": "env-secret"}, key: "scratch.secret", wantRaw: "env-secret", wantFrom: OriginEnv},
		{name: "file beats secret environment", env: map[string]string{"SCRATCH_SECRET": "env-secret"}, file: map[string]string{"scratch.secret": "file-secret"}, key: "scratch.secret", wantRaw: "file-secret", wantFrom: OriginFile},
	} {
		t.Run(tc.name, func(t *testing.T) {
			useSettings(t,
				Setting{Key: "scratch.value", Env: "SCRATCH_VALUE", Flag: "scratch-value", Kind: KindString, Default: "default", Usage: "ordinary"},
				Setting{Key: "scratch.enabled", Env: "SCRATCH_ENABLED", Flag: "scratch-enabled", Kind: KindBool, Default: "true", Usage: "boolean"},
				Setting{Key: "scratch.empty", Env: "SCRATCH_EMPTY", Flag: "scratch-empty", Kind: KindString, Default: "fallback", Usage: "string"},
				Setting{Key: "scratch.secret", Env: "SCRATCH_SECRET", Kind: KindString, Secret: true, Usage: "secret"},
			)
			var f *File
			if tc.file != nil {
				f = fileWith(tc.file)
			}
			r, err := Resolve(flagsWith(t, tc.flags), ResolveOptions{File: f, Env: tc.env})
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			v, ok := r.Value(tc.key)
			if !ok {
				t.Fatalf("Value(%q) missing", tc.key)
			}
			if v.Raw != tc.wantRaw || v.From != tc.wantFrom || !v.Supplied && tc.wantFrom != OriginDefault {
				t.Errorf("Value(%q) = %#v, want raw %q from %v", tc.key, v, tc.wantRaw, tc.wantFrom)
			}
		})
	}
}

// An empty value a channel supplied is not the same as one nobody set. The
// Cloud SQL instance setting depends on the distinction.
func TestResolveSuppliedDistinguishesEmptyFromAbsent(t *testing.T) {
	useFixtures(t)
	const key, env = "fixture.name", "FIXTURE_NAME"

	for _, tc := range []struct {
		name         string
		flags        map[string]string
		wantSupplied bool
	}{
		{name: "absent", wantSupplied: false},
		{name: "present but empty", flags: map[string]string{"fixture-name": ""}, wantSupplied: true},
		{name: "present with a value", flags: map[string]string{"fixture-name": "p:r:i"}, wantSupplied: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := Resolve(flagsWith(t, tc.flags), ResolveOptions{})
			if err != nil {
				t.Fatalf("Resolve() error = %v", err)
			}
			if got := r.Supplied(key); got != tc.wantSupplied {
				t.Errorf("Supplied(%q) = %v, want %v", key, got, tc.wantSupplied)
			}
		})
	}
}

// A setting may have no declared default, whatever its kind. Resolving one
// that nothing supplied is not an error: it is the case Require exists to
// report, and only where the setting is actually needed. bind already accepts
// an empty default for these kinds.
func TestResolveAcceptsAnUnsetOptionalSetting(t *testing.T) {
	for _, kind := range []struct {
		name string
		kind ValueKind
	}{
		{name: "int", kind: KindInt},
		{name: "duration", kind: KindDuration},
		{name: "string", kind: KindString},
		{name: "bool", kind: KindBool},
	} {
		t.Run(kind.name, func(t *testing.T) {
			useSettings(t, Setting{
				Key: "scratch.optional", Env: "SCRATCH_OPTIONAL", Flag: "scratch-optional",
				Kind: kind.kind, Usage: "no declared default",
			})
			r, err := Resolve(nil, ResolveOptions{})
			if err != nil {
				t.Fatalf("Resolve() error = %v, want nil for an unset optional", err)
			}
			if r.Supplied("scratch.optional") {
				t.Error("Supplied() = true for a setting nothing supplied")
			}
		})
	}

	// Resolve walks the merged registry, so a setting owned by one command is
	// resolved for every command. An optional one must not fail the rest.
	t.Run("an optional setting owned by one command does not fail the others", func(t *testing.T) {
		useSettings(t,
			Setting{
				Key: "scratch.unrelated", Env: "SCRATCH_UNRELATED", Flag: "scratch-unrelated",
				Kind: KindString, Default: "fine", Usage: "owned by another command",
			},
			Setting{
				Key: "scratch.workers", Env: "SCRATCH_WORKERS", Flag: "scratch-workers",
				Kind: KindInt, Commands: []string{"deploy benchmarks"}, Usage: "optional",
			},
		)
		r, err := Resolve(nil, ResolveOptions{})
		if err != nil {
			t.Fatalf("Resolve() error = %v", err)
		}
		if got := r.String("scratch.unrelated"); got != "fine" {
			t.Errorf("String(scratch.unrelated) = %q, want %q", got, "fine")
		}
	})
}

// An explicitly empty string is a value: it is how an operator clears a
// setting, and how a setting is removed from an installation by writing its
// zero value and re-applying. Neither the declared default nor the channel
// changes that -- a value that is present is a value, and only an absent one
// falls through to the layer below.
func TestResolveHonorsAnExplicitlyEmptyString(t *testing.T) {
	const key, env, flag = "scratch.str", "SCRATCH_STR", "scratch-str"

	for _, decl := range []struct {
		name    string
		setting Setting
	}{
		{
			name: "no default",
			setting: Setting{Key: key, Env: env, Flag: flag, Kind: KindString,
				Usage: "a string"},
		},
		{
			name: "has a default",
			setting: Setting{Key: key, Env: env, Flag: flag, Kind: KindString,
				Default: "alpha", Usage: "a string"},
		},
	} {
		for _, ch := range []struct {
			name  string
			opts  ResolveOptions
			flags map[string]string
		}{
			{name: "file", opts: ResolveOptions{File: fileWith(map[string]string{key: ""})}},
			{name: "flag", flags: map[string]string{flag: ""}},
		} {
			t.Run(decl.name+"/"+ch.name, func(t *testing.T) {
				useSettings(t, decl.setting)
				r, err := Resolve(flagsWith(t, ch.flags), ch.opts)
				if err != nil {
					t.Fatalf("Resolve() error = %v", err)
				}
				if got := r.String(key); got != "" {
					t.Errorf("String(%q) = %q, want the empty value that was set", key, got)
				}
				if !r.Supplied(key) {
					t.Errorf("Supplied(%q) = false; an explicit empty value was supplied", key)
				}
			})
		}
	}
}

// An explicitly empty flag remains supplied even when a setting has a default.
func TestAddingADefaultDoesNotChangeWhatEmptyMeans(t *testing.T) {
	const key, flag = "scratch.str", "scratch-str"
	useSettings(t, Setting{Key: key, Flag: flag, Kind: KindString, Default: "alpha", Usage: "a string"})
	r, err := Resolve(flagsWith(t, map[string]string{flag: ""}), ResolveOptions{})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got := r.String(key); got != "" || !r.Supplied(key) {
		t.Errorf("explicit empty flag = %q, supplied %v; want empty and supplied", got, r.Supplied(key))
	}
}

func TestResolveTypedAccessors(t *testing.T) {
	useFixtures(t)
	r, err := Resolve(flagsWith(t, map[string]string{
		"fixture-enabled": "true", "fixture-count": "6", "fixture-timeout": "90s", "fixture-name": "custom",
	}), ResolveOptions{})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got := r.Bool("fixture.enabled"); !got {
		t.Errorf("Bool(fixture.enabled) = false, want true")
	}
	if got := r.Int("fixture.count"); got != 6 {
		t.Errorf("Int(fixture.count) = %d, want 6", got)
	}
	if got := r.Duration("fixture.timeout"); got != 90*time.Second {
		t.Errorf("Duration(fixture.timeout) = %v, want 90s", got)
	}
	if got := r.String("fixture.name"); got != "custom" {
		t.Errorf("String(fixture.name) = %q, want custom", got)
	}
}

// Defaults apply when no channel supplies a value.
func TestResolveDefaults(t *testing.T) {
	useFixtures(t)
	r, err := Resolve(flagsWith(t, nil), ResolveOptions{})
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	for _, tc := range []struct{ key, want string }{
		{"fixture.mode", "alpha"},
		{"fixture.count", "1"},
		{"fixture.enabled", "false"},
		{"fixture.timeout", "1m0s"},
	} {
		if got := r.String(tc.key); got != tc.want {
			t.Errorf("String(%q) = %q, want %q", tc.key, got, tc.want)
		}
		if r.Supplied(tc.key) {
			t.Errorf("Supplied(%q) = true for a defaulted setting", tc.key)
		}
	}
}

// A malformed value is rejected at resolve time, naming the channel it came
// from so the reader knows where to look.
func TestResolveRejectsMalformedValue(t *testing.T) {
	useFixtures(t)
	_, err := Resolve(flagsWith(t, map[string]string{"fixture-timeout": "ninety"}), ResolveOptions{})
	if err == nil {
		t.Fatal("Resolve() = nil error, want an error for a malformed duration")
	}
	var want = []string{"fixture.timeout", "--fixture-timeout", "ninety"}
	for _, w := range want {
		if !strings.Contains(err.Error(), w) {
			t.Errorf("error %q does not mention %q", err, w)
		}
	}
}

func TestResolveOriginsListsOnlySuppliedSettings(t *testing.T) {
	useFixtures(t)
	r, err := Resolve(
		flagsWith(t, map[string]string{"fixture-mode": "beta"}),
		ResolveOptions{},
	)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	got := map[string]Origin{}
	for _, v := range r.Origins() {
		got[v.Setting.Key] = v.From
	}
	if len(got) != 1 {
		t.Fatalf("Origins() = %v, want exactly the one supplied setting", got)
	}
	if got["fixture.mode"] != OriginFlag {
		t.Errorf("fixture.mode from %v, want flag", got["fixture.mode"])
	}
}

func TestOriginDescribeNamesTheEditableThing(t *testing.T) {
	useFixtures(t)
	s := setting(t, "fixture.secret")
	for _, tc := range []struct {
		origin Origin
		want   string
	}{
		{OriginEnv, "FIXTURE_SECRET"},
		{OriginFile, "config.fixture.secret"},
		{OriginFlag, "--fixture-secret"},
		{OriginDefault, "default"},
	} {
		if got := tc.origin.Describe(s); got != tc.want {
			t.Errorf("Describe(%v) = %q, want %q", tc.origin, got, tc.want)
		}
	}
}
