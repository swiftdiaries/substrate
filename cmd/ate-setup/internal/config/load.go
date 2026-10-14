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
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/pflag"

	"github.com/agent-substrate/substrate/cmd/ate-setup/internal/images"
	"github.com/agent-substrate/substrate/internal/installdefaults"
	"github.com/agent-substrate/substrate/internal/oidcdiscovery"
)

// LoadOptions carries the inputs that select where configuration comes from,
// as opposed to the settings themselves.
type LoadOptions struct {
	// ConfigPath is the --config document. Empty means no file layer.
	ConfigPath string
}

// RepoRoot walks up from the working directory to the module root. It works
// in a source tarball without a .git directory.
func RepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("while resolving the working directory: %w", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("no go.mod found above %s: run ate-setup from inside the repository", dir)
		}
		dir = parent
	}
}

// LoadFlags resolves the effective configuration from a bound flag set.
//
// Ordinary setting precedence, lowest to highest: default, configuration file,
// then flags. Secret environment values sit below the file.
func LoadFlags(fs *pflag.FlagSet, o LoadOptions) (*Config, error) {
	root, err := RepoRoot()
	if err != nil {
		return nil, err
	}

	env := environ()

	// The document is parsed before the Kind question is answered, because it
	// is one of the channels that answers it. Parsing reads only the file, so
	// nothing here depends on the environment it precedes.
	var file *File
	if path := o.ConfigPath; path != "" {
		if file, err = ParseFile(path); err != nil {
			return nil, err
		}
	}

	res, err := Resolve(fs, ResolveOptions{File: file, Env: env})
	if err != nil {
		return nil, err
	}
	return buildConfig(root, env, res)
}

// buildConfig projects the resolved settings onto Config.
func buildConfig(root string, env map[string]string, r *Resolved) (*Config, error) {
	// Before the projection, so Config, the report and the record all describe
	// one install rather than Config describing a different one.
	if r.Bool(kindEnabledKey) {
		applyKindDefaults(r)
	}

	rolloutTimeout := r.Duration("rolloutTimeout")
	if r.Supplied("rolloutTimeout") && rolloutTimeout <= 0 {
		// kubectl reads --timeout=0 as "wait forever"; ate-setup does not offer
		// an unbounded wait, so say so rather than silently meaning the opposite.
		v, _ := r.Value("rolloutTimeout")
		return nil, &InvalidError{Value: v, Want: "a positive duration"}
	}

	podcertWorkers := r.Int("podcert.workersPerSigner")
	if r.Supplied("podcert.workersPerSigner") && podcertWorkers < 1 {
		v, _ := r.Value("podcert.workersPerSigner")
		return nil, &InvalidError{Value: v, Want: "a positive integer"}
	}

	kubeconfig, kubeconfigEnv := splitKubeconfig(r, env)

	// One configured connection serves both pools; both empty selects the
	// bundled PostgreSQL.
	readWriteConnectionString := r.String("ateapi.postgres.readWrite.connectionString")
	ownerConnectionString := r.String("ateapi.postgres.owner.connectionString")
	if ownerConnectionString == "" {
		ownerConnectionString = readWriteConnectionString
	}
	instance, _ := r.Value("ateapi.postgres.cloudsql.instance")
	actorJWTIssuer := r.String("actorJWT.issuer")
	if actorJWTIssuer == "" {
		actorJWTIssuer = installdefaults.ActorJWTIssuer(r.String("namespace"))
	}

	cfg := &Config{
		Root:               root,
		Kind:               r.Bool("kindCluster.enabled"),
		Namespace:          r.String("namespace"),
		Kubeconfig:         kubeconfig,
		Context:            r.String("context"),
		ProjectID:          r.String("gcp.projectID"),
		ClusterName:        r.String("gcp.clusterName"),
		ClusterLocation:    r.String("gcp.clusterLocation"),
		ExpectedJWTIssuer:  r.String("ateapi.expectedJWTIssuer"),
		BucketName:         r.String("storage.bucketName"),
		KODockerRepo:       r.String("ko.dockerRepo"),
		KODefaultPlatforms: r.String("ko.defaultPlatforms"),
		BuildVersion:       r.String("build.version"),
		ActorJWTAlgorithm:  r.String("actorJWT.algorithm"),
		ActorJWTIssuer:     actorJWTIssuer,
		DockerBuildFlags:   strings.Fields(r.String("docker.buildFlags")),
		Images: images.Source{
			Repo: strings.TrimSuffix(r.String("images.repo"), "/"),
			Tag:  r.String("images.tag"),
		},
		Router:               r.String("atenet.dataplane"),
		PostgresSchema:       r.String("ateapi.postgres.schema"),
		PostgresPoolMaxConns: r.String("ateapi.postgres.poolMaxConns"),
		PostgresServerCAFile: r.String("ateapi.postgres.serverCAFile"),
		PostgresStorageClass: r.String("ateapi.postgres.storageClass"),

		PostgresReadWriteConnectionString: readWriteConnectionString,
		PostgresOwnerConnectionString:     ownerConnectionString,
		PostgresReadWriteRole:             r.String("ateapi.postgres.readWrite.role"),
		PostgresOwnerRole:                 r.String("ateapi.postgres.owner.role"),
		// Whether a role was chosen rather than defaulted. Taken from the
		// resolver, which knows whether the file or flag supplied the value.
		PostgresReadWriteRoleSet: r.Supplied("ateapi.postgres.readWrite.role"),
		PostgresOwnerRoleSet:     r.Supplied("ateapi.postgres.owner.role"),
		CloudSQL: CloudSQLConfig{
			Instance: instance.Raw,
			// A file or flag can explicitly clear Cloud SQL.
			InstanceSet: instance.Supplied,
			GSA:         r.String("ateapi.postgres.cloudsql.gsa"),
			IAMAuth:     r.String("ateapi.postgres.cloudsql.iamAuth"),
			IPType:      r.String("ateapi.postgres.cloudsql.ipType"),
		},
		RolloutTimeout:                 rolloutTimeout,
		rolloutTimeoutSet:              r.Supplied("rolloutTimeout"),
		PodcertWorkersPerSigner:        podcertWorkers,
		ClusterSize:                    r.String("clusterSize"),
		CordonControlPlane:             r.Bool("cordonControlPlane"),
		AdditionalEgressExtprocService: r.String("atenet.egress.additionalExtprocService"),
		CredentialProviderJSON:         r.String("atenet.egress.credentialProvider"),
		AnthropicAPIKey:                r.String("demo.anthropicAPIKey"),
		OtlpEndpoint:                   r.String("otlpEndpoint"),
		BenchmarkActorMemory:           r.String("benchmark.actorMemory"),
		kubeconfigEnv:                  kubeconfigEnv,
		shellEnv:                       env,
		resolved:                       r,
	}

	if err := validateResolved(cfg, r); err != nil {
		return nil, err
	}
	return cfg, nil
}

// splitKubeconfig divides the setting into the path handed to client-go and the
// value exported to the shell scripts.
//
// $KUBECONFIG is a PATH-style list, and developers who juggle clusters do set it
// to several files. Ambient lists stay with client-go's default loading rules;
// explicit lists are passed through and interpreted as a precedence list by
// ateclient.LoadKubeConfig.
func splitKubeconfig(r *Resolved, env map[string]string) (explicitPath, scriptValue string) {
	v, _ := r.Value("kubeconfig")
	if v.From == OriginFlag || v.From == OriginFile {
		return v.Raw, v.Raw
	}
	ambient := env["KUBECONFIG"]
	if strings.ContainsRune(ambient, os.PathListSeparator) {
		return "", ambient
	}
	return ambient, ambient
}

// applyKindDefaults fills the Kind-only settings, leaving alone any a channel
// actually supplied.
//
// The values are written into the resolved set rather than onto Config, so
// that the configuration report and the install record name what the install
// uses. Writing them onto Config alone would report "" for four settings the
// run is about to apply, and leave a Kind record holding nothing but
// kindCluster.enabled -- which replays to whatever the defaults happen to be
// on the next machine and the next release, not to the install it recorded.
func applyKindDefaults(r *Resolved) {
	// A Kind cluster has no GCP project, so the coordinates are cleared even
	// when a file or flag named them. Only when something did name
	// them: clearing an already-empty setting would report a value as set by
	// the profile when the profile changed nothing.
	for _, key := range []string{"gcp.projectID", "gcp.clusterLocation"} {
		if r.String(key) != "" {
			r.set(key, "", OriginKindProfile)
		}
	}

	if r.String("context") == "" {
		r.set("context", "kind-"+r.String("kindCluster.name"), OriginKindProfile)
	}
	if r.String("ko.dockerRepo") == "" {
		r.set("ko.dockerRepo", "localhost:5001", OriginKindProfile)
	}
	// Ambient environment is ignored: a caller must choose GKE and GCS targets
	// through a flag or configuration file.
	if !namedExplicitly(r, "ko.defaultPlatforms") {
		r.set("ko.defaultPlatforms", "linux/"+runtime.GOARCH, OriginKindProfile)
	}
	if !namedExplicitly(r, "storage.bucketName") {
		r.set("storage.bucketName", "ate-snapshots", OriginKindProfile)
	}
}

// namedExplicitly reports whether a flag or configuration file supplied the
// setting, as opposed to it holding its default value.
func namedExplicitly(r *Resolved, key string) bool {
	v, ok := r.Value(key)
	return ok && (v.From == OriginFlag || v.From == OriginFile)
}

// validateResolved rejects invalid and conflicting settings, naming the channel
// each value came from so the reader knows which one to edit.
func validateResolved(cfg *Config, r *Resolved) error {
	// The repo/tag pairing is checked here rather than in images.Source.Validate
	// because only here is the channel that supplied each half known. A tag on
	// its own would otherwise be discarded in silence, leaving a build from
	// source that looks like the release the tag names.
	repo, _ := r.Value("images.repo")
	tag, _ := r.Value("images.tag")
	switch {
	case cfg.Images.IsPrebuilt() && cfg.Images.Tag == "":
		return &ConflictError{A: repo, B: tag, Why: "a prebuilt image repository needs a tag"}
	case !cfg.Images.IsPrebuilt() && cfg.Images.Tag != "":
		return &ConflictError{A: tag, B: repo,
			Why: "without a repository, images are built from source and the tag names nothing"}
	}
	if cfg.Images.IsPrebuilt() && r.Supplied("build.version") {
		v, _ := r.Value("build.version")
		return &InvalidError{Value: v, Want: "unset when installing prebuilt images; the image tag supplies their version"}
	}

	dataplane, _ := r.Value("atenet.dataplane")
	switch cfg.Router {
	case RouterEnvoy, RouterAgentgateway:
	default:
		return &InvalidError{Value: dataplane, Want: RouterEnvoy + " or " + RouterAgentgateway}
	}

	if ipType, _ := r.Value("ateapi.postgres.cloudsql.ipType"); ipType.Raw != "" {
		switch ipType.Raw {
		case CloudSQLIPTypePrivate, CloudSQLIPTypePublic, CloudSQLIPTypePSC:
		default:
			return &InvalidError{Value: ipType, Want: strings.Join(
				[]string{CloudSQLIPTypePrivate, CloudSQLIPTypePublic, CloudSQLIPTypePSC}, ", ")}
		}
	}

	if size, _ := r.Value("clusterSize"); cfg.ClusterSize != ClusterSizeSize0 && cfg.ClusterSize != ClusterSizeSize10 {
		return &InvalidError{Value: size, Want: ClusterSizeSize0 + " or " + ClusterSizeSize10}
	}

	switch cfg.ActorJWTAlgorithm {
	case "ES256", "RS256":
	default:
		alg, _ := r.Value("actorJWT.algorithm")
		return &InvalidError{Value: alg, Want: "ES256 or RS256"}
	}
	if err := oidcdiscovery.ValidateIssuer(cfg.ActorJWTIssuer); err != nil {
		issuer, _ := r.Value("actorJWT.issuer")
		return fmt.Errorf("%s: %w (from %s)", issuer.Setting.Key, err, issuer.From.Describe(issuer.Setting))
	}
	if cfg.AdditionalEgressExtprocService != "" {
		extproc, _ := r.Value("atenet.egress.additionalExtprocService")
		if err := validateExtprocService(extproc); err != nil {
			return err
		}
		if cfg.Router != RouterEnvoy {
			return &ConflictError{A: extproc, B: dataplane,
				Why: "an additional ext_proc filter requires dataplane " + RouterEnvoy}
		}
	}
	// Parsed here so a malformed provider is rejected with the rest of the
	// configuration rather than part-way through a deploy. Whether one is
	// required at all is the deploy's question, not this one's.
	if cfg.CredentialProviderJSON != "" {
		if _, err := cfg.CredentialProvider(); err != nil {
			// The parser's own message says which part is wrong and what to
			// write instead; only the channel is added. Replacing it with the
			// generic "must be <usage>" would drop the reason.
			provider, _ := r.Value("atenet.egress.credentialProvider")
			return fmt.Errorf("%w (from %s)", err, provider.From.Describe(provider.Setting))
		}
	}
	return nil
}
