# Installing Agent Substrate as an operator

Ordinary installer settings come from defaults, a configuration file, or flags.
Only the read/write DSN, owner DSN, and Anthropic API key accept environment
input; a file overrides those secrets. This page describes the configuration
file, which operators can review and reuse across runs.

## The document

```yaml
apiVersion: install.ate.dev/v1alpha1
kind: SubstrateInstall

namespace: ate-system
atenet:
  dataplane: envoy
images:
  repo: ghcr.io/agent-substrate/substrate
  tag: v0.4.1
```

Pass it with `--config`:

```bash
ate-setup deploy ate-system --config substrate-install.yaml
hack/install-ate.sh --config substrate-install.yaml --deploy-ate-system
```

The shell installer forwards `--config` to each action.

A worked example is at [docs/examples/substrate-install.yaml](examples/substrate-install.yaml).

### Keys are nested; the table below is flat

A key written `ateapi.postgres.schema` is nested in the document:

```yaml
ateapi:
  postgres:
    schema: public
```

An unrecognized key is rejected rather than ignored, naming the file:

```
Error: install.yaml: unknown setting "ateapi.postgres.schemas"
```

A typo would otherwise leave the install running on the default with nothing
on screen to say so.

## Precedence

Lowest to highest:

```
default  <  configuration file  <  flag
```

Flags override file values because they were supplied for this run. The three
secret inputs are the exception: environment values are accepted, and a secret
value in the file overrides its environment value.

A channel that does not mention a setting leaves the one below it in place, so
a document need only name what differs from the defaults.

## Empty values

A setting a channel mentions is set, whatever it is set to. An empty value is
a value:

```yaml
namespace: ""
```

```bash
ate-setup deploy ate-system --namespace=
```

Both clear the setting rather than falling through to the default. That
is how a setting is removed from an installation: write its zero value and
re-apply.

Only an *absent* setting falls through. Ordinary settings have no environment
channel; empty file values and explicit empty flags remain supplied values.

## Every run reports what it resolved

Every setting is listed with its source:

```
configuration: 6 set, 35 default
  ateapi.expectedJWTIssuer                    ""            (from default)
  ateapi.postgres.readWrite.connectionString  <set>         (from ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING)
  ...
  atenet.dataplane                            envoy         (from default)
  benchmark.sandboxClass                      microvm       (from --sandbox-class)
  benchmark.workerCount                       7             (from config.benchmark.workerCount)
  clusterSize                                 size0         (from default)
  context                                     kind-kind     (from the --kind profile)
  namespace                                   ate-prod      (from config.namespace)
```

The report shows which file or flag supplied each setting and makes defaults
visible when they affect the install.

A setting marked secret in the table below prints as `<set>` when something
supplied it, with the channel still named so you know where to change it. One
nothing supplied prints its default, which is empty: no credential appears
either way.

To list only what was set:

```bash
ate-setup deploy ate-system --no-report-defaults
```

```
configuration: 6 set, 35 default
  ateapi.postgres.readWrite.connectionString  <set>         (from ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING)
  atenet.dataplane                            envoy         (from default)
  benchmark.sandboxClass                      microvm       (from --sandbox-class)
  benchmark.workerCount                       7             (from config.benchmark.workerCount)
  context                                     kind-kind     (from the --kind profile)
  namespace                                   ate-prod      (from config.namespace)
```

## Errors name the source

```
Error: atenet.dataplane must be envoy or agentgateway, got "nginx" (from config.atenet.dataplane)
Error: at least one of ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING or
       config.ateapi.postgres.readWrite.connectionString must be set, got none
```

## Command-scoped settings

Most settings apply to every command and are flags on the root. A few belong
to one command and are flags only there; the configuration file can describe
a whole install:

```yaml
csi:
  setup: nfs
benchmark:
  workerCount: 4
```

## Install runs record what they used

A `deploy` command that installs Substrate writes the settings it resolved to
a file, so a later run can reproduce it without anyone remembering what was
typed. `deploy demo` and `deploy benchmarks` write nothing: they deploy on top
of the system with only their own settings.

```
recorded this configuration in ~/.cache/ate-setup/installs/prod.yaml
```

Records go under the user cache directory, or wherever `record.dir` points.
Nothing reads them on its own -- a record only takes effect when you pass it
to `--config`.

| Outcome   | Path                                             | Retention                                                  |
|-----------|--------------------------------------------------|------------------------------------------------------------|
| Succeeded | `<record.dir>/installs/<context>.yaml`           | replaced each run: a cluster has one current configuration |
| Failed    | `<record.dir>/failed/<context>-<timestamp>.yaml` | kept: each attempt is its own artifact                     |

### Retrying a failed install

A run that fails prints the record it wrote and the command that replays it:

```
Error: while reading kubeconfig: context "prod" does not exist
the settings this run used were written to ~/.cache/ate-setup/failed/prod-20260929T231438Z.yaml
retry with: ate-setup deploy atenet --config ~/.cache/ate-setup/failed/prod-20260929T231438Z.yaml
1 secret setting(s) were not written and must be supplied again: ateapi.postgres.readWrite.connectionString
```

The record is ordinary `--config` input. Nothing has to be stripped from it
first: the `cluster:` block holding the timestamp and outcome is skipped when
the file is read back.

**A retry is not a complete replay.** Secrets are never written, so the last
line above lists what to supply again:

```bash
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING='postgres://...'
ate-setup deploy atenet --config ~/.cache/ate-setup/failed/prod-20260929T231438Z.yaml
```

A failed run changes nothing about later runs. Its record is inert until you
name it -- a value that caused the failure must not become the default that
reaches the cluster on the next success.

### Reusing a successful install

The same applies to the success record, which is the one to keep:

```bash
# Rebuild a cluster the way it was, or set up a second one to match.
ate-setup deploy ate-system --config ~/.cache/ate-setup/installs/prod.yaml
```

Point `record.dir` at a checkout to keep these under version control, which is
how several operators share one configuration and how an install survives a
cluster that no longer exists:

```bash
ate-setup deploy ate-system --record-dir ~/ops/substrate-installs --config substrate-install.yaml
```

Records hold no credentials, so they are safe to commit. They are written
readable only by you regardless.

### What a record looks like

```yaml
apiVersion: install.ate.dev/v1alpha1
kind: SubstrateInstall
cluster:
  context: prod
  outcome: succeeded
  substrateVersion: v0.4.1
  writtenAt: "2026-09-29T23:14:38Z"
  writtenBy: ate-setup v0.4.1
atenet:
  dataplane: agentgateway
context: prod
namespace: ate-prod
```

`cluster.substrateVersion` is the Substrate release the run installed: the
version stamped into the binaries it built, or the image tag of a prebuilt
install. `cluster.writtenBy` is the `ate-setup` that wrote the file.

Only settings something supplied are recorded. A setting left on its default
is left out on purpose, so it keeps following the default if a later release
changes it.

## Settings

Defaults are also shown by `ate-setup --help` and by `--help` on the owning
command. A setting with no default is unset unless something supplies it.

| Key                                          | Environment input / tool convention                            | Flag                                               | Default               | Applies to                               |
|----------------------------------------------|-------------------------------------------------|----------------------------------------------------|-----------------------|------------------------------------------|
| `actorJWT.algorithm` | — | `--actor-jwt-algorithm` | `ES256` | (global) |
| `actorJWT.issuer` | — | `--actor-jwt-issuer` | — | (global) |
| `ateapi.expectedJWTIssuer` | — | `--ateapi-expected-jwt-issuer` | — | (global) |
| `ateapi.postgres.cloudsql.gsa` | — | `--ateapi-postgres-cloudsql-gsa` | — | (global) |
| `ateapi.postgres.cloudsql.iamAuth` | — | `--ateapi-postgres-cloudsql-iam-auth` | — | (global) |
| `ateapi.postgres.cloudsql.instance` | — | `--ateapi-postgres-cloudsql-instance` | — | (global) |
| `ateapi.postgres.cloudsql.ipType` | — | `--ateapi-postgres-cloudsql-ip-type` | — | (global) |
| `ateapi.postgres.owner.connectionString` | `ATE_API_POSTGRES_OWNER_CONNECTION_STRING` | — (no flag) | — (secret) | (global) |
| `ateapi.postgres.owner.role` | — | `--ateapi-postgres-owner-role` | `substrate_owner` | (global) |
| `ateapi.postgres.poolMaxConns` | — | `--ateapi-postgres-pool-max-conns` | — | (global) |
| `ateapi.postgres.readWrite.connectionString` | `ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING` | — (no flag) | — (secret) | (global) |
| `ateapi.postgres.readWrite.role` | — | `--ateapi-postgres-read-write-role` | `substrate_readwrite` | (global) |
| `ateapi.postgres.schema` | — | `--ateapi-postgres-schema` | — | (global) |
| `ateapi.postgres.serverCAFile` | — | `--ateapi-postgres-server-ca-file` | — | (global) |
| `ateapi.postgres.storageClass` | — | `--ateapi-postgres-storage-class` | — | (global) |
| `atenet.dataplane` | — | `--atenet-dataplane` | `envoy` | (global) |
| `atenet.egress.additionalExtprocService` | — | `--experimental-additional-egress-extproc-service` | — | (global) |
| `atenet.egress.credentialProvider` | — | `--credential-provider` | — | (global) |
| `benchmark.actorMemory` | — | `--benchmark-actor-memory` | — | (global) |
| `benchmark.sandboxClass` | — | `--sandbox-class` | `gvisor` | `deploy benchmarks`, `delete benchmarks` |
| `benchmark.workerCount` | — | `--worker-count` | `1` | `deploy benchmarks`, `delete benchmarks` |
| `clusterSize` | — | `--cluster-size` | `size0` | (global) |
| `context` | — | `--context` | — | (global) |
| `cordonControlPlane` | — | `--cordon-control-plane` | `false` | (global) |
| `csi.setup` | — | `--setup-csi` | `none` | `deploy ate-system` |
| `build.version` | — | `--build-version` | — | source builds only |
| `demo.anthropicAPIKey` | `ANTHROPIC_API_KEY` | — (no flag) | — (secret) | (global) |
| `demo.counter.storageClass` | — | `--storage-class` | `standard` | `deploy demo counter` |
| `demo.counter.withExternalVolume` | — | `--with-external-volume` | `false` | `deploy demo counter` |
| `docker.buildFlags` | — | `--docker-build-flags` | — | (global) |
| `gcp.clusterLocation` | — | `--gcp-cluster-location` | — | (global) |
| `gcp.clusterName` | — | `--gcp-cluster-name` | — | (global) |
| `gcp.projectID` | — | `--gcp-project-id` | — | (global) |
| `images.repo` | — | `--image-repo` | — | (global) |
| `images.tag` | — | `--image-tag` | — | (global) |
| `kindCluster.enabled` | — | `--kind` | `false` | (global) |
| `kindCluster.name` | — | `--kind-cluster-name` | `kind` | (global) |
| `ko.defaultPlatforms` | — | `--ko-default-platforms` | — | (global) |
| `ko.dockerRepo` | — | `--ko-docker-repo` | — | (global) |
| `kubeconfig` | `KUBECONFIG` (standard client-go convention) | `--kubeconfig` | — | (global) |
| `namespace` | — | `--namespace` | `ate-system` | (global) |
| `otlpEndpoint` | — | `--otlp-endpoint` | — | (global) |
| `podcert.workersPerSigner` | — | `--podcert-workers-per-signer` | `0` | (global) |
| `record.dir` | — | `--record-dir` | — | (global) |
| `rolloutTimeout` | — | `--rollout-timeout` | `1m0s` | (global) |
| `storage.bucketName` | — | `--storage-bucket-name` | — | (global) |
Source builds use `build.version` / `--build-version` when supplied, otherwise
the checkout version. Prebuilt installs derive the version from `--image-tag`;
`--build-version` is rejected with `--image-repo`.

Ambient `KUBECONFIG` follows client-go loading behavior. `--kubeconfig` or the
file setting takes precedence.

## Secrets

The three secret inputs can come from the environment or the configuration
file. The file overrides the environment, so protect any file containing a
credential:

```bash
export ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING='postgres://...'
ate-setup deploy ate-system --config substrate-install.yaml
```

These settings have no flag. A command line is visible in `ps` to every user
on the host and is written to shell history, so supply credentials through the
environment or a file with restricted permissions.

The run report prints `<set>` for these rather than the value.
