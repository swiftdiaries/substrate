#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Installs Agent Substrate.
#
# This is a translation shim: cmd/ate-setup carries out every action, and this
# script maps its action flags onto commands. Ordinary settings come from
# --config or explicit flags; only secret inputs and native tool credentials
# remain in the environment.
#
# New work belongs in cmd/ate-setup. See cmd/ate-setup/commands.md for the
# equivalent invocation of each flag below, and cmd/ate-setup/cli-diff.md for
# the behaviors that differ after the move.
#
# KUBECONFIG and native cloud credentials keep their tool-defined behavior.
# PostgreSQL DSNs and ANTHROPIC_API_KEY are the only installer settings read
# from the environment; ordinary settings use --config or flags.

set -o errexit -o nounset -o pipefail

CALLER_DIR="$PWD"

caller_path() {
  local path="$1"
  if [[ -z "${path}" || "${path}" = /* ]]; then
    printf '%s' "${path}"
  else
    printf '%s/%s' "${CALLER_DIR}" "${path}"
  fi
}

# Keep kubeconfig paths relative to the directory that invoked the shim even
# though the shim runs ate-setup from the repository root. Empty list entries
# and absolute paths keep client-go's native KUBECONFIG semantics.
# Internal automation mode: parse all options, suppress actions, then validate.
VALIDATE_CONFIG=0
if [[ "${1:-}" == --validate-config ]]; then
  VALIDATE_CONFIG=1
  shift
fi
caller_kubeconfig() {
  local remaining="$1" entry result="" separator=""
  while :; do
    entry="${remaining%%:*}"
    [[ -z "${entry}" || "${entry}" = /* ]] || entry="${CALLER_DIR}/${entry}"
    result+="${separator}${entry}"
    [[ "${remaining}" == *:* ]] || break
    remaining="${remaining#*:}"
    separator=":"
  done
  printf '%s' "${result}"
}
if [[ -n "${KUBECONFIG:-}" ]]; then
  KUBECONFIG="$(caller_kubeconfig "${KUBECONFIG}")"
  export KUBECONFIG
fi

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Demos, in the order --help lists them. Keeping the list here rather than
# asking ate-setup for it means --help answers without a build, and that a demo
# flag this script does not know is rejected outright rather than handed to
# ate-setup as an unknown subcommand. `go run ./cmd/ate-setup deploy demo
# --help` is the authoritative list; add new demos there and mirror them here.
ATE_DEMOS=(
  demo-counter
  demo-counter-microvm
  demo-egress
  demo-egress-microvm
  demo-jupyter
  demo-sandbox
  demo-claude-code-multiplex
  demo-multi-template
  demo-parking
  demo-autoscaled-workerpool
)

# Prerequisites and caveats worth repeating in --help, keyed by demo.
demo_usage() {
  case "$1" in
    demo-counter)
      echo "  --deploy-demo-counter-with-external-volume    Deploy demo-counter with external volume validation"
      echo "  --with-external-volume                        Attach the volume when using the ate-setup command"
      echo "  --storage-class NAME                          StorageClass backing the external volume"
      ;;
    demo-counter-microvm|demo-egress-microvm)
      echo "  Needs hack/install-microvm-deps.sh --install to have run (cluster-wide microvm SandboxConfig)."
      ;;
    demo-claude-code-multiplex)
      echo "  Required env: ANTHROPIC_API_KEY; configure --storage-bucket-name and --ko-docker-repo"
      echo "  See demos/claude-code-multiplex/README.md for the walkthrough."
      ;;
    demo-autoscaled-workerpool)
      echo "  Kind only: it ships its own prometheus-adapter and a Kind-specific HPA."
      ;;
  esac
}

usage() {
  echo "Usage: $0 [options]"
  echo ""
  echo "Configuration:"
  echo ""
  echo "  --config PATH                          Read settings from a configuration document; explicit flags override it."
  echo "                                         Relative paths are resolved from the directory that called this script."
  echo "                                         See docs/operator-install.md."
  echo ""
  echo "  --no-report-defaults                   List only settings supplied by the file or flags."
  echo "  --kind                                 Apply the local Kind defaults."
  echo "  --docker-build-flags FLAGS             Pass build flags to the Docker-built dataplane image."
  echo ""
  echo "Overall infrastructure (all infrastructure components):"
  echo ""
  echo "  --deploy-ate-system                    Deploy core system (CRDs, atelet, apiserver)"
  echo "  --setup-microvm                         Stage microVM assets and install the cluster-wide SandboxConfig"
  echo "  --setup-csi[=DRIVER]                   Setup CSI driver: nfs, hostpath, both, none (default: none;"
  echo "                                         a bare --setup-csi means nfs; hostpath is Kind only)"
  echo "  --delete-ate-system                    Delete core system"
  echo "  --delete-all                           Delete core system and all registered demos"
  echo "  --atenet-dataplane=envoy|agentgateway  Select the atenet ingress and egress dataplane (default: envoy)"
  echo "  --podcert-workers-per-signer N         Concurrent workers per podcertificate-controller signer (default: 1)"
  echo "  --cluster-size size0|size10            Cluster size profile (default: size0). \"size10\" assumes a dedicated postgres node"
  echo "  --cordon-control-plane                 Keep the control plane off the worker nodes: assumes a small shared pool labeled and"
  echo "                                         tainted ate.dev/workloadType=ate-control-plane:NoSchedule, and a one-node pool"
  echo "                                         labeled and tainted ate.dev/workloadType=ate-postgres:NoSchedule for postgres alone"
  echo "  --rollout-timeout DURATION             Per-workload readiness wait timeout, kubectl-style Go duration (default: 60s)"
  echo "  --otlp-endpoint URL                    Send all control plane telemetry to URL, not to the cluster default (see benchmarking/telemetry/README.md)"
  echo ""
  echo "Experiments:"
  echo ""
  echo "  --experimental-additional-egress-extproc-service NS/SVC:PORT"
  echo "                                         Run an additional ext_proc authorization filter, served by that Service."
  echo "                                         Requires --atenet-dataplane=envoy."
  echo ""
  echo "Egress credential injection (required by --deploy-ate-system and --deploy-atenet):"
  echo ""
  echo "  --credential-provider JSON             Credential provider the egress gateway injects credentials from. One of:"
  echo "                                           '{\"name\":\"k8s.io\"}'  deploy the bundled Kubernetes Secrets provider,"
  echo "                                                                with a NetworkPolicy that admits only the egress gateway"
  echo "                                           '{\"enabled\":false}'  turn injection off"
  echo "                                           '{\"name\":\"<provider>\",\"address\":\"<host>:<port>\"}'"
  echo "                                                                use a provider you deploy yourself"
  echo "                                         Any provider requires --atenet-dataplane=envoy."
  echo ""
  echo "Infrastructure components:"
  echo ""
  echo "  --deploy-atelet                        Deploy atelet only"
  echo "  --deploy-ate-apiserver                 Deploy ate-api-server only"
  echo "  --deploy-atenet                        Deploy atenet only"
  echo "  --delete-atenet                        Delete atenet only"
  echo ""
  echo "To create individual resources used by ate-system (Note: These are"
  echo "called automatically by --deploy-ate-system):"
  echo ""
  echo "  --create-jwt-authority-pool-secret     Create JWT authority pool secret"
  echo "  --create-actor-id-ca-pool-secret       Create actor ID CA pool secret"
  echo "  --create-actor-id-ca-certs-secret      Create actor ID CA certs secret"
  echo "  --create-egress-mitm-ca-pool-secret    Create egress MITM CA pool secret"
  echo "  --create-podcertificate-controller-cas Create podcertificate controller CAs"
  echo "  --create-api-server-env-vars           Create ate-api-server env vars"
  echo "  --create-api-authentication-config     Create the default ate-api-server authentication config"
  echo ""
  echo "PostgreSQL configuration (a connection DSN or Cloud SQL instance selects"
  echo "an external database and skips the bundled instance):"
  echo ""
  echo "  ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING  Secret application DSN for external PostgreSQL (stored in a Secret;"
  echo "                                         pair with --ateapi-postgres-server-ca-file for sslmode=verify-ca)"
  echo "  ATE_API_POSTGRES_OWNER_CONNECTION_STRING       Optional secret owner DSN; defaults to the read/write DSN"
  echo "  --ateapi-postgres-read-write-role ROLE         Role assumed by the read/write pool (default: substrate_readwrite)"
  echo "  --ateapi-postgres-owner-role ROLE              Role assumed by the owner pool (default: substrate_owner)"
  echo "  --ateapi-postgres-cloudsql-instance NAME       Cloud SQL instance connection name (project:region:instance)."
  echo "                                         Deploys the Cloud SQL Auth Proxy sidecar: connector-managed TLS"
  echo "                                         and automatic IAM database auth, no passwords (see tools/setup-gcp/cloud-sql.md)."
  echo "                                         Unset = keep the cluster's current Cloud SQL config; set to \"\" to remove it"
  echo "  --ateapi-postgres-cloudsql-gsa EMAIL           GSA email backing Workload Identity + the IAM database user"
  echo "  --ateapi-postgres-cloudsql-ip-type TYPE        private (default) | public | psc"
  echo "  --ateapi-postgres-cloudsql-iam-auth BOOL       true (default) | false (password-over-proxy escape hatch)"
  echo "  --ateapi-postgres-pool-max-conns N             pgxpool max connections per ateapi replica (default: max(4, NumCPU))"
  echo "  --ateapi-postgres-server-ca-file PATH          PEM file to mount for verify-ca DSNs (non-Cloud-SQL databases)"
  echo "  --ateapi-postgres-schema NAME                  Select the Substrate schema (default: substrate)"
  echo ""
  echo "Authentication configuration:"
  echo ""
  echo "  --ateapi-expected-jwt-issuer URL       Issuer URL ate-api-server requires in service account tokens, verbatim."
  echo "                                         Default: derived from PROJECT_ID/CLUSTER_LOCATION/CLUSTER_NAME"
  echo "                                         (https://container.googleapis.com/v1/projects/.../clusters/...),"
  echo "                                         else the cluster's OIDC discovery document"
  echo "  --actor-jwt-algorithm ALGORITHM        Signing algorithm of a newly created actor JWT pool: ES256 (default) | RS256."
  echo "                                         Use RS256 (a 4096-bit RSA key) for relying parties that don't support ES256. Has no effect once the pool exists."
  echo "  --actor-jwt-issuer URL                  iss claim of actor JWTs, verbatim. Relying parties fetch <issuer>/.well-known/openid-configuration."
  echo "                                         Default: https://idp.<namespace>.svc"
  echo ""
  echo "Benchmarks (see benchmarking/README.md for details and customization):"
  echo ""
  echo "  --deploy-benchmarks                    Deploy workloads + locust load test stack"
  echo "  --delete-benchmarks                    Delete the locust stack and workloads"
  echo "  --benchmark-worker-count N             Number of WorkerPool replicas (default: 1)"
  echo "  --benchmark-sandbox-class CLASS        Sandbox runtime for the benchmark WorkerPool: gvisor | microvm (default: gvisor)."
  echo "                                         microvm requires hack/install-microvm-deps.sh --install to have run."
  echo "  --benchmark-actor-memory SIZE          Memory limit for the benchmark ActorTemplates (default: 256Mi,"
  echo "                                         the smallest size microvm admits)"
  echo ""
  local demo_name
  for demo_name in "${ATE_DEMOS[@]}"; do
    echo "Demo: ${demo_name}"
    echo ""
    echo "  --deploy-${demo_name}                         Deploy ${demo_name}"
    echo "  --delete-${demo_name}                         Delete ${demo_name}"
    demo_usage "${demo_name}"
    echo ""
  done
}

# ate_setup runs one ate-setup command. GLOBAL_FLAGS holds the pre-scanned
# flags that shape every action, which is how they behaved here: a single
# command line could ask for several actions and one --atenet-dataplane.
ate_setup() {
  if (( VALIDATE_CONFIG )); then return; fi
  go run ./cmd/ate-setup ${GLOBAL_FLAGS[@]+"${GLOBAL_FLAGS[@]}"} "$@"
}

# run_demo maps --deploy-demo-NAME / --delete-demo-NAME onto
# `ate-setup {deploy,delete} demo NAME`. A demo this installer does not
# register is rejected the way any other unknown flag is, rather than reaching
# ate-setup as an unknown subcommand.
run_demo() {
  local action="$1" flag="$2"
  local demo="${flag#--"${action}"-}"
  local known
  for known in "${ATE_DEMOS[@]}"; do
    if [[ "${demo}" == "${known}" ]]; then
      if [[ "${demo}" == "demo-counter" ]]; then
        ate_setup "${action}" demo counter ${COUNTER_FLAGS[@]+"${COUNTER_FLAGS[@]}"}
      else
        ate_setup "${action}" demo "${demo#demo-}"
      fi
      return
    fi
  done
  echo "Error: unknown option: ${flag}" >&2
  echo ""
  usage
  exit 1
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

# If -h or --help appears anywhere in the command line, print the usage and exit.
for arg in "$@"; do
  case "$arg" in
    -h|--help)
      if (( VALIDATE_CONFIG )); then
        echo "Error: help is not an installation setting" >&2
        exit 1
      fi
      usage
      exit 0
      ;;
  esac
done

# Pre-scan value-bearing flags so they can appear before or after the action
# flag they configure (e.g. --benchmark-worker-count before/after
# --deploy-benchmarks). The dispatch loop below also accepts these flags but
# treats them as no-ops since the value is already captured here.
GLOBAL_FLAGS=()
# Valid values for SETUP_CSI: nfs, hostpath, both, none. Defaults to none: a
# CSI driver is an opt-in extra, and NFS needs kernel modules a plain
# workstation will not have loaded.
SETUP_CSI=""
SETUP_CSI_SET=0
BENCHMARK_FLAGS=()
COUNTER_FLAGS=()
GLOBAL_VALUE_FLAGS=(
  --kind-cluster-name --namespace --kubeconfig --context --gcp-project-id --gcp-cluster-name --gcp-cluster-location
  --ko-docker-repo --ko-default-platforms --build-version --actor-jwt-algorithm --actor-jwt-issuer
  --docker-build-flags --image-repo --image-tag --storage-bucket-name --atenet-dataplane
  --experimental-additional-egress-extproc-service --ateapi-expected-jwt-issuer --credential-provider
  --ateapi-postgres-read-write-role --ateapi-postgres-owner-role --ateapi-postgres-schema
  --ateapi-postgres-pool-max-conns --ateapi-postgres-server-ca-file --ateapi-postgres-storage-class
  --ateapi-postgres-cloudsql-instance --ateapi-postgres-cloudsql-gsa --ateapi-postgres-cloudsql-iam-auth
  --ateapi-postgres-cloudsql-ip-type --podcert-workers-per-signer --rollout-timeout --cluster-size
  --otlp-endpoint --benchmark-actor-memory --record-dir --config
)
GLOBAL_BOOL_FLAGS=(--kind --cordon-control-plane --no-report-defaults)
prescan_args=("$@")

# A Docker build flag may start with `--`; reject only options and actions the
# shim itself owns so a missing value cannot consume the requested action.
is_shim_option_or_action() {
  local token="${1%%=*}" flag
  for flag in "${GLOBAL_VALUE_FLAGS[@]}" "${GLOBAL_BOOL_FLAGS[@]}"; do
    [[ "${token}" == "${flag}" ]] && return 0
  done
  case "${token}" in
    --setup-microvm|--setup-csi|--benchmark-worker-count|--benchmark-sandbox-class|--storage-class|--with-external-volume|\
    --deploy-ate-system|--delete-ate-system|--delete-all|--deploy-atelet|--deploy-ate-apiserver|\
    --deploy-atenet|--delete-atenet|--deploy-benchmarks|--delete-benchmarks|\
    --create-jwt-authority-pool-secret|--create-actor-id-ca-pool-secret|--create-actor-id-ca-certs-secret|\
    --create-egress-mitm-ca-pool-secret|--create-podcertificate-controller-cas|\
    --create-api-server-env-vars|--create-api-authentication-config|--deploy-demo-*|--delete-demo-*)
      return 0
      ;;
  esac
  return 1
}

for ((i = 0; i < ${#prescan_args[@]}; i++)); do
  handled=0
  for flag in "${GLOBAL_VALUE_FLAGS[@]}"; do
    if [[ "${prescan_args[i]}" == "${flag}="* ]]; then
      value="${prescan_args[i]#*=}"
      if [[ "${flag}" == "--config" || "${flag}" == "--kubeconfig" ]]; then
        value="$(caller_path "${value}")"
      fi
      GLOBAL_FLAGS+=("${flag}=${value}")
      handled=1
      break
    elif [[ "${prescan_args[i]}" == "${flag}" ]]; then
      if (( i + 1 >= ${#prescan_args[@]} )) || \
          { [[ "${flag}" == "--docker-build-flags" ]] && is_shim_option_or_action "${prescan_args[$((i + 1))]}"; } || \
          { [[ "${prescan_args[$((i + 1))]}" == --* ]] && [[ "${flag}" != "--docker-build-flags" ]]; }; then
        echo "Error: ${flag} requires a value" >&2
        exit 1
      fi
      value="${prescan_args[$((i + 1))]}"
      if [[ "${flag}" == "--config" || "${flag}" == "--kubeconfig" ]]; then
        value="$(caller_path "${value}")"
      fi
      GLOBAL_FLAGS+=("${flag}=${value}")
      ((i += 1))
      handled=1
      break
    fi
  done
  if (( handled )); then
    continue
  fi
  for flag in "${GLOBAL_BOOL_FLAGS[@]}"; do
    if [[ "${prescan_args[i]}" == "${flag}" || "${prescan_args[i]}" == "${flag}="* ]]; then
      GLOBAL_FLAGS+=("${prescan_args[i]}")
      handled=1
      break
    fi
  done
  if (( handled )); then
    continue
  fi
  case "${prescan_args[i]}" in
    --setup-csi=*) SETUP_CSI="${prescan_args[i]#*=}"; SETUP_CSI_SET=1 ;;
    --setup-csi)
      if (( i + 1 < ${#prescan_args[@]} )) && [[ "${prescan_args[$((i + 1))]}" != --* ]]; then
        SETUP_CSI="${prescan_args[$((i + 1))]}"
        ((i += 1))
      else
        SETUP_CSI="nfs"
      fi
      SETUP_CSI_SET=1
      ;;
    --benchmark-worker-count=*) BENCHMARK_FLAGS+=("--worker-count=${prescan_args[i]#*=}") ;;
    --benchmark-worker-count)
      if (( i + 1 >= ${#prescan_args[@]} )) || [[ "${prescan_args[$((i + 1))]}" == --* ]]; then
        echo "Error: --benchmark-worker-count requires a number" >&2
        exit 1
      fi
      BENCHMARK_FLAGS+=("--worker-count=${prescan_args[$((i + 1))]}")
      ;;
    --benchmark-sandbox-class=*) BENCHMARK_FLAGS+=("--sandbox-class=${prescan_args[i]#*=}") ;;
    --benchmark-sandbox-class)
      if (( i + 1 >= ${#prescan_args[@]} )) || [[ "${prescan_args[$((i + 1))]}" == --* ]]; then
        echo "Error: --benchmark-sandbox-class requires gvisor or microvm" >&2
        exit 1
      fi
      BENCHMARK_FLAGS+=("--sandbox-class=${prescan_args[$((i + 1))]}")
      ;;
    --storage-class=*) COUNTER_FLAGS+=("${prescan_args[i]}") ;;
    --storage-class)
      if (( i + 1 >= ${#prescan_args[@]} )) || [[ "${prescan_args[$((i + 1))]}" == --* ]]; then
        echo "Error: --storage-class requires a value" >&2
        exit 1
      fi
      COUNTER_FLAGS+=("--storage-class=${prescan_args[$((i + 1))]}")
      ((i += 1))
      ;;
    --with-external-volume|--with-external-volume=*) COUNTER_FLAGS+=("${prescan_args[i]}") ;;
  esac
done

# Actions run in command line order, one ate-setup invocation each, so a single
# line can still ask for several of them.
while [[ "$#" -gt 0 ]]; do
  skip=0
  for flag in "${GLOBAL_VALUE_FLAGS[@]}"; do
    if [[ "$1" == "${flag}="* ]]; then
      skip=1
      break
    elif [[ "$1" == "${flag}" ]]; then
      skip=2
      break
    fi
  done
  if (( ! skip )); then
    for flag in "${GLOBAL_BOOL_FLAGS[@]}"; do
      if [[ "$1" == "${flag}" || "$1" == "${flag}="* ]]; then
        skip=1
        break
      fi
    done
  fi
  if (( skip )); then
    shift "${skip}"
    continue
  fi
  case $1 in
    # Captured in the pre-scan above; matched here only so the `*)` branch does
    # not reject them, and so a separated value is consumed with its flag.
    --benchmark-worker-count|--benchmark-sandbox-class|--benchmark-actor-memory) shift ;;
    --benchmark-worker-count=*|--benchmark-sandbox-class=*|--benchmark-actor-memory=*) ;;
    --storage-class) shift ;;
    --storage-class=*|--with-external-volume|--with-external-volume=*) ;;

    --deploy-ate-system)
      if (( SETUP_CSI_SET )); then
        ate_setup deploy ate-system "--setup-csi=${SETUP_CSI}"
      else
        ate_setup deploy ate-system
      fi
      ;;
    --setup-microvm) ate_setup setup microvm ;;
    --setup-csi=*) ate_setup setup csi "${SETUP_CSI}" ;;
    --setup-csi)
      if [[ "$#" -gt 1 && "$2" != --* ]]; then
        shift
      fi
      ate_setup setup csi "${SETUP_CSI}"
      ;;
    --delete-ate-system) ate_setup delete ate-system ;;
    --delete-all) ate_setup delete all ;;

    --deploy-atelet) ate_setup deploy atelet ;;
    --deploy-ate-apiserver) ate_setup deploy apiserver ;;

    --deploy-atenet) ate_setup deploy atenet ;;
    --delete-atenet) ate_setup delete atenet ;;

    --deploy-benchmarks) ate_setup deploy benchmarks ${BENCHMARK_FLAGS[@]+"${BENCHMARK_FLAGS[@]}"} ;;
    --delete-benchmarks) ate_setup delete benchmarks ${BENCHMARK_FLAGS[@]+"${BENCHMARK_FLAGS[@]}"} ;;

    --create-jwt-authority-pool-secret) ate_setup create jwt-authority-pool ;;
    --create-actor-id-ca-pool-secret) ate_setup create actor-id-ca-pool ;;
    --create-actor-id-ca-certs-secret) ate_setup create actor-id-ca-certs ;;
    --create-egress-mitm-ca-pool-secret) ate_setup create egress-mitm-ca-pool ;;
    --create-podcertificate-controller-cas) ate_setup create podcertificate-controller-cas ;;
    --create-api-server-env-vars) ate_setup create api-server-env-vars ;;
    --create-api-authentication-config) ate_setup create api-authentication-config ;;

    # The one demo flag that is not just a demo name.
    --deploy-demo-counter-with-external-volume)
      ate_setup deploy demo counter --with-external-volume ${COUNTER_FLAGS[@]+"${COUNTER_FLAGS[@]}"}
      ;;
    --deploy-demo-*) run_demo deploy "$1" ;;
    --delete-demo-*) run_demo delete "$1" ;;

    *)
      # Invalid option, should usage and exit with an error.
      echo "Error: unknown option: $1" >&2
      echo ""
      usage
      exit 1
      ;;
  esac
  shift
done

if (( VALIDATE_CONFIG )); then
  if (( SETUP_CSI_SET )); then GLOBAL_FLAGS+=("--setup-csi=${SETUP_CSI}"); fi
  go run ./cmd/ate-setup ${GLOBAL_FLAGS[@]+"${GLOBAL_FLAGS[@]}"} validate
fi
