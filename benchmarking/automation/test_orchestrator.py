# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for orchestrator.py: python3 benchmarking/automation/test_orchestrator.py"""

import argparse
import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

import orchestrator
from testtypes import locust


class AteSetupConfigTest(unittest.TestCase):
    def test_target_file_does_not_inherit_ordinary_installer_settings(self):
        preserved = {
            "PATH": os.environ["PATH"],
            "GCE_REGION": "tool-region",
            "KUBECONFIG": "/tool-kubeconfig",
            "GOOGLE_APPLICATION_CREDENTIALS": "/tool-credentials",
            "ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": "secret-read-write-dsn",
            "ATE_API_POSTGRES_OWNER_CONNECTION_STRING": "secret-owner-dsn",
            "ANTHROPIC_API_KEY": "secret-api-key",
        }
        ambient = {**preserved, "ATE_ATENET_DATAPLANE": "agentgateway"}
        for explicit in (False, True):
            with self.subTest(explicit=explicit), tempfile.TemporaryDirectory() as tmp, \
                 mock.patch.dict(os.environ, ambient, clear=True), \
                 mock.patch.object(orchestrator, "_ORIG_ENV", ambient), \
                 mock.patch.object(orchestrator, "SUBSTRATE_DIR", tmp):
                target = Path(tmp, "target.sh")
                target.write_text("PROJECT_ID=p\nCLUSTER_NAME=c\nCLUSTER_LOCATION=z\nKO_DOCKER_REPO=registry/target\n"
                                  + "ACTOR_JWT_ISSUER=https://issuer.example/actors\n"
                                  + ("ATE_ATENET_DATAPLANE=agentgateway\n" if explicit else ""))
                orchestrator.apply_target_cluster("target", tmp)
                serialized = Path(tmp, orchestrator.ATE_CONFIG_FILE_NAME).read_text()
                document = yaml.safe_load(serialized)
                self.assertEqual(document["actorJWT"]["issuer"], "https://issuer.example/actors")
                self.assertEqual(document.get("atenet", {}).get("dataplane"), "agentgateway" if explicit else None)
                for name, value in preserved.items():
                    self.assertEqual(os.environ.get(name), value, name)
                    if name in ("ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING", "ATE_API_POSTGRES_OWNER_CONNECTION_STRING", "ANTHROPIC_API_KEY"):
                        self.assertNotIn(value, serialized)

    def test_maps_nested_settings_and_excludes_secrets(self):
        env = {
            "PROJECT_ID": "project-a",
            "KUBECTL_CONTEXT": "gke_project-a_zone-a_cluster-a",
            "ATE_CREDENTIAL_PROVIDER": '{"name":"k8s.io"}',
            "ATE_INSTALL_CLUSTER_SIZE": "size10",
            "ACTOR_JWT_ISSUER": "https://issuer.example/actors",
            "ATE_API_POSTGRES_CLOUDSQL_INSTANCE": "project-a:zone-a:db-a",
            "ATE_API_POSTGRES_CLOUDSQL_GSA": "db@project-a.iam.gserviceaccount.com",
            "ATE_API_POSTGRES_READ_WRITE_CONNECTION_STRING": "secret-read-write-dsn",
            "ATE_API_POSTGRES_OWNER_CONNECTION_STRING": "secret-owner-dsn",
            "ANTHROPIC_API_KEY": "secret-api-key",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch.dict(os.environ, env, clear=True), mock.patch.object(
            orchestrator, "SUBSTRATE_DIR", tmp
        ):
            path = orchestrator.write_ate_config()
            serialized = path.read_text(encoding="utf-8")
            config = yaml.safe_load(serialized)

        self.assertEqual(config["gcp"], {"projectID": "project-a"})
        self.assertEqual(config["context"], "gke_project-a_zone-a_cluster-a")
        self.assertEqual(config["atenet"]["egress"]["credentialProvider"], '{"name":"k8s.io"}')
        self.assertEqual(config["clusterSize"], "size10")
        self.assertEqual(config["actorJWT"]["issuer"], "https://issuer.example/actors")
        self.assertEqual(
            config["ateapi"]["postgres"]["cloudsql"],
            {"instance": "project-a:zone-a:db-a", "gsa": "db@project-a.iam.gserviceaccount.com"},
        )
        for secret in (
            "secret-read-write-dsn",
            "secret-owner-dsn",
            "secret-api-key",
        ):
            self.assertNotIn(secret, serialized)


class DeployWorkloadsTest(unittest.TestCase):
    @mock.patch("orchestrator.run")
    def test_defaults_omit_optional_flags(self, run):
        orchestrator.deploy_workloads()
        run.assert_called_once_with(
            [
                "benchmarking/workloads/deploy.sh",
                "--deploy",
                "--worker-count",
                "1",
                "--sandbox-class",
                "gvisor",
            ]
        )

    @mock.patch("orchestrator.run")
    def test_worker_memory(self, run):
        orchestrator.deploy_workloads(worker_memory="12Gi")
        cmd = run.call_args.args[0]
        i = cmd.index("--worker-memory")
        self.assertEqual(cmd[i + 1], "12Gi")

    @mock.patch("orchestrator.run")
    def test_all_options(self, run):
        orchestrator.deploy_workloads(
            worker_count=3,
            sandbox_class="microvm",
            actor_memory="1536Mi",
            wait_timeout_secs=600,
            worker_memory="5Gi",
        )
        run.assert_called_once_with(
            [
                "benchmarking/workloads/deploy.sh",
                "--deploy",
                "--worker-count",
                "3",
                "--sandbox-class",
                "microvm",
                "--actor-memory",
                "1536Mi",
                "--worker-memory",
                "5Gi",
                "--wait-timeout",
                "600",
            ]
        )


class TargetLifecycleTest(unittest.TestCase):
    def test_ate_args_target_reaches_deploy_workloads_and_both_cleanups(self):
        variants = (
            "file",
            "flags",
            "kind-override",
            "non-target-flag",
            "tool-settings-file",
            "tool-settings-flags",
            "invalid-file",
            "invalid-csi-file",
            "invalid-csi-flag",
        )
        for variant in variants:
            with self.subTest(variant=variant), tempfile.TemporaryDirectory() as tmp:
                default = Path(tmp, "default.yaml")
                override = Path(tmp, "override.yaml")
                header = "apiVersion: install.ate.dev/v1alpha1\nkind: SubstrateInstall\n"
                default.write_text(header + "context: ambient-context\nkubeconfig: /ambient.yaml\n")
                body = (
                    "contxt: file-context\n"
                    if variant == "invalid-file"
                    else "context: file-context\nkubeconfig: /file.yaml\n"
                )
                if variant == "kind-override":
                    body = "kindCluster.enabled: true\nkindCluster.name: original\ncontext: null\nkubeconfig: /file.yaml\n"
                if variant == "tool-settings-file":
                    body += (
                        "gcp.projectID: file-project\ngcp.clusterName: file-cluster\n"
                        "gcp.clusterLocation: file-zone\nko.dockerRepo: registry/file\n"
                        "storage.bucketName: file-bucket\n"
                    )
                body += "csi.setup: bogus\n" if variant == "invalid-csi-file" else "csi.setup: nfs\n"
                override.write_text(header + body)
                ate_args = ["--config", str(override)]
                if variant == "non-target-flag":
                    ate_args += ["--image-tag=v1"]
                if variant == "kind-override":
                    ate_args += ["--kind-cluster-name=selected"]
                if variant == "invalid-csi-flag":
                    ate_args += ["--setup-csi=bogus"]
                if variant == "tool-settings-flags":
                    ate_args += [
                        "--gcp-project-id=flag-project",
                        "--gcp-cluster-name=flag-cluster",
                        "--gcp-cluster-location=flag-zone",
                        "--ko-docker-repo=registry/flag",
                        "--storage-bucket-name=flag-bucket",
                    ]
                context, kubeconfig = "file-context", "/file.yaml"
                if variant == "kind-override":
                    context = "kind-selected"
                if variant == "flags":
                    context, kubeconfig = "flag-context", "/first.yaml:/second.yaml"
                    ate_args += ["--context=" + context, "--kubeconfig", kubeconfig]
                tests = Path(tmp, "tests.yaml")
                tests.write_text(yaml.safe_dump({"tests": [{
                    "name": "target-check", "targetCluster": "base", "type": "locust",
                    "file": "example.py", "duration": "1s", "users": 1,
                    "sandboxClass": "microvm", "ateArgs": ate_args,
                }]}))
                args = argparse.Namespace(repo="repo", branch="main", dest="out",
                    tests=str(tests), target_cluster_dir=tmp, manifests_dir=tmp, junit_output=None)
                commands = []
                built_repositories = []

                def capture(command, **kwargs):
                    commands.append((command, dict(kwargs.get("env", os.environ))))
                    if "--validate-config" in command and variant.startswith("invalid"):
                        self.assertEqual(command, ["hack/install-ate.sh", "--validate-config", "--config", str(default), *ate_args])
                        raise subprocess.CalledProcessError(1, command)

                def build_image(_commit):
                    built_repositories.append(os.environ["KO_DOCKER_REPO"])
                    return "runner-image"

                with mock.patch.dict(os.environ, {
                    "KUBECONFIG": "/ambient.yaml",
                    "KUBECTL_CONTEXT": "ambient-context",
                    "PROJECT_ID": "ambient-project",
                    "CLUSTER_NAME": "ambient-cluster",
                    "CLUSTER_LOCATION": "ambient-zone",
                    "KO_DOCKER_REPO": "registry/ambient",
                    "BUCKET_NAME": "ambient-bucket",
                }, clear=True), \
                     mock.patch.object(orchestrator, "ATE_CONFIG_FILE_NAME", str(default)), \
                     mock.patch.object(orchestrator, "parse_args", return_value=args), \
                     mock.patch.object(orchestrator, "wait_for_docker"), \
                     mock.patch.object(orchestrator, "apply_target_cluster"), \
                     mock.patch.object(orchestrator, "clear_target_cluster"), \
                     mock.patch.object(orchestrator.os, "chdir"), \
                     mock.patch.object(orchestrator.subprocess, "check_output", return_value="deadbeef"), \
                     mock.patch.object(orchestrator, "run", side_effect=capture), \
                     mock.patch.object(orchestrator, "run_no_check", side_effect=capture), \
                     mock.patch.object(orchestrator, "run_test", return_value="complete"), \
                     mock.patch.object(locust, "build_image", side_effect=build_image):
                    if variant.startswith("invalid"):
                        with self.assertRaises(subprocess.CalledProcessError):
                            orchestrator.main()
                        self.assertEqual([cmd[0] for cmd, _ in commands], ["git", "hack/install-ate.sh"])
                        self.assertIn("--validate-config", commands[-1][0])
                        continue
                    else:
                        with self.assertRaises(SystemExit) as exit:
                            orchestrator.main()
                        self.assertEqual(exit.exception.code, 0)

                cluster_commands = [
                    (cmd, env)
                    for cmd, env in commands
                    if cmd[0] not in ("git", "gcloud") and "--validate-config" not in cmd
                ]
                self.assertEqual(len(cluster_commands), 9, cluster_commands)
                for command, env in cluster_commands:
                    self.assertEqual(env.get("KUBECONFIG"), kubeconfig, command)
                    self.assertIn("--context", command, command)
                    self.assertEqual(command[command.index("--context") + 1], context, command)
                    if command[0] == "hack/install-ate.sh":
                        # Cleanup must use the same explicit file as deployment.
                        self.assertIn(str(override), command)
                if variant == "non-target-flag":
                    cleanup = [cmd for cmd, _ in cluster_commands if "--delete-ate-system" in cmd]
                    self.assertEqual(len(cleanup), 2)
                    for command in cleanup:
                        self.assertIn("--image-tag=v1", command)
                if variant == "kind-override":
                    setup = [cmd for cmd, _ in cluster_commands if "--setup-microvm" in cmd]
                    self.assertEqual(len(setup), 1)
                    self.assertIn("--kind-cluster-name=selected", setup[0])
                    self.assertIn("--kind=true", setup[0])
                expected = {
                    "tool-settings-file": (
                        "file-project", "file-cluster", "file-zone", "registry/file", "file-bucket"
                    ),
                    "tool-settings-flags": (
                        "flag-project", "flag-cluster", "flag-zone", "registry/flag", "flag-bucket"
                    ),
                }.get(
                    variant,
                    ("ambient-project", "ambient-cluster", "ambient-zone", "registry/ambient", "ambient-bucket"),
                )
                project, cluster, zone, repo, bucket = expected
                self.assertEqual(built_repositories, [repo])
                gcloud_commands = [cmd for cmd, _ in commands if cmd[0] == "gcloud"]
                self.assertEqual(
                    gcloud_commands,
                    [
                        ["gcloud", "auth", "configure-docker", repo.split("/", 1)[0], "--quiet"],
                        [
                            "gcloud", "container", "clusters", "get-credentials", cluster,
                            "--location", zone, "--project", project,
                        ],
                    ],
                )
                workload = next(
                    (
                        env
                        for cmd, env in cluster_commands
                        if cmd[0] == "benchmarking/workloads/deploy.sh" and "--deploy" in cmd
                    ),
                    None,
                )
                self.assertIsNotNone(workload)
                self.assertEqual(workload.get("BUCKET_NAME"), bucket)
                for command, env in cluster_commands:
                    if "--delete-ate-system" in command or "--delete" in command:
                        self.assertEqual(env.get("PROJECT_ID"), project, command)
                        self.assertEqual(env.get("KO_DOCKER_REPO"), repo, command)
                        self.assertEqual(env.get("BUCKET_NAME"), bucket, command)

    def test_same_cluster_with_changed_tool_settings_rebuilds_and_reconfigures(self):
        with tempfile.TemporaryDirectory() as tmp:
            tests = Path(tmp, "tests.yaml")
            tests.write_text(
                yaml.safe_dump(
                    {
                        "tests": [
                            {
                                "name": name,
                                "targetCluster": "same",
                                "type": "locust",
                                "file": "example.py",
                                "duration": "1s",
                                "users": 1,
                                "ateArgs": [f"--ko-docker-repo=registry/{name}"],
                            }
                            for name in ("first", "second")
                        ]
                    }
                )
            )
            args = argparse.Namespace(
                repo="repo", branch="main", dest="out", tests=str(tests),
                target_cluster_dir=tmp, manifests_dir=tmp, junit_output=None
            )
            configured, built = [], []

            def configure(ate_args):
                repo = next(
                    arg.split("=", 1)[1]
                    for arg in ate_args
                    if arg.startswith("--ko-docker-repo=")
                )
                os.environ["KO_DOCKER_REPO"] = repo
                configured.append(repo)
                return []

            def build_image(_commit):
                built.append(os.environ["KO_DOCKER_REPO"])
                return "runner-image"

            with mock.patch.dict(os.environ, {
                "PROJECT_ID": "project", "CLUSTER_NAME": "cluster",
                "CLUSTER_LOCATION": "zone", "KO_DOCKER_REPO": "registry/base",
                "BUCKET_NAME": "bucket",
            }, clear=True), \
                 mock.patch.object(orchestrator, "parse_args", return_value=args), \
                 mock.patch.object(orchestrator, "wait_for_docker"), \
                 mock.patch.object(orchestrator, "apply_target_cluster"), \
                 mock.patch.object(orchestrator, "clear_target_cluster"), \
                 mock.patch.object(orchestrator, "configure_kube_target", side_effect=configure), \
                mock.patch.object(
                    orchestrator,
                    "gcloud_setup_for_target_cluster",
                    side_effect=lambda: configured.append("setup:" + os.environ["KO_DOCKER_REPO"]),
                ), \
                 mock.patch.object(orchestrator.os, "chdir"), \
                 mock.patch.object(orchestrator.subprocess, "check_output", return_value="deadbeef"), \
                 mock.patch.object(orchestrator, "run"), \
                 mock.patch.object(orchestrator, "run_no_check"), \
                 mock.patch.object(orchestrator, "run_test", return_value="complete"), \
                 mock.patch.object(locust, "build_image", side_effect=build_image):
                with self.assertRaises(SystemExit) as exit:
                    orchestrator.main()

            self.assertEqual(exit.exception.code, 0)
            self.assertEqual(configured, [
                "registry/first", "setup:registry/first",
                "registry/second", "setup:registry/second",
            ])
            self.assertEqual(built, ["registry/first", "registry/second"])


class ConfigureKubeTargetTest(unittest.TestCase):
    @mock.patch("orchestrator.run")
    @mock.patch("orchestrator.subprocess.check_output", return_value="current-context\n")
    def test_kind_gke_and_current_context_follow_selected_configuration(self, current, run):
        cases = [
            ("context: 001\n", [], "001", False),
            ("kindCluster.enabled: true\nkindCluster.name: test\ncontext: null\n", [], "kind-test", False),
            ("gcp.projectID: p\ngcp.clusterName: c\ngcp.clusterLocation: z\n", ["--gcp-cluster-name=override"], "current-context", True),
            ("context: file-context\n", ["--context="], "current-context", False),
        ]
        for body, args, context, fetch in cases:
            with self.subTest(body=body), tempfile.TemporaryDirectory() as tmp, \
                 mock.patch.dict(os.environ, {"KUBECONFIG": "/selected.yaml", "KUBECTL_CONTEXT": "hostile"}, clear=True):
                run.reset_mock()
                current.reset_mock()
                path = Path(tmp, "config.yaml")
                path.write_text("apiVersion: install.ate.dev/v1alpha1\nkind: SubstrateInstall\n" + body)
                target = orchestrator.configure_kube_target(["--config", str(path), *args])
                self.assertEqual(target[target.index("--context") + 1], context)
                self.assertEqual(os.environ["KUBECTL_CONTEXT"], context)
                expected = [mock.call(["hack/install-ate.sh", "--validate-config", "--config", orchestrator.ATE_CONFIG_FILE_NAME, "--config", str(path), *args])]
                if fetch:
                    expected.append(mock.call(["gcloud", "container", "clusters", "get-credentials", "override", "--location", "z", "--project", "p"]))
                self.assertEqual(run.call_args_list, expected)
                self.assertEqual(current.called, context == "current-context")

    @mock.patch("orchestrator.run")
    @mock.patch("orchestrator.subprocess.check_output", return_value="default-context\n")
    def test_explicit_empty_kubeconfig_ignores_ambient_environment(self, current, run):
        cases = [
            ("kubeconfig: \"\"\n", []),
            ("kubeconfig: null\n", []),
            ("kubeconfig: /file.yaml\n", ["--kubeconfig="]),
        ]
        for body, args in cases:
            with self.subTest(body=body, args=args), tempfile.TemporaryDirectory() as tmp, \
                 mock.patch.dict(os.environ, {"KUBECONFIG": "/ambient.yaml"}, clear=True):
                current.reset_mock()
                path = Path(tmp, "config.yaml")
                path.write_text("apiVersion: install.ate.dev/v1alpha1\nkind: SubstrateInstall\n" + body)

                target = orchestrator.configure_kube_target(["--config", str(path), *args])

                self.assertNotIn("KUBECONFIG", os.environ)
                self.assertNotIn("--kubeconfig", target)
                self.assertEqual(target[target.index("--context") + 1], "default-context")
                current.assert_called_once_with(["kubectl", "config", "current-context"], text=True)


class RunnerSizingTest(unittest.TestCase):
    TMPL = os.path.join(os.path.dirname(__file__), "manifests", "runner-job.yaml.tmpl")

    def render(self, test):
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}
        subs.update(locust.job_subs(test))
        text = orchestrator.render_template(self.TMPL, subs)
        job = next(d for d in yaml.safe_load_all(text) if d and d.get("kind") == "Job")
        return job["spec"]["template"]["spec"]["containers"][0]["resources"]

    def test_defaults(self):
        res = self.render({"file": "f", "duration": "1m", "users": 1})
        self.assertEqual(res, {"requests": {"cpu": "500m", "memory": "512Mi"}})

    def test_runner_cpu_and_memory(self):
        res = self.render(
            {"file": "f", "duration": "1m", "users": 1000, "runnerCpu": "4", "runnerMemory": "8Gi"}
        )
        self.assertEqual(res["requests"], {"cpu": "4", "memory": "8Gi"})
        self.assertNotIn("limits", res)

    def test_no_placeholder_survives(self):
        subs = {"JOB_NAME": "j", "IMAGE": "i", "TAG": "t", "NAME": "n", "DEST": "d"}
        subs.update(locust.job_subs({"file": "f", "duration": "1m", "users": 1}))
        self.assertNotIn("${", orchestrator.render_template(self.TMPL, subs))


class JobNameTest(unittest.TestCase):
    COMMIT = "ac41c06deadbeef"

    def test_short_name_keeps_full_test_name(self):
        name = orchestrator.job_name("Eng Review", self.COMMIT)
        self.assertRegex(name, r"^runner-eng-review-ac41c06-[0-9a-f]{6}$")

    def test_long_name_fits_a_label_and_keeps_suffix(self):
        test = "very-long-benchmark-name-that-overflows-the-kubernetes-label-limit"
        name = orchestrator.job_name(test, self.COMMIT)
        self.assertLessEqual(len(name), orchestrator.MAX_JOB_NAME_LEN)
        self.assertRegex(name, r"-ac41c06-[0-9a-f]{6}$")
        self.assertTrue(name.startswith("runner-very-long-benchmark-name"))

    def test_truncation_never_leaves_a_double_hyphen(self):
        # Cutting right after a hyphen must not yield "...-x--ac41c06-...".
        for i in range(1, 80):
            test = "-".join(["ab"] * i)
            name = orchestrator.job_name(test, self.COMMIT)
            self.assertLessEqual(len(name), orchestrator.MAX_JOB_NAME_LEN)
            self.assertNotIn("--", name, test)
            self.assertRegex(name, r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")

    def test_two_runs_of_the_same_long_test_differ(self):
        test = "x" * 100
        self.assertNotEqual(
            orchestrator.job_name(test, self.COMMIT),
            orchestrator.job_name(test, self.COMMIT),
        )


if __name__ == "__main__":
    unittest.main()
