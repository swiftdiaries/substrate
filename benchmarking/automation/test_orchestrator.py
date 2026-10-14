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

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import yaml

import orchestrator
from testtypes import locust


class AteSetupConfigTest(unittest.TestCase):
    def test_target_environment_is_written_and_passed_to_installer(self):
        values = {
            "PROJECT_ID": "project-a",
            "CLUSTER_NAME": "cluster-a",
            "CLUSTER_LOCATION": "zone-a",
            "KUBECTL_CONTEXT": "gke_project-a_zone-a_cluster-a",
            "KO_DOCKER_REPO": "gcr.io/project-a/ate",
            "KO_DEFAULTPLATFORMS": "linux/amd64",
            "BUCKET_NAME": "snapshots-a",
            "ATE_CREDENTIAL_PROVIDER": '{"name":"k8s.io"}',
            "ATE_ATENET_DATAPLANE": "envoy",
            "ATE_API_POSTGRES_CLOUDSQL_INSTANCE": "project-a:zone-a:db-a",
            "ATE_API_POSTGRES_CLOUDSQL_GSA": "db@project-a.iam.gserviceaccount.com",
            "ATE_API_POSTGRES_CLOUDSQL_IAM_AUTH": "true",
            "ATE_API_POSTGRES_CLOUDSQL_IP_TYPE": "private",
            "BENCHMARK_ACTOR_MEMORY": "512Mi",
        }
        with tempfile.TemporaryDirectory() as tmp, mock.patch.object(
            orchestrator, "SUBSTRATE_DIR", tmp
        ), mock.patch.dict(os.environ, {"PATH": os.environ["PATH"]}, clear=True):
            target_dir = Path(tmp, "targets")
            target_dir.mkdir()
            (target_dir / "target.sh").write_text(
                "\n".join(f"export {name}='{value}'" for name, value in values.items()),
                encoding="utf-8",
            )
            orchestrator.apply_target_cluster("target", str(target_dir))

            config_path = Path(tmp, orchestrator.ATE_CONFIG_FILE_NAME)
            config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
            self.assertEqual(config["gcp"], {
                "projectID": "project-a", "clusterName": "cluster-a", "clusterLocation": "zone-a"
            })
            self.assertEqual(config["ko"], {
                "dockerRepo": "gcr.io/project-a/ate", "defaultPlatforms": "linux/amd64"
            })
            self.assertEqual(config["ateapi"]["postgres"]["cloudsql"], {
                "instance": "project-a:zone-a:db-a",
                "gsa": "db@project-a.iam.gserviceaccount.com",
                "iamAuth": "true",
                "ipType": "private",
            })
            self.assertEqual(config["benchmark"]["actorMemory"], "512Mi")
            self.assertEqual(config["atenet"]["egress"]["credentialProvider"], '{"name":"k8s.io"}')
            self.assertEqual(config["atenet"]["dataplane"], "envoy")
            with mock.patch.object(orchestrator, "run") as run, mock.patch.object(
                orchestrator, "run_no_check"
            ) as run_no_check:
                orchestrator.deploy_substrate()
                orchestrator.teardown_substrate()
                self.assertEqual(run.call_args.args[0], [
                    "hack/install-ate.sh", "--config", orchestrator.ATE_CONFIG_FILE_NAME,
                    "--deploy-ate-system",
                ])
                self.assertEqual(run_no_check.call_args.args[0], [
                    "hack/install-ate.sh", "--config", orchestrator.ATE_CONFIG_FILE_NAME,
                    "--delete-ate-system",
                ])
            orchestrator.clear_target_cluster()
            self.assertFalse(config_path.exists())


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
