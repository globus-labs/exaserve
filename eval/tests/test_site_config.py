import os
import sys
import tempfile
import unittest
from contextlib import contextmanager


REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

import eval.site_config as site_config


@contextmanager
def patched_site_env(**updates):
    keys_to_clear = {
        "HOME",
        "USER",
        "AURORA_SITE_CONFIG_LOCAL",
        *(f"AURORA_{field.upper()}" for field in site_config._FIELD_NAMES),
    }
    original = {key: os.environ.get(key) for key in keys_to_clear}
    try:
        for key in keys_to_clear:
            os.environ.pop(key, None)
        for key, value in updates.items():
            if value is not None:
                os.environ[key] = value
        yield
    finally:
        for key in keys_to_clear:
            os.environ.pop(key, None)
        for key, value in original.items():
            if value is not None:
                os.environ[key] = value


class SiteConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        site_config.clear_site_config_cache()

    def tearDown(self) -> None:
        site_config.clear_site_config_cache()

    def test_default_paths_follow_user_and_home(self) -> None:
        with tempfile.TemporaryDirectory() as temp_home:
            with patched_site_env(USER="alice", HOME=temp_home):
                cfg = site_config.get_site_config()

        self.assertEqual(cfg.user_data_root, "/lus/flare/projects/AuroraGPT/alice/data")
        self.assertEqual(cfg.model_storage_path, "/lus/flare/projects/AuroraGPT/alice/models")
        self.assertEqual(cfg.input_trace_path, "/lus/flare/projects/AuroraGPT/alice/data/input_traces/AzureLLMInferenceTrace_code_1week.csv")
        self.assertEqual(cfg.input_prompt_path, "/lus/flare/projects/AuroraGPT/alice/data/input_traces/ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json")
        self.assertEqual(cfg.experiments_root, "/lus/flare/projects/AuroraGPT/alice/data/experiments")
        self.assertEqual(cfg.litellm_python_path, f"{temp_home}/agpt/venv/litellm/bin/python3")
        self.assertEqual(cfg.snapshot_dir, f"{temp_home}/agpt/data/snapshots")
        self.assertEqual(cfg.bench_results_dir, f"{temp_home}/agpt/data/bench_results")

    def test_local_override_applies_before_env(self) -> None:
        with tempfile.TemporaryDirectory() as temp_home, tempfile.NamedTemporaryFile("w", suffix=".py") as handle:
            handle.write(
                "SITE_OVERRIDES = {\n"
                "    'user_data_root': '/tmp/team/data',\n"
                "    'pbs_mail_user': 'team@example.com',\n"
                "    'num_gpus_per_node': 16,\n"
                "}\n"
            )
            handle.flush()

            with patched_site_env(
                USER="alice",
                HOME=temp_home,
                AURORA_SITE_CONFIG_LOCAL=handle.name,
            ):
                cfg = site_config.get_site_config()

        self.assertEqual(cfg.user_data_root, "/tmp/team/data")
        self.assertEqual(cfg.experiments_root, "/tmp/team/data/experiments")
        self.assertEqual(cfg.output_trace_dir, "/tmp/team/data/output_traces")
        self.assertEqual(cfg.pbs_mail_user, "team@example.com")
        self.assertEqual(cfg.num_gpus_per_node, 16)

    def test_env_override_wins_over_local_override(self) -> None:
        with tempfile.TemporaryDirectory() as temp_home, tempfile.NamedTemporaryFile("w", suffix=".py") as handle:
            handle.write("SITE_OVERRIDES = {'model_storage_path': '/tmp/local/models'}\n")
            handle.flush()

            with patched_site_env(
                USER="alice",
                HOME=temp_home,
                AURORA_SITE_CONFIG_LOCAL=handle.name,
                AURORA_MODEL_STORAGE_PATH="/tmp/env/models",
                AURORA_NUM_GPUS_PER_NODE="24",
            ):
                cfg = site_config.get_site_config()

        self.assertEqual(cfg.model_storage_path, "/tmp/env/models")
        self.assertEqual(cfg.num_gpus_per_node, 24)

    def test_cache_reset_is_required_after_env_changes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_home:
            with patched_site_env(USER="alice", HOME=temp_home, AURORA_MODEL_STORAGE_PATH="/tmp/first"):
                first = site_config.get_site_config()
                os.environ["AURORA_MODEL_STORAGE_PATH"] = "/tmp/second"
                cached = site_config.get_site_config()
                site_config.clear_site_config_cache()
                updated = site_config.get_site_config()

        self.assertEqual(first.model_storage_path, "/tmp/first")
        self.assertEqual(cached.model_storage_path, "/tmp/first")
        self.assertEqual(updated.model_storage_path, "/tmp/second")

if __name__ == "__main__":
    unittest.main()
