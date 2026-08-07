"""IMP-H03: the CLI entry is the composition root, not a Bash supervisor."""

from __future__ import annotations

import os

import pytest

from exaserve import launcher


def test_the_entry_point_is_the_composition_root_not_bash():
    """It used to supervise `bash launch_cluster.sh`, so the shell owned the
    lifecycle and the 'Python supervisor' supervised a shell."""
    import inspect

    source = inspect.getsource(launcher)
    assert "CompositionRoot" in source
    assert "def run(" in source
    # Bash appears only in the explicitly-legacy branch.
    assert source.count("execvp") <= 1
    assert launcher.LEGACY_ENTRY_ENV in source


def test_the_legacy_shell_lifecycle_is_off_by_default(monkeypatch):
    monkeypatch.delenv(launcher.LEGACY_ENTRY_ENV, raising=False)
    assert launcher.use_supervisor() is True
    monkeypatch.setenv(launcher.LEGACY_ENTRY_ENV, "1")
    assert launcher.use_supervisor() is False


def test_cli_launch_cluster_delegates_to_the_composition_root():
    import inspect

    from exaserve import cli

    source = inspect.getsource(cli.launch_cluster)
    assert "execvp" not in source
    assert "launcher" in source


def test_a_plan_that_cannot_compile_is_a_typed_nonzero_exit(tmp_path):
    """Plan compilation failure must not become a running deployment."""
    config = tmp_path / "bad.yaml"
    config.write_text("model_deployment_config:\n  num_node: 4\n")
    assert launcher.run(str(config)) == 2


def test_a_persisted_plan_artifact_is_hash_verified(tmp_path):
    from exaserve.plan.compiler import compile_deployment_plan
    from exaserve.plan.contracts import PlanError, SiteProfile

    site = SiteProfile(
        schema_version=2, site_id="s", max_nodes=8, gpus_per_node=12,
        cpus_per_node=64, scheduler_types=("pbs",), gateway_kinds=("haproxy",),
        vendors=("xpu",), engines=("vllm",), model_storage_path="/m",
        local_stage_path="/t").finalize()
    plan = compile_deployment_plan(
        {"num_nodes": 1,
         "models": [{"model_id": "a/b", "tensor_parallel_size": 1,
                     "max_model_len": 4096, "size": 8}],
         "gateway": {"kind": "haproxy", "port": 4001}},
        site=site, deployment_id="d")

    import json
    from dataclasses import asdict

    payload = asdict(plan)
    artifact = tmp_path / "run.plan.json"
    artifact.write_text(json.dumps(payload, default=str))
    loaded = launcher.load_or_compile_plan(str(artifact), deployment_id="d")
    assert loaded.deployment_plan_hash == plan.deployment_plan_hash

    payload["deployment_plan_hash"] = "0" * 64
    artifact.write_text(json.dumps(payload, default=str))
    with pytest.raises(PlanError, match="hash mismatch"):
        launcher.load_or_compile_plan(str(artifact), deployment_id="d")


def test_the_root_derives_its_own_run_directory(tmp_path, monkeypatch):
    """Durable artifacts must not land in whatever directory the process
    happened to start in."""
    monkeypatch.delenv("EXASERVE_RUN_LOG_DIR", raising=False)
    monkeypatch.setenv("EXASERVE_RUN_LOG_ROOT", str(tmp_path))
    run_dir = launcher._resolve_run_dir(42, "/some/where/config.direct.8b.yaml")
    assert run_dir == str(tmp_path / "gen42_config.direct.8b")
    assert os.path.isdir(run_dir)
    assert os.environ["EXASERVE_RUN_LOG_DIR"] == run_dir


def test_an_explicit_run_directory_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("EXASERVE_RUN_LOG_DIR", str(tmp_path / "explicit"))
    monkeypatch.setenv("EXASERVE_RUN_LOG_ROOT", str(tmp_path))
    assert launcher._resolve_run_dir(1, "c.yaml") == str(tmp_path / "explicit")
