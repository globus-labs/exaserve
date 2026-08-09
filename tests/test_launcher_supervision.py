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
    assert "execvp" not in source
    assert "EXASERVE_LEGACY_SHELL_LIFECYCLE" not in source


def test_the_legacy_shell_lifecycle_no_longer_exists():
    """WP13: there is exactly one lifecycle owner.

    The historical compatibility flag and its shell adapter are both gone.
    """
    import inspect

    source = inspect.getsource(launcher)
    assert "use_supervisor" not in source
    assert "EXASERVE_LEGACY_SHELL_LIFECYCLE" not in source


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
        schema_version=3,
        site_id="s",
        max_nodes=8,
        gpus_per_node=12,
        cpus_per_node=64,
        scheduler_types=("pbs",),
        gateway_kinds=("haproxy",),
        vendors=("xpu",),
        engines=("vllm",),
        model_storage_path="/m",
        local_stage_path="/t",
        launcher_capabilities=("ray_serve.run_many",),
    ).finalize()
    plan = compile_deployment_plan(
        {
            "num_nodes": 1,
            "models": [
                {"model_id": "a/b", "tensor_parallel_size": 1, "max_model_len": 4096, "size": 8}
            ],
            "gateway": {"kind": "haproxy", "port": 4001},
        },
        site=site,
        deployment_id="d",
    )

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


def test_launcher_resolves_native_allocation_identity_without_shell(monkeypatch):
    for name in ("EXASERVE_JOBID", "PBS_JOBID", "SLURM_JOB_ID"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PBS_JOBID", "123.aurora")
    assert launcher._scheduler_allocation_id() == "123.aurora"
    monkeypatch.setenv("EXASERVE_JOBID", "123.aurora")
    assert launcher._scheduler_allocation_id() == "123.aurora"
    monkeypatch.setenv("EXASERVE_JOBID", "fabricated")
    with pytest.raises(ValueError, match="disagrees with native pbs identity"):
        launcher._scheduler_allocation_id()


def test_launcher_refuses_to_invent_a_local_allocation_identity(monkeypatch):
    for name in ("EXASERVE_JOBID", "PBS_JOBID", "SLURM_JOB_ID"):
        monkeypatch.delenv(name, raising=False)
    with pytest.raises(ValueError, match="allocation identity"):
        launcher._scheduler_allocation_id()


def test_python_launcher_infers_and_validates_pbs_without_a_shell(tmp_path, monkeypatch):
    nodefile = tmp_path / "nodes"
    nodefile.write_text("node0\n", encoding="utf-8")
    for name in (
        "EXASERVE_SCHEDULER",
        "EXASERVE_JOBID",
        "EXASERVE_NODEFILE",
        "SLURM_JOB_ID",
        "SLURM_JOB_NODELIST",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PBS_JOBID", "123.aurora")
    monkeypatch.setenv("PBS_NODEFILE", str(nodefile))
    assert launcher._prepare_scheduler_environment() == "123.aurora"
    assert os.environ["EXASERVE_SCHEDULER"] == "pbs"
    assert os.environ["EXASERVE_NODEFILE"] == str(nodefile)
    assert os.environ["EXASERVE_JOBID"] == "123.aurora"


def test_python_launcher_rejects_ambiguous_or_incomplete_scheduler_state(monkeypatch):
    for name in (
        "EXASERVE_SCHEDULER",
        "EXASERVE_JOBID",
        "EXASERVE_NODEFILE",
        "PBS_JOBID",
        "PBS_NODEFILE",
        "SLURM_JOB_ID",
        "SLURM_JOB_NODELIST",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PBS_JOBID", "pbs")
    monkeypatch.setenv("SLURM_JOB_ID", "slurm")
    with pytest.raises(ValueError, match="cannot infer exactly one scheduler"):
        launcher._prepare_scheduler_environment()

    monkeypatch.delenv("SLURM_JOB_ID")
    with pytest.raises(ValueError, match="NODEFILE"):
        launcher._prepare_scheduler_environment()


def test_explicit_scheduler_cannot_mask_the_other_native_allocation(tmp_path, monkeypatch):
    nodefile = tmp_path / "nodes"
    nodefile.write_text("node0\n", encoding="utf-8")
    monkeypatch.setenv("EXASERVE_SCHEDULER", "pbs")
    monkeypatch.setenv("EXASERVE_NODEFILE", str(nodefile))
    monkeypatch.setenv("SLURM_JOB_ID", "slurm-123")
    monkeypatch.delenv("PBS_JOBID", raising=False)
    monkeypatch.delenv("EXASERVE_JOBID", raising=False)
    with pytest.raises(ValueError, match="disagrees with the native Slurm allocation"):
        launcher._prepare_scheduler_environment()


def test_runtime_scheduler_is_bound_to_site_envelope_and_native_production_identity(
    monkeypatch,
):
    from types import SimpleNamespace

    site = SimpleNamespace(site_id="aurora", scheduler_types=("pbs",))
    envelope = SimpleNamespace(envelope_id="pbs-envelope", scheduler_type="pbs")
    plan = SimpleNamespace(scale_envelope=envelope, validation_mode=False)
    monkeypatch.setenv("EXASERVE_SCHEDULER", "pbs")
    monkeypatch.delenv("PBS_JOBID", raising=False)
    with pytest.raises(ValueError, match="native scheduler job identity"):
        launcher._validate_runtime_scheduler(plan, site)

    monkeypatch.setenv("PBS_JOBID", "123.aurora")
    launcher._validate_runtime_scheduler(plan, site)

    monkeypatch.setenv("EXASERVE_SCHEDULER", "slurm")
    with pytest.raises(ValueError, match="not supported by SiteProfile"):
        launcher._validate_runtime_scheduler(plan, site)


def test_validation_allocation_may_use_an_explicit_non_native_identity(monkeypatch):
    from types import SimpleNamespace

    site = SimpleNamespace(site_id="aurora", scheduler_types=("pbs",))
    envelope = SimpleNamespace(envelope_id="pbs-envelope", scheduler_type="pbs")
    plan = SimpleNamespace(scale_envelope=envelope, validation_mode=True)
    monkeypatch.setenv("EXASERVE_SCHEDULER", "pbs")
    monkeypatch.delenv("PBS_JOBID", raising=False)
    launcher._validate_runtime_scheduler(plan, site)


def test_unset_generation_is_unique_and_explicit_generation_is_strict(monkeypatch):
    monkeypatch.delenv("EXASERVE_GENERATION", raising=False)
    first = launcher._generation_from_environment()
    second = launcher._generation_from_environment()
    assert second > first > 0
    monkeypatch.setenv("EXASERVE_GENERATION", "0")
    assert launcher._generation_from_environment() == 0
    monkeypatch.setenv("EXASERVE_GENERATION", "nan")
    with pytest.raises(ValueError, match="nonnegative integer"):
        launcher._generation_from_environment()


def test_the_run_path_drives_the_gateway_and_readiness():
    """The audit's failure mode: a component that exists and is not used.

    start_gateway / build_readiness / establish_advertised_endpoint existed,
    were tested, and nothing on the reachable path called them -- so a
    PROXIED_INTERNAL plan reached READY without a gateway ever starting.
    """
    import inspect

    source = inspect.getsource(launcher)
    assert "_drive_readiness(root, prepared_gateway_argv=" in source, (
        "run() does not drive readiness"
    )
    driver_src = inspect.getsource(launcher._drive_readiness)
    for call in (
        "build_readiness",
        "establish_advertised_endpoint",
        "start_gateway",
        "await_initial_readiness",
        "commit_ready",
    ):
        assert call in driver_src, f"the run path does not call {call}"
    from exaserve.composition import CompositionRoot

    convergence_src = inspect.getsource(CompositionRoot.await_initial_readiness)
    assert "canary_advertised_endpoint" in convergence_src


def test_production_gateway_listener_is_reserved_before_expensive_startup():
    """Port conflicts must fail before staging, Ray, or model loading.

    HAProxy receives the exact reserved FD later; readiness must consume the
    prepared command rather than opening a second close/bind race.
    """
    import inspect

    run_source = inspect.getsource(launcher.run)
    signal_owner = run_source.index("root.supervisor.install_signal_handlers()")
    prepare = run_source.index("prepared_gateway_argv = root.gateway_argv")
    assert signal_owner < prepare
    assert prepare < run_source.index("root.bind_control_listener()")
    assert prepare < run_source.index("root.run_staging(")
    assert prepare < run_source.index("root.start_ray_cluster(")
    assert prepare < run_source.index("root.start_deployment(")


def test_control_listener_is_bound_exactly_once():
    """One generation has one authenticated listener and one owning thread."""
    import inspect

    run_source = inspect.getsource(launcher.run)
    assert run_source.count("root.bind_control_listener()") == 1


def test_verified_plan_identity_drives_supervisor_compatibility_activation():
    """A scheduler-derived candidate ID cannot outlive plan verification."""
    import inspect

    run_source = inspect.getsource(launcher.run)
    assert "deployment_id=plan.deployment_id" in run_source

    readiness_source = inspect.getsource(launcher._drive_readiness)
    assert "gateway_argv(" not in readiness_source
    assert "prepared_gateway_argv" in readiness_source


def test_component_exit_classification_and_cleanup_budget_have_one_owner():
    """The supervisor classifies child death; the plan owns cleanup time."""
    import inspect

    run_source = inspect.getsource(launcher.run)
    assert "return component.process" not in run_source
    assert "cause = cause or root.supervisor.first_cause" in run_source
    assert "root.shutdown(drain_s=plan.control.watchdog_cleanup_deadline_s)" in run_source


def test_readiness_failure_is_a_typed_composition_error():
    """An unsatisfied predicate must abort, not serve unrecorded."""
    import inspect

    from exaserve.composition import CompositionRoot

    src = inspect.getsource(CompositionRoot.await_initial_readiness)
    assert "raise CompositionError" in src
    assert "not satisfied via the advertised endpoint" in src
