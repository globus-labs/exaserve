"""P04 / IMP-H03: the shell is an adapter; Python owns the lifecycle."""

from __future__ import annotations

import json
import os

import pytest

from exaserve.composition import (
    CompositionError,
    CompositionRoot,
    StagingStep,
    read_nodefile,
)
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import SiteProfile


def _site():
    return SiteProfile(
        schema_version=2, site_id="s", max_nodes=64, gpus_per_node=12,
        cpus_per_node=64, scheduler_types=("pbs",), gateway_kinds=("haproxy",),
        vendors=("xpu",), engines=("vllm",), model_storage_path="/m",
        local_stage_path="/t").finalize()


def _plan(nodes=2):
    raw = {"num_nodes": nodes,
           "models": [{"model_id": "a/b", "tensor_parallel_size": 1,
                       "max_model_len": 4096, "size": 8}],
           "gateway": {"kind": "haproxy", "port": 4001}}
    return compile_deployment_plan(raw, site=_site(), deployment_id="d")


def _root(tmp_path, nodes=2):
    return CompositionRoot(plan=_plan(nodes), generation=7,
                           run_dir=str(tmp_path), log=lambda *_: None)


# -- the shell is an adapter -------------------------------------------------

def test_the_shell_ends_in_exactly_one_exec_with_nothing_after_it():
    from importlib import resources

    script = (resources.files("exaserve") / "resources" /
              "launch_cluster.sh").read_text()
    lines = [ln for ln in script.splitlines() if ln.strip()]
    # A process exec, not a redirection exec (`exec > >(tee ...)`) -- the
    # adapter must not own the run's log routing either.
    exec_lines = [i for i, ln in enumerate(lines)
                  if ln.strip().startswith("exec ")
                  and not ln.strip().startswith("exec >")]
    assert len(exec_lines) == 1, f"expected exactly one process exec, found {len(exec_lines)}"
    assert not any(ln.strip().startswith("exec >") for ln in lines), (
        "the adapter still re-routes logs, which is lifecycle ownership")
    after = [ln for ln in lines[exec_lines[0] + 1:]
             if not ln.strip().startswith("#")]
    assert not after, f"shell has lifecycle code after its exec: {after[:3]}"


def test_no_lifecycle_runs_after_the_handoff():
    """Nothing may run after the adapter hands the run to Python."""
    from importlib import resources

    script = (resources.files("exaserve") / "resources" /
              "launch_cluster.sh").read_text()
    tail = script[script.rindex("\nexec "):]
    for forbidden in ("stop_copper", "gather", "cleanup_run", "trap "):
        assert forbidden not in tail, f"{forbidden!r} still runs after the exec"


def test_the_composition_root_is_the_exec_target():
    from importlib import resources

    script = (resources.files("exaserve") / "resources" /
              "launch_cluster.sh").read_text()
    assert "-m exaserve.launcher" in script


# -- fail-closed listener ----------------------------------------------------

def test_listener_failure_launches_no_ranks(tmp_path, monkeypatch):
    """A run nobody can observe is not a run worth starting."""
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "job1")

    import exaserve.control.channel_runtime as cr

    def _boom(**_kw):
        raise OSError("address in use")

    monkeypatch.setattr(cr, "HeadChannel", _boom)
    with pytest.raises(CompositionError, match="refusing to launch ranks"):
        root.bind_control_listener(retries=2, backoff_s=0.01)
    assert root.head_channel is None


# -- allocation binding ------------------------------------------------------

def test_the_binding_is_persisted_atomically(tmp_path):
    root = _root(tmp_path)
    binding = root.bind_allocation(["n0", "n1"], "job42")
    data = json.loads((tmp_path / "allocation_binding.json").read_text())
    assert data["allocation_binding_hash"] == binding.allocation_binding_hash
    assert data["rank_to_node"] == [[0, "n0"], [1, "n1"]]
    assert not list(tmp_path.glob("*.tmp*"))


def test_a_node_count_mismatch_refuses_to_bind(tmp_path):
    from exaserve.plan.contracts import PlanError

    root = _root(tmp_path, nodes=4)
    with pytest.raises(PlanError, match="different deployment"):
        root.bind_allocation(["n0", "n1"], "job1")


# -- staging as owned finite components --------------------------------------

def test_a_staging_step_that_exits_zero_without_its_result_fails(tmp_path):
    root = _root(tmp_path)
    step = StagingStep(name="distribute", argv=["/bin/true"],
                       result_paths=(str(tmp_path / "never_written"),))
    with pytest.raises(CompositionError, match="exited 0 but"):
        root.run_staging([step])


def test_a_staging_step_that_produces_its_result_succeeds(tmp_path):
    root = _root(tmp_path)
    marker = tmp_path / "manifest.json"
    step = StagingStep(name="stage", argv=["/bin/sh", "-c", f"echo x > {marker}"],
                       result_paths=(str(marker),))
    root.run_staging([step])
    assert marker.exists()


def test_a_nonzero_staging_step_fails(tmp_path):
    root = _root(tmp_path)
    with pytest.raises(CompositionError, match="exited 3"):
        root.run_staging([StagingStep(name="bad", argv=["/bin/sh", "-c", "exit 3"])])


def test_a_staging_step_has_a_bounded_deadline(tmp_path):
    root = _root(tmp_path)
    step = StagingStep(name="slow", argv=["/bin/sleep", "5"], deadline_s=0.3)
    with pytest.raises(CompositionError, match="deadline"):
        root.run_staging([step])


# -- gateway ownership -------------------------------------------------------

def test_the_head_owns_the_gateway_not_rank_zero(tmp_path):
    root = _root(tmp_path)
    root.bind_allocation(["n0", "n1"], "j")
    component = root.start_gateway(["/bin/sleep", "30"])
    try:
        assert component is not None
        assert component.component_id == "gateway/haproxy"
        assert component.owner_scope == "GLOBAL"
        assert root.gateway_alive() is True
    finally:
        root.shutdown(drain_s=5)


def test_the_advertised_endpoint_is_the_gateway_port(tmp_path):
    root = _root(tmp_path)
    assert root.advertised_endpoint("10.0.0.1") == "http://10.0.0.1:4001"


def test_validation_direct_advertises_the_serve_port(tmp_path):
    raw = {"num_nodes": 1, "validation_mode": True,
           "exposure": {"mode": "DIRECT_VALIDATION", "serve_port": 8000},
           "models": [{"model_id": "a/b", "tensor_parallel_size": 1,
                       "max_model_len": 4096, "size": 8}]}
    plan = compile_deployment_plan(raw, site=_site(), deployment_id="d")
    root = CompositionRoot(plan=plan, generation=1, run_dir=str(tmp_path),
                           log=lambda *_: None)
    assert root.advertised_endpoint("10.0.0.1") == "http://10.0.0.1:8000"
    assert root.start_gateway(["/bin/true"]) is None


# -- nodefile ----------------------------------------------------------------

def test_a_missing_nodefile_is_a_typed_failure(monkeypatch):
    monkeypatch.setenv("EXASERVE_NODEFILE", "/nonexistent")
    with pytest.raises(CompositionError, match="no nodefile"):
        read_nodefile()


def test_the_nodefile_deduplicates_in_order(tmp_path, monkeypatch):
    path = tmp_path / "nodes"
    path.write_text("nB\nnA\nnB\nnC\n")
    monkeypatch.setenv("EXASERVE_NODEFILE", str(path))
    assert read_nodefile() == ["nB", "nA", "nC"]
