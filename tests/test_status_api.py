"""The shared DeploymentStatus boundary (plan §3.4 table, WP9).

`state/status.py` had the durable CAS-guarded record and nothing wrote to it,
so consumers had no typed surface and the only way to learn a deployment's
state was to parse the root's private readiness file or grep a log. These tests
pin the writer, the reader, and the two things the boundary exists to prevent:
a stale generation's record being mistaken for this one's, and a client getting
an endpoint for a deployment that is not READY.
"""

from __future__ import annotations

import pytest

from clientlab.targets import exaserve_target
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import build_allocation_binding
from exaserve.site import default_site_profile
from exaserve.state.status import DeploymentState
from exaserve.status_api import (
    DeploymentNotReady,
    DeploymentStatusPublisher,
    read_deployment_status,
    require_ready_endpoint,
)


def _plan(num_nodes: int = 2):
    return compile_deployment_plan(
        {"num_nodes": num_nodes, "num_gpus_per_node": 12, "validation_mode": True,
         "models": [{"model_id": "m", "tensor_parallel_size": 1,
                     "max_model_len": 128, "size": 8}]},
        site=default_site_profile(), deployment_id="d1")


def _binding(plan, generation=3):
    return build_allocation_binding(
        plan=plan, generation=generation, scheduler_allocation_id="job1",
        nodes=[f"n{i}" for i in range(plan.num_nodes)])


def _publisher(tmp_path, generation=3):
    plan = _plan()
    binding = _binding(plan, generation)
    pub = DeploymentStatusPublisher(str(tmp_path), plan=plan, binding=binding,
                                    generation=generation, log=lambda *_: None)
    pub.initialize()
    return pub, plan, binding


def _walk_to_ready(pub, endpoint="http://h:8000"):
    pub.advance_through(DeploymentState.STAGING, DeploymentState.CLUSTER_STARTING,
                        DeploymentState.DEPLOYING, reason_code="X")
    pub.advance(DeploymentState.VALIDATING, reason_code="X",
                advertised_endpoint=endpoint)
    pub.advance(DeploymentState.READY, reason_code="READY",
                advertised_endpoint=endpoint)


# -- writer ---------------------------------------------------------------
def test_publisher_walks_the_real_lifecycle(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    assert pub.state == "PLANNED"
    _walk_to_ready(pub)
    assert pub.state == "READY"
    assert read_deployment_status(str(tmp_path)).ready


def test_an_illegal_transition_is_refused_not_written(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    assert pub.advance(DeploymentState.READY, reason_code="X") is None
    assert read_deployment_status(str(tmp_path)).state == "PLANNED"


def test_a_second_publisher_does_not_take_over_the_record(tmp_path):
    """Two generations writing one file is how a stale READY survives."""
    pub, plan, binding = _publisher(tmp_path)
    _walk_to_ready(pub)
    intruder = DeploymentStatusPublisher(str(tmp_path), plan=plan, binding=binding,
                                         generation=9, log=lambda *_: None)
    assert intruder.initialize() is None
    assert intruder.advance(DeploymentState.FAILED, reason_code="X") is None
    assert read_deployment_status(str(tmp_path)).ready


def test_failure_is_published_with_its_first_cause(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance(DeploymentState.FAILED, reason_code="FIRST_CAUSE",
                detail="the gateway never started")
    status = read_deployment_status(str(tmp_path))
    assert status.terminal and status.detail == "the gateway never started"


def test_provenance_carries_the_generation_and_hashes(tmp_path):
    pub, plan, binding = _publisher(tmp_path)
    status = read_deployment_status(str(tmp_path))
    assert status.generation == 3
    assert status.deployment_plan_hash == plan.deployment_plan_hash
    assert status.allocation_binding_hash == binding.allocation_binding_hash


# -- reader ---------------------------------------------------------------
def test_no_record_is_not_an_endpoint(tmp_path):
    assert read_deployment_status(str(tmp_path)) is None
    with pytest.raises(DeploymentNotReady, match="no deployment status"):
        require_ready_endpoint(str(tmp_path))


def test_a_deploying_deployment_yields_no_endpoint(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance_through(DeploymentState.STAGING, DeploymentState.CLUSTER_STARTING,
                        DeploymentState.DEPLOYING, reason_code="X")
    with pytest.raises(DeploymentNotReady, match="DEPLOYING"):
        require_ready_endpoint(str(tmp_path))


def test_a_stale_generation_is_rejected_by_the_reader(tmp_path):
    pub, plan, _ = _publisher(tmp_path, generation=3)
    _walk_to_ready(pub)
    with pytest.raises(DeploymentNotReady, match="generation 3"):
        require_ready_endpoint(str(tmp_path), expected_generation=4)
    with pytest.raises(DeploymentNotReady, match="!="):
        require_ready_endpoint(str(tmp_path), expected_plan_hash="f" * 64)
    assert require_ready_endpoint(
        str(tmp_path), expected_generation=3,
        expected_plan_hash=plan.deployment_plan_hash) == "http://h:8000"


def test_ready_without_an_endpoint_is_not_usable(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    _walk_to_ready(pub, endpoint="")
    with pytest.raises(DeploymentNotReady, match="published no endpoint"):
        require_ready_endpoint(str(tmp_path))


# -- ClientLab consumer ---------------------------------------------------
def test_clientlab_resolves_only_a_ready_deployment(tmp_path):
    pub, plan, _ = _publisher(tmp_path)
    with pytest.raises(exaserve_target.DeploymentTargetError):
        exaserve_target.resolve(str(tmp_path))
    _walk_to_ready(pub)
    target = exaserve_target.resolve(
        str(tmp_path), expected_plan_hash=plan.deployment_plan_hash)
    assert target.base_url == "http://h:8000"
    assert target.generation == 3
    assert target.is_validation_only          # this plan is DIRECT_VALIDATION


def test_clientlab_stops_waiting_on_a_terminal_deployment(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance(DeploymentState.FAILED, reason_code="FIRST_CAUSE", detail="boom")
    with pytest.raises(exaserve_target.DeploymentTargetError, match="FAILED"):
        exaserve_target.wait_until_ready(str(tmp_path), timeout_s=30.0, poll_s=0.01)


def test_clientlab_wait_times_out_with_the_last_state(tmp_path):
    pub, _, _ = _publisher(tmp_path)
    pub.advance(DeploymentState.STAGING, reason_code="X")
    with pytest.raises(exaserve_target.DeploymentTargetError, match="STAGING"):
        exaserve_target.wait_until_ready(str(tmp_path), timeout_s=0.05, poll_s=0.01)


def test_clientlab_does_not_monitor_processes_or_grep_logs():
    """The boundary is the point; a fallback would quietly reinstate the coupling."""
    import inspect

    source = inspect.getsource(exaserve_target)
    for forbidden in ("subprocess", "Popen", "psutil", "pgrep", "CLUSTER FULLY READY",
                      "readiness.json", "launch.log"):
        assert forbidden not in source, f"{forbidden} reintroduces a private view"


def test_clientlab_has_no_deployment_plan_compiler_of_its_own():
    """Deployment identity has exactly one producer (IMP-H01)."""
    import pathlib

    root = pathlib.Path(__file__).resolve().parents[1] / "clientlab"
    offenders = []
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "compile_deployment_plan" in text or "build_receipt_requirements" in text:
            offenders.append(str(path))
    assert offenders == []


# -- readiness inputs derived from the child's evidence --------------------
def test_a_single_model_matches_the_default_serve_application():
    """Serve does not name applications after model ids.

    A single-model deployment is just `default`, so matching on the model id
    found nothing and every model resolved to a zero replica target -- which
    reads as "the deployment is empty" for a deployment that is fully up.
    """
    from exaserve.launcher import _application_for

    plan = _plan()
    model = plan.models[0]
    apps = {"default": {"running": 24, "target": 24, "route_prefix": "/",
                        "status": "RUNNING"}}
    assert _application_for(model, apps, plan.models)["running"] == 24


def test_multi_model_matches_by_route_not_by_luck():
    from exaserve.launcher import _application_for

    plan = compile_deployment_plan(
        {"num_nodes": 2, "num_gpus_per_node": 12, "validation_mode": True,
         "models": [{"model_id": "org/alpha", "tensor_parallel_size": 1,
                     "max_model_len": 128, "size": 8},
                    {"model_id": "org/beta", "tensor_parallel_size": 1,
                     "max_model_len": 128, "size": 8}]},
        site=default_site_profile(), deployment_id="d1")
    apps = {
        plan.models[0].route_name: {"running": 2, "target": 2,
                                    "route_prefix": f"/{plan.models[0].route_name}",
                                    "status": "RUNNING"},
        plan.models[1].route_name: {"running": 3, "target": 3,
                                    "route_prefix": f"/{plan.models[1].route_name}",
                                    "status": "RUNNING"},
    }
    assert _application_for(plan.models[0], apps, plan.models)["running"] == 2
    assert _application_for(plan.models[1], apps, plan.models)["running"] == 3


def test_no_matching_application_is_none_not_a_guess():
    from exaserve.launcher import _application_for

    plan = _plan()
    assert _application_for(plan.models[0], {}, plan.models) is None


def test_the_root_exports_its_own_binding_hash(tmp_path, monkeypatch):
    """The root's own receipts are built from the environment, like everyone's.

    Exporting the binding hash only into the ranks' env left the root's GLOBAL
    receipt with an empty allocation_binding_hash, which the strict validator
    rejected -- blocking readiness on `global/supervisor`.
    """
    import os

    from exaserve.composition import CompositionRoot

    monkeypatch.delenv("EXASERVE_ALLOCATION_BINDING_HASH", raising=False)
    plan = _plan()
    root = CompositionRoot(plan=plan, generation=3, run_dir=str(tmp_path),
                           log=lambda *_: None)
    binding = root.bind_allocation([f"n{i}" for i in range(plan.num_nodes)], "job1")
    assert os.environ["EXASERVE_ALLOCATION_BINDING_HASH"] == \
        binding.allocation_binding_hash
