"""Dense real-engine replicas behind HAProxy share one Serve application."""

from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
import shutil
import sys
import tempfile

import pytest

from exaserve.composition import CompositionRoot
from exaserve.control.plan_readiness import (
    planned_application_names,
    planned_proxy_anchor_names,
    serve_application_layout,
)
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import build_allocation_binding
from exaserve.plan.runtime_binding import (
    LiveNodeInventory,
    bind_runtime_deployment,
)
from exaserve.site import default_site_profile


def _model(**overrides):
    model = {
        "model_id": "m",
        "tensor_parallel_size": 1,
        "num_replicas": 24,
        "max_model_len": 128,
        "size": 8,
    }
    model.update(overrides)
    return model


def _plan(*, models=None, null_compute=False):
    document = {
        "num_nodes": 2,
        "validation_mode": True,
        "gateway": {"kind": "haproxy", "port": 4001},
        "models": models or [_model()],
    }
    if null_compute:
        document["runtime"] = {"null_compute": True}
    return compile_deployment_plan(
        document,
        site=default_site_profile(),
        deployment_id="single-application",
    )


def _bound_plan(monkeypatch, request):
    scratch = Path(tempfile.mkdtemp(prefix="exaserve-sa-", dir="/tmp"))
    request.addfinalizer(lambda: shutil.rmtree(scratch, ignore_errors=True))
    runtime_root = scratch / "runtime"
    state_root = scratch / "state"
    (runtime_root / "python" / "overlay").mkdir(parents=True, mode=0o700)
    (runtime_root / "run").mkdir(mode=0o700)
    for relative in ("ipc", "home", "tmp"):
        (state_root / relative).mkdir(parents=True, mode=0o700)
    plan = _plan()
    binding = build_allocation_binding(
        plan=plan,
        generation=7,
        scheduler_allocation_id="job",
        nodes=["n0", "n1"],
    )
    bound = bind_runtime_deployment(
        plan,
        binding,
        [
            LiveNodeInventory("10.0.0.1", "node:0", 12, 64, hostname="n0"),
            LiveNodeInventory("10.0.0.2", "node:1", 12, 64, hostname="n1"),
        ],
    )
    for key, value in {
        "EXASERVE_DEPLOYMENT_ID": plan.deployment_id,
        "EXASERVE_GENERATION": "7",
        "EXASERVE_PLAN_HASH": plan.deployment_plan_hash,
        "EXASERVE_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_ALLOCATION_BINDING_HASH": binding.allocation_binding_hash,
        "EXASERVE_LOCAL_RUNTIME_ROOT": str(runtime_root),
        "EXASERVE_LOCAL_STATE_ROOT": str(state_root),
        "VLLM_RPC_BASE_PATH": str(state_root / "ipc"),
        "EXASERVE_QUALIFIED_PYTHON": sys.executable,
        "EXASERVE_QUALIFIED_PYTHON_SHA256": "a" * 64,
        "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_COMPAT_PROFILE_ID": plan.compatibility_profile_hash,
        "EXASERVE_COMPAT_MANIFEST_HASH": plan.manifest_hash,
        "EXASERVE_COMPAT_SOURCES_NODE_PROFILE": plan.compatibility_profile_hash,
        "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST": plan.manifest_hash,
        "EXASERVE_COMPAT_OVERLAY_ROOT": str(runtime_root / "python" / "overlay"),
        "EXASERVE_PLAN_PATH": str(runtime_root / "run" / "deployment.plan.json"),
        "EXASERVE_SITE_PROFILE_PATH": str(runtime_root / "run" / "site.profile.json"),
        "EXASERVE_ALLOCATION_BINDING_PATH": (str(runtime_root / "run" / "allocation_binding.json")),
        "PYTHONPATH": str(runtime_root / "python"),
        "PYTHONNOUSERSITE": "1",
        "PYTHONSAFEPATH": "1",
        "HOME": str(state_root / "home"),
        "TMPDIR": str(state_root / "tmp"),
    }.items():
        monkeypatch.setenv(key, value)
    return plan, bound


def test_dense_haproxy_model_is_one_application():
    plan = _plan()
    model = plan.models[0]

    assert plan.uses_single_serve_application(model) is True
    assert plan.node_grouped_null_application_groups(model) == ()
    assert serve_application_layout(plan) == "single_application"
    assert planned_application_names(plan) == planned_proxy_anchor_names(plan) | {model.route_name}


@pytest.mark.parametrize(
    "models",
    [
        # half of the GPUs: one shared placement template cannot name the slots
        [_model(num_replicas=12)],
        # two-device replicas: Ray's device assignment need not match a planned pair
        [_model(tensor_parallel_size=2, num_replicas=12)],
        # two models: one application per replica keeps their slots apart
        [_model(num_replicas=12), _model(model_id="n", num_replicas=12)],
    ],
)
def test_other_layouts_keep_one_application_per_replica(models):
    plan = _plan(models=models)

    for model in plan.models:
        assert plan.uses_single_serve_application(model) is False
    assert serve_application_layout(plan) == "per_replica"
    expected = set(planned_proxy_anchor_names(plan))
    for model in plan.models:
        expected.update(f"{model.route_name}_r{index}" for index in range(model.num_replicas))
    assert planned_application_names(plan) == expected


def test_dense_null_compute_keeps_its_node_groups():
    plan = _plan(null_compute=True)
    model = plan.models[0]

    assert plan.uses_single_serve_application(model) is False
    assert len(plan.node_grouped_null_application_groups(model)) == 2
    assert serve_application_layout(plan) == "node_grouped_null"


def test_gateway_bypass_uses_node_proxies_for_one_application():
    from eval.lib.backends.ray import _canonical_direct_replica_urls

    addresses = {0: "10.0.0.1", 1: "10.0.0.2"}
    dense = _plan()
    assert _canonical_direct_replica_urls(dense, addresses, 8000) is None

    sparse = _plan(models=[_model(num_replicas=12)])
    urls = _canonical_direct_replica_urls(sparse, addresses, 8000)
    route = sparse.models[0].route_name
    assert len(urls) == 12
    assert all(f":8000/{route}_r" in url for url in urls)


def test_dense_graph_runs_one_deployment_with_every_replica(monkeypatch, request):
    from exaserve import server

    plan, bound = _bound_plan(monkeypatch, request)
    deploy_calls = []
    runs = []

    def fake_deploy_model(*args, **kwargs):
        deploy_calls.append(kwargs)
        return "deployment", args[0].model_id

    def fail_run_many(*_args, **_kwargs):
        raise AssertionError("the dense layout must not create one application per replica")

    monkeypatch.setattr(server, "deploy_model", fake_deploy_model)
    monkeypatch.setattr(
        server.serve, "run", lambda deployment, **kwargs: runs.append((deployment, kwargs))
    )
    monkeypatch.setattr(server.serve, "run_many", fail_run_many, raising=False)
    monkeypatch.setattr(server.tracer, "phase", lambda *_args, **_kwargs: nullcontext())

    server.deploy_from_canonical_binding(plan, {}, bound)

    assert len(deploy_calls) == 1
    assert deploy_calls[0]["replica_index"] == -1
    assert deploy_calls[0]["planned_placement"] is None
    assert deploy_calls[0]["native_replicas"] == 24
    assert runs == [("deployment", {"name": plan.models[0].route_name, "route_prefix": "/"})]


def test_dense_gateway_routes_to_node_proxies_without_replica_routes(
    monkeypatch, tmp_path, request
):
    plan, _bound = _bound_plan(monkeypatch, request)
    root = CompositionRoot(plan=plan, generation=7, run_dir=str(tmp_path), log=lambda *_: None)
    root.bind_allocation(["n0", "n1"], "job")
    endpoints = root._gateway_backend_endpoints()

    assert len(endpoints) == 2
    assert {endpoint.host for endpoint in endpoints} == {"n0", "n1"}
    assert {endpoint.replica_routes for endpoint in endpoints} == {0}
    assert {endpoint.path_prefix for endpoint in endpoints} == {""}
