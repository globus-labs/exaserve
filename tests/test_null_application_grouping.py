"""Dense null-compute replicas share node apps without losing exact slots."""

from __future__ import annotations

from contextlib import nullcontext
import sys
from exaserve.composition import CompositionRoot
from exaserve.plan.compiler import compile_deployment_plan
from exaserve.plan.contracts import build_allocation_binding
from exaserve.plan.runtime_binding import (
    LiveNodeInventory,
    bind_runtime_deployment,
)
from exaserve.site import default_site_profile


def _bound_plan(monkeypatch):
    plan = compile_deployment_plan(
        {
            "num_nodes": 2,
            "validation_mode": True,
            "runtime": {"null_compute": True},
            "gateway": {"kind": "haproxy", "port": 4001},
            "models": [
                {
                    "model_id": "m",
                    "tensor_parallel_size": 1,
                    "num_replicas": 24,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
        },
        site=default_site_profile(),
        deployment_id="node-grouped-null",
    )
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
        "EXASERVE_LOCAL_RUNTIME_ROOT": "/tmp/exaserve-test-runtime",
        "EXASERVE_LOCAL_STATE_ROOT": "/tmp/exaserve-test-state",
        "EXASERVE_QUALIFIED_PYTHON": sys.executable,
        "EXASERVE_QUALIFIED_PYTHON_SHA256": "a" * 64,
        "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_COMPAT_PROFILE_ID": plan.compatibility_profile_hash,
        "EXASERVE_COMPAT_MANIFEST_HASH": plan.manifest_hash,
        "EXASERVE_COMPAT_SOURCES_NODE_PROFILE": plan.compatibility_profile_hash,
        "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST": plan.manifest_hash,
        "EXASERVE_COMPAT_OVERLAY_ROOT": "/tmp/exaserve-test-runtime/python/overlay",
        "EXASERVE_PLAN_PATH": "/tmp/exaserve-test-runtime/run/deployment.plan.json",
        "EXASERVE_SITE_PROFILE_PATH": "/tmp/exaserve-test-runtime/run/site.profile.json",
        "EXASERVE_ALLOCATION_BINDING_PATH": (
            "/tmp/exaserve-test-runtime/run/allocation_binding.json"
        ),
        "PYTHONPATH": "/tmp/exaserve-test-runtime/python",
        "PYTHONNOUSERSITE": "1",
        "PYTHONSAFEPATH": "1",
        "HOME": "/tmp/exaserve-test-state/home",
        "TMPDIR": "/tmp/exaserve-test-state/tmp",
    }.items():
        monkeypatch.setenv(key, value)
    return plan, bound


def test_node_group_graph_has_one_twelve_replica_app_per_rank(monkeypatch):
    from exaserve import server

    plan, bound = _bound_plan(monkeypatch)
    deploy_calls = []
    run_targets = []

    def fake_deploy_model(*args, **kwargs):
        deploy_calls.append(kwargs)
        return object(), args[0].model_id

    class RunTarget:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            run_targets.append(self)

    observed_run_many = []
    monkeypatch.setattr(server, "deploy_model", fake_deploy_model)
    monkeypatch.setattr(server.serve, "RunTarget", RunTarget)
    monkeypatch.setattr(
        server.serve,
        "run_many",
        lambda targets, **kwargs: observed_run_many.append((targets, kwargs)),
    )
    monkeypatch.setattr(server.tracer, "phase", lambda *_args, **_kwargs: nullcontext())

    server.deploy_from_canonical_binding(plan, {}, bound)

    assert len(deploy_calls) == 2
    assert [call["native_replicas"] for call in deploy_calls] == [12, 12]
    assert [call["replica_index"] for call in deploy_calls] == [-1, -1]
    assert all(call["dynamic_replica_binding"] is True for call in deploy_calls)
    assert [call["planned_placement"].owner_rank for call in deploy_calls] == [0, 1]
    assert [target.name for target in run_targets] == [
        f"{plan.models[0].route_name}_g0",
        f"{plan.models[0].route_name}_g1",
    ]
    assert [target.route_prefix for target in run_targets] == [
        f"/{plan.models[0].route_name}_g0",
        f"/{plan.models[0].route_name}_g1",
    ]
    assert len(observed_run_many) == 1
    assert observed_run_many[0][1] == {"wait_for_applications_running": True}


def test_node_group_actor_options_pin_rank_and_resolve_replica_dynamically(monkeypatch):
    from exaserve import server

    plan, bound = _bound_plan(monkeypatch)
    captured = {}

    class BoundWorker:
        def bind(self, **kwargs):
            captured["bind"] = kwargs
            return "deployment"

    class FakeEngineWorker:
        @staticmethod
        def options(**kwargs):
            captured["options"] = kwargs
            return BoundWorker()

    monkeypatch.setattr(server, "EngineWorker", FakeEngineWorker)
    placement = bound.models[0].replicas[0]
    deployment, _model_id = server.deploy_model(
        plan.models[0],
        {},
        plan,
        replica_index=-1,
        planned_placement=placement,
        native_replicas=12,
        dynamic_replica_binding=True,
    )

    assert deployment == "deployment"
    options = captured["options"]
    assert options["num_replicas"] == 12
    actor = options["ray_actor_options"]
    assert actor["num_gpus"] == 1
    assert actor["resources"] == {"node:0": 0.001}
    assert actor["runtime_env"]["env_vars"]["EXASERVE_RECEIPT_RANK"] == "0"
    assert captured["bind"]["replica_index"] == -1


def test_node_group_gateway_keeps_node_backends_and_group_route_count(monkeypatch, tmp_path):
    plan, _bound = _bound_plan(monkeypatch)
    root = CompositionRoot(plan=plan, generation=7, run_dir=str(tmp_path), log=lambda *_: None)
    root.bind_allocation(["n0", "n1"], "job")
    endpoints = root._gateway_backend_endpoints()

    assert len(endpoints) == 2
    assert {endpoint.host for endpoint in endpoints} == {"n0", "n1"}
    assert {endpoint.replica_routes for endpoint in endpoints} == {2}
    assert {endpoint.route_suffix for endpoint in endpoints} == {"_g"}
