"""Bounded diagnostics for public Serve status and replica startup phases."""

from __future__ import annotations

from contextlib import nullcontext
import json
import threading
from types import SimpleNamespace

import pytest

pytest.importorskip("ray", exc_type=ImportError)
pytest.importorskip("fastapi", exc_type=ImportError)

from exaserve import server


class _State:
    def __init__(self, value: str):
        self.value = value


def _fake_serve_status():
    healthy = {
        f"healthy-{index}": SimpleNamespace(
            status=_State("RUNNING"),
            message="",
            deployments={
                "worker": SimpleNamespace(
                    status=_State("HEALTHY"),
                    message="",
                )
            },
        )
        for index in range(20)
    }
    healthy["failed-last"] = SimpleNamespace(
        status=_State("DEPLOY_FAILED"),
        message="OPENAI_API_KEY=must-not-appear " + "x" * 10_000,
        deployments={
            "EngineWorker": SimpleNamespace(
                status=_State("UNHEALTHY"),
                message="constructor failed on replica 7",
            )
        },
    )
    return SimpleNamespace(applications=healthy)


def test_public_serve_status_diagnostic_is_bounded_and_prioritizes_failure(monkeypatch):
    monkeypatch.setattr(server.serve, "status", _fake_serve_status)

    encoded = server.serialize_public_serve_status()
    payload = json.loads(encoded)

    assert len(encoded) <= 4096
    assert payload["application_count"] == 21
    assert payload["applications"][0] == {
        "deployment_count": 1,
        "deployments": [
            {
                "message": "constructor failed on replica 7",
                "name": "EngineWorker",
                "status": "UNHEALTHY",
            }
        ],
        "message": "<redacted sensitive detail>",
        "name": "failed-last",
        "omitted_deployment_count": 0,
        "status": "DEPLOY_FAILED",
    }
    assert payload["omitted_application_count"] > 0
    assert "must-not-appear" not in encoded


@pytest.mark.parametrize(
    "detail",
    [
        "OPENAI_API_KEY=must-not-appear",
        "AWS_SECRET_ACCESS_KEY=must-not-appear",
        "LITELLM_MASTER_KEY=must-not-appear",
        "Incorrect API key provided: sk-must-not-appear",
        "Authorization: Bearer must-not-appear",
        "access token must-not-appear",
    ],
)
def test_diagnostic_redaction_covers_identifier_and_prose_secret_labels(detail):
    assert server._bounded_diagnostic_text(detail) == "<redacted sensitive detail>"


def test_public_serve_status_diagnostic_has_a_finite_fetch_budget(monkeypatch):
    release = threading.Event()
    entered = threading.Event()

    def blocked_status():
        entered.set()
        release.wait(10)
        return _fake_serve_status()

    monkeypatch.setattr(server, "_SERVE_STATUS_FETCH_TIMEOUT_S", 0.01)
    monkeypatch.setattr(server.serve, "status", blocked_status)
    try:
        payload = json.loads(server.serialize_public_serve_status())
    finally:
        release.set()

    assert entered.is_set()
    assert payload["application_count"] is None
    assert payload["applications"] == []
    assert "TimeoutError" in payload["diagnostic_error"]
    assert "diagnostic budget" in payload["diagnostic_error"]


def test_run_many_failure_includes_and_prints_public_status(monkeypatch, capsys):
    model = SimpleNamespace(route_name="model-route", model_id="model-a")
    placement = SimpleNamespace(owner_rank=0, replica_index=0)
    bound_model = SimpleNamespace(model=model, assigned_replicas=1, replicas=[placement])
    config = SimpleNamespace(
        models=[model],
        uses_head_only_serve_proxy=lambda: False,
        node_grouped_null_application_groups=lambda _model: ((0, (0,)),),
    )
    binding = SimpleNamespace(models=[bound_model])

    class RunTarget:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

    monkeypatch.setattr(
        server,
        "deploy_model",
        lambda *_args, **_kwargs: (object(), "model-a"),
    )
    monkeypatch.setattr(server.serve, "RunTarget", RunTarget)
    monkeypatch.setattr(server.serve, "status", _fake_serve_status)
    monkeypatch.setattr(
        server.serve,
        "run_many",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("AWS_SECRET_ACCESS_KEY=must-not-appear " + "x" * 10_000)
        ),
    )
    monkeypatch.setattr(server.tracer, "phase", lambda *_args, **_kwargs: nullcontext())

    with pytest.raises(RuntimeError) as caught:
        server.deploy_from_canonical_binding(config, {}, binding)

    message = str(caught.value)
    output = capsys.readouterr().out
    assert "node-grouped null deployment failed" in message
    assert '"status":"DEPLOY_FAILED"' in message
    assert "serve_status=" in message
    assert "must-not-appear" not in message
    assert message in output
    assert caught.value.__cause__ is None


def _constructor_fixture(monkeypatch, *, phase: str):
    import exaserve.compat.activator as activator
    import exaserve.engines as engines
    import exaserve.model_staging as model_staging
    import exaserve.plan.io as plan_io

    logical_replica = SimpleNamespace(
        replica_index=0,
        planned_ranks=(0,),
        tensor_parallel_size=1,
        pipeline_parallel_size=1,
    )
    model = SimpleNamespace(model_id="model-a", replicas=(logical_replica,))
    requirements = (
        SimpleNamespace(receipt_requirement_id="replica-requirement", component_slot="replica"),
        SimpleNamespace(receipt_requirement_id="engine-requirement", component_slot="engine"),
    )
    plan = SimpleNamespace(
        models=(model,), receipt_requirements=requirements, local_stage_path="/tmp"
    )

    def fail():
        raise RuntimeError("LITELLM_MASTER_KEY=must-not-appear " + "x" * 10_000)

    monkeypatch.setattr(server.ray, "get_gpu_ids", lambda: [0])
    monkeypatch.setattr(
        plan_io,
        "load_deployment_plan",
        (lambda _path: fail()) if phase == "canonical_bind" else (lambda _path: plan),
    )
    monkeypatch.setattr(plan_io, "load_allocation_binding", lambda _path: object())
    monkeypatch.setattr(plan_io, "rank_for_node", lambda *_args: 0)
    monkeypatch.setattr(
        plan_io,
        "resolve_replica_receipt_requirement",
        lambda **kwargs: (
            "engine-requirement" if kwargs["role"] == "engine_core" else "replica-requirement"
        ),
    )
    monkeypatch.setattr(
        activator.CompatibilityActivator,
        "activate",
        (lambda _self, _role: fail()) if phase == "compat_activate" else lambda *_args: None,
    )
    monkeypatch.setattr(
        model_staging,
        "validate_node_local_tree",
        (lambda *_args, **_kwargs: fail()) if phase == "tree_validate" else lambda *_a, **_k: None,
    )

    class BrokenNullEngine:
        def __init__(self, **_kwargs):
            pass

        def create(self, _spec):
            fail()

    if phase == "backend_create":
        monkeypatch.setattr(engines, "NullEngine", BrokenNullEngine)

    for key, value in {
        "EXASERVE_PLAN_PATH": "/tmp/deployment.plan.json",
        "EXASERVE_ALLOCATION_BINDING_PATH": "/tmp/allocation_binding.json",
        "EXASERVE_DEPLOYMENT_ID": "diagnostic-test",
        "EXASERVE_GENERATION": "7",
    }.items():
        monkeypatch.setenv(key, value)

    return phase != "tree_validate"


@pytest.mark.parametrize(
    "phase",
    ["canonical_bind", "compat_activate", "tree_validate", "backend_create"],
)
def test_engine_constructor_failure_names_exact_bounded_phase(monkeypatch, capsys, phase):
    null_compute = _constructor_fixture(monkeypatch, phase=phase)
    worker_type = server.EngineWorker.func_or_class.__mro__[1]

    class WorkerForTest(worker_type):
        def __del__(self):
            return None

    worker = object.__new__(WorkerForTest)
    with pytest.raises(RuntimeError) as caught:
        worker_type.__init__(
            worker,
            model_id="model-a",
            replica_index=0,
            local_model_path="/tmp/model",
            null_compute=null_compute,
        )

    message = str(caught.value)
    output = capsys.readouterr().out
    assert f"startup phase {phase} failed" in message
    assert "<redacted sensitive detail>" in message
    assert "must-not-appear" not in message
    assert len(message) < 512
    assert message in output
    assert caught.value.__cause__ is None
