"""Bounded diagnostics for public Serve status and replica startup phases."""

from __future__ import annotations

from contextlib import nullcontext
import json
import logging
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


def test_deployment_diagnostic_strips_ansi_and_preserves_traceback_head_and_tail():
    traceback = (
        "The deployment failed to start 3 times in a row. Error:\n"
        "\x1b[36mray::ServeReplica.initialize_and_get_metadata()\x1b[0m\n"
        + "frame in ray internals\n"
        * 200
        + "RuntimeError: [EngineWorker pid=42] startup phase backend_create failed: "
        "RuntimeError: exact-root-cause"
    )
    failed = SimpleNamespace(
        applications={
            "failed": SimpleNamespace(
                status=_State("DEPLOY_FAILED"),
                message="application failed",
                deployments={
                    "EngineWorker": SimpleNamespace(
                        status=_State("DEPLOY_FAILED"),
                        message=traceback,
                    )
                },
            )
        }
    )

    encoded = server.serialize_public_serve_status(failed)
    message = json.loads(encoded)["applications"][0]["deployments"][0]["message"]

    assert len(encoded) <= 4096
    assert message.startswith("The deployment failed to start 3 times")
    assert "...[middle truncated]..." in message
    assert "startup phase backend_create failed" in message
    assert message.endswith("RuntimeError: exact-root-cause")
    assert "\x1b" not in message
    assert "[36m" not in message


def test_head_tail_diagnostic_scans_unretained_middle_for_secrets():
    detail = "safe head " + "x" * 1000 + " API_KEY=must-not-appear " + "safe tail"

    assert (
        server._bounded_diagnostic_text(
            detail,
            limit=128,
            preserve_tail=True,
        )
        == "<redacted sensitive detail>"
    )


def test_async_engine_args_registry_failure_survives_phase_and_status_tail(capsys):
    from exaserve.engines import vllm as vllm_module

    class FakeArgs:
        def __init__(self):
            try:
                raise RuntimeError("registry subprocess stderr exact-root")
            except RuntimeError:
                logging.getLogger("vllm.model_executor.models.registry").exception(
                    "Error in inspecting model architecture"
                )
            raise ValueError("Model architectures failed to be inspected")

    captured_error = None
    try:
        with vllm_module._capture_registry_diagnostics() as capture:
            try:
                FakeArgs()
            except ValueError as engine_error:
                capture.attach_to(engine_error)
                raise
    except ValueError as engine_error:
        captured_error = engine_error
    else:  # pragma: no cover - fixture contract
        raise AssertionError("FakeArgs did not fail")

    assert captured_error is not None
    with pytest.raises(RuntimeError) as phase_failure:
        server._raise_engine_startup_failure("backend_create", captured_error)
    phase_message = str(phase_failure.value)
    ray_message = (
        "The deployment failed to start 3 times in a row. Error:\n"
        + "frame in ray internals\n" * 200
        + phase_message
    )
    status = SimpleNamespace(
        applications={
            "failed": SimpleNamespace(
                status=_State("DEPLOY_FAILED"),
                message="application failed",
                deployments={
                    "EngineWorker": SimpleNamespace(
                        status=_State("DEPLOY_FAILED"),
                        message=ray_message,
                    )
                },
            )
        }
    )
    encoded = server.serialize_public_serve_status(status)
    message = json.loads(encoded)["applications"][0]["deployments"][0]["message"]

    assert len(phase_message) < 512
    assert "startup phase backend_create failed" in message
    assert "Model architectures failed to be inspected" in message
    assert "registry subprocess stderr exact-root" in message
    assert phase_message in capsys.readouterr().out


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


@pytest.mark.parametrize("head_only", [False, True])
def test_serve_run_failure_includes_tail_preserved_phase_for_every_branch(
    monkeypatch, capsys, head_only
):
    model = SimpleNamespace(route_name="model-route", model_id="model-a")
    placement = SimpleNamespace(owner_rank=0, replica_index=0)
    bound_model = SimpleNamespace(model=model, assigned_replicas=1, replicas=[placement])
    config = SimpleNamespace(
        models=[model],
        uses_head_only_serve_proxy=lambda: head_only,
        node_grouped_null_application_groups=lambda _model: (),
    )
    binding = SimpleNamespace(models=[bound_model])
    ray_message = (
        "The deployment failed to start 3 times in a row. Error:\n"
        "\x1b[36mray::ServeReplica.initialize_and_get_metadata()\x1b[0m\n"
        + "frame in ray internals\n"
        * 200
        + "RuntimeError: [EngineWorker pid=42] startup phase backend_create failed: "
        "RuntimeError: exact-root-cause"
    )
    status = SimpleNamespace(
        applications={
            "model-route": SimpleNamespace(
                status=_State("DEPLOY_FAILED"),
                message="application failed",
                deployments={
                    "EngineWorker": SimpleNamespace(
                        status=_State("DEPLOY_FAILED"),
                        message=ray_message,
                    )
                },
            )
        }
    )

    monkeypatch.setattr(
        server,
        "deploy_model",
        lambda *_args, **_kwargs: (object(), "model-a"),
    )
    monkeypatch.setattr(server.serve, "status", lambda: status)
    monkeypatch.setattr(
        server.serve,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            RuntimeError("AWS_SECRET_ACCESS_KEY=must-not-appear")
        ),
    )
    monkeypatch.setattr(server.tracer, "phase", lambda *_args, **_kwargs: nullcontext())

    with pytest.raises(RuntimeError) as caught:
        server.deploy_from_canonical_binding(config, {}, binding)

    message = str(caught.value)
    output = capsys.readouterr().out
    expected_context = "native HeadOnly" if head_only else "canonical single-replica"
    assert expected_context in message
    assert "startup phase backend_create failed" in message
    assert "exact-root-cause" in message
    assert "<redacted sensitive detail>" in message
    assert "must-not-appear" not in message
    assert "\x1b" not in message
    assert "[36m" not in message
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
