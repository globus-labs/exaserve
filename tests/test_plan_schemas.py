"""Canonical plan compiler regression coverage after the schema cutover."""

from __future__ import annotations

import dataclasses

import pytest

from exaserve.plan import PlanError, compile_deployment_plan


def _raw(**overrides):
    raw = {
        "num_nodes": 2,
        "model_storage_path": "/lus/flare/models",
        "validation_mode": True,
        "exposure": {"mode": "DIRECT_VALIDATION"},
        "gateway": None,
        "models": [
            {
                "model_id": "meta-llama/Meta-Llama-3-8B-Instruct",
                "tensor_parallel_size": 1,
                "pipeline_parallel_size": 1,
                "max_model_len": 2048,
                "size": 8,
            }
        ],
    }
    raw.update(overrides)
    return raw


def test_string_false_is_rejected_instead_of_coerced():
    raw = _raw()
    raw["models"][0]["enforce_eager"] = "false"
    with pytest.raises(PlanError, match="must be a boolean"):
        compile_deployment_plan(raw)


def test_invalid_num_replicas_rejected_not_silently_auto():
    raw = _raw()
    raw["models"][0]["num_replicas"] = "many"
    with pytest.raises(PlanError, match=r"models\[0\]\.num_replicas.*integer"):
        compile_deployment_plan(raw)


def test_unknown_model_keys_name_the_canonical_path():
    raw = _raw()
    raw["models"][0]["max_model_length"] = 4096
    with pytest.raises(PlanError, match=r"deployment.models\[0\].*max_model_length"):
        compile_deployment_plan(raw)


def test_derived_model_identity_collisions_rejected():
    raw = _raw(
        models=[
            {"model_id": "a/b--c", "size": 1, "max_model_len": 64},
            {"model_id": "a--b/c", "size": 1, "max_model_len": 64},
        ]
    )
    with pytest.raises(PlanError, match=r"identity collision \(storage_name\)"):
        compile_deployment_plan(raw)


@pytest.mark.parametrize("field", ["model_storage_path", "local_stage_path"])
def test_compiler_rejects_parent_traversal_in_storage_paths(field):
    with pytest.raises(PlanError, match=rf"deployment\.{field}.*parent traversal"):
        compile_deployment_plan(_raw(**{field: "/tmp/cache/../escape"}))


def test_plan_is_frozen_with_stable_hash_and_exact_slots():
    plan_a = compile_deployment_plan(_raw())
    plan_b = compile_deployment_plan(_raw())
    assert plan_a.deployment_plan_hash == plan_b.deployment_plan_hash
    assert all(model.num_replicas == len(model.replicas) for model in plan_a.models)
    assert any(item.role == "replica" for item in plan_a.receipt_requirements)
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan_a.num_nodes = 4  # type: ignore[misc]


def test_null_compute_does_not_invent_engine_process_receipts():
    plan = compile_deployment_plan(_raw(runtime={"null_compute": True}), deployment_id="null")
    assert any(item.role == "replica" for item in plan.receipt_requirements)
    assert not any(item.role.startswith("engine_") for item in plan.receipt_requirements)


def test_nonproduction_gateway_requires_validation_mode():
    raw = _raw(
        gateway={"kind": "pingora", "port": 4001},
        exposure={"mode": "PROXIED_INTERNAL"},
    )
    plan = compile_deployment_plan(raw)
    assert plan.gateway is not None and plan.gateway.kind == "pingora"
    assert plan.validation_mode is True


@pytest.mark.parametrize(
    ("field", "value"),
    [("request_mode", "arbitrary"), ("streaming_mode", "sometimes")],
)
def test_workload_protocol_envelope_fields_are_closed_enums(field, value):
    raw = _raw()
    raw[field] = value
    with pytest.raises(PlanError, match=field):
        compile_deployment_plan(raw)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("stats_retention", 0),
        ("stats_retention", 10_001),
        ("stats_push_period_s", 0),
        ("stats_sample_cap", 10_001),
    ],
)
def test_runtime_telemetry_limits_are_bounded(field, value):
    with pytest.raises(PlanError, match=field):
        compile_deployment_plan(_raw(runtime={field: value}))


def test_runtime_telemetry_policy_changes_the_plan_identity():
    default = compile_deployment_plan(_raw())
    tuned = compile_deployment_plan(_raw(runtime={"stats_retention": 1999}))
    assert default.deployment_plan_hash != tuned.deployment_plan_hash
