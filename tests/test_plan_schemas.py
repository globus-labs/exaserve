"""WP1 acceptance (AC-PLAN-01 core, PR-006, PR-007, ADR-000 envelope)."""

from __future__ import annotations

import dataclasses

import pytest

from exaserve.plan import (
    PlanError,
    ScaleEnvelope,
    compile_deployment_plan,
    from_legacy_yaml,
)


def _raw(**overrides):
    base = {
        "model_deployment_config": {
            "num_nodes": 2,
            "model_storage_path": "/lus/models",
            "model_configs": [
                {"model_id": "meta-llama/Meta-Llama-3-8B-Instruct",
                 "tensor_parallel_size": 1, "pipeline_parallel_size": 1,
                 "max_model_len": 2048, "size": 8},
            ],
        },
        "proxy_config": {"type": "none"},
    }
    for key, value in overrides.items():
        base[key] = value
    return base


def test_string_false_is_false_not_true():  # PR-006 flagship
    raw = _raw()
    raw["model_deployment_config"]["model_configs"][0]["enforce_eager"] = "false"
    plan = compile_deployment_plan(raw)
    assert plan.models[0].enforce_eager is False


def test_invalid_num_replicas_rejected_not_silently_auto():  # PR-006
    raw = _raw()
    raw["model_deployment_config"]["model_configs"][0]["num_replicas"] = "many"
    with pytest.raises(PlanError, match=r"num_replicas.*expected an integer"):
        compile_deployment_plan(raw)


def test_unknown_keys_rejected_with_path():  # PR-006
    raw = _raw()
    raw["model_deployment_config"]["model_configs"][0]["max_model_length"] = 4096
    with pytest.raises(PlanError, match=r"model_configs\[0\].*max_model_length"):
        compile_deployment_plan(raw)


def test_derived_model_identity_collisions_rejected():  # PR-007
    raw = _raw()
    raw["model_deployment_config"]["model_configs"] = [
        {"model_id": "a/b--c", "size": 1, "max_model_len": 64},
        {"model_id": "a--b/c", "size": 1, "max_model_len": 64},
    ]
    with pytest.raises(PlanError, match="storage_name"):
        compile_deployment_plan(raw)

    raw["model_deployment_config"]["model_configs"] = [
        {"model_id": "a.b/c", "size": 1, "max_model_len": 64},
        {"model_id": "a-b/c", "size": 1, "max_model_len": 64},
    ]
    with pytest.raises(PlanError, match="route_name"):
        compile_deployment_plan(raw)


def test_scheduler_nodes_inherits_and_explicit_mismatch_rejected():  # AC-PLAN-01
    plan = compile_deployment_plan(_raw())
    assert plan.scheduler.nodes == plan.num_nodes == 2  # omitted inherits

    raw = _raw(scheduler={"type": "pbs", "nodes": 1})
    with pytest.raises(PlanError, match="reservation_topology"):
        compile_deployment_plan(raw)

    raw = _raw(scheduler={"type": "pbs", "nodes": 4,
                          "reservation_topology": "oversized-control"})
    with pytest.raises(PlanError, match="qualification|supported"):
        # 4 > supported_max default 2 without validation mode
        compile_deployment_plan(raw)

    with pytest.raises(PlanError, match="nodes must|minimum"):
        compile_deployment_plan(_raw(scheduler={"type": "pbs", "nodes": 0}))


def test_envelope_supported_max_vs_validation_mode():  # AC-SCALE-01 slice
    raw = _raw()
    raw["model_deployment_config"]["num_nodes"] = 16
    with pytest.raises(PlanError, match="supported maximum"):
        compile_deployment_plan(raw)
    plan = compile_deployment_plan(
        raw, envelope=ScaleEnvelope(validation_mode=True))
    assert plan.num_nodes == 16
    raw["model_deployment_config"]["num_nodes"] = 128
    with pytest.raises(PlanError, match="qualification target"):
        compile_deployment_plan(raw, envelope=ScaleEnvelope(validation_mode=True))


def test_pp_exceeding_nodes_rejected_at_compile_time():  # S03 attempt-1 lesson
    raw = _raw()
    raw["model_deployment_config"]["num_nodes"] = 1
    raw["model_deployment_config"]["model_configs"][0]["pipeline_parallel_size"] = 2
    with pytest.raises(PlanError, match="one PP stage per node"):
        compile_deployment_plan(raw)


def test_plan_is_frozen_with_stable_hash():
    plan_a = compile_deployment_plan(_raw())
    plan_b = compile_deployment_plan(_raw())
    assert plan_a.plan_hash == plan_b.plan_hash and len(plan_a.plan_hash) == 64
    with pytest.raises(dataclasses.FrozenInstanceError):
        plan_a.num_nodes = 4  # type: ignore[misc]
    raw = _raw()
    raw["model_deployment_config"]["model_configs"][0]["max_model_len"] = 4096
    assert compile_deployment_plan(raw).plan_hash != plan_a.plan_hash


def test_benchmark_gateways_are_marked():  # PR-025 contract surface
    plan = compile_deployment_plan(_raw(proxy_config={"type": "pingora"}))
    assert plan.gateway.benchmark_only is True
    plan = compile_deployment_plan(_raw(proxy_config={"type": "haproxy"}))
    assert plan.gateway.benchmark_only is False


def test_legacy_example_config_compiles():
    plan = from_legacy_yaml("examples/config.direct.yaml")
    assert plan.num_nodes == 1
    assert plan.models[0].storage_name == "meta-llama--Meta-Llama-3-8B-Instruct"
    assert plan.gateway.type == "none"
    assert plan.source_path.endswith("config.direct.yaml")
