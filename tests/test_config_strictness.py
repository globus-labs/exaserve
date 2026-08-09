"""The canonical YAML input either means exactly what it says or fails."""

from __future__ import annotations

from pathlib import Path

import pytest

from exaserve.launcher import load_or_compile_plan
from exaserve.plan import PlanError, compile_deployment_plan


def _raw(**overrides):
    raw = {
        "num_nodes": 1,
        "num_gpus_per_node": 12,
        "validation_mode": True,
        "exposure": {"mode": "DIRECT_VALIDATION"},
        "gateway": None,
        "models": [{"model_id": "org/model", "max_model_len": 64, "size": 1}],
    }
    raw.update(overrides)
    return raw


def test_a_typo_in_a_deployment_key_is_refused_not_defaulted():
    with pytest.raises(PlanError, match="unknown key.*num_node"):
        compile_deployment_plan(_raw(num_node=64))


def test_a_typo_in_a_model_key_is_refused():
    raw = _raw()
    raw["models"][0]["tensor_parallel"] = 2
    with pytest.raises(PlanError, match="unknown key.*tensor_parallel"):
        compile_deployment_plan(raw)


def test_a_missing_model_id_is_refused():
    with pytest.raises(PlanError, match="model_id"):
        compile_deployment_plan(_raw(models=[{"max_model_len": 64, "size": 1}]))


@pytest.mark.parametrize("value", [8.9, "eight", True, [], {}])
def test_a_non_integer_field_never_silently_truncates(value):
    raw = _raw()
    raw["models"][0]["num_cpus_per_replica"] = value
    with pytest.raises(PlanError, match="integer"):
        compile_deployment_plan(raw)


def test_integral_float_and_boolean_are_not_accepted_as_integers():
    raw = _raw()
    raw["models"][0]["max_model_len"] = 4096.0
    with pytest.raises(PlanError, match="integer"):
        compile_deployment_plan(raw)
    raw["models"][0]["max_model_len"] = 4096
    raw["num_nodes"] = True
    with pytest.raises(PlanError, match="integer"):
        compile_deployment_plan(raw)


def test_quoted_booleans_are_rejected_instead_of_coerced():
    raw = _raw(collect_stats="false")
    with pytest.raises(PlanError, match="boolean"):
        compile_deployment_plan(raw)


def test_duplicate_yaml_keys_are_rejected_before_plan_compilation(tmp_path):
    path = tmp_path / "duplicate.yaml"
    path.write_text("num_nodes: 1\nnum_nodes: 2\n", encoding="utf-8")

    with pytest.raises(Exception, match="duplicate key 'num_nodes'"):
        load_or_compile_plan(str(path), deployment_id="duplicate-check")


def test_every_shipped_yaml_uses_the_canonical_schema():
    repo_root = Path(__file__).resolve().parents[1]
    paths = sorted((repo_root / "examples").glob("*.yaml")) + sorted(
        (repo_root / "scripts/hardening").glob("config.*.yaml")
    )
    assert paths
    for path in paths:
        plan = load_or_compile_plan(str(path), deployment_id="schema-check")
        assert plan.models, path


def test_retired_nested_schema_is_rejected_explicitly():
    with pytest.raises(PlanError, match="model_deployment_config"):
        compile_deployment_plan({"model_deployment_config": {"num_nodes": 1}})
