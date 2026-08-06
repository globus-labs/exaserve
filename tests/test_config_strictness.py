"""PR-006: a config either means what it says or fails to load."""

from __future__ import annotations

import pytest

from exaserve.schemas import (
    _deployment_config_from_dict,
    _model_config_from_dict,
    load_deployment_config,
)


def _model(**kw):
    base = {"model_id": "m"}
    base.update(kw)
    return base


def test_a_typo_in_a_deployment_key_is_refused_not_defaulted():
    """`num_node: 64` silently left the deployment at 1 node."""
    with pytest.raises(ValueError, match="unknown key"):
        _deployment_config_from_dict({"num_node": 64, "model_configs": []})


def test_a_typo_in_a_model_key_is_refused():
    with pytest.raises(ValueError, match="unknown key"):
        _model_config_from_dict(_model(tensor_parallel=2))


def test_the_error_names_what_was_wrong_and_what_is_accepted():
    with pytest.raises(ValueError) as excinfo:
        _deployment_config_from_dict({"num_node": 64})
    message = str(excinfo.value)
    assert "num_node" in message and "num_nodes" in message


def test_a_missing_model_id_is_refused():
    with pytest.raises(ValueError, match="model_id"):
        _model_config_from_dict({"tensor_parallel_size": 2})


@pytest.mark.parametrize("value", [8.9, "eight", True, [], {}])
def test_a_non_integer_field_never_silently_truncates(value):
    with pytest.raises(ValueError):
        _model_config_from_dict(_model(num_cpus_per_replica=value))


def test_an_omitted_optional_field_takes_its_default():
    assert _model_config_from_dict(
        _model(num_cpus_per_replica=None)).num_cpus_per_replica == 4


def test_an_integral_float_is_accepted():
    """YAML writes 4.0 for an int often enough that refusing it is hostile."""
    assert _model_config_from_dict(_model(max_model_len=4096.0)).max_model_len == 4096


def test_a_boolean_in_a_numeric_field_is_refused():
    """True is an int in Python; it must not read as 1 node."""
    with pytest.raises(ValueError, match="boolean"):
        _deployment_config_from_dict({"num_nodes": True, "model_configs": []})


def test_a_quoted_false_does_not_become_true():
    """The original PR-006 defect: any non-empty string was truthy."""
    assert _deployment_config_from_dict(
        {"collect_stats": "false", "model_configs": []}).collect_stats is False
    assert _deployment_config_from_dict(
        {"collect_stats": "yes", "model_configs": []}).collect_stats is True
    with pytest.raises(ValueError, match="boolean"):
        _deployment_config_from_dict({"collect_stats": "maybe", "model_configs": []})


def test_a_valid_config_still_loads(tmp_path):
    path = tmp_path / "c.yaml"
    path.write_text(
        "model_deployment_config:\n"
        "  num_nodes: 2\n"
        "  num_gpus_per_node: 12\n"
        "  model_storage_path: /models\n"
        "  local_stage_path: /tmp/hf\n"
        "  model_configs:\n"
        "    - model_id: meta-llama/Meta-Llama-3-8B-Instruct\n"
        "      tensor_parallel_size: 1\n"
        "      max_model_len: 4096\n"
        "      size: 8\n")
    config = load_deployment_config(str(path))
    assert config.num_nodes == 2 and len(config.model_configs) == 1
    assert config.model_configs[0].tensor_parallel_size == 1


def test_the_real_hardening_configs_still_load():
    """The configs used by the smoke harnesses must not regress."""
    import glob

    from importlib import resources  # noqa: F401

    loaded = 0
    for path in sorted(glob.glob("scripts/hardening/config.*.yaml")):
        load_deployment_config(path)
        loaded += 1
    assert loaded >= 1, "no hardening configs found to validate"
