from __future__ import annotations

import yaml
import pytest

from exaserve.proxy.base import BackendEndpoint
from exaserve.proxy.litellm_proxy import LiteLLMProxy


def _model_list(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))["model_list"]


def _config(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_litellm_targets_bound_replica_routes(tmp_path):
    path = LiteLLMProxy().generate_config(
        [
            BackendEndpoint("node-a", 8000, "org/model", "/org--model", 2),
            BackendEndpoint("node-b", 8000, "org/model", "/org--model", 2),
        ],
        tmp_path,
    )

    entries = _model_list(path)
    assert [entry["model_name"] for entry in entries] == ["org/model"] * 4
    assert {entry["litellm_params"]["api_base"] for entry in entries} == {
        "http://node-a:8000/org--model_r0/v1",
        "http://node-a:8000/org--model_r1/v1",
        "http://node-b:8000/org--model_r0/v1",
        "http://node-b:8000/org--model_r1/v1",
    }


def test_litellm_preserves_single_application_route(tmp_path):
    path = LiteLLMProxy().generate_config(
        [BackendEndpoint("node-a", 8000, "org/model", "/org--model")],
        tmp_path,
    )

    assert _model_list(path)[0]["litellm_params"]["api_base"] == (
        "http://node-a:8000/org--model/v1"
    )


def test_litellm_disables_runtime_huggingface_downloads_by_default(tmp_path):
    path = LiteLLMProxy().generate_config(
        [BackendEndpoint("node-a", 8000, "org/model", "/org--model")],
        tmp_path,
    )

    assert _config(path)["litellm_settings"] == {"disable_hf_tokenizer_download": True}


def test_litellm_offline_tokenizer_policy_is_strict(tmp_path):
    endpoint = [BackendEndpoint("node-a", 8000, "org/model", "/org--model")]
    path = LiteLLMProxy().generate_config(
        endpoint,
        tmp_path / "enabled",
        disable_hf_tokenizer_download=False,
    )
    assert _config(path)["litellm_settings"] == {"disable_hf_tokenizer_download": False}

    with pytest.raises(ValueError, match="must be boolean"):
        LiteLLMProxy().generate_config(
            endpoint,
            tmp_path / "invalid",
            disable_hf_tokenizer_download="true",
        )
