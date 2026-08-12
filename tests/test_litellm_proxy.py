from __future__ import annotations

import yaml

from exaserve.proxy.base import BackendEndpoint
from exaserve.proxy.litellm_proxy import LiteLLMProxy


def _model_list(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))["model_list"]


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
