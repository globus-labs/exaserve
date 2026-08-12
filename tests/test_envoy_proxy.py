from __future__ import annotations

import yaml

from exaserve.proxy.base import BackendEndpoint
from exaserve.proxy.envoy_proxy import EnvoyProxy, _safe_name


def _config(path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_envoy_rewrites_canonical_paths_to_bound_replica_routes(tmp_path):
    path = EnvoyProxy().generate_config(
        [
            BackendEndpoint("node-a", 8000, "org/model", "/org--model", 12),
            BackendEndpoint("node-b", 8000, "org/model", "/org--model", 12),
        ],
        tmp_path,
    )
    config = _config(path)
    manager = config["static_resources"]["listeners"][0]["filter_chains"][0]["filters"][0][
        "typed_config"
    ]
    filters = manager["http_filters"]
    assert [item["name"] for item in filters] == [
        "envoy.filters.http.lua",
        "envoy.filters.http.router",
    ]
    lua = filters[0]["typed_config"]["default_source_code"]["inline_string"]
    assert 'prefix = "/org--model", replicas = 12, root = true' in lua
    assert 'headers:replace(":path", prefix .. "_r"' in lua


def test_envoy_multimodel_routes_rewritten_replica_prefixes(tmp_path):
    path = EnvoyProxy().generate_config(
        [
            BackendEndpoint("node-a", 8000, "org/model", "/org--model", 2),
            BackendEndpoint("node-a", 8000, "other/model", "/other--model", 3),
        ],
        tmp_path,
    )
    routes = _config(path)["static_resources"]["listeners"][0]["filter_chains"][0]["filters"][0][
        "typed_config"
    ]["route_config"]["virtual_hosts"][0]["routes"]
    routed = {(item.get("match") or {}).get("prefix"): item for item in routes}
    assert routed["/org--model_r"]["route"]["cluster"] == _safe_name("org/model")
    assert routed["/other--model_r"]["route"]["cluster"] == _safe_name("other/model")


def test_envoy_cluster_names_are_collision_resistant(tmp_path):
    path = EnvoyProxy().generate_config(
        [
            BackendEndpoint("node-a", 8000, "a/b", "/a--b"),
            BackendEndpoint("node-a", 8000, "a-b", "/a-b"),
        ],
        tmp_path,
    )
    names = [item["name"] for item in _config(path)["static_resources"]["clusters"]]
    assert len(names) == len(set(names)) == 2


def test_envoy_retires_upstream_connections_before_ray_serve(tmp_path):
    path = EnvoyProxy().generate_config(
        [BackendEndpoint("node-a", 8000, "org/model", "/org--model")],
        tmp_path,
        upstream_idle_timeout_s=60,
    )
    cluster = _config(path)["static_resources"]["clusters"][0]
    protocol_options = cluster["typed_extension_protocol_options"][
        "envoy.extensions.upstreams.http.v3.HttpProtocolOptions"
    ]
    assert protocol_options["common_http_protocol_options"]["idle_timeout"] == "60s"
    assert protocol_options["explicit_http_config"] == {"http_protocol_options": {}}
