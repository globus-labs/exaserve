"""
Envoy backend implementation.

Generates an envoy.yaml that load-balances across Ray Serve HTTP proxies.
The composition root owns process launch.

Envoy is a high-performance C++ L7 proxy from the Istio/CNCF ecosystem.
Like HAProxy and NGINX it is OpenAI-agnostic; included as a third pure-LB
baseline. The default LB policy here is LEAST_REQUEST (the closest Envoy
analog to HAProxy's leastconn).

envoy must be installed and on PATH (see scripts/install_envoy.sh).
"""

import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path

from .base import (
    BackendEndpoint,
    ProxyBackend,
    reject_unknown_options,
    strict_int,
    strict_text,
    validate_endpoint,
)


class EnvoyProxy(ProxyBackend):
    """Render the Envoy artifact consumed by the composition root."""

    def generate_config(
        self,
        backends: list[BackendEndpoint],
        output_dir: Path,
        **options,
    ) -> Path:
        """
        Write envoy.yaml to output_dir. Returns the config path.

        Options (all optional):
            lb_policy (str):   "LEAST_REQUEST" | "ROUND_ROBIN" | "RANDOM".
                               Default: "LEAST_REQUEST".
            admin_port (int):  Envoy admin interface port (0 = disabled).
                               Default: 9902.
            concurrency (int): Worker thread count. Default: 0 (== nproc).
            request_timeout (int): Per-route timeout in seconds. Default: 330.
            connect_timeout (str): cluster connect timeout. Default: "5s".
            max_connections (int): Max upstream connections per cluster. Default: 50000.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        reject_unknown_options(
            options,
            {
                "admin_port",
                "concurrency",
                "connect_timeout",
                "lb_policy",
                "listen_port",
                "max_connections",
                "request_timeout",
            },
            "envoy",
        )

        lb_policy = strict_text(
            options.get("lb_policy", "LEAST_REQUEST"),
            "proxy.options.lb_policy",
            choices={"LEAST_REQUEST", "ROUND_ROBIN", "RANDOM"},
        )
        admin_port = strict_int(
            options.get("admin_port", 9902), "proxy.options.admin_port", minimum=0, maximum=65535
        )
        request_timeout = strict_int(
            options.get("request_timeout", 330),
            "proxy.options.request_timeout",
            minimum=1,
            maximum=86400,
        )
        connect_timeout = strict_text(
            options.get("connect_timeout", "5s"), "proxy.options.connect_timeout"
        )
        if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?(?:ms|s)", connect_timeout):
            raise ValueError("proxy.options.connect_timeout has invalid duration")
        max_connections = strict_int(
            options.get("max_connections", 50000),
            "proxy.options.max_connections",
            minimum=1,
            maximum=10_000_000,
        )
        listen_port = strict_int(
            options.get("listen_port", 4001), "proxy.options.listen_port", minimum=1, maximum=65535
        )

        by_model: dict[str, list[BackendEndpoint]] = defaultdict(list)
        for ep in backends:
            validate_endpoint(ep, "envoy")
            by_model[ep.model_id].append(ep)
        if not by_model:
            raise ValueError("Envoy requires at least one backend endpoint")

        # Build clusters (one per model).
        clusters = []
        for model_id, eps in by_model.items():
            cluster_name = _safe_name(model_id)
            # STRICT_DNS lets Envoy accept DNS hostnames (Aurora HSN endpoints
            # are addressed by `x...hsn.cm.aurora.alcf.anl.gov`, not by IP).
            # Envoy resolves at startup and periodically re-resolves.
            clusters.append(
                {
                    "name": cluster_name,
                    "type": "STRICT_DNS",
                    "connect_timeout": connect_timeout,
                    "lb_policy": lb_policy,
                    "dns_lookup_family": "V4_ONLY",
                    "circuit_breakers": {
                        "thresholds": [
                            {
                                "priority": "DEFAULT",
                                "max_connections": max_connections,
                                "max_pending_requests": max_connections,
                                "max_requests": max_connections,
                                "max_retries": 3,
                            }
                        ]
                    },
                    "load_assignment": {
                        "cluster_name": cluster_name,
                        "endpoints": [
                            {
                                "lb_endpoints": [
                                    {
                                        "endpoint": {
                                            "address": {
                                                "socket_address": {
                                                    "address": ep.host,
                                                    "port_value": ep.port,
                                                }
                                            }
                                        }
                                    }
                                    for ep in eps
                                ]
                            }
                        ],
                    },
                }
            )

        # Build routes.
        if len(by_model) == 1:
            cluster_name = _safe_name(next(iter(by_model)))
            routes = [
                {
                    "match": {"prefix": "/"},
                    "route": {"cluster": cluster_name, "timeout": f"{request_timeout}s"},
                }
            ]
        else:
            routes = []
            for model_id, eps in by_model.items():
                cluster_name = _safe_name(model_id)
                path_prefix = _shared_path_prefix(eps)
                replica_routes = _replica_route_count(eps, model_id)
                matchers = (
                    [{"prefix": f"{path_prefix}_r"}]
                    if replica_routes
                    else [{"path": path_prefix}, {"prefix": f"{path_prefix}/"}]
                )
                for matcher in matchers:
                    routes.append(
                        {
                            "match": matcher,
                            "route": {
                                "cluster": cluster_name,
                                "timeout": f"{request_timeout}s",
                            },
                        }
                    )
            routes.append(
                {
                    "match": {"prefix": "/"},
                    "direct_response": {
                        "status": 404,
                        "body": {"inline_string": "missing or unknown model route prefix\n"},
                    },
                }
            )

        http_filters = []
        replica_rewrite_lua = _replica_rewrite_lua(by_model)
        if replica_rewrite_lua is not None:
            # Envoy load-balances across node-local Serve proxies, while this
            # bounded Lua filter selects one canonical replica application and
            # rewrites /<model>/v1/... to /<model>_rN/v1/....  Without this,
            # every multi-replica request reaches an undeployed base route and
            # returns 404.
            http_filters.append(
                {
                    "name": "envoy.filters.http.lua",
                    "typed_config": {
                        "@type": "type.googleapis.com/envoy.extensions.filters.http.lua.v3.Lua",
                        "default_source_code": {"inline_string": replica_rewrite_lua},
                    },
                }
            )
        http_filters.append(
            {
                "name": "envoy.filters.http.router",
                "typed_config": {
                    "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router",
                },
            }
        )

        listener = {
            "name": "listener_main",
            "address": {
                "socket_address": {
                    "address": "0.0.0.0",
                    "port_value": listen_port,
                }
            },
            "filter_chains": [
                {
                    "filters": [
                        {
                            "name": "envoy.filters.network.http_connection_manager",
                            "typed_config": {
                                "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
                                "stat_prefix": "ingress_http",
                                # This benchmark does not consume Envoy's
                                # internal-request classification. Declare an
                                # empty policy explicitly so upgrades do not
                                # silently inherit Envoy's changing RFC1918
                                # trust default.
                                "internal_address_config": {},
                                "stream_idle_timeout": f"{request_timeout}s",
                                "route_config": {
                                    "name": "local_route",
                                    "virtual_hosts": [
                                        {
                                            "name": "backend",
                                            "domains": ["*"],
                                            "routes": routes,
                                        }
                                    ],
                                },
                                "http_filters": http_filters,
                            },
                        }
                    ],
                }
            ],
        }

        config = {
            "static_resources": {
                "listeners": [listener],
                "clusters": clusters,
            },
        }
        # Concurrency is an Envoy process flag, not a bootstrap field.  Parse
        # it here so a renderer call still validates the complete option set;
        # the composition root adds the corresponding ``--concurrency`` argv.
        strict_int(
            options.get("concurrency", 0), "proxy.options.concurrency", minimum=0, maximum=4096
        )
        if admin_port > 0:
            config["admin"] = {
                "address": {"socket_address": {"address": "127.0.0.1", "port_value": admin_port}}
            }

        config_path = output_dir / "envoy.yaml"
        from ..state.atomic import atomic_create_or_verify_yaml

        atomic_create_or_verify_yaml(config_path, config, default_flow_style=False, sort_keys=False)

        total_servers = sum(len(eps) for eps in by_model.values())
        print(
            f"[EnvoyProxy] Config written to {config_path} "
            f"({len(by_model)} cluster(s), {total_servers} endpoint(s), lb={lb_policy})"
        )
        return config_path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _safe_name(model_id: str) -> str:
    stem = re.sub(r"[^A-Za-z0-9_]", "_", model_id).strip("_") or "backend"
    digest = hashlib.sha256(model_id.encode("utf-8")).hexdigest()[:12]
    return f"c_{stem[:48]}_{digest}"


def _shared_path_prefix(endpoints: list[BackendEndpoint]) -> str:
    prefixes = {ep.path_prefix for ep in endpoints}
    if len(prefixes) != 1:
        raise ValueError(f"Inconsistent path_prefix values in backend set: {sorted(prefixes)!r}")
    return prefixes.pop()


def _replica_route_count(endpoints: list[BackendEndpoint], model_id: str) -> int:
    counts = {endpoint.replica_routes for endpoint in endpoints}
    if len(counts) != 1:
        raise ValueError(f"inconsistent replica-route counts for model {model_id!r}")
    return counts.pop()


def _replica_rewrite_lua(
    by_model: dict[str, list[BackendEndpoint]],
) -> str | None:
    """Render a bounded per-worker round-robin replica-route selector.

    Envoy's endpoint load balancer selects a node proxy, not a Ray Serve
    application. Canonical multi-replica placement uses one application route
    per replica, so selecting that route is a separate operation. Each Envoy
    worker owns its counter; no shared lock or unbounded request state exists.
    """
    rules: list[tuple[str, int, bool]] = []
    single_model = len(by_model) == 1
    for model_id, endpoints in by_model.items():
        replica_routes = _replica_route_count(endpoints, model_id)
        if replica_routes == 0:
            continue
        path_prefix = _shared_path_prefix(endpoints)
        if not path_prefix:
            raise ValueError(f"replica-routed model {model_id!r} requires a path_prefix")
        rules.append((path_prefix, replica_routes, single_model))
    if not rules:
        return None

    table_rows = "\n".join(
        "  { prefix = %s, replicas = %d, root = %s },"
        % (json.dumps(prefix), replicas, "true" if root else "false")
        for prefix, replicas, root in sorted(rules, key=lambda item: (-len(item[0]), item[0]))
    )
    return f"""local routes = {{
{table_rows}
}}
local counters = {{}}

function envoy_on_request(handle)
  local headers = handle:headers()
  local path = headers:get(":path")
  if path == nil then
    return
  end
  local query_at = string.find(path, "?", 1, true)
  local bare = path
  local query = ""
  if query_at ~= nil then
    bare = string.sub(path, 1, query_at - 1)
    query = string.sub(path, query_at)
  end
  for _, route in ipairs(routes) do
    local prefix = route.prefix
    local already_bound = string.sub(bare, 1, string.len(prefix) + 2) == prefix .. "_r"
    local has_canonical_prefix = bare == prefix or
      string.sub(bare, 1, string.len(prefix) + 1) == prefix .. "/"
    if already_bound then
      return
    end
    if route.root or has_canonical_prefix then
      local next_index = counters[prefix] or 0
      counters[prefix] = (next_index + 1) % route.replicas
      local suffix = bare
      if has_canonical_prefix then
        suffix = string.sub(bare, string.len(prefix) + 1)
      end
      headers:replace(":path", prefix .. "_r" .. next_index .. suffix .. query)
      return
    end
  end
end
"""
