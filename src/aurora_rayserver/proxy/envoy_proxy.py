"""
Envoy backend implementation.

Generates an envoy.yaml that load-balances across Ray Serve HTTP proxies,
then launches the envoy binary.

Envoy is a high-performance C++ L7 proxy from the Istio/CNCF ecosystem.
Like HAProxy and NGINX it is OpenAI-agnostic; included as a third pure-LB
baseline. The default LB policy here is LEAST_REQUEST (the closest Envoy
analog to HAProxy's leastconn).

envoy must be installed and on PATH (see scripts/install_envoy.sh).
"""

import os
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections import defaultdict
from pathlib import Path

import yaml

from .base import BackendEndpoint, ProxyBackend


class EnvoyProxy(ProxyBackend):
    """Manages an Envoy process as the request router."""

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

        lb_policy = options.get("lb_policy", "LEAST_REQUEST")
        admin_port = int(options.get("admin_port", 9902))
        request_timeout = int(options.get("request_timeout", 330))
        connect_timeout = str(options.get("connect_timeout", "5s"))
        max_connections = int(options.get("max_connections", 50000))

        by_model: dict[str, list[BackendEndpoint]] = defaultdict(list)
        for ep in backends:
            by_model[ep.model_id].append(ep)

        # Build clusters (one per model).
        clusters = []
        for model_id, eps in by_model.items():
            cluster_name = _safe_name(model_id)
            clusters.append({
                "name": cluster_name,
                "type": "STATIC",
                "connect_timeout": connect_timeout,
                "lb_policy": lb_policy,
                "circuit_breakers": {
                    "thresholds": [{
                        "priority": "DEFAULT",
                        "max_connections": max_connections,
                        "max_pending_requests": max_connections,
                        "max_requests": max_connections,
                        "max_retries": 3,
                    }]
                },
                "load_assignment": {
                    "cluster_name": cluster_name,
                    "endpoints": [{
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
                    }]
                },
            })

        # Build routes.
        if len(by_model) == 1:
            cluster_name = _safe_name(next(iter(by_model)))
            routes = [{
                "match": {"prefix": "/"},
                "route": {"cluster": cluster_name, "timeout": f"{request_timeout}s"},
            }]
        else:
            routes = []
            for model_id, eps in by_model.items():
                cluster_name = _safe_name(model_id)
                path_prefix = _shared_path_prefix(eps)
                routes.append({
                    "match": {"prefix": f"{path_prefix}/"},
                    "route": {"cluster": cluster_name, "timeout": f"{request_timeout}s"},
                })
            routes.append({
                "match": {"prefix": "/"},
                "direct_response": {
                    "status": 404,
                    "body": {"inline_string": "missing or unknown model route prefix\n"},
                },
            })

        listener = {
            "name": "listener_main",
            "address": {
                "socket_address": {
                    "address": "0.0.0.0",
                    "port_value": "PORT_PLACEHOLDER",  # patched in start()
                }
            },
            "filter_chains": [{
                "filters": [{
                    "name": "envoy.filters.network.http_connection_manager",
                    "typed_config": {
                        "@type": "type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager",
                        "stat_prefix": "ingress_http",
                        "stream_idle_timeout": f"{request_timeout}s",
                        "route_config": {
                            "name": "local_route",
                            "virtual_hosts": [{
                                "name": "backend",
                                "domains": ["*"],
                                "routes": routes,
                            }],
                        },
                        "http_filters": [{
                            "name": "envoy.filters.http.router",
                            "typed_config": {
                                "@type": "type.googleapis.com/envoy.extensions.filters.http.router.v3.Router",
                            },
                        }],
                    },
                }],
            }],
        }

        config = {
            "static_resources": {
                "listeners": [listener],
                "clusters": clusters,
            },
        }
        if admin_port > 0:
            config["admin"] = {
                "address": {
                    "socket_address": {"address": "127.0.0.1", "port_value": admin_port}
                }
            }

        config_path = output_dir / "envoy.yaml"
        with open(config_path, "w") as f:
            yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

        total_servers = sum(len(eps) for eps in by_model.values())
        print(
            f"[EnvoyProxy] Config written to {config_path} "
            f"({len(by_model)} cluster(s), {total_servers} endpoint(s), lb={lb_policy})"
        )
        # Stash for start()
        self._concurrency = options.get("concurrency", 0)
        self._admin_port = admin_port
        return config_path

    def start(self, config_path: Path, host: str, port: int, **kwargs) -> tuple[subprocess.Popen, int]:
        """Launch envoy with --base-id 1 + concurrency override."""
        # Patch the port placeholder (we couldn't use a Python int for the
        # YAML value because Envoy's port_value is an int, not a string,
        # but we needed it deferred to start() so the spec.port can change).
        text = config_path.read_text()
        text = text.replace("port_value: PORT_PLACEHOLDER", f"port_value: {port}")
        config_path.write_text(text)

        cmd = ["envoy", "-c", str(config_path), "--base-id", "1"]
        concurrency = int(getattr(self, "_concurrency", 0) or 0)
        if concurrency > 0:
            cmd.extend(["--concurrency", str(concurrency)])

        env = os.environ.copy()
        # Avoid going through ALCF Squid for HSN-internal traffic.
        env.pop("HTTP_PROXY", None)
        env.pop("HTTPS_PROXY", None)
        env.pop("http_proxy", None)
        env.pop("https_proxy", None)

        log_path = config_path.parent / "envoy_stdout.log"
        log_fh = open(log_path, "w")
        self._log_fh = log_fh
        print(f"[EnvoyProxy] Starting: {' '.join(cmd)} (log={log_path})", flush=True)
        proc = subprocess.Popen(cmd, env=env, stdout=log_fh, stderr=subprocess.STDOUT)
        print(f"[EnvoyProxy] Process started (pid={proc.pid}, port={port})", flush=True)
        return proc, port

    def health_check(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        process: subprocess.Popen | None = None,
    ) -> bool:
        """Use the admin /ready endpoint if available, otherwise TCP poll the data port."""
        check_host = "127.0.0.1" if host in ("0.0.0.0", "") else host
        deadline = time.monotonic() + timeout
        admin_port = int(getattr(self, "_admin_port", 0))
        attempt = 0
        while time.monotonic() < deadline:
            if process is not None and process.poll() is not None:
                print(
                    f"[EnvoyProxy] Process exited with code {process.returncode} "
                    f"before becoming healthy.",
                    flush=True,
                )
                return False
            attempt += 1
            # Prefer admin /ready (returns LIVE when the server is fully up).
            if admin_port > 0:
                try:
                    with urllib.request.urlopen(
                        f"http://127.0.0.1:{admin_port}/ready", timeout=2,
                    ) as resp:
                        body = resp.read().decode("utf-8", errors="ignore").strip()
                        if resp.status == 200 and "LIVE" in body:
                            print(
                                f"[EnvoyProxy] Healthy after {attempt} attempt(s) "
                                f"(admin {admin_port} reports LIVE, data port {port})",
                                flush=True,
                            )
                            return True
                except (urllib.error.URLError, OSError):
                    pass
            else:
                try:
                    with socket.create_connection((check_host, port), timeout=2):
                        print(
                            f"[EnvoyProxy] Healthy after {attempt} attempt(s) "
                            f"(port {port} accepts connections)",
                            flush=True,
                        )
                        return True
                except OSError:
                    pass
            time.sleep(1)
        print(f"[EnvoyProxy] Health check timed out after {timeout}s", flush=True)
        return False

    def stop(self, process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        print(f"[EnvoyProxy] Stopping proxy (pid={process.pid})", flush=True)
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            print("[EnvoyProxy] SIGTERM timed out, sending SIGKILL", flush=True)
            process.kill()
            process.wait()
        print("[EnvoyProxy] Proxy stopped.", flush=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_name(model_id: str) -> str:
    return "c_" + model_id.replace("/", "_").replace(".", "_").replace("-", "_")


def _shared_path_prefix(endpoints: list[BackendEndpoint]) -> str:
    prefixes = {ep.path_prefix for ep in endpoints}
    if len(prefixes) != 1:
        raise ValueError(f"Inconsistent path_prefix values in backend set: {sorted(prefixes)!r}")
    return prefixes.pop()
