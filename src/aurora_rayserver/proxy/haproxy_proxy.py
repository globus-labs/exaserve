"""
HAProxy backend implementation.

Generates an haproxy.cfg that balances across Ray Serve HTTP proxies, then
launches the haproxy binary.

HAProxy is a pure load balancer -- it has no OpenAI awareness.
Use it as a performance baseline or when you only need L7 TCP/HTTP routing
without API key management, usage tracking, or model-aware routing.

If you need those features, use LiteLLMProxy instead.

haproxy must be installed and on PATH.
"""

import signal
import socket
import subprocess
import time
from pathlib import Path
from textwrap import dedent

from .base import BackendEndpoint, ProxyBackend


class HAProxyProxy(ProxyBackend):
    """Manages an HAProxy process as the request router."""

    def generate_config(
        self,
        backends: list[BackendEndpoint],
        output_dir: Path,
        **options,
    ) -> Path:
        """
        Write haproxy.cfg to output_dir.

        Options (all optional):
            balance (str):       Load-balancing algorithm.
                                 "leastconn" | "roundrobin" | "random"
                                 Default: "leastconn".
            check_interval (int): Health check interval in ms. Default: 5000.
            check_fall (int):    Consecutive failures before marking backend down.
                                 Default: 3.
            check_rise (int):    Consecutive successes to mark backend up. Default: 2.
            stats_port (int):    HAProxy stats page port. Default: 9999.
                                 Set to 0 to disable stats.
            maxconn (int):       Max concurrent connections. Default: 50000.

        Note: HAProxy uses one backend *per unique model_id*. Nodes serving the
        same model are grouped together. If all backends serve the same model (the
        common case), there is one backend section named after that model.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        balance = options.get("balance", "leastconn")
        check_interval = int(options.get("check_interval", 5000))
        check_fall = int(options.get("check_fall", 3))
        check_rise = int(options.get("check_rise", 2))
        stats_port = int(options.get("stats_port", 9999))
        maxconn = int(options.get("maxconn", 50000))

        # Group endpoints by model_id so each model gets its own backend section
        from collections import defaultdict
        by_model: dict[str, list[BackendEndpoint]] = defaultdict(list)
        for ep in backends:
            by_model[ep.model_id].append(ep)

        lines: list[str] = []

        # --- global section ---
        lines.append(dedent(f"""\
            global
                maxconn {maxconn}
                log stdout format raw local0 info

            defaults
                mode http
                timeout connect 5s
                timeout client  330s
                timeout server  330s
                option http-server-close
                option forwardfor
                log global
            """))

        # --- frontend ---
        # A single frontend receives all incoming OpenAI API requests.
        # For multi-model deployments, requests must already target the
        # per-model Ray Serve route prefix because HAProxy does not inspect
        # the OpenAI JSON body to recover the model name.
        if len(by_model) == 1:
            model_id = next(iter(by_model))
            safe_name = _safe_backend_name(model_id)
            lines.append(dedent(f"""\
                frontend openai_api
                    bind *:{{PORT}}
                    default_backend {safe_name}
                """))
        else:
            lines.append("frontend openai_api")
            lines.append("    bind *:{PORT}")
            for model_id, eps in by_model.items():
                safe_name = _safe_backend_name(model_id)
                path_prefix = _shared_path_prefix(eps)
                acl_name = f"is_{safe_name}"
                lines.append(f"    acl {acl_name} path_beg {path_prefix} {path_prefix}/")
                lines.append(f"    use_backend {safe_name} if {acl_name}")
            lines.append(
                '    http-request return status 404 content-type text/plain '
                'lf-string "missing or unknown model route prefix\\n"'
            )
            lines.append("")

        # --- backend section(s) ---
        if len(by_model) == 1:
            model_id, eps = next(iter(by_model.items()))
            safe_name = _safe_backend_name(model_id)
            path_prefix = _shared_path_prefix(eps)
            lines.append(_render_backend(
                name=safe_name,
                endpoints=eps,
                path_prefix=path_prefix,
                balance=balance,
                check_interval=check_interval,
                check_fall=check_fall,
                check_rise=check_rise,
            ))
        else:
            for model_id, eps in by_model.items():
                safe_name = _safe_backend_name(model_id)
                path_prefix = _shared_path_prefix(eps)
                lines.append(_render_backend(
                    name=safe_name,
                    endpoints=eps,
                    path_prefix=path_prefix,
                    balance=balance,
                    check_interval=check_interval,
                    check_fall=check_fall,
                    check_rise=check_rise,
                ))

        # --- optional stats page ---
        if stats_port > 0:
            lines.append(dedent(f"""\
                listen stats
                    bind *:{stats_port}
                    stats enable
                    stats uri /stats
                    stats refresh 10s
                    stats admin if TRUE
                """))

        # Resolve {PORT} placeholder (frontend used it above)
        config_text = "\n".join(lines)

        config_path = output_dir / "haproxy.cfg"
        with open(config_path, "w") as f:
            f.write(config_text)

        total_servers = sum(len(eps) for eps in by_model.values())
        print(
            f"[HAProxyProxy] Config written to {config_path} "
            f"({len(by_model)} model(s), {total_servers} server entries, balance={balance})"
        )
        return config_path

    def start(self, config_path: Path, host: str, port: int, **kwargs) -> tuple[subprocess.Popen, int]:
        """
        Launch haproxy with the generated config.

        The {PORT} placeholder in the config is resolved here by re-writing
        the config with the actual port before launching.

        Returns (proc, port) to satisfy the ProxyBackend interface.
        """
        # Patch the port placeholder in the config file
        text = config_path.read_text()
        text = text.replace("{PORT}", str(port))
        config_path.write_text(text)

        cmd = ["haproxy", "-f", str(config_path)]
        print(f"[HAProxyProxy] Starting: {' '.join(cmd)}", flush=True)
        proc = subprocess.Popen(cmd)
        print(f"[HAProxyProxy] Process started (pid={proc.pid}, port={port})", flush=True)
        return proc, port

    def health_check(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        process: subprocess.Popen | None = None,
    ) -> bool:
        """
        Poll TCP connect to host:port until it accepts connections or timeout.

        Returns False immediately if the proxy process has already exited.
        """
        check_host = "127.0.0.1" if host in ("0.0.0.0", "") else host
        deadline = time.monotonic() + timeout
        attempt = 0
        while time.monotonic() < deadline:
            if process is not None and process.poll() is not None:
                print(
                    f"[HAProxyProxy] Process exited with code {process.returncode} "
                    f"before becoming healthy.",
                    flush=True,
                )
                return False

            attempt += 1
            try:
                with socket.create_connection((check_host, port), timeout=2):
                    print(
                        f"[HAProxyProxy] Healthy after {attempt} attempt(s) "
                        f"(port {port})",
                        flush=True,
                    )
                    return True
            except OSError:
                pass
            time.sleep(1)

        print(
            f"[HAProxyProxy] Health check timed out after {timeout}s",
            flush=True,
        )
        return False

    def stop(self, process: subprocess.Popen) -> None:
        """Send SIGTERM for graceful drain, then SIGKILL after 15s."""
        if process.poll() is not None:
            return
        print(f"[HAProxyProxy] Stopping proxy (pid={process.pid})", flush=True)
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            print("[HAProxyProxy] SIGTERM timed out, sending SIGKILL", flush=True)
            process.kill()
            process.wait()
        print("[HAProxyProxy] Proxy stopped.", flush=True)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_backend_name(model_id: str) -> str:
    """Convert a model_id to an HAProxy-safe identifier (no slashes or dots)."""
    return model_id.replace("/", "_").replace(".", "_").replace("-", "_")


def _render_backend(
    name: str,
    endpoints: list[BackendEndpoint],
    path_prefix: str,
    balance: str,
    check_interval: int,
    check_fall: int,
    check_rise: int,
) -> str:
    """Render a single HAProxy backend section."""
    health_path = f"{path_prefix}/health" if path_prefix else "/health"
    lines = [
        f"backend {name}",
        f"    balance {balance}",
        f"    option httpchk GET {health_path}",
        f"    http-check expect status 200",
    ]
    for i, ep in enumerate(endpoints):
        server_name = f"{_safe_backend_name(ep.host)}_{ep.port}"
        lines.append(
            f"    server {server_name} {ep.host}:{ep.port} "
            f"check inter {check_interval}ms fall {check_fall} rise {check_rise}"
        )
    lines.append("")  # blank line between sections
    return "\n".join(lines)


def _shared_path_prefix(endpoints: list[BackendEndpoint]) -> str:
    prefixes = {ep.path_prefix for ep in endpoints}
    if len(prefixes) != 1:
        raise ValueError(f"Inconsistent path_prefix values in backend set: {sorted(prefixes)!r}")
    return prefixes.pop()
