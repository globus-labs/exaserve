"""
Pingora backend implementation.

Writes a small YAML config consumed by the project's custom pingora_lb
binary (scripts/pingora_lb/), then launches that binary as the proxy
process.

Pingora is Cloudflare's Rust async-I/O proxy framework. Unlike HAProxy and
NGINX (single process, single-thread accept loops historically) Pingora
runs an N-thread tokio runtime out of the box, which makes it the most
interesting candidate to break the HAProxy single-process plateau seen at
256+ nodes.

pingora_lb must be installed and on PATH (see scripts/build_pingora.sh).
"""

import os
import signal
import socket
import subprocess
import time
from collections import defaultdict
from pathlib import Path

import yaml

from .base import BackendEndpoint, ProxyBackend


class PingoraProxy(ProxyBackend):
    """Manages a pingora_lb process as the request router."""

    def generate_config(
        self,
        backends: list[BackendEndpoint],
        output_dir: Path,
        **options,
    ) -> Path:
        """
        Write pingora.yaml to output_dir. Returns the config path.

        Options (all optional):
            lb_method (str):  "round_robin" | "least_request". Default "round_robin".
            threads (int):    Worker thread count (0 = all cores). Default: 0.
            connect_timeout_ms (int):  Default: 5000.
            request_timeout_ms (int):  0 = unbounded. Default: 330000.
            health_check_interval_s (int): 0 = disabled. Default: 5.

        Multi-model deployments are NOT supported by the minimal Rust binary;
        we raise here if asked to mix models. Add per-route dispatch in
        scripts/pingora_lb/src/main.rs if needed.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        by_model: dict[str, list[BackendEndpoint]] = defaultdict(list)
        for ep in backends:
            by_model[ep.model_id].append(ep)
        if len(by_model) > 1:
            raise ValueError(
                "PingoraProxy currently supports a single model_id only "
                f"(got {sorted(by_model)}). Extend scripts/pingora_lb/src/main.rs."
            )

        eps = next(iter(by_model.values()))
        upstreams = [f"{ep.host}:{ep.port}" for ep in eps]

        cfg = {
            "listen": "0.0.0.0:PORT_PLACEHOLDER",
            "lb_method": options.get("lb_method", "round_robin"),
            "upstreams": upstreams,
            "threads": int(options.get("threads", 0)),
            "connect_timeout_ms": int(options.get("connect_timeout_ms", 5000)),
            "request_timeout_ms": int(options.get("request_timeout_ms", 330000)),
            "health_check_interval_s": int(options.get("health_check_interval_s", 5)),
        }

        config_path = output_dir / "pingora.yaml"
        with open(config_path, "w") as f:
            yaml.safe_dump(cfg, f, default_flow_style=False, sort_keys=False)

        print(
            f"[PingoraProxy] Config written to {config_path} "
            f"({len(upstreams)} upstream(s), lb={cfg['lb_method']}, "
            f"threads={cfg['threads']})"
        )
        return config_path

    def start(self, config_path: Path, host: str, port: int, **kwargs) -> tuple[subprocess.Popen, int]:
        """Patch the listen-port placeholder in the YAML and launch pingora_lb."""
        text = config_path.read_text()
        text = text.replace("0.0.0.0:PORT_PLACEHOLDER", f"0.0.0.0:{port}")
        config_path.write_text(text)

        env = os.environ.copy()
        env.pop("HTTP_PROXY", None)
        env.pop("HTTPS_PROXY", None)
        env.pop("http_proxy", None)
        env.pop("https_proxy", None)
        # Pingora logs through env_logger; default to info.
        env.setdefault("RUST_LOG", "info")

        cmd = ["pingora_lb", "--config", str(config_path)]
        log_path = config_path.parent / "pingora_stdout.log"
        log_fh = open(log_path, "w")
        self._log_fh = log_fh
        print(f"[PingoraProxy] Starting: {' '.join(cmd)} (log={log_path})", flush=True)
        proc = subprocess.Popen(cmd, env=env, stdout=log_fh, stderr=subprocess.STDOUT)
        print(f"[PingoraProxy] Process started (pid={proc.pid}, port={port})", flush=True)
        return proc, port

    def health_check(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        process: subprocess.Popen | None = None,
    ) -> bool:
        check_host = "127.0.0.1" if host in ("0.0.0.0", "") else host
        deadline = time.monotonic() + timeout
        attempt = 0
        while time.monotonic() < deadline:
            if process is not None and process.poll() is not None:
                print(
                    f"[PingoraProxy] Process exited with code {process.returncode} "
                    f"before becoming healthy.",
                    flush=True,
                )
                return False
            attempt += 1
            try:
                with socket.create_connection((check_host, port), timeout=2):
                    print(
                        f"[PingoraProxy] Healthy after {attempt} attempt(s) "
                        f"(port {port})",
                        flush=True,
                    )
                    return True
            except OSError:
                pass
            time.sleep(1)
        print(f"[PingoraProxy] Health check timed out after {timeout}s", flush=True)
        return False

    def stop(self, process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        print(f"[PingoraProxy] Stopping proxy (pid={process.pid})", flush=True)
        process.send_signal(signal.SIGTERM)
        try:
            process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            print("[PingoraProxy] SIGTERM timed out, sending SIGKILL", flush=True)
            process.kill()
            process.wait()
        print("[PingoraProxy] Proxy stopped.", flush=True)
