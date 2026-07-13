"""
"Ray Serve only" mode -- the no-external-proxy baseline.

This isn't really a proxy: it just selects ProxyLocation.HeadOnly inside
exaserve_serve.server (instead of EveryNode) and points the eval client at
the head node's Ray Serve HTTP port (default 8000). All request fan-out
to replicas happens inside Ray Serve's own gRPC router, which is the
out-of-the-box behavior most users would get without exaserve's
HAProxy/LiteLLM bolt-on.

The HeadOnly switch is read by server.py via ProxyConfig.type and is the
ONLY behavior change. Discover_targets in eval/lib/backends/ray.py also
keys on the ray_serve type to point at the head:backend_port instead of
the proxy port.

We do not launch an external process; the start() method spawns a tiny
sentinel subprocess so the existing ProxyBackend lifecycle (poll/stop)
works without driver-side special cases.
"""

import os
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path

from .base import BackendEndpoint, ProxyBackend


class RayServeProxy(ProxyBackend):
    """Null proxy: hands the client straight to Ray Serve's head-node HTTP proxy."""

    def generate_config(
        self,
        backends: list[BackendEndpoint],
        output_dir: Path,
        **options,
    ) -> Path:
        """
        No real config to write. We stash the backend port (must match the
        port Ray Serve binds to in HeadOnly mode -- default 8000) so start()
        can return it as the "proxy" port.
        """
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        if not backends:
            raise RuntimeError("RayServeProxy needs at least one backend endpoint")
        # All backends share the same Ray Serve HTTP port.
        ports = {ep.port for ep in backends}
        if len(ports) != 1:
            raise RuntimeError(
                f"RayServeProxy expected a single backend_port across endpoints, got {sorted(ports)}"
            )
        self._backend_port = next(iter(ports))

        # Write a marker file. Nothing reads it; it just documents what mode
        # this run is in alongside the other proxies' configs.
        config_path = output_dir / "ray_serve.txt"
        with open(config_path, "w") as f:
            f.write(
                "ray_serve mode (ProxyLocation.HeadOnly)\n"
                f"backend_port = {self._backend_port}\n"
                f"upstream nodes = {len({ep.host for ep in backends})}\n"
            )
        print(
            f"[RayServeProxy] No external proxy; clients hit Ray Serve directly at "
            f"head:{self._backend_port} (HeadOnly mode handled in server.py)"
        )
        return config_path

    def start(self, config_path: Path, host: str, port: int, **kwargs) -> tuple[subprocess.Popen, int]:
        """
        Don't launch a real proxy. Spawn a sentinel that just waits for SIGTERM,
        so the driver's poll/stop semantics work unchanged.

        Returns (sentinel_proc, backend_port) -- the port file written by the
        driver will then point at backend_port (typically 8000), and the eval
        client connects there directly.
        """
        # Sentinel: ignore HUP, exit cleanly on TERM/INT.
        sentinel_script = "trap 'exit 0' TERM INT; while true; do sleep 60; done"
        proc = subprocess.Popen(
            ["bash", "-c", sentinel_script],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        actual_port = int(getattr(self, "_backend_port", port))
        print(
            f"[RayServeProxy] Sentinel started (pid={proc.pid}); reporting port={actual_port} "
            f"(head-node Ray Serve HTTP proxy)",
            flush=True,
        )
        return proc, actual_port

    def health_check(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        process: subprocess.Popen | None = None,
    ) -> bool:
        """
        Poll the head-node Ray Serve HTTP proxy on port `port`.

        Ray Serve in HeadOnly mode binds the HTTP proxy on the head node only;
        we want to wait until that proxy is actually accepting connections
        (the driver's "ALL SERVICES READY" comes after this so the sequencing
        is fine, but we still validate here).
        """
        check_host = "127.0.0.1" if host in ("0.0.0.0", "") else host
        deadline = time.monotonic() + timeout
        attempt = 0
        last_err = None
        while time.monotonic() < deadline:
            if process is not None and process.poll() is not None:
                print(
                    f"[RayServeProxy] Sentinel exited unexpectedly with "
                    f"code {process.returncode}.",
                    flush=True,
                )
                return False
            attempt += 1
            try:
                with socket.create_connection((check_host, port), timeout=2):
                    # TCP up; try a quick HTTP probe to confirm a real Serve proxy
                    # (not some stale listener).
                    try:
                        with urllib.request.urlopen(
                            f"http://{check_host}:{port}/-/routes", timeout=3,
                        ) as resp:
                            if resp.status == 200:
                                print(
                                    f"[RayServeProxy] Healthy after {attempt} attempt(s); "
                                    f"Ray Serve proxy on {check_host}:{port} responded /-/routes 200",
                                    flush=True,
                                )
                                return True
                    except (urllib.error.URLError, OSError) as e:
                        last_err = e
            except OSError as e:
                last_err = e
            time.sleep(1)
        print(
            f"[RayServeProxy] Health check timed out after {timeout}s "
            f"(last error: {last_err!r})",
            flush=True,
        )
        return False

    def stop(self, process: subprocess.Popen) -> None:
        if process.poll() is not None:
            return
        print(f"[RayServeProxy] Stopping sentinel (pid={process.pid})", flush=True)
        try:
            process.send_signal(signal.SIGTERM)
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
        print("[RayServeProxy] Sentinel stopped.", flush=True)
