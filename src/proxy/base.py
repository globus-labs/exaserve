"""
Abstract interface for pluggable proxy backends.

Each concrete backend (LiteLLM, HAProxy, custom) implements ProxyBackend.
driver.py is the only caller -- it uses only this interface and never imports
backend-specific code directly.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
import subprocess


@dataclass
class BackendEndpoint:
    """A single Ray Serve HTTP endpoint (one per cluster node)."""
    host: str       # HSN hostname or IP, e.g. "x4616c6s5b0n0.hsn.cm.aurora.alcf.anl.gov"
    port: int       # Ray Serve HTTP port, e.g. 8000
    model_id: str   # e.g. "meta-llama/Meta-Llama-3-8B-Instruct"
    path_prefix: str = ""  # e.g. "/meta-llama--Llama-3-1-8B-Instruct" for multi-model


class ProxyBackend(ABC):
    """
    Contract for all proxy implementations.

    Lifecycle called by driver.py (rank 0 only):
        backends = discover_backends(...)
        config_path = proxy.generate_config(backends, output_dir, **options)
        proc = proxy.start(config_path, host, port)
        proxy.health_check(host, port)          # blocks until ready
        ...
        proxy.stop(proc)                         # on shutdown
    """

    @abstractmethod
    def generate_config(
        self,
        backends: list[BackendEndpoint],
        output_dir: Path,
        **options,
    ) -> Path:
        """
        Write a proxy-specific config file to output_dir.

        Args:
            backends:   List of Ray Serve endpoints to load-balance across.
            output_dir: Directory where the config file should be written.
            **options:  Backend-specific knobs (routing strategy, auth keys, etc.).

        Returns:
            Absolute path to the generated config file.
        """

    @abstractmethod
    def start(self, config_path: Path, host: str, port: int, **kwargs) -> tuple[subprocess.Popen, int]:
        """
        Launch the proxy process.

        Args:
            config_path: Path returned by generate_config().
            host:        Interface to bind (e.g. "0.0.0.0").
            port:        Preferred port to listen on (e.g. 4001). The
                         implementation may fall back to a different port if
                         the preferred one is unavailable.
            **kwargs:    Backend-specific options (e.g. num_workers).

        Returns:
            (proc, actual_port) — Popen handle and the port the proxy
            actually bound to (may differ from the requested port).
        """

    @abstractmethod
    def health_check(
        self,
        host: str,
        port: int,
        timeout: float = 30.0,
        process: subprocess.Popen | None = None,
    ) -> bool:
        """
        Block until the proxy is accepting requests or timeout expires.

        Args:
            host:    Host to poll (use "127.0.0.1" for localhost checks).
            port:    Port to poll.
            timeout: Max seconds to wait.
            process: If provided, check whether the process is still alive
                     on each poll iteration and return False immediately
                     if it has exited.

        Returns:
            True if healthy, False if timed out or the process died.
        """

    @abstractmethod
    def stop(self, process: subprocess.Popen) -> None:
        """
        Gracefully stop the proxy process.

        Should send SIGTERM first, then SIGKILL after a short grace period.
        """
