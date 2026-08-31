"""
Abstract interface for pluggable proxy backends.

Each backend is a pure, deterministic config renderer.  The composition root
is the sole lifecycle owner: it preflights the rendered artifact, starts one
``ManagedComponent``, observes it, and performs bounded cleanup.  Keeping
``Popen`` out of renderers prevents a second, less supervised launch path.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
import re


@dataclass
class BackendEndpoint:
    """A single Ray Serve HTTP endpoint (one per cluster node)."""

    host: str  # cluster-fabric hostname or IP, e.g. "node0042.hsn.cluster.example"
    port: int  # Ray Serve HTTP port, e.g. 8000
    model_id: str  # e.g. "meta-llama/Meta-Llama-3-8B-Instruct"
    path_prefix: str = ""  # e.g. "/meta-llama--Llama-3-1-8B-Instruct" for multi-model
    # A positive value tells the gateway to distribute over N canonical
    # application routes: single-replica ``_rN`` or equal-sized node groups
    # ``_gN`` as selected by route_suffix.
    replica_routes: int = 0
    # Canonical per-replica apps use ``_r``; node-grouped null apps use ``_g``.
    # It is ignored when replica_routes == 0.
    route_suffix: str = "_r"


def reject_unknown_options(options: dict, allowed: set[str], kind: str) -> None:
    unknown = sorted(set(options) - allowed)
    if unknown:
        raise ValueError(f"{kind} proxy options contain unknown fields: {unknown}")


def strict_int(value, path: str, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{path} must be an integer")
    if not minimum <= value <= maximum:
        raise ValueError(f"{path} must be in {minimum}..{maximum}")
    return value


def strict_text(value, path: str, *, choices=None, allow_empty=False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise ValueError(f"{path} must be text")
    if choices is not None and value not in choices:
        raise ValueError(f"{path} must be one of {sorted(choices)}")
    return value


def validate_endpoint(endpoint: BackendEndpoint, kind: str) -> None:
    if (
        not isinstance(endpoint.host, str)
        or not endpoint.host
        or len(endpoint.host) > 253
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]*", endpoint.host)
        or ".." in endpoint.host
    ):
        raise ValueError(f"unsafe {kind} backend host {endpoint.host!r}")
    if (
        isinstance(endpoint.port, bool)
        or not isinstance(endpoint.port, int)
        or not 1 <= endpoint.port <= 65535
    ):
        raise ValueError(f"invalid {kind} backend port {endpoint.port!r}")
    if (
        not isinstance(endpoint.model_id, str)
        or not endpoint.model_id
        or any(ord(char) < 32 for char in endpoint.model_id)
    ):
        raise ValueError(f"invalid {kind} backend model_id")
    if endpoint.path_prefix and (
        not isinstance(endpoint.path_prefix, str)
        or not re.fullmatch(r"/[A-Za-z0-9._~/-]+", endpoint.path_prefix)
        or ".." in endpoint.path_prefix
    ):
        raise ValueError(f"unsafe {kind} route prefix {endpoint.path_prefix!r}")
    if (
        isinstance(endpoint.replica_routes, bool)
        or not isinstance(endpoint.replica_routes, int)
        or endpoint.replica_routes < 0
    ):
        raise ValueError(f"invalid {kind} replica-route count")
    if endpoint.route_suffix not in {"_r", "_g"}:
        raise ValueError(f"invalid {kind} replica-route suffix")


class ProxyBackend(ABC):
    """
    Contract for all proxy implementations.

    Lifecycle called by the composition root (rank 0 only):
        backends = discover_backends(...)
        config_path = proxy.generate_config(backends, output_dir, **options)
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
