"""Backend adapter registry.

Backends encapsulate how the inference serving cluster is launched, monitored,
and torn down. The adapter interface (BackendAdapter in base.py) defines the
lifecycle: validate -> build_runtime_manifest -> launch -> wait_ready ->
discover_targets -> stop.

Currently supported:
  - "ray": launches the real Ray Serve cluster via src/exaserve/resources/launch_cluster.sh.
  - "mock": no-op adapter for testing the control plane without a real cluster.

Adding a new backend: implement BackendAdapter, register it in _BACKENDS.
"""

from __future__ import annotations

from .base import BackendAdapter
from .mock import MockBackendAdapter
from .ray import RayBackendAdapter


_BACKENDS: dict[str, type[BackendAdapter]] = {
    "mock": MockBackendAdapter,
    "ray": RayBackendAdapter,
}


def get_backend_adapter(name: str) -> BackendAdapter:
    try:
        return _BACKENDS[name]()
    except KeyError as exc:
        available = ", ".join(sorted(_BACKENDS))
        raise ValueError(f"Unknown backend '{name}'. Available: {available}") from exc
