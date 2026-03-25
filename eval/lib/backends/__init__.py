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
