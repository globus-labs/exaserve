"""
Pluggable inference engines.

Usage (from the EngineWorker host in server.py):

    from exaserve.engines import get_engine
    engine = get_engine("vllm")     # or "sglang", or "null"
    engine.create(spec)
    result = await engine.generate(prompt, params)

To add an engine:
    1. Create src/exaserve/engines/my_engine.py implementing EngineBackend (base.py).
    2. Register it in _register() below (lazy import — the heavy dependency must
       only be imported when that engine is actually selected, because vLLM and
       SGLang pin conflicting transformers versions).

Selection is passed from the verified DeploymentPlan. Native subprocesses also
receive the same value as a deterministic environment projection.
"""

from __future__ import annotations

from typing import Dict, Type

from .base import (  # noqa: F401
    EngineBackend,
    EngineCaps,
    EngineSpec,
    GenDelta,
    GenResult,
    NullEngine,
)

_REGISTRY: Dict[str, Type[EngineBackend]] = {}


def _register() -> None:
    """Populate the registry with built-in engines (lazy, import-on-select)."""
    if _REGISTRY:
        return
    # NOTE: import inside each branch would be ideal, but the classes are cheap to
    # reference; the *heavy* dependency (vllm/sglang) is imported lazily inside
    # each backend's create(), so merely registering does not import it.
    from .base import NullEngine  # noqa: F811

    _REGISTRY["null"] = NullEngine
    # These modules do not import their heavyweight engine packages until
    # create(). A local import failure is therefore an ExaServe defect and must
    # retain its original cause instead of masquerading as an unknown engine.
    from .sglang import SGLangEngine
    from .vllm import VLLMEngine

    _REGISTRY["vllm"] = VLLMEngine
    _REGISTRY["sglang"] = SGLangEngine


def get_engine(name: str, **kwargs) -> EngineBackend:
    """Return an instantiated EngineBackend for the given name.

    Args:
        name: One of "vllm", "sglang", "null", or any registered engine.
        **kwargs: forwarded to the backend constructor (e.g. NullEngine latency).

    Raises:
        KeyError: if no engine is registered under ``name``.
    """
    _register()
    key = name.lower()
    if key not in _REGISTRY:
        raise KeyError(f"unknown engine {name!r}; registered: {sorted(_REGISTRY)}")
    return _REGISTRY[key](**kwargs)


def available_engines() -> list[str]:
    """List registered engine names without importing heavyweight dependencies.

    Availability means ExaServe ships the adapter. Exact dependency/profile
    compatibility is validated before launch and again when ``create()`` runs.
    """
    _register()
    return sorted(_REGISTRY)


__all__ = [
    "get_engine",
    "available_engines",
    "EngineBackend",
    "EngineSpec",
    "GenResult",
    "GenDelta",
    "EngineCaps",
    "NullEngine",
]
