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

Selection at runtime is driven by ``EXASERVE_ENGINE`` (default "vllm"), read in
server.py's deploy_model.
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
    SamplingParams,
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
    # vLLM / SGLang backends are added by their modules once ported
    # (see doc/design/pluggable_interfaces.md migration plan):
    try:
        from .vllm import VLLMEngine
        _REGISTRY["vllm"] = VLLMEngine
    except ImportError:
        pass
    try:
        from .sglang import SGLangEngine
        _REGISTRY["sglang"] = SGLangEngine
    except ImportError:
        pass


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
        raise KeyError(
            f"unknown engine {name!r}; registered: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[key](**kwargs)


def available_engines() -> list[str]:
    """List registered engine names (those whose deps import successfully)."""
    _register()
    return sorted(_REGISTRY)


__all__ = [
    "get_engine",
    "available_engines",
    "EngineBackend",
    "EngineSpec",
    "SamplingParams",
    "GenResult",
    "GenDelta",
    "EngineCaps",
    "NullEngine",
]
