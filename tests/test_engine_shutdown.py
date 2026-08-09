"""Engine backends explicitly reap their dependency-owned child processes."""

from __future__ import annotations

import asyncio

import pytest

from exaserve.engines.sglang import SGLangEngine
from exaserve.engines.vllm import VLLMEngine


class _FakeEngine:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls = 0
        self.error = error

    def shutdown(self) -> None:
        self.calls += 1
        if self.error is not None:
            raise self.error


def test_vllm_engine_shutdown_runs_even_when_stats_are_disabled():
    backend = VLLMEngine()
    engine = _FakeEngine()
    backend.engine = engine
    assert backend._stats_thread is None
    asyncio.run(backend.shutdown())
    assert engine.calls == 1
    assert backend.engine is None
    asyncio.run(backend.shutdown())  # idempotent
    assert engine.calls == 1


def test_vllm_engine_shutdown_failure_is_not_silenced():
    backend = VLLMEngine()
    backend.engine = _FakeEngine(RuntimeError("core stuck"))
    with pytest.raises(RuntimeError, match="core stuck"):
        asyncio.run(backend.shutdown())


def test_sglang_process_tree_shutdown_is_explicit_and_idempotent():
    backend = SGLangEngine()
    engine = _FakeEngine()
    backend.engine = engine
    asyncio.run(backend.shutdown())
    assert engine.calls == 1
    assert backend.engine is None
    asyncio.run(backend.shutdown())
    assert engine.calls == 1
