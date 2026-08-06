"""
Abstract interface for pluggable inference engines.

One shared Ray Serve host (``EngineWorker`` in server.py) owns the OpenAI HTTP
surface, per-tile placement, and stats; each concrete engine (vLLM, SGLang,
null) implements only the model-specific core behind this interface.

Design doc: doc/design/pluggable_interfaces.md

The generation contract is intentionally the *same dict/return shape the worker
already used* so the port is behavior-preserving:

- ``sampling`` is a neutral OpenAI-parsed dict with keys among
  ``temperature, top_p, max_tokens, min_tokens, stop, ignore_eos`` (defaults
  applied by the worker). Each backend maps it to its engine's params.
- ``generate`` returns a ``GenResult``; ``generate_stream`` yields ``GenDelta``.

IMPORTANT: concrete backends import their heavy dep (``vllm``/``sglang``) lazily
inside ``create()`` — never at module import — because vLLM and SGLang pin
conflicting ``transformers`` versions and only the chosen engine may be imported.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional


@dataclass
class EngineSpec:
    """Everything an engine needs to instantiate itself on its assigned tile(s)."""
    model_id: str
    local_path: str                       # node-local weights dir, or model_id
    tensor_parallel_size: int = 1
    pipeline_parallel_size: int = 1
    max_model_len: int = 4096
    gpu_memory_utilization: float = 0.90
    enforce_eager: bool = True
    max_num_seqs: Optional[int] = None
    device_ids: List[int] = field(default_factory=list)   # Ray tile ids this replica owns
    collect_stats: bool = False
    enable_log_requests: bool = True      # PR-022: operator setting reaches the backend
    extra_engine_kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GenResult:
    """Result of a non-streaming generation. ``error`` set => HTTP 500."""
    text: str = ""
    finish_reason: str = "stop"
    prompt_tokens: int = 0
    completion_tokens: int = 0
    error: Optional[str] = None


@dataclass
class GenDelta:
    """One streaming chunk. ``finish_reason`` set only on the final delta."""
    delta: str = ""
    finish_reason: Optional[str] = None
    prompt_tokens: int = 0            # populated on the final delta when known
    completion_tokens: int = 0


@dataclass
class EngineCaps:
    """What a backend supports; the host uses this to gate optional behavior."""
    streaming: bool = True
    serving_stats: bool = False
    needs_warmup: bool = False       # host awaits warmup() before reporting healthy


class EngineBackend(ABC):
    """Contract for all inference-engine implementations.

    Lifecycle, driven by the ``EngineWorker`` Ray Serve deployment (per replica):

        engine = get_engine(name)
        engine.create(spec)                       # heavy: instantiate on tile(s)
        await engine.warmup()                     # optional (Serve reconfigure)
        prompt = engine.build_chat_prompt(...)    # tokenizer apply_chat_template
        res  = await engine.generate(prompt, sampling)
        async for d in engine.generate_stream(prompt, sampling): ...
    """

    name: str = "base"

    @abstractmethod
    def create(self, spec: EngineSpec) -> None:
        """Instantiate the underlying engine (device isolation + engine build)."""

    def build_chat_prompt(
        self,
        messages: list,
        *,
        add_generation_prompt: bool = True,
        continue_final_message: bool = False,
        chat_template: Optional[str] = None,
        chat_template_kwargs: Optional[dict] = None,
    ) -> str:
        """Render chat messages to a single prompt string (engine tokenizer).

        Default: flatten to plain text (used by NullEngine and as a fallback).
        """
        from ..server import _chat_messages_to_plain_prompt
        return _chat_messages_to_plain_prompt(
            messages,
            add_generation_prompt=add_generation_prompt and not continue_final_message,
        )

    @abstractmethod
    async def generate(self, prompt: str, sampling: Dict[str, Any]) -> GenResult:
        """Run one non-streaming completion to completion."""

    @abstractmethod
    async def generate_stream(
        self, prompt: str, sampling: Dict[str, Any]
    ) -> AsyncIterator[GenDelta]:
        """Yield generation deltas (SSE token streaming)."""
        raise NotImplementedError
        yield  # pragma: no cover  (marks this an async generator)

    def capabilities(self) -> EngineCaps:
        return EngineCaps()

    async def warmup(self) -> None:
        """Optional pre-healthy warmup (e.g. JIT kernel compile). Default no-op."""
        return None

    def collect_stats(self) -> Dict[str, Any]:
        return {}

    def live_stats(self) -> Dict[str, Any]:
        """Snapshot for the /stats HTTP endpoint. Default: empty."""
        return {}

    async def shutdown(self) -> None:
        return None


class NullEngine(EngineBackend):
    """No-forward-pass stand-in for ``EXASERVE_NULL_COMPUTE=1``.

    Replaces a real engine with a fixed-latency sleep echoing deterministic
    tokens, so bring-up / placement / proxy wiring can be exercised at scale
    without loading weights. Unifies what were VLLMWorker._null_generate*.
    """

    name = "null"

    def __init__(self, latency_s: float = 1.0):
        self._latency = latency_s

    def create(self, spec: EngineSpec) -> None:
        self.model_id = spec.model_id

    @staticmethod
    def _max_tokens(sampling: Dict[str, Any]) -> int:
        return int(sampling.get("max_tokens", 10))

    async def generate(self, prompt: str, sampling: Dict[str, Any]) -> GenResult:
        import asyncio
        await asyncio.sleep(self._latency)
        n = self._max_tokens(sampling)
        # word-count prompt approximation, matching the old null path
        return GenResult(
            text="null " * n,
            finish_reason="stop",
            prompt_tokens=len(prompt.split()),
            completion_tokens=n,
        )

    async def generate_stream(
        self, prompt: str, sampling: Dict[str, Any]
    ) -> AsyncIterator[GenDelta]:
        import asyncio
        await asyncio.sleep(self._latency)
        n = self._max_tokens(sampling)
        yield GenDelta(
            delta="null " * n,
            finish_reason="stop",
            prompt_tokens=len(prompt.split()),
            completion_tokens=n,
        )
