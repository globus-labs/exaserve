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
import copy
from dataclasses import dataclass, field
import math
from typing import Any, AsyncIterator, Dict, List, Optional


def merge_engine_kwargs(
    canonical: Dict[str, Any],
    extra: Dict[str, Any],
    *,
    protected: tuple[str, ...] = (),
) -> Dict[str, Any]:
    """Add backend-specific extras without overriding lifecycle/security fields."""
    if not isinstance(extra, dict) or any(not isinstance(key, str) or not key for key in extra):
        raise ValueError("EngineSpec.extra_engine_kwargs must be a mapping with text keys")
    collisions = sorted(set(extra) & (set(canonical) | set(protected)))
    if collisions:
        raise ValueError(
            "extra_engine_kwargs cannot override ExaServe-owned fields: " + ", ".join(collisions)
        )
    return canonical | extra


@dataclass
class EngineSpec:
    """Everything an engine needs to instantiate itself on its assigned tile(s)."""

    model_id: str
    local_path: str  # node-local weights dir, or model_id
    vendor_name: str = "xpu"
    tensor_parallel_size: int = 1
    pipeline_parallel_size: int = 1
    max_model_len: int = 4096
    gpu_memory_utilization: float = 0.90
    enforce_eager: bool = True
    max_num_seqs: Optional[int] = None
    device_ids: List[int] = field(default_factory=list)  # Ray tile ids this replica owns
    collect_stats: bool = False
    stats_retention: int = 2000
    stats_push_period_s: float = 10.0
    stats_sample_cap: int = 1500
    enable_log_requests: bool = True  # PR-022: operator setting reaches the backend
    extra_engine_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("model_id", "local_path", "vendor_name"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"EngineSpec.{name} must be non-empty text")
        for name in (
            "tensor_parallel_size",
            "pipeline_parallel_size",
            "max_model_len",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"EngineSpec.{name} must be a positive integer")
        if (
            not isinstance(self.gpu_memory_utilization, float)
            or not math.isfinite(self.gpu_memory_utilization)
            or not 0 < self.gpu_memory_utilization <= 1
        ):
            raise ValueError("EngineSpec.gpu_memory_utilization must be finite in (0, 1]")
        if self.max_num_seqs is not None and (
            isinstance(self.max_num_seqs, bool)
            or not isinstance(self.max_num_seqs, int)
            or self.max_num_seqs < 1
        ):
            raise ValueError("EngineSpec.max_num_seqs must be null or positive")
        for name in ("enforce_eager", "collect_stats", "enable_log_requests"):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"EngineSpec.{name} must be boolean")
        if not isinstance(self.device_ids, (tuple, list)) or any(
            isinstance(device, bool) or not isinstance(device, int) or device < 0
            for device in self.device_ids
        ):
            raise ValueError("EngineSpec.device_ids must contain non-negative integers")
        self.device_ids = list(self.device_ids)
        if len(self.device_ids) != len(set(self.device_ids)):
            raise ValueError("EngineSpec.device_ids must be unique")
        if self.device_ids and len(self.device_ids) != self.tensor_parallel_size:
            raise ValueError("EngineSpec.device_ids must match tensor_parallel_size")
        if not isinstance(self.extra_engine_kwargs, dict) or any(
            not isinstance(key, str) or not key for key in self.extra_engine_kwargs
        ):
            raise ValueError("EngineSpec.extra_engine_kwargs must have non-empty text keys")
        self.extra_engine_kwargs = copy.deepcopy(self.extra_engine_kwargs)
        for name in ("stats_retention", "stats_sample_cap"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 10_000:
                raise ValueError(f"EngineSpec.{name} must be an integer in [1, 10000]")
        if (
            isinstance(self.stats_push_period_s, bool)
            or not isinstance(self.stats_push_period_s, (int, float))
            or not math.isfinite(float(self.stats_push_period_s))
            or self.stats_push_period_s <= 0
        ):
            raise ValueError("EngineSpec.stats_push_period_s must be finite and positive")


@dataclass
class GenResult:
    """Result of a non-streaming generation. ``error`` set => HTTP 500."""

    text: str = ""
    finish_reason: str = "stop"
    prompt_tokens: int = 0
    completion_tokens: int = 0
    token_count_source: str = "engine_tokenizer"
    error: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.text, str):
            raise ValueError("GenResult.text must be text")
        if not isinstance(self.finish_reason, str) or not self.finish_reason:
            raise ValueError("GenResult.finish_reason must be non-empty text")
        for field_name in ("prompt_tokens", "completion_tokens"):
            value = getattr(self, field_name)
            if type(value) is not int or value < 0:
                raise ValueError(f"GenResult.{field_name} must be a nonnegative integer")
        if not isinstance(self.token_count_source, str) or not self.token_count_source:
            raise ValueError("GenResult.token_count_source must be non-empty text")
        if self.error is not None and (not isinstance(self.error, str) or not self.error):
            raise ValueError("GenResult.error must be null or non-empty text")


@dataclass
class GenDelta:
    """One streaming chunk. ``finish_reason`` set only on the final delta."""

    delta: str = ""
    finish_reason: Optional[str] = None
    prompt_tokens: int = 0  # populated on the final delta when known
    completion_tokens: int = 0
    token_count_source: str = "engine_tokenizer"

    def __post_init__(self) -> None:
        if not isinstance(self.delta, str):
            raise ValueError("GenDelta.delta must be text")
        if self.finish_reason is not None and (
            not isinstance(self.finish_reason, str) or not self.finish_reason
        ):
            raise ValueError("GenDelta.finish_reason must be null or non-empty text")
        for field_name in ("prompt_tokens", "completion_tokens"):
            value = getattr(self, field_name)
            if type(value) is not int or value < 0:
                raise ValueError(f"GenDelta.{field_name} must be a nonnegative integer")
        if not isinstance(self.token_count_source, str) or not self.token_count_source:
            raise ValueError("GenDelta.token_count_source must be non-empty text")


@dataclass
class EngineCaps:
    """What a backend supports; the host uses this to gate optional behavior."""

    streaming: bool = True
    serving_stats: bool = False
    needs_warmup: bool = False  # host awaits warmup() before reporting healthy

    def __post_init__(self) -> None:
        for field_name in ("streaming", "serving_stats", "needs_warmup"):
            if not isinstance(getattr(self, field_name), bool):
                raise ValueError(f"EngineCaps.{field_name} must be boolean")


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
        from ..prompting import chat_messages_to_plain_prompt

        return chat_messages_to_plain_prompt(
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
    """No-forward-pass stand-in selected by ``DeploymentPlan.runtime``.

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
        value = sampling.get("max_tokens", 10)
        if type(value) is not int or value < 1:
            raise ValueError("NullEngine max_tokens must be a positive integer")
        return value

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
            token_count_source="whitespace_approximation",
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
            token_count_source="whitespace_approximation",
        )
