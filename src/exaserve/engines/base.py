"""
Abstract interface for pluggable inference engines.

Mirrors ``exaserve.proxy.base``: one shared Ray Serve host (``EngineWorker``, in
server.py) owns the OpenAI-compatible HTTP surface, per-tile placement, and
stats; each concrete engine (vLLM, SGLang, …) implements only the model-specific
core behind this interface.

Design doc: doc/design/pluggable_interfaces.md

Engine-neutral dataclasses (EngineSpec / SamplingParams / GenResult / GenDelta)
keep the host free of any vLLM- or SGLang-specific types, so adding an engine —
or running the same engine on a different vendor's accelerator — is a matter of
implementing ``EngineBackend`` rather than copy-pasting a ~500-line deployment
class.

IMPORTANT: concrete backends import their heavy dependency (``vllm`` / ``sglang``)
lazily inside ``create()`` — never at module import — because vLLM and SGLang pin
conflicting ``transformers`` versions and only the *chosen* engine may be
imported in a given process (see server.py header notes).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, List, Optional


@dataclass
class EngineSpec:
    """Everything an engine needs to instantiate itself on its assigned tile(s).

    Populated by the ``EngineWorker`` host from the model's ``ModelConfig`` plus
    the runtime placement decisions (device ids, distributed master).
    """
    model_id: str
    local_path: str                       # node-local weights dir, or model_id
    tensor_parallel_size: int = 1
    pipeline_parallel_size: int = 1
    max_model_len: int = 4096
    gpu_memory_utilization: float = 0.90
    enforce_eager: bool = True
    max_num_seqs: int = 64
    # Device isolation is applied by the vendor layer before create() (e.g.
    # ZE_AFFINITY_MASK on Intel XPU, CUDA_VISIBLE_DEVICES on NVIDIA). device_ids
    # is the logical tile/GPU list this replica owns, for logging + validation.
    device_ids: List[int] = field(default_factory=list)
    # Multi-node PP: address:port of the distributed master, when set.
    dist_master_addr: Optional[str] = None
    dist_master_port: Optional[int] = None
    # Escape hatch for backend-specific engine kwargs not modeled above.
    extra_engine_kwargs: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SamplingParams:
    """Engine-neutral sampling parameters, parsed from an OpenAI request body."""
    max_tokens: int = 128
    temperature: float = 1.0
    top_p: float = 1.0
    stop: Optional[List[str]] = None
    seed: Optional[int] = None
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class GenResult:
    """Result of a non-streaming generation."""
    text: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    finish_reason: str = "stop"


@dataclass
class GenDelta:
    """One streaming chunk. ``finish_reason`` is set only on the final delta."""
    text: str = ""
    finish_reason: Optional[str] = None
    prompt_tokens: int = 0            # populated on the final delta when known
    completion_tokens: int = 0


@dataclass
class EngineCaps:
    """What a backend supports; the host uses this to gate optional features."""
    streaming: bool = True
    logprobs: bool = False
    serving_stats: bool = False


class EngineBackend(ABC):
    """Contract for all inference-engine implementations.

    Lifecycle, driven by the ``EngineWorker`` Ray Serve deployment (per replica):

        engine = get_engine(name)          # registry lookup
        engine.create(spec)                # heavy: instantiate on the tile(s)
        ...
        result = await engine.generate(prompt, params)          # non-stream
        async for delta in engine.generate_stream(prompt, params):  # stream
            ...
        await engine.shutdown()            # on replica teardown
    """

    #: short registry key, e.g. "vllm"
    name: str = "base"

    @abstractmethod
    def create(self, spec: EngineSpec) -> None:
        """Instantiate the underlying engine on this replica's assigned tile(s).

        Import the heavy dependency lazily here (not at module scope). Device
        isolation env (ZE_AFFINITY_MASK / CUDA_VISIBLE_DEVICES) is already set by
        the vendor layer before this is called.
        """

    @abstractmethod
    async def generate(self, prompt: str, params: SamplingParams) -> GenResult:
        """Run one non-streaming completion to completion."""

    @abstractmethod
    async def generate_stream(
        self, prompt: str, params: SamplingParams
    ) -> AsyncIterator[GenDelta]:
        """Yield generation deltas as they are produced (SSE token streaming)."""
        raise NotImplementedError
        yield  # pragma: no cover  (marks this an async generator)

    def capabilities(self) -> EngineCaps:
        """Declare optional-feature support. Override as needed."""
        return EngineCaps()

    def collect_stats(self) -> Dict[str, Any]:
        """Return engine-side serving stats, or {} if unsupported."""
        return {}

    async def shutdown(self) -> None:
        """Release engine resources. Default: no-op."""
        return None


class NullEngine(EngineBackend):
    """No-forward-pass stand-in for ``EXASERVE_NULL_COMPUTE=1``.

    Replaces a real engine with a fixed-latency sleep that echoes deterministic
    tokens, so cluster bring-up / placement / proxy wiring can be exercised at
    scale without loading weights. Implemented once here instead of duplicated in
    every worker class (was ``_null_generate*`` in VLLMWorker).
    """

    name = "null"

    def __init__(self, latency_s: float = 1.0, tokens: int = 8):
        self._latency = latency_s
        self._tokens = tokens

    def create(self, spec: EngineSpec) -> None:  # noqa: D401 - trivial
        self._model_id = spec.model_id

    async def generate(self, prompt: str, params: SamplingParams) -> GenResult:
        import asyncio
        await asyncio.sleep(self._latency)
        n = min(self._tokens, params.max_tokens)
        return GenResult(text=" ".join(["null"] * n), completion_tokens=n)

    async def generate_stream(
        self, prompt: str, params: SamplingParams
    ) -> AsyncIterator[GenDelta]:
        import asyncio
        n = min(self._tokens, params.max_tokens)
        # Spread the fixed latency across the streamed tokens so TBT looks real.
        per = self._latency / max(n, 1)
        for i in range(n):
            await asyncio.sleep(per)
            last = i == n - 1
            yield GenDelta(
                text="null ",
                finish_reason="length" if last else None,
                completion_tokens=n if last else 0,
            )
