"""SGLang engine backend — extracted verbatim from the former ``SGLangWorker``.

``sglang`` is imported lazily inside ``create()`` (transformers-pin isolation).
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
import time
import uuid
from typing import Any, AsyncIterator, Dict, Optional

from .base import EngineBackend, EngineCaps, EngineSpec, GenDelta, GenResult


class SGLangEngine(EngineBackend):
    name = "sglang"

    def __init__(self) -> None:
        self.engine = None
        self.tokenizer = None
        self._warmed = False
        self._init_stats: Dict[str, Any] = {}

    # ---- lifecycle -----------------------------------------------------------

    def create(self, spec: EngineSpec) -> None:
        from ..server import print_red
        from ..vendors import get_vendor

        init_start = time.monotonic()
        pid = os.getpid()
        hostname = socket.gethostname()
        self.model_id = spec.model_id
        self._vendor = get_vendor()
        gpu_ids = list(spec.device_ids)
        device_id = gpu_ids[0] if gpu_ids else 0

        # Device isolation (delegated to the vendor layer; XPU sets a valid
        # ONEAPI_DEVICE_SELECTOR + ZE_AFFINITY_MASK, CUDA/ROCm set their own).
        self._vendor.isolate_devices(gpu_ids, "sglang")
        os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
        os.environ.setdefault("RAYON_NUM_THREADS", "1")
        print(
            f"[SGLangEngine pid={pid}] vendor={self._vendor.name} tile {gpu_ids}",
            flush=True,
        )

        model_path = spec.local_path or spec.model_id
        attention_backend = (
            os.environ.get("EXASERVE_XPU_SGLANG_ATTENTION")
            or self._vendor.sglang_default_attention()
        )

        import sglang as sgl
        from transformers import AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True)
        engine_kwargs = dict(
            model_path=model_path,
            device=self._vendor.torch_device(),
            tp_size=spec.tensor_parallel_size,
            mem_fraction_static=spec.gpu_memory_utilization,
            context_length=spec.max_model_len,
            disable_overlap_schedule=True,
            grammar_backend="none",
            page_size=64,
            max_running_requests=(spec.max_num_seqs or 256),
            trust_remote_code=True,
            log_level="warning",
            # unique per node to avoid the get_free_port() race across 12 engines
            nccl_port=25100 + device_id,
        )
        if attention_backend:
            engine_kwargs["attention_backend"] = attention_backend
        engine_kwargs.update(spec.extra_engine_kwargs)

        # SGLang spawns its scheduler via multiprocessing 'spawn', re-importing
        # __main__ (Ray's default_worker -> pyarrow jemalloc SIGSEGV). Neutralize
        # __main__ so the spawned scheduler re-imports nothing.
        _main = sys.modules.get("__main__")
        if _main is not None:
            try:
                _main.__spec__ = None
            except Exception:
                pass
            if hasattr(_main, "__file__"):
                try:
                    del _main.__file__
                except Exception:
                    pass
        print(
            f"[SGLangEngine pid={pid}] Creating sgl.Engine for {spec.model_id} "
            f"(attn={attention_backend})...",
            flush=True,
        )
        self.engine = sgl.Engine(**engine_kwargs)
        self._warmed = False
        print_red(
            f"[SGLangEngine pid={pid}] ★ INIT TOTAL: {time.monotonic() - init_start:.2f}s ★"
        )
        self._init_stats = {
            "device_id": device_id,
            "engine": "sglang",
            "total_init_s": round(time.monotonic() - init_start, 4),
        }

    def init_stats(self) -> Dict[str, Any]:
        return dict(self._init_stats)

    def capabilities(self) -> EngineCaps:
        return EngineCaps(streaming=True, serving_stats=False, needs_warmup=True)

    async def warmup(self) -> None:
        """JIT-compile sglang's paged-allocator kernels before reporting healthy."""
        if self._warmed:
            return
        t_warm = time.monotonic()
        await self.engine.async_generate(
            prompt="warmup", sampling_params={"max_new_tokens": 8, "temperature": 0.0}
        )
        self._warmed = True
        from ..server import print_red
        print_red(
            f"[SGLangEngine pid={os.getpid()}] warmup generate: "
            f"{time.monotonic() - t_warm:.2f}s"
        )

    # ---- prompt --------------------------------------------------------------

    def build_chat_prompt(
        self,
        messages: list,
        *,
        add_generation_prompt: bool = True,
        continue_final_message: bool = False,
        chat_template: Optional[str] = None,
        chat_template_kwargs: Optional[dict] = None,
    ) -> str:
        from ..server import _chat_messages_to_plain_prompt
        try:
            return self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=add_generation_prompt,
                continue_final_message=continue_final_message,
            )
        except Exception:
            return _chat_messages_to_plain_prompt(
                messages,
                add_generation_prompt=add_generation_prompt and not continue_final_message,
            )

    # ---- generation ----------------------------------------------------------

    @staticmethod
    def _to_sglang_sp(sampling: Dict[str, Any]) -> Dict[str, Any]:
        """Map the neutral OpenAI-parsed dict to SGLang sampling params."""
        sp: Dict[str, Any] = {}
        if "temperature" in sampling:
            sp["temperature"] = float(sampling["temperature"])
        if "top_p" in sampling:
            sp["top_p"] = float(sampling["top_p"])
        if "max_tokens" in sampling:
            sp["max_new_tokens"] = int(sampling["max_tokens"])
        if "min_tokens" in sampling:
            sp["min_new_tokens"] = int(sampling["min_tokens"])
        if "stop" in sampling:
            sp["stop"] = sampling["stop"]
        if sampling.get("ignore_eos"):
            sp["ignore_eos"] = True
        sp.setdefault("temperature", 0.7)
        sp.setdefault("max_new_tokens", 1024)
        return sp

    @staticmethod
    def _finish_reason(meta: dict):
        fr = meta.get("finish_reason") if isinstance(meta, dict) else None
        if isinstance(fr, dict):
            return fr.get("type", "stop")
        return fr or "stop"

    def _abort_engine_request(self, rid: str):
        try:
            self.engine.tokenizer_manager.abort_request(rid=rid)
        except Exception:
            pass

    async def generate(self, prompt: str, sampling: Dict[str, Any]) -> GenResult:
        sp = self._to_sglang_sp(sampling)
        rid = f"exa-{uuid.uuid4().hex}"
        try:
            out = await self.engine.async_generate(prompt=prompt, sampling_params=sp, rid=rid)
        except asyncio.CancelledError:
            self._abort_engine_request(rid)
            raise
        except Exception as exc:
            return GenResult(error=f"{type(exc).__name__}: {exc}")
        if isinstance(out, list):
            out = out[0]
        meta = out.get("meta_info", {}) or {}
        return GenResult(
            text=out.get("text", ""),
            finish_reason=self._finish_reason(meta),
            prompt_tokens=int(meta.get("prompt_tokens", 0)),
            completion_tokens=int(meta.get("completion_tokens", 0)),
        )

    async def generate_stream(
        self, prompt: str, sampling: Dict[str, Any]
    ) -> AsyncIterator[GenDelta]:
        sp = self._to_sglang_sp(sampling)
        prev = ""
        last_meta: Dict[str, Any] = {}
        rid = f"exa-{uuid.uuid4().hex}"
        finished = False
        gen = await self.engine.async_generate(
            prompt=prompt, sampling_params=sp, stream=True, rid=rid
        )
        try:
            async for out in gen:
                if isinstance(out, list):
                    out = out[0]
                text = out.get("text", "")
                last_meta = out.get("meta_info", {}) or last_meta
                delta = text[len(prev):]
                prev = text
                if delta:
                    yield GenDelta(delta=delta)
            finished = True
        finally:
            if not finished:
                self._abort_engine_request(rid)
        yield GenDelta(
            delta="",
            finish_reason=self._finish_reason(last_meta),
            prompt_tokens=int(last_meta.get("prompt_tokens", 0)),
            completion_tokens=int(last_meta.get("completion_tokens", 0)),
        )
