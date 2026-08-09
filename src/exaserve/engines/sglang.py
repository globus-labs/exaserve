"""SGLang engine backend — extracted verbatim from the former ``SGLangWorker``.

``sglang`` is imported lazily inside ``create()`` (transformers-pin isolation).
"""

from __future__ import annotations

import asyncio
import os
import sys
import time
import uuid
from typing import Any, AsyncIterator, Dict, Optional

from ..exception_notes import add_exception_note
from .base import EngineBackend, EngineCaps, EngineSpec, GenDelta, GenResult, merge_engine_kwargs


class SGLangEngine(EngineBackend):
    name = "sglang"
    _ENGINE_SHUTDOWN_TIMEOUT_S = 30.0

    def __init__(self) -> None:
        self.engine = None
        self.tokenizer = None
        self._warmed = False
        self._init_stats: Dict[str, Any] = {}

    # ---- lifecycle -----------------------------------------------------------

    def create(self, spec: EngineSpec) -> None:
        # Keep the backend importable without Ray.  Engine unit tests and
        # tooling must not load the Ray Serve host merely to use a console
        # helper.
        from ..model_staging import print_red
        from ..vendors import get_vendor

        init_start = time.monotonic()
        pid = os.getpid()
        self.model_id = spec.model_id
        self._vendor = get_vendor(spec.vendor_name)
        gpu_ids = list(spec.device_ids)
        device_id = gpu_ids[0] if gpu_ids else 0

        # Device isolation is delegated to the vendor layer.  Aurora XPU uses
        # ZE_AFFINITY_MASK and explicitly removes ONEAPI_DEVICE_SELECTOR;
        # CUDA/ROCm adapters set their own visibility variables.
        self._vendor.isolate_devices(gpu_ids, "sglang")
        os.environ["TOKENIZERS_PARALLELISM"] = "false"
        os.environ["RAYON_NUM_THREADS"] = "1"
        print(
            f"[SGLangEngine pid={pid}] vendor={self._vendor.name} tile {gpu_ids}",
            flush=True,
        )

        model_path = spec.local_path or spec.model_id
        attention_backend = self._vendor.sglang_default_attention()

        import sglang as sgl
        from transformers import AutoTokenizer

        self.tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=False)
        from ..state.ports import reserve_port

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
            trust_remote_code=False,
            log_level="warning",
        )
        if attention_backend:
            engine_kwargs["attention_backend"] = attention_backend
        engine_kwargs = merge_engine_kwargs(
            engine_kwargs,
            spec.extra_engine_kwargs,
            protected=(
                "device",
                "model_path",
                "nccl_port",
                "tp_size",
                "trust_remote_code",
            ),
        )

        # SGLang spawns its scheduler via multiprocessing 'spawn', re-importing
        # __main__ (Ray's default_worker -> pyarrow jemalloc SIGSEGV). Neutralize
        # __main__ so the spawned scheduler re-imports nothing.
        _main = sys.modules.get("__main__")
        _main_spec = getattr(_main, "__spec__", None) if _main is not None else None
        _main_had_file = _main is not None and hasattr(_main, "__file__")
        _main_file = getattr(_main, "__file__", None) if _main_had_file else None
        port_lease = reserve_port(25100 + device_id * 100, bind_host="0.0.0.0")
        engine_kwargs["nccl_port"] = port_lease.port
        try:
            print(
                f"[SGLangEngine pid={pid}] Creating sgl.Engine for {spec.model_id} "
                f"(attn={attention_backend}, nccl_port={port_lease.port})...",
                flush=True,
            )
            if _main is not None:
                _main.__spec__ = None
                if _main_had_file:
                    del _main.__file__
            self.engine = sgl.Engine(**engine_kwargs)
        finally:
            # The spawn workaround is needed only while Engine creates its
            # children.  Leaving __main__ corrupted breaks unrelated future
            # multiprocessing in this long-lived Serve replica.
            active_error = sys.exc_info()[1]
            try:
                if _main is not None:
                    _main.__spec__ = _main_spec
                    if _main_had_file:
                        _main.__file__ = _main_file
            except BaseException as restore_exc:
                if active_error is None:
                    active_error = restore_exc
                else:
                    add_exception_note(
                        active_error,
                        f"SGLang __main__ restoration also failed: {restore_exc}",
                    )
            try:
                port_lease.release()
            except BaseException as release_exc:
                if active_error is None:
                    raise
                add_exception_note(
                    active_error, f"SGLang port lease cleanup also failed: {release_exc}"
                )
            if active_error is not None and sys.exc_info()[1] is None:
                raise active_error
        self._warmed = False
        print_red(f"[SGLangEngine pid={pid}] ★ INIT TOTAL: {time.monotonic() - init_start:.2f}s ★")
        self._init_stats = {
            "device_id": device_id,
            "engine": "sglang",
            "total_init_s": round(time.monotonic() - init_start, 4),
        }

    def init_stats(self) -> Dict[str, Any]:
        return dict(self._init_stats)

    def capabilities(self) -> EngineCaps:
        return EngineCaps(streaming=True, serving_stats=False, needs_warmup=True)

    async def shutdown(self) -> None:
        """Bound SGLang's scheduler/detokenizer process-tree teardown."""
        engine = self.engine
        if engine is None:
            return
        shutdown = getattr(engine, "shutdown", None)
        if not callable(shutdown):
            raise RuntimeError("installed SGLang engine exposes no shutdown method")
        try:
            await asyncio.wait_for(
                asyncio.to_thread(shutdown),
                timeout=self._ENGINE_SHUTDOWN_TIMEOUT_S,
            )
        except TimeoutError as exc:
            raise RuntimeError(
                f"SGLang process-tree shutdown exceeded {self._ENGINE_SHUTDOWN_TIMEOUT_S:g} seconds"
            ) from exc
        self.engine = None

    async def warmup(self) -> None:
        """JIT-compile sglang's paged-allocator kernels before reporting healthy."""
        if self._warmed:
            return
        t_warm = time.monotonic()
        await self.engine.async_generate(
            prompt="warmup", sampling_params={"max_new_tokens": 8, "temperature": 0.0}
        )
        self._warmed = True
        from ..model_staging import print_red

        print_red(
            f"[SGLangEngine pid={os.getpid()}] warmup generate: {time.monotonic() - t_warm:.2f}s"
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
        from ..prompting import chat_messages_to_plain_prompt

        try:
            return self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=add_generation_prompt,
                continue_final_message=continue_final_message,
                chat_template=chat_template,
                **(chat_template_kwargs or {}),
            )
        except ValueError as exc:
            if "chat_template" not in str(exc):
                raise
            # Flattening structured messages changes the model input and can
            # change the answer. It therefore uses the same explicit capability
            # gate as the vLLM backend rather than silently degrading.
            from ..capabilities import degrade_or_refuse

            degrade_or_refuse(
                "chat_template_fallback", f"model {self.model_id} has no chat template"
            )
            print(
                "[ExaServe] Tokenizer has no chat template; "
                f"falling back to plain-text prompt for {self.model_id}",
                flush=True,
            )
            return chat_messages_to_plain_prompt(
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
            fr = fr.get("type", "stop")
        if fr is None:
            return "stop"
        if not isinstance(fr, str) or not fr:
            raise RuntimeError("SGLang returned an invalid finish reason")
        return fr

    @staticmethod
    def _token_count(meta: dict, field: str) -> int:
        value = meta.get(field, 0)
        if type(value) is not int or value < 0:
            raise RuntimeError(f"SGLang returned invalid {field}={value!r}")
        return value

    @staticmethod
    def _output(value: object) -> tuple[str, dict]:
        if isinstance(value, list):
            if len(value) != 1:
                raise RuntimeError("SGLang returned an unexpected output list")
            value = value[0]
        if not isinstance(value, dict):
            raise RuntimeError("SGLang returned a non-object output")
        text = value.get("text")
        meta = value.get("meta_info", {})
        if not isinstance(text, str) or not isinstance(meta, dict):
            raise RuntimeError("SGLang output text/metadata has an invalid type")
        return text, meta

    def _abort_engine_request(self, rid: str) -> bool:
        try:
            self.engine.tokenizer_manager.abort_request(rid=rid)
        except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
            print(
                f"[SGLangEngine] WARNING: failed to abort request {rid}: "
                f"{type(exc).__name__}: {exc}",
                file=sys.stderr,
                flush=True,
            )
            return False
        return True

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
        text, meta = self._output(out)
        return GenResult(
            text=text,
            finish_reason=self._finish_reason(meta),
            prompt_tokens=self._token_count(meta, "prompt_tokens"),
            completion_tokens=self._token_count(meta, "completion_tokens"),
        )

    async def generate_stream(
        self, prompt: str, sampling: Dict[str, Any]
    ) -> AsyncIterator[GenDelta]:
        sp = self._to_sglang_sp(sampling)
        prev = ""
        last_meta: Dict[str, Any] = {}
        rid = f"exa-{uuid.uuid4().hex}"
        finished = False
        saw_output = False
        gen = await self.engine.async_generate(
            prompt=prompt, sampling_params=sp, stream=True, rid=rid
        )
        try:
            async for out in gen:
                saw_output = True
                text, meta = self._output(out)
                last_meta = meta or last_meta
                delta = text[len(prev) :]
                prev = text
                if delta:
                    yield GenDelta(delta=delta)
            if not saw_output:
                raise RuntimeError("SGLang stream ended without an output")
            finished = True
        finally:
            if not finished:
                self._abort_engine_request(rid)
        yield GenDelta(
            delta="",
            finish_reason=self._finish_reason(last_meta),
            prompt_tokens=self._token_count(last_meta, "prompt_tokens"),
            completion_tokens=self._token_count(last_meta, "completion_tokens"),
        )
