"""vLLM engine backend — extracted verbatim from the former ``VLLMWorker``.

All server-module helpers are imported lazily (inside methods) to avoid a
circular import with ``exaserve.server``, and ``vllm`` itself is imported only
inside ``create()`` / generation (transformers-pin isolation).
"""

from __future__ import annotations

import os
import platform
import socket
import time
import uuid
from typing import Any, AsyncIterator, Dict, Optional

from .base import EngineBackend, EngineCaps, EngineSpec, GenDelta, GenResult


class VLLMEngine(EngineBackend):
    name = "vllm"

    def __init__(self) -> None:
        self.engine = None
        self.stats_collector = None
        self._collect_stats = False
        self._init_stats: Dict[str, Any] = {}

    # ---- lifecycle -----------------------------------------------------------

    _leased_port = None

    def create(self, spec: EngineSpec) -> None:
        from ..server import (
            async_engine_arg_supported,
            get_hsn_ip,
            get_open_port,
            get_ray_node_ip,
            print_red,
            _parse_own_engine_log,
            _serving_stats_push_loop,
            CollectingStatLogger,
        )

        from ..vendors import get_vendor
        from ..compat import engine_shim as _shim

        init_mono = time.monotonic()
        pid = os.getpid()
        hostname = socket.gethostname()
        self.model_id = spec.model_id
        self._collect_stats = spec.collect_stats
        self._vendor = get_vendor()
        gpu_ids = list(spec.device_ids)
        device_id = gpu_ids[0] if gpu_ids else 0

        # ---- Device isolation (delegated to the vendor layer) ---------------
        t0 = time.monotonic()
        self._vendor.isolate_devices(gpu_ids, "vllm")
        if gpu_ids:
            print(
                f"[VLLMEngine pid={pid}] vendor={self._vendor.name} assigned "
                f"devices {gpu_ids}",
                flush=True,
            )
        else:
            print(
                f"[VLLMEngine pid={pid}] No Ray GPUs assigned to coordinator actor; "
                f"waiting for vLLM Ray workers to claim GPUs",
                flush=True,
            )
        device_isolation_s = time.monotonic() - t0

        # ---- Engine self-attestation shim (EN-01) ---------------------------
        # Installed for every engine so the spawned EngineCore can attest what
        # it received. Patch DELIVERY is unchanged: only PP asks the shim to
        # import the patch module, exactly as before this existed.
        pp = spec.pipeline_parallel_size
        self._engine_receipt_dir = _shim.receipt_dir_for(
            os.environ.get("EXASERVE_DEPLOYMENT_ID", "unknown"),
            int(os.environ.get("EXASERVE_GENERATION", "0") or 0))
        _shim_versions = {"python": platform.python_version()}
        for _mod in ("ray", "vllm"):
            try:
                _shim_versions[_mod] = __import__(_mod).__version__
            except Exception:
                pass
        if not _shim.install(
                "/tmp/exaserve_pp_shim", self._engine_receipt_dir,
                import_patches=(pp > 1),
                deployment_id=os.environ.get("EXASERVE_DEPLOYMENT_ID", "unknown"),
                generation=int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
                vendor=os.environ.get("EXASERVE_VENDOR", "xpu"),
                versions=_shim_versions):
            print(f"[VLLMEngine pid={pid}] WARNING: engine shim setup failed; "
                  "the engine will fall back to owner attestation", flush=True)

        # ---- Distributed init port ------------------------------------------
        t0 = time.monotonic()
        master_addr = "127.0.0.1"
        bind_host = "127.0.0.1"
        if pp > 1:
            if not async_engine_arg_supported("pipeline_parallel_size"):
                raise RuntimeError(
                    "Installed vLLM build does not expose pipeline_parallel_size on "
                    "AsyncEngineArgs. Validate the runtime before using PP."
                )
            for _k, _v in self._vendor.distributed_env("vllm").items():
                os.environ.setdefault(_k, _v)
            master_addr = get_ray_node_ip() or get_hsn_ip()
            bind_host = "0.0.0.0"
            os.environ["VLLM_HOST_IP"] = master_addr

        # PR-012/KI-A2: the port stays LEASED until the engine has bound it,
        # so a sibling replica starting in the same instant cannot pick it.
        port = get_open_port(23000 + device_id * 100, bind_host=bind_host)
        if port is None:
            raise RuntimeError(f"No free port for distributed init (device {device_id})")
        self._leased_port = port
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = str(port)
        dist_setup_s = time.monotonic() - t0
        print(
            f"[VLLMEngine pid={pid}] Using distributed master {master_addr}:{port} "
            f"(PP={pp})",
            flush=True,
        )

        # ---- vLLM async engine ----------------------------------------------
        model_path = spec.local_path or spec.model_id
        engine_kwargs = dict(
            model=model_path,
            tensor_parallel_size=spec.tensor_parallel_size,
            master_addr=master_addr,
            master_port=port,
            gpu_memory_utilization=spec.gpu_memory_utilization,
            max_model_len=spec.max_model_len,
            enforce_eager=spec.enforce_eager,
        )
        if spec.max_num_seqs is not None:
            engine_kwargs["max_num_seqs"] = spec.max_num_seqs
        if pp > 1:
            engine_kwargs["pipeline_parallel_size"] = pp
            engine_kwargs["distributed_executor_backend"] = "ray"
        engine_kwargs.update(spec.extra_engine_kwargs)

        from vllm.engine.arg_utils import AsyncEngineArgs
        from vllm.engine.async_llm_engine import AsyncLLMEngine
        t0 = time.monotonic()
        engine_args = AsyncEngineArgs(**engine_kwargs)
        # PR-022: honor the operator's enable_log_requests setting. The
        # hasattr guard remains for vLLM versions that lack the field.
        if hasattr(engine_args, "enable_log_requests"):
            engine_args.enable_log_requests = spec.enable_log_requests
        elif hasattr(engine_args, "disable_log_requests"):
            engine_args.disable_log_requests = not spec.enable_log_requests
        engine_args_s = time.monotonic() - t0

        print(f"[VLLMEngine pid={pid}] Creating vLLM engine for {spec.model_id}...", flush=True)
        engine_start = time.monotonic()
        extra_engine_kwargs: Dict[str, Any] = {}
        if self._collect_stats:
            extra_engine_kwargs["stat_loggers"] = [CollectingStatLogger]
        try:
            self.engine = AsyncLLMEngine.from_engine_args(engine_args, **extra_engine_kwargs)
        finally:
            # The engine has bound (or failed to bind) MASTER_PORT by now, so
            # the lease has done its job. Releasing on the failure path too
            # keeps a crashed replica from permanently reserving a port.
            from ..server import release_port

            release_port(self._leased_port)
            self._leased_port = None
        engine_create_s = time.monotonic() - engine_start
        print_red(f"[VLLMEngine pid={pid}] Engine creation: {engine_create_s:.2f}s")

        if self._collect_stats:
            self.stats_collector = CollectingStatLogger.get_instance()
            if self.stats_collector is not None:
                print(f"[VLLMEngine pid={pid}] Stats collection enabled", flush=True)
            else:
                print(f"[VLLMEngine pid={pid}] WARNING: CollectingStatLogger not instantiated by engine", flush=True)
            try:
                import threading
                threading.Thread(target=_serving_stats_push_loop, daemon=True).start()
                print(f"[VLLMEngine pid={pid}] serving-stats push thread started", flush=True)
            except Exception as _e:
                print(f"[VLLMEngine pid={pid}] serving-stats push thread failed: {_e}", flush=True)

        total_s = time.monotonic() - init_mono
        print_red(f"[VLLMEngine pid={pid}] ★ INIT TOTAL: {total_s:.2f}s ★")

        self._init_stats = {
            "device_id": device_id,
            "gpu_ids": gpu_ids,
            "tensor_parallel_size": spec.tensor_parallel_size,
            "pipeline_parallel_size": pp,
            "total_init_s": round(total_s, 4),
            "device_isolation_s": round(device_isolation_s, 4),
            "dist_setup_s": round(dist_setup_s, 4),
            "engine_args_s": round(engine_args_s, 4),
            "engine_create_s": round(engine_create_s, 4),
            "wall_end": time.time(),
        }
        engine_sub = _parse_own_engine_log(pid)
        if engine_sub:
            self._init_stats["engine_sub_phases"] = engine_sub

    def init_stats(self) -> Dict[str, Any]:
        return dict(self._init_stats)

    def capabilities(self) -> EngineCaps:
        return EngineCaps(streaming=True, serving_stats=bool(self._collect_stats))

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
        tokenizer = self.engine.get_tokenizer()
        try:
            return tokenizer.apply_chat_template(
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
            # TD-CHATTPL: flattening messages to plain text is a DIFFERENT
            # prompt than the model was tuned on, so outputs are not comparable
            # to a correctly templated run. That is an answer-changing
            # substitution and must be requested, not assumed.
            from ..capabilities import degrade_or_refuse

            degrade_or_refuse("chat_template_fallback",
                              f"model {self.model_id} has no chat template")
            print(
                "[ExaServe] Tokenizer has no chat template; "
                f"falling back to plain-text prompt for {self.model_id} "
                "(EXASERVE_ALLOW_CHAT_TEMPLATE_FALLBACK=1)",
                flush=True,
            )
            return _chat_messages_to_plain_prompt(
                messages,
                add_generation_prompt=add_generation_prompt and not continue_final_message,
            )

    # ---- generation ----------------------------------------------------------

    def _params(self, sampling: Dict[str, Any]):
        from vllm import SamplingParams
        kwargs = {k: v for k, v in sampling.items() if not k.startswith("_")}
        return SamplingParams(**kwargs)

    async def generate(self, prompt: str, sampling: Dict[str, Any]) -> GenResult:
        params = self._params(sampling)
        request_id = sampling.get("_request_id") or str(uuid.uuid4())
        final = None
        async for output in self.engine.generate(prompt, params, request_id):
            final = output
        if final is None:
            return GenResult(error="No output generated")
        choice = final.outputs[0]
        return GenResult(
            text=choice.text,
            finish_reason=choice.finish_reason or "stop",
            prompt_tokens=len(final.prompt_token_ids),
            completion_tokens=len(choice.token_ids),
        )

    async def generate_stream(
        self, prompt: str, sampling: Dict[str, Any]
    ) -> AsyncIterator[GenDelta]:
        params = self._params(sampling)
        request_id = sampling.get("_request_id") or str(uuid.uuid4())
        prev_text = ""
        output = None
        async for output in self.engine.generate(prompt, params, request_id):
            text = output.outputs[0].text
            delta = text[len(prev_text):]
            prev_text = text
            if delta:
                yield GenDelta(delta=delta)
        if output is not None:
            choice = output.outputs[0]
            yield GenDelta(
                delta="",
                finish_reason=choice.finish_reason or "stop",
                prompt_tokens=len(output.prompt_token_ids),
                completion_tokens=len(choice.token_ids),
            )

    # ---- stats ---------------------------------------------------------------

    def collect_stats(self) -> Dict[str, Any]:
        # PR-021: consume the SAME schema the producer ships
        # (summary/sample/scheduler_snapshots). The old code read a
        # nonexistent `finished_requests` key -> KeyError on every call, and
        # even its field names (e2e_latency/queued_time/prefill_time)
        # disagreed with what sample() emits. to_dict() already computes the
        # bounded per-replica summary; we just annotate and return it.
        pid = os.getpid()
        if self.stats_collector is None:
            return {"pid": pid, "model": self.model_id, "error": "stats collection not enabled"}
        data = self.stats_collector.to_dict()
        data["pid"] = pid
        data["model"] = self.model_id
        return data

    def live_stats(self) -> Dict[str, Any]:
        """Snapshot for the /stats endpoint (was VLLMWorker.stats)."""
        from ..server import CollectingStatLogger
        result: Dict[str, Any] = {}
        collector = CollectingStatLogger.get_instance()
        if collector is not None:
            snaps = collector.scheduler_snapshots
            reqs = collector.finished_requests
            result["latest_scheduler"] = snaps[-1] if snaps else None
            result["total_finished_requests"] = len(reqs)
            result["scheduler_snapshot_count"] = len(snaps)
        return result
