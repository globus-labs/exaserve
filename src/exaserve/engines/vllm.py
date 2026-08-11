"""vLLM engine backend — extracted verbatim from the former ``VLLMWorker``.

All server-module helpers are imported lazily (inside methods) to avoid a
circular import with ``exaserve.server``, and ``vllm`` itself is imported only
inside ``create()`` / generation (transformers-pin isolation).
"""

from __future__ import annotations

import os
import platform
import sys
import threading
import time
import uuid
from typing import Any, AsyncIterator, Dict, Optional

from .base import EngineBackend, EngineCaps, EngineSpec, GenDelta, GenResult, merge_engine_kwargs
from ..exception_notes import add_exception_note


class VLLMEngine(EngineBackend):
    name = "vllm"
    _ENGINE_SHUTDOWN_TIMEOUT_S = 30.0

    def __init__(self) -> None:
        self.engine = None
        self._leased_port: Any = None
        self.stats_collector = None
        self._collect_stats = False
        self._init_stats: Dict[str, Any] = {}
        self._stats_stop = threading.Event()
        self._stats_thread: threading.Thread | None = None

    # ---- lifecycle -----------------------------------------------------------

    def create(self, spec: EngineSpec) -> None:
        from ..model_staging import print_red
        from ..state.ports import reserve_port
        from .vllm_support import (
            async_engine_arg_supported,
            parse_engine_log,
            ray_node_ip,
            routed_node_ip,
        )

        from ..vendors import get_vendor
        from ..compat import engine_shim as _shim

        init_mono = time.monotonic()
        pid = os.getpid()
        self.model_id = spec.model_id
        self._collect_stats = spec.collect_stats
        CollectingStatLogger = None
        if self._collect_stats:
            # Serving-stat publication is a Ray-host integration and is loaded
            # only when the plan requests it.  Normal backend construction no
            # longer imports the whole Serve host.
            from ..server import CollectingStatLogger as _CollectingStatLogger

            CollectingStatLogger = _CollectingStatLogger
            CollectingStatLogger.configure(retention=spec.stats_retention)
        self._vendor = get_vendor(spec.vendor_name)
        gpu_ids = list(spec.device_ids)
        device_id = gpu_ids[0] if gpu_ids else 0

        # ---- Device isolation (delegated to the vendor layer) ---------------
        t0 = time.monotonic()
        self._vendor.isolate_devices(gpu_ids, "vllm")
        if gpu_ids:
            print(
                f"[VLLMEngine pid={pid}] vendor={self._vendor.name} assigned devices {gpu_ids}",
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
            int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
            os.environ.get("EXASERVE_RECEIPT_COMPONENT_ID_ENGINE", "unknown-engine"),
        )
        _shim_versions = {"python": platform.python_version()}
        for _mod in ("ray", "vllm"):
            try:
                _shim_versions[_mod] = __import__(_mod).__version__
            except (ImportError, AttributeError):
                _shim_versions[_mod] = "unavailable"
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
                os.environ[_k] = _v
            master_addr = ray_node_ip() or routed_node_ip()
            bind_host = "0.0.0.0"
            os.environ["VLLM_HOST_IP"] = master_addr

        # PR-012/KI-A2: the port stays LEASED until the engine has bound it,
        # so a sibling replica starting in the same instant cannot pick it.
        self._leased_port = reserve_port(23000 + device_id * 100, bind_host=bind_host)
        port = self._leased_port.port
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = str(port)
        # vLLM v1's UniprocExecutor does not consume AsyncEngineArgs.master_port
        # when it creates its local torch process group; it calls
        # network_utils.get_open_port() again.  Point that supported allocator
        # at the same ExaServe-owned lease so concurrent Serve replicas cannot
        # independently probe and select one ephemeral port.
        os.environ["VLLM_PORT"] = str(port)
        dist_setup_s = time.monotonic() - t0
        print(
            f"[VLLMEngine pid={pid}] Using distributed master {master_addr}:{port} (PP={pp})",
            flush=True,
        )

        # ---- vLLM async engine ----------------------------------------------
        def construct_engine() -> tuple[float, float]:
            model_path = spec.local_path or spec.model_id
            engine_kwargs = dict(
                model=model_path,
                tensor_parallel_size=spec.tensor_parallel_size,
                master_addr=master_addr,
                master_port=port,
                gpu_memory_utilization=spec.gpu_memory_utilization,
                max_model_len=spec.max_model_len,
                enforce_eager=spec.enforce_eager,
                trust_remote_code=False,
            )
            if spec.max_num_seqs is not None:
                engine_kwargs["max_num_seqs"] = spec.max_num_seqs
            if pp > 1:
                engine_kwargs["pipeline_parallel_size"] = pp
                engine_kwargs["distributed_executor_backend"] = "ray"
            engine_kwargs = merge_engine_kwargs(
                engine_kwargs,
                spec.extra_engine_kwargs,
                protected=(
                    "distributed_executor_backend",
                    "pipeline_parallel_size",
                    "tensor_parallel_size",
                    "master_addr",
                    "master_port",
                    "model",
                    "trust_remote_code",
                ),
            )

            from vllm.engine.arg_utils import AsyncEngineArgs
            from vllm.engine.async_llm_engine import AsyncLLMEngine

            args_start = time.monotonic()
            engine_args = AsyncEngineArgs(**engine_kwargs)
            # PR-022: honor the operator's enable_log_requests setting. The
            # hasattr guard remains for vLLM versions that lack the field.
            if hasattr(engine_args, "enable_log_requests"):
                engine_args.enable_log_requests = spec.enable_log_requests
            elif hasattr(engine_args, "disable_log_requests"):
                engine_args.disable_log_requests = not spec.enable_log_requests
            engine_args_elapsed = time.monotonic() - args_start

            print(
                f"[VLLMEngine pid={pid}] Creating vLLM engine for {spec.model_id}...",
                flush=True,
            )
            engine_start = time.monotonic()
            extra_engine_kwargs: Dict[str, Any] = {}
            if self._collect_stats:
                assert CollectingStatLogger is not None
                extra_engine_kwargs["stat_loggers"] = [CollectingStatLogger]
            shim_parent_environment = _shim.environment_snapshot()
            try:
                _shim.prepare_environment(
                    self._engine_receipt_dir,
                    import_patches=(pp > 1 or spec.tensor_parallel_size > 1),
                    deployment_id=os.environ.get("EXASERVE_DEPLOYMENT_ID", "unknown"),
                    generation=int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
                    vendor=os.environ.get("EXASERVE_VENDOR", "xpu"),
                    engine_kind="vllm",
                    versions=_shim_versions,
                )
                self.engine = AsyncLLMEngine.from_engine_args(engine_args, **extra_engine_kwargs)
            finally:
                # EngineCore has inherited the startup shim. Leaving it installed
                # here would inject sitecustomize into unrelated subprocesses.
                active_error = sys.exc_info()[1]
                try:
                    _shim.restore_environment(shim_parent_environment)
                except BaseException as restore_exc:
                    if active_error is None:
                        raise
                    add_exception_note(
                        active_error,
                        f"engine shim environment restoration also failed: {restore_exc}",
                    )
            return engine_args_elapsed, time.monotonic() - engine_start

        try:
            engine_args_s, engine_create_s = construct_engine()
        finally:
            # Release on every preflight/constructor path, not only after the
            # final constructor call.  Invalid kwargs or missing imports must
            # not strand a port claim in the long-lived Serve process.
            active_error = sys.exc_info()[1]
            lease = self._leased_port
            try:
                if lease is not None:
                    lease.release()
            except BaseException as release_exc:
                if active_error is None:
                    raise
                add_exception_note(
                    active_error, f"vLLM port lease cleanup also failed: {release_exc}"
                )
            else:
                self._leased_port = None
        print_red(f"[VLLMEngine pid={pid}] Engine creation: {engine_create_s:.2f}s")

        try:
            if self._collect_stats:
                assert CollectingStatLogger is not None
                self.stats_collector = CollectingStatLogger.get_instance()
                if self.stats_collector is not None:
                    print(f"[VLLMEngine pid={pid}] Stats collection enabled", flush=True)
                else:
                    raise RuntimeError(
                        "collect_stats was requested but vLLM did not instantiate "
                        "CollectingStatLogger"
                    )
                from ..server import _serving_stats_push_loop

                self._stats_stop.clear()
                self._stats_thread = threading.Thread(
                    target=_serving_stats_push_loop,
                    args=(
                        self.model_id,
                        self._stats_stop,
                        spec.stats_push_period_s,
                        spec.stats_sample_cap,
                    ),
                    name="exaserve-serving-stats",
                    daemon=True,
                )
                self._stats_thread.start()
                print(f"[VLLMEngine pid={pid}] serving-stats push thread started", flush=True)
        except BaseException as exc:
            if self._collect_stats:
                from ..observability import record_telemetry_drop

                record_telemetry_drop("serving_stats", "push_failed")
            cleanup_errors = []
            if self._stats_thread is not None:
                self._stats_stop.set()
                try:
                    self._stats_thread.join(6.0)
                except RuntimeError as cleanup_exc:
                    cleanup_errors.append(f"serving-stats thread join failed: {cleanup_exc}")
                else:
                    if self._stats_thread.is_alive():
                        cleanup_errors.append("serving-stats thread survived failed engine startup")
                    else:
                        self._stats_thread = None
            shutdown = getattr(self.engine, "shutdown", None)
            if callable(shutdown):
                try:
                    shutdown()
                    self.engine = None
                except BaseException as cleanup_exc:
                    cleanup_errors.append(
                        f"vLLM cleanup after failed startup raised "
                        f"{type(cleanup_exc).__name__}: {cleanup_exc}"
                    )
            elif self.engine is not None:
                cleanup_errors.append("created vLLM engine exposes no shutdown method")
            for message in cleanup_errors:
                add_exception_note(exc, message)
            raise

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
        engine_sub = parse_engine_log(pid)
        if engine_sub:
            self._init_stats["engine_sub_phases"] = engine_sub

    def init_stats(self) -> Dict[str, Any]:
        return dict(self._init_stats)

    def capabilities(self) -> EngineCaps:
        return EngineCaps(streaming=True, serving_stats=bool(self._collect_stats))

    async def shutdown(self) -> None:
        """Stop telemetry and the owned EngineCore/IPC process tree."""
        import asyncio

        failures: list[str] = []
        thread = self._stats_thread
        if thread is not None:
            self._stats_stop.set()
            await asyncio.to_thread(thread.join, 6.0)
            if thread.is_alive():
                from ..observability import record_telemetry_drop

                record_telemetry_drop("serving_stats", "shutdown_timeout")
                failures.append("serving-stats thread did not stop within 6 seconds")
            else:
                self._stats_thread = None

        engine = self.engine
        if engine is not None:
            shutdown = getattr(engine, "shutdown", None)
            if not callable(shutdown):
                failures.append("installed vLLM engine exposes no shutdown method")
            else:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(shutdown),
                        timeout=self._ENGINE_SHUTDOWN_TIMEOUT_S,
                    )
                except TimeoutError:
                    failures.append(
                        "vLLM EngineCore shutdown exceeded "
                        f"{self._ENGINE_SHUTDOWN_TIMEOUT_S:g} seconds"
                    )
                except Exception as exc:  # engine API boundary; surfaced below
                    failures.append(f"vLLM shutdown failed: {type(exc).__name__}: {exc}")
                else:
                    self.engine = None

        if failures:
            raise RuntimeError("; ".join(failures))

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
            delta = text[len(prev_text) :]
            prev_text = text
            if delta:
                yield GenDelta(delta=delta)
        if output is None:
            # The HTTP host renders ``[DONE]`` only after this iterator ends.
            # Silent exhaustion must therefore be a failed stream, not a
            # successful zero-token completion with no usage evidence.
            raise RuntimeError("vLLM stream ended without an output")
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
        result: Dict[str, Any] = {}
        collector = self.stats_collector
        if collector is not None:
            result.update(collector.live_snapshot())
        return result
