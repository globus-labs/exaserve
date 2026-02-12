"""
Ray Serve LLM inference on Aurora (Intel XPU) — split Router/Worker architecture.

Architecture:
  ┌──────────────────────────────────────────┐
  │  Router (CPU-only, N replicas)           │  ← HTTP ingress, chat template,
  │  @serve.ingress  /v1/chat/completions    │     routes via DeploymentHandle
  └──────────────┬───────────────────────────┘
                 │  DeploymentHandle (stream=True/False)
                 ▼
  ┌──────────────────────────────────────────┐
  │  ModelWorker (1 GPU each, M replicas)    │  ← vLLM engine, generation
  │  max_ongoing_requests for backpressure   │     load-balanced by Ray Serve
  └──────────────────────────────────────────┘

Scales to hundreds of nodes.  Device isolation via torch.xpu.set_device().
No patched Ray libraries needed — just NOSET=1 + global ONEAPI_DEVICE_SELECTOR.
"""

import json
import os
import time
import uuid
import socket
from typing import Optional

import ray
import torch
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from ray import serve
from ray.serve.handle import DeploymentHandle
from transformers import AutoTokenizer
from vllm import SamplingParams
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.engine.async_llm_engine import AsyncLLMEngine

# ---------------------------------------------------------------------------
# Configuration (overridable via env vars)
# ---------------------------------------------------------------------------
MODEL_ID = os.getenv("MODEL_ID", "meta-llama/Meta-Llama-3-8B-Instruct")
NUM_GPU_TILES = int(os.getenv("NUM_GPU_TILES", "12"))
NUM_ROUTERS = int(os.getenv("NUM_ROUTERS", "4"))
# How many concurrent requests each model worker will accept before
# Ray Serve back-pressures and picks a less-loaded replica.
WORKER_MAX_ONGOING = int(os.getenv("WORKER_MAX_ONGOING", "16"))

# PVC card layout: tiles 2K and 2K+1 share card K (with ZE_FLAT_DEVICE_HIERARCHY=FLAT).
# When tiles on the same card run vLLM's memory profiling (dummy forward pass + SYCL
# kernel JIT compilation) concurrently, the Level Zero driver can report inflated
# non-torch memory on some tiles (31-47 GB instead of ~3 GB), causing
# "No available memory for the cache blocks" even though 64 GB tiles have >40 GB free.
# Staggering the second tile on each card by INIT_STAGGER_SECONDS avoids this.
TILES_PER_CARD = int(os.getenv("TILES_PER_CARD", "2"))
INIT_STAGGER_SECONDS = int(os.getenv("INIT_STAGGER_SECONDS", "20"))
# Number of times to retry engine creation if it fails (e.g. transient memory spike).
ENGINE_INIT_RETRIES = int(os.getenv("ENGINE_INIT_RETRIES", "3"))

os.environ.setdefault("VLLM_TARGET_DEVICE", "xpu")


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------
def get_open_port(start_port: int, max_retries: int = 100) -> Optional[int]:
    """Finds a free port starting from `start_port`."""
    for port in range(start_port, start_port + max_retries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    return None


# ═══════════════════════════════════════════════════════════════════════════
# ModelWorker — one per GPU tile, runs vLLM engine
# ═══════════════════════════════════════════════════════════════════════════
@serve.deployment(
    name="ModelWorker",
    num_replicas=NUM_GPU_TILES,
    ray_actor_options={
        "num_gpus": 1,
        "num_cpus": 2,
    },
    max_ongoing_requests=WORKER_MAX_ONGOING,
    health_check_period_s=30,
    health_check_timeout_s=10,
)
class ModelWorker:
    """Runs a vLLM AsyncLLMEngine on one GPU tile."""

    def __init__(self):
        pid = os.getpid()

        # ---- Device isolation ------------------------------------------------
        # vLLM v1 spawns a separate EngineCore *subprocess* for GPU work.
        # torch.xpu.set_device() only affects the current process — the child
        # inherits os.environ and Level Zero reads ONEAPI_DEVICE_SELECTOR fresh
        # at library-load time.  By restricting it here BEFORE engine creation,
        # the subprocess sees only the assigned tile (device 0 inside the child
        # = the physical tile we want).
        gpu_ids = ray.get_gpu_ids()
        device_id = int(gpu_ids[0]) if gpu_ids else 0

        os.environ["ONEAPI_DEVICE_SELECTOR"] = f"level_zero:{device_id}"
        # We do NOT set ZE_AFFINITY_MASK here because it re-indexes devices,
        # causing "level_zero:{device_id}" to be out of bounds if device_id > 0.
        # Driver sets ZE_AFFINITY_MASK="" (all visible), so ONEAPI_DEVICE_SELECTOR
        # correctly picks the physical device by index.

        # NOTE: Do NOT call torch.xpu.set_device() here.
        # Initialising the XPU runtime in the parent process creates
        # Level-Zero device handles that compete with the EngineCore
        # subprocess's own fresh initialisation (vLLM forces 'spawn' when
        # inside a Ray actor), causing most children to fail
        # "assert current_platform.is_xpu()".  The env-var restriction
        # above is sufficient — the child inherits ONEAPI_DEVICE_SELECTOR
        # and sees only the assigned tile as device 0.
        print(
            f"[ModelWorker pid={pid}] GPU tile {device_id} "
            f"(ray.get_gpu_ids()={gpu_ids})  "
            f"ONEAPI_DEVICE_SELECTOR=level_zero:{device_id}",
            flush=True,
        )

        # ---- Stagger init to avoid concurrent profiling on same PVC card -----
        # PVC tiles 2K and 2K+1 share a physical card.  When both tiles run
        # vLLM's memory-profiling forward pass (and SYCL kernel JIT) at the
        # same time, the Level Zero driver can report massively inflated
        # non-torch GPU memory (31-47 GB instead of ~3 GB), starving the
        # KV cache and causing "No available memory for the cache blocks".
        # Staggering the second tile on each card eliminates the overlap.
        if TILES_PER_CARD > 1 and INIT_STAGGER_SECONDS > 0:
            position_in_card = device_id % TILES_PER_CARD
            if position_in_card > 0:
                stagger = position_in_card * INIT_STAGGER_SECONDS
                print(
                    f"[ModelWorker pid={pid}] Staggering init by {stagger}s "
                    f"(tile {device_id} shares PVC card with tile "
                    f"{device_id - position_in_card})",
                    flush=True,
                )
                time.sleep(stagger)

        # ---- vLLM async engine -----------------------------------------------
        # Assign a unique port per worker to avoid race conditions during distributed init.
        # Even with TP=1, vLLM may initialize torch.distributed which requires a unique port.
        # Use a dynamic port allocation strategy based on device_id + random offset/search.
        
        base_port = 23000 + (device_id * 100)  # Give each device a 100-port range
        port = get_open_port(base_port)
        if port is None:
            # Fallback to letting OS pick? But torch.distributed needs MASTER_PORT.
            # Just try a wider range or fail with a clear error.
            raise RuntimeError(f"Could not find a free port for distributed init (device {device_id})")

        print(f"[ModelWorker pid={pid}] Selected distributed port {port} for device {device_id}", flush=True)
        
        os.environ["MASTER_ADDR"] = "127.0.0.1"
        os.environ["MASTER_PORT"] = str(port)

        self.model_id = MODEL_ID
        engine_args = AsyncEngineArgs(
            model=MODEL_ID,
            tensor_parallel_size=1,
            gpu_memory_utilization=0.95,
            max_model_len=4096,
            enforce_eager=True,
        )
        if not hasattr(engine_args, "enable_log_requests"):
            engine_args.enable_log_requests = True

        # Retry engine creation — transient Level Zero memory spikes from
        # concurrent SYCL kernel compilation can still occur despite the
        # stagger above (e.g. if model-loading times vary).  A retry with
        # a back-off delay lets the driver's memory state settle.
        last_err: Optional[Exception] = None
        for attempt in range(1, ENGINE_INIT_RETRIES + 1):
            try:
                self.engine = AsyncLLMEngine.from_engine_args(engine_args)
                last_err = None
                break
            except Exception as exc:
                last_err = exc
                if attempt < ENGINE_INIT_RETRIES:
                    wait = 15 * attempt + device_id * 2
                    print(
                        f"[ModelWorker pid={pid}] Engine init attempt "
                        f"{attempt}/{ENGINE_INIT_RETRIES} failed: {exc}\n"
                        f"  Retrying in {wait}s …",
                        flush=True,
                    )
                    time.sleep(wait)
        if last_err is not None:
            raise last_err

        print(f"[ModelWorker pid={pid}] Engine ready on tile {device_id}", flush=True)

    async def check_health(self):
        return True

    # ------------------------------------------------------------------
    # Non-streaming: returns a complete response dict
    # ------------------------------------------------------------------
    async def generate(self, prompt: str, sampling_kwargs: dict) -> dict:
        request_id = sampling_kwargs.pop("_request_id", str(uuid.uuid4()))
        params = SamplingParams(**sampling_kwargs)

        final = None
        async for output in self.engine.generate(prompt, params, request_id):
            final = output

        if final is None:
            return {"error": "No output generated"}

        choice = final.outputs[0]
        return {
            "text": choice.text,
            "finish_reason": choice.finish_reason or "stop",
            "prompt_tokens": len(final.prompt_token_ids),
            "completion_tokens": len(choice.token_ids),
        }

    # ------------------------------------------------------------------
    # Streaming: yields delta-chunk dicts one at a time
    # ------------------------------------------------------------------
    async def generate_stream(self, prompt: str, sampling_kwargs: dict):
        request_id = sampling_kwargs.pop("_request_id", str(uuid.uuid4()))
        params = SamplingParams(**sampling_kwargs)

        prev_text = ""
        async for output in self.engine.generate(prompt, params, request_id):
            text = output.outputs[0].text
            delta = text[len(prev_text):]
            prev_text = text

            if delta:
                yield {"delta": delta, "finish_reason": None}

        # Final chunk carries finish_reason + token counts
        choice = output.outputs[0]
        yield {
            "delta": "",
            "finish_reason": choice.finish_reason or "stop",
            "prompt_tokens": len(output.prompt_token_ids),
            "completion_tokens": len(choice.token_ids),
        }


# ═══════════════════════════════════════════════════════════════════════════
# Router — CPU-only ingress, handles HTTP + chat template + routing
# ═══════════════════════════════════════════════════════════════════════════
app = FastAPI()


@serve.deployment(
    name="Router",
    num_replicas=NUM_ROUTERS,
    ray_actor_options={
        "num_cpus": 1,
        # No GPU — this is a lightweight HTTP/routing actor
    },
    # Routers can handle many concurrent requests (they just forward)
    max_ongoing_requests=200,
)
@serve.ingress(app)
class Router:
    """
    Accepts OpenAI-format HTTP requests, applies the chat template, and
    forwards to the least-loaded ModelWorker via DeploymentHandle.

    Ray Serve automatically picks the worker with the fewest in-flight
    requests (bounded by ModelWorker.max_ongoing_requests).
    """

    def __init__(self, worker_handle: DeploymentHandle):
        self.worker = worker_handle
        self.stream_handle = worker_handle.options(stream=True)
        self.model_id = MODEL_ID
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL_ID)
        print(f"[Router pid={os.getpid()}] Ready (model={MODEL_ID})", flush=True)

    # ------------------------------------------------------------------
    # GET /v1/models
    # ------------------------------------------------------------------
    @app.get("/v1/models")
    async def list_models(self):
        return JSONResponse(
            {
                "object": "list",
                "data": [
                    {
                        "id": self.model_id,
                        "object": "model",
                        "created": int(time.time()),
                        "owned_by": "aurora",
                    }
                ],
            }
        )

    # ------------------------------------------------------------------
    # POST /v1/chat/completions
    # ------------------------------------------------------------------
    @app.post("/v1/chat/completions")
    async def chat_completions(self, request: Request):
        body = await request.json()
        messages = body.get("messages", [])
        stream = body.get("stream", False)

        # -- Build sampling kwargs -----------------------------------------
        sampling_kwargs = {}
        for key in ("temperature", "top_p"):
            if key in body:
                sampling_kwargs[key] = float(body[key])
        for key in ("max_tokens", "min_tokens"):
            if key in body:
                sampling_kwargs[key] = int(body[key])
        if "stop" in body:
            sampling_kwargs["stop"] = body["stop"]
        if body.get("ignore_eos"):
            sampling_kwargs["ignore_eos"] = True
        sampling_kwargs.setdefault("temperature", 0.7)
        sampling_kwargs.setdefault("max_tokens", 1024)

        # -- Apply chat template (CPU work, done in the router) ------------
        prompt = self.tokenizer.apply_chat_template(
            messages, tokenize=False, add_generation_prompt=True
        )

        request_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        sampling_kwargs["_request_id"] = request_id

        if stream:
            return StreamingResponse(
                self._stream(request_id, prompt, sampling_kwargs),
                media_type="text/event-stream",
            )
        else:
            return await self._non_stream(request_id, prompt, sampling_kwargs)

    # ------------------------------------------------------------------
    # Non-streaming path
    # ------------------------------------------------------------------
    async def _non_stream(self, request_id, prompt, sampling_kwargs):
        result = await self.worker.generate.remote(prompt, sampling_kwargs)

        if "error" in result:
            return JSONResponse({"error": result["error"]}, status_code=500)

        return JSONResponse(
            {
                "id": request_id,
                "object": "chat.completion",
                "created": int(time.time()),
                "model": self.model_id,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": result["text"]},
                        "finish_reason": result["finish_reason"],
                    }
                ],
                "usage": {
                    "prompt_tokens": result["prompt_tokens"],
                    "completion_tokens": result["completion_tokens"],
                    "total_tokens": result["prompt_tokens"] + result["completion_tokens"],
                },
            }
        )

    # ------------------------------------------------------------------
    # Streaming path (SSE)
    # ------------------------------------------------------------------
    async def _stream(self, request_id, prompt, sampling_kwargs):
        created = int(time.time())
        gen = self.stream_handle.generate_stream.remote(prompt, sampling_kwargs)

        usage = None
        async for chunk in gen:
            delta = chunk.get("delta", "")
            finish_reason = chunk.get("finish_reason")

            # Accumulate usage from the final chunk
            if "prompt_tokens" in chunk:
                usage = {
                    "prompt_tokens": chunk["prompt_tokens"],
                    "completion_tokens": chunk["completion_tokens"],
                    "total_tokens": chunk["prompt_tokens"] + chunk["completion_tokens"],
                }

            if delta:
                sse = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": self.model_id,
                    "choices": [
                        {"index": 0, "delta": {"content": delta}, "finish_reason": None}
                    ],
                }
                yield f"data: {json.dumps(sse)}\n\n"

            if finish_reason is not None:
                final = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": self.model_id,
                    "choices": [
                        {"index": 0, "delta": {}, "finish_reason": finish_reason}
                    ],
                }
                if usage:
                    final["usage"] = usage
                yield f"data: {json.dumps(final)}\n\n"

        yield "data: [DONE]\n\n"


# ---------------------------------------------------------------------------
# Entry point — wire the two deployments together
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    ray.init(address="auto", namespace="serve")

    # ---- Auto-detect cluster size ----------------------------------------
    # If NUM_GPU_TILES / NUM_ROUTERS env vars are set, honour them.
    # Otherwise, scale to the entire cluster.
    resources = ray.cluster_resources()
    total_gpus = int(resources.get("GPU", NUM_GPU_TILES))
    num_workers = int(os.getenv("NUM_GPU_TILES", str(total_gpus)))
    num_routers = int(os.getenv("NUM_ROUTERS", str(max(2, num_workers // 3))))

    print(
        f"[AuroraServe] Deploying {MODEL_ID}\n"
        f"  Cluster GPUs  : {total_gpus}\n"
        f"  Model workers : {num_workers} (1 GPU each, "
        f"max_ongoing={WORKER_MAX_ONGOING})\n"
        f"  Routers       : {num_routers} (CPU-only)",
        flush=True,
    )

    # Build the deployment graph: Router → ModelWorker
    worker = ModelWorker.options(num_replicas=num_workers).bind()
    router = Router.options(num_replicas=num_routers).bind(worker)

    serve.run(router, route_prefix="/")

    print(
        "[AuroraServe] Deployment active. "
        "Service available at http://localhost:8000/v1",
        flush=True,
    )

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("[AuroraServe] Shutting down...")
