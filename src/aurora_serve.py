"""
Ray Serve LLM inference on Aurora (Intel XPU).

Each VLLMWorker replica handles both HTTP ingress (OpenAI-compatible API) and
vLLM inference on a single GPU tile, eliminating the separate Router layer.

For multi-model serving, each model is deployed as an independent Ray Serve
application at its own route_prefix (e.g. /llama-3-8b/v1).

No patched Ray libraries needed — just NOSET=1 + global ONEAPI_DEVICE_SELECTOR.
"""

import asyncio
import inspect
import json
import os
import time
import uuid
import socket
import argparse
from typing import Optional, List, Dict

import ray
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from ray import serve
from ray.serve.config import HTTPOptions, ProxyLocation
from vllm import SamplingParams
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.engine.async_llm_engine import AsyncLLMEngine

from schemas import ModelConfig, DeploymentConfig, load_deployment_config
from model_paths import get_model_route_name
from model_staging import print_red, resolve_model_paths


def get_hsn_ip():
    """
    Connects to a dummy internal IP to force the OS to pick the
    default route interface (High Speed Network).
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return socket.gethostbyname(socket.gethostname())


def get_open_port(
    start_port: int,
    max_retries: int = 100,
    bind_host: str = "127.0.0.1",
) -> Optional[int]:
    """Find a free port starting from `start_port`."""
    for port in range(start_port, start_port + max_retries):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind((bind_host, port))
                return port
            except OSError:
                continue
    return None


def async_engine_arg_supported(arg_name: str) -> bool:
    """Best-effort compatibility gate for installed vLLM builds."""
    try:
        signature = inspect.signature(AsyncEngineArgs.__init__)
    except (TypeError, ValueError):
        return hasattr(AsyncEngineArgs, arg_name)

    if any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in signature.parameters.values()
    ):
        return True
    return arg_name in signature.parameters


def default_num_replicas(
    model_config: ModelConfig,
    total_gpus: int,
    config: DeploymentConfig,
) -> int:
    """Compute the default replica count for a model."""
    if model_config.pipeline_parallel_size > 1:
        return max(1, config.num_nodes // model_config.pipeline_parallel_size)
    return max(1, total_gpus // model_config.tensor_parallel_size)


# ═══════════════════════════════════════════════════════════════════════════
# VLLMWorker — HTTP ingress + vLLM engine in one deployment
# ═══════════════════════════════════════════════════════════════════════════
app = FastAPI()


@serve.deployment
@serve.ingress(app)
class VLLMWorker:
    """
    Single Ray Serve deployment that handles OpenAI-format HTTP requests and
    runs vLLM inference on one GPU tile.

    Set null_compute=True to skip the vLLM engine entirely and simulate
    inference with a configurable sleep (useful for isolating Ray/routing
    overhead from actual inference cost).
    """

    def __init__(
        self,
        model_id: str,
        local_model_path: str = None,
        null_compute: bool = False,
        tensor_parallel_size: int = 1,
        pipeline_parallel_size: int = 1,
        gpu_memory_utilization: float = 0.9,
        max_model_len: int = 4096,
        enforce_eager: bool = True,
    ):
        init_start = time.time()
        pid = os.getpid()
        self.model_id = model_id
        self.null_compute = null_compute

        gpu_ids = ray.get_gpu_ids()
        device_id = int(gpu_ids[0]) if gpu_ids else 0

        if null_compute:
            self.latency = float(os.environ.get("AURORA_NULL_COMPUTE_LATENCY", "1.0"))
            print(
                f"[VLLMWorker pid={pid}] NullCompute mode on tile {device_id} "
                f"(latency={self.latency:.2f}s, no vLLM engine)",
                flush=True,
            )
            print_red(f"[VLLMWorker pid={pid}] ★ INIT TOTAL: {time.time() - init_start:.2f}s ★")
            return

        # ---- Device isolation ------------------------------------------------
        os.environ["ZE_AFFINITY_MASK"] = str(device_id)
        os.environ["ONEAPI_DEVICE_SELECTOR"] = "level_zero:0"
        print(
            f"[VLLMWorker pid={pid}] GPU tile {device_id} "
            f"ZE_AFFINITY_MASK={device_id} ONEAPI_DEVICE_SELECTOR=level_zero:0",
            flush=True,
        )

        # ---- Distributed init port -------------------------------------------
        master_addr = "127.0.0.1"
        bind_host = "127.0.0.1"
        if pipeline_parallel_size > 1:
            if not async_engine_arg_supported("pipeline_parallel_size"):
                raise RuntimeError(
                    "Installed vLLM build does not expose pipeline_parallel_size on "
                    "AsyncEngineArgs. Validate the Aurora runtime before using PP."
                )
            master_addr = get_hsn_ip()
            bind_host = "0.0.0.0"

        port = get_open_port(23000 + device_id * 100, bind_host=bind_host)
        if port is None:
            raise RuntimeError(f"No free port for distributed init (device {device_id})")
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = str(port)
        print(
            f"[VLLMWorker pid={pid}] Using distributed master {master_addr}:{port} "
            f"(PP={pipeline_parallel_size})",
            flush=True,
        )

        # ---- vLLM async engine -----------------------------------------------
        model_path = local_model_path or model_id
        engine_kwargs = dict(
            model=model_path,
            tensor_parallel_size=tensor_parallel_size,
            gpu_memory_utilization=gpu_memory_utilization,
            max_model_len=max_model_len,
            enforce_eager=enforce_eager,
        )
        if pipeline_parallel_size > 1:
            engine_kwargs["pipeline_parallel_size"] = pipeline_parallel_size
        engine_args = AsyncEngineArgs(**engine_kwargs)
        if not hasattr(engine_args, "enable_log_requests"):
            engine_args.enable_log_requests = True

        print(f"[VLLMWorker pid={pid}] Creating vLLM engine for {model_id}...", flush=True)
        engine_start = time.time()
        self.engine = AsyncLLMEngine.from_engine_args(engine_args)
        print_red(f"[VLLMWorker pid={pid}] Engine creation: {time.time() - engine_start:.2f}s")
        print_red(f"[VLLMWorker pid={pid}] ★ INIT TOTAL: {time.time() - init_start:.2f}s ★")

    # ---- HTTP endpoints ------------------------------------------------------

    @app.get("/health")
    async def health_check(self):
        return JSONResponse({"status": "healthy", "model": self.model_id})

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

    @app.post("/v1/chat/completions")
    async def chat_completions(self, request: Request):
        body = await request.json()
        messages = body.get("messages", [])
        stream = body.get("stream", False)

        sampling_kwargs: dict = {}
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

        if self.null_compute:
            # No tokenizer available; flatten messages to a plain string.
            prompt = " ".join(m.get("content", "") for m in messages)
        else:
            tokenizer = self.engine.get_tokenizer()
            prompt = tokenizer.apply_chat_template(
                messages, tokenize=False, add_generation_prompt=True
            )

        request_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        sampling_kwargs["_request_id"] = request_id

        if stream:
            return StreamingResponse(
                self._stream(request_id, prompt, sampling_kwargs),
                media_type="text/event-stream",
            )
        return await self._non_stream(request_id, prompt, sampling_kwargs)

    # ---- Internal helpers ----------------------------------------------------

    async def _non_stream(self, request_id: str, prompt: str, sampling_kwargs: dict):
        result = (
            await self._null_generate(prompt, sampling_kwargs)
            if self.null_compute
            else await self._generate(prompt, sampling_kwargs)
        )

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

    async def _stream(self, request_id: str, prompt: str, sampling_kwargs: dict):
        created = int(time.time())
        gen = (
            self._null_generate_stream(prompt, sampling_kwargs)
            if self.null_compute
            else self._generate_stream(prompt, sampling_kwargs)
        )

        usage = None
        async for chunk in gen:
            delta = chunk.get("delta", "")
            finish_reason = chunk.get("finish_reason")

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
                    "choices": [{"index": 0, "delta": {"content": delta}, "finish_reason": None}],
                }
                yield f"data: {json.dumps(sse)}\n\n"

            if finish_reason is not None:
                final = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": self.model_id,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": finish_reason}],
                }
                if usage:
                    final["usage"] = usage
                yield f"data: {json.dumps(final)}\n\n"

        yield "data: [DONE]\n\n"

    async def _generate(self, prompt: str, sampling_kwargs: dict) -> dict:
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

    async def _generate_stream(self, prompt: str, sampling_kwargs: dict):
        request_id = sampling_kwargs.pop("_request_id", str(uuid.uuid4()))
        params = SamplingParams(**sampling_kwargs)

        prev_text = ""
        async for output in self.engine.generate(prompt, params, request_id):
            text = output.outputs[0].text
            delta = text[len(prev_text):]
            prev_text = text
            if delta:
                yield {"delta": delta, "finish_reason": None}

        choice = output.outputs[0]
        yield {
            "delta": "",
            "finish_reason": choice.finish_reason or "stop",
            "prompt_tokens": len(output.prompt_token_ids),
            "completion_tokens": len(choice.token_ids),
        }

    async def _null_generate(self, prompt: str, sampling_kwargs: dict) -> dict:
        await asyncio.sleep(self.latency)
        max_tokens = int(sampling_kwargs.get("max_tokens", 10))
        # No tokenizer in null_compute mode; word count is the same approximation
        # used by the chat handler when it flattens messages into a plain string.
        prompt_tokens = len(prompt.split())
        return {
            "text": "null " * max_tokens,
            "finish_reason": "stop",
            "prompt_tokens": prompt_tokens,
            "completion_tokens": max_tokens,
        }

    async def _null_generate_stream(self, prompt: str, sampling_kwargs: dict):
        await asyncio.sleep(self.latency)
        max_tokens = int(sampling_kwargs.get("max_tokens", 10))
        prompt_tokens = len(prompt.split())
        yield {
            "delta": "null " * max_tokens,
            "finish_reason": "stop",
            "prompt_tokens": prompt_tokens,
            "completion_tokens": max_tokens,
        }


# ---------------------------------------------------------------------------
# Deployment helpers
# ---------------------------------------------------------------------------
def deploy_model(
    model_config: ModelConfig,
    model_path_map: Dict[str, str],
    total_gpus: int,
    config: DeploymentConfig,
    model_index: int = 0,
) -> tuple:
    """
    Build a bound VLLMWorker deployment for one model.

    Ray Serve options (num_gpus, replicas, max_ongoing_requests, …) are passed
    via .options() so no factory/class-creation indirection is needed.

    Returns:
        (deployment, model_id)
    """
    model_id = model_config.model_id
    local_path = model_path_map.get(model_id, model_id)
    null_compute = os.environ.get("AURORA_NULL_COMPUTE", "0") == "1"

    num_replicas = model_config.num_replicas or default_num_replicas(
        model_config, total_gpus, config
    )

    safe_name = get_model_route_name(model_id)

    print(
        f"[AuroraServe] Configuring VLLMWorker for {model_id}\n"
        f"  Replicas    : {num_replicas} "
        f"(TP={model_config.tensor_parallel_size}, PP={model_config.pipeline_parallel_size})\n"
        f"  NullCompute : {null_compute}\n"
        f"  Local path  : {local_path}",
        flush=True,
    )
    if null_compute:
        latency = float(os.environ.get("AURORA_NULL_COMPUTE_LATENCY", "1.0"))
        print(
            f"[AuroraServe] NULL-COMPUTE mode — vLLM replaced by sleep({latency:.2f}s)",
            flush=True,
        )

    # Replicas per node = tiles per node / TP size. Ray's GPU resource scheduling
    # already enforces this implicitly, but being explicit avoids stacking replicas
    # onto a subset of nodes when cluster membership fluctuates.
    if model_config.pipeline_parallel_size > 1:
        replicas_per_node = 1
    else:
        replicas_per_node = max(1, config.num_gpus_per_node // model_config.tensor_parallel_size)

    deployment = VLLMWorker.options(
        name=f"VLLMWorker-{safe_name}",
        num_replicas=num_replicas,
        max_replicas_per_node=replicas_per_node,
        ray_actor_options={
            "num_gpus": model_config.tensor_parallel_size,
            "num_cpus": model_config.num_cpus_per_replica,
        },
        max_ongoing_requests=config.worker_max_ongoing,
        health_check_period_s=30,
        health_check_timeout_s=10,
    ).bind(
        model_id=model_id,
        local_model_path=local_path,
        null_compute=null_compute,
        tensor_parallel_size=model_config.tensor_parallel_size,
        pipeline_parallel_size=model_config.pipeline_parallel_size,
        gpu_memory_utilization=model_config.gpu_memory_utilization,
        max_model_len=model_config.max_model_len,
        enforce_eager=model_config.enforce_eager,
    )

    return deployment, model_id


def deploy_multi_model(
    config: DeploymentConfig,
    model_path_map: Dict[str, str],
    total_gpus: int,
) -> None:
    """
    Deploy each model as an independent Ray Serve app at its own route_prefix.

    For example, with models ["meta-llama/Meta-Llama-3-8B-Instruct",
    "mistralai/Mistral-7B-Instruct-v0.3"] the service is available at:
        http://localhost:8000/meta-llama--Meta-Llama-3-8B-Instruct/v1/...
        http://localhost:8000/mistralai--Mistral-7B-Instruct-v0-3/v1/...
    """
    print(
        f"[AuroraServe] Deploying {len(config.model_configs)} models with per-model route_prefix",
        flush=True,
    )
    for idx, model_config in enumerate(config.model_configs):
        print(
            f"\n[AuroraServe] ═══ Deploying model {idx + 1}/{len(config.model_configs)} ═══",
            flush=True,
        )
        deployment, model_id = deploy_model(model_config, model_path_map, total_gpus, config, idx)
        safe_name = get_model_route_name(model_id)
        route_prefix = f"/{safe_name}"
        serve.run(deployment, name=safe_name, route_prefix=route_prefix)
        print(
            f"[AuroraServe] ✓ {model_id} → http://localhost:8000{route_prefix}/v1",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    overall_start = time.time()

    parser = argparse.ArgumentParser(description="Ray Serve LLM inference on Aurora")
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="Path to deployment config YAML (default: config.yaml). "
             "Can be full experiment config (model_deployment_config key) or deployment-only.",
    )
    args = parser.parse_args()

    config_path = os.path.abspath(args.config)
    if not os.path.isfile(config_path):
        raise SystemExit(f"[AuroraServe] Config file not found: {config_path}")

    # ---- Load configuration -------------------------------------------------
    config = load_deployment_config(config_path)
    print(f"[AuroraServe] Loaded config from {config_path}: {config.deployment_name}", flush=True)
    print(f"[AuroraServe] Models: {len(config.model_configs)}", flush=True)
    for cfg in config.model_configs:
        print(
            f"  - {cfg.model_id} "
            f"(size={cfg.size}B, TP={cfg.tensor_parallel_size}, PP={cfg.pipeline_parallel_size})",
            flush=True,
        )

    # ---- Stage 1: Initialize Ray cluster ------------------------------------
    stage_start = time.time()
    print("[AuroraServe] Stage 1: Initializing Ray cluster...", flush=True)
    ray.init(address="auto", namespace="serve", include_dashboard=False)
    serve.start(
        http_options=HTTPOptions(
            host="0.0.0.0",  # Bind to all interfaces so HSN hostnames are reachable
            location=ProxyLocation.EveryNode,
            port=8000,
        )
    )
    print("[AuroraServe] HTTP proxy location: EveryNode, host=0.0.0.0, port=8000", flush=True)
    print_red(f"[AuroraServe] ✓ Stage 1 ray.init() completed in {time.time() - stage_start:.2f}s")

    # ---- Detect cluster resources -------------------------------------------
    resources = ray.cluster_resources()
    default_gpus = config.num_gpus_per_node * config.num_nodes
    total_gpus = int(resources.get("GPU", default_gpus))
    print(f"[AuroraServe] Detected {total_gpus} GPUs in cluster", flush=True)

    # ---- Stage 2: Resolve staged local models -------------------------------
    null_compute = os.environ.get("AURORA_NULL_COMPUTE", "0") == "1"
    if null_compute:
        print(
            "[AuroraServe] Stage 2: NULL-COMPUTE mode — model staging skipped",
            flush=True,
        )
        model_path_map = {cfg.model_id: cfg.model_id for cfg in config.model_configs}
    else:
        stage_start = time.time()
        print(
            f"[AuroraServe] Stage 2: Resolving staged models from {config.local_stage_path}...",
            flush=True,
        )
        model_path_map = resolve_model_paths(
            config.model_configs,
            config.local_stage_path,
            require_complete=True,
        )
        print_red(
            f"[AuroraServe] ✓ Stage 2 local model resolution completed in "
            f"{time.time() - stage_start:.2f}s"
        )

    # ---- Stage 3: Deploy model services -------------------------------------
    stage_start = time.time()
    print("[AuroraServe] Stage 3: Deploying model services to Ray Serve...", flush=True)

    if len(config.model_configs) == 1:
        primary_config = config.model_configs[0]
        deployment, model_id = deploy_model(primary_config, model_path_map, total_gpus, config)
        serve_start = time.time()
        serve.run(deployment, route_prefix="/")
        print_red(f"[AuroraServe] serve.run() call: {time.time() - serve_start:.2f}s")
        print(f"[AuroraServe] Service available at http://localhost:8000/v1 (model: {model_id})", flush=True)
    else:
        deploy_multi_model(config, model_path_map, total_gpus)

    print_red(
        f"[AuroraServe] ✓ Stage 3 completed in {time.time() - stage_start:.2f}s"
    )

    # ---- All stages complete ------------------------------------------------
    print_red(
        f"[AuroraServe] ✓✓✓ CLUSTER FULLY READY ✓✓✓ Total time: {time.time() - overall_start:.2f}s"
    )

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("[AuroraServe] Shutting down...")
