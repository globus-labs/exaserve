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
from typing import Optional, List, Dict

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

# Import our configuration modules
from model_config import ModelConfig, DeploymentConfig
from model_staging import stage_models, get_local_model_path, print_red
import aurora

os.environ.setdefault("VLLM_TARGET_DEVICE", "xpu")


def get_hsn_ip():
    """
    Connects to a dummy internal IP to force the OS to pick the 
    default route interface (High Speed Network).
    """
    try:
        # We don't actually send data, just open a socket to determine routing.
        # 10.255.255.255 is a safe dummy target for internal routing.
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("10.255.255.255", 1))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception as e:
        # Fallback for login nodes or single-node testing
        return socket.gethostbyname(socket.gethostname())
    # return f"{socket.gethostname()}.hsn.cm.aurora.alcf.anl.gov"

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
def create_model_worker_class(config: DeploymentConfig):
    """Factory function to create ModelWorker class with embedded config."""
    
    class ModelWorker:
        """Runs a vLLM AsyncLLMEngine on one GPU tile."""

        def __init__(self, model_id: str = None, local_model_path: str = None):
            worker_init_start = time.time()
            pid = os.getpid()
            
            # Use local model path if provided, otherwise fall back to HuggingFace
            self.model_id = model_id
            self.model_path = local_model_path or model_id

            print(f"[ModelWorker pid={pid}] Starting initialization for {model_id}...", flush=True)

            # ---- Device isolation ------------------------------------------------
            gpu_ids = ray.get_gpu_ids()
            device_id = int(gpu_ids[0]) if gpu_ids else 0
            os.environ["ZE_AFFINITY_MASK"] = str(device_id)
            os.environ["ONEAPI_DEVICE_SELECTOR"] = "level_zero:0"

            print(
                f"[ModelWorker pid={pid}] GPU tile {device_id} "
                f"(ray.get_gpu_ids()={gpu_ids})  "
                f"ZE_AFFINITY_MASK={device_id}  "
                f"ONEAPI_DEVICE_SELECTOR=level_zero:0",
                flush=True,
            )

            # ---- Stagger init to avoid concurrent profiling on same PVC card -----
            if config.tiles_per_card > 1 and config.init_stagger_seconds > 0:
                position_in_card = device_id % config.tiles_per_card
                if position_in_card > 0:
                    stagger = position_in_card * config.init_stagger_seconds
                    print(
                        f"[ModelWorker pid={pid}] Staggering init by {stagger}s "
                        f"(tile {device_id} shares PVC card with tile "
                        f"{device_id - position_in_card})",
                        flush=True,
                    )
                    time.sleep(stagger)

            # ---- vLLM async engine -----------------------------------------------
            engine_setup_start = time.time()
            
            base_port = 23000 + (device_id * 100)
            port = get_open_port(base_port)
            if port is None:
                raise RuntimeError(f"Could not find a free port for distributed init (device {device_id})")

            print(f"[ModelWorker pid={pid}] Using distributed port {port}", flush=True)
            
            os.environ["MASTER_ADDR"] = "127.0.0.1"
            os.environ["MASTER_PORT"] = str(port)

            engine_args = AsyncEngineArgs(
                model=self.model_path,
                tensor_parallel_size=1,
                gpu_memory_utilization=0.9,
                max_model_len=4096,
                enforce_eager=True,
            )
            if not hasattr(engine_args, "enable_log_requests"):
                engine_args.enable_log_requests = True

            # Retry engine creation
            last_err: Optional[Exception] = None
            for attempt in range(1, config.engine_init_retries + 1):
                try:
                    print(f"[ModelWorker pid={pid}] Creating vLLM engine (attempt {attempt}/{config.engine_init_retries})...", flush=True)
                    engine_create_start = time.time()
                    self.engine = AsyncLLMEngine.from_engine_args(engine_args)
                    engine_create_elapsed = time.time() - engine_create_start
                    print_red(f"[ModelWorker pid={pid}] vLLM engine creation: {engine_create_elapsed:.2f}s")
                    last_err = None
                    break
                except Exception as exc:
                    last_err = exc
                    if attempt < config.engine_init_retries:
                        wait = 15 * attempt + device_id * 2
                        print(
                            f"[ModelWorker pid={pid}] Engine init attempt "
                            f"{attempt}/{config.engine_init_retries} failed: {exc}\n"
                            f"  Retrying in {wait}s …",
                            flush=True,
                        )
                        time.sleep(wait)
            if last_err is not None:
                raise last_err

            engine_setup_elapsed = time.time() - engine_setup_start
            worker_total_elapsed = time.time() - worker_init_start
            print(f"[ModelWorker pid={pid}] Engine ready on tile {device_id}", flush=True)
            print_red(f"[ModelWorker pid={pid}] Engine setup total: {engine_setup_elapsed:.2f}s")
            print_red(f"[ModelWorker pid={pid}] ★ WORKER INIT TOTAL: {worker_total_elapsed:.2f}s ★")

        async def check_health(self):
            return True

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
    
    return ModelWorker


# ═══════════════════════════════════════════════════════════════════════════
# Router — CPU-only ingress, handles HTTP + chat template + routing
# ═══════════════════════════════════════════════════════════════════════════
app = FastAPI()


def create_router_class(model_configs: List[ModelConfig] = None):
    """Factory function to create Router class.
    
    Args:
        model_configs: List of ModelConfig objects for multi-model routing.
                      If provided, creates a multiplexing router.
    """
    
    class Router:
        """
        Accepts OpenAI-format HTTP requests, applies the chat template, and
        forwards to the least-loaded ModelWorker via DeploymentHandle.

        Ray Serve automatically picks the worker with the fewest in-flight
        requests (bounded by ModelWorker.max_ongoing_requests).
        """

        def __init__(self, worker_handle: DeploymentHandle, model_id: str, local_model_path: str = None):
            router_init_start = time.time()
            self.worker = worker_handle
            self.stream_handle = worker_handle.options(stream=True)
            self.model_id = model_id
            # Use local path for tokenizer if available
            tokenizer_path = local_model_path or model_id
            
            tokenizer_load_start = time.time()
            self.tokenizer = AutoTokenizer.from_pretrained(tokenizer_path)
            tokenizer_load_elapsed = time.time() - tokenizer_load_start
            
            router_total_elapsed = time.time() - router_init_start
            print(f"[Router pid={os.getpid()}] Ready (model={self.model_id})", flush=True)
            print_red(f"[Router pid={os.getpid()}] Tokenizer load: {tokenizer_load_elapsed:.2f}s")
            print_red(f"[Router pid={os.getpid()}] ★ ROUTER INIT TOTAL: {router_total_elapsed:.2f}s ★")

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

            # Build sampling kwargs
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

            # Apply chat template
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
    
    return Router


def create_model_multiplexer_class():
    """Factory function to create a multiplexing router for multi-model serving."""
    
    class ModelMultiplexer:
        """
        Top-level router that directs requests to the appropriate model-specific
        Router based on the 'model' field in the request.
        """
        
        def __init__(self, model_router_map: Dict[str, DeploymentHandle]):
            """
            Initialize the multiplexer with model-to-router mapping.
            
            Args:
                model_router_map: Dict mapping model_id to Router DeploymentHandle
            """
            self.model_routers = model_router_map
            self.available_models = list(model_router_map.keys())
            print(
                f"[ModelMultiplexer pid={os.getpid()}] Initialized with {len(self.available_models)} models: "
                f"{', '.join(self.available_models)}",
                flush=True
            )
        
        @app.get("/v1/models")
        async def list_models(self):
            """List all available models across all deployments."""
            return JSONResponse(
                {
                    "object": "list",
                    "data": [
                        {
                            "id": model_id,
                            "object": "model",
                            "created": int(time.time()),
                            "owned_by": "aurora",
                        }
                        for model_id in self.available_models
                    ],
                }
            )
        
        @app.post("/v1/chat/completions")
        async def chat_completions(self, request: Request):
            """Route chat completion requests to the appropriate model."""
            body = await request.json()
            requested_model = body.get("model")
            
            # If no model specified, use the first available one
            if not requested_model:
                requested_model = self.available_models[0]
                print(f"[ModelMultiplexer] No model specified, using default: {requested_model}", flush=True)
            
            # Find the router for this model
            router_handle = self.model_routers.get(requested_model)
            if router_handle is None:
                return JSONResponse(
                    {
                        "error": {
                            "message": f"Model '{requested_model}' not found. Available models: {', '.join(self.available_models)}",
                            "type": "invalid_request_error",
                            "code": "model_not_found",
                        }
                    },
                    status_code=404,
                )
            
            # Forward the request to the appropriate model router
            # Use .remote() to call the method on the remote deployment
            stream = body.get("stream", False)
            
            if stream:
                # For streaming, we need to forward to the router and return its stream
                return await router_handle.chat_completions.remote(request)
            else:
                # For non-streaming, forward and return the response
                return await router_handle.chat_completions.remote(request)
    
    return ModelMultiplexer


# ---------------------------------------------------------------------------
# Entry point — wire the two deployments together
# ---------------------------------------------------------------------------
def deploy_model(model_config: ModelConfig, model_path_map: Dict[str, str], 
                 total_gpus: int, config: DeploymentConfig, model_index: int = 0) -> tuple:
    """
    Deploy a single model with Router and ModelWorker.
    
    Args:
        model_config: ModelConfig for this model
        model_path_map: Mapping of model_id to local paths
        total_gpus: Total available GPUs in cluster
        config: DeploymentConfig with system settings
        model_index: Index to create unique deployment names
        
    Returns:
        tuple: (router_deployment, model_id) for tracking
    """
    deploy_start = time.time()
    model_id = model_config.model_id
    local_path = model_path_map.get(model_id, model_id)
    
    # Determine replica counts
    if model_config.num_replicas:
        num_workers = model_config.num_replicas
    else:
        # Auto-scale based on tensor parallel size
        num_workers = max(1, total_gpus // model_config.tensor_parallel_size)
    
    # For multi-model, use fewer routers per model
    num_routers = max(1, config.num_routers // len(config.model_configs)) if len(config.model_configs) > 1 else config.num_routers
    
    print(
        f"[AuroraServe] Configuring deployment for {model_id}\n"
        f"  Model workers : {num_workers} (TP={model_config.tensor_parallel_size}, "
        f"max_ongoing={config.worker_max_ongoing})\n"
        f"  Routers       : {num_routers} (CPU-only)\n"
        f"  Local path    : {local_path}",
        flush=True,
    )
    
    # Create deployment classes with config
    print(f"[AuroraServe] Creating deployment classes for model {model_index}...", flush=True)
    class_create_start = time.time()
    ModelWorker = create_model_worker_class(config)
    Router = create_router_class()
    class_create_elapsed = time.time() - class_create_start
    print_red(f"[AuroraServe] Deployment class creation: {class_create_elapsed:.2f}s")
    
    # Build Ray Serve deployments with unique names
    print(f"[AuroraServe] Building Ray Serve deployment graph for model {model_index}...", flush=True)
    graph_build_start = time.time()
    
    # Create unique deployment names for multi-model support
    safe_model_name = model_id.replace("/", "--").replace(".", "-")
    worker_name = f"ModelWorker-{safe_model_name}"
    router_name = f"Router-{safe_model_name}"
    
    worker_deployment = serve.deployment(
        name=worker_name,
        ray_actor_options={
            "num_gpus": model_config.tensor_parallel_size,
            "num_cpus": 2,
        },
        max_ongoing_requests=config.worker_max_ongoing,
        health_check_period_s=30,
        health_check_timeout_s=10,
    )(ModelWorker)
    
    router_deployment = serve.deployment(
        name=router_name,
        ray_actor_options={
            "num_cpus": 1,
        },
        max_ongoing_requests=200,
    )(serve.ingress(app)(Router))
    
    worker = worker_deployment.options(num_replicas=num_workers).bind(
        model_id=model_id, 
        local_model_path=local_path
    )
    
    router = router_deployment.options(num_replicas=num_routers).bind(
        worker, 
        model_id=model_id,
        local_model_path=local_path
    )
    
    graph_build_elapsed = time.time() - graph_build_start
    deploy_elapsed = time.time() - deploy_start
    print_red(f"[AuroraServe] Deployment graph build: {graph_build_elapsed:.2f}s")
    print_red(f"[AuroraServe] Deploy function total: {deploy_elapsed:.2f}s")
    
    return router, model_id


def deploy_multi_model(config: DeploymentConfig, model_path_map: Dict[str, str], 
                       total_gpus: int) -> serve.deployment:
    """
    Deploy multiple models with a multiplexer router.
    
    Args:
        config: DeploymentConfig with multiple model configurations
        model_path_map: Mapping of model_id to local paths
        total_gpus: Total available GPUs in cluster
        
    Returns:
        Multiplexer deployment bound with all model routers
    """
    print(f"[AuroraServe] Deploying {len(config.model_configs)} models with multiplexer", flush=True)
    
    # Deploy each model and collect their router handles
    model_routers = {}
    for idx, model_config in enumerate(config.model_configs):
        print(f"\n[AuroraServe] ═══ Deploying model {idx + 1}/{len(config.model_configs)} ═══", flush=True)
        router, model_id = deploy_model(model_config, model_path_map, total_gpus, config, model_index=idx)
        model_routers[model_id] = router
    
    # Create the multiplexer
    print("\n[AuroraServe] Creating model multiplexer...", flush=True)
    ModelMultiplexer = create_model_multiplexer_class()
    
    multiplexer_deployment = serve.deployment(
        name="ModelMultiplexer",
        ray_actor_options={
            "num_cpus": 1,
        },
        max_ongoing_requests=500,
    )(serve.ingress(app)(ModelMultiplexer))
    
    # Bind the multiplexer with all model routers
    multiplexer = multiplexer_deployment.options(num_replicas=config.num_routers).bind(
        model_router_map=model_routers
    )
    
    print("[AuroraServe] ✓ Multi-model deployment graph complete", flush=True)
    return multiplexer


if __name__ == "__main__":
    overall_start = time.time()
    
    # ---- Load Configuration -------------------------------------
    config = aurora.get_deployment_config("default")
    print(f"[AuroraServe] Loaded config: {config.deployment_name}", flush=True)
    print(f"[AuroraServe] Models: {len(config.model_configs)}", flush=True)
    for cfg in config.model_configs:
        print(f"  - {cfg.model_id} (size={cfg.size}B, TP={cfg.tensor_parallel_size})", flush=True)
  
    # ---- Timed Stage 1: Initialize Ray Cluster ---------------------------------
    stage_start = time.time()
    print("[AuroraServe] Stage 1: Initializing Ray cluster...", flush=True)
    
 
    ray_init_start = time.time()
    ray.init(address="auto", namespace="serve", include_dashboard=False)
    ray_init_elapsed = time.time() - ray_init_start
    
    stage_elapsed = time.time() - stage_start
    print_red(f"[AuroraServe] ✓ Stage 1 ray.init() completed in {stage_elapsed:.2f}s")

    # ---- Detect Cluster Resources ------------------------------
    resources = ray.cluster_resources()
    total_gpus = int(resources.get("GPU", config.num_gpu_tiles))
    print(f"[AuroraServe] Detected {total_gpus} GPUs in cluster", flush=True)

    # ---- Timed Stage 2: Stage Models (Download/Verify) -------------------------
    stage_start = time.time()
    print(f"[AuroraServe] Stage 2: Staging models to {config.model_storage_path}...", flush=True)
    model_path_map = stage_models(config.model_configs, config.model_storage_path)
    stage_elapsed = time.time() - stage_start
    print_red(f"[AuroraServe] ✓ Stage 2 stage_models() completed in {stage_elapsed:.2f}s")

    # ---- Timed Stage 3: Deploy Model Services ----------------------------------
    stage_start = time.time()
    print("[AuroraServe] Stage 3: Deploying model services to Ray Serve...", flush=True)
    
    # Deploy models based on configuration
    if len(config.model_configs) == 1:
        # Single model deployment - use direct routing
        print("[AuroraServe] Single model detected, deploying with direct routing", flush=True)
        primary_config = config.model_configs[0]
        deployment, model_id = deploy_model(primary_config, model_path_map, total_gpus, config)
        print(f"[AuroraServe] Service will be available for model: {model_id}", flush=True)
    else:
        # Multi-model deployment - use multiplexer
        print(f"[AuroraServe] {len(config.model_configs)} models detected, deploying with multiplexer", flush=True)
        deployment = deploy_multi_model(config, model_path_map, total_gpus)
        print(f"[AuroraServe] Service will route between {len(config.model_configs)} models", flush=True)
    
    print("[AuroraServe] Calling serve.run() to deploy to Ray cluster...", flush=True)
    serve_run_start = time.time()
    serve.run(deployment, route_prefix="/")
    serve_run_elapsed = time.time() - serve_run_start
    print_red(f"[AuroraServe] serve.run() call: {serve_run_elapsed:.2f}s")
    
    stage_elapsed = time.time() - stage_start
    print_red(f"[AuroraServe] ✓ Stage 3 deploy_model() andserve.run() completed in {stage_elapsed:.2f}s")

    # ---- All Stages Complete ---------------------------------------------
    total_elapsed = time.time() - overall_start
    print_red(f"[AuroraServe] ✓✓✓ CLUSTER FULLY READY ✓✓✓ Total time: {total_elapsed:.2f}s")
    print(
        "[AuroraServe] Service available at http://localhost:8000/v1",
        flush=True,
    )

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("[AuroraServe] Shutting down...")
