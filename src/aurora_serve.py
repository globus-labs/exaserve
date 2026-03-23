"""
Ray Serve LLM inference on Aurora (Intel XPU).

Each VLLMWorker replica handles both HTTP ingress (OpenAI-compatible API) and
vLLM inference on a single GPU tile, eliminating the separate Router layer.

For multi-model serving, each model is deployed as an independent Ray Serve
application at its own route_prefix (e.g. /llama-3-8b/v1).

No patched Ray libraries needed. Aurora relies on ZE_AFFINITY_MASK for device
isolation and keeps ONEAPI_DEVICE_SELECTOR unset because Triton's SYCL probe
crashes on Aurora when Ray rewrites it to a "level_zero:..." list.
"""

import asyncio
import inspect
import json
import os
import time
import uuid
import socket
import argparse
from typing import Optional, List, Dict, Any

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


def get_ray_node_ip() -> Optional[str]:
    """
    Best-effort lookup of the IP Ray associates with the current node.
    """
    try:
        return ray.util.get_node_ip_address()
    except Exception:
        return None


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
        return 1
    return max(1, total_gpus // model_config.tensor_parallel_size)


def get_alive_ray_gpu_nodes() -> List[Dict[str, Any]]:
    """Return alive Ray nodes that advertise GPU resources."""
    alive_nodes: List[Dict[str, Any]] = []
    for node in ray.nodes():
        if not node.get("Alive"):
            continue
        resources = node.get("Resources", {}) or {}
        gpu_count = int(resources.get("GPU", 0))
        if gpu_count < 1:
            continue
        node_ip = str(node.get("NodeManagerAddress", ""))
        resource_key = f"node:{node_ip}"
        if resource_key not in resources:
            resource_key = next(
                (key for key in resources if key.startswith("node:")),
                resource_key,
            )
        alive_nodes.append(
            {
                "ip": node_ip,
                "resource_key": resource_key,
                "gpu_count": gpu_count,
            }
        )
    return alive_nodes


def build_pp_placement_group_bundles(
    model_config: ModelConfig,
    config: DeploymentConfig,
) -> tuple[List[Dict[str, float]], List[str]]:
    """
    Build a stage-aware placement group for one PP replica.

    Bundle 0 is a CPU-only coordinator bundle pinned to the stage-0 node.
    The remaining bundles are one-GPU worker bundles ordered by
    (pp_rank, tp_rank), which matches vLLM's Ray rank layout.
    """
    current_ip = get_ray_node_ip() or get_hsn_ip()
    alive_gpu_nodes = get_alive_ray_gpu_nodes()
    if len(alive_gpu_nodes) < model_config.pipeline_parallel_size:
        raise RuntimeError(
            f"Need at least {model_config.pipeline_parallel_size} alive Ray GPU nodes "
            f"for PP, but found {len(alive_gpu_nodes)}: {alive_gpu_nodes}"
        )

    ordered_nodes = sorted(
        alive_gpu_nodes,
        key=lambda node: (node["ip"] != current_ip, node["ip"]),
    )
    stage_nodes = ordered_nodes[: model_config.pipeline_parallel_size]
    for node in stage_nodes:
        if int(node["gpu_count"]) < model_config.tensor_parallel_size:
            raise RuntimeError(
                f"Node {node['ip']} only has {node['gpu_count']} GPUs available, "
                f"but PP stage requires tensor_parallel_size={model_config.tensor_parallel_size}"
            )

    bundles: List[Dict[str, float]] = [
        {
            "CPU": float(model_config.num_cpus_per_replica),
            str(stage_nodes[0]["resource_key"]): 0.001,
        }
    ]
    for node in stage_nodes:
        resource_key = str(node["resource_key"])
        for _ in range(model_config.tensor_parallel_size):
            bundles.append({"GPU": 1.0, resource_key: 0.001})

    return bundles, [str(node["ip"]) for node in stage_nodes]


def build_actor_runtime_env() -> Dict[str, Dict[str, str]]:
    """Propagate Aurora-specific env vars into Serve replica actors."""
    env_vars: Dict[str, str] = {}
    for key in (
        "PYTHONPATH",
        "AURORA_VLLM_PATCH_PP_LAYER_FILTER",
        "AURORA_VLLM_PATCH_VERBOSE",
        "AURORA_VLLM_DISABLE_RAY_COMPILED_DAG",
        "AURORA_VLLM_FORCE_RAY_CHANNEL_TYPE",
        "RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR",
        "VLLM_USE_RAY_COMPILED_DAG_CHANNEL_TYPE",
        "VLLM_USE_RAY_WRAPPED_PP_COMM",
        "ZE_FLAT_DEVICE_HIERARCHY",
        "VLLM_TARGET_DEVICE",
    ):
        value = os.environ.get(key)
        if value:
            env_vars[key] = value

    return {"env_vars": env_vars}


def _message_content_to_text(content) -> str:
    """Convert OpenAI-style message content into a plain string."""
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
                continue
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str):
                    parts.append(text)
                    continue
            parts.append(str(item))
        return " ".join(part for part in parts if part)
    if isinstance(content, dict):
        text = content.get("text")
        if isinstance(text, str):
            return text
        return json.dumps(content)
    return str(content)


def _chat_messages_to_plain_prompt(
    messages: List[Dict],
    *,
    add_generation_prompt: bool,
) -> str:
    """Serialize chat messages for tokenizers without a native chat template."""
    lines = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        role = str(message.get("role", "user")).strip() or "user"
        content = _message_content_to_text(message.get("content"))
        if not content:
            continue
        lines.append(f"{role.title()}: {content}")

    if add_generation_prompt:
        lines.append("Assistant:")

    return "\n".join(lines).strip()


def init_ray_cluster(
    address: str,
    namespace: str = "serve",
    include_dashboard: bool = False,
    retries: int = 12,
    retry_delay_s: float = 5.0,
) -> None:
    """Retry Ray bootstrap for slow Aurora control-plane startup."""
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            ray.init(
                address=address,
                namespace=namespace,
                include_dashboard=include_dashboard,
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt == retries:
                break
            print(
                f"[AuroraServe] ray.init attempt {attempt}/{retries} failed for "
                f"{address}: {exc}. Retrying in {retry_delay_s:.1f}s...",
                flush=True,
            )
            time.sleep(retry_delay_s)
    raise RuntimeError(
        f"Failed to connect to Ray cluster at {address} after {retries} attempts"
    ) from last_error


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

        gpu_ids = [int(gpu_id) for gpu_id in ray.get_gpu_ids()]
        device_id = gpu_ids[0] if gpu_ids else 0

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
        if gpu_ids:
            affinity_mask = ",".join(str(gpu_id) for gpu_id in gpu_ids)
            os.environ["ZE_AFFINITY_MASK"] = affinity_mask
            os.environ.pop("ONEAPI_DEVICE_SELECTOR", None)
            print(
                f"[VLLMWorker pid={pid}] Assigned GPU tiles {gpu_ids} "
                f"ZE_AFFINITY_MASK={affinity_mask} "
                "ONEAPI_DEVICE_SELECTOR=<unset>",
                flush=True,
            )
        else:
            os.environ.pop("ZE_AFFINITY_MASK", None)
            os.environ.pop("ONEAPI_DEVICE_SELECTOR", None)
            print(
                f"[VLLMWorker pid={pid}] No Ray GPUs assigned to coordinator actor; "
                f"waiting for vLLM Ray workers to claim GPUs",
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
            if os.environ.get("VLLM_TARGET_DEVICE") == "xpu":
                os.environ.setdefault("AURORA_VLLM_DISABLE_RAY_COMPILED_DAG", "1")
                os.environ.setdefault("AURORA_VLLM_FORCE_RAY_CHANNEL_TYPE", "auto")
            master_addr = get_ray_node_ip() or get_hsn_ip()
            bind_host = "0.0.0.0"
            os.environ["VLLM_HOST_IP"] = master_addr

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
            master_addr=master_addr,
            master_port=port,
            gpu_memory_utilization=gpu_memory_utilization,
            max_model_len=max_model_len,
            enforce_eager=enforce_eager,
        )
        if pipeline_parallel_size > 1:
            engine_kwargs["pipeline_parallel_size"] = pipeline_parallel_size
            engine_kwargs["distributed_executor_backend"] = "ray"
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
        add_generation_prompt = bool(body.get("add_generation_prompt", True))
        continue_final_message = bool(body.get("continue_final_message", False))

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
            prompt = _chat_messages_to_plain_prompt(
                messages,
                add_generation_prompt=add_generation_prompt
                and not continue_final_message,
            )
        else:
            tokenizer = self.engine.get_tokenizer()
            chat_template = body.get("chat_template")
            chat_template_kwargs = body.get("chat_template_kwargs") or {}
            try:
                prompt = tokenizer.apply_chat_template(
                    messages,
                    tokenize=False,
                    add_generation_prompt=add_generation_prompt,
                    continue_final_message=continue_final_message,
                    chat_template=chat_template,
                    **chat_template_kwargs,
                )
            except ValueError as exc:
                if "chat_template" not in str(exc):
                    raise
                print(
                    "[AuroraServe] Tokenizer has no chat template; "
                    f"falling back to plain-text prompt for {self.model_id}",
                    flush=True,
                )
                prompt = _chat_messages_to_plain_prompt(
                    messages,
                    add_generation_prompt=add_generation_prompt
                    and not continue_final_message,
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

    placement_group_bundles: Optional[List[Dict[str, float]]] = None
    placement_group_strategy: Optional[str] = None

    # Replicas per node = tiles per node / TP size. Ray's GPU resource scheduling
    # already enforces this implicitly, but being explicit avoids stacking replicas
    # onto a subset of nodes when cluster membership fluctuates.
    if model_config.pipeline_parallel_size > 1:
        replicas_per_node = 1
        placement_group_bundles, stage_node_ips = build_pp_placement_group_bundles(
            model_config, config
        )
        placement_group_strategy = "PACK"
        print(
            f"[AuroraServe] PP placement for {model_id}: "
            f"stage nodes={stage_node_ips}, bundles={len(placement_group_bundles)} "
            f"(1 coordinator + {model_config.pipeline_parallel_size} x {model_config.tensor_parallel_size} GPU workers)",
            flush=True,
        )
    else:
        replicas_per_node = max(1, config.num_gpus_per_node // model_config.tensor_parallel_size)

    actor_num_gpus = 0 if model_config.pipeline_parallel_size > 1 else model_config.tensor_parallel_size

    deployment_options = dict(
        name=f"VLLMWorker-{safe_name}",
        num_replicas=num_replicas,
        ray_actor_options={
            "num_gpus": actor_num_gpus,
            "num_cpus": model_config.num_cpus_per_replica,
            "runtime_env": build_actor_runtime_env(),
        },
        max_ongoing_requests=config.worker_max_ongoing,
        health_check_period_s=30,
        health_check_timeout_s=10,
    )
    if placement_group_bundles is not None:
        deployment_options["placement_group_bundles"] = placement_group_bundles
        deployment_options["placement_group_strategy"] = placement_group_strategy
    else:
        deployment_options["max_replicas_per_node"] = replicas_per_node

    deployment = VLLMWorker.options(**deployment_options).bind(
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
    ray_address = os.environ.get("RAY_ADDRESS", "auto")
    print(
        f"[AuroraServe] Stage 1: Initializing Ray cluster at {ray_address}...",
        flush=True,
    )
    init_ray_cluster(ray_address, namespace="serve", include_dashboard=False)
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
