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

import argparse
import asyncio
import inspect
import json
import os
import socket
import sys
import time
import uuid
from typing import Optional, List, Dict, Any

# Patches MUST be installed before any ray.serve / vllm imports below —
# Ray Serve instantiates classes from ray.serve._private during package
# import, so monkey-patching after the fact is too late. The launcher
# arranges PYTHONPATH so the overlay tree wins import precedence; the
# vLLM monkey-patches in exaserve._sitecustomize are applied here.
# NOTE: this covers the server and Serve replica processes only. The vLLM
# EngineCore (a multiprocessing-spawn child) never imports this package;
# for PP it gets the patches via the PYTHONPATH sitecustomize shim written
# in VLLMWorker.__init__.
from .patches import apply_all as _apply_all  # noqa: I001
_apply_all()

import random
import ray
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from starlette.background import BackgroundTask
from ray import serve
from ray.serve.config import HTTPOptions, ProxyLocation
# vLLM is imported lazily (inside VLLMWorker / async_engine_arg_supported) so this
# module can be imported in a SGLang environment where vLLM is absent or where its
# transformers pin conflicts with SGLang's. The EXASERVE_ENGINE env var (read in
# deploy_model) selects VLLMWorker vs SGLangWorker; only the chosen engine is imported.

from . import request_validation as _rv
from .schemas import ModelConfig, DeploymentConfig, load_deployment_config, load_proxy_config
from .model_paths import get_model_route_name, get_model_storage_name, get_model_storage_path
from .model_staging import print_red, resolve_model_paths
from .replica_planner import (
    NodeInventory,
    DeploymentReplicaPlan,
    ModelReplicaPlan,
    compute_replica_plan,
    format_replica_plan,
    tp_replica_capacity_for_nodes,
)
from .scaling_trace import tracer, tracing_enabled



def _parse_own_engine_log(pid: int) -> dict:
    """Parse this replica's EngineCore sub-phase timings from its own Ray log.

    Each VLLMWorker's EngineCore output appears in the Ray worker log file
    at /tmp/ray/session_latest/logs/worker-*-{pid}.out. Since from_engine_args()
    blocks until model loading completes, the log lines exist by the time this
    function is called. Gives per-replica, per-GPU attribution.
    """
    import glob as _glob
    import re

    log_files = _glob.glob(f"/tmp/ray/session_latest/logs/worker-*-{pid}.out")
    if not log_files:
        return {}

    weight_pattern = re.compile(r"Loading weights took ([\d.]+) seconds")
    kv_pattern = re.compile(
        r"init engine \(profile, create kv cache, warmup model\) took ([\d.]+) seconds"
    )

    result = {}
    try:
        with open(log_files[0], "r") as f:
            for line in f:
                m = weight_pattern.search(line)
                if m:
                    result["weight_load_s"] = round(float(m.group(1)), 4)
                m = kv_pattern.search(line)
                if m:
                    result["kv_cache_init_s"] = round(float(m.group(1)), 4)
    except Exception:
        pass

    return result



def _instrumentation_enabled() -> bool:
    return os.environ.get("EXASERVE_INSTRUMENTATION", "0") == "1"


def _collect_instrumentation_all() -> None:
    """Gather /tmp/exaserve_inst/* from every node onto Lustre once at the end
    of startup. Only runs when EXASERVE_INSTRUMENTATION=1 — the overlay probes
    that produce these files are also gated on that flag, so on a clean Ray
    install this is a no-op.
    """
    if not _instrumentation_enabled():
        return
    run_log = os.environ.get("EXASERVE_RUN_LOG_DIR")
    if not run_log:
        print("[ExaServe] No EXASERVE_RUN_LOG_DIR; skipping instrumentation gather", flush=True)
        return

    @ray.remote(num_cpus=0)
    def _read_node_inst():
        import glob, os, socket
        host = socket.gethostname()
        out = {"hostname": host, "files": {}}
        for path in glob.glob("/tmp/exaserve_inst/*"):
            if os.path.isdir(path):
                continue
            try:
                with open(path, "rb") as f:
                    out["files"][os.path.basename(path)] = f.read()
            except Exception:
                pass
        return out

    alive_node_ids = [n["NodeID"] for n in ray.nodes() if n["Alive"]]
    refs = [
        _read_node_inst.options(
            scheduling_strategy=ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                node_id=node_id, soft=False
            )
        ).remote()
        for node_id in alive_node_ids
    ]
    try:
        results = ray.get(refs, timeout=120)
    except Exception as e:
        print(f"[ExaServe] Instrumentation gather failed: {e}", flush=True)
        return

    total_files = 0
    total_bytes = 0
    for r in results:
        dest_dir = f"{run_log}/instrumentation/{r['hostname']}"
        try:
            os.makedirs(dest_dir, exist_ok=True)
        except Exception:
            pass
        for name, content in r["files"].items():
            try:
                with open(f"{dest_dir}/{name}", "wb") as f:
                    f.write(content)
                total_files += 1
                total_bytes += len(content)
            except Exception:
                pass
    print(
        f"[ExaServe] Instrumentation gather: {total_files} files "
        f"({total_bytes/1024/1024:.1f} MB) from {len(results)} nodes",
        flush=True,
    )


def _ray_serve_timeout_patches() -> Dict[str, Any]:
    return {
        "HTTP_PROXY_TIMEOUT": int(os.environ.get("RAY_SERVE_HTTP_PROXY_TIMEOUT", "3600")),
        "PROXY_HEALTH_CHECK_TIMEOUT_S": 300.0,
        "PROXY_READY_CHECK_TIMEOUT_S": 60.0,
        "PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD": 100,
        "DEFAULT_HEALTH_CHECK_TIMEOUT_S": 600,
        "DEFAULT_HEALTH_CHECK_PERIOD_S": 120,
        "REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD": 100,
    }


# GCS-bootstrap hardening exported by launch_cluster.sh. RayConfig consumes
# RAY_* env at process start, so the value is only live in a process whose
# environment carries it — env present in a spawned Ray worker == RayConfig in
# that worker read it. Verified against a real remote worker (the process
# class that failed in sglang_direct_n256 run0-run3) instead of the driver,
# because a driver-only export that misses the mpiexec'd `ray start` is
# exactly the silent failure mode this guards against.
_CORE_ENV_EXPECTED = (
    "RAY_gcs_rpc_server_connect_timeout_s",
    "RAY_gcs_rpc_server_reconnect_timeout_s",
    "RAY_worker_register_timeout_seconds",
    "RAY_SERVE_MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT",
)


def _verify_core_env() -> None:
    """Probe a fresh remote Ray worker and fail fast if the GCS-hardening env
    did not propagate (set EXASERVE_SKIP_ENV_PROBE=1 to bypass)."""
    if os.environ.get("EXASERVE_SKIP_ENV_PROBE", "0") == "1":
        return
    expected = {k: os.environ[k] for k in _CORE_ENV_EXPECTED if k in os.environ}
    if not expected:
        print("[ExaServe] Core env probe: nothing exported to verify", flush=True)
        return

    @ray.remote(num_cpus=0)
    def _probe(keys):
        import os as _os
        return {k: _os.environ.get(k) for k in keys}

    seen = ray.get(_probe.remote(list(expected)), timeout=120)
    mismatched = {k: (v, seen.get(k)) for k, v in expected.items() if seen.get(k) != v}
    if mismatched:
        detail = ", ".join(f"{k}: driver={v} worker={w}" for k, (v, w) in mismatched.items())
        raise RuntimeError(
            f"GCS-hardening env not live in Ray workers ({detail}). "
            "Exports did not reach the mpiexec'd ray start; aborting before "
            "deploy rather than replaying the run0-run3 bootstrap lottery."
        )
    print(
        "[ExaServe] Core env verified in remote worker: "
        + ", ".join(f"{k}={v}" for k, v in expected.items()),
        flush=True,
    )


def _patch_ray_serve_proxy_constants() -> None:
    """Worker setup hook applied via runtime_env. Relaxes Ray Serve proxy/
    replica health-check thresholds so the ServeController doesn't kill
    proxies during 128+-node startup. Functional patch — runs on clean Ray
    too. Idempotent w.r.t. the overlay's static patches in constants.py.
    """
    import sys
    patches = _ray_serve_timeout_patches()
    try:
        from ray.serve._private import constants
        for attr, val in patches.items():
            setattr(constants, attr, val)
    except Exception:
        pass
    for mod_name in list(sys.modules):
        if "ray.serve" in mod_name:
            mod = sys.modules[mod_name]
            for attr, val in patches.items():
                if hasattr(mod, attr):
                    setattr(mod, attr, val)


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
    from vllm.engine.arg_utils import AsyncEngineArgs
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
    cluster_capacity, _ = get_tp_replica_capacity(model_config, config)
    if cluster_capacity < 1:
        raise RuntimeError(
            f"No feasible TP-only replica placement for {model_config.model_id} "
            f"with TP={model_config.tensor_parallel_size} and "
            f"CPUs/replica={model_config.num_cpus_per_replica}"
        )
    return cluster_capacity


def should_use_global_planner(config: DeploymentConfig) -> bool:
    """Use the planner for any PP deployment or any mixed-model deployment."""
    return len(config.model_configs) > 1 or any(
        model_config.pipeline_parallel_size > 1
        for model_config in config.model_configs
    )


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
                "hostname": str(node.get("NodeManagerHostname", "") or node_ip),
                "resource_key": resource_key,
                "gpu_count": gpu_count,
                "cpu_count": int(resources.get("CPU", 0)),
            }
        )
    return alive_nodes


def build_node_inventory() -> list[NodeInventory]:
    """Build a deterministic planner inventory from current alive Ray GPU nodes."""
    alive_nodes = sorted(get_alive_ray_gpu_nodes(), key=lambda item: item["ip"])
    return [
        NodeInventory(
            ip=str(node["ip"]),
            resource_key=str(node["resource_key"]),
            total_gpus=int(node["gpu_count"]),
            remaining_gpus=int(node["gpu_count"]),
            total_cpus=int(node["cpu_count"]),
            remaining_cpus=int(node["cpu_count"]),
        )
        for node in alive_nodes
    ]


def get_tp_replica_capacity(
    model_config: ModelConfig,
    config: DeploymentConfig,
) -> tuple[int, int]:
    """
    Compute TP-only capacity using live Ray nodes when available.

    Returns:
        (total_cluster_capacity, max_replicas_that_fit_on_any_single_node)
    """
    live_nodes = build_node_inventory()
    if live_nodes:
        return tp_replica_capacity_for_nodes(
            live_nodes,
            tensor_parallel_size=model_config.tensor_parallel_size,
            num_cpus_per_replica=model_config.num_cpus_per_replica,
        )

    per_node_cap = config.num_gpus_per_node // model_config.tensor_parallel_size
    return config.num_nodes * per_node_cap, per_node_cap


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


def ordered_pp_nodes() -> list[dict]:
    """Alive Ray GPU nodes in a STABLE global order (sorted by ip). This is the
    shared contract between shard-aware staging (pp_stage.assign_pp_nodes, which
    sorts the same node ips) and node-pinned deployment, so replica r's stage s
    is placed on the same node that staging put stage s's shards on."""
    return sorted(get_alive_ray_gpu_nodes(), key=lambda node: str(node["ip"]))


def build_pp_replica_bundles(
    model_config: ModelConfig,
    config: DeploymentConfig,
    replica_index: int,
) -> tuple[List[Dict[str, float]], List[str]]:
    """Node-pinned PP bundles for ONE specific replica in the shard-aware path:
    replica r occupies ordered_pp_nodes()[r*PP : (r+1)*PP], stage s on node r*PP+s
    (same mapping as pp_stage.assign_pp_nodes). Unlike build_pp_placement_group_bundles
    (which always pins the FIRST PP nodes — single-replica only), this pins each
    replica to its own disjoint node group so N single-replica deployments don't
    contend. Bundle 0 is the CPU coordinator on the stage-0 node; the rest are
    one-GPU worker bundles ordered by (pp_rank, tp_rank)."""
    pp = model_config.pipeline_parallel_size
    nodes = ordered_pp_nodes()
    start = replica_index * pp
    stage_nodes = nodes[start:start + pp]
    if len(stage_nodes) < pp:
        raise RuntimeError(
            f"shard-aware PP: replica {replica_index} needs nodes "
            f"[{start}:{start + pp}] but only {len(nodes)} alive GPU nodes exist")
    for node in stage_nodes:
        if int(node["gpu_count"]) < model_config.tensor_parallel_size:
            raise RuntimeError(
                f"Node {node['ip']} has {node['gpu_count']} GPUs < "
                f"TP={model_config.tensor_parallel_size}")
    bundles: List[Dict[str, float]] = [
        {"CPU": float(model_config.num_cpus_per_replica),
         str(stage_nodes[0]["resource_key"]): 0.001}
    ]
    for node in stage_nodes:
        for _ in range(model_config.tensor_parallel_size):
            bundles.append({"GPU": 1.0, str(node["resource_key"]): 0.001})
    return bundles, [str(node["ip"]) for node in stage_nodes]


def build_planner_placement_group(
    model_config: ModelConfig,
) -> tuple[List[Dict[str, float]], str, int]:
    """
    Build placement-group bundles that match the planner's capacity model.

    For PP, bundle 0 is a CPU-only coordinator bundle and the rest are one
    single-GPU bundle per worker (vLLM 0.15 rejects bundles with >1 GPU).
    These bundles carry no node resource keys so Serve can reuse the template
    across replicas; single-replica PP instead uses the node-pinned layout
    from build_pp_placement_group_bundles (see deploy_model). For TP-only,
    the actor directly consumes a single CPU+GPU bundle.
    """
    if model_config.pipeline_parallel_size > 1:
        # vLLM 0.15's Ray executor rejects bundles with more than 1 GPU
        # (ray_utils.initialize_ray_cluster), so PP must use one bundle per
        # GPU worker. Without node resource keys all replicas can share this
        # template, but a stage's TP group may straddle nodes under PACK.
        bundles = [{"CPU": float(model_config.num_cpus_per_replica)}]
        bundles.extend(
            {"GPU": 1.0}
            for _ in range(
                model_config.pipeline_parallel_size
                * model_config.tensor_parallel_size
            )
        )
        return bundles, "PACK", 0

    bundles = [
        {
            "CPU": float(model_config.num_cpus_per_replica),
            "GPU": float(model_config.tensor_parallel_size),
        }
    ]
    return bundles, "PACK", model_config.tensor_parallel_size


def build_actor_runtime_env(
    extra_env_vars: Optional[Dict[str, str]] = None,
) -> Dict[str, Dict[str, str]]:
    """Propagate Aurora-specific env vars into Serve replica actors."""
    env_vars: Dict[str, str] = {}
    for key in (
        "PYTHONPATH",
        "EXASERVE_VLLM_PATCH_PP_LAYER_FILTER",
        "EXASERVE_VLLM_PATCH_VERBOSE",
        "EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG",
        "EXASERVE_XPU_VLLM_FORCE_RAY_CHANNEL_TYPE",
        "RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR",
        "VLLM_USE_RAY_COMPILED_DAG_CHANNEL_TYPE",
        "VLLM_USE_RAY_WRAPPED_PP_COMM",
        "ZE_FLAT_DEVICE_HIERARCHY",
        "VLLM_TARGET_DEVICE",
        "EXASERVE_SCALING_TRACE",
        "RAYON_NUM_THREADS",
        "TOKENIZERS_PARALLELISM",
    ):
        value = os.environ.get(key)
        if value:
            env_vars[key] = value
    if extra_env_vars:
        env_vars.update(extra_env_vars)

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
    retries: int = 60,
    retry_delay_s: float = 5.0,
) -> None:
    """Retry Ray bootstrap for slow Aurora control-plane startup."""
    last_error: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            attempt_start = time.monotonic()
            ray.init(
                address=address,
                namespace=namespace,
                include_dashboard=include_dashboard,
                runtime_env=ray.runtime_env.RuntimeEnv(
                    worker_process_setup_hook=_patch_ray_serve_proxy_constants,
                ),
            )
            tracer.record_phase(
                "ray.init.connect",
                time.monotonic() - attempt_start,
                attempt=attempt,
                address=address,
            )
            return
        except Exception as exc:
            last_error = exc
            tracer.event("ray.init.retry", attempt=attempt, error=str(exc))
            if attempt == retries:
                break
            print(
                f"[ExaServe] ray.init attempt {attempt}/{retries} failed for "
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


class CollectingStatLogger:
    """Buffers vLLM scheduler and per-request stats for post-run collection.

    Conforms to vLLM v1 StatLoggerBase interface:
      __init__(vllm_config, engine_index)
      record(scheduler_stats, iteration_stats, mm_cache_stats=None, engine_idx=0)
      log()

    Passed as a class to AsyncLLMEngine.from_engine_args(stat_loggers=[CollectingStatLogger]).
    vLLM instantiates it; retrieve the instance via the class-level registry.
    """

    # Class-level registry: pid → instance (one per replica process).
    _instances = {}

    def __init__(self, vllm_config=None, engine_index=0):
        self.scheduler_snapshots = []
        self.finished_requests = []
        CollectingStatLogger._instances[os.getpid()] = self

    def record(self, scheduler_stats=None, iteration_stats=None, **kwargs):
        if scheduler_stats is not None:
            self.scheduler_snapshots.append({
                "timestamp": time.time(),
                "running": getattr(scheduler_stats, "num_running_reqs", 0),
                "waiting": getattr(scheduler_stats, "num_waiting_reqs", 0),
                "kv_cache_usage": getattr(scheduler_stats, "kv_cache_usage", 0.0),
            })
        if iteration_stats is not None:
            now = time.time()
            for req in getattr(iteration_stats, "finished_requests", []):
                self.finished_requests.append({
                    "finished_at": now,  # wall-clock; lets analysis drop warm-up-run requests
                    "e2e_latency": getattr(req, "e2e_latency", 0.0),
                    "queued_time": getattr(req, "queued_time", 0.0),
                    "prefill_time": getattr(req, "prefill_time", 0.0),
                    "inference_time": getattr(req, "inference_time", 0.0),
                    "decode_time": getattr(req, "decode_time", 0.0),
                    "num_prompt_tokens": getattr(req, "num_prompt_tokens", 0),
                    "num_generation_tokens": getattr(req, "num_generation_tokens", 0),
                    "num_cached_tokens": getattr(req, "num_cached_tokens", 0),
                })

    def log(self):
        pass

    def log_engine_initialized(self):
        pass

    def record_sleep_state(self, is_awake=0, level=0):
        pass

    @staticmethod
    def _pct(sorted_vals, q):
        if not sorted_vals:
            return None
        if len(sorted_vals) == 1:
            return sorted_vals[0]
        idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
        return sorted_vals[idx]

    def summary(self):
        """Per-replica server-side metrics, immune to proxy/delivery effects.

        server-TTFT = queued_time + prefill_time (arrival -> first token GENERATED).
        server-TBT  = decode_time / (gen_tokens - 1)  (true per-request decode cadence).
        These are what the decode SLO is really about; client-side TBT can be
        distorted by proxy coalescing (http-no-delay off), these cannot.
        """
        fr = list(self.finished_requests)  # snapshot: record() appends concurrently
        ttft, tbt, e2e, dec, pre, que = [], [], [], [], [], []
        for r in fr:
            q = float(r.get("queued_time", 0.0) or 0.0)
            p = float(r.get("prefill_time", 0.0) or 0.0)
            d = float(r.get("decode_time", 0.0) or 0.0)
            n = int(r.get("num_generation_tokens", 0) or 0)
            ttft.append(q + p); que.append(q); pre.append(p); dec.append(d)
            e2e.append(float(r.get("e2e_latency", 0.0) or 0.0))
            if n > 1:
                tbt.append(d / (n - 1))
        snaps = list(self.scheduler_snapshots)
        run = [s.get("running", 0) for s in snaps]
        kv = [s.get("kv_cache_usage", 0.0) for s in snaps]

        def stats(v):
            if not v:
                return {"n": 0}
            s = sorted(v)
            return {"n": len(s), "mean": sum(s) / len(s),
                    "p50": self._pct(s, 0.50), "p90": self._pct(s, 0.90),
                    "p99": self._pct(s, 0.99), "max": s[-1]}
        return {
            "total_requests": len(fr),
            "server_ttft": stats(ttft),     # queued+prefill
            "server_tbt": stats(tbt),       # decode/(gen-1)
            "e2e": stats(e2e),
            "queued_time": stats(que),
            "prefill_time": stats(pre),
            "decode_time": stats(dec),
            "mean_batch_size": (sum(run) / len(run)) if run else 0,
            "max_batch_size": max(run) if run else 0,
            "kv_cache_peak": max(kv) if kv else 0.0,
        }

    def sample(self, cap=4000):
        """A bounded, evenly-strided per-request sample for fleet-wide pooled
        percentiles (computing exact pooled p99 needs raw values; this caps the
        wire payload at ~cap/replica while staying representative across the run)."""
        fr = list(self.finished_requests)  # snapshot
        if not fr:
            return []
        step = max(1, len(fr) // cap)
        out = []
        for r in fr[::step][:cap]:
            n = int(r.get("num_generation_tokens", 0) or 0)
            d = float(r.get("decode_time", 0.0) or 0.0)
            out.append({
                "finished_at": r.get("finished_at"),
                "ttft": float(r.get("queued_time", 0.0) or 0.0) + float(r.get("prefill_time", 0.0) or 0.0),
                "tbt": (d / (n - 1)) if n > 1 else None,
                "e2e": float(r.get("e2e_latency", 0.0) or 0.0),
            })
        return out

    def to_dict(self):
        # Ship the per-replica summary + a capped sample (for pooled fleet
        # percentiles) + the scheduler trace. The full per-request array is NOT
        # shipped (unbounded at 256n); summary+sample preserve what we report.
        return {
            "summary": self.summary(),
            "sample": self.sample(),
            "scheduler_snapshots": self.scheduler_snapshots[:2000],
        }

    @classmethod
    def get_instance(cls):
        return cls._instances.get(os.getpid())


# --- Serving-stats collection: replicas PUSH summaries to a named head actor ---
# Avoids serve.status() replica enumeration (no per-replica handles in this Ray
# version) and Lustre MDS load (no per-replica files). Independent of the
# EXASERVE_SCALING_TRACE gating used by the init-stats collector.
_SERVING_STATS_ACTOR = "ServingStatsCollector"
_SERVING_STATS_NS = "serve"


def _deployment_scope() -> str:
    """PR-029: a stable per-deployment id so telemetry actors in a REUSED Ray
    cluster cannot collide or inherit a prior deployment's state. Prefers an
    explicit id, then the scaling-trace token, then the scheduler job id."""
    for var in ("EXASERVE_DEPLOYMENT_ID", "EXASERVE_SCALING_TRACE_TOKEN",
                "EXASERVE_JOBID", "PBS_JOBID"):
        val = os.environ.get(var)
        if val:
            return str(val).split(".")[0][:40]
    return "default"


class _ServingStatsCollectorImpl:
    """Head-node in-memory sink; keeps the latest payload per replica key."""

    def __init__(self):
        self._data = {}

    def report(self, key, payload):
        self._data[str(key)] = payload

    def get_all(self):
        return self._data

    def count(self):
        return len(self._data)


def _serving_collector_name() -> str:
    # PR-029: deployment-scoped name — a new deployment gets a fresh actor,
    # never a prior deployment's stale per-replica data.
    return f"{_SERVING_STATS_ACTOR}:{_deployment_scope()}"


def get_or_create_serving_collector():
    import ray
    cls = ray.remote(_ServingStatsCollectorImpl)
    return cls.options(
        name=_serving_collector_name(), namespace=_SERVING_STATS_NS,
        lifetime="detached", num_cpus=0, get_if_exists=True,
    ).remote()


def _serving_stats_push_loop(period_s=None):
    """Daemon-thread loop in the replica process: every period_s, compute this
    replica's server-side summary+sample and push to the head collector. The
    last push before teardown carries near-complete data. Logs the buffered
    request count once so we can confirm the logger is actually recording.

    Scale-safe: at 256n there are ~3072 replicas pushing to one actor, so the
    period and sample cap are env-tunable (defaults sized for 256n: ~256 pushes/s
    of ~1k-sample payloads). EXASERVE_SS_PERIOD, EXASERVE_SS_SAMPLE_CAP."""
    import ray
    period_s = period_s or float(os.environ.get("EXASERVE_SS_PERIOD", "10"))
    sample_cap = int(os.environ.get("EXASERVE_SS_SAMPLE_CAP", "1500"))
    try:
        node_ip = ray.util.get_node_ip_address()
    except Exception:
        node_ip = "?"
    key = f"{node_ip}:{os.getpid()}"
    collector = None
    logged_records = False
    logged_none = False
    while True:
        time.sleep(period_s)
        logger = CollectingStatLogger.get_instance()
        if logger is None:
            if not logged_none:
                print(f"[serving-stats] {key}: logger get_instance()=None "
                      "(stat logger not in this process)", flush=True)
                logged_none = True
            continue
        try:
            n = len(logger.finished_requests)
            if n and not logged_records:
                print(f"[serving-stats] {key}: recording ({n} reqs buffered)", flush=True)
                logged_records = True
            payload = {"summary": logger.summary(), "sample": logger.sample(sample_cap),
                       "node_ip": node_ip, "pid": os.getpid(), "n": n}
            if collector is None:
                collector = get_or_create_serving_collector()
            collector.report.remote(key, payload)
        except Exception as e:
            print(f"[serving-stats] {key}: push failed: {e}", flush=True)


@serve.deployment
@serve.ingress(app)
class EngineWorker:
    """Single Ray Serve deployment: OpenAI-format HTTP ingress on one GPU tile,
    delegating inference to a pluggable EngineBackend (vLLM / SGLang / null).

    The engine is selected by ``engine_name`` (from EXASERVE_ENGINE) or replaced
    by NullEngine when ``null_compute`` is set. This host owns everything engine-
    agnostic — the HTTP surface, per-tile placement wiring (via deploy_model),
    stats, and the readiness warmup — while each EngineBackend owns device
    isolation + engine creation + generation. See exaserve.engines.
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
        max_num_seqs: int = None,
        collect_stats: bool = False,
        enable_log_requests: bool = True,
        engine_name: str = "vllm",
    ):
        from .engines import EngineSpec, NullEngine, get_engine

        init_start = time.time()
        pid = os.getpid()
        hostname = socket.gethostname()
        self.model_id = model_id
        self.null_compute = null_compute

        gpu_ids = []
        for g in ray.get_gpu_ids():
            try:
                gpu_ids.append(int(g))
            except (ValueError, TypeError):
                pass  # non-integer Ray GPU id (e.g. test harness) -> skip mask
        device_id = gpu_ids[0] if gpu_ids else 0

        spec = EngineSpec(
            model_id=model_id,
            local_path=local_model_path or model_id,
            tensor_parallel_size=tensor_parallel_size,
            pipeline_parallel_size=pipeline_parallel_size,
            max_model_len=max_model_len,
            gpu_memory_utilization=gpu_memory_utilization,
            enforce_eager=enforce_eager,
            max_num_seqs=max_num_seqs,
            device_ids=gpu_ids,
            collect_stats=collect_stats,
            enable_log_requests=enable_log_requests,
        )

        if null_compute:
            latency = float(os.environ.get("EXASERVE_NULL_COMPUTE_LATENCY", "1.0"))
            self.backend = NullEngine(latency_s=latency)
            print(
                f"[EngineWorker pid={pid}] NullCompute mode on tile {device_id} "
                f"(latency={latency:.2f}s, no engine)",
                flush=True,
            )
        else:
            self.backend = get_engine(engine_name)

        self.backend.create(spec)

        total_s = time.time() - init_start
        print_red(f"[EngineWorker pid={pid}] ★ INIT TOTAL: {total_s:.2f}s ★")

        from .scaling_trace import report_replica_stats
        replica_info = {
            "pid": pid,
            "hostname": hostname,
            "model_id": model_id,
            "device_id": device_id,
            "null_compute": null_compute,
            "total_init_s": round(total_s, 4),
            "wall_start": init_start,
            "wall_end": time.time(),
        }
        init_stats = getattr(self.backend, "init_stats", None)
        if callable(init_stats):
            replica_info.update(init_stats())
        report_replica_stats(replica_info)

    async def reconfigure(self, user_config):
        """Serve awaits this pre-healthy when a deployment sets user_config (see
        deploy_model). Engines that need a warmup (e.g. SGLang JIT kernels) run it
        here; others are a no-op."""
        await self.backend.warmup()

    # ---- HTTP endpoints ------------------------------------------------------

    @app.get("/health")
    async def health_check(self):
        return JSONResponse({"status": "healthy", "model": self.model_id})

    @app.get("/stats")
    async def stats(self):
        pid = os.getpid()
        if self.null_compute:
            return JSONResponse({"pid": pid, "model": self.model_id, "null_compute": True})
        result = {"pid": pid, "model": self.model_id}
        result.update(self.backend.live_stats())
        return JSONResponse(result)

    def collect_stats(self) -> dict:
        """Called via ray.get(actor_handle.collect_stats.remote())."""
        data = self.backend.collect_stats()
        data.setdefault("pid", os.getpid())
        data.setdefault("model", self.model_id)
        return data

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
                        "owned_by": "exaserve",
                    }
                ],
            }
        )

    @app.post("/v1/chat/completions")
    async def chat_completions(self, request: Request):
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "malformed JSON body"}, status_code=400)
        try:
            _rv.require_object_body(body)   # IMP-H04: list/scalar body -> 400
            _rv.validate_model_field(body, self._served_model_names())
            _rv.validate_messages(body.get("messages"))
            sampling = self._parse_sampling(body)
            stream = _rv.strict_flag(body, "stream", False)
            add_generation_prompt = _rv.strict_flag(body, "add_generation_prompt", True)
            continue_final_message = _rv.strict_flag(body, "continue_final_message", False)
        except _rv.RequestValidationError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        messages = body.get("messages", [])

        prompt = self.backend.build_chat_prompt(
            messages,
            add_generation_prompt=add_generation_prompt,
            continue_final_message=continue_final_message,
            chat_template=body.get("chat_template"),
            chat_template_kwargs=body.get("chat_template_kwargs") or {},
        )

        request_id = f"chatcmpl-{uuid.uuid4().hex[:12]}"
        sampling["_request_id"] = request_id
        # PR-032: preserve a caller-supplied correlation id (linked to, not
        # conflated with, our completion id) so a request can be traced
        # gateway -> Serve -> engine.
        sampling["_correlation_id"] = request.headers.get("x-request-id") or request_id
        if stream:
            return StreamingResponse(
                self._chat_stream(request_id, prompt, sampling),
                media_type="text/event-stream",
            )
        return await self._chat_non_stream(request_id, prompt, sampling)

    @app.post("/v1/completions")
    async def completions(self, request: Request):
        try:
            body = await request.json()
        except Exception:
            return JSONResponse({"error": "malformed JSON body"}, status_code=400)
        try:
            _rv.require_object_body(body)   # IMP-H04: list/scalar body -> 400
            _rv.validate_model_field(body, self._served_model_names())
            sampling = self._parse_sampling(body)
            stream = _rv.strict_flag(body, "stream", False)
        except _rv.RequestValidationError as exc:
            return JSONResponse({"error": str(exc)}, status_code=400)
        prompt = body.get("prompt", "")
        if not isinstance(prompt, (str, list)):
            return JSONResponse({"error": "prompt must be a string or list"},
                                status_code=400)
        request_id = f"cmpl-{uuid.uuid4().hex[:12]}"
        sampling["_request_id"] = request_id
        sampling["_correlation_id"] = request.headers.get("x-request-id") or request_id  # PR-032
        if stream:
            return StreamingResponse(
                self._completion_stream(request_id, prompt, sampling),
                media_type="text/event-stream",
            )
        res = await self.backend.generate(prompt, sampling)
        _corr = sampling.get("_correlation_id", request_id)  # PR-032
        if res.error:
            return JSONResponse({"error": res.error}, status_code=500,
                                headers={"X-Request-ID": _corr})
        return JSONResponse(
            {
                "id": request_id,
                "object": "text_completion",
                "created": int(time.time()),
                "model": self.model_id,
                "choices": [
                    {"index": 0, "text": res.text, "finish_reason": res.finish_reason}
                ],
                "usage": {
                    "prompt_tokens": res.prompt_tokens,
                    "completion_tokens": res.completion_tokens,
                    "total_tokens": res.prompt_tokens + res.completion_tokens,
                },
            },
            headers={"X-Request-ID": _corr},  # PR-032: echo the correlation id
        )

    # ---- Internal helpers ----------------------------------------------------

    def _served_model_names(self) -> set:
        """Identities a client may name in the `model` field for THIS
        replica: the HF id, plus its route name (PR-011)."""
        names = {self.model_id}
        try:
            from .model_paths import get_model_storage_name

            storage = get_model_storage_name(self.model_id)
            names.add(storage)
            names.add(storage.replace(".", "-"))  # route name
        except Exception:
            pass
        return names

    @staticmethod
    def _parse_sampling(body: dict) -> dict:
        """OpenAI request body -> validated neutral sampling dict (PR-011)."""
        return _rv.parse_sampling(body)

    async def _chat_non_stream(self, request_id: str, prompt: str, sampling: dict):
        res = await self.backend.generate(prompt, sampling)
        if res.error:
            return JSONResponse({"error": res.error}, status_code=500)
        return JSONResponse(
            {
                "id": request_id,
                "object": "chat.completion",
                "created": int(time.time()),
                "model": self.model_id,
                "choices": [
                    {
                        "index": 0,
                        "message": {"role": "assistant", "content": res.text},
                        "finish_reason": res.finish_reason,
                    }
                ],
                "usage": {
                    "prompt_tokens": res.prompt_tokens,
                    "completion_tokens": res.completion_tokens,
                    "total_tokens": res.prompt_tokens + res.completion_tokens,
                },
            }
        )

    async def _chat_stream(self, request_id: str, prompt: str, sampling: dict):
        created = int(time.time())
        usage = None
        async for chunk in self.backend.generate_stream(prompt, sampling):
            if chunk.finish_reason is not None:
                usage = {
                    "prompt_tokens": chunk.prompt_tokens,
                    "completion_tokens": chunk.completion_tokens,
                    "total_tokens": chunk.prompt_tokens + chunk.completion_tokens,
                }
            if chunk.delta:
                sse = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": self.model_id,
                    "choices": [{"index": 0, "delta": {"content": chunk.delta}, "finish_reason": None}],
                }
                yield f"data: {json.dumps(sse)}\n\n"
            if chunk.finish_reason is not None:
                final = {
                    "id": request_id,
                    "object": "chat.completion.chunk",
                    "created": created,
                    "model": self.model_id,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": chunk.finish_reason}],
                }
                if usage:
                    final["usage"] = usage
                yield f"data: {json.dumps(final)}\n\n"
        yield "data: [DONE]\n\n"

    async def _completion_stream(self, request_id: str, prompt: str, sampling: dict):
        created = int(time.time())
        usage = None
        async for chunk in self.backend.generate_stream(prompt, sampling):
            if chunk.finish_reason is not None:
                usage = {
                    "prompt_tokens": chunk.prompt_tokens,
                    "completion_tokens": chunk.completion_tokens,
                    "total_tokens": chunk.prompt_tokens + chunk.completion_tokens,
                }
            if chunk.delta:
                sse = {
                    "id": request_id,
                    "object": "text_completion",
                    "created": created,
                    "model": self.model_id,
                    "choices": [{"index": 0, "text": chunk.delta, "finish_reason": None}],
                }
                yield f"data: {json.dumps(sse)}\n\n"
            if chunk.finish_reason is not None:
                final = {
                    "id": request_id,
                    "object": "text_completion",
                    "created": created,
                    "model": self.model_id,
                    "choices": [{"index": 0, "text": "", "finish_reason": chunk.finish_reason}],
                }
                if usage:
                    final["usage"] = usage
                yield f"data: {json.dumps(final)}\n\n"
        yield "data: [DONE]\n\n"


# ═══════════════════════════════════════════════════════════════════════════
# ShardUmbrella — root-route fan-out for shard-aware PP (proxy-agnostic serving)
# ═══════════════════════════════════════════════════════════════════════════
umbrella_app = FastAPI()


@serve.deployment
@serve.ingress(umbrella_app)
class ShardUmbrella:
    """Root-route ingress that fans out to the N node-pinned PP replica apps
    served at /<route>_r{0..N-1}. Lets ANY external proxy -- or a direct client --
    treat a shard-aware PP deployment as ONE root endpoint: the proxy just
    round-robins across nodes at "/", and this deployment picks a replica and
    rewrites the path. No per-proxy shard routing (HAProxy set-path) required, so
    a new proxy backend needs zero shard-awareness.

    Each request is reverse-proxied (streaming, so SSE passes through untouched)
    to http://127.0.0.1:<backend_port>/<route>_r{rand}/<path>, which the node's
    own Ray Serve HTTP proxy (EveryNode) routes to that replica. CPU-only and
    replicated across nodes, so the umbrella is not itself a single bottleneck.
    """

    def __init__(self, route_name: str, n_replicas: int, backend_port: int = 8000):
        self._route = str(route_name)
        self._n = int(n_replicas)
        self._base = f"http://127.0.0.1:{int(backend_port)}"
        # Long timeout to match replica/proxy streaming; unbounded pool so the
        # umbrella never becomes the connection limiter.
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(330.0, connect=10.0),
            limits=httpx.Limits(max_connections=None, max_keepalive_connections=None),
        )

    @umbrella_app.api_route(
        "/{fwd_path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"]
    )
    async def _fanout(self, request: Request, fwd_path: str):
        idx = random.randrange(self._n) if self._n > 1 else 0
        url = f"{self._base}/{self._route}_r{idx}/{fwd_path}"
        body = await request.body()
        fwd_headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in ("host", "content-length", "connection")
        }
        upstream_req = self._client.build_request(
            request.method, url, content=body,
            headers=fwd_headers, params=request.query_params,
        )
        upstream = await self._client.send(upstream_req, stream=True)
        resp_headers = {
            k: v for k, v in upstream.headers.items()
            if k.lower() not in ("content-length", "transfer-encoding", "connection")
        }
        return StreamingResponse(
            upstream.aiter_raw(),
            status_code=upstream.status_code,
            headers=resp_headers,
            media_type=upstream.headers.get("content-type"),
            background=BackgroundTask(upstream.aclose),
        )


def deploy_shard_umbrella(
    safe_name: str, n_rep: int, backend_port: int, n_umbrella: int
) -> None:
    """Deploy ShardUmbrella at the root route, in front of the N /<route>_r{i}
    replica apps. Opt-in via EXASERVE_PP_UMBRELLA=1 in the shard-aware PP path."""
    n_umbrella = max(1, int(n_umbrella))
    umb = ShardUmbrella.options(
        name=f"{safe_name}_umbrella",
        num_replicas=n_umbrella,
        ray_actor_options={"num_cpus": 1},
        max_ongoing_requests=1000,
    ).bind(safe_name, n_rep, backend_port)
    serve.run(umb, name=f"{safe_name}_umbrella", route_prefix="/")
    print(
        f"[ExaServe] ✓ ShardUmbrella at http://localhost:{backend_port}/v1 "
        f"({n_umbrella} replica(s)) fans out to {n_rep} /{safe_name}_r(i) apps; "
        f"any proxy can front '/' with NO shard routing.",
        flush=True,
    )


# ---------------------------------------------------------------------------
# Deployment helpers
# ---------------------------------------------------------------------------
def deploy_model(
    model_config: ModelConfig,
    model_path_map: Dict[str, str],
    total_gpus: int,
    config: DeploymentConfig,
    model_index: int = 0,
    *,
    num_replicas_override: Optional[int] = None,
    use_global_planner: bool = False,
    planner_max_replicas_per_node: Optional[int] = None,
    pp_replica_index: Optional[int] = None,
) -> tuple:
    """
    Build a bound VLLMWorker deployment for one model.

    Ray Serve options (num_gpus, replicas, max_ongoing_requests, …) are passed
    via .options() so no factory/class-creation indirection is needed.

    When `pp_replica_index` is set (shard-aware PP path), this builds ONE
    node-pinned single-replica deployment for that replica (name suffix -r{idx}),
    pinned via build_pp_replica_bundles — the caller loops to create the N
    deployments. Otherwise behaviour is unchanged.

    Returns:
        (deployment, model_id)
    """
    model_id = model_config.model_id
    local_path = model_path_map.get(model_id, model_id)
    null_compute = os.environ.get("EXASERVE_NULL_COMPUTE", "0") == "1"

    shard_aware_pp = pp_replica_index is not None
    num_replicas = (
        1 if shard_aware_pp else (
            num_replicas_override
            if num_replicas_override is not None
            else model_config.num_replicas
            or default_num_replicas(model_config, total_gpus, config)
        )
    )

    safe_name = get_model_route_name(model_id)
    deployment_name_suffix = f"-r{pp_replica_index}" if shard_aware_pp else ""

    print(
        f"[ExaServe] Configuring EngineWorker for {model_id}\n"
        f"  Replicas    : {num_replicas} "
        f"(TP={model_config.tensor_parallel_size}, PP={model_config.pipeline_parallel_size})\n"
        f"  NullCompute : {null_compute}\n"
        f"  Local path  : {local_path}",
        flush=True,
    )
    if null_compute:
        latency = float(os.environ.get("EXASERVE_NULL_COMPUTE_LATENCY", "1.0"))
        print(
            f"[ExaServe] NULL-COMPUTE mode — engine replaced by sleep({latency:.2f}s)",
            flush=True,
        )

    placement_group_bundles: Optional[List[Dict[str, float]]] = None
    placement_group_strategy: Optional[str] = None
    actor_num_gpus: int
    extra_env_vars: Dict[str, str] = {}

    if use_global_planner:
        if model_config.pipeline_parallel_size > 1:
            if shard_aware_pp:
                # Shard-aware path: this deployment is ONE replica, pinned to its
                # own disjoint node group (ordered_pp_nodes()[r*PP:(r+1)*PP]),
                # matching where pp_stage staged that replica's per-stage shards.
                placement_group_bundles, stage_node_ips = (
                    build_pp_replica_bundles(model_config, config, pp_replica_index)
                )
                placement_group_strategy = "PACK"
                actor_num_gpus = 0
                print(
                    f"[ExaServe] Shard-aware PP placement for {model_id} "
                    f"replica {pp_replica_index}: node-pinned stages={stage_node_ips}, "
                    f"bundles={len(placement_group_bundles)}",
                    flush=True,
                )
            elif num_replicas == 1:
                # Node-pinned per-GPU bundles: each stage's TP group stays on
                # one node. Only valid for a single replica — Serve shares one
                # bundle template across replicas, so pinned bundles would make
                # multiple replicas contend for the same nodes.
                placement_group_bundles, stage_node_ips = (
                    build_pp_placement_group_bundles(model_config, config)
                )
                placement_group_strategy = "PACK"
                actor_num_gpus = 0
                print(
                    f"[ExaServe] Planner PP placement for {model_id}: "
                    f"stage nodes={stage_node_ips}, "
                    f"bundles={len(placement_group_bundles)} "
                    f"(1 coordinator + {model_config.pipeline_parallel_size}"
                    f" x {model_config.tensor_parallel_size} GPU workers)",
                    flush=True,
                )
            else:
                placement_group_bundles, placement_group_strategy, actor_num_gpus = (
                    build_planner_placement_group(model_config)
                )
                print(
                    f"[ExaServe] Planner placement for {model_id}: "
                    f"strategy={placement_group_strategy}, "
                    f"bundles={placement_group_bundles}; WARNING: multi-replica "
                    "PP uses location-agnostic bundles — a stage's TP group may "
                    "straddle nodes",
                    flush=True,
                )
        else:
            actor_num_gpus = model_config.tensor_parallel_size
            print(
                f"[ExaServe] Planner scheduling for {model_id}: "
                "using direct actor GPU reservation for TP-only replicas",
                flush=True,
            )
    else:
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
                f"[ExaServe] PP placement for {model_id}: "
                f"stage nodes={stage_node_ips}, bundles={len(placement_group_bundles)} "
                f"(1 coordinator + {model_config.pipeline_parallel_size} x {model_config.tensor_parallel_size} GPU workers)",
                flush=True,
            )
        else:
            _, replicas_per_node = get_tp_replica_capacity(
                model_config,
                config,
            )
            if replicas_per_node < 1:
                raise RuntimeError(
                    f"No single Ray node can fit TP-only replica for {model_id} "
                    f"(TP={model_config.tensor_parallel_size}, "
                    f"CPUs/replica={model_config.num_cpus_per_replica})"
                )
        actor_num_gpus = (
            0
            if model_config.pipeline_parallel_size > 1
            else model_config.tensor_parallel_size
        )

    # Engine selection: one EngineWorker host serves any pluggable EngineBackend
    # (exaserve.engines). EXASERVE_ENGINE picks the backend (vllm/sglang/...);
    # everything else (placement, replicas, HAProxy, Go replay client) is
    # identical — a single-variable engine swap.
    engine = os.environ.get("EXASERVE_ENGINE", "vllm").lower()

    deployment_options = dict(
        name=f"EngineWorker-{safe_name}{deployment_name_suffix}",
        num_replicas=num_replicas,
        ray_actor_options={
            "num_gpus": actor_num_gpus,
            "num_cpus": model_config.num_cpus_per_replica,
            "runtime_env": build_actor_runtime_env(extra_env_vars),
        },
        max_ongoing_requests=config.replica_max_ongoing_requests,
        health_check_period_s=30,
        health_check_timeout_s=120,
    )
    if placement_group_bundles is not None:
        deployment_options["placement_group_bundles"] = placement_group_bundles
        deployment_options["placement_group_strategy"] = placement_group_strategy
    elif not use_global_planner:
        deployment_options["max_replicas_per_node"] = replicas_per_node
    elif planner_max_replicas_per_node is not None:
        deployment_options["max_replicas_per_node"] = planner_max_replicas_per_node
    if engine == "sglang":
        # A non-None user_config makes Serve await EngineWorker.reconfigure during
        # replica init (pre-healthy), which is where SGLangEngine.warmup() runs
        # (JIT kernel compile). Other engines' warmup is a no-op.
        deployment_options["user_config"] = {"warmup": True}

    deployment = EngineWorker.options(**deployment_options).bind(
        model_id=model_id,
        local_model_path=local_path,
        null_compute=null_compute,
        tensor_parallel_size=model_config.tensor_parallel_size,
        pipeline_parallel_size=model_config.pipeline_parallel_size,
        gpu_memory_utilization=model_config.gpu_memory_utilization,
        max_model_len=model_config.max_model_len,
        enforce_eager=model_config.enforce_eager,
        max_num_seqs=model_config.max_num_seqs,
        collect_stats=getattr(config, "collect_stats", False),
        enable_log_requests=model_config.enable_log_requests,
        engine_name=engine,
    )

    return deployment, model_id


def stage_pp_sharded_models(config: DeploymentConfig) -> Dict[str, str]:
    """Shard-aware PP staging (EXASERVE_PP_SHARD_AWARE). For each PP multi-replica
    model, two-group bcast each PP stage's shards to that stage's node group, so
    every node holds ONLY its stage (~size/PP, fits node-local tmpfs) and only the
    PP seed nodes read the shared store (no read storm). Runs post-ray.init so it
    shares ordered_pp_nodes() with the node-pinned deploy (same node↔stage map).
    Returns {model_id: node-local model path} for the staged models."""
    from . import pp_stage
    from .model_bcast import compile_bcast

    ordered = ordered_pp_nodes()
    hosts = [str(n["hostname"]) for n in ordered]
    bcast_bin = str(compile_bcast())
    staged: Dict[str, str] = {}
    for mc in config.model_configs:
        n_rep = mc.num_replicas or 0
        if mc.pipeline_parallel_size <= 1 or n_rep <= 1:
            continue
        need = n_rep * mc.pipeline_parallel_size
        if len(hosts) < need:
            raise RuntimeError(
                f"shard-aware PP {mc.model_id}: need {need} nodes "
                f"({n_rep} replicas x PP{mc.pipeline_parallel_size}), got {len(hosts)}")
        storage_name = get_model_storage_name(mc.model_id)
        lustre_model = get_model_storage_path(mc.model_id, config.model_storage_path)
        stage_base = os.path.join(str(config.model_storage_path), "_pp_stage", storage_name)
        print(f"[ExaServe] Shard-aware PP staging {mc.model_id}: "
              f"PP{mc.pipeline_parallel_size} x {n_rep} replicas over {need} nodes "
              f"(source {lustre_model})", flush=True)
        pp_stage.stage_pp_sharded(
            str(lustre_model), storage_name, stage_base, str(config.local_stage_path),
            mc.pipeline_parallel_size, hosts[:need], n_rep, bcast_bin)
        staged[mc.model_id] = str(get_model_storage_path(mc.model_id, config.local_stage_path))
    return staged


def _wait_for_apps_running(app_names, timeout_s: float = 3000.0, poll_s: float = 5.0) -> None:
    """Poll serve.status() until every named app reports RUNNING. Used by the
    shard-aware PP path, which submits N deployments non-blocking and then waits
    for them collectively (so replicas come up in parallel)."""
    import time as _time
    deadline = _time.monotonic() + timeout_s
    remaining = set(app_names)
    last_log = 0.0
    while _time.monotonic() < deadline:
        apps = serve.status().applications
        for name in list(remaining):
            a = apps.get(name)
            if a is not None and str(getattr(a, "status", "")).upper().endswith("RUNNING"):
                remaining.discard(name)
            elif a is not None and "DEPLOY_FAILED" in str(getattr(a, "status", "")).upper():
                raise RuntimeError(f"shard-aware PP: app {name} DEPLOY_FAILED")
        if not remaining:
            return
        now = _time.monotonic()
        if now - last_log > 30:
            print(f"[ExaServe] shard-aware deploy: "
                  f"{len(app_names) - len(remaining)}/{len(app_names)} replicas RUNNING...",
                  flush=True)
            last_log = now
        _time.sleep(poll_s)
    raise RuntimeError(
        f"shard-aware PP: {len(remaining)}/{len(app_names)} replica apps not RUNNING "
        f"after {timeout_s}s: {sorted(remaining)}")


def deploy_from_replica_plan(
    config: DeploymentConfig,
    model_path_map: Dict[str, str],
    total_gpus: int,
    replica_plan: DeploymentReplicaPlan,
) -> None:
    """Deploy model services using planner-assigned replica counts."""
    active_plans = replica_plan.active_model_plans
    if not active_plans:
        raise RuntimeError(
            "Replica planner assigned zero replicas to every model; nothing to deploy."
        )

    # PR-023: a config naming N models normally REQUIRES all N. Default to
    # failing the deployment when any declared model cannot be placed; an
    # operator opts into best-effort with EXASERVE_ALLOW_PARTIAL_MODELS=1.
    from .replica_planner import enforce_required_models_policy

    allow_partial = os.environ.get("EXASERVE_ALLOW_PARTIAL_MODELS") == "1"
    skipped_reasons = enforce_required_models_policy(
        replica_plan, allow_partial=allow_partial
    )
    if skipped_reasons and allow_partial:
        print(
            f"[ExaServe] WARNING: serving a SUBSET — {len(skipped_reasons)} model(s) "
            f"could not be placed (EXASERVE_ALLOW_PARTIAL_MODELS=1): "
            + "; ".join(skipped_reasons),
            flush=True,
        )

    use_root_route = len(config.model_configs) == 1
    if not use_root_route:
        print(
            f"[ExaServe] Deploying {len(active_plans)} active planned models "
            "with per-model route_prefix",
            flush=True,
        )

    for model_index, model_plan in enumerate(active_plans):
        model_config = model_plan.model_config
        planner_max_replicas_per_node: Optional[int] = None
        if model_config.pipeline_parallel_size == 1 and model_plan.placements:
            primary_node_counts: Dict[str, int] = {}
            for placement in model_plan.placements:
                primary_node = placement.node_ips[0]
                primary_node_counts[primary_node] = (
                    primary_node_counts.get(primary_node, 0) + 1
                )
            planner_max_replicas_per_node = max(primary_node_counts.values())
        print(
            f"\n[ExaServe] ═══ Deploying planned model {model_index + 1}/{len(active_plans)} ═══",
            flush=True,
        )

        # Shard-aware PP: deploy N node-pinned single-replica deployments (Serve
        # has no per-replica placement), each pinned to replica r's nodes via
        # build_pp_replica_bundles — matching where pp_stage staged its shards.
        # Each replica gets its own route /{safe_name}_r{r}; the external proxy
        # round-robins across them (routing wired separately).
        shard_aware = (
            os.environ.get("EXASERVE_PP_SHARD_AWARE", "0") == "1"
            and model_config.pipeline_parallel_size > 1
            and model_plan.assigned_replicas > 1
        )
        if shard_aware:
            safe_name = get_model_route_name(model_plan.model_config.model_id)
            n_rep = model_plan.assigned_replicas
            # Deploy all N node-pinned replicas CONCURRENTLY. serve.run(blocking=False)
            # submits each app without waiting, so the controller schedules every
            # replica's actors in parallel (vLLM engine init overlaps across nodes).
            # A sequential blocking loop is O(N x per-replica-load) — ~3h at 32
            # replicas, ~12h at 128 — which blows any walltime.
            model_id = model_plan.model_config.model_id
            # Deploy all N node-pinned replicas in ONE controller call so their
            # vLLM engines initialise CONCURRENTLY across nodes. A per-replica
            # serve.run() loop serialises: serve.run -> _run -> _run_many with a
            # SINGLE app + wait_for_applications_running, which blocks ~5-10min per
            # 405B replica -> ~N*10min, blowing the walltime (n64 reached only
            # replica 5 in 1h). _run_many with ALL N RunTargets submits them
            # together and waits for the whole batch to come up in parallel.
            # PR-026: this uses private Serve internals. Guard with an explicit
            # capability check so an unsupported Ray version fails with a clear
            # message instead of a bare ImportError mid-deploy.
            try:
                from ray.serve.api import _run_many, RunTarget
            except ImportError as exc:
                raise RuntimeError(
                    "shard-aware multi-replica PP requires ray.serve.api._run_many "
                    f"/ RunTarget, absent in this Ray ({exc}). Pin the supported "
                    "Ray version (see doc/hardening/COMPATIBILITY_MATRIX.md) or "
                    "avoid multi-replica shard-aware PP."
                ) from exc
            targets = []
            for r in range(n_rep):
                dep, model_id = deploy_model(
                    model_config, model_path_map, total_gpus, config, model_index,
                    use_global_planner=True, pp_replica_index=r,
                )
                targets.append(RunTarget(target=dep, name=f"{safe_name}_r{r}",
                                         route_prefix=f"/{safe_name}_r{r}"))
            print(f"[ExaServe] Deploying {n_rep} shard-aware PP replicas CONCURRENTLY "
                  f"(single _run_many for {n_rep} apps) for {model_id}...", flush=True)
            with tracer.phase("shard_serve.run_many", replicas=n_rep):
                try:
                    _run_many(targets, wait_for_applications_running=True)
                except Exception as _deploy_exc:
                    # FAIL FAST. A replica's deploy failed (e.g. vLLM KV-cache OOM ->
                    # EngineCore crash). _run_many raises, but Ray Serve's background
                    # actors + atexit keep this process alive, so the driver would
                    # wait EXASERVE_SERVE_READY_TIMEOUT_S and burn the ENTIRE walltime.
                    # os._exit bypasses the hanging cleanup; the driver sees the
                    # process die within ~1s (poll()) and aborts the job cleanly so
                    # it can be retried in minutes instead of hours.
                    import traceback
                    print(f"[ExaServe] ✗✗✗ SHARD-AWARE DEPLOY FAILED "
                          f"({n_rep} replicas): {_deploy_exc}", flush=True)
                    traceback.print_exc()
                    sys.stdout.flush()
                    sys.stderr.flush()
                    os._exit(1)
            print(f"[ExaServe] ✓ all {n_rep} replicas running → "
                  f"http://localhost:8000/{safe_name}_r{{0..{n_rep - 1}}}/v1", flush=True)
            # Optional umbrella: a root-route ingress that fans out to the N
            # replica apps, so external proxies (and direct clients) need no
            # shard-aware routing. Default 1 umbrella replica per node.
            if os.environ.get("EXASERVE_PP_UMBRELLA", "0") == "1":
                n_umbrella = int(os.environ.get(
                    "EXASERVE_PP_UMBRELLA_REPLICAS", str(len(ordered_pp_nodes()))))
                with tracer.phase("shard_umbrella.run", replicas=n_umbrella):
                    deploy_shard_umbrella(safe_name, n_rep, 8000, n_umbrella)
            continue

        deployment, model_id = deploy_model(
            model_config,
            model_path_map,
            total_gpus,
            config,
            model_index,
            num_replicas_override=model_plan.assigned_replicas,
            use_global_planner=True,
            planner_max_replicas_per_node=planner_max_replicas_per_node,
        )

        if use_root_route:
            with tracer.phase("serve.run", model_id=model_id, replicas=model_plan.assigned_replicas):
                serve.run(deployment, route_prefix="/")
            print(
                f"[ExaServe] Service available at http://localhost:8000/v1 "
                f"(model: {model_id}, replicas={model_plan.assigned_replicas})",
                flush=True,
            )
            continue

        safe_name = get_model_route_name(model_id)
        route_prefix = f"/{safe_name}"
        with tracer.phase("serve.run", model_id=model_id, replicas=model_plan.assigned_replicas):
            serve.run(deployment, name=safe_name, route_prefix=route_prefix)
        print(
            f"[ExaServe] ✓ {model_id} → http://localhost:8000{route_prefix}/v1 "
            f"(replicas={model_plan.assigned_replicas})",
            flush=True,
        )


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
        f"[ExaServe] Deploying {len(config.model_configs)} models with per-model route_prefix",
        flush=True,
    )
    for idx, model_config in enumerate(config.model_configs):
        print(
            f"\n[ExaServe] ═══ Deploying model {idx + 1}/{len(config.model_configs)} ═══",
            flush=True,
        )
        deployment, model_id = deploy_model(model_config, model_path_map, total_gpus, config, idx)
        safe_name = get_model_route_name(model_id)
        route_prefix = f"/{safe_name}"
        with tracer.phase("serve.run", model_id=model_id):
            serve.run(deployment, name=safe_name, route_prefix=route_prefix)
        print(
            f"[ExaServe] ✓ {model_id} → http://localhost:8000{route_prefix}/v1",
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
        raise SystemExit(f"[ExaServe] Config file not found: {config_path}")

    # ---- Load configuration -------------------------------------------------
    config = load_deployment_config(config_path)
    # Determine Ray Serve proxy placement. The "ray_serve" proxy mode is the
    # out-of-the-box baseline: ProxyLocation.HeadOnly, no external proxy. All
    # other modes (haproxy/litellm/nginx/envoy/pingora/none) keep EveryNode so
    # each node has its own Ray Serve HTTP proxy actor for the external LB to
    # fan out to.
    _proxy_cfg = load_proxy_config(config_path)
    if _proxy_cfg.type == "ray_serve":
        _ray_serve_proxy_location = ProxyLocation.HeadOnly
    else:
        _ray_serve_proxy_location = ProxyLocation.EveryNode
    print(f"[ExaServe] Loaded config from {config_path}: {config.deployment_name}", flush=True)
    print(
        f"[ExaServe] proxy_config.type={_proxy_cfg.type!r} -> "
        f"Ray Serve proxy_location={_ray_serve_proxy_location}",
        flush=True,
    )
    print(f"[ExaServe] Models: {len(config.model_configs)}", flush=True)
    for cfg in config.model_configs:
        print(
            f"  - {cfg.model_id} "
            f"(size={cfg.size}B, TP={cfg.tensor_parallel_size}, PP={cfg.pipeline_parallel_size})",
            flush=True,
        )

    # ---- Serve Init: ray.init + serve.start ----------------------------------
    stage1_start = time.monotonic()
    ray_address = os.environ.get("RAY_ADDRESS", "auto")
    tracer.set_metadata(
        ray_address=ray_address,
        num_nodes=config.num_nodes,
        num_gpus_per_node=config.num_gpus_per_node,
        models=[cfg.model_id for cfg in config.model_configs],
    )
    # Record model broadcast timing from launch_cluster.sh (passed via env var)
    bcast_timing_raw = os.environ.get("EXASERVE_MODEL_BCAST_TIMING", "").strip()
    if bcast_timing_raw:
        try:
            bcast_timing = json.loads(bcast_timing_raw)
            tracer.record_phase(
                "model_bcast", bcast_timing["model_bcast_total_s"],
                models=bcast_timing.get("models", []),
            )
        except (json.JSONDecodeError, KeyError):
            pass

    print(
        f"[ExaServe] Serve Init: Connecting to Ray cluster at {ray_address}...",
        flush=True,
    )

    with tracer.phase("ray.init"):
        init_ray_cluster(ray_address, namespace="serve", include_dashboard=False)

    # Create stats collector actor for per-replica init timing (replaces
    # per-file Lustre I/O).  Must be created after ray.init, before serve.run.
    from .scaling_trace import create_stats_collector, collect_replica_stats
    create_stats_collector()

    _verify_core_env()

    # Patch proxy timeouts in *this* (driver) process before serve.start()
    # spawns ProxyActors. The runtime_env worker hook covers Ray workers, but
    # the driver imports ray.serve directly and needs its own patch.
    from ray.serve._private import constants as _serve_constants
    _serve_patches = _ray_serve_timeout_patches()
    for _attr, _val in _serve_patches.items():
        setattr(_serve_constants, _attr, _val)
    # Also patch modules that imported constants by name.
    import sys as _sys
    for _mod_name in list(_sys.modules):
        if "ray.serve" in _mod_name:
            _mod = _sys.modules[_mod_name]
            for _attr, _val in _serve_patches.items():
                if hasattr(_mod, _attr):
                    setattr(_mod, _attr, _val)
    print(
        "[ExaServe] Ray Serve timeouts patched: "
        f"HTTP_PROXY_TIMEOUT={_serve_patches['HTTP_PROXY_TIMEOUT']}s, "
        f"PROXY_READY_CHECK_TIMEOUT={_serve_patches['PROXY_READY_CHECK_TIMEOUT_S']}s, "
        f"PROXY_HEALTH_CHECK_TIMEOUT={_serve_patches['PROXY_HEALTH_CHECK_TIMEOUT_S']}s, "
        f"UNHEALTHY_THRESHOLD={_serve_patches['PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD']}",
        flush=True,
    )

    with tracer.phase("serve.start", proxy_location=str(_ray_serve_proxy_location)):
        serve.start(
            http_options=HTTPOptions(
                host="0.0.0.0",
                location=_ray_serve_proxy_location,
                port=8000,
            )
        )
    print(
        f"[ExaServe] HTTP proxy location: {_ray_serve_proxy_location}, "
        f"host=0.0.0.0, port=8000",
        flush=True,
    )
    tracer.record_phase("stage1.total", time.monotonic() - stage1_start)
    print_red(f"[ExaServe] ✓ Serve Init completed in {time.monotonic() - stage1_start:.2f}s")

    # ---- GPU Poll: wait for all nodes to register --------------------------
    expected_gpus = config.num_gpus_per_node * config.num_nodes
    deadline = time.monotonic() + 600  # 10 min max wait
    total_gpus = 0
    poll_iteration = 0
    with tracer.phase("node_registration_poll", expected_gpus=expected_gpus):
        while time.monotonic() < deadline:
            iter_start = time.monotonic()

            resources = tracer.timed_call("ray.cluster_resources", ray.cluster_resources)
            total_gpus = int(resources.get("GPU", 0))

            nodes_result = tracer.timed_call("ray.nodes", ray.nodes)
            alive_nodes = sum(1 for n in nodes_result if n.get("Alive"))

            iter_elapsed = time.monotonic() - iter_start
            tracer.record_poll_iteration(
                "node_registration",
                poll_iteration,
                elapsed_s=iter_elapsed,
                alive_nodes=alive_nodes,
                total_gpus=total_gpus,
                expected_gpus=expected_gpus,
                pct=round(total_gpus / max(expected_gpus, 1) * 100, 1),
            )

            if total_gpus >= expected_gpus:
                break

            print(
                f"[ExaServe] Waiting for nodes: {alive_nodes} alive, "
                f"{total_gpus}/{expected_gpus} GPUs ({total_gpus/max(expected_gpus,1)*100:.0f}%)",
                flush=True,
            )
            poll_iteration += 1
            time.sleep(15)

    tracer.set_metadata(actual_gpus=total_gpus, alive_nodes=alive_nodes)
    if total_gpus < expected_gpus:
        # PR-008: fail CLOSED. Declaring a degraded cluster "ready" is how the
        # 256-node false-ready incident happened (KNOWN_ISSUES D1). An operator
        # may explicitly opt into degraded startup with
        # EXASERVE_ALLOW_DEGRADED_GPUS=1, which is recorded in the trace.
        allow_degraded = os.environ.get("EXASERVE_ALLOW_DEGRADED_GPUS") == "1"
        tracer.set_metadata(degraded_gpus=True, degraded_allowed=allow_degraded)
        message = (
            f"Only {total_gpus}/{expected_gpus} GPUs registered after the "
            f"{600}s deadline ({total_gpus/max(expected_gpus,1)*100:.0f}%)."
        )
        if not allow_degraded:
            raise RuntimeError(
                f"[ExaServe] {message} Refusing to declare the cluster ready "
                "with missing resources (set EXASERVE_ALLOW_DEGRADED_GPUS=1 to "
                "start in an explicitly degraded mode)."
            )
        print(
            f"[ExaServe] WARNING: {message} Proceeding in EXPLICITLY DEGRADED "
            "mode (EXASERVE_ALLOW_DEGRADED_GPUS=1).",
            flush=True,
        )
    else:
        print(
            f"[ExaServe] All {total_gpus}/{expected_gpus} GPUs registered",
            flush=True,
        )

    # Write per-node Ray IPs (NodeManagerAddress) next to the config so the eval
    # backend's direct-mode discover_targets can reach the per-node Ray Serve proxy
    # by IP. The PBS .hsn. FQDN resolves to an address whose :8000 returns 503;
    # only the Ray-bound IP serves /health=200. See discover_targets in eval/lib/backends/ray.py.
    try:
        _ray_ips = [n["ip"] for n in get_alive_ray_gpu_nodes() if n.get("ip")]
        _ips_path = os.path.join(os.path.dirname(config_path), "ray_node_ips.txt")
        with open(_ips_path, "w", encoding="utf-8") as _fh:
            _fh.write("\n".join(_ray_ips) + "\n")
        print(f"[ExaServe] Wrote {len(_ray_ips)} Ray node IP(s) -> {_ips_path}", flush=True)
    except Exception as _e:  # noqa: BLE001
        print(f"[ExaServe] WARNING: failed to write ray_node_ips.txt: {_e}", flush=True)

    # ---- Model Resolution: resolve staged local models ---------------------
    null_compute = os.environ.get("EXASERVE_NULL_COMPUTE", "0") == "1"
    if null_compute:
        print(
            "[ExaServe] Model Resolution: NULL-COMPUTE mode — model staging skipped",
            flush=True,
        )
        model_path_map = {cfg.model_id: cfg.model_id for cfg in config.model_configs}
    else:
        # Shard-aware PP models are staged per-stage HERE (post-ray.init); their
        # per-node dirs are intentionally PARTIAL, so they bypass resolve_model_paths
        # (which requires a complete model on every node).
        shard_models: Dict[str, str] = {}
        if os.environ.get("EXASERVE_PP_SHARD_AWARE", "0") == "1":
            with tracer.phase("pp_shard_stage"):
                shard_models = stage_pp_sharded_models(config)
        stage_start = time.time()
        print(
            f"[ExaServe] Model Resolution: Resolving staged models from {config.local_stage_path}...",
            flush=True,
        )
        model_path_map = dict(shard_models)
        non_shard = [cfg for cfg in config.model_configs if cfg.model_id not in shard_models]
        if non_shard:
            model_path_map.update(resolve_model_paths(
                non_shard,
                config.local_stage_path,
                require_complete=True,
            ))
        print_red(
            f"[ExaServe] ✓ Model Resolution completed in "
            f"{time.time() - stage_start:.2f}s"
        )

    planner_enabled = should_use_global_planner(config)

    # ---- Model Deploy: deploy model services to Ray Serve -------------------
    stage3_start = time.monotonic()
    print("[ExaServe] Model Deploy: Deploying model services to Ray Serve...", flush=True)

    # Diagnostic: print effective health check constants in this process
    try:
        from ray.serve._private import constants as _diag_c
        print(
            f"[ExaServe] DIAG health check constants in driver process:\n"
            f"  HTTP_PROXY_TIMEOUT            = {getattr(_diag_c, 'HTTP_PROXY_TIMEOUT', '?')}\n"
            f"  PROXY_HEALTH_CHECK_TIMEOUT_S   = {getattr(_diag_c, 'PROXY_HEALTH_CHECK_TIMEOUT_S', '?')}\n"
            f"  PROXY_HEALTH_CHECK_UNHEALTHY   = {getattr(_diag_c, 'PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD', '?')}\n"
            f"  DEFAULT_HEALTH_CHECK_TIMEOUT_S = {getattr(_diag_c, 'DEFAULT_HEALTH_CHECK_TIMEOUT_S', '?')}\n"
            f"  DEFAULT_HEALTH_CHECK_PERIOD_S  = {getattr(_diag_c, 'DEFAULT_HEALTH_CHECK_PERIOD_S', '?')}\n"
            f"  REPLICA_HEALTH_CHECK_UNHEALTHY = {getattr(_diag_c, 'REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD', '?')}",
            flush=True,
        )
    except Exception as _diag_e:
        print(f"[ExaServe] DIAG constants import failed: {_diag_e}", flush=True)

    if planner_enabled:
        with tracer.phase("build_node_inventory"):
            planner_nodes = build_node_inventory()
        if not planner_nodes:
            raise RuntimeError("No alive Ray GPU nodes found for replica planning")
        with tracer.phase("compute_replica_plan"):
            replica_plan = compute_replica_plan(config.model_configs, planner_nodes)
        print(format_replica_plan(replica_plan), flush=True)
        with tracer.phase("deploy_from_replica_plan"):
            deploy_from_replica_plan(config, model_path_map, total_gpus, replica_plan)
    elif len(config.model_configs) == 1:
        primary_config = config.model_configs[0]
        with tracer.phase("deploy_model.build", model_id=primary_config.model_id):
            deployment, model_id = deploy_model(primary_config, model_path_map, total_gpus, config)

        # Monitor deployment progress + proxy spawning timeline in background
        import threading
        _deploy_done = threading.Event()
        _proxy_spawn_log = []  # list of (wall_time, num_proxies, num_replicas_running)
        _monitor_start = time.time()
        _monitor_error_logged = {"value": False}
        def _monitor_deploy():
            prev_proxy_count = -1
            while not _deploy_done.is_set():
                _deploy_done.wait(timeout=5)
                if _deploy_done.is_set():
                    break
                try:
                    # Query controller directly instead of serve.status()
                    import ray as _mray
                    proxy_handles = _mray.get(
                        _mray.get_actor("SERVE_CONTROLLER_ACTOR", namespace="serve")
                        .get_proxies.remote()
                    )
                    n_proxies = len(proxy_handles)
                    elapsed = time.time() - _monitor_start
                    _proxy_spawn_log.append({
                        "elapsed_s": round(elapsed, 2),
                        "proxies": n_proxies,
                    })
                    if n_proxies != prev_proxy_count:
                        print(
                            f"[ExaServe] Deploy monitor: proxies={n_proxies} (+{elapsed:.0f}s)",
                            flush=True,
                        )
                        prev_proxy_count = n_proxies
                except Exception as e:
                    if not _monitor_error_logged["value"]:
                        print(f"[ExaServe] Deploy monitor error: {e}", flush=True)
                        _monitor_error_logged["value"] = True
        monitor = threading.Thread(target=_monitor_deploy, daemon=True)
        monitor.start()

        # Decompose serve.run() into its two blocking phases for timing.
        # serve.run() internally calls: deploy_applications(wait=True) + wait_for_proxies_serving()
        # PR-026: capability-guard the private-API import path.
        try:
            from ray.serve._private.api import serve_start as _serve_start
            from ray.serve.api import build_app
            from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME
        except ImportError as exc:
            raise RuntimeError(
                "the instrumented deploy path uses private Serve APIs "
                f"(serve_start/build_app/constants) absent in this Ray ({exc}); "
                "pin the supported Ray version — see "
                "doc/hardening/COMPATIBILITY_MATRIX.md."
            ) from exc

        # _serve_start accepts either a ProxyLocation enum or the underlying
        # string ("EveryNode" / "HeadOnly" / ...). ProxyLocation is a str-enum
        # so .value is the canonical name.
        client = _serve_start(
            http_options={"location": _ray_serve_proxy_location.value},
            global_logging_config=None,
        )
        built = build_app(deployment, name=SERVE_DEFAULT_APP_NAME, route_prefix="/")

        # --- Decomposed deploy with per-step timing ---
        # Monkey-patch the client to add timing around each internal step.
        _orig_deploy = client.deploy_applications.__wrapped__ if hasattr(client.deploy_applications, '__wrapped__') else None

        _deploy_step_times = {}

        def _timed_deploy_applications(built_apps, **kwargs):
            import ray as _ray
            from ray.serve._private.deploy_utils import get_deploy_args
            from ray.serve._private.utils import get_random_string
            from ray.serve.generated.serve_pb2 import DeploymentArgs, ApplicationArgs

            # Step 1: Build and submit to controller
            t0 = time.monotonic()
            name_to_deployment_args_list = {}
            name_to_application_args = {}
            for app in built_apps:
                deployment_args_list = []
                for dep in app.deployments:
                    if dep.logging_config is None and app.logging_config:
                        dep = dep.options(logging_config=app.logging_config)
                    is_ingress = dep.name == app.ingress_deployment_name
                    da = get_deploy_args(
                        dep.name, ingress=is_ingress,
                        replica_config=dep._replica_config,
                        deployment_config=dep._deployment_config,
                        version=dep._version or get_random_string(),
                        route_prefix=app.route_prefix if is_ingress else None,
                    )
                    dap = DeploymentArgs()
                    dap.deployment_name = da["deployment_name"]
                    dap.deployment_config = da["deployment_config_proto_bytes"]
                    dap.replica_config = da["replica_config_proto_bytes"]
                    dap.deployer_job_id = da["deployer_job_id"]
                    if da["route_prefix"]:
                        dap.route_prefix = da["route_prefix"]
                    dap.ingress = da["ingress"]
                    deployment_args_list.append(dap.SerializeToString())
                aap = ApplicationArgs()
                aap.external_scaler_enabled = app.external_scaler_enabled
                name_to_deployment_args_list[app.name] = deployment_args_list
                name_to_application_args[app.name] = aap.SerializeToString()
            client._check_ingress_deployments(built_apps)
            _ray.get(client._controller.deploy_applications.remote(
                name_to_deployment_args_list, name_to_application_args
            ))
            _deploy_step_times["submit_to_controller"] = round(time.monotonic() - t0, 4)
            print(f"[ExaServe] Step 1 submit_to_controller: {_deploy_step_times['submit_to_controller']}s", flush=True)

            # Step 2: Wait for deployment created
            t0 = time.monotonic()
            for app in built_apps:
                client._wait_for_deployment_created(app.ingress_deployment_name, app.name)
            _deploy_step_times["wait_deployment_created"] = round(time.monotonic() - t0, 4)
            print(f"[ExaServe] Step 2 wait_deployment_created: {_deploy_step_times['wait_deployment_created']}s", flush=True)

            # Step 3: Wait for application RUNNING
            t0 = time.monotonic()
            for app in built_apps:
                client._wait_for_application_running(app.name)
            _deploy_step_times["wait_app_running"] = round(time.monotonic() - t0, 4)
            print(f"[ExaServe] Step 3 wait_app_running: {_deploy_step_times['wait_app_running']}s", flush=True)

            return [client.get_handle(app.ingress_deployment_name, app.name, check_exists=False) for app in built_apps]

        with tracer.phase("serve.run.deploy_apps", model_id=model_id):
            try:
                _timed_deploy_applications([built])
            finally:
                _deploy_done.set()
                monitor.join(timeout=2)

        # Record sub-steps as phases
        for step_name, step_dur in _deploy_step_times.items():
            tracer.record_phase(f"serve.run.deploy.{step_name}", step_dur)

        # Step 4: Wait for proxies serving (with per-proxy timing)
        import ray as _ray
        t0 = time.monotonic()
        proxy_handles = _ray.get(client._controller.get_proxies.remote())
        t_get_proxies = time.monotonic() - t0
        print(f"[ExaServe] Step 4a get_proxies: {t_get_proxies:.3f}s ({len(proxy_handles)} proxies)", flush=True)

        t0 = time.monotonic()
        serving_refs = [h.serving.remote(wait_for_applications_running=True) for h in proxy_handles.values()]
        t_issue = time.monotonic() - t0
        print(f"[ExaServe] Step 4b issue .serving.remote() x{len(serving_refs)}: {t_issue:.3f}s", flush=True)

        t0 = time.monotonic()
        # Track when each proxy finishes
        remaining = list(serving_refs)
        proxy_complete_times = []
        wait_start = time.monotonic()
        # PR-008: bound the overall wait. The old loop had only a per-wait 5s
        # timeout and could hang forever on a stuck proxy. A proxy that never
        # reports serving is a fail-closed readiness error.
        proxy_deadline_s = float(os.environ.get("EXASERVE_PROXY_READY_DEADLINE_S", "900"))
        while remaining:
            done, remaining = _ray.wait(remaining, num_returns=1, timeout=5.0)
            elapsed = time.monotonic() - wait_start
            if done:
                proxy_complete_times.append(elapsed)
                if len(proxy_complete_times) % 10 == 0 or not remaining:
                    print(f"[ExaServe] Step 4c proxies ready: {len(proxy_complete_times)}/{len(serving_refs)} at +{elapsed:.1f}s", flush=True)
            elif elapsed > proxy_deadline_s:
                raise RuntimeError(
                    f"[ExaServe] {len(remaining)}/{len(serving_refs)} proxies did "
                    f"not report serving within {proxy_deadline_s:.0f}s; failing "
                    "closed rather than declaring readiness with unhealthy proxies."
                )
        t_wait = time.monotonic() - t0
        tracer.record_phase("serve.run.wait_proxies", t_wait)

        # Log proxy completion distribution
        if proxy_complete_times:
            import statistics
            print(
                f"[ExaServe] Step 4 wait_proxies: {t_wait:.1f}s total, "
                f"proxy completion: first={proxy_complete_times[0]:.1f}s "
                f"median={statistics.median(proxy_complete_times):.1f}s "
                f"p90={proxy_complete_times[int(len(proxy_complete_times)*0.9)]:.1f}s "
                f"last={proxy_complete_times[-1]:.1f}s",
                flush=True,
            )
            tracer.set_metadata(
                proxy_wait_first_s=round(proxy_complete_times[0], 2),
                proxy_wait_median_s=round(statistics.median(proxy_complete_times), 2),
                proxy_wait_last_s=round(proxy_complete_times[-1], 2),
            )
        # Log proxy spawn timeline from monitor thread
        if _proxy_spawn_log:
            tracer.set_metadata(proxy_spawn_timeline=_proxy_spawn_log)
            # Print summary: when did we first see N proxies?
            proxy_milestones = {}
            for entry in _proxy_spawn_log:
                n = entry["proxies"]
                if n not in proxy_milestones:
                    proxy_milestones[n] = entry["elapsed_s"]
            print(f"[ExaServe] Proxy spawn timeline: {proxy_milestones}", flush=True)

        # Collect per-proxy status from serve.status(). status.proxies maps
        # node_id -> ProxyStatus (an ENUM: HEALTHY/UNHEALTHY/STARTING/DRAINING/
        # DRAINED) in current Ray; some versions expose a details object with a
        # .status field instead. Handle both, and SURFACE any non-healthy proxy
        # (previously this swallowed an AttributeError and reported nothing, so a
        # degraded proxy at deploy time was invisible).
        # PR-008: proxy health is now a READINESS PREDICATE, not a warning. An
        # unhealthy proxy, or a failure to even collect proxy status, fails
        # closed unless degraded mode is explicitly requested.
        _proxy_health_ok = False
        _proxy_health_detail = ""
        try:
            from collections import Counter
            status = serve.status()
            proxy_status_list = []
            for node_id, proxy in status.proxies.items():
                ps = getattr(proxy, "status", proxy)         # details.status or the enum itself
                ps_str = getattr(ps, "name", None) or getattr(ps, "value", None) or str(ps)
                proxy_status_list.append({"node_id": node_id, "status": str(ps_str)})
            tracer.set_metadata(proxy_statuses=proxy_status_list)
            counts = Counter(d["status"] for d in proxy_status_list)
            n_total = len(proxy_status_list)
            n_healthy = sum(v for k, v in counts.items() if "HEALTHY" in k.upper() and "UN" not in k.upper())
            print(f"[ExaServe] Proxy statuses: {n_total} proxies, "
                  f"{n_healthy} healthy — {dict(counts)}", flush=True)
            _proxy_health_ok = n_total > 0 and n_healthy == n_total
            _proxy_health_detail = f"{n_healthy}/{n_total} healthy: {dict(counts)}"
        except Exception as e:
            import traceback
            _proxy_health_detail = f"could not collect proxy status: {e}"
            print(f"[ExaServe] Failed to collect proxy statuses: {e}", flush=True)
            traceback.print_exc()

        if not _proxy_health_ok:
            allow_degraded = os.environ.get("EXASERVE_ALLOW_DEGRADED_PROXIES") == "1"
            tracer.set_metadata(degraded_proxies=True, degraded_proxies_allowed=allow_degraded)
            if not allow_degraded:
                raise RuntimeError(
                    f"[ExaServe] proxy readiness not satisfied ({_proxy_health_detail}); "
                    "refusing to declare CLUSTER FULLY READY. Set "
                    "EXASERVE_ALLOW_DEGRADED_PROXIES=1 to start in degraded mode."
                )
            print(f"[ExaServe] WARNING: proceeding with degraded proxies "
                  f"({_proxy_health_detail}); EXASERVE_ALLOW_DEGRADED_PROXIES=1.",
                  flush=True)

        # When instrumentation is on, gather /tmp/exaserve_inst from every node
        # to Lustre once. No-op for clean Ray installs.
        _collect_instrumentation_all()

        print(f"[ExaServe] Service available at http://localhost:8000/v1 (model: {model_id})", flush=True)
    else:
        with tracer.phase("deploy_multi_model"):
            deploy_multi_model(config, model_path_map, total_gpus)

    tracer.record_phase("stage3.total", time.monotonic() - stage3_start)
    print_red(
        f"[ExaServe] ✓ Model Deploy completed in {time.monotonic() - stage3_start:.2f}s"
    )

    # ---- Collect per-replica stats via Ray actor (no filesystem I/O) ---------
    replica_stats = collect_replica_stats()

    for rs in replica_stats:
        tracer.record_replica_init(rs)
    if replica_stats:
        has_sub = sum(1 for rs in replica_stats if "engine_sub_phases" in rs)
        print(
            f"[ExaServe] Collected {len(replica_stats)} replica init stats "
            f"(engine_create avg={sum(r.get('engine_create_s', 0) for r in replica_stats)/len(replica_stats):.1f}s, "
            f"sub-phases: {has_sub}/{len(replica_stats)})",
            flush=True,
        )

    # ---- All stages complete ------------------------------------------------
    total_time = time.time() - overall_start
    tracer.set_metadata(total_time_s=round(total_time, 4))
    trace_path = tracer.save()
    print_red(
        f"[ExaServe] ✓✓✓ CLUSTER FULLY READY ✓✓✓ Total time: {total_time:.2f}s"
    )
    print(f"[ExaServe] Scaling trace: {trace_path}", flush=True)

    # PR-028: the driver (and PBS) deliver SIGTERM; without a handler the
    # process dies mid-request with no Serve shutdown. Convert both signals
    # into one orderly drain path with a bounded forced-cleanup deadline.
    import signal as _signal
    import threading

    _shutdown_requested = threading.Event()

    def _request_shutdown(signum, frame):  # noqa: ARG001
        _shutdown_requested.set()

    _signal.signal(_signal.SIGTERM, _request_shutdown)
    _signal.signal(_signal.SIGINT, _request_shutdown)

    try:
        while not _shutdown_requested.wait(timeout=10):
            pass
    except KeyboardInterrupt:
        pass
    print("[ExaServe] Shutdown requested; draining Ray Serve...", flush=True)
    _deadline = time.monotonic() + 60.0

    def _forced_exit():
        remaining = _deadline - time.monotonic()
        if remaining > 0:
            time.sleep(remaining)
        print("[ExaServe] Forced-cleanup deadline reached; exiting.", flush=True)
        os._exit(143)

    _killer = threading.Thread(target=_forced_exit, daemon=True)
    _killer.start()
    try:
        serve.shutdown()
        print("[ExaServe] Ray Serve shut down cleanly.", flush=True)
    except Exception as exc:
        print(f"[ExaServe] Serve shutdown error (continuing): {exc}", flush=True)
