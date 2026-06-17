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
import time
import uuid
from typing import Optional, List, Dict, Any

# Patches MUST be installed before any ray.serve / vllm imports below —
# Ray Serve instantiates classes from ray.serve._private during package
# import, so monkey-patching after the fact is too late. The launcher
# arranges PYTHONPATH so the overlay tree wins import precedence; the
# vLLM monkey-patches in aurora_rayserver._sitecustomize are applied here.
# NOTE: this covers the server and Serve replica processes only. The vLLM
# EngineCore (a multiprocessing-spawn child) never imports this package;
# for PP it gets the patches via the PYTHONPATH sitecustomize shim written
# in VLLMWorker.__init__.
from .patches import apply_all as _apply_all  # noqa: I001
_apply_all()

import ray
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from ray import serve
from ray.serve.config import HTTPOptions, ProxyLocation
from vllm import SamplingParams
from vllm.engine.arg_utils import AsyncEngineArgs
from vllm.engine.async_llm_engine import AsyncLLMEngine

from .schemas import ModelConfig, DeploymentConfig, load_deployment_config, load_proxy_config
from .model_paths import get_model_route_name
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
    return os.environ.get("AURORA_INSTRUMENTATION", "0") == "1"


def _collect_instrumentation_all() -> None:
    """Gather /tmp/aurora_inst/* from every node onto Lustre once at the end
    of startup. Only runs when AURORA_INSTRUMENTATION=1 — the overlay probes
    that produce these files are also gated on that flag, so on a clean Ray
    install this is a no-op.
    """
    if not _instrumentation_enabled():
        return
    run_log = os.environ.get("AURORA_RUN_LOG_DIR")
    if not run_log:
        print("[AuroraServe] No AURORA_RUN_LOG_DIR; skipping instrumentation gather", flush=True)
        return

    @ray.remote(num_cpus=0)
    def _read_node_inst():
        import glob, os, socket
        host = socket.gethostname()
        out = {"hostname": host, "files": {}}
        for path in glob.glob("/tmp/aurora_inst/*"):
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
        print(f"[AuroraServe] Instrumentation gather failed: {e}", flush=True)
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
        f"[AuroraServe] Instrumentation gather: {total_files} files "
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
        "AURORA_VLLM_PATCH_PP_LAYER_FILTER",
        "AURORA_VLLM_PATCH_VERBOSE",
        "AURORA_VLLM_DISABLE_RAY_COMPILED_DAG",
        "AURORA_VLLM_FORCE_RAY_CHANNEL_TYPE",
        "RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR",
        "VLLM_USE_RAY_COMPILED_DAG_CHANNEL_TYPE",
        "VLLM_USE_RAY_WRAPPED_PP_COMM",
        "ZE_FLAT_DEVICE_HIERARCHY",
        "VLLM_TARGET_DEVICE",
        "AURORA_SCALING_TRACE",
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
            for req in getattr(iteration_stats, "finished_requests", []):
                self.finished_requests.append({
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
        fr = self.finished_requests
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
        run = [s.get("running", 0) for s in self.scheduler_snapshots]
        kv = [s.get("kv_cache_usage", 0.0) for s in self.scheduler_snapshots]

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
        fr = self.finished_requests
        if not fr:
            return []
        step = max(1, len(fr) // cap)
        out = []
        for r in fr[::step][:cap]:
            n = int(r.get("num_generation_tokens", 0) or 0)
            d = float(r.get("decode_time", 0.0) or 0.0)
            out.append({
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
        max_num_seqs: int = None,
        collect_stats: bool = False,
    ):
        init_start = time.time()
        init_mono = time.monotonic()
        pid = os.getpid()
        hostname = socket.gethostname()
        self.model_id = model_id
        self.null_compute = null_compute
        self.stats_collector = None
        self._collect_stats = collect_stats

        gpu_ids = [int(gpu_id) for gpu_id in ray.get_gpu_ids()]
        device_id = gpu_ids[0] if gpu_ids else 0

        if null_compute:
            self.latency = float(os.environ.get("AURORA_NULL_COMPUTE_LATENCY", "1.0"))
            print(
                f"[VLLMWorker pid={pid}] NullCompute mode on tile {device_id} "
                f"(latency={self.latency:.2f}s, no vLLM engine)",
                flush=True,
            )
            total_s = time.time() - init_start
            print_red(f"[VLLMWorker pid={pid}] ★ INIT TOTAL: {total_s:.2f}s ★")
            from .scaling_trace import report_replica_stats
            report_replica_stats({
                "pid": pid, "hostname": hostname, "model_id": model_id,
                "device_id": device_id, "null_compute": True,
                "total_init_s": round(total_s, 4),
            })
            return

        # ---- Device isolation ------------------------------------------------
        t0 = time.monotonic()
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
        device_isolation_s = time.monotonic() - t0

        # ---- Distributed init port -------------------------------------------
        t0 = time.monotonic()
        master_addr = "127.0.0.1"
        bind_host = "127.0.0.1"
        if pipeline_parallel_size > 1:
            if not async_engine_arg_supported("pipeline_parallel_size"):
                raise RuntimeError(
                    "Installed vLLM build does not expose pipeline_parallel_size on "
                    "AsyncEngineArgs. Validate the Aurora runtime before using PP."
                )
            # Unconditional: the VLLM_TARGET_DEVICE gate silently skipped this
            # when the env var was absent, leaving the compiled DAG enabled —
            # which crashes XPU workers in Ray's accelerator context at first
            # inference. We only deploy on XPU.
            os.environ.setdefault("AURORA_VLLM_DISABLE_RAY_COMPILED_DAG", "1")
            os.environ.setdefault("AURORA_VLLM_FORCE_RAY_CHANNEL_TYPE", "auto")
            # The EngineCore is a multiprocessing-spawn child that never
            # imports aurora_rayserver, so the vLLM executor patches in
            # _sitecustomize (incl. the uncompiled-PP fallback these env vars
            # select) are otherwise inert exactly where the Ray executor
            # lives. Prepend a node-local sitecustomize shim so the spawned
            # interpreter applies them at startup.
            shim_dir = "/tmp/aurora_pp_shim"
            try:
                os.makedirs(shim_dir, exist_ok=True)
                shim_path = os.path.join(shim_dir, "sitecustomize.py")
                if not os.path.exists(shim_path):
                    with open(shim_path, "w", encoding="utf-8") as shim_fh:
                        shim_fh.write(
                            "try:\n"
                            "    import aurora_rayserver._sitecustomize  # noqa: F401\n"
                            "except Exception:\n"
                            "    pass\n"
                        )
                existing_pp = os.environ.get("PYTHONPATH", "")
                if shim_dir not in existing_pp.split(os.pathsep):
                    os.environ["PYTHONPATH"] = (
                        shim_dir + (os.pathsep + existing_pp if existing_pp else "")
                    )
            except OSError as shim_exc:
                print(
                    f"[VLLMWorker pid={pid}] WARNING: PP sitecustomize shim "
                    f"setup failed: {shim_exc}",
                    flush=True,
                )
            master_addr = get_ray_node_ip() or get_hsn_ip()
            bind_host = "0.0.0.0"
            os.environ["VLLM_HOST_IP"] = master_addr

        port = get_open_port(23000 + device_id * 100, bind_host=bind_host)
        if port is None:
            raise RuntimeError(f"No free port for distributed init (device {device_id})")
        os.environ["MASTER_ADDR"] = master_addr
        os.environ["MASTER_PORT"] = str(port)
        dist_setup_s = time.monotonic() - t0
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
        if max_num_seqs is not None:
            engine_kwargs["max_num_seqs"] = max_num_seqs
        if pipeline_parallel_size > 1:
            engine_kwargs["pipeline_parallel_size"] = pipeline_parallel_size
            engine_kwargs["distributed_executor_backend"] = "ray"

        # -- Sub-phase: AsyncEngineArgs construction --
        t0 = time.monotonic()
        engine_args = AsyncEngineArgs(**engine_kwargs)
        if not hasattr(engine_args, "enable_log_requests"):
            engine_args.enable_log_requests = True
        engine_args_s = time.monotonic() - t0

        print(f"[VLLMWorker pid={pid}] Creating vLLM engine for {model_id}...", flush=True)

        # -- Sub-phase: engine creation (weight loading + GPU init + KV cache) --
        engine_start = time.monotonic()
        extra_engine_kwargs = {}
        if self._collect_stats:
            extra_engine_kwargs["stat_loggers"] = [CollectingStatLogger]
        self.engine = AsyncLLMEngine.from_engine_args(engine_args, **extra_engine_kwargs)
        engine_create_s = time.monotonic() - engine_start
        print_red(f"[VLLMWorker pid={pid}] Engine creation: {engine_create_s:.2f}s")

        if self._collect_stats:
            self.stats_collector = CollectingStatLogger.get_instance()
            if self.stats_collector is not None:
                print(f"[VLLMWorker pid={pid}] Stats collection enabled", flush=True)
            else:
                print(f"[VLLMWorker pid={pid}] WARNING: CollectingStatLogger not instantiated by engine", flush=True)

        total_s = time.monotonic() - init_mono
        print_red(f"[VLLMWorker pid={pid}] ★ INIT TOTAL: {total_s:.2f}s ★")

        # -- Record replica init breakdown for trace --
        replica_info = {
            "pid": pid,
            "hostname": hostname,
            "model_id": model_id,
            "device_id": device_id,
            "gpu_ids": gpu_ids,
            "null_compute": False,
            "tensor_parallel_size": tensor_parallel_size,
            "pipeline_parallel_size": pipeline_parallel_size,
            "total_init_s": round(total_s, 4),
            "device_isolation_s": round(device_isolation_s, 4),
            "dist_setup_s": round(dist_setup_s, 4),
            "engine_args_s": round(engine_args_s, 4),
            "engine_create_s": round(engine_create_s, 4),
            "wall_start": init_start,
            "wall_end": time.time(),
        }
        # Parse EngineCore sub-phase timings from this replica's own Ray log.
        # from_engine_args() blocks until model loading completes, so the
        # log lines exist by this point. Each replica reads its own log
        # (local /tmp), giving per-replica, per-GPU attribution.
        engine_sub = _parse_own_engine_log(pid)
        if engine_sub:
            replica_info["engine_sub_phases"] = engine_sub

        from .scaling_trace import report_replica_stats
        report_replica_stats(replica_info)

    # ---- HTTP endpoints ------------------------------------------------------

    @app.get("/health")
    async def health_check(self):
        return JSONResponse({"status": "healthy", "model": self.model_id})

    @app.get("/stats")
    async def stats(self):
        """Per-replica live stats from CollectingStatLogger (if enabled) or basic info."""
        pid = os.getpid()
        if self.null_compute:
            return JSONResponse({"pid": pid, "model": self.model_id, "null_compute": True})
        result = {"pid": pid, "model": self.model_id}
        collector = CollectingStatLogger.get_instance()
        if collector is not None:
            snaps = collector.scheduler_snapshots
            reqs = collector.finished_requests
            result["latest_scheduler"] = snaps[-1] if snaps else None
            result["total_finished_requests"] = len(reqs)
            result["scheduler_snapshot_count"] = len(snaps)
        return JSONResponse(result)

    def collect_stats(self) -> dict:
        """Return buffered stats. Called via ray.get(actor_handle.collect_stats.remote())."""
        pid = os.getpid()
        if self.stats_collector is None:
            return {"pid": pid, "model": self.model_id, "error": "stats collection not enabled"}
        data = self.stats_collector.to_dict()
        data["pid"] = pid
        data["model"] = self.model_id
        reqs = data["finished_requests"]
        snaps = data["scheduler_snapshots"]
        data["summary"] = {
            "total_requests": len(reqs),
            "mean_batch_size": sum(s["running"] for s in snaps) / max(len(snaps), 1),
            "max_batch_size": max((s["running"] for s in snaps), default=0),
            "mean_e2e_latency": sum(r["e2e_latency"] for r in reqs) / max(len(reqs), 1),
            "mean_queued_time": sum(r["queued_time"] for r in reqs) / max(len(reqs), 1),
            "mean_prefill_time": sum(r["prefill_time"] for r in reqs) / max(len(reqs), 1),
            "kv_cache_peak": max((s["kv_cache_usage"] for s in snaps), default=0),
        }
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
    *,
    num_replicas_override: Optional[int] = None,
    use_global_planner: bool = False,
    planner_max_replicas_per_node: Optional[int] = None,
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

    num_replicas = (
        num_replicas_override
        if num_replicas_override is not None
        else model_config.num_replicas
        or default_num_replicas(model_config, total_gpus, config)
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
    actor_num_gpus: int
    extra_env_vars: Dict[str, str] = {}

    if use_global_planner:
        if model_config.pipeline_parallel_size > 1:
            if num_replicas == 1:
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
                    f"[AuroraServe] Planner PP placement for {model_id}: "
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
                    f"[AuroraServe] Planner placement for {model_id}: "
                    f"strategy={placement_group_strategy}, "
                    f"bundles={placement_group_bundles}; WARNING: multi-replica "
                    "PP uses location-agnostic bundles — a stage's TP group may "
                    "straddle nodes",
                    flush=True,
                )
        else:
            actor_num_gpus = model_config.tensor_parallel_size
            print(
                f"[AuroraServe] Planner scheduling for {model_id}: "
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
                f"[AuroraServe] PP placement for {model_id}: "
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

    deployment_options = dict(
        name=f"VLLMWorker-{safe_name}",
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

    deployment = VLLMWorker.options(**deployment_options).bind(
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
    )

    return deployment, model_id


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

    for skipped_plan in replica_plan.skipped_model_plans:
        if skipped_plan.skipped_reason:
            print(
                f"[AuroraServe] Skipping {skipped_plan.model_config.model_id}: "
                f"{skipped_plan.skipped_reason}",
                flush=True,
            )

    use_root_route = len(config.model_configs) == 1
    if not use_root_route:
        print(
            f"[AuroraServe] Deploying {len(active_plans)} active planned models "
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
            f"\n[AuroraServe] ═══ Deploying planned model {model_index + 1}/{len(active_plans)} ═══",
            flush=True,
        )
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
                f"[AuroraServe] Service available at http://localhost:8000/v1 "
                f"(model: {model_id}, replicas={model_plan.assigned_replicas})",
                flush=True,
            )
            continue

        safe_name = get_model_route_name(model_id)
        route_prefix = f"/{safe_name}"
        with tracer.phase("serve.run", model_id=model_id, replicas=model_plan.assigned_replicas):
            serve.run(deployment, name=safe_name, route_prefix=route_prefix)
        print(
            f"[AuroraServe] ✓ {model_id} → http://localhost:8000{route_prefix}/v1 "
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
        with tracer.phase("serve.run", model_id=model_id):
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
    print(f"[AuroraServe] Loaded config from {config_path}: {config.deployment_name}", flush=True)
    print(
        f"[AuroraServe] proxy_config.type={_proxy_cfg.type!r} -> "
        f"Ray Serve proxy_location={_ray_serve_proxy_location}",
        flush=True,
    )
    print(f"[AuroraServe] Models: {len(config.model_configs)}", flush=True)
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
    bcast_timing_raw = os.environ.get("AURORA_MODEL_BCAST_TIMING", "").strip()
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
        f"[AuroraServe] Serve Init: Connecting to Ray cluster at {ray_address}...",
        flush=True,
    )

    with tracer.phase("ray.init"):
        init_ray_cluster(ray_address, namespace="serve", include_dashboard=False)

    # Create stats collector actor for per-replica init timing (replaces
    # per-file Lustre I/O).  Must be created after ray.init, before serve.run.
    from .scaling_trace import create_stats_collector, collect_replica_stats
    create_stats_collector()

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
        "[AuroraServe] Ray Serve timeouts patched: "
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
        f"[AuroraServe] HTTP proxy location: {_ray_serve_proxy_location}, "
        f"host=0.0.0.0, port=8000",
        flush=True,
    )
    tracer.record_phase("stage1.total", time.monotonic() - stage1_start)
    print_red(f"[AuroraServe] ✓ Serve Init completed in {time.monotonic() - stage1_start:.2f}s")

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
                f"[AuroraServe] Waiting for nodes: {alive_nodes} alive, "
                f"{total_gpus}/{expected_gpus} GPUs ({total_gpus/max(expected_gpus,1)*100:.0f}%)",
                flush=True,
            )
            poll_iteration += 1
            time.sleep(15)

    tracer.set_metadata(actual_gpus=total_gpus, alive_nodes=alive_nodes)
    if total_gpus < expected_gpus:
        print(
            f"[AuroraServe] WARNING: Only {total_gpus}/{expected_gpus} GPUs registered "
            f"after 10min deadline ({total_gpus/max(expected_gpus,1)*100:.0f}%). "
            f"Proceeding with reduced capacity.",
            flush=True,
        )
    else:
        print(
            f"[AuroraServe] All {total_gpus}/{expected_gpus} GPUs registered",
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
        print(f"[AuroraServe] Wrote {len(_ray_ips)} Ray node IP(s) -> {_ips_path}", flush=True)
    except Exception as _e:  # noqa: BLE001
        print(f"[AuroraServe] WARNING: failed to write ray_node_ips.txt: {_e}", flush=True)

    # ---- Model Resolution: resolve staged local models ---------------------
    null_compute = os.environ.get("AURORA_NULL_COMPUTE", "0") == "1"
    if null_compute:
        print(
            "[AuroraServe] Model Resolution: NULL-COMPUTE mode — model staging skipped",
            flush=True,
        )
        model_path_map = {cfg.model_id: cfg.model_id for cfg in config.model_configs}
    else:
        stage_start = time.time()
        print(
            f"[AuroraServe] Model Resolution: Resolving staged models from {config.local_stage_path}...",
            flush=True,
        )
        model_path_map = resolve_model_paths(
            config.model_configs,
            config.local_stage_path,
            require_complete=True,
        )
        print_red(
            f"[AuroraServe] ✓ Model Resolution completed in "
            f"{time.time() - stage_start:.2f}s"
        )

    planner_enabled = should_use_global_planner(config)

    # ---- Model Deploy: deploy model services to Ray Serve -------------------
    stage3_start = time.monotonic()
    print("[AuroraServe] Model Deploy: Deploying model services to Ray Serve...", flush=True)

    # Diagnostic: print effective health check constants in this process
    try:
        from ray.serve._private import constants as _diag_c
        print(
            f"[AuroraServe] DIAG health check constants in driver process:\n"
            f"  HTTP_PROXY_TIMEOUT            = {getattr(_diag_c, 'HTTP_PROXY_TIMEOUT', '?')}\n"
            f"  PROXY_HEALTH_CHECK_TIMEOUT_S   = {getattr(_diag_c, 'PROXY_HEALTH_CHECK_TIMEOUT_S', '?')}\n"
            f"  PROXY_HEALTH_CHECK_UNHEALTHY   = {getattr(_diag_c, 'PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD', '?')}\n"
            f"  DEFAULT_HEALTH_CHECK_TIMEOUT_S = {getattr(_diag_c, 'DEFAULT_HEALTH_CHECK_TIMEOUT_S', '?')}\n"
            f"  DEFAULT_HEALTH_CHECK_PERIOD_S  = {getattr(_diag_c, 'DEFAULT_HEALTH_CHECK_PERIOD_S', '?')}\n"
            f"  REPLICA_HEALTH_CHECK_UNHEALTHY = {getattr(_diag_c, 'REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD', '?')}",
            flush=True,
        )
    except Exception as _diag_e:
        print(f"[AuroraServe] DIAG constants import failed: {_diag_e}", flush=True)

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
                            f"[AuroraServe] Deploy monitor: proxies={n_proxies} (+{elapsed:.0f}s)",
                            flush=True,
                        )
                        prev_proxy_count = n_proxies
                except Exception as e:
                    if not _monitor_error_logged["value"]:
                        print(f"[AuroraServe] Deploy monitor error: {e}", flush=True)
                        _monitor_error_logged["value"] = True
        monitor = threading.Thread(target=_monitor_deploy, daemon=True)
        monitor.start()

        # Decompose serve.run() into its two blocking phases for timing.
        # serve.run() internally calls: deploy_applications(wait=True) + wait_for_proxies_serving()
        from ray.serve._private.api import serve_start as _serve_start
        from ray.serve.api import build_app
        from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME

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
            print(f"[AuroraServe] Step 1 submit_to_controller: {_deploy_step_times['submit_to_controller']}s", flush=True)

            # Step 2: Wait for deployment created
            t0 = time.monotonic()
            for app in built_apps:
                client._wait_for_deployment_created(app.ingress_deployment_name, app.name)
            _deploy_step_times["wait_deployment_created"] = round(time.monotonic() - t0, 4)
            print(f"[AuroraServe] Step 2 wait_deployment_created: {_deploy_step_times['wait_deployment_created']}s", flush=True)

            # Step 3: Wait for application RUNNING
            t0 = time.monotonic()
            for app in built_apps:
                client._wait_for_application_running(app.name)
            _deploy_step_times["wait_app_running"] = round(time.monotonic() - t0, 4)
            print(f"[AuroraServe] Step 3 wait_app_running: {_deploy_step_times['wait_app_running']}s", flush=True)

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
        print(f"[AuroraServe] Step 4a get_proxies: {t_get_proxies:.3f}s ({len(proxy_handles)} proxies)", flush=True)

        t0 = time.monotonic()
        serving_refs = [h.serving.remote(wait_for_applications_running=True) for h in proxy_handles.values()]
        t_issue = time.monotonic() - t0
        print(f"[AuroraServe] Step 4b issue .serving.remote() x{len(serving_refs)}: {t_issue:.3f}s", flush=True)

        t0 = time.monotonic()
        # Track when each proxy finishes
        remaining = list(serving_refs)
        proxy_complete_times = []
        wait_start = time.monotonic()
        while remaining:
            done, remaining = _ray.wait(remaining, num_returns=1, timeout=5.0)
            elapsed = time.monotonic() - wait_start
            if done:
                proxy_complete_times.append(elapsed)
                if len(proxy_complete_times) % 10 == 0 or not remaining:
                    print(f"[AuroraServe] Step 4c proxies ready: {len(proxy_complete_times)}/{len(serving_refs)} at +{elapsed:.1f}s", flush=True)
        t_wait = time.monotonic() - t0
        tracer.record_phase("serve.run.wait_proxies", t_wait)

        # Log proxy completion distribution
        if proxy_complete_times:
            import statistics
            print(
                f"[AuroraServe] Step 4 wait_proxies: {t_wait:.1f}s total, "
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
            print(f"[AuroraServe] Proxy spawn timeline: {proxy_milestones}", flush=True)

        # Collect per-proxy status from serve.status()
        try:
            status = serve.status()
            proxy_status_list = []
            for node_id, proxy in status.proxies.items():
                proxy_status_list.append({
                    "node_id": node_id,
                    "status": str(proxy.status),
                })
            tracer.set_metadata(proxy_statuses=proxy_status_list)
            print(f"[AuroraServe] Proxy statuses: {len(proxy_status_list)} proxies", flush=True)
        except Exception as e:
            print(f"[AuroraServe] Failed to collect proxy statuses: {e}", flush=True)

        # When instrumentation is on, gather /tmp/aurora_inst from every node
        # to Lustre once. No-op for clean Ray installs.
        _collect_instrumentation_all()

        print(f"[AuroraServe] Service available at http://localhost:8000/v1 (model: {model_id})", flush=True)
    else:
        with tracer.phase("deploy_multi_model"):
            deploy_multi_model(config, model_path_map, total_gpus)

    tracer.record_phase("stage3.total", time.monotonic() - stage3_start)
    print_red(
        f"[AuroraServe] ✓ Model Deploy completed in {time.monotonic() - stage3_start:.2f}s"
    )

    # ---- Collect per-replica stats via Ray actor (no filesystem I/O) ---------
    replica_stats = collect_replica_stats()

    for rs in replica_stats:
        tracer.record_replica_init(rs)
    if replica_stats:
        has_sub = sum(1 for rs in replica_stats if "engine_sub_phases" in rs)
        print(
            f"[AuroraServe] Collected {len(replica_stats)} replica init stats "
            f"(engine_create avg={sum(r.get('engine_create_s', 0) for r in replica_stats)/len(replica_stats):.1f}s, "
            f"sub-phases: {has_sub}/{len(replica_stats)})",
            flush=True,
        )

    # ---- All stages complete ------------------------------------------------
    total_time = time.time() - overall_start
    tracer.set_metadata(total_time_s=round(total_time, 4))
    trace_path = tracer.save()
    print_red(
        f"[AuroraServe] ✓✓✓ CLUSTER FULLY READY ✓✓✓ Total time: {total_time:.2f}s"
    )
    print(f"[AuroraServe] Scaling trace: {trace_path}", flush=True)

    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("[AuroraServe] Shutting down...")
