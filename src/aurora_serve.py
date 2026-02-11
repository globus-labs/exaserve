import os
import time
import ray
from ray import serve
from ray.serve.llm import LLMConfig, build_openai_app, LLMServingArgs

# --- CONFIGURATION ---
MODEL_ID = os.getenv("MODEL_ID", "meta-llama/Meta-Llama-3-8B-Instruct")

os.environ["OMP_NUM_THREADS"] = "4"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "4"
os.environ["TMPDIR"] = "/tmp"
# Disable Ray log deduplication to see all replica logs
os.environ["RAY_DEDUP_LOGS"] = "0"
os.environ["ZE_FLAT_DEVICE_HIERARCHY"] = "FLAT"
os.environ["ZE_AFFINITY_MASK"] = ""
os.environ["VLLM_TARGET_DEVICE"] = "xpu"
# --- WORKAROUND for two Ray bugs on Intel XPU ---
# Bug 1 (without NOSET): Ray's intel_gpu.py set_current_process_visible_accelerator_ids
#   produces "level_zero:" (empty) for non-GPU actors → SYCL parsing crash.
# Bug 2 (with NOSET): ONEAPI_DEVICE_SELECTOR is never set → compiled DAG's
#   accelerator_context.py can't resolve device IDs → ValueError: '0' is not in list.
#
# Fix: Set NOSET=1 so Ray never overwrites ONEAPI_DEVICE_SELECTOR (avoids Bug 1),
# AND pre-set ONEAPI_DEVICE_SELECTOR with all 12 tiles so the compiled DAG can
# resolve device IDs via .index() (avoids Bug 2).  All workers inherit this value.
NUM_GPU_TILES = 12  # 6 PVC cards × 2 tiles with ZE_FLAT_DEVICE_HIERARCHY=FLAT
os.environ["RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR"] = "1"
os.environ["ONEAPI_DEVICE_SELECTOR"] = (
    "level_zero:" + ",".join(str(i) for i in range(NUM_GPU_TILES))
)
# --- BUILD APP ---
# We use build_openai_app to create an OpenAI-compatible serving application.
# This setup is tailored for Aurora (Intel XPU) with specific engine arguments.

llm_config = LLMConfig(
    model_loading_config=dict(
        model_id=MODEL_ID,
    ),
    deployment_config=dict(
        # Scaling config for LLM workers
        autoscaling_config=dict(
            min_replicas=1,
            max_replicas=2,
            target_num_ongoing_requests_per_replica=5,
        ),
        # ray_actor_options=dict(
        #     num_cpus=16,
        #     num_gpus=1,
        # ),
    ),
    # Aurora/vLLM specific engine arguments
    engine_kwargs=dict(
        tensor_parallel_size=1,  # Number of GPUs for tensor parallelism (EngineCore bundle)
        gpu_memory_utilization=0.90,
        max_model_len=4096,
        enforce_eager=True, # Intel XPU stability
        enable_log_requests=True, # Workaround for Aurora vLLM
    ),
    # Scale Ingress/Router workers
    experimental_configs=dict(
        # num_router_replicas=4, 
        # num_ingress_replicas=1,
    ),
)

print(f"[AuroraServe] Building OpenAI App for model: {MODEL_ID}", flush=True)
# app = build_openai_app(dict(llm_configs=[llm_config]))
app = build_openai_app(LLMServingArgs(llm_configs=[llm_config]))

print("[AuroraServe] Running OpenAI App", flush=True)
serve.run(app)
print("[AuroraServe] Deployment active. Service available at http://localhost:8000/v1", flush=True)

try:
    while True:
        time.sleep(10)
except KeyboardInterrupt:
    print("[AuroraServe] Shutting down...")
