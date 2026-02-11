import argparse
import os
import subprocess
import sys
import time
import socket

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
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

def get_rank():
    """
    Detects MPI Rank from environment variables provided by Hydra/PALS.
    """
    # Check standard Hydra/MPICH variables
    for var in ["PMI_RANK", "PMI_ID", "ALPS_APP_PE", "OMPI_COMM_WORLD_RANK"]:
        if var in os.environ:
            return int(os.environ[var])
    return 0  # Default to 0 if not found (e.g. local testing)

def get_ray_env():
    """
    Injects the critical environment variables for Intel GPU visibility.
    """
    env = os.environ.copy()
    
    # Aurora Specifics for PVC (Ponte Vecchio)
    env["ZE_FLAT_DEVICE_HIERARCHY"] = "FLAT"  # Exposes all 12 tiles
    env["ZE_AFFINITY_MASK"] = ""              # Ensure no masking hides GPUs
    env["VLLM_TARGET_DEVICE"] = "xpu"         # Tell vLLM we are on Intel
    
    # See top-of-file comments for the NOSET + ONEAPI_DEVICE_SELECTOR workaround.
    env["RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR"] = "1"
    env["ONEAPI_DEVICE_SELECTOR"] = (
        "level_zero:" + ",".join(str(i) for i in range(NUM_GPU_TILES))
    )
    
    return env

def start_ray_head(ip, port):
    print(f"[Driver] Starting RAY HEAD on {ip}:{port}", flush=True)
    # --block is CRITICAL: It keeps the subprocess alive.
    cmd = [
        "ray", "start",
        "--head",
        "--num-cpus=64",
        "--num-gpus=12",
        f"--node-ip-address={ip}",
        f"--port={port}",
        "--dashboard-host=0.0.0.0",
        "--disable-usage-stats",
        "--include-dashboard=false",
        "--block" 
    ]
    return subprocess.Popen(cmd, env=get_ray_env())

def start_ray_worker(head_ip, head_port):
    print(f"[Driver] Starting RAY WORKER connecting to {head_ip}:{head_port}", flush=True)
    cmd = [
        "ray", "start",
        f"--address={head_ip}:{head_port}",
        "--block"
    ]
    return subprocess.Popen(cmd)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--head-ip", required=True, help="IP Address of the Head Node")
    parser.add_argument("--port", default="6379", help="Ray GCS Port")
    args = parser.parse_args()

    rank = get_rank()
    hostname = socket.gethostname()
    print(f"[Driver] Node: {hostname} | Rank: {rank} | Role: {'HEAD' if rank == 0 else 'WORKER'}", flush=True)

    ray_process = None

    try:
        if rank == 0:
            # === HEAD NODE LOGIC ===
            
            # 1. Start Ray Head (Background)
            ray_process = start_ray_head(args.head_ip, args.port)
            
            # 2. Wait for GCS to initialize (Grace period)
            print("[Driver] Waiting 10s for Ray GCS to stabilize...", flush=True)
            time.sleep(10)
            
            # 3. Launch the Orchestrator (Application Logic)
            print("[Driver] Launching Orchestrator...", flush=True)
            # We run this as a blocking call. If the app finishes, we tear down.
            # subprocess.run([sys.executable, "src/orchestrator.py"], check=True)
            
            print(f"[Driver] Launching Aurora Serve {sys.executable} src/aurora_serve.py", flush=True)
            subprocess.run([sys.executable, "src/aurora_serve.py"], check=True, env=get_ray_env())
            print("[Driver] Orchestrator finished. Shutting down cluster.", flush=True)

        else:
            # === WORKER NODE LOGIC ===
            
            # 1. Start Ray Worker (Blocking)
            # This process will stay alive as long as the Raylet is running.
            ray_process = start_ray_worker(args.head_ip, args.port)
            ray_process.wait() # Block until Ray dies or is killed

    except KeyboardInterrupt:
        print("[Driver] Caught interrupt, shutting down...", flush=True)
    except Exception as e:
        print(f"[Driver] Critical Error: {e}", flush=True)
    finally:
        # Cleanup ensures we don't leave zombie processes
        if ray_process and ray_process.poll() is None:
            print("[Driver] Terminating Ray process...", flush=True)
            ray_process.terminate()
            ray_process.wait()

if __name__ == "__main__":
    main()
