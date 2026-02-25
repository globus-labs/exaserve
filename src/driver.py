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

# --- GPU device isolation ---
# NOSET=1 keeps all tiles visible to every Ray worker process (avoiding
# the "level_zero:" SYCL crash for non-GPU actors).  The global
# ONEAPI_DEVICE_SELECTOR lists all 12 tiles so SYCL can parse it.
#
# Per-tile isolation: each ModelWorker.__init__ narrows
# ONEAPI_DEVICE_SELECTOR to its assigned tile *before* creating the vLLM
# engine.  vLLM v1 forks a separate EngineCore subprocess that inherits
# the restricted selector; Level Zero reads it fresh at library-load time,
# so the subprocess sees only the correct tile.
#
# Architecture: Router (CPU, HTTP ingress) → ModelWorker (1 GPU, vLLM engine).
# Replica counts auto-scale to cluster size in aurora_serve.py.
NUM_GPU_TILES_PER_NODE = 12  # 6 PVC cards × 2 tiles (ZE_FLAT_DEVICE_HIERARCHY=FLAT)


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
    Build the environment for ray start subprocesses.
    """
    env = os.environ.copy()

    # Aurora Specifics for PVC (Ponte Vecchio)
    env["ZE_FLAT_DEVICE_HIERARCHY"] = "FLAT"  # Exposes all 12 tiles
    env["ZE_AFFINITY_MASK"] = ""              # All tiles visible (baseline)
    env["VLLM_TARGET_DEVICE"] = "xpu"         # Tell vLLM we are on Intel 
    env["RAY_ENABLE_METRICS_COLLECTION"] = "0"

    # At large replica counts (64+ nodes) the default 0.1s deadline causes
    # every replica to time out simultaneously, triggering a NoneType crash
    # in _fulfill_pending_requests. 2.0s gives enough headroom for cross-node
    # round trips in a large cluster.
    env["RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S"] = "300.0"

    # NOSET=1 prevents Ray from writing per-worker ONEAPI_DEVICE_SELECTOR
    # (avoids the "level_zero:" empty-string SYCL crash for non-GPU actors).
    # Global ONEAPI_DEVICE_SELECTOR lists all tiles as a safe baseline for
    # non-GPU actors (Routers).  Real per-tile isolation: each ModelWorker
    # overrides ZE_AFFINITY_MASK to its assigned tile and sets
    # ONEAPI_DEVICE_SELECTOR=level_zero:0 (re-indexed).  ZE_AFFINITY_MASK
    # provides hardware-level Level Zero isolation that is reliably inherited
    # by the vLLM EngineCore subprocess under 'spawn' multiprocessing.
    env["RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR"] = "1"
    env["ONEAPI_DEVICE_SELECTOR"] = (
        "level_zero:" + ",".join(str(i) for i in range(NUM_GPU_TILES_PER_NODE))
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
        # "--dashboard-host=0.0.0.0",
        "--disable-usage-stats",
        "--include-dashboard=false",
        "--block",
    ]
    return subprocess.Popen(cmd, env=get_ray_env())


def start_ray_worker(head_ip, head_port):
    print(f"[Driver] Starting RAY WORKER connecting to {head_ip}:{head_port}", flush=True)
    cmd = [
        "ray", "start",
        f"--address={head_ip}:{head_port}",
        "--num-gpus=12",
        "--block",
    ]
    return subprocess.Popen(cmd, env=get_ray_env())


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

            # 3. Launch Aurora Serve
            print(f"[Driver] Launching Aurora Serve: {sys.executable} src/aurora_serve.py", flush=True)
            subprocess.run([sys.executable, "src/aurora_serve.py"], check=True, env=get_ray_env())
            print("[Driver] Aurora Serve finished. Shutting down cluster.", flush=True)

        else:
            # === WORKER NODE LOGIC ===

            # 1. Start Ray Worker (Blocking)
            # This process will stay alive as long as the Raylet is running.
            ray_process = start_ray_worker(args.head_ip, args.port)
            ray_process.wait()  # Block until Ray dies or is killed

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
