import argparse
import os
import subprocess
import sys
import time
import socket
import urllib.error
import urllib.request

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ["TMPDIR"] = "/tmp"
# Disable Ray log deduplication to see all replica logs
# os.environ["RAY_DEDUP_LOGS"] = "0"

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

# Ray Serve HTTP proxy port -- must match aurora_serve.py serve.start() config
RAY_SERVE_PORT = 8000

# How long to wait for Ray Serve to become healthy before starting the proxy
RAY_SERVE_HEALTH_TIMEOUT_S = 1800  # 30 min covers large-scale deployments


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


def wait_for_ray_serve(
    port: int = RAY_SERVE_PORT,
    timeout: float = RAY_SERVE_HEALTH_TIMEOUT_S,
) -> bool:
    """
    Poll http://localhost:{port}/health until 200 OK or timeout.

    Returns True when Ray Serve is healthy, False on timeout.
    """
    url = f"http://127.0.0.1:{port}/health"
    deadline = time.monotonic() + timeout
    attempt = 0
    print(f"[Driver] Waiting for Ray Serve to become healthy on port {port}...", flush=True)
    while time.monotonic() < deadline:
        attempt += 1
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                if resp.status == 200:
                    print(
                        f"[Driver] Ray Serve healthy after {attempt} poll(s).",
                        flush=True,
                    )
                    return True
        except (urllib.error.URLError, OSError):
            pass
        time.sleep(5)
    print(
        f"[Driver] WARNING: Ray Serve did not become healthy within {timeout}s.",
        flush=True,
    )
    return False


def start_proxy(proxy_config, deploy_config, config_path: str):
    """
    Start the configured proxy backend (if type != 'none').

    Returns (proxy_backend_instance, proxy_process) or (None, None).
    """
    if proxy_config.type == "none":
        return None, None

    # proxy/ lives alongside driver.py in src/; Python adds src/ to sys.path
    # automatically when running src/driver.py, so no path manipulation needed.
    from proxy import get_proxy
    from proxy.backends import discover_backends

    proxy = get_proxy(proxy_config.type)

    # Use the config file's parent directory as output dir for generated configs
    output_dir = os.path.join(os.path.dirname(os.path.abspath(config_path)), "proxy_out")

    backends = discover_backends(
        deploy_config=deploy_config,
        backend_port=proxy_config.backend_port,
    )

    # Merge python_path into options so proxy backends can use it.
    proxy_options = dict(proxy_config.options)
    if proxy_config.python_path:
        proxy_options["python_path"] = proxy_config.python_path

    cfg_file = proxy.generate_config(
        backends,
        output_dir=output_dir,
        **proxy_options,
    )

    proc = proxy.start(cfg_file, host="0.0.0.0", port=proxy_config.port,
                        num_workers=proxy_config.num_workers)

    healthy = proxy.health_check(
        "127.0.0.1", proxy_config.port, timeout=3600.0, process=proc,
    )
    if not healthy:
        # Collect exit code for the error message
        rc = proc.poll()
        if rc is not None:
            msg = (
                f"Proxy ({proxy_config.type}) process died immediately "
                f"(exit code {rc}). Check logs above for errors."
            )
        else:
            proc.kill()
            proc.wait()
            msg = (
                f"Proxy ({proxy_config.type}) health check timed out after 60s "
                f"on port {proxy_config.port}."
            )
        raise RuntimeError(f"[Driver] FATAL: {msg}")

    print(
        f"[Driver] Proxy ({proxy_config.type}) ready on "
        f"http://0.0.0.0:{proxy_config.port}",
        flush=True,
    )
    return proxy, proc


def stop_proxy(proxy, proc):
    """Stop the proxy process if one was started."""
    if proxy is not None and proc is not None:
        proxy.stop(proc)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--head-ip", required=True, help="IP Address of the Head Node")
    parser.add_argument("--port", default="6379", help="Ray GCS Port")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to deployment config YAML for Aurora Serve (optional). "
             "If not set, aurora_serve will use its default (config.yaml in cwd).",
    )
    args = parser.parse_args()

    rank = get_rank()
    hostname = socket.gethostname()
    print(f"[Driver] Node: {hostname} | Rank: {rank} | Role: {'HEAD' if rank == 0 else 'WORKER'}", flush=True)

    ray_process = None
    serve_process = None
    proxy_backend = None
    proxy_process = None

    try:
        if rank == 0:
            # === HEAD NODE LOGIC ===

            # 1. Start Ray Head (Background)
            ray_process = start_ray_head(args.head_ip, args.port)

            # 2. Wait for GCS to initialize (Grace period)
            print("[Driver] Waiting 10s for Ray GCS to stabilize...", flush=True)
            time.sleep(10)

            # 3. Launch Aurora Serve as a non-blocking subprocess so that the
            #    proxy can be started after Ray Serve is ready, and both run
            #    concurrently for the lifetime of the cluster.
            serve_cmd = [sys.executable, "src/aurora_serve.py"]
            if args.config:
                serve_cmd.extend(["--config", args.config])
            print(f"[Driver] Launching Aurora Serve: {' '.join(serve_cmd)}", flush=True)
            serve_process = subprocess.Popen(serve_cmd, env=get_ray_env())

            # 4. Load proxy config (if a config file was provided)
            if args.config:
                from schemas import load_deployment_config, load_proxy_config
                proxy_config = load_proxy_config(args.config)
                deploy_config = load_deployment_config(args.config)

                if proxy_config.type != "none":
                    # Wait for Ray Serve to be healthy before starting the proxy
                    wait_for_ray_serve(port=RAY_SERVE_PORT)
                    proxy_backend, proxy_process = start_proxy(
                        proxy_config, deploy_config, args.config
                    )

            # 5. Signal that ALL services (Ray Serve + proxy) are ready.
            #    run_exp.sh watches for this exact line to start the client.
            print("[Driver] ALL SERVICES READY", flush=True)

            # 6. Wait for Aurora Serve to exit (it blocks until the cluster shuts down)
            serve_process.wait()
            if serve_process.returncode != 0:
                print(
                    f"[Driver] Aurora Serve exited with code {serve_process.returncode}.",
                    flush=True,
                )

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
        # Stop the proxy before shutting down Ray
        if rank == 0:
            stop_proxy(proxy_backend, proxy_process)

        # Ensure aurora_serve doesn't outlive driver.py on unexpected exit
        if serve_process and serve_process.poll() is None:
            print("[Driver] Terminating Aurora Serve process...", flush=True)
            serve_process.terminate()
            try:
                serve_process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                serve_process.kill()
                serve_process.wait()

        # Cleanup ensures we don't leave zombie processes
        if ray_process and ray_process.poll() is None:
            print("[Driver] Terminating Ray process...", flush=True)
            ray_process.terminate()
            ray_process.wait()


if __name__ == "__main__":
    main()
