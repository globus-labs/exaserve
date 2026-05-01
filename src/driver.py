import argparse
import collections
import hashlib
import os
import subprocess
import sys
import threading
import time
import socket
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from typing import Sequence

from scaling_trace import (
    default_scaling_trace_path,
    tracing_enabled,
)

os.environ["OMP_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["VECLIB_MAXIMUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"
os.environ["TMPDIR"] = "/tmp"
# Disable Ray log deduplication to see all replica logs
# os.environ["RAY_DEDUP_LOGS"] = "0"

# Ray Serve HTTP proxy port -- must match aurora_serve.py serve.start() config
RAY_SERVE_PORT = 8000

# Ray is not truly ready until aurora_serve.py prints its cluster-wide ready marker.
AURORA_SERVE_READY_MARKER = "CLUSTER FULLY READY"
AURORA_SERVE_READY_TIMEOUT_S = int(os.environ.get("AURORA_SERVE_READY_TIMEOUT_S", "3600"))

# After the marker, also confirm the HTTP endpoints respond before starting the proxy.
RAY_SERVE_HEALTH_TIMEOUT_S = 1800  # 30 min covers large-scale deployments

DEBUG_HOLD_ON_FAILURE_S = int(os.environ.get("AURORA_DEBUG_HOLD_ON_FAILURE_S", "0"))
SRC_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_RAY_HEAD_PORT = 6379
DEFAULT_RAY_NODE_CPUS = 8
DEFAULT_RAY_INTERNAL_STARTUP_LIMIT = 8
RAY_INTERNAL_STARTUP_LIMIT_ENV = "AURORA_RAY_INTERNAL_STARTUP_LIMIT"


@dataclass(frozen=True)
class RayClusterConfig:
    head_ip: str
    port: int = DEFAULT_RAY_HEAD_PORT
    node_cpus: int = DEFAULT_RAY_NODE_CPUS


@dataclass
class ProcessOutputRelay:
    process: subprocess.Popen[str]
    ready_marker: str
    label: str
    ready_event: threading.Event = field(init=False)
    recent_lines: collections.deque[str] = field(init=False)
    _thread: threading.Thread | None = None

    def __post_init__(self) -> None:
        self.ready_event = threading.Event()
        self.recent_lines = collections.deque(maxlen=40)

    def start(self) -> "ProcessOutputRelay":
        if self.process.stdout is None:
            return self

        def reader() -> None:
            assert self.process.stdout is not None
            for line in self.process.stdout:
                print(line, end="", flush=True)
                stripped = line.rstrip()
                self.recent_lines.append(stripped)
                if self.ready_marker and self.ready_marker in line:
                    self.ready_event.set()

        self._thread = threading.Thread(target=reader, daemon=True)
        self._thread.start()
        return self

    def wait_for_ready(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.ready_event.wait(timeout=min(1.0, max(0.0, deadline - time.monotonic()))):
                return True
            if self.process.poll() is not None:
                return False
        return self.ready_event.is_set()

    def close(self) -> None:
        if self.process.stdout is None:
            return
        try:
            self.process.stdout.close()
        except Exception:
            pass


def prepend_pythonpath(env: dict[str, str], path: str) -> None:
    current = env.get("PYTHONPATH")
    if not current:
        env["PYTHONPATH"] = path
        return

    entries = current.split(os.pathsep)
    if path in entries:
        return
    env["PYTHONPATH"] = os.pathsep.join([path, *entries])


def get_hsn_ip() -> str:
    """
    Pick the IP address on the high-speed fabric.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            sock.connect(("10.255.255.255", 1))
            return sock.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())


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
    env["AURORA_VLLM_PATCH_PP_LAYER_FILTER"] = env.get(
        "AURORA_VLLM_PATCH_PP_LAYER_FILTER",
        "1",
    )
    prepend_pythonpath(env, SRC_DIR)

    # At large replica counts (64+ nodes) the default 0.1s deadline causes
    # every replica to time out simultaneously, triggering a NoneType crash
    # in _fulfill_pending_requests. 2.0s gives enough headroom for cross-node
    # round trips in a large cluster.
    env["RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S"] = "300.0"

    # Throughput optimizations: separate thread for user code and separate
    # event loop for the router. Available in Ray 2.53+.
    env.setdefault("RAY_SERVE_THROUGHPUT_OPTIMIZED", "1")

    # Ray's Intel GPU integration rewrites ONEAPI_DEVICE_SELECTOR to a
    # "level_zero:..." list, but Triton's SYCL probe crashes on Aurora when
    # that value is present. Keep the selector unset and rely on
    # ZE_AFFINITY_MASK for device isolation instead.
    env["RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR"] = "1"
    env.pop("ONEAPI_DEVICE_SELECTOR", None)

    return env


def load_ray_cluster_config(config_path: str) -> RayClusterConfig:
    from schemas import require_yaml

    yaml = require_yaml()
    with open(config_path, "r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    cluster_cfg = payload.get("ray_cluster_config", {}) or {}
    head_ip = str(cluster_cfg.get("head_ip", "")).strip()
    if not head_ip:
        raise ValueError(
            "ray_cluster_config.head_ip is missing from the runtime config. "
            "launch_cluster.sh must resolve and write it before starting driver.py."
        )

    port = int(cluster_cfg.get("port", DEFAULT_RAY_HEAD_PORT))
    if port < 1:
        raise ValueError(f"ray_cluster_config.port must be >= 1, got {port}")

    node_cpus = int(cluster_cfg.get("node_cpus", DEFAULT_RAY_NODE_CPUS))
    if node_cpus < 1:
        raise ValueError(f"ray_cluster_config.node_cpus must be >= 1, got {node_cpus}")

    return RayClusterConfig(head_ip=head_ip, port=port, node_cpus=node_cpus)


def get_ray_internal_startup_limit(cluster: RayClusterConfig) -> int:
    # Keep the advertised CPU resources high for scheduling while limiting
    # Raylet's eager worker/process fan-out on Aurora.
    raw_limit = os.environ.get(RAY_INTERNAL_STARTUP_LIMIT_ENV, "").strip()
    if raw_limit:
        try:
            configured_limit = int(raw_limit)
        except ValueError as exc:
            raise ValueError(
                f"{RAY_INTERNAL_STARTUP_LIMIT_ENV} must be an integer, got {raw_limit!r}"
            ) from exc
    else:
        configured_limit = DEFAULT_RAY_INTERNAL_STARTUP_LIMIT
    return max(1, min(cluster.node_cpus, configured_limit))


def start_ray_head(cluster: RayClusterConfig):
    startup_limit = get_ray_internal_startup_limit(cluster)
    print(
        f"[Driver] Starting RAY HEAD on {cluster.head_ip}:{cluster.port} "
        f"with advertised CPUs={cluster.node_cpus} "
        f"(startup/prestart cap={startup_limit})",
        flush=True,
    )
    # --block is CRITICAL: It keeps the subprocess alive.
    cmd = [
        sys.executable,
        os.path.join(SRC_DIR, "ray_start.py"),
        "--head",
        f"--node-ip-address={cluster.head_ip}",
        f"--num-cpus={cluster.node_cpus}",
        "--num-gpus=12",
        f"--port={cluster.port}",
        "--disable-usage-stats",
        "--include-dashboard=false",
        "--block",
        f"--max-startup-concurrency={startup_limit}",
        f"--prestart-python-workers={startup_limit}",
    ]
    return subprocess.Popen(cmd, env=get_ray_env())


def start_ray_worker(cluster: RayClusterConfig):
    worker_ip = get_hsn_ip()
    startup_limit = get_ray_internal_startup_limit(cluster)
    print(
        f"[Driver] Starting RAY WORKER connecting to {cluster.head_ip}:{cluster.port} "
        f"from {worker_ip} with advertised CPUs={cluster.node_cpus} "
        f"(startup/prestart cap={startup_limit})",
        flush=True,
    )
    cmd = [
        sys.executable,
        os.path.join(SRC_DIR, "ray_start.py"),
        f"--address={cluster.head_ip}:{cluster.port}",
        f"--node-ip-address={worker_ip}",
        f"--num-cpus={cluster.node_cpus}",
        "--num-gpus=12",
        "--block",
        f"--max-startup-concurrency={startup_limit}",
        f"--prestart-python-workers={startup_limit}",
    ]
    return subprocess.Popen(cmd, env=get_ray_env())


def wait_for_ray_serve(
    port: int = RAY_SERVE_PORT,
    timeout: float = RAY_SERVE_HEALTH_TIMEOUT_S,
    process: subprocess.Popen | None = None,
    health_paths: Sequence[str] | None = None,
) -> bool:
    """
    Poll the configured Ray Serve health path(s) until 200 OK or timeout.

    Returns True when Ray Serve is healthy, False on timeout.
    """
    paths = list(health_paths or ["/health"])
    urls = [f"http://127.0.0.1:{port}{path}" for path in paths]
    deadline = time.monotonic() + timeout
    attempt = 0
    print(
        f"[Driver] Waiting for Ray Serve health on {', '.join(paths)} "
        f"(port {port})...",
        flush=True,
    )
    while time.monotonic() < deadline:
        attempt += 1
        if process is not None:
            rc = process.poll()
            if rc is not None:
                print(
                    f"[Driver] WARNING: Ray Serve process exited before health check passed (exit code {rc}).",
                    flush=True,
                )
                return False
        all_healthy = True
        for url in urls:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    if resp.status != 200:
                        all_healthy = False
                        break
            except (urllib.error.URLError, OSError):
                all_healthy = False
                break
        if all_healthy:
            print(
                f"[Driver] Ray Serve healthy after {attempt} poll(s): "
                f"{', '.join(paths)}",
                flush=True,
            )
            return True
        time.sleep(5)
    print(
        "[Driver] WARNING: Ray Serve did not become healthy within "
        f"{timeout}s for {', '.join(paths)}.",
        flush=True,
    )
    return False


def wait_for_process_ready_marker(
    relay: ProcessOutputRelay,
    *,
    timeout: float,
) -> bool:
    print(
        f"[Driver] Waiting for {relay.label} readiness marker: {relay.ready_marker}",
        flush=True,
    )
    if relay.wait_for_ready(timeout):
        print(
            f"[Driver] {relay.label} readiness marker observed.",
            flush=True,
        )
        return True

    rc = relay.process.poll()
    if rc is not None:
        print(
            f"[Driver] WARNING: {relay.label} exited before readiness marker was observed "
            f"(exit code {rc}).",
            flush=True,
        )
    else:
        print(
            f"[Driver] WARNING: Timed out waiting {timeout}s for {relay.label} readiness marker.",
            flush=True,
        )
    return False


def start_proxy(proxy_config, deploy_config, config_path: str):
    """
    Start the configured proxy backend (if type != 'none').

    Returns (proxy_backend_instance, proxy_process, actual_port) or (None, None, None).
    """
    if proxy_config.type == "none":
        return None, None, None

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

    proc, actual_port = proxy.start(cfg_file, host="0.0.0.0", port=proxy_config.port,
                                     num_workers=proxy_config.num_workers)

    healthy = proxy.health_check(
        "127.0.0.1", actual_port, timeout=3600.0, process=proc,
    )
    if not healthy:
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
                f"Proxy ({proxy_config.type}) health check timed out "
                f"on port {actual_port}."
            )
        raise RuntimeError(f"[Driver] FATAL: {msg}")

    # Write the actual port to a file so replay_client.py can read it
    # even when the port differed from the configured preference.
    port_file = os.path.join(output_dir, "proxy_port")
    os.makedirs(output_dir, exist_ok=True)
    with open(port_file, "w") as f:
        f.write(str(actual_port))
    print(
        f"[Driver] Proxy ({proxy_config.type}) ready on "
        f"http://0.0.0.0:{actual_port} (port file: {port_file})",
        flush=True,
    )
    return proxy, proc, actual_port


def stop_proxy(proxy, proc):
    """Stop the proxy process if one was started."""
    if proxy is not None and proc is not None:
        proxy.stop(proc)


def _compute_scaling_trace_token(config_path: str, cluster: RayClusterConfig) -> str:
    seed = os.environ.get("AURORA_RUN_LOG_DIR", "").strip() or os.environ.get("PBS_JOBID", "").strip()
    if not seed:
        seed = f"{os.path.abspath(config_path)}:{cluster.head_ip}:{cluster.port}"
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:16]


def _merge_driver_phases_into_trace(driver_phases: list[dict], hostname: str, rank: int) -> None:
    """Merge rank-0 driver phases into the scaling trace written by aurora_serve.py.

    No per-rank file I/O — phases are passed directly in-memory from rank 0.
    """
    if not tracing_enabled():
        print("[Driver][Trace] Scaling trace disabled; skipping merge", flush=True)
        return
    import json as _json

    trace_path = default_scaling_trace_path()
    if not os.path.isfile(trace_path):
        print(f"[Driver][Trace] No scaling trace found at {trace_path}", flush=True)
        return

    try:
        with open(trace_path) as f:
            merged = _json.load(f)
    except Exception as exc:
        print(f"[Driver][Trace] Failed to read scaling trace {trace_path}: {exc}", flush=True)
        return

    for phase in driver_phases:
        phase["source"] = f"driver.rank{rank}.{hostname}"
    merged.setdefault("driver_phases", []).extend(driver_phases)

    with open(trace_path, "w") as f:
        _json.dump(merged, f, indent=2, default=str)
    print(
        f"[Driver][Trace] Merged {len(driver_phases)} driver phases into {trace_path}",
        flush=True,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Path to runtime config YAML")
    args = parser.parse_args()
    cluster = load_ray_cluster_config(args.config)
    os.environ.setdefault(
        "AURORA_SCALING_TRACE_TOKEN",
        _compute_scaling_trace_token(args.config, cluster),
    )

    rank = get_rank()
    hostname = socket.gethostname()
    print(f"[Driver] Node: {hostname} | Rank: {rank} | Role: {'HEAD' if rank == 0 else 'WORKER'}", flush=True)
    print(
        f"[Driver] Cluster config: head_ip={cluster.head_ip} "
        f"port={cluster.port} node_cpus={cluster.node_cpus}",
        flush=True,
    )

    ray_process = None
    serve_process = None
    serve_output = None
    proxy_backend = None
    proxy_process = None

    # Driver-level timing (independent of aurora_serve.py's tracer)
    _driver_phases: list[dict] = []

    def _driver_phase(name: str, duration_s: float, **extra) -> None:
        entry = {"name": name, "duration_s": round(duration_s, 4), "wall_end": time.time(), **extra}
        _driver_phases.append(entry)
        print(f"[Driver][Trace] {name}: {duration_s:.3f}s", flush=True)

    try:
        if rank == 0:
            # === HEAD NODE LOGIC ===
            head_start = time.monotonic()

            # 1. Start Ray Head (Background)
            t0 = time.monotonic()
            ray_process = start_ray_head(cluster)
            _driver_phase("start_ray_head.launch", time.monotonic() - t0)

            # 2. Launch Aurora Serve as a non-blocking subprocess so that the
            #    proxy can be started after Ray Serve is ready, and both run
            #    concurrently for the lifetime of the cluster.
            serve_cmd = [sys.executable, os.path.join(SRC_DIR, "aurora_serve.py")]
            serve_cmd.extend(["--config", args.config])
            serve_env = get_ray_env()
            serve_env["RAY_ADDRESS"] = f"{cluster.head_ip}:{cluster.port}"
            print(f"[Driver] Launching Aurora Serve: {' '.join(serve_cmd)}", flush=True)
            t0 = time.monotonic()
            serve_process = subprocess.Popen(
                serve_cmd,
                env=serve_env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
            serve_output = ProcessOutputRelay(
                process=serve_process,
                ready_marker=AURORA_SERVE_READY_MARKER,
                label="Aurora Serve",
            ).start()
            _driver_phase("aurora_serve.launch", time.monotonic() - t0)

            proxy_config = None
            deploy_config = None
            serve_health_paths = ["/health"]
            from schemas import load_deployment_config, load_proxy_config
            from model_paths import get_model_route_name
            proxy_config = load_proxy_config(args.config)
            deploy_config = load_deployment_config(args.config)
            if len(deploy_config.model_configs) > 1:
                serve_health_paths = [
                    f"/{get_model_route_name(model_config.model_id)}/health"
                    for model_config in deploy_config.model_configs
                ]

            # Ray is only ready once aurora_serve.py reports its cluster-wide
            # readiness marker. The per-route /health endpoints can return 200
            # earlier while replicas are still coming up elsewhere in the cluster.
            t0 = time.monotonic()
            if serve_output is None or not wait_for_process_ready_marker(
                serve_output,
                timeout=AURORA_SERVE_READY_TIMEOUT_S,
            ):
                tail = "\n".join(serve_output.recent_lines) if serve_output else ""
                raise RuntimeError(
                    "[Driver] FATAL: Aurora Serve never reported full cluster readiness."
                    + (f"\n{tail}" if tail else "")
                )
            _driver_phase("wait_for_ready_marker", time.monotonic() - t0)

            # After the explicit cluster-ready marker, confirm the public HTTP
            # routes respond before starting the proxy.
            t0 = time.monotonic()
            if not wait_for_ray_serve(
                port=RAY_SERVE_PORT,
                process=serve_process,
                health_paths=serve_health_paths,
            ):
                raise RuntimeError(
                    f"[Driver] FATAL: Ray Serve health check timed out on port {RAY_SERVE_PORT}."
                )
            _driver_phase("wait_for_health_check", time.monotonic() - t0)

            if proxy_config is not None and proxy_config.type != "none":
                t0 = time.monotonic()
                proxy_backend, proxy_process, _actual_port = start_proxy(
                    proxy_config, deploy_config, args.config
                )
                _driver_phase("start_proxy", time.monotonic() - t0)

            _driver_phase("head_total", time.monotonic() - head_start)

            # 5. Merge driver phases into the scaling trace (no per-rank files).
            _merge_driver_phases_into_trace(_driver_phases, hostname, rank)

            # 6. Signal that ALL services (Ray Serve + proxy) are ready.
            #    run_exp.sh watches for this exact line to start the client.
            print("[Driver] ALL SERVICES READY", flush=True)

            # 6. Wait for Aurora Serve to exit (it blocks until the cluster shuts down)
            serve_process.wait()
            if serve_process.returncode != 0:
                print(
                    f"[Driver] Aurora Serve exited with code {serve_process.returncode}.",
                    flush=True,
                )
                if DEBUG_HOLD_ON_FAILURE_S > 0:
                    print(
                        "[Driver] Debug hold enabled; keeping Ray alive for "
                        f"{DEBUG_HOLD_ON_FAILURE_S}s before cleanup.",
                        flush=True,
                    )
                    time.sleep(DEBUG_HOLD_ON_FAILURE_S)

            print("[Driver] Aurora Serve finished. Shutting down cluster.", flush=True)

        else:
            # === WORKER NODE LOGIC ===

            # 1. Start Ray Worker (Blocking)
            # This process will stay alive as long as the Raylet is running.
            t0 = time.monotonic()
            ray_process = start_ray_worker(cluster)
            _driver_phase("start_ray_worker.launch", time.monotonic() - t0)
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
        if serve_output is not None:
            serve_output.close()

        # Cleanup ensures we don't leave zombie processes
        if ray_process and ray_process.poll() is None:
            print("[Driver] Terminating Ray process...", flush=True)
            ray_process.terminate()
            ray_process.wait()


if __name__ == "__main__":
    main()
