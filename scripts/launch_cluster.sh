#!/bin/bash
set -e # Fail fast if anything goes wrong

# --- 1. Environment Setup ---
# Check if we are inside a PBS job or interactive session
if [ -z "$PBS_NODEFILE" ]; then
    echo "ERROR: \$PBS_NODEFILE not found. Are you in a debug session (qsub -I)?"
    exit 1
fi

# Get the absolute path of the directory containing this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

sanitize_pythonpath() {
    local raw_path="${1:-}"
    local sanitized=""
    local entry

    IFS=':' read -r -a _py_entries <<< "$raw_path"
    for entry in "${_py_entries[@]}"; do
        [ -z "$entry" ] && continue
        case "$entry" in
            *"/venv/"*"/site-packages"*|*"/.venv/"*"/site-packages"*)
                continue
                ;;
        esac
        sanitized="${sanitized:+$sanitized:}$entry"
    done

    printf '%s\n' "$sanitized"
}

resolve_frameworks_python() {
    local current_python
    current_python="$(command -v python3 2>/dev/null || true)"
    case "$current_python" in
        /opt/aurora/*/frameworks/*/bin/python3)
            printf '%s\n' "$current_python"
            return
            ;;
    esac

    local fallback_python
    fallback_python="$(ls -1d /opt/aurora/*/frameworks/aurora_frameworks-*/bin/python3 2>/dev/null | sort | tail -n 1)"
    if [ -n "$fallback_python" ] && [ -x "$fallback_python" ]; then
        printf '%s\n' "$fallback_python"
        return
    fi

    printf '%s\n' "$current_python"
}

write_ray_cluster_head_ip() {
    local config_path="$1"
    local head_ip="$2"
    "$PYTHON_EXEC" - "$config_path" "$head_ip" <<'PY'
import sys
from schemas import require_yaml

config_path, head_ip = sys.argv[1:3]
yaml = require_yaml()
with open(config_path, "r", encoding="utf-8") as handle:
    data = yaml.safe_load(handle) or {}
cluster_cfg = dict(data.get("ray_cluster_config", {}) or {})
cluster_cfg["head_ip"] = head_ip
data["ray_cluster_config"] = cluster_cfg
with open(config_path, "w", encoding="utf-8") as handle:
    yaml.safe_dump(data, handle, default_flow_style=False, sort_keys=False)
PY
}

# Optional: prepend a local debug_libs checkout if you need one.
# export PYTHONPATH="/path/to/debug_libs:$PYTHONPATH"
unset VIRTUAL_ENV PYTHONHOME CONDA_DEFAULT_ENV CONDA_PREFIX CONDA_PROMPT_MODIFIER _CE_CONDA _CE_M
SANITIZED_PYTHONPATH="$(sanitize_pythonpath "${PYTHONPATH:-}")"
export PYTHONPATH="$PROJECT_ROOT/src${SANITIZED_PYTHONPATH:+:$SANITIZED_PYTHONPATH}"

# Ensure user-local binaries (e.g. haproxy built from source) are reachable.
export PATH="$HOME/bin:$PATH"
PYTHON_EXEC="$(resolve_frameworks_python)"
if [ -z "$PYTHON_EXEC" ] || [ ! -x "$PYTHON_EXEC" ]; then
    echo "ERROR: Failed to resolve Aurora frameworks python3."
    exit 1
fi

DEPLOYMENT_CONFIG_PATH="${1:-config.yaml}"
if [ ! -f "$DEPLOYMENT_CONFIG_PATH" ]; then
    echo "ERROR: Deployment config not found: $DEPLOYMENT_CONFIG_PATH"
    exit 1
fi
DEPLOYMENT_CONFIG_PATH="$(readlink -f "$DEPLOYMENT_CONFIG_PATH")"

RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
CONFIG_BASENAME="$(basename "$DEPLOYMENT_CONFIG_PATH")"
CONFIG_STEM="${CONFIG_BASENAME%.*}"
RUN_LOG_ROOT="${AURORA_RUN_LOG_ROOT:-$PROJECT_ROOT/run_logs}"
RUN_LOG_DIR="${AURORA_RUN_LOG_DIR:-$RUN_LOG_ROOT/${RUN_STAMP}_${CONFIG_STEM}}"
RUN_LOG_FILE="${AURORA_RUN_LOG_FILE:-$RUN_LOG_DIR/launch.log}"
mkdir -p "$RUN_LOG_DIR"

if [ "${AURORA_PROJECT_LOGGING_INITIALIZED:-0}" != "1" ]; then
    export AURORA_PROJECT_LOGGING_INITIALIZED=1
    export AURORA_RUN_LOG_DIR="$RUN_LOG_DIR"
    export AURORA_RUN_LOG_FILE="$RUN_LOG_FILE"
    exec > >(tee -a "$RUN_LOG_FILE") 2>&1
fi

HOSTNAME_SHORT="$(hostname -s)"
UNIQUE_NODES_FILE="$RUN_LOG_DIR/pbs_nodes.txt"
sort -u "$PBS_NODEFILE" > "$UNIQUE_NODES_FILE"
cp "$DEPLOYMENT_CONFIG_PATH" "$RUN_LOG_DIR/deployment_config.yaml"

collect_ray_logs() {
    local source_logs=$1
    local dest_dir=$2
    mkdir -p "$dest_dir"
    if [ -d "$source_logs" ]; then
        cp -a "$source_logs/." "$dest_dir/" 2>/dev/null || true
    fi
}

collect_remote_ray_logs() {
    local node=$1
    local dest_dir=$2
    mkdir -p "$dest_dir"
    ssh "$node" "readlink -f /tmp/ray/session_latest 2>/dev/null || true" \
        > "$dest_dir/session_path.txt" 2>/dev/null || true
    scp -r "$node:/tmp/ray/session_latest/logs/." "$dest_dir/" >/dev/null 2>&1 || true
}

finalize_run_logs() {
    local exit_code=$1
    local metadata_file="$RUN_LOG_DIR/run_metadata.txt"
    {
        echo "run_timestamp_utc=$RUN_STAMP"
        echo "launcher_host=$HOSTNAME_SHORT"
        echo "deployment_config=$DEPLOYMENT_CONFIG_PATH"
        echo "pbs_jobid=${PBS_JOBID:-}"
        echo "pbs_nodefile=$PBS_NODEFILE"
        echo "exit_code=$exit_code"
    } > "$metadata_file"

    local ray_log_root="$RUN_LOG_DIR/ray_logs"
    mkdir -p "$ray_log_root"
    while read -r node; do
        [ -z "$node" ] && continue
        local short_node="${node%%.*}"
        local dest_dir="$ray_log_root/$short_node"
        mkdir -p "$dest_dir"
        if [ "$short_node" = "$HOSTNAME_SHORT" ] || [ "$node" = "$(hostname)" ]; then
            local session_dir
            session_dir="$(readlink -f /tmp/ray/session_latest 2>/dev/null || true)"
            if [ -n "$session_dir" ]; then
                echo "$session_dir" > "$dest_dir/session_path.txt"
                collect_ray_logs "$session_dir/logs" "$dest_dir"
            fi
        else
            collect_remote_ray_logs "$node" "$dest_dir"
        fi
    done < "$UNIQUE_NODES_FILE"

    # Collect instrumentation data from all nodes
    local inst_root="$RUN_LOG_DIR/instrumentation"
    mkdir -p "$inst_root"
    while read -r node; do
        [ -z "$node" ] && continue
        local short_node="${node%%.*}"
        local inst_dest="$inst_root/$short_node"
        mkdir -p "$inst_dest"
        if [ "$short_node" = "$HOSTNAME_SHORT" ] || [ "$node" = "$(hostname)" ]; then
            cp /tmp/aurora_inst/*.json /tmp/aurora_inst/*.jsonl "$inst_dest/" 2>/dev/null || true
        else
            scp "$node:/tmp/aurora_inst/*" "$inst_dest/" >/dev/null 2>&1 || true
        fi
    done < "$UNIQUE_NODES_FILE"
    echo "[System] Instrumentation data: $inst_root"

    echo "[System] Persistent run log: $RUN_LOG_FILE"
    echo "[System] Persistent Ray logs: $ray_log_root"
    echo "[System] Launcher exit code: $exit_code"
}

trap 'finalize_run_logs $?' EXIT

echo "[System] Project Root: $PROJECT_ROOT"
echo "[System] Nodefile: $PBS_NODEFILE"
echo "[System] PYTHONPATH: $PYTHONPATH"
echo "[System] Backend Python: $PYTHON_EXEC"
echo "[System] Deployment config: $DEPLOYMENT_CONFIG_PATH"
echo "[System] Persistent run dir: $RUN_LOG_DIR"

# --- 2. IP Resolution (The Scout) ---
echo "[System] Resolving Head Node IP..."
HEAD_IP=$(AURORA_VLLM_PATCH_PP_LAYER_FILTER=0 $PYTHON_EXEC - <<'PY'
import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    sock.connect(("10.255.255.255", 1))
    print(sock.getsockname()[0])
finally:
    sock.close()
PY
)


if [ -z "$HEAD_IP" ]; then
    echo "ERROR: Failed to resolve Head IP."
    exit 1
fi

echo "[System] Head IP (HSN): $HEAD_IP"
write_ray_cluster_head_ip "$DEPLOYMENT_CONFIG_PATH" "$HEAD_IP"

# --- 3. Calculate Node Count ---
NODE_COUNT=$(wc -l < "$UNIQUE_NODES_FILE")
echo "[System] Total Nodes: $NODE_COUNT"

# --- 4. Atomic Launch ---
echo "[System] Launching Cluster..."

export ZE_FLAT_DEVICE_HIERARCHY="FLAT"
export ZE_AFFINITY_MASK=""
export CCL_PROCESS_LAUNCHER="hydra"

# Aurora shells can inherit an unusually large per-thread stack size, which
# causes Ray worker creation to fail once vLLM launches many distributed
# workers. Clamp it before starting Ray so child processes can create threads.
ulimit -s 8192 || true

# Ray's Intel GPU runtime rewrites ONEAPI_DEVICE_SELECTOR to
# "level_zero:...". On Aurora that value crashes Triton's SYCL device probe.
# Keep Ray from touching the selector and rely on ZE_AFFINITY_MASK for all
# device isolation instead.
export RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR="1"
unset ONEAPI_DEVICE_SELECTOR

# Whole-node PP launches many Ray core workers at once. On Aurora the default
# per-worker gRPC/server thread fan-out can exhaust the node's process/thread
# budget before model init completes, so clamp the Ray internals to a smaller
# footprint for this launcher.
# Ray internal thread counts. The defaults below are conservative (1 thread)
# to avoid thread exhaustion during PP launches with many workers. For serving
# throughput benchmarks, increase RAY_num_server_call_thread (e.g. 4-8) to
# allow the HTTP proxy to handle more concurrent requests per node.
export RAY_num_server_call_thread="${RAY_num_server_call_thread:-4}"
export RAY_core_worker_num_server_call_thread="${RAY_core_worker_num_server_call_thread:-1}"
export RAY_num_grpc_internal_threads="${RAY_num_grpc_internal_threads:-1}"
export RAY_worker_num_grpc_internal_threads="${RAY_worker_num_grpc_internal_threads:-1}"
export RAY_task_events_report_interval_ms="${RAY_task_events_report_interval_ms:-0}"
export RAY_enable_metrics_collection="${RAY_enable_metrics_collection:-0}"

# GCS stability at 256+ nodes: increase timeouts and thread counts so the
# head node's GCS server can handle registration storms from many Raylets.
export RAY_gcs_server_num_threads="${RAY_gcs_server_num_threads:-8}"
export RAY_gcs_server_request_timeout_seconds="${RAY_gcs_server_request_timeout_seconds:-60}"
export RAY_raylet_client_num_connect_attempts="${RAY_raylet_client_num_connect_attempts:-20}"
export RAY_raylet_client_connect_timeout_milliseconds="${RAY_raylet_client_connect_timeout_milliseconds:-30000}"

# Ray Serve throughput optimizations (available in Ray 2.53+).
# Enables separate thread for user code and separate event loop for the router.
export RAY_SERVE_THROUGHPUT_OPTIMIZED="${RAY_SERVE_THROUGHPUT_OPTIMIZED:-1}"

# Proxy readiness check: default 5s is too short on Aurora because the
# metrics agent timeout (30s, hardcoded in C++) blocks proxy startup.
# Without this, the controller kills the proxy after 3×5s=15s < 30s.
export RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S="${RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S:-60}"

# Patch Ray Serve proxy timeouts in ALL Python processes (including the
# ServeController Ray actor which runs as a separate process).
# Install a .pth file in the user site-packages directory. Python's site
# module executes .pth lines starting with "import" at interpreter startup,
# and the user site-packages is always loaded before any user code.
USER_SITE=$($PYTHON_EXEC -m site --user-site 2>/dev/null || echo "")
if [ -n "$USER_SITE" ]; then
    mkdir -p "$USER_SITE"
    # Write usercustomize.py in user site-packages. Python's site module
    # loads sitecustomize from SYSTEM site-packages (before user site is on
    # sys.path), then loads usercustomize from USER site-packages. So we
    # must use usercustomize.py for user-site hooks.
    #
    # Strategy: hook builtins.__import__ with recursion guard, patch any
    # ray.serve module that has our target constants.
    rm -f "$USER_SITE/sitecustomize.py"  # remove old misnamed file
    cat > "$USER_SITE/usercustomize.py" <<'PYEOF'
import builtins as _b
import os as _os
import time as _time

_orig = _b.__import__
_processing = set()  # guard against re-entrant import of the SAME module

# Record process birth time for proxy profiling
_process_birth_time = _time.time()

def _aurora_import(name, *args, **kwargs):
    if name in _processing:
        return _orig(name, *args, **kwargs)
    _processing.add(name)
    try:
        mod = _orig(name, *args, **kwargs)
        # Only check for Ray Serve constants on ray.serve modules.
        # Checking hasattr on arbitrary modules (e.g. pydantic) triggers
        # lazy __getattr__ and circular imports.
        if name.startswith("ray.serve"):
            # Proxy timeouts — effectively disable health-check killing
            if hasattr(mod, 'HTTP_PROXY_TIMEOUT') and getattr(mod, 'HTTP_PROXY_TIMEOUT') == 60:
                mod.HTTP_PROXY_TIMEOUT = 3600
            if hasattr(mod, 'PROXY_HEALTH_CHECK_TIMEOUT_S') and getattr(mod, 'PROXY_HEALTH_CHECK_TIMEOUT_S') == 10.0:
                mod.PROXY_HEALTH_CHECK_TIMEOUT_S = 300.0
            if hasattr(mod, 'PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD') and getattr(mod, 'PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD') == 3:
                mod.PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 100
            # PROXY_READY_CHECK_TIMEOUT_S is set via RAY_SERVE_PROXY_READY_CHECK_TIMEOUT_S
            # env var (not import hook) because proxy_state.py copies it on import.
            # Replica timeouts — effectively disable health-check killing
            if hasattr(mod, 'DEFAULT_HEALTH_CHECK_TIMEOUT_S') and getattr(mod, 'DEFAULT_HEALTH_CHECK_TIMEOUT_S') == 30:
                mod.DEFAULT_HEALTH_CHECK_TIMEOUT_S = 600
            if hasattr(mod, 'DEFAULT_HEALTH_CHECK_PERIOD_S') and getattr(mod, 'DEFAULT_HEALTH_CHECK_PERIOD_S') == 10:
                mod.DEFAULT_HEALTH_CHECK_PERIOD_S = 120
            if hasattr(mod, 'REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD') and getattr(mod, 'REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD') == 3:
                mod.REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 100

        # ProxyActor lifecycle profiling (AURORA_PROXY_PROFILE=1)
        if name == "ray.serve._private.proxy" and _os.environ.get("AURORA_PROXY_PROFILE") == "1":
            _wrap_proxy_for_profiling(mod)

        # Controller/proxy_state instrumentation (AURORA_CONTROLLER_INST=1)
        # Deferred wrapping: modules have circular imports, so the class
        # may not exist on the first __import__ call.  We check sys.modules
        # after every ray.serve import and wrap once the class appears.
        if _os.environ.get("AURORA_CONTROLLER_INST") == "1" and name.startswith("ray.serve"):
            import sys as _sys
            _ctrl_mod = _sys.modules.get("ray.serve._private.controller")
            if _ctrl_mod is not None and not getattr(getattr(_ctrl_mod, "ServeController", None), "_aurora_ctrl_inst", False):
                if hasattr(_ctrl_mod, "ServeController"):
                    _wrap_controller_for_inst(_ctrl_mod)
            _ps_mod = _sys.modules.get("ray.serve._private.proxy_state")
            if _ps_mod is not None and not getattr(getattr(_ps_mod, "ProxyStateManager", None), "_aurora_psm_inst", False):
                if hasattr(_ps_mod, "ProxyStateManager"):
                    _wrap_proxy_state_for_inst(_ps_mod)

        return mod
    finally:
        _processing.discard(name)

def _wrap_proxy_for_profiling(proxy_module):
    """Wrap ProxyActor.__init__ and ready() to record per-process timestamps."""
    import socket as _socket, json as _json
    cls = getattr(proxy_module, "ProxyActor", None)
    if cls is None or getattr(cls, "_aurora_profiled", False):
        return

    _orig_init = cls.__init__
    _orig_ready = cls.ready

    def _profiled_init(self, *a, **kw):
        with open("/tmp/aurora_proxy_init_" + str(_os.getpid()) + ".log", "w") as _if:
            _if.write("INIT CALLED pid=" + str(_os.getpid()) + "\n")
        self._aurora_profile = {
            "process_python_start_s": _process_birth_time,
            "init_start_s": _time.time(),
            "hostname": _socket.gethostname(),
            "pid": _os.getpid(),
        }
        try:
            _orig_init(self, *a, **kw)
        finally:
            self._aurora_profile["init_end_s"] = _time.time()
            self._aurora_profile["init_duration_s"] = round(
                self._aurora_profile["init_end_s"] - self._aurora_profile["init_start_s"], 4)
            self._aurora_profile["node_id"] = getattr(self, "_node_id", "unknown")

    async def _profiled_ready(self):
        if hasattr(self, "_aurora_profile"):
            self._aurora_profile["ready_start_s"] = _time.time()
        try:
            result = await _orig_ready(self)
        finally:
            if hasattr(self, "_aurora_profile"):
                self._aurora_profile["ready_end_s"] = _time.time()
                self._aurora_profile["ready_duration_s"] = round(
                    self._aurora_profile["ready_end_s"] - self._aurora_profile["ready_start_s"], 4)
                self._aurora_profile["total_python_s"] = round(
                    self._aurora_profile["ready_end_s"] - _process_birth_time, 4)
                # Save to /tmp
                _d = "/tmp/aurora_proxy_profile"
                try:
                    _os.makedirs(_d, exist_ok=True)
                    with open(f"{_d}/{self._aurora_profile['hostname']}_{_os.getpid()}.json", "w") as _f:
                        _json.dump(self._aurora_profile, _f, indent=2)
                except Exception:
                    pass
        return result

    cls.__init__ = _profiled_init
    cls.ready = _profiled_ready
    cls._aurora_profiled = True
    with open("/tmp/aurora_proxy_wrap_" + str(_os.getpid()) + ".log", "w") as _wf:
        _wf.write("wrapped pid=" + str(_os.getpid()) + " init_ok=" + str(cls.__init__ is _profiled_init) + "\n")

def _wrap_proxy_state_for_inst(proxy_state_module):
    """Instrument ProxyStateManager to log proxy spawn events and state transitions."""
    import socket as _socket, json as _json
    mgr_cls = getattr(proxy_state_module, "ProxyStateManager", None)
    if mgr_cls is None or getattr(mgr_cls, "_aurora_psm_inst", False):
        return

    _inst_dir = "/tmp/aurora_inst"
    _os.makedirs(_inst_dir, exist_ok=True)
    _host = _socket.gethostname()
    _pid = _os.getpid()
    _inst_log_path = f"{_inst_dir}/proxy_state_{_host}_{_pid}.jsonl"

    def _log_ps_event(event_type, **data):
        entry = {"t": _time.time(), "event": event_type, **data}
        try:
            with open(_inst_log_path, "a") as f:
                f.write(_json.dumps(entry) + "\n")
        except Exception:
            pass

    # Wrap _start_proxies_if_needed to log each proxy spawn
    _orig_start_proxies = mgr_cls._start_proxies_if_needed
    def _inst_start_proxies(self, target_nodes):
        pre_count = len(self._proxy_states)
        _orig_start_proxies(self, target_nodes)
        post_count = len(self._proxy_states)
        new_count = post_count - pre_count
        if new_count > 0:
            _log_ps_event("proxies_spawned", new=new_count, total=post_count, target_nodes=len(target_nodes))
    mgr_cls._start_proxies_if_needed = _inst_start_proxies

    # Wrap _stop_proxies_if_needed to log proxy stops and restarts
    _orig_stop_proxies = mgr_cls._stop_proxies_if_needed
    def _inst_stop_proxies(self):
        pre = set(self._proxy_states.keys())
        result = _orig_stop_proxies(self)
        post = set(self._proxy_states.keys())
        stopped = pre - post
        if stopped:
            _log_ps_event("proxies_stopped", count=len(stopped), node_ids=[n[:16] for n in stopped])
        return result
    mgr_cls._stop_proxies_if_needed = _inst_stop_proxies

    # Wrap the update method to log proxy state summary periodically
    _orig_update = mgr_cls.update
    _update_count = [0]
    def _inst_update(self, **kwargs):
        _orig_update(self, **kwargs)
        _update_count[0] += 1
        if _update_count[0] <= 3 or _update_count[0] % 50 == 0:
            status_counts = {}
            for ps in self._proxy_states.values():
                s = str(ps.status)
                status_counts[s] = status_counts.get(s, 0) + 1
            _log_ps_event("proxy_state_summary",
                update_num=_update_count[0],
                total=len(self._proxy_states),
                status_counts=status_counts,
            )
    mgr_cls.update = _inst_update

    mgr_cls._aurora_psm_inst = True
    _log_ps_event("proxy_state_inst_installed", host=_host, pid=_pid)

def _wrap_controller_for_inst(controller_module):
    """Instrument ServeController to log proxy spawning decisions and control loop timing."""
    import socket as _socket, json as _json
    cls = getattr(controller_module, "ServeController", None)
    if cls is None or getattr(cls, "_aurora_ctrl_inst", False):
        return

    _inst_dir = "/tmp/aurora_inst"
    _os.makedirs(_inst_dir, exist_ok=True)
    _host = _socket.gethostname()
    _pid = _os.getpid()
    _inst_log_path = f"{_inst_dir}/controller_{_host}_{_pid}.jsonl"
    # No threading.Lock — append-mode writes <4KB are atomic on Linux.
    # Mutable state via list refs (picklable, unlike threading.Lock).
    _loop_count = [0]
    _prev_proxy_nodes_count = [0]

    def _log_event(event_type, **data):
        entry = {"t": _time.time(), "event": event_type, **data}
        try:
            with open(_inst_log_path, "a") as f:
                f.write(_json.dumps(entry) + "\n")
        except Exception:
            pass

    # Wrap _update_proxy_nodes to log node set changes
    _orig_update_proxy_nodes = cls._update_proxy_nodes
    def _inst_update_proxy_nodes(self):
        _orig_update_proxy_nodes(self)
        n = len(self._proxy_nodes)
        if n != _prev_proxy_nodes_count[0]:
            _log_event("proxy_nodes_changed", prev=_prev_proxy_nodes_count[0], new=n)
            _prev_proxy_nodes_count[0] = n
    cls._update_proxy_nodes = _inst_update_proxy_nodes

    # Wrap run_control_loop_step to log timing and recovering state
    _orig_loop_step = cls.run_control_loop_step
    async def _inst_loop_step(self, start_time, recovering_timeout, num_loops):
        t0 = _time.time()
        was_recovering = not self.done_recovering_event.is_set()
        await _orig_loop_step(self, start_time, recovering_timeout, num_loops)
        dur = _time.time() - t0
        is_recovering = not self.done_recovering_event.is_set()
        _loop_count[0] += 1
        # Log every 100th loop, or when recovering state changes, or if loop is slow
        if (was_recovering and not is_recovering) or dur > 1.0 or _loop_count[0] <= 5 or _loop_count[0] % 100 == 0:
            _log_event("control_loop",
                loop_num=_loop_count[0],
                duration_s=round(dur, 4),
                recovering_changed=was_recovering and not is_recovering,
                proxy_nodes=_prev_proxy_nodes_count[0],
            )
        if was_recovering and not is_recovering:
            _log_event("done_recovering", elapsed_since_start=round(_time.time() - start_time, 3))
    cls.run_control_loop_step = _inst_loop_step

    cls._aurora_ctrl_inst = True
    _log_event("controller_inst_installed", host=_host, pid=_pid)

_b.__import__ = _aurora_import
PYEOF
    echo "[System] Installed usercustomize.py import hook in $USER_SITE"
else
    echo "[System] WARNING: Could not determine user site-packages for proxy timeout patch"
fi

echo "[System] Deployment config: $DEPLOYMENT_CONFIG_PATH"

AURORA_MODEL_BCAST_TIMING=""
if [ "${AURORA_NULL_COMPUTE:-0}" = "1" ]; then
    echo "[System] NULL-COMPUTE mode enabled; skipping model staging"
else
    echo "[System] Staging models to node-local storage via MPI bcast..."
    $PYTHON_EXEC src/model_bcast.py --config "$DEPLOYMENT_CONFIG_PATH" --num-nodes "$NODE_COUNT"
    # model_bcast.py writes timing JSON to a well-known path
    BCAST_TIMING_FILE="$RUN_LOG_DIR/model_bcast_timing.json"
    if [ -f "$BCAST_TIMING_FILE" ]; then
        AURORA_MODEL_BCAST_TIMING=$(cat "$BCAST_TIMING_FILE")
    fi
fi
export AURORA_MODEL_BCAST_TIMING

export AURORA_VLLM_PATCH_PP_LAYER_FILTER="${AURORA_VLLM_PATCH_PP_LAYER_FILTER:-1}"

# Scaling trace instrumentation: collects per-replica init timing and
# driver phases via Ray object store (no Lustre file I/O).  Safe at any
# scale.  Set AURORA_SCALING_TRACE=0 to fully disable.
export AURORA_SCALING_TRACE="${AURORA_SCALING_TRACE:-1}"
echo "[System] AURORA_SCALING_TRACE=$AURORA_SCALING_TRACE"

# ProxyActor lifecycle profiling: wraps ProxyActor.__init__ and ready()
# to collect per-process timestamps.  Writes JSON to /tmp/aurora_proxy_profile/.
export AURORA_PROXY_PROFILE="${AURORA_PROXY_PROFILE:-1}"
echo "[System] AURORA_PROXY_PROFILE=$AURORA_PROXY_PROFILE"

# Controller instrumentation: logs proxy spawning decisions and control loop
# timing to /tmp/aurora_inst/ for post-mortem analysis.
export AURORA_CONTROLLER_INST="${AURORA_CONTROLLER_INST:-1}"
echo "[System] AURORA_CONTROLLER_INST=$AURORA_CONTROLLER_INST"

# At 128+ nodes, each vLLM replica process has ~1500 gRPC connections to
# other Ray actors, consuming many threads.  When the HuggingFace Rust
# tokenizer lazily spawns its rayon thread pool on first request, it
# tries to create ~nproc (208) threads and hits EAGAIN, panicking with
# ThreadPoolBuildError.  Force single-threaded tokenizer parallelism to
# avoid the rayon pool spawn entirely.
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"
export TOKENIZERS_PARALLELISM="${TOKENIZERS_PARALLELISM:-false}"
echo "[System] RAYON_NUM_THREADS=$RAYON_NUM_THREADS TOKENIZERS_PARALLELISM=$TOKENIZERS_PARALLELISM"

# --- Copper: scalable Python module distribution ---
# Copper is a read-only caching layer that distributes Python modules across
# nodes via cooperative caching, avoiding Lustre stampedes.  Recommended at
# >2k nodes but useful whenever we have overlay files to distribute.
# Enable with AURORA_USE_COPPER=1; auto-disabled for single-node runs.
COPPER_ACTIVE=0
if [ "${AURORA_USE_COPPER:-0}" = "1" ] && [ "$NODE_COUNT" -ge 2 ]; then
    if module load copper 2>/dev/null; then
        COPPER_LOG_DIR="$RUN_LOG_DIR/copper"
        mkdir -p "$COPPER_LOG_DIR"
        launch_copper_aurora.sh -d "$COPPER_LOG_DIR" -v /tmp/${USER}/copper_mount 2>&1 || true
        COPPER_ACTIVE=1
        # Prepend Copper-mounted overlay path to PYTHONPATH
        OVERLAY_DIR="$HOME/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages"
        if [ -d "$OVERLAY_DIR" ]; then
            export PYTHONPATH="/tmp/${USER}/copper/${OVERLAY_DIR}:$PYTHONPATH"
            echo "[System] Copper active: overlay at /tmp/${USER}/copper/${OVERLAY_DIR}"
        fi
    else
        echo "[System] Copper module not available, continuing without it"
    fi
fi

# Force unbuffered Python output so tee gets lines immediately
export PYTHONUNBUFFERED=1

mpiexec -n $NODE_COUNT -ppn 1 --cpu-bind none \
    $PYTHON_EXEC src/driver.py --config "$DEPLOYMENT_CONFIG_PATH"

# Stop Copper if it was started
if [ "$COPPER_ACTIVE" = "1" ]; then
    stop_copper_aurora.sh -d "$COPPER_LOG_DIR" -v /tmp/${USER}/copper_mount 2>&1 || true
fi
