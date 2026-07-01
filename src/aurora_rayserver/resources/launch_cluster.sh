#!/bin/bash
set -e # Fail fast if anything goes wrong

# --- 1. Environment Setup ---
# Check if we are inside a PBS job or interactive session
if [ -z "$PBS_NODEFILE" ]; then
    echo "ERROR: \$PBS_NODEFILE not found. Are you in a debug session (qsub -I)?"
    exit 1
fi

# Get the absolute paths for the installed package and the working tree.
# In a source checkout, SCRIPT_DIR is <repo>/src/aurora_rayserver/resources.
# In an installed wheel, SCRIPT_DIR is <site-packages>/aurora_rayserver/resources.
# Runtime code and helper resources come from the package. PROJECT_ROOT is only
# the working directory used for relative configs and default run_logs.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PACKAGE_ROOT="${AURORA_RAYSERVER_PACKAGE_ROOT:-$( cd "$SCRIPT_DIR/.." && pwd )}"
PACKAGE_PARENT="${AURORA_RAYSERVER_PACKAGE_PARENT:-$( cd "$PACKAGE_ROOT/.." && pwd )}"
if [ -n "${AURORA_PROJECT_ROOT:-}" ]; then
    PROJECT_ROOT="$AURORA_PROJECT_ROOT"
elif [ -d "$SCRIPT_DIR/../../../src/aurora_rayserver" ] && [ -d "$SCRIPT_DIR/../../../tools" ]; then
    # Source-tree compatibility: resources -> aurora_rayserver -> src -> repo.
    PROJECT_ROOT="$( cd "$SCRIPT_DIR/../../.." && pwd )"
else
    PROJECT_ROOT="${AURORA_WORKDIR:-$PWD}"
fi
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
from aurora_rayserver.schemas import require_yaml

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
export PYTHONPATH="$PACKAGE_PARENT${SANITIZED_PYTHONPATH:+:$SANITIZED_PYTHONPATH}"

# Ensure user-local binaries (e.g. haproxy built from source) are reachable.
export PATH="$HOME/bin:$PATH"
# AURORA_PYTHON_EXEC overrides the frameworks python — used for the SGLang engine,
# which runs in a --system-site-packages venv (inherits frameworks Ray + Aurora XPU
# torch, adds sglang/sgl_kernel). Falls back to frameworks python (vLLM path).
PYTHON_EXEC="${AURORA_PYTHON_EXEC:-$(resolve_frameworks_python)}"
if [ -z "$PYTHON_EXEC" ] || [ ! -x "$PYTHON_EXEC" ]; then
    echo "ERROR: Failed to resolve python3 (AURORA_PYTHON_EXEC=$AURORA_PYTHON_EXEC)."
    exit 1
fi
echo "[System] PYTHON_EXEC=$PYTHON_EXEC (AURORA_ENGINE=${AURORA_ENGINE:-vllm})"

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

finalize_run_logs() {
    # MPI-driven log/artifact collection (replaces the prior parallel-ssh
    # fan-out). Each PBS-allocated rank tars its node-local files and
    # writes <RUN_LOG_DIR>/per_node/<hostname>.tar.gz directly to Lustre.
    # mpiexec is launched once via PALS — no head-side ssh fork storm.
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

    set +e

    local node_count
    node_count="$(wc -l < "$UNIQUE_NODES_FILE")"
    local per_node_dir="$RUN_LOG_DIR/per_node"
    local dbg="$RUN_LOG_DIR/finalize_debug.log"
    mkdir -p "$per_node_dir"
    : > "$dbg"
    echo "finalize start $(date -u +%Y-%m-%dT%H:%M:%SZ) exit_code=$exit_code node_count=$node_count" >> "$dbg"

    # Resolve the gather binary. It's compiled once per run group by
    # distribute_to_nodes.sh; rebuild on demand if missing (covers the case
    # where finalize fires before staging completed — e.g. early failure).
    local build_dir="${AURORA_BCAST_BUILD_DIR:-$RUN_LOG_DIR/bcast_build}"
    local gather_bin="$build_dir/gather"
    if [ ! -x "$gather_bin" ]; then
        echo "[finalize] gather binary missing; compiling under $build_dir" | tee -a "$dbg"
        AURORA_BCAST_BUILD_DIR="$build_dir" "$PYTHON_EXEC" - <<PY 2>>"$dbg" || true
from aurora_rayserver.model_bcast import compile_gather
from pathlib import Path
compile_gather(Path("$build_dir"))
PY
    fi

    if [ ! -x "$gather_bin" ]; then
        echo "[finalize] WARNING: gather binary unavailable; falling back to head-only collect" | tee -a "$dbg"
        # Head-only fallback: at least we get this node's logs to Lustre.
        local self_short="$(hostname -s)"
        local local_session
        local_session="$(readlink -f /tmp/ray/session_latest 2>/dev/null || true)"
        if [ -n "$local_session" ] && [ -d "$local_session/logs" ]; then
            local out="$per_node_dir/$self_short.tar.gz"
            local args=()
            for f in gcs_server.out gcs_server.err raylet.out raylet.err dashboard.log dashboard.err dashboard.out; do
                [ -f "$local_session/logs/$f" ] && args+=("$local_session/logs/$f")
            done
            [ -d "$local_session/logs/serve" ] && args+=("$local_session/logs/serve")
            [ -d /tmp/aurora_inst ] && args+=("/tmp/aurora_inst")
            if [ "${#args[@]}" -gt 0 ]; then
                tar --ignore-failed-read --warning=no-file-changed -czf "$out" "${args[@]}" 2>>"$dbg" || true
            fi
        fi
    else
        # The shared "safe" set of Ray session files. Whole-directory tarring
        # of /tmp/ray/session_latest/logs/ hung historically (live FIFOs/sockets
        # written by Ray); naming files explicitly avoids that.
        local gather_args=(
            "$per_node_dir"
            "/tmp/ray/session_latest/logs/gcs_server.out"
            "/tmp/ray/session_latest/logs/gcs_server.err"
            "/tmp/ray/session_latest/logs/raylet.out"
            "/tmp/ray/session_latest/logs/raylet.err"
            "/tmp/ray/session_latest/logs/dashboard.log"
            "/tmp/ray/session_latest/logs/dashboard.err"
            "/tmp/ray/session_latest/logs/dashboard.out"
            "/tmp/ray/session_latest/logs/serve"
            "/tmp/aurora_inst"
        )
        echo "[finalize] mpiexec gather to $per_node_dir ($node_count rank(s))" | tee -a "$dbg"
        # bound the whole collective at 5 min — at 256 nodes with ~100 MB each
        # writing in parallel to Lustre this is far longer than needed.
        timeout 300 mpiexec -n "$node_count" -ppn 1 --cpu-bind none \
            "$gather_bin" "${gather_args[@]}" >>"$dbg" 2>&1
        echo "[finalize] gather rc=$?" >> "$dbg"
    fi

    local collected
    collected="$(find "$per_node_dir" -maxdepth 1 -name '*.tar.gz' | wc -l)"
    echo "finalize end $(date -u +%Y-%m-%dT%H:%M:%SZ) archives=$collected" >> "$dbg"

    echo "[System] Per-node archives: $per_node_dir ($collected/$node_count nodes collected)"
    echo "[System] Persistent run log: $RUN_LOG_FILE"
    echo "[System] Launcher exit code: $exit_code"
}

trap 'finalize_run_logs $?' EXIT

echo "[System] Project Root: $PROJECT_ROOT"
echo "[System] Package Root: $PACKAGE_ROOT"
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

# Timeout patches (HTTP_PROXY_TIMEOUT, PROXY_READY_CHECK_TIMEOUT_S,
# PROXY_HEALTH_CHECK_TIMEOUT_S, PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
# DEFAULT_HEALTH_CHECK_*, REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD) are applied two ways:
#   - On clean Ray: aurora_serve._patch_ray_serve_proxy_constants runs as a
#     runtime_env worker_process_setup_hook on every Ray worker.
#   - With AURORA_INSTRUMENTATION=1: src/patches/ray_serve_overlay/ray/serve/_private/constants.py
#     ships the same values statically.


echo "[System] Deployment config: $DEPLOYMENT_CONFIG_PATH"

# Optional per-run clean stage (experiment flag launch.clean_stage ->
# AURORA_CLEAN_STAGE): wipe node-local artifacts BEFORE staging so the MPI
# weight broadcast (Phase 2) is exercised + timed every run, and the node
# starts from a clean slate. See resources/cleanup_run.sh.
if [ "${AURORA_CLEAN_STAGE:-0}" = "1" ]; then
    echo "[System] CLEAN-STAGE: wiping per-node run artifacts before staging"
    timeout 180 mpiexec -n "$NODE_COUNT" -ppn 1 --cpu-bind none \
        bash "$SCRIPT_DIR/cleanup_run.sh" \
        || echo "[System] WARN: clean-stage cleanup reported errors (continuing)"
fi

AURORA_MODEL_BCAST_TIMING=""
if [ "${AURORA_NULL_COMPUTE:-0}" = "1" ]; then
    echo "[System] NULL-COMPUTE mode enabled; skipping model staging"
else
    echo "[System] Staging models to node-local storage via MPI bcast..."
    PYTHONPATH="$PACKAGE_PARENT${PYTHONPATH:+:$PYTHONPATH}" \
        $PYTHON_EXEC -m aurora_rayserver.model_bcast --config "$DEPLOYMENT_CONFIG_PATH" --num-nodes "$NODE_COUNT"
    # model_bcast.py writes timing JSON to a well-known path
    BCAST_TIMING_FILE="$RUN_LOG_DIR/model_bcast_timing.json"
    if [ -f "$BCAST_TIMING_FILE" ]; then
        AURORA_MODEL_BCAST_TIMING=$(cat "$BCAST_TIMING_FILE")
    fi
fi
export AURORA_MODEL_BCAST_TIMING

export AURORA_VLLM_PATCH_PP_LAYER_FILTER="${AURORA_VLLM_PATCH_PP_LAYER_FILTER:-1}"

# Ray's compiled-DAG channels crash in the RayWorkerWrapper accelerator
# context on XPU (ONEAPI_DEVICE_SELECTOR device-id mapping), so PP>1 must use
# the uncompiled Ray executor fallback from _sitecustomize. Only affects
# pipeline_parallel_size > 1; TP-only paths never consult this flag.
export AURORA_VLLM_DISABLE_RAY_COMPILED_DAG="${AURORA_VLLM_DISABLE_RAY_COMPILED_DAG:-1}"

# Scaling trace instrumentation: collects per-replica init timing and
# driver phases via Ray object store (no Lustre file I/O).  Safe at any
# scale.  Set AURORA_SCALING_TRACE=0 to fully disable.
export AURORA_SCALING_TRACE="${AURORA_SCALING_TRACE:-1}"
echo "[System] AURORA_SCALING_TRACE=$AURORA_SCALING_TRACE"

# Instrumentation gate. When 1, distribute_to_nodes.sh stages a Ray Serve
# overlay (with probes/timeout patches) under /tmp/aurora_overlay on every
# node, and aurora_serve._collect_instrumentation_all gathers the resulting
# /tmp/aurora_inst/* files at end of startup. Default 0 = clean Ray.
export AURORA_INSTRUMENTATION="${AURORA_INSTRUMENTATION:-0}"
echo "[System] AURORA_INSTRUMENTATION=$AURORA_INSTRUMENTATION"

# Ray's built-in per-RPC event stats — written to {gcs_server,raylet,core-worker-*}.out
# every RAY_event_stats_print_interval_ms. Used to measure GCS contention quantitatively.
export RAY_event_stats="${RAY_event_stats:-1}"
export RAY_event_stats_print_interval_ms="${RAY_event_stats_print_interval_ms:-1000}"
echo "[System] RAY_event_stats=$RAY_event_stats (print_interval=${RAY_event_stats_print_interval_ms}ms)"

# At 128+ nodes, each vLLM replica process has ~1500 gRPC connections to
# other Ray actors, consuming many threads.  When the HuggingFace Rust
# tokenizer lazily spawns its rayon thread pool on first request, it
# tries to create ~nproc (208) threads and hits EAGAIN, panicking with
# ThreadPoolBuildError.  Force single-threaded tokenizer parallelism to
# avoid the rayon pool spawn entirely.
export RAYON_NUM_THREADS="${RAYON_NUM_THREADS:-1}"
export TOKENIZERS_PARALLELISM="${TOKENIZERS_PARALLELISM:-false}"
echo "[System] RAYON_NUM_THREADS=$RAYON_NUM_THREADS TOKENIZERS_PARALLELISM=$TOKENIZERS_PARALLELISM"

# --- Per-node distribution: aurora_serve src + (optional) Ray Serve overlay ---
# Stages /tmp/aurora_src on every node so user code runs from local tmpfs.
# When AURORA_INSTRUMENTATION=1, also stages /tmp/aurora_overlay (symlink farm
# pointing at the system Ray package, with patched files from
# aurora_rayserver/patches/ray_serve_overlay/).
export PROJECT_ROOT PACKAGE_ROOT PACKAGE_PARENT PYTHON_EXEC UNIQUE_NODES_FILE HOSTNAME_SHORT
export AURORA_RAYSERVER_PACKAGE_ROOT="$PACKAGE_ROOT"
export AURORA_RAYSERVER_PACKAGE_PARENT="$PACKAGE_PARENT"
DISTRIBUTE_SCRIPT="${AURORA_DISTRIBUTE_SCRIPT:-$SCRIPT_DIR/distribute_to_nodes.sh}"
bash "$DISTRIBUTE_SCRIPT"

export PYTHONPATH="/tmp/aurora_src${PYTHONPATH:+:$PYTHONPATH}"
if [ "${AURORA_INSTRUMENTATION:-0}" = "1" ] && [ -d /tmp/aurora_overlay/ray/serve/_private ]; then
    export PYTHONPATH="/tmp/aurora_overlay:$PYTHONPATH"
    echo "[System] Ray overlay active at /tmp/aurora_overlay (from $PACKAGE_ROOT/patches/ray_serve_overlay)"
fi
echo "[System] PYTHONPATH after distribution: $PYTHONPATH"

# --- Copper: scalable Python module distribution ---
# Copper is a read-only caching layer that distributes Python modules across
# nodes via cooperative caching, avoiding Lustre stampedes.  The Ray Serve
# overlay and aurora sources are now staged explicitly under /tmp above; do not
# prepend the legacy ~/.local Ray overlay here, because that bypasses
# AURORA_INSTRUMENTATION=0 and silently turns clean-Ray runs into patched runs.
# Enable with AURORA_USE_COPPER=1; auto-disabled for single-node runs.
COPPER_ACTIVE=0
if [ "${AURORA_USE_COPPER:-0}" = "1" ] && [ "$NODE_COUNT" -ge 2 ]; then
    if module load copper 2>/dev/null; then
        COPPER_LOG_DIR="$RUN_LOG_DIR/copper"
        mkdir -p "$COPPER_LOG_DIR"
        launch_copper_aurora.sh -d "$COPPER_LOG_DIR" -v /tmp/${USER}/copper_mount 2>&1 || true
        COPPER_ACTIVE=1
        echo "[System] Copper active; PYTHONPATH unchanged (per-run /tmp staging is source of truth)"
    else
        echo "[System] Copper module not available, continuing without it"
    fi
fi

# Force unbuffered Python output so tee gets lines immediately
export PYTHONUNBUFFERED=1

# driver runs from the per-node /tmp/aurora_src copy so all node-local
# imports (aurora_rayserver.driver, .server, .model_paths, ...) come from
# tmpfs, not Lustre. Invoke as a module so relative imports resolve.
mpiexec -n $NODE_COUNT -ppn 1 --cpu-bind none \
    $PYTHON_EXEC -m aurora_rayserver.driver --config "$DEPLOYMENT_CONFIG_PATH"

# Stop Copper if it was started
if [ "$COPPER_ACTIVE" = "1" ]; then
    stop_copper_aurora.sh -d "$COPPER_LOG_DIR" -v /tmp/${USER}/copper_mount 2>&1 || true
fi
