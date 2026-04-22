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

    # Disable set -e inside finalize: we use `|| true` to tolerate individual
    # node collection failures, but bash's set -e can still kill the function
    # in surprising ways (especially with command substitutions inside `local`).
    set +e

    local ray_log_root="$RUN_LOG_DIR/ray_logs"
    local inst_root="$RUN_LOG_DIR/instrumentation"
    mkdir -p "$ray_log_root" "$inst_root"

    local dbg="$RUN_LOG_DIR/finalize_debug.log"
    : > "$dbg"
    echo "finalize start $(date -u +%Y-%m-%dT%H:%M:%SZ) exit_code=$exit_code" >> "$dbg"

    # Step 1: head node collects its own files synchronously (local cp — no ssh).
    local self_short="$(hostname -s)"
    echo "self_short=$self_short hostname=$(hostname)" >> "$dbg"
    local head_ray_dest="$ray_log_root/$self_short"
    local head_inst_dest="$inst_root/$self_short"
    mkdir -p "$head_ray_dest" "$head_inst_dest"
    echo "head dirs mkdir OK" >> "$dbg"
    local session_dir
    session_dir="$(readlink -f /tmp/ray/session_latest 2>/dev/null || true)"
    echo "session_dir=$session_dir" >> "$dbg"
    if [ -n "$session_dir" ] && [ -d "$session_dir/logs" ]; then
        echo "$session_dir" > "$head_ray_dest/session_path.txt"
        echo "tar-piping head session logs..." >> "$dbg"
        # Use timeout-bounded tar instead of cp -a: cp hung indefinitely on
        # run13 (shell got stuck writing Lustre while Ray was still appending
        # to gcs_server.out). tar handles open files gracefully and timeout
        # guarantees forward progress.
        timeout 60 bash -c "tar -cf - -C '$session_dir/logs' . 2>/dev/null" \
            | tar -xf - -C "$head_ray_dest/" 2>>"$dbg"
        echo "head ray_logs tar rc=${PIPESTATUS[*]}" >> "$dbg"
    fi
    if [ -d /tmp/aurora_inst ]; then
        echo "head aurora_inst listing:" >> "$dbg"
        ls /tmp/aurora_inst >> "$dbg" 2>&1
        timeout 30 bash -c "tar -cf - -C /tmp/aurora_inst . 2>/dev/null" \
            | tar -xf - -C "$head_inst_dest/" 2>>"$dbg"
        echo "head inst tar rc=${PIPESTATUS[*]}" >> "$dbg"
    else
        echo "head has no /tmp/aurora_inst dir" >> "$dbg"
    fi
    local head_ray_count=$(ls "$head_ray_dest" 2>/dev/null | wc -l)
    local head_inst_count=$(ls "$head_inst_dest" 2>/dev/null | wc -l)
    echo "head_ray_count=$head_ray_count head_inst_count=$head_inst_count" >> "$dbg"
    echo "[finalize] head collected: ray_logs=$head_ray_count files, inst=$head_inst_count files"

    # Step 2: fan out tar-over-ssh to worker nodes in parallel (skip self).
    echo "entering worker loop" >> "$dbg"
    local pids=()
    local loop_iterations=0
    while read -r node; do
        [ -z "$node" ] && continue
        loop_iterations=$((loop_iterations + 1))
        local short_node="${node%%.*}"
        [ "$short_node" = "$self_short" ] && continue
        {
            local ray_dest="$ray_log_root/$short_node"
            local inst_dest="$inst_root/$short_node"
            mkdir -p "$ray_dest" "$inst_dest"
            timeout 60 ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
                "$node" '
                    sp=$(readlink -f /tmp/ray/session_latest 2>/dev/null || true);
                    echo "$sp" > /tmp/_session_path_out;
                    if [ -n "$sp" ] && [ -d "$sp/logs" ]; then
                        cd "$sp/logs" && tar cf - . 2>/dev/null;
                    fi
                ' 2>/dev/null \
                | tar xf - -C "$ray_dest/" 2>/dev/null || true
            timeout 10 ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no \
                "$node" "cat /tmp/_session_path_out 2>/dev/null || true" \
                > "$ray_dest/session_path.txt" 2>/dev/null || true
            timeout 30 ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no \
                "$node" '
                    if [ -d /tmp/aurora_inst ]; then
                        cd /tmp/aurora_inst && tar cf - . 2>/dev/null;
                    fi
                ' 2>/dev/null \
                | tar xf - -C "$inst_dest/" 2>/dev/null || true
        } &
        pids+=($!)
    done < "$UNIQUE_NODES_FILE"

    # Each background job is already bounded by per-command `timeout` on the
    # ssh/scp steps (30s+60s+30s worst case = 120s). They finish naturally;
    # just wait for all of them. Previous attempt used `timeout N bash -c "wait PID"`
    # which runs the inner `wait` in a subshell that is NOT the parent of the
    # backgrounded jobs — wait returns 127 immediately, the outer `timeout`
    # exits nonzero, and the else-branch `kill`ed every SCP before it copied
    # anything. Job 8444548 hit this bug (all 32 dirs created but 0 payloads).
    echo "loop_iterations=$loop_iterations pids_spawned=${#pids[@]}" >> "$dbg"
    wait 2>/dev/null || true
    echo "finalize end $(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$dbg"

    local ray_collected=$(find "$ray_log_root" -maxdepth 1 -mindepth 1 -type d | wc -l)
    local inst_collected=$(find "$inst_root" -maxdepth 1 -mindepth 1 -type d | wc -l)
    echo "[System] Instrumentation data: $inst_root ($inst_collected/$(wc -l < "$UNIQUE_NODES_FILE") nodes collected)"
    echo "[System] Ray logs: $ray_log_root ($ray_collected/$(wc -l < "$UNIQUE_NODES_FILE") nodes collected)"
    echo "[System] Persistent run log: $RUN_LOG_FILE"
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

# Timeout patches (HTTP_PROXY_TIMEOUT, PROXY_HEALTH_CHECK_TIMEOUT_S,
# PROXY_READY_CHECK_TIMEOUT_S, PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD,
# DEFAULT_HEALTH_CHECK_*, REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD) are
# applied directly in the overlaid constants.py (see Ray overlay section
# below and the overlay git repo at
# ~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray).
# Prior approach: sitecustomize/usercustomize monkey-patching, removed on
# perf-inst-dev for clean profiling. Restore from main-repo commit 2f32633
# if needed.


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

# ProxyActor JSON collection gate: aurora_serve.py:1846 reads
# /tmp/aurora_inst/proxy_init_*.json written by the overlaid proxy.py.
# Default on for perf-inst-dev. Set to 0 to skip collection.
export AURORA_PROXY_PROFILE="${AURORA_PROXY_PROFILE:-1}"
echo "[System] AURORA_PROXY_PROFILE=$AURORA_PROXY_PROFILE"

# Ray overlay: replace ray/serve/_private/{proxy,controller,proxy_state,constants}.py
# (whichever exist in the overlay dir) with our patched versions. Source of truth:
# ~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray (git-tracked).
export AURORA_RAY_OVERLAY="${AURORA_RAY_OVERLAY:-1}"
echo "[System] AURORA_RAY_OVERLAY=$AURORA_RAY_OVERLAY"

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

# --- Ray overlay: patched files via symlink tree ---
# Creates /tmp/ray_overlay on each node with symlinks to the system ray
# package, replacing any file found in OVERLAY_ROOT with our patched version.
# This is prepended to PYTHONPATH so Python finds our patched files first.
#
# The overlay source dir is a git-tracked repo; see commit history there
# for the full patch set. Any *.py under serve/_private/ that exists in the
# overlay gets copied in; anything missing falls through to the system ray.
OVERLAY_ROOT="$HOME/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray"
if [ "${AURORA_RAY_OVERLAY:-1}" = "1" ] && [ -d "$OVERLAY_ROOT/serve/_private" ]; then
    SYSRAY="$(dirname "$(dirname "$PYTHON_EXEC")")/lib/python3.12/site-packages/ray"
    if [ -d "$SYSRAY/serve/_private" ]; then
        RAY_OVERLAY="/tmp/ray_overlay"
        # List patched files (just the filenames under serve/_private/).
        # Enumerated at launch-script generation time so the remote script is
        # self-contained (doesn't need OVERLAY_ROOT at runtime on workers).
        PATCHED_FILES=$(cd "$OVERLAY_ROOT/serve/_private" && ls *.py 2>/dev/null | tr '\n' ' ')
        echo "[System] Ray overlay: patched serve/_private files = $PATCHED_FILES"

        # Script must live on shared storage so SSHed workers can read it.
        # /tmp is local tmpfs per-node, so use $HOME (Lustre).
        _OVERLAY_SCRIPT=$(mktemp "$HOME/.aurora_overlay_XXXX.sh")
        cat > "$_OVERLAY_SCRIPT" << OVERLAYEOF
#!/bin/bash
set -e
rm -rf $RAY_OVERLAY
mkdir -p $RAY_OVERLAY/ray/serve/_private
# Symlink top-level ray/* (excluding serve)
for f in $SYSRAY/*; do n=\$(basename \$f); [ "\$n" = serve ] && continue; ln -s "\$f" $RAY_OVERLAY/ray/\$n 2>/dev/null; done
# Symlink ray/serve/* (excluding _private)
for f in $SYSRAY/serve/*; do n=\$(basename \$f); [ "\$n" = _private ] && continue; ln -s "\$f" $RAY_OVERLAY/ray/serve/\$n 2>/dev/null; done
# Symlink ray/serve/_private/* except the patched files and __pycache__
PATCHED="$PATCHED_FILES"
for f in $SYSRAY/serve/_private/*; do
    n=\$(basename \$f)
    [ "\$n" = __pycache__ ] && continue
    # Skip if this filename is in the patched list
    skip=0
    for p in \$PATCHED; do [ "\$n" = "\$p" ] && skip=1 && break; done
    [ \$skip -eq 1 ] && continue
    ln -s "\$f" $RAY_OVERLAY/ray/serve/_private/\$n 2>/dev/null
done
# Copy each patched file from OVERLAY_ROOT (dereference to avoid stale cache)
for p in \$PATCHED; do
    cp "$OVERLAY_ROOT/serve/_private/\$p" "$RAY_OVERLAY/ray/serve/_private/\$p"
done
OVERLAYEOF
        chmod +x "$_OVERLAY_SCRIPT"
        # Run locally (head node) first
        bash "$_OVERLAY_SCRIPT"
        # Run on remote nodes in parallel (script lives on shared Lustre)
        for node in $(sort -u "$UNIQUE_NODES_FILE"); do
            short="${node%%.*}"
            [ "$short" = "$HOSTNAME_SHORT" ] && continue
            ssh -o StrictHostKeyChecking=no -o ConnectTimeout=10 -f "$node" "bash $_OVERLAY_SCRIPT" 2>/dev/null
        done
        sleep 5  # give SSH background processes time to complete
        rm -f "$_OVERLAY_SCRIPT"
        export PYTHONPATH="$RAY_OVERLAY:$PYTHONPATH"
        echo "[System] Ray overlay active at $RAY_OVERLAY (from $OVERLAY_ROOT)"
    fi
fi

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
