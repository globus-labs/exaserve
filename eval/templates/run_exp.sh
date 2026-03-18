#!/bin/bash
set -e  # Exit on error

# ==============================================================================
# UNIVERSAL EXPERIMENT DRIVER
# ==============================================================================
# USAGE: ./run_exp.sh <path_to_config.yaml> [--backend {ray|mpi}] [--dest {proxy|direct}]
#
# This script:
# 1. Validates all required files and environment
# 2. Activates the Python environment
# 3. Launches the appropriate backend service (Ray orchestrator or MPI API server)
# 4. Waits for service health
# 5. Runs the replay client benchmark (with --include-tp for MPI)
# 6. Cleans up on exit
# ==============================================================================

CONFIG_PATH=""
BACKEND="ray"  # Default to ray if not specified
NUM_RUNS=1
DEST=""  # Empty = defer to job_replay_client_config.dest in the YAML config

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --backend)
            BACKEND="$2"
            shift 2
            ;;
        --no-warmup)
            # Legacy flag, ignored — warmup is now controlled by Go client config
            shift
            ;;
        --num-runs)
            NUM_RUNS="$2"
            shift 2
            ;;
        --dest|--destination)
            DEST="$2"
            shift 2
            ;;
        *)
            if [ -z "$CONFIG_PATH" ]; then
                CONFIG_PATH="$1"
            fi
            shift
            ;;
    esac
done

# Validate backend
if [ "$BACKEND" != "ray" ]; then
    echo "!!! ERROR: Invalid backend '$BACKEND'. Must be 'ray'."
    # if [ "$BACKEND" != "ray" ] && [ "$BACKEND" != "mpi" ]; then
    #    echo "!!! ERROR: Invalid backend '$BACKEND'. Must be 'ray' or 'mpi'."
    exit 1
    # fi
fi

# Locate script directory and derive project root (works in both dev repo and snapshots)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LAUNCH_CLUSTER_SCRIPT="$PROJECT_ROOT/scripts/launch_cluster.sh"
REPLAY_CLIENT_SCRIPT="$PROJECT_ROOT/eval/replay_client.py"
ENV_SETUP_SCRIPT="/home/wenyiw/script/env_aurora"
# ENV_SETUP_SCRIPT="/home/wenyiw/script/env_local"

# ==============================================================================
# VALIDATION
# ==============================================================================

echo "=================================================="
echo "UNIVERSAL EXPERIMENT DRIVER - VALIDATION"
echo "=================================================="
echo "Backend: $BACKEND"

# Check config file
if [ -z "$CONFIG_PATH" ]; then
    echo "!!! ERROR: No config file provided."
    echo "Usage: $0 <path_to_config.yaml> [--backend {ray|mpi}]"
    exit 1
fi

if [ ! -f "$CONFIG_PATH" ]; then
    echo "!!! ERROR: Config file not found: $CONFIG_PATH"
    exit 1
fi
echo "[✓] Config file:      $CONFIG_PATH"

# If --dest was not given on the CLI, read it from the YAML config
if [ -z "$DEST" ]; then
    DEST=$(python3 -c "
import yaml, sys
c = yaml.safe_load(open('$CONFIG_PATH'))
print(c.get('job_replay_client_config', {}).get('dest', 'proxy'))
" 2>/dev/null || echo "proxy")
fi
# Validate dest value
if [ "$DEST" != "proxy" ] && [ "$DEST" != "direct" ]; then
    echo "!!! ERROR: Invalid --dest '$DEST'. Must be 'proxy' or 'direct'."
    exit 1
fi
echo "[✓] Dest mode:        $DEST"

# Check for trace file in config
TRACE_PATH=$(python3 -c "import yaml; c=yaml.safe_load(open('$CONFIG_PATH')); print(c['benchmark']['output_trace_path'])" 2>/dev/null || echo "")
if [ -n "$TRACE_PATH" ] && [ ! -f "$TRACE_PATH" ]; then
    echo "!!! ERROR: Trace file not found: $TRACE_PATH"
    echo "    Generate trace first with trace_generator.py"
    exit 1
fi
echo "[✓] Trace file:       $TRACE_PATH"

# Check backend-specific scripts
if [ "$BACKEND" == "ray" ]; then
    if [ ! -f "$LAUNCH_CLUSTER_SCRIPT" ]; then
        echo "!!! ERROR: Launch cluster script not found: $LAUNCH_CLUSTER_SCRIPT"
        exit 1
    fi
    echo "[✓] Launch Cluster Script: $LAUNCH_CLUSTER_SCRIPT"
fi

if [ ! -f "$REPLAY_CLIENT_SCRIPT" ]; then
    echo "!!! ERROR: Replay client script not found: $REPLAY_CLIENT_SCRIPT"
    exit 1
fi
echo "[✓] Replay client:    $REPLAY_CLIENT_SCRIPT"

# Build Go dispatch binary if not already built
GO_CLIENT_DIR="$PROJECT_ROOT/eval/go_client"
GO_DISPATCH_BIN="$GO_CLIENT_DIR/bin/go_dispatch"
if [ -f "$GO_CLIENT_DIR/build.sh" ]; then
    bash "$GO_CLIENT_DIR/build.sh"
    if [ -f "$GO_DISPATCH_BIN" ]; then
        echo "[✓] Go dispatch:      $GO_DISPATCH_BIN"
    else
        echo "!!! ERROR: Go dispatch binary not built — Go binary is required"
        exit 1
    fi
fi

# Check environment setup
if [ ! -f "$ENV_SETUP_SCRIPT" ]; then
    echo "!!! ERROR: Environment setup script not found: $ENV_SETUP_SCRIPT"
    exit 1
fi
echo "[✓] Environment setup: $ENV_SETUP_SCRIPT"

echo "=================================================="

# ==============================================================================
# CLEANUP HANDLER
# ==============================================================================

CLEANUP_DONE=false

cleanup() {
    # Prevent duplicate cleanup calls
    if [ "$CLEANUP_DONE" = true ]; then
        return
    fi
    CLEANUP_DONE=true
    
    echo ""
    echo ">>> [DRIVER] Caught EXIT/Signal. Cleaning up..."
    
    # Backend-specific cleanup - Do Ray cleanup FIRST
    if [ "$BACKEND" == "ray" ]; then
        echo "    Stopping Ray cluster forcefully..."
        # Force stop Ray immediately to prevent hanging processes
        ray stop --force 2>/dev/null || true
        # Give it a moment to terminate
        sleep 2
    fi
    
    # Kill the service process and its children
    if [ ! -z "$SERVICE_PID" ]; then
        echo "    Killing Service process group (PID $SERVICE_PID)..."
        # Kill the entire process group to catch all children
        pkill -P $SERVICE_PID 2>/dev/null || true
        kill -TERM $SERVICE_PID 2>/dev/null || true
        
        # Wait briefly for graceful termination
        sleep 1
        
        # Force kill if still running
        if kill -0 $SERVICE_PID 2>/dev/null; then
            echo "    Force killing Service (PID $SERVICE_PID)..."
            kill -KILL $SERVICE_PID 2>/dev/null || true
        fi
    fi
    
    # Kill the background reader process
    if [ ! -z "$READER_PID" ]; then
        kill -TERM $READER_PID 2>/dev/null || true
    fi
    
    # Clean up temporary named pipes and flag files
    rm -f /tmp/aurora_pipe_$$_* 2>/dev/null || true
    rm -f /tmp/aurora_ready_$$_* 2>/dev/null || true
    
    echo ">>> [DRIVER] Cleanup complete."
}
trap cleanup EXIT

# ==============================================================================
# ENVIRONMENT SETUP
# ==============================================================================

echo ""
echo "=================================================="
echo "EXPERIMENT DRIVER - STARTING"
echo "=================================================="
echo "Date:    $(date)"
echo "Host:    $(hostname)"
echo "Backend: $BACKEND"
echo "Code:    $PROJECT_ROOT"
echo "=================================================="

echo ">>> [DRIVER] Setting up environment..."
source "$ENV_SETUP_SCRIPT"
echo ">>> [DRIVER] Conda environment: $CONDA_DEFAULT_ENV"

MAX_EXP_RETRIES=3
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_EXP_RETRIES ]; do
    echo ">>> [DRIVER] Starting Experiment Attempt $((RETRY_COUNT+1))/$MAX_EXP_RETRIES"
    
    # Reset cleanup flag for each attempt
    CLEANUP_DONE=false

    # ==============================================================================
    # LAUNCH BACKEND SERVICE
    # ==============================================================================

    echo ""
    # Create a named pipe for capturing service output
    SERVICE_PIPE="/tmp/aurora_pipe_$$_${RETRY_COUNT}"
    mkfifo "$SERVICE_PIPE"
    
    # Background reader that monitors the pipe for the ready message
    SERVICE_READY_FLAG="/tmp/aurora_ready_$$_${RETRY_COUNT}"
    rm -f "$SERVICE_READY_FLAG"
    
    # Start a background process to read from pipe and detect ready message.
    # driver.py prints "[Driver] ALL SERVICES READY" after both Ray Serve
    # and the optional proxy (LiteLLM/HAProxy) are fully up.
    while IFS= read -r line; do
        echo "$line"  # Echo to stdout for visibility
        if [[ "$line" == *"[Driver] ALL SERVICES READY"* ]]; then
            touch "$SERVICE_READY_FLAG"
        fi
    done < "$SERVICE_PIPE" &
    READER_PID=$!
    
    if [ "$BACKEND" == "ray" ]; then
        echo ">>> [DRIVER] Launching Ray Cluster..."
        bash "$LAUNCH_CLUSTER_SCRIPT" "$CONFIG_PATH" > "$SERVICE_PIPE" 2>&1 &
        SERVICE_PID=$!
        echo "    Ray Cluster PID: $SERVICE_PID"
        echo "    Output Reader PID: $READER_PID"
        SERVICE_PORT=8000
    # elif [ "$BACKEND" == "mpi" ]; then
    #     echo ">>> [DRIVER] Launching MPI API Server..."
    #     python "$MPI_API_SERVER_SCRIPT" serve "$CONFIG_PATH" > "$SERVICE_PIPE" 2>&1 &
    #     SERVICE_PID=$!
    #     echo "    MPI API Server PID: $SERVICE_PID"
    #     echo "    Output Reader PID: $READER_PID"
    #     SERVICE_PORT=8000
    fi

    # ==============================================================================
    # WAIT FOR SERVICE READY
    # ==============================================================================

    echo ""
    echo ">>> [DRIVER] Waiting for AuroraServe to be ready..."
    MAX_RETRIES=720 # 120 minutes
    COUNT=0
    SERVICE_STARTED=true

    while true; do
        # Check if the ready flag file exists (created by background reader)
        if [ -f "$SERVICE_READY_FLAG" ]; then
            # Service is ready - vLLM models are loaded
            break
        fi
        
        sleep 10
        COUNT=$((COUNT+1))
        
        # Check if service died early
        if ! kill -0 $SERVICE_PID 2>/dev/null; then
            echo ""
            echo "!!! [DRIVER] Service died unexpectedly!"
            echo "    The service process terminated before becoming ready."
            SERVICE_STARTED=false
            break
        fi
        
        if [ $COUNT -ge $MAX_RETRIES ]; then
            echo ""
            echo "!!! [DRIVER] Timeout waiting for service (${MAX_RETRIES} retries)."
            echo "    Service did not become ready in 1000 seconds."
            echo "    The ready message '[AuroraServe] Service available...' was not detected."
            SERVICE_STARTED=false
            break
        fi
        echo -ne "    Waiting... ($COUNT/$MAX_RETRIES)\r"
    done

    if [ "$SERVICE_STARTED" = true ]; then
        echo -e "\n[✓] Service is READY."

        # ==============================================================================
        # RUN REPLAY CLIENT
        # proxy mode: always run locally on the head node — no MPI needed since all
        #             traffic goes to the local proxy regardless of cluster size.
        # direct mode: CLIENT_NODES = num_nodes from config
        #              When CLIENT_NODES > 1, launches with mpiexec (one rank per node)
        #              using the first CLIENT_NODES unique hostnames from $PBS_NODEFILE.
        # ==============================================================================

        echo ""

        if [ "$DEST" == "direct" ]; then
            CLIENT_NODES=$(python3 -c "
import yaml
c = yaml.safe_load(open('$CONFIG_PATH'))
cfg = c.get('job_replay_client_config', {})
num_nodes = cfg.get('num_nodes', 1)
print(num_nodes)
" 2>/dev/null || echo "1")
            echo ">>> [DRIVER] Starting Replay Client in direct mode (client nodes: $CLIENT_NODES)..."

            if [ "$CLIENT_NODES" -gt 1 ]; then
                # Build a deduplicated hostfile from the first CLIENT_NODES unique PBS nodes
                CLIENT_HOSTFILE="/tmp/aurora_client_hosts_$$"
                sort -u "$PBS_NODEFILE" | head -n "$CLIENT_NODES" > "$CLIENT_HOSTFILE"
                ACTUAL_CLIENT_NODES=$(wc -l < "$CLIENT_HOSTFILE")
                if [ "$ACTUAL_CLIENT_NODES" -lt "$CLIENT_NODES" ]; then
                    echo "!!! WARNING: Requested $CLIENT_NODES client nodes but $PBS_NODEFILE only has $ACTUAL_CLIENT_NODES unique hostnames. Proceeding with $ACTUAL_CLIENT_NODES."
                    CLIENT_NODES=$ACTUAL_CLIENT_NODES
                fi
                echo "    Client hostfile: $CLIENT_HOSTFILE"
                cat "$CLIENT_HOSTFILE"
                mpiexec -n "$CLIENT_NODES" --ppn 1 --cpu-bind none --hostfile "$CLIENT_HOSTFILE" \
                    python "$REPLAY_CLIENT_SCRIPT" --config "$CONFIG_PATH" --num-runs $NUM_RUNS --dest $DEST
                EXIT_CODE=$?
                rm -f "$CLIENT_HOSTFILE"
            else
                python "$REPLAY_CLIENT_SCRIPT" --config "$CONFIG_PATH" --num-runs $NUM_RUNS --dest $DEST
                EXIT_CODE=$?
            fi
        else
            # proxy mode: single local process, no MPI
            echo ">>> [DRIVER] Starting Replay Client in proxy mode (local, no MPI)..."
            python "$REPLAY_CLIENT_SCRIPT" --config "$CONFIG_PATH" --num-runs $NUM_RUNS --dest $DEST
            EXIT_CODE=$?
        fi
    else
        EXIT_CODE=1
    fi

    # Stop current service using cleanup function
    cleanup

    # Check exit code
    if [ $EXIT_CODE -eq 0 ]; then
        echo ">>> [DRIVER] Experiment succeeded!"
        break
    else
        echo "!!! [DRIVER] Experiment failed with exit code $EXIT_CODE"
        RETRY_COUNT=$((RETRY_COUNT+1))
        if [ $RETRY_COUNT -lt $MAX_EXP_RETRIES ]; then
            echo ">>> [DRIVER] Cleaning up compute nodes before retry..."
            # if [ "$BACKEND" == "mpi" ]; then
            #      # Kill python/sllm-store processes on all nodes allocated to the job
            #      if [ -n "$PBS_NODEFILE" ]; then
            #          pdsh -w ^$PBS_NODEFILE "pkill -9 python; pkill -9 sllm-store" || true
            #      else
            #          # Fallback for local testing or if pdsh/PBS_NODEFILE unavailable
            #          pkill -9 python || true
            #          pkill -9 sllm-store || true
            #      fi
            # fi
            sleep 5
        fi
    fi
done

echo ""
echo "=================================================="
echo "EXPERIMENT DRIVER - FINISHED"
echo "=================================================="
echo "Backend:   $BACKEND"
echo "Exit Code: $EXIT_CODE"
echo "Date:      $(date)"
echo "=================================================="

exit $EXIT_CODE
