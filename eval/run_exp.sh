#!/bin/bash
set -e  # Exit on error

# ==============================================================================
# UNIVERSAL EXPERIMENT DRIVER
# ==============================================================================
# USAGE: ./run_exp.sh <path_to_config.yaml> [--backend {ray|mpi}]
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
NO_WARMUP=""
NUM_RUNS=1

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --backend)
            BACKEND="$2"
            shift 2
            ;;
        --no-warmup)
            NO_WARMUP="--no-warmup"
            shift
            ;;
        --num-runs)
            NUM_RUNS="$2"
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
if [ "$BACKEND" != "ray" ] && [ "$BACKEND" != "mpi" ]; then
    echo "!!! ERROR: Invalid backend '$BACKEND'. Must be 'ray' or 'mpi'."
    exit 1
fi

# Locate script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAY_ORCHESTRATOR_SCRIPT="$SCRIPT_DIR/../multinode_server/orchestrator.py"
MPI_API_SERVER_SCRIPT="$SCRIPT_DIR/../mpi_customized/api_server/run_api_server.py"
REPLAY_CLIENT_SCRIPT="$SCRIPT_DIR/replay_client.py"
ENV_SETUP_SCRIPT="/home/wenyiw/script/prepare_env"

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
    if [ ! -f "$RAY_ORCHESTRATOR_SCRIPT" ]; then
        echo "!!! ERROR: Ray orchestrator script not found: $RAY_ORCHESTRATOR_SCRIPT"
        exit 1
    fi
    echo "[✓] Ray Orchestrator: $RAY_ORCHESTRATOR_SCRIPT"
elif [ "$BACKEND" == "mpi" ]; then
    if [ ! -f "$MPI_API_SERVER_SCRIPT" ]; then
        echo "!!! ERROR: MPI API server script not found: $MPI_API_SERVER_SCRIPT"
        exit 1
    fi
    echo "[✓] MPI API Server:   $MPI_API_SERVER_SCRIPT"
fi

if [ ! -f "$REPLAY_CLIENT_SCRIPT" ]; then
    echo "!!! ERROR: Replay client script not found: $REPLAY_CLIENT_SCRIPT"
    exit 1
fi
echo "[✓] Replay client:    $REPLAY_CLIENT_SCRIPT"

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

cleanup() {
    echo ""
    echo ">>> [DRIVER] Caught EXIT/Signal. Cleaning up..."
    if [ ! -z "$SERVICE_PID" ]; then
        echo "    Killing Service (PID $SERVICE_PID)..."
        kill $SERVICE_PID 2>/dev/null || true
    fi
    
    # Backend-specific cleanup
    if [ "$BACKEND" == "ray" ]; then
        # Force clean Ray just in case
        ray stop --force 2>/dev/null || true
    fi
    
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
echo "Script:  $SCRIPT_DIR"
echo "=================================================="

echo ">>> [DRIVER] Setting up environment..."
source "$ENV_SETUP_SCRIPT"
conda activate mpi-vllm

# Verify conda environment
if [ "$CONDA_DEFAULT_ENV" != "mpi-vllm" ]; then
    echo "!!! ERROR: Failed to activate mpi-vllm conda environment"
    exit 1
fi
echo "[✓] Conda environment: $CONDA_DEFAULT_ENV"

MAX_EXP_RETRIES=3
RETRY_COUNT=0

while [ $RETRY_COUNT -lt $MAX_EXP_RETRIES ]; do
    echo ">>> [DRIVER] Starting Experiment Attempt $((RETRY_COUNT+1))/$MAX_EXP_RETRIES"

    # ==============================================================================
    # LAUNCH BACKEND SERVICE
    # ==============================================================================

    echo ""
    if [ "$BACKEND" == "ray" ]; then
        echo ">>> [DRIVER] Launching Ray Orchestrator..."
        python "$RAY_ORCHESTRATOR_SCRIPT" --config "$CONFIG_PATH" &
        SERVICE_PID=$!
        echo "    Ray Orchestrator PID: $SERVICE_PID"
        SERVICE_PORT=8000
    elif [ "$BACKEND" == "mpi" ]; then
        echo ">>> [DRIVER] Launching MPI API Server..."
        python "$MPI_API_SERVER_SCRIPT" serve "$CONFIG_PATH" &
        SERVICE_PID=$!
        echo "    MPI API Server PID: $SERVICE_PID"
        SERVICE_PORT=8000
    fi

    # ==============================================================================
    # WAIT FOR SERVICE HEALTH
    # ==============================================================================

    echo ""
    echo ">>> [DRIVER] Waiting for Service Health (Port $SERVICE_PORT)..."
    if [ "$BACKEND" == "ray" ]; then
        URL="http://localhost:$SERVICE_PORT/health"
    else
        URL="http://localhost:$SERVICE_PORT/health"
    fi

    MAX_RETRIES=200
    COUNT=0
    SERVICE_STARTED=true

    while ! curl -s "$URL" > /dev/null 2>&1; do
        sleep 5
        COUNT=$((COUNT+1))
        
        # Check if service died early
        if ! kill -0 $SERVICE_PID 2>/dev/null; then
            echo ""
            echo "!!! [DRIVER] Service died unexpectedly!"
            echo "    Check logs for errors."
            SERVICE_STARTED=false
            break
        fi
        
        if [ $COUNT -ge $MAX_RETRIES ]; then
            echo ""
            echo "!!! [DRIVER] Timeout waiting for service (${MAX_RETRIES} retries)."
            echo "    Service did not become healthy in 10 minutes."
            SERVICE_STARTED=false
            break
        fi
        echo -ne "    Waiting... ($COUNT/$MAX_RETRIES)\r"
    done

    if [ "$SERVICE_STARTED" = true ]; then
        echo -e "\n[✓] Service is READY."

        # ==============================================================================
        # RUN REPLAY CLIENT
        # ==============================================================================

        echo ""
        echo ">>> [DRIVER] Starting Replay Client..."

        # Add --include-tp flag for MPI backend
        if [ "$BACKEND" == "mpi" ]; then
            echo "    (Using --include-tp for MPI backend)"
            python "$REPLAY_CLIENT_SCRIPT" --config "$CONFIG_PATH" --include-tp $NO_WARMUP --num-runs $NUM_RUNS
        else
            python "$REPLAY_CLIENT_SCRIPT" --config "$CONFIG_PATH" $NO_WARMUP --num-runs $NUM_RUNS
        fi
        EXIT_CODE=$?
    else
        EXIT_CODE=1
    fi

    # Stop current service
    if [ ! -z "$SERVICE_PID" ]; then
        echo "    Killing Service (PID $SERVICE_PID)..."
        kill $SERVICE_PID 2>/dev/null || true
        wait $SERVICE_PID 2>/dev/null || true
    fi

    # Check exit code
    if [ $EXIT_CODE -eq 0 ]; then
        echo ">>> [DRIVER] Experiment succeeded!"
        break
    else
        echo "!!! [DRIVER] Experiment failed with exit code $EXIT_CODE"
        RETRY_COUNT=$((RETRY_COUNT+1))
        if [ $RETRY_COUNT -lt $MAX_EXP_RETRIES ]; then
            echo ">>> [DRIVER] Cleaning up compute nodes before retry..."
            if [ "$BACKEND" == "mpi" ]; then
                 # Kill python/sllm-store processes on all nodes allocated to the job
                 if [ -n "$PBS_NODEFILE" ]; then
                     pdsh -w ^$PBS_NODEFILE "pkill -9 python; pkill -9 sllm-store" || true
                 else
                     # Fallback for local testing or if pdsh/PBS_NODEFILE unavailable
                     pkill -9 python || true
                     pkill -9 sllm-store || true
                 fi
            fi
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
