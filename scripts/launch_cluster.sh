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

echo "[System] Project Root: $PROJECT_ROOT"
echo "[System] Nodefile: $PBS_NODEFILE"

# Use debug_libs
# export PYTHONPATH="/home/wenyiw/debug_libs:$PYTHONPATH"
echo "[System] PYTHONPATH: $PYTHONPATH"
# --- 2. IP Resolution (The Scout) ---
echo "[System] Resolving Head Node IP..."
# HEAD_IP=$(python3 scripts/resolve_ip.py)
HEAD_IP=$(getent hosts $(hostname).hsn.cm.aurora.alcf.anl.gov | awk '{ print $1 }' | tr ' ' '\n' | sort | head -n 1)


if [ -z "$HEAD_IP" ]; then
    echo "ERROR: Failed to resolve Head IP."
    exit 1
fi

echo "[System] Head IP (HSN): $HEAD_IP"

# --- 3. Calculate Node Count ---
NODE_COUNT=$(wc -l < $PBS_NODEFILE)
echo "[System] Total Nodes: $NODE_COUNT"

# --- 4. Atomic Launch ---
echo "[System] Launching Cluster..."

export ZE_FLAT_DEVICE_HIERARCHY="FLAT"
export ZE_AFFINITY_MASK=""
export CCL_PROCESS_LAUNCHER="hydra"

# NOSET=1 + all-tiles-visible prevents the SYCL crash for non-GPU actors.
# Each ModelWorker narrows ONEAPI_DEVICE_SELECTOR to its assigned tile
# before forking the vLLM EngineCore subprocess, giving real per-tile
# device isolation (Level Zero reads the selector fresh in the child).
export RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR="1"
export ONEAPI_DEVICE_SELECTOR="level_zero:0,1,2,3,4,5,6,7,8,9,10,11"

PYTHON_EXEC=$(which python3)

mpiexec -n $NODE_COUNT -ppn 1 --cpu-bind none \
    $PYTHON_EXEC src/driver.py --head-ip $HEAD_IP --port 6379
