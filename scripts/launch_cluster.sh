#!/bin/bash
set -e # Fail fast if anything goes wrong

# --- 1. Environment Setup ---
# Check if we are inside a PBS job or interactive session
if [ -z "$PBS_NODEFILE" ]; then
    echo "ERROR: \$PBS_NODEFILE not found. Are you in a debug session (qsub -I)?"
    exit 1
fi

# Go to the project root (assuming script is run from project root or scripts dir)
# Get the absolute path of the directory containing this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

echo "[System] Project Root: $PROJECT_ROOT"
echo "[System] Nodefile: $PBS_NODEFILE"

# --- 2. IP Resolution (The Scout) ---
echo "[System] Resolving Head Node IP..."
HEAD_IP=$(python3 scripts/resolve_ip.py)

if [ -z "$HEAD_IP" ]; then
    echo "ERROR: Failed to resolve Head IP."
    exit 1
fi

echo "[System] Head IP (HSN): $HEAD_IP"

# --- 3. Calculate Node Count ---
# PBS_SELECT might not be set in all interactive modes, rely on nodefile line count
NODE_COUNT=$(wc -l < $PBS_NODEFILE)
echo "[System] Total Nodes: $NODE_COUNT"

# --- 4. Atomic Launch ---
echo "[System] Launching Cluster..."

export ZE_FLAT_DEVICE_HIERARCHY="FLAT"
export ZE_AFFINITY_MASK=""
export RAY_EXPERIMENTAL_NOSET_XPU_VISIBLE_DEVICES="1"

# REMOVED: -f $PBS_NODEFILE
# ADDED: Full path to python (Safety best practice)
PYTHON_EXEC=$(which python3)

mpiexec -n $NODE_COUNT -ppn 1 --cpu-bind depth \
    $PYTHON_EXEC src/driver.py --head-ip $HEAD_IP --port 6379
