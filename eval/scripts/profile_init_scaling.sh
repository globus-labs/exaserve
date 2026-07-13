#!/bin/bash
# Profile replica init time at increasing node counts to show scaling behavior.
# Usage: profile_init_scaling.sh <config.yaml> [max_nodes]
#
# Runs null-compute deploys at 1, 2, 4, 8, 16, 32, 64, 128 nodes and
# collects the INIT TOTAL and serve.run() timings from logs.
# Requires: PBS_NODEFILE set, env_aurora sourced.
set -eo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

CONFIG="${1:-$PROJECT_ROOT/config.yaml}"
MAX_NODES="${2:-256}"

if [ ! -f "$CONFIG" ]; then
    echo "ERROR: Config not found: $CONFIG"
    exit 1
fi
if [ -z "$PBS_NODEFILE" ] || [ ! -f "$PBS_NODEFILE" ]; then
    echo "ERROR: PBS_NODEFILE not set or missing"
    exit 1
fi

TOTAL_NODES=$(sort -u "$PBS_NODEFILE" | wc -l)
echo "=== Init Scaling Profile ==="
echo "Config: $CONFIG"
echo "Total nodes available: $TOTAL_NODES"
echo "Max nodes to test: $MAX_NODES"
echo ""

RESULTS_DIR="$PROJECT_ROOT/tmp/profile_init_scaling_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$RESULTS_DIR"

# Generate node counts: 1, 2, 4, 8, ..., up to MAX_NODES
NODE_COUNTS=()
n=1
while [ "$n" -le "$MAX_NODES" ] && [ "$n" -le "$TOTAL_NODES" ]; do
    NODE_COUNTS+=("$n")
    n=$((n * 2))
done

echo "Node counts to test: ${NODE_COUNTS[*]}"
echo ""

# Summary file
SUMMARY="$RESULTS_DIR/summary.csv"
echo "nodes,gpus,replicas,serve_run_s,mean_init_s,max_init_s,min_init_s,proxy_count,status" > "$SUMMARY"

for NC in "${NODE_COUNTS[@]}"; do
    echo "--- Testing $NC nodes ---"
    RUN_DIR="$RESULTS_DIR/${NC}n"
    mkdir -p "$RUN_DIR"

    # Create a truncated nodefile with NC unique nodes
    SUBSET_NODEFILE="$RUN_DIR/nodefile"
    sort -u "$PBS_NODEFILE" | head -n "$NC" > "$SUBSET_NODEFILE"
    ACTUAL_NC=$(wc -l < "$SUBSET_NODEFILE")

    echo "  Nodes: $ACTUAL_NC (from $SUBSET_NODEFILE)"

    # Clean any leftover Ray processes on these nodes
    while read -r node; do
        ssh "$node" "pkill -9 -u $USER -f 'ray|serve|vllm' 2>/dev/null; rm -rf /tmp/ray 2>/dev/null" &
    done < "$SUBSET_NODEFILE"
    wait
    sleep 2

    # Run with null-compute, capture output
    LOG="$RUN_DIR/launch.log"
    export EXASERVE_NULL_COMPUTE=1
    export EXASERVE_NULL_COMPUTE_LATENCY=0.1
    export PBS_NODEFILE="$SUBSET_NODEFILE"
    export EXASERVE_RUN_LOG_DIR="$RUN_DIR"
    export EXASERVE_RUN_LOG_FILE="$LOG"
    export EXASERVE_PROJECT_LOGGING_INITIALIZED=1

    # Timeout: 10 min per run (generous)
    timeout 600 bash "$PROJECT_ROOT/src/exaserve/resources/launch_cluster.sh" "$CONFIG" > "$LOG" 2>&1 &
    LAUNCH_PID=$!

    # Wait for "CLUSTER FULLY READY" or timeout
    READY=0
    for i in $(seq 1 120); do
        if ! kill -0 "$LAUNCH_PID" 2>/dev/null; then
            break
        fi
        if grep -q "CLUSTER FULLY READY" "$LOG" 2>/dev/null; then
            READY=1
            break
        fi
        sleep 5
    done

    # Collect results
    if [ "$READY" -eq 1 ]; then
        # Extract serve.run() time
        SERVE_RUN_TIME=$(grep -oP 'serve\.run\(\) call: \K[0-9.]+' "$LOG" | tail -1 || echo "?")

        # Extract all INIT TOTAL times
        INIT_TIMES=$(grep -oP 'INIT TOTAL: \K[0-9.]+' "$LOG" || echo "")
        if [ -n "$INIT_TIMES" ]; then
            MEAN_INIT=$(echo "$INIT_TIMES" | awk '{s+=$1; n++} END {printf "%.2f", s/n}')
            MAX_INIT=$(echo "$INIT_TIMES" | sort -n | tail -1)
            MIN_INIT=$(echo "$INIT_TIMES" | sort -n | head -1)
            NUM_INITS=$(echo "$INIT_TIMES" | wc -l)
        else
            MEAN_INIT="?" ; MAX_INIT="?" ; MIN_INIT="?" ; NUM_INITS=0
        fi

        # Extract proxy count
        PROXY_COUNT=$(grep -oP 'proxies=\K[0-9]+' "$LOG" | tail -1 || echo "?")

        GPUS=$((ACTUAL_NC * 12))
        echo "  READY: serve.run=${SERVE_RUN_TIME}s, inits=${NUM_INITS}, mean=${MEAN_INIT}s, max=${MAX_INIT}s, proxies=${PROXY_COUNT}"
        echo "${ACTUAL_NC},${GPUS},${NUM_INITS},${SERVE_RUN_TIME},${MEAN_INIT},${MAX_INIT},${MIN_INIT},${PROXY_COUNT},OK" >> "$SUMMARY"
        STATUS="OK"
    else
        echo "  FAILED or TIMEOUT"
        echo "${ACTUAL_NC},$((ACTUAL_NC*12)),?,?,?,?,?,?,FAIL" >> "$SUMMARY"
        STATUS="FAIL"
    fi

    # Kill the cluster
    kill "$LAUNCH_PID" 2>/dev/null || true
    wait "$LAUNCH_PID" 2>/dev/null || true

    # Clean up Ray on all nodes
    while read -r node; do
        ssh "$node" "pkill -9 -u $USER -f 'ray|serve|vllm' 2>/dev/null; rm -rf /tmp/ray 2>/dev/null" &
    done < "$SUBSET_NODEFILE"
    wait
    sleep 5

    echo ""
done

echo "=== Results ==="
cat "$SUMMARY"
echo ""
echo "Full results: $RESULTS_DIR"
