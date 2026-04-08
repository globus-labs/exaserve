#!/bin/bash
# submit_and_wait.sh - Submit experiment runs sequentially, waiting for completion
# Usage: bash scripts/submit_and_wait.sh <spec_name> <run_group>
# Example: bash scripts/submit_and_wait.sh weakscaling_haproxy run3

set -e
SPEC_NAME="$1"
RUN_GROUP="${2:-run3}"
EXPERIMENTS_ROOT="/lus/flare/projects/AuroraGPT/wenyiw/data/experiments"
RUN_DIR="$EXPERIMENTS_ROOT/runs/$SPEC_NAME/$RUN_GROUP"

if [ ! -d "$RUN_DIR" ]; then
    echo "ERROR: $RUN_DIR does not exist"
    exit 1
fi

# Node count order for submission
NODE_COUNTS=(1 2 4 8 16 32 64 128)

for N in "${NODE_COUNTS[@]}"; do
    VARIANT_DIR="$RUN_DIR/${N}-nodes"
    RESULT_FILE="$VARIANT_DIR/results/result0.json"
    RUN_YAML="$VARIANT_DIR/run.yaml"
    JOB_PBS="$VARIANT_DIR/job/job.pbs"

    if [ ! -f "$RUN_YAML" ]; then
        echo "[SKIP] $N-nodes: no run.yaml"
        continue
    fi

    if [ -f "$RESULT_FILE" ]; then
        echo "[DONE] $N-nodes: result0.json exists"
        continue
    fi

    echo "[SUBMIT] $N-nodes ..."

    # Try to submit, retry if queue is full
    MAX_RETRIES=60
    RETRY=0
    JOB_ID=""
    while [ -z "$JOB_ID" ] && [ $RETRY -lt $MAX_RETRIES ]; do
        JOB_ID=$(qsub "$JOB_PBS" 2>/dev/null) || true
        if [ -z "$JOB_ID" ]; then
            RETRY=$((RETRY + 1))
            echo "  Queue full, waiting 60s... (attempt $RETRY/$MAX_RETRIES)"
            sleep 60
        fi
    done

    if [ -z "$JOB_ID" ]; then
        echo "[FAIL] $N-nodes: could not submit after $MAX_RETRIES attempts"
        continue
    fi

    echo "  Job: $JOB_ID"

    # Wait for job to complete
    while true; do
        STATE=$(qstat -f "$JOB_ID" 2>/dev/null | grep "job_state" | awk '{print $NF}' || echo "")
        if [ "$STATE" = "C" ] || [ "$STATE" = "E" ] || [ -z "$STATE" ]; then
            # Job completed or gone from scheduler
            break
        fi
        sleep 30
    done

    # Check result
    sleep 5  # brief grace for NFS sync
    if [ -f "$RESULT_FILE" ]; then
        echo "[OK]   $N-nodes: result0.json saved"
    else
        echo "[WARN] $N-nodes: job completed but no result0.json"
        # Check stderr for clues
        STDERR_DIR="$VARIANT_DIR/logs/pbs/stderr"
        if [ -d "$STDERR_DIR" ]; then
            echo "  Stderr tail:"
            tail -5 "$STDERR_DIR"/*.ER 2>/dev/null || true
        fi
    fi
done

echo ""
echo "=== Summary ==="
for N in "${NODE_COUNTS[@]}"; do
    RESULT_FILE="$RUN_DIR/${N}-nodes/results/result0.json"
    if [ -f "$RESULT_FILE" ]; then
        echo "  $N-nodes: OK"
    else
        echo "  $N-nodes: MISSING"
    fi
done
