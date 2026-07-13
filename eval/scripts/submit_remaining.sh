#!/bin/bash
# Submit remaining HAProxy and direct-mode experiments.
set -e

RUNS_ROOT="/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs"
RUN_GROUP="run3"
cd /home/wenyiw/exaserve
source ~/script/env_aurora 2>/dev/null

check_result() {
    [ -f "$RUNS_ROOT/weakscaling_${1}/${RUN_GROUP}/${2}-nodes/results/result0.json" ]
}

submit_and_wait() {
    local mode=$1 nodes=$2
    local run_yaml="$RUNS_ROOT/weakscaling_${mode}/${RUN_GROUP}/${nodes}-nodes/run.yaml"

    if check_result "$mode" "$nodes"; then
        echo "[SKIP] ${mode} ${nodes}: already done"
        return 0
    fi

    # Wait until no non-capacity jobs are queued
    while true; do
        local queued
        queued=$(qstat -u wenyiw 2>/dev/null | grep " Q " | grep -v "capacity" | wc -l)
        [ "$queued" -eq 0 ] && break
        sleep 30
    done

    echo "[SUBMIT] ${mode} ${nodes}..."
    local job_id
    job_id=$(python3 -m eval.cli run submit "$run_yaml" 2>&1)
    if [[ "$job_id" == *"exceed"* ]] || [[ "$job_id" == *"Error"* ]]; then
        echo "[ERROR] ${mode} ${nodes}: $job_id"
        sleep 60
        return 1
    fi
    echo "[QUEUED] ${mode} ${nodes}: $job_id"

    local elapsed=0
    while [ $elapsed -lt 3600 ]; do
        sleep 30
        elapsed=$((elapsed + 30))
        if check_result "$mode" "$nodes"; then
            echo "[DONE] ${mode} ${nodes} (${elapsed}s)"
            return 0
        fi
        local state
        state=$(qstat -f "$job_id" 2>/dev/null | grep "job_state" | awk '{print $NF}' || echo "F")
        if [ "$state" = "F" ] || [ "$state" = "C" ] || [ "$state" = "E" ]; then
            sleep 10
            if check_result "$mode" "$nodes"; then
                echo "[DONE] ${mode} ${nodes} (${elapsed}s)"
                return 0
            fi
            echo "[FAIL] ${mode} ${nodes}: no results"
            return 1
        fi
        [ $((elapsed % 120)) -eq 0 ] && echo "[WAIT] ${mode} ${nodes}: state=$state ${elapsed}s"
    done
    echo "[TIMEOUT] ${mode} ${nodes}"
    return 1
}

echo "=== Remaining submissions: $(date -u) ==="
for nodes in 4 8 16 32 64 128; do
    for mode in haproxy direct; do
        submit_and_wait "$mode" "$nodes" || echo "[SKIP] ${mode} ${nodes} failed"
    done
done
echo "=== Done: $(date -u) ==="
