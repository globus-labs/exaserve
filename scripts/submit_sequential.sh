#!/bin/bash
# Submit HAProxy and direct-mode experiments sequentially.
# Alternates between haproxy and direct for each node count,
# waiting for each job to finish before submitting the next.
set -e

RUNS_ROOT="/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs"
RUN_GROUP="run3"

check_result() {
    local mode=$1 nodes=$2
    local result_file="$RUNS_ROOT/weakscaling_${mode}/${RUN_GROUP}/${nodes}-nodes/results/result0.json"
    [ -f "$result_file" ]
}

get_status() {
    local mode=$1 nodes=$2
    local state_file="$RUNS_ROOT/weakscaling_${mode}/${RUN_GROUP}/${nodes}-nodes/state/status.json"
    python3 -c "import json; print(json.load(open('$state_file'))['status'])" 2>/dev/null || echo "unknown"
}

submit_and_wait() {
    local mode=$1 nodes=$2
    local run_yaml="$RUNS_ROOT/weakscaling_${mode}/${RUN_GROUP}/${nodes}-nodes/run.yaml"

    if check_result "$mode" "$nodes"; then
        echo "[OK] ${mode} ${nodes}-nodes: already has results, skipping"
        return 0
    fi

    echo "[SUBMIT] ${mode} ${nodes}-nodes..."
    local job_id
    job_id=$(source ~/script/env_aurora 2>/dev/null; python3 -m eval.cli run submit "$run_yaml" 2>&1)

    if [[ "$job_id" == *"exceed"* ]] || [[ "$job_id" == *"Error"* ]]; then
        echo "[WAIT] Queue full, waiting for slot..."
        while true; do
            sleep 30
            job_id=$(source ~/script/env_aurora 2>/dev/null; python3 -m eval.cli run submit "$run_yaml" 2>&1)
            if [[ "$job_id" != *"exceed"* ]] && [[ "$job_id" != *"Error"* ]]; then
                break
            fi
        done
    fi

    echo "[QUEUED] ${mode} ${nodes}-nodes: $job_id"

    # Wait for job to complete
    local max_wait=3600
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        sleep 30
        elapsed=$((elapsed + 30))
        local state
        state=$(qstat -f "$job_id" 2>/dev/null | grep "job_state" | awk '{print $NF}' || echo "?")

        if [ "$state" = "F" ] || [ "$state" = "C" ] || [ "$state" = "E" ] || [ "$state" = "?" ]; then
            # Job finished
            sleep 5  # Give filesystem a moment
            if check_result "$mode" "$nodes"; then
                echo "[DONE] ${mode} ${nodes}-nodes: results OK (${elapsed}s)"
                return 0
            else
                # Check if status file says succeeded
                local run_status
                run_status=$(get_status "$mode" "$nodes")
                if [ "$run_status" = "succeeded" ]; then
                    echo "[DONE] ${mode} ${nodes}-nodes: succeeded (${elapsed}s)"
                    return 0
                else
                    echo "[WARN] ${mode} ${nodes}-nodes: job finished but status=$run_status (${elapsed}s)"
                    return 1
                fi
            fi
        fi

        # Print progress every 2 minutes
        if [ $((elapsed % 120)) -eq 0 ]; then
            echo "[WAIT] ${mode} ${nodes}-nodes: state=$state elapsed=${elapsed}s"
        fi
    done

    echo "[TIMEOUT] ${mode} ${nodes}-nodes after ${max_wait}s"
    return 1
}

echo "=== Sequential experiment submission ==="
echo "Start: $(date -u)"
echo ""

# Submit in order: small nodes first, alternating modes
for nodes in 4 8 16 32 64 128; do
    for mode in haproxy direct; do
        submit_and_wait "$mode" "$nodes" || echo "[SKIP] ${mode} ${nodes}-nodes failed, continuing..."
        echo ""
    done
done

echo "=== All submissions complete ==="
echo "End: $(date -u)"

# Print final results
echo ""
echo "=== Final Results ==="
source ~/script/env_aurora 2>/dev/null
python3 -c "
import json, os

results = {}
for mode in ['haproxy', 'direct']:
    base = '$RUNS_ROOT/weakscaling_' + mode + '/$RUN_GROUP'
    for d in sorted(os.listdir(base)):
        path = os.path.join(base, d, 'results', 'result0.json')
        if os.path.exists(path):
            with open(path) as f:
                data = json.load(f)
            o = data['overall']
            nodes = int(d.split('-')[0])
            results[(mode, nodes)] = o

print(f\"{'Mode':<10} {'Nodes':>5} {'Target':>7} {'RPS':>8} {'Eff%':>5} {'P50(ms)':>8} {'P99(ms)':>8} {'Err':>4} {'Dur(s)':>7}\")
print('-' * 70)
for (mode, nodes), o in sorted(results.items(), key=lambda x: (x[0][0], x[0][1])):
    target = 17.5 * nodes
    eff = o['rps'] / target * 100 if target > 0 else 0
    print(f\"{mode:<10} {nodes:>5} {target:>7.1f} {o['rps']:>8.1f} {eff:>4.0f}% {o['p50_s']*1000:>8.0f} {o['p99_s']*1000:>8.0f} {o.get('errors',0):>4} {o['duration_s']:>7.1f}\")
"
