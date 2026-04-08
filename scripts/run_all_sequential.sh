#!/bin/bash
# run_all_sequential.sh - Run ALL remaining experiments one at a time in order
set -e

EXPERIMENTS_ROOT="/lus/flare/projects/AuroraGPT/wenyiw/data/experiments"
RUN_GROUP="run3"

submit_and_wait_one() {
    local spec=$1 nodes=$2
    local result="$EXPERIMENTS_ROOT/runs/$spec/$RUN_GROUP/${nodes}-nodes/results/result0.json"
    local job_pbs="$EXPERIMENTS_ROOT/runs/$spec/$RUN_GROUP/${nodes}-nodes/job/job.pbs"

    if [ -f "$result" ]; then
        echo "[SKIP] $spec ${nodes}-nodes: already done"
        return 0
    fi

    echo "[SUBMIT] $spec ${nodes}-nodes ..."

    # Wait for queue slot
    while true; do
        local job_id=$(qsub "$job_pbs" 2>&1) || true
        if echo "$job_id" | grep -q "aurora-pbs"; then
            echo "  Job: $job_id"
            break
        fi
        sleep 30
    done

    # Wait for completion
    echo "  Waiting for completion..."
    local max_wait=3600
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        if [ -f "$result" ]; then
            # Parse result
            local rps=$(python3 -c "import json; d=json.load(open('$result')); print(f\"{d['overall']['rps']:.2f}\")" 2>/dev/null || echo "?")
            local errors=$(python3 -c "import json; d=json.load(open('$result')); print(d['overall']['errors'])" 2>/dev/null || echo "?")
            echo "  [OK] rps=$rps errors=$errors"
            return 0
        fi
        sleep 30
        elapsed=$((elapsed + 30))
    done

    echo "  [TIMEOUT] No result after ${max_wait}s"
    return 1
}

echo "=== Sequential experiment runner ==="
echo "Start: $(date)"
echo ""

# HAProxy first, then direct - from small to large
for spec in weakscaling_haproxy weakscaling_direct; do
    echo "--- $spec ---"
    for N in 4 8 16 32 64 128; do
        submit_and_wait_one "$spec" "$N"
    done
    echo ""
done

echo "=== Done: $(date) ==="
echo ""

# Print final summary
echo "=== Final Results ==="
source ~/script/env_aurora 2>/dev/null
python3 -c "
import json, glob
for spec in ['weakscaling_haproxy', 'weakscaling_direct']:
    print(f'--- {spec} ---')
    for n in [1,2,4,8,16,32,64,128]:
        f = f'$EXPERIMENTS_ROOT/runs/{spec}/$RUN_GROUP/{n}-nodes/results/result0.json'
        try:
            d = json.load(open(f))
            o = d['overall']
            target = n * 17.5
            eff = o['rps'] / target * 100
            print(f'  {n:>3} nodes: rps={o[\"rps\"]:>8.2f}/{target:>7.0f} ({eff:>5.1f}%) err={o[\"errors\"]} p50={o[\"p50_s\"]:.3f}s p99={o[\"p99_s\"]:.3f}s')
        except FileNotFoundError:
            print(f'  {n:>3} nodes: MISSING')
"
