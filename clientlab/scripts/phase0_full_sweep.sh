#!/bin/bash
# Phase 0 full sweep: max_ongoing + max_num_seqs on a single node.
exec 2>&1
cd /home/wenyiw/exaserve

source /opt/cray/pe/lmod/default/init/bash 2>/dev/null
source /etc/bash.bashrc 2>/dev/null
module load frameworks 2>/dev/null
module load go 2>/dev/null
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
set -uo pipefail

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/phase0_full_sweep_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
BASE="/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/server_char_llama8b/run5/mor128/runtime/ray_runtime.yaml"
CLIENT="eval/go_client/bin/go_dispatch"

echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="

(cd eval/go_client && bash build.sh 2>&1 | tail -1)

run_one() {
    local LABEL="$1"
    local MOR="$2"
    local MNS="$3"

    echo ""
    echo "--- $LABEL: mor=$MOR mns=$MNS ---"

    python3 << PYEOF
import yaml
m = yaml.safe_load(open("$BASE"))
m["model_deployment_config"]["replica_max_ongoing_requests"] = $MOR
for mc in m["model_deployment_config"]["model_configs"]:
    mc["max_num_seqs"] = $MNS
yaml.dump(m, open("$OUTDIR/runtime_${LABEL}.yaml", "w"), default_flow_style=False)
PYEOF

    export PBS_NODEFILE="$OUTDIR/nodefile"
    echo "$(hostname)" > "$PBS_NODEFILE"

    bash src/exaserve/resources/launch_cluster.sh "$OUTDIR/runtime_${LABEL}.yaml" > "$OUTDIR/cluster_${LABEL}.log" 2>&1 &
    local CPID=$!

    local tries=0
    while ! python3 -c "
from urllib.request import build_opener, ProxyHandler
build_opener(ProxyHandler({})).open('http://127.0.0.1:8000/health', timeout=5)
" 2>/dev/null; do
        tries=$((tries + 1))
        if [ $tries -gt 90 ]; then
            echo "  DEPLOY FAILED"
            kill $CPID 2>/dev/null; wait $CPID 2>/dev/null || true
            ray stop --force 2>/dev/null; sleep 3
            return 1
        fi
        sleep 2
    done
    echo "  Server healthy"

    $CLIENT \
        --mode saturation --base-urls http://127.0.0.1:8000 \
        --max-active-requests 1024 --num-go-workers 4 --sat-stream \
        --sat-model "meta-llama/Meta-Llama-3-8B-Instruct" \
        --sat-prompt-words 2048 --sat-output-tokens 128 \
        --sat-initial-rate 50 --sat-step-duration 10 --sat-warmup-duration 3 \
        --sat-tolerance 0.1 --sat-verify=false \
        --sat-output "$OUTDIR/sat_${LABEL}.json" \
        2>"$OUTDIR/sat_${LABEL}_stderr.log"

    python3 << PYEOF
import json
d = json.load(open("$OUTDIR/sat_${LABEL}.json"))
best = max(d["steps"], key=lambda s: s["achieved_rate"]) if d["steps"] else {}
print(f"  sat={d['saturation_rate']:>4d}  best={best.get('achieved_rate',0):>6.1f}  p99_ttft={best.get('p99_ttft_s',0)*1000:.0f}ms  p99_lat={best.get('p99_latency_s',0)*1000:.0f}ms")
PYEOF

    kill $CPID 2>/dev/null; wait $CPID 2>/dev/null || true
    ray stop --force 2>/dev/null; sleep 5
}

echo ""
echo "=== Sweep 1: max_ongoing (fixed mns=64) ==="
for MOR in 1 4 16 32; do
    run_one "mor${MOR}" "$MOR" 64
done

echo ""
echo "=== Sweep 2: max_num_seqs (fixed mor=128) ==="
for MNS in 8 16 32 128 256; do
    run_one "mns${MNS}" 128 "$MNS"
done

echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
