#!/bin/bash
# Phase 0: Sweep max_ongoing_requests on a single debug node.
# Deploys vLLM for each config, runs saturation finder, collects results.
# Usage: bash phase0_sweep.sh
exec 2>&1
cd /home/wenyiw/aurora_rayserver

source /opt/cray/pe/lmod/default/init/bash 2>/dev/null
source /etc/bash.bashrc 2>/dev/null
module load frameworks 2>/dev/null
module load go 2>/dev/null
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

set -euo pipefail

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/phase0_sweep_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
NODE=$(hostname)
echo "=== Phase 0 sweep ==="
echo "=== Node: $NODE ==="
echo "=== Output: $OUTDIR ==="
echo "=== Start: $(date -u) ==="

# Build Go client
echo "[build] Go client..."
(cd eval/go_client && bash build.sh 2>&1 | tail -1)
CLIENT_BIN="eval/go_client/bin/go_dispatch"

# Use an existing runtime manifest as base — we'll modify max_ongoing_requests
BASE_MANIFEST="/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/server_char_llama8b/run5/mor128/runtime/ray_runtime.yaml"

SERVER_PORT=8000

for MOR in 64 96 128 256; do
    echo ""
    echo "========================================"
    echo "=== Config: max_ongoing_requests=$MOR ==="
    echo "========================================"
    STEP_DIR="$OUTDIR/mor${MOR}"
    mkdir -p "$STEP_DIR"

    # Create modified manifest with this max_ongoing_requests
    python3 -c "
import yaml
m = yaml.safe_load(open('$BASE_MANIFEST'))
m['model_deployment_config']['replica_max_ongoing_requests'] = $MOR
yaml.dump(m, open('$STEP_DIR/runtime.yaml', 'w'), default_flow_style=False)
print(f'  Manifest written: max_ongoing={$MOR}')
"

    # Setup PBS_NODEFILE for launch_cluster.sh
    export PBS_NODEFILE="$STEP_DIR/nodefile"
    echo "$NODE" > "$PBS_NODEFILE"

    # Launch cluster
    echo "  Launching cluster..."
    bash scripts/launch_cluster.sh "$STEP_DIR/runtime.yaml" > "$STEP_DIR/cluster.log" 2>&1 &
    CLUSTER_PID=$!

    # Wait for health
    echo "  Waiting for server health..."
    tries=0
    while ! python3 -c "
from urllib.request import build_opener, ProxyHandler
o = build_opener(ProxyHandler({}))
o.open('http://127.0.0.1:$SERVER_PORT/health', timeout=5)
" 2>/dev/null; do
        tries=$((tries + 1))
        if [ $tries -gt 120 ]; then
            echo "  FATAL: server not healthy after 120 tries"
            kill $CLUSTER_PID 2>/dev/null; wait $CLUSTER_PID 2>/dev/null || true
            continue 2
        fi
        sleep 2
    done
    echo "  Server healthy"

    # Run saturation finder
    echo "  Running saturation finder..."
    $CLIENT_BIN \
        --mode saturation \
        --base-urls "http://127.0.0.1:$SERVER_PORT" \
        --max-active-requests 1024 \
        --num-go-workers 4 \
        --sat-stream \
        --sat-model "meta-llama/Meta-Llama-3-8B-Instruct" \
        --sat-prompt-words 2048 \
        --sat-output-tokens 128 \
        --sat-initial-rate 50 \
        --sat-step-duration 15 \
        --sat-warmup-duration 5 \
        --sat-tolerance 0.05 \
        --sat-verify=false \
        --sat-output "$STEP_DIR/saturation_output.json" \
        2>"$STEP_DIR/saturation_stderr.log"

    # Print result
    python3 -c "
import json
d = json.load(open('$STEP_DIR/saturation_output.json'))
rate = d['saturation_rate']
steps = d['steps']
best = max(steps, key=lambda s: s['achieved_rate']) if steps else {}
print(f'  Saturation: {rate} rps')
print(f'  Best achieved: {best.get(\"achieved_rate\", 0):.1f} rps')
print(f'  P99 TTFT: {best.get(\"p99_ttft_s\", 0)*1000:.0f}ms')
print(f'  P99 lat: {best.get(\"p99_latency_s\", 0)*1000:.0f}ms')
" 2>/dev/null || echo "  (failed to parse result)"

    # Tear down cluster
    echo "  Tearing down cluster..."
    kill $CLUSTER_PID 2>/dev/null
    wait $CLUSTER_PID 2>/dev/null || true
    # Kill ray
    ray stop --force 2>/dev/null || true
    sleep 5
done

# Summary
echo ""
echo "========================================"
echo "=== Summary ==="
echo "========================================"
python3 << 'PYEOF'
import json, os
base = os.environ.get("OUTDIR", "")
if not base:
    import sys; sys.exit(0)
print(f"{'MOR':>6s} {'Sat Rate':>10s} {'Best RPS':>10s} {'P99 TTFT':>10s} {'P99 Lat':>10s}")
print("-" * 50)
for mor in [64, 96, 128, 256]:
    path = os.path.join(base, f"mor{mor}", "saturation_output.json")
    if not os.path.exists(path): continue
    d = json.load(open(path))
    steps = d["steps"]
    best = max(steps, key=lambda s: s["achieved_rate"]) if steps else {}
    print(f"{mor:>6d} {d['saturation_rate']:>10d} {best.get('achieved_rate',0):>10.1f} {best.get('p99_ttft_s',0)*1000:>9.0f}ms {best.get('p99_latency_s',0)*1000:>9.0f}ms")
PYEOF

echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
