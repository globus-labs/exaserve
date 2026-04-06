#!/bin/bash
# Launch a Ray Serve cluster and run the profiling script against it.
# Usage: bash launch_and_profile.sh [target_rps] [duration_s]
# Must run ON the compute node (not via SSH).
exec 2>&1
cd /home/wenyiw/aurora_rayserver

source /opt/cray/pe/lmod/default/init/bash 2>/dev/null
source /etc/bash.bashrc 2>/dev/null
module load frameworks 2>/dev/null
module load go 2>/dev/null
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
set -uo pipefail

TARGET_RPS="${1:-17.5}"
DURATION="${2:-30}"
PROXY_PORT=4001
DIRECT_PORT=8000

MANIFEST="/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/server_char_llama8b/run5/mor128/runtime/ray_runtime.yaml"
OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/profile_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"

export PBS_NODEFILE="$OUTDIR/nodefile"
echo "$(hostname)" > "$PBS_NODEFILE"

echo "=== Launching cluster ==="
bash scripts/launch_cluster.sh "$MANIFEST" > "$OUTDIR/cluster.log" 2>&1 &
CLUSTER_PID=$!

# Wait for Ray Serve health
echo "  Waiting for Ray Serve..."
tries=0
while ! python3 -c "from urllib.request import build_opener,ProxyHandler; build_opener(ProxyHandler({})).open('http://127.0.0.1:$DIRECT_PORT/health',timeout=5)" 2>/dev/null; do
    tries=$((tries+1))
    [ $tries -gt 120 ] && { echo "FATAL: Ray Serve not healthy"; kill $CLUSTER_PID; exit 1; }
    sleep 2
done
echo "  Ray Serve healthy"

# Wait for LiteLLM
echo "  Waiting for LiteLLM proxy..."
tries=0
while ! python3 -c "from urllib.request import build_opener,ProxyHandler; build_opener(ProxyHandler({})).open('http://127.0.0.1:$PROXY_PORT/health/liveliness',timeout=5)" 2>/dev/null; do
    tries=$((tries+1))
    [ $tries -gt 60 ] && { echo "FATAL: LiteLLM not healthy"; kill $CLUSTER_PID; exit 1; }
    sleep 2
done
echo "  LiteLLM healthy"
sleep 10  # grace period

# Run profiling against PROXY (the production path)
echo ""
echo "=== Profile 1: via LiteLLM proxy (port $PROXY_PORT) ==="
bash clientlab/scripts/profile_bottleneck.sh "http://127.0.0.1:$PROXY_PORT" "$TARGET_RPS" "$DURATION" "$OUTDIR/via_proxy"

# Run profiling against DIRECT (bypass proxy)
echo ""
echo "=== Profile 2: direct to Ray Serve (port $DIRECT_PORT) ==="
bash clientlab/scripts/profile_bottleneck.sh "http://127.0.0.1:$DIRECT_PORT" "$TARGET_RPS" "$DURATION" "$OUTDIR/via_direct"

# Compare
echo ""
echo "=== Comparison ==="
export OUTDIR="$OUTDIR"
python3 << 'PYEOF'
import json, statistics, os

def summarize(path):
    traces = []
    for line in open(path):
        line = line.strip()
        if not line: continue
        try:
            t = json.loads(line)
            if t.get("req_id"): traces.append(t)
        except: pass
    if not traces: return None
    tth = [t.get("time_to_headers_s", 0) for t in traces if t.get("time_to_headers_s", 0) > 0]
    lag = [t.get("dispatch_lag_s", 0) for t in traces if "dispatch_lag_s" in t]
    success = sum(1 for t in traces if t.get("success"))
    return {
        "count": len(traces),
        "success": success,
        "tth_p50": statistics.median(tth) * 1000 if tth else 0,
        "tth_p99": sorted(tth)[int(len(tth)*0.99)] * 1000 if tth else 0,
        "lag_p99": sorted(lag)[int(len(lag)*0.99)] * 1000 if lag else 0,
    }

base = os.environ.get("OUTDIR", "/tmp")
proxy = summarize(f"{base}/via_proxy/phase_trace.jsonl")
direct = summarize(f"{base}/via_direct/phase_trace.jsonl")

if proxy and direct:
    print(f"{'':>15s} {'Via Proxy':>15s} {'Direct':>15s} {'Proxy Overhead':>15s}")
    print(f"{'TTH p50 (ms)':>15s} {proxy['tth_p50']:>15.1f} {direct['tth_p50']:>15.1f} {proxy['tth_p50']-direct['tth_p50']:>15.1f}")
    print(f"{'TTH p99 (ms)':>15s} {proxy['tth_p99']:>15.1f} {direct['tth_p99']:>15.1f} {proxy['tth_p99']-direct['tth_p99']:>15.1f}")
    print(f"{'Lag p99 (ms)':>15s} {proxy['lag_p99']:>15.1f} {direct['lag_p99']:>15.1f} {proxy['lag_p99']-direct['lag_p99']:>15.1f}")
    print(f"{'Success':>15s} {proxy['success']:>15d} {direct['success']:>15d}")
else:
    print("Could not compare — missing data")
PYEOF

echo ""
echo "=== Done ==="
echo "=== Output: $OUTDIR ==="

# Keep cluster alive — don't kill it
echo "Cluster still running (PID=$CLUSTER_PID). Kill manually when done."
