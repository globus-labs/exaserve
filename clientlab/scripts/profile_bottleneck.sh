#!/bin/bash
# Profile the weak-scaling bottleneck by collecting per-request phase traces.
# Runs a replay workload against an already-running cluster.
# Usage: bash profile_bottleneck.sh <server_url> <target_rps> <duration_s> <output_dir>
exec 2>&1
cd /home/wenyiw/aurora_rayserver

source /opt/cray/pe/lmod/default/init/bash 2>/dev/null
source /etc/bash.bashrc 2>/dev/null
module load frameworks 2>/dev/null
module load go 2>/dev/null
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
set -uo pipefail

SERVER_URL="${1:-http://127.0.0.1:4001}"
TARGET_RPS="${2:-17.5}"
DURATION="${3:-30}"
OUTDIR="${4:-/home/wenyiw/agpt/data/bench_results/clientlab/profile_$(date -u +%Y%m%dT%H%M%SZ)}"
mkdir -p "$OUTDIR"

CLIENT="eval/go_client/bin/go_dispatch"
(cd eval/go_client && bash build.sh 2>&1 | tail -1)

echo "=== Bottleneck profiling ==="
echo "  Server: $SERVER_URL"
echo "  Target: $TARGET_RPS rps for ${DURATION}s"
echo "  Output: $OUTDIR"

# Health check
python3 -c "
from urllib.request import build_opener, ProxyHandler
build_opener(ProxyHandler({})).open('${SERVER_URL}/health', timeout=5)
" 2>/dev/null || { echo "FATAL: server not healthy"; exit 1; }
echo "  Server healthy"

# Generate trace
NUM_REQUESTS=$(python3 -c "print(int($TARGET_RPS * $DURATION))")
echo "  Generating trace: $NUM_REQUESTS requests at $TARGET_RPS rps..."
python3 << PYEOF
import json, uuid
n = $NUM_REQUESTS
rate = $TARGET_RPS
with open("$OUTDIR/trace.jsonl", "w") as f:
    f.write(json.dumps({"schema_version": "trace.v1", "total_requests": n}) + "\n")
    for i in range(n):
        f.write(json.dumps({
            "req_id": uuid.uuid4().hex, "timestamp": (i + 1) / rate,
            "model": "meta-llama/Meta-Llama-3-8B-Instruct",
            "prompt": " ".join(["word"] * 2048),
            "input_len": 2048, "output_len": 128, "mode": "chat"
        }) + "\n")
PYEOF

# Run with full phase trace sampling
echo "  Running replay with phase trace (100% sampling)..."
T0=$(python3 -c "import time; print(time.time() + 2.0)")
echo "$T0" | $CLIENT \
    --base-urls "$SERVER_URL" \
    --max-active-requests 1024 \
    --num-go-workers 4 \
    --generation-mode deterministic \
    --trace-file "$OUTDIR/trace.jsonl" \
    --result-file "$OUTDIR/results.jsonl" \
    --metrics-file "$OUTDIR/metrics.json" \
    --phase-trace-file "$OUTDIR/phase_trace.jsonl" \
    --phase-trace-sample-rate 1.0 \
    --enable-httptrace \
    --worker-id "profile" \
    2>"$OUTDIR/stderr.log"

echo "  Analyzing phase traces..."
export OUTDIR="$OUTDIR"
python3 << 'PYEOF'
import json, statistics, os

traces = []
for line in open(os.environ["OUTDIR"] + "/phase_trace.jsonl"):
    line = line.strip()
    if not line:
        continue
    try:
        t = json.loads(line)
        if t.get("req_id"):
            traces.append(t)
    except:
        pass

if not traces:
    print("  No phase traces found!")
    exit(0)

print(f"  {len(traces)} phase traces collected")
print()

# Per-phase latency breakdown
phases = ["queue_wait_s", "time_to_headers_s", "body_read_s", "slot_hold_s"]
for phase in phases:
    vals = [t.get(phase, 0) for t in traces if t.get(phase, 0) > 0]
    if vals:
        print(f"  {phase:>20s}: p50={statistics.median(vals)*1000:>8.1f}ms  p99={sorted(vals)[int(len(vals)*0.99)]*1000:>8.1f}ms  mean={statistics.mean(vals)*1000:>8.1f}ms")

print()

# Connection reuse
new_conns = sum(1 for t in traces if t.get("new_connection"))
reused = sum(1 for t in traces if t.get("reused_connection"))
print(f"  Connections: new={new_conns}  reused={reused}  reuse_ratio={reused/max(len(traces),1)*100:.1f}%")

# Dispatch lag (how late was the request vs its scheduled time)
lags = [t.get("dispatch_lag_s", 0) for t in traces if "dispatch_lag_s" in t]
if lags:
    print(f"  Dispatch lag: p50={statistics.median(lags)*1000:.1f}ms  p99={sorted(lags)[int(len(lags)*0.99)]*1000:.1f}ms  max={max(lags)*1000:.1f}ms")

# Success rate
success = sum(1 for t in traces if t.get("success"))
errors = sum(1 for t in traces if t.get("error_class"))
print(f"  Success: {success}/{len(traces)} ({success/len(traces)*100:.1f}%)")
if errors:
    error_classes = {}
    for t in traces:
        ec = t.get("error_class", "")
        if ec:
            error_classes[ec] = error_classes.get(ec, 0) + 1
    print(f"  Error classes: {error_classes}")

PYEOF

echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
