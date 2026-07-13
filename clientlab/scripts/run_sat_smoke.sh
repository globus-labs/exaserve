#!/bin/bash
# Saturation finder smoke test on a compute node.
# Run via: ssh <compute-node> "cd /home/wenyiw/exaserve && bash clientlab/scripts/run_sat_smoke.sh"
exec 2>&1

cd /home/wenyiw/exaserve

module load frameworks 2>/dev/null || true
module load go 2>/dev/null || true
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

set -euo pipefail

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/sat_smoke_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="
echo "=== Start: $(date -u) ==="
echo "=== nproc: $(nproc) ==="

# Build
echo "[sat_smoke] Building C++ server..."
(cd clientlab/targets/cpp_server && bash build.sh 2>&1 | tail -1)
echo "[sat_smoke] Building Go client..."
(cd eval/go_client && bash build.sh 2>&1 | tail -1)

SERVER_BIN="clientlab/targets/cpp_server/bin/synthetic_server"
CLIENT_BIN="eval/go_client/bin/go_dispatch"
SERVER_PORT=18700

# Write server config (zero-latency stub)
cat > "$OUTDIR/server_config.json" << CFGEOF
{
  "target": {
    "host": "127.0.0.1",
    "port": $SERVER_PORT,
    "response_tokens": 16
  },
  "client": {
    "model": "stub-model",
    "prompt_words": 32,
    "max_active_requests": 0
  },
  "faults": {
    "service_time": {"distribution": "fixed", "value_ms": 0.0, "stddev_ms": 0.0},
    "max_inflight": 0,
    "max_queue": 0,
    "queue_delay_ms": 0.0,
    "error_rate": 0.0,
    "error_status": 500,
    "reject_status": 429,
    "close_after_response": false,
    "reset_after_response": false,
    "idle_timeout_s": 0.0,
    "burst_every": 0,
    "burst_duration": 0
  }
}
CFGEOF

# Start server
echo "[sat_smoke] Starting server on port $SERVER_PORT..."
$SERVER_BIN --config "$OUTDIR/server_config.json" > "$OUTDIR/server.log" 2>&1 &
SERVER_PID=$!
tries=0
while ! python3 -c "
from urllib.request import build_opener, ProxyHandler
o = build_opener(ProxyHandler({}))
r = o.open('http://127.0.0.1:$SERVER_PORT/health', timeout=2)
" 2>/dev/null; do
    tries=$((tries + 1))
    if [ $tries -gt 20 ]; then
        echo "FATAL: server health check failed"
        kill $SERVER_PID 2>/dev/null
        exit 1
    fi
    sleep 0.5
done
echo "  Server healthy (PID=$SERVER_PID)"

# ============================================================
# Test 1: Single-process saturation (should find ~150K+ RPS)
# ============================================================
echo ""
echo "===== Test 1: Single-process saturation ====="
$CLIENT_BIN \
    --mode saturation \
    --base-urls "http://127.0.0.1:$SERVER_PORT" \
    --max-active-requests 80 \
    --num-go-workers 4 \
    --sat-initial-rate 1000 \
    --sat-step-duration 5 \
    --sat-warmup-duration 2 \
    --sat-cooldown-pause 1 \
    --sat-tolerance 0.08 \
    --sat-verify=false \
    --sat-output "$OUTDIR/sat_single.json" \
    2>&1

echo ""
echo "--- Single-process result ---"
python3 << PYEOF
import json
data = json.load(open("$OUTDIR/sat_single.json"))
print(f"  Saturation rate: {data['saturation_rate']} rps")
print(f"  Steps: {len(data['steps'])}")
for s in data['steps']:
    print(f"    target={s['target_rate']:>8d}  achieved={s['achieved_rate']:>10.1f}  healthy={s['healthy']}")
PYEOF

# ============================================================
# Test 2: Multi-process saturation via shell (12 procs)
# Each proc does saturation-step at rate/12 for a few rates.
# ============================================================
echo ""
echo "===== Test 2: Multi-process aggregate (12 procs) ====="

for TARGET_RATE in 100000 200000 400000 600000 800000; do
    PER_PROC=$((TARGET_RATE / 12))
    STEP_DIR="$OUTDIR/multi_${TARGET_RATE}"
    mkdir -p "$STEP_DIR"

    pids=()
    for ((p=0; p<12; p++)); do
        $CLIENT_BIN \
            --mode saturation-step \
            --base-urls "http://127.0.0.1:$SERVER_PORT" \
            --max-active-requests 80 \
            --num-go-workers 4 \
            --sat-target-rate $PER_PROC \
            --sat-step-duration 5 \
            --sat-warmup-duration 2 \
            --sat-cooldown-pause 0 \
            --sat-output "$STEP_DIR/step_p${p}.json" \
            > "$STEP_DIR/stdout_p${p}.log" \
            2>"$STEP_DIR/stderr_p${p}.log" &
        pids+=($!)
    done

    any_failed=0
    for pid in "${pids[@]}"; do
        wait $pid || any_failed=1
    done

    python3 << PYEOF
import json, glob
results = []
for f in sorted(glob.glob("$STEP_DIR/step_p*.json")):
    try:
        results.append(json.load(open(f)))
    except:
        pass
if results:
    total_completed = sum(r.get("completed", 0) for r in results)
    total_failed = sum(r.get("failed", 0) for r in results)
    duration = max(r.get("duration_s", 0) for r in results)
    achieved = total_completed / duration if duration > 0 else 0
    err_rate = total_failed / (total_completed + total_failed) if (total_completed + total_failed) > 0 else 0
    p99 = max(r.get("p99_latency_s", 0) for r in results)
    ratio = achieved / $TARGET_RATE if $TARGET_RATE > 0 else 0
    healthy = err_rate <= 0.01 and ratio >= 0.95
    print(f"  target={$TARGET_RATE:>8d}  achieved={achieved:>10.1f}  err={err_rate:.4f}  p99={p99*1000:.2f}ms  ratio={ratio:.3f}  healthy={healthy}")
else:
    print(f"  target={$TARGET_RATE}: no results")
PYEOF

    if [ "$any_failed" -ne 0 ]; then
        echo "  WARNING: some processes failed at rate=$TARGET_RATE"
    fi
    sleep 2
done

# Cleanup
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
