#!/bin/bash
# Compare saturation finder vs ceiling study on the same node.
# This runs both tests against the same C++ stub server to rule out node variance.
exec 2>&1
cd /home/wenyiw/exaserve

module load frameworks 2>/dev/null || true
module load go 2>/dev/null || true
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

set -euo pipefail

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/sat_vs_ceiling_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="
echo "=== Start: $(date -u) ==="
echo "=== nproc: $(nproc) ==="

# Build
echo "[build] C++ server..."
(cd clientlab/targets/cpp_server && bash build.sh 2>&1 | tail -1)
echo "[build] Go client..."
(cd eval/go_client && bash build.sh 2>&1 | tail -1)

SERVER_BIN="clientlab/targets/cpp_server/bin/synthetic_server"
CLIENT_BIN="eval/go_client/bin/go_dispatch"
SERVER_PORT=18800

# Server config
cat > "$OUTDIR/server_config.json" << CFGEOF
{
  "target": {"host": "127.0.0.1", "port": $SERVER_PORT, "response_tokens": 16},
  "client": {"model": "stub-model", "prompt_words": 32, "max_active_requests": 0},
  "faults": {
    "service_time": {"distribution": "fixed", "value_ms": 0.0, "stddev_ms": 0.0},
    "max_inflight": 0, "max_queue": 0, "queue_delay_ms": 0.0,
    "error_rate": 0.0, "error_status": 500, "reject_status": 429,
    "close_after_response": false, "reset_after_response": false,
    "idle_timeout_s": 0.0, "burst_every": 0, "burst_duration": 0
  }
}
CFGEOF

$SERVER_BIN --config "$OUTDIR/server_config.json" > "$OUTDIR/server.log" 2>&1 &
SERVER_PID=$!
tries=0
while ! python3 -c "
from urllib.request import build_opener, ProxyHandler
o = build_opener(ProxyHandler({}))
o.open('http://127.0.0.1:$SERVER_PORT/health', timeout=2)
" 2>/dev/null; do
    tries=$((tries + 1))
    [ $tries -gt 20 ] && { echo "FATAL: server failed"; kill $SERVER_PID; exit 1; }
    sleep 0.5
done
echo "  Server healthy (PID=$SERVER_PID)"

# ============================================================
# Control: Ceiling study (trace-based, 1M rps target, 15s)
# Same parameters as the dispatch ceiling profiling study
# ============================================================
echo ""
echo "========================================"
echo "=== CONTROL: Ceiling study (trace)   ==="
echo "========================================"

# Generate traces for 1-proc and 12-proc configs
export TRACE_DIR="$OUTDIR/traces"
mkdir -p "$TRACE_DIR"
export DURATION=15
export RATE=1000000
export NUM_REQUESTS=$((RATE * DURATION))

echo "[ceiling] Generating trace ($NUM_REQUESTS requests)..."
python3 << 'PYEOF'
import json, uuid, os
num_requests = int(os.environ["NUM_REQUESTS"])
rate = int(os.environ["RATE"])
duration = int(os.environ["DURATION"])
trace_dir = os.environ["TRACE_DIR"]
prompt = " ".join(["word"] * 32)
header = {"schema_version": "trace.v1", "total_requests": num_requests, "rate": rate, "duration_s": duration}
# Full trace
with open(os.path.join(trace_dir, "trace_full.jsonl"), "w") as f:
    f.write(json.dumps(header) + "\n")
    dt = 1.0 / rate
    for i in range(num_requests):
        f.write(json.dumps({"req_id": uuid.uuid4().hex, "timestamp": i * dt, "model": "stub-model",
            "prompt": prompt, "input_len": 32, "output_len": 16, "mode": "chat"}) + "\n")
print(f"  Generated {num_requests} requests")
# Partition for 12 procs
for nprocs in [1, 12]:
    os.makedirs(os.path.join(trace_dir, f"p{nprocs}"), exist_ok=True)
    lines = open(os.path.join(trace_dir, "trace_full.jsonl")).readlines()
    header_line, data = lines[0], lines[1:]
    h = json.loads(header_line)
    for p in range(nprocs):
        part = [data[i] for i in range(p, len(data), nprocs)]
        h2 = dict(h); h2["total_requests"] = len(part)
        with open(os.path.join(trace_dir, f"p{nprocs}", f"trace_p{p}.jsonl"), "w") as f:
            f.write(json.dumps(h2) + "\n")
            for line in part: f.write(line)
        print(f"  p{nprocs}/trace_p{p}: {len(part)} requests")
PYEOF

run_ceiling() {
    local label=$1
    local nprocs=$2
    local dir="$OUTDIR/ceiling_$label"
    mkdir -p "$dir"

    echo ""
    echo "--- Ceiling: $label ($nprocs procs) ---"
    local run_t0
    run_t0=$(python3 -c "import time; print(repr(time.time() + 10.0))")

    local pids=()
    for ((p=0; p<nprocs; p++)); do
        echo "$run_t0" | $CLIENT_BIN \
            --base-urls "http://127.0.0.1:$SERVER_PORT" \
            --generation-mode deterministic \
            --timeout 3600 \
            --max-active-requests 80 \
            --queue-capacity 0 \
            --max-conns-per-host 0 \
            --num-go-workers 4 \
            --worker-id "${label}_p${p}" \
            --trace-file "$TRACE_DIR/p${nprocs}/trace_p${p}.jsonl" \
            --result-file "$dir/result_p${p}.jsonl" \
            --metrics-file "$dir/metrics_p${p}.json" \
            --phase-trace-file "$dir/phase_p${p}.jsonl" \
            --phase-trace-sample-rate 0.001 \
            --enable-httptrace \
            --sum-only \
            > "$dir/stdout_p${p}.log" \
            2>"$dir/stderr_p${p}.log" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait $pid 2>/dev/null || true; done

    python3 << PYEOF
import json, glob
metrics = []
for f in sorted(glob.glob("$dir/metrics_p*.json")):
    try: metrics.append(json.load(open(f)))
    except: pass
if metrics:
    total_completed = sum(m.get("requests_completed", 0) for m in metrics)
    earliest = min(m.get("started_at", 1e18) for m in metrics)
    latest_done = max(m.get("last_body_done_at", 0) for m in metrics)
    duration = latest_done - earliest
    rps = total_completed / duration if duration > 0 else 0
    print(f"  completed={total_completed}  duration={duration:.3f}s  rps={rps:.1f}")
PYEOF
}

run_ceiling "1proc" 1
run_ceiling "12procs" 12

# ============================================================
# TEST: Saturation finder
# ============================================================
echo ""
echo "========================================"
echo "=== TEST: Saturation finder          ==="
echo "========================================"

echo ""
echo "--- Saturation: single-proc binary search ---"
$CLIENT_BIN \
    --mode saturation \
    --base-urls "http://127.0.0.1:$SERVER_PORT" \
    --max-active-requests 80 \
    --num-go-workers 4 \
    --sat-initial-rate 1000 \
    --sat-step-duration 10 \
    --sat-warmup-duration 3 \
    --sat-cooldown-pause 1 \
    --sat-tolerance 0.05 \
    --sat-verify=false \
    --sat-output "$OUTDIR/sat_single.json" \
    2>&1

python3 -c "
import json
data = json.load(open('$OUTDIR/sat_single.json'))
print(f'  Saturation: {data[\"saturation_rate\"]} rps  ({len(data[\"steps\"])} steps)')
for s in data['steps']:
    print(f'    target={s[\"target_rate\"]:>8d}  achieved={s[\"achieved_rate\"]:>10.1f}  dur={s[\"duration_s\"]:.2f}s  err={s[\"error_rate\"]:.4f}  healthy={s[\"healthy\"]}')
"

echo ""
echo "--- Saturation: 12-proc step sweep ---"
for TARGET_RATE in 200000 400000 500000 600000; do
    PER_PROC=$((TARGET_RATE / 12))
    STEP_DIR="$OUTDIR/sat_multi_${TARGET_RATE}"
    mkdir -p "$STEP_DIR"

    pids=()
    for ((p=0; p<12; p++)); do
        $CLIENT_BIN \
            --mode saturation-step \
            --base-urls "http://127.0.0.1:$SERVER_PORT" \
            --max-active-requests 80 \
            --num-go-workers 4 \
            --sat-target-rate $PER_PROC \
            --sat-step-duration 10 \
            --sat-warmup-duration 3 \
            --sat-cooldown-pause 0 \
            --sat-output "$STEP_DIR/step_p${p}.json" \
            > /dev/null 2>"$STEP_DIR/stderr_p${p}.log" &
        pids+=($!)
    done
    for pid in "${pids[@]}"; do wait $pid 2>/dev/null || true; done

    python3 << PYEOF
import json, glob
results = []
for f in sorted(glob.glob("$STEP_DIR/step_p*.json")):
    try: results.append(json.load(open(f)))
    except: pass
if results:
    total_ok = sum(r.get("completed", 0) for r in results)
    total_fail = sum(r.get("failed", 0) for r in results)
    dur = max(r.get("duration_s", 0) for r in results)
    achieved = total_ok / dur if dur > 0 else 0
    total_all = total_ok + total_fail
    err = total_fail / total_all if total_all > 0 else 0
    p99 = max(r.get("p99_latency_s", 0) for r in results)
    ratio = achieved / $TARGET_RATE if $TARGET_RATE > 0 else 0
    healthy = "Y" if (err <= 0.01 and ratio >= 0.95) else "N"
    print(f"  target={$TARGET_RATE:>8d}  achieved={achieved:>10.1f}  dur={dur:.2f}s  err={err:.4f}  p99={p99*1000:.2f}ms  ratio={ratio:.3f}  healthy={healthy}")
PYEOF
    sleep 2
done

# Cleanup
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "=== All done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
