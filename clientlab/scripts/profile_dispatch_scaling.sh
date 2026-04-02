#!/bin/bash
# Profile dispatch scaling to disambiguate kernel TCP contention vs CPU over-subscription.
#
# Runs 3 configs with mpstat/vmstat/ss collection:
#   A) 1 proc, 4 workers   — baseline (no contention)
#   B) 12 procs, 4 workers — plateau config (48 spin-wait threads)
#   C) 12 procs, 1 worker  — reduced spin-wait (12 spin-wait threads)
#
# If B and C perform similarly → kernel TCP is the bottleneck.
# If C >> B → spin-wait CPU burn is the bottleneck.

# Redirect all output to stdout so PBS captures it even on early failure.
exec 2>&1

cd /home/wenyiw/aurora_rayserver

# Module loads may return non-zero; don't use set -e until after.
module load frameworks || true
module load go || true

# Unset proxy to avoid routing localhost health checks through ALCF proxy.
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

set -euo pipefail

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/dispatch_profiling_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="
echo "=== Start: $(date -u) ==="
echo "=== nproc: $(nproc) ==="

# Build server and client
echo "[profile] Building C++ server..."
(cd clientlab/targets/cpp_server && bash build.sh 2>&1 | tail -1)
echo "[profile] Building Go client..."
(cd eval/go_client && bash build.sh 2>&1 | tail -1)

SERVER_BIN="clientlab/targets/cpp_server/bin/synthetic_server"
CLIENT_BIN="eval/go_client/bin/go_dispatch"

export SERVER_PORT=18500
export DURATION=15
export RATE=1000000
export MAX_ACTIVE=80
export PROMPT_WORDS=32
export OUTPUT_TOKENS=16
export NUM_REQUESTS=$((RATE * DURATION))

# Generate trace once (shared across all configs)
echo "[profile] Generating trace ($NUM_REQUESTS requests)..."
export TRACE_DIR="$OUTDIR/traces"
mkdir -p "$TRACE_DIR"
python3 << 'PYEOF'
import json, uuid, os, sys

num_requests = int(os.environ.get("NUM_REQUESTS", 15000000))
rate = int(os.environ.get("RATE", 1000000))
duration = int(os.environ.get("DURATION", 15))
prompt_words = int(os.environ.get("PROMPT_WORDS", 32))
output_tokens = int(os.environ.get("OUTPUT_TOKENS", 16))
trace_dir = os.environ["TRACE_DIR"]

body_content = json.dumps({
    "model": "stub-model",
    "messages": [{"role": "user", "content": " ".join(["word"] * prompt_words)}],
    "max_tokens": output_tokens
})

header = {
    "schema_version": "trace.v1",
    "total_requests": num_requests,
    "rate": rate,
    "duration_s": duration,
}

dt = 1.0 / rate
path = os.path.join(trace_dir, "trace_full.jsonl")
with open(path, "w") as f:
    f.write(json.dumps(header) + "\n")
    for i in range(num_requests):
        row = {
            "req_id": uuid.uuid4().hex,
            "timestamp": i * dt,
            "model": "stub-model",
            "prompt_words": prompt_words,
            "output_tokens": output_tokens,
            "input_len": prompt_words,
            "output_len": output_tokens,
            "mode": "chat",
            "endpoint": "/v1/chat/completions",
            "body": body_content,
        }
        f.write(json.dumps(row) + "\n")
print(f"Generated {num_requests} requests", flush=True)
PYEOF

# Partition traces for multi-proc runs
for NPROCS in 1 12; do
    mkdir -p "$TRACE_DIR/p${NPROCS}"
    python3 << PYEOF
import json, os

trace_dir = "$TRACE_DIR"
nprocs = $NPROCS

lines = open(os.path.join(trace_dir, "trace_full.jsonl")).readlines()
header_line = lines[0]
data = lines[1:]
header = json.loads(header_line)

for p in range(nprocs):
    partition = [data[i] for i in range(p, len(data), nprocs)]
    h = dict(header)
    h["total_requests"] = len(partition)
    outpath = os.path.join(trace_dir, f"p{nprocs}", f"trace_p{p}.jsonl")
    with open(outpath, "w") as f:
        f.write(json.dumps(h) + "\n")
        for line in partition:
            f.write(line)
    print(f"  p{nprocs}/trace_p{p}: {len(partition)} requests")
PYEOF
done

start_server() {
    local dir=$1
    echo "[profile] Starting server on port $SERVER_PORT..."
    # Write server config JSON (same format as clientlab runtime produces).
    cat > "$dir/server_config.json" << CFGEOF
{
  "target": {
    "host": "127.0.0.1",
    "port": $SERVER_PORT,
    "response_tokens": $OUTPUT_TOKENS
  },
  "client": {
    "model": "stub-model",
    "prompt_words": $PROMPT_WORDS,
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
    $SERVER_BIN --config "$dir/server_config.json" \
        > "$dir/server_stdout.log" 2>&1 &
    SERVER_PID=$!
    # Wait for server to be ready (retry health check).
    local tries=0
    while ! python3 -c "
from urllib.request import build_opener, ProxyHandler
o = build_opener(ProxyHandler({}))
r = o.open('http://127.0.0.1:$SERVER_PORT/health', timeout=2)
" 2>/dev/null; do
        tries=$((tries + 1))
        if [ $tries -gt 20 ]; then
            echo "FATAL: server health check failed after 10s"
            cat "$dir/server_stdout.log" | tail -5
            kill $SERVER_PID 2>/dev/null
            exit 1
        fi
        sleep 0.5
    done
    echo "  Server healthy (PID=$SERVER_PID)"
}

stop_server() {
    kill $SERVER_PID 2>/dev/null
    wait $SERVER_PID 2>/dev/null || true
    sleep 1
}

start_profilers() {
    local dir=$1
    mpstat -P ALL 1 > "$dir/mpstat.log" 2>&1 &
    MPSTAT_PID=$!
    vmstat 1 > "$dir/vmstat.log" 2>&1 &
    VMSTAT_PID=$!
    (while true; do
        echo "--- $(date +%s.%N) ---"
        cat /proc/net/softnet_stat 2>/dev/null
        sleep 1
    done) > "$dir/softnet.log" 2>&1 &
    SOFTNET_PID=$!
    (while true; do
        echo "--- $(date +%s.%N) ---"
        ss -s 2>/dev/null
        sleep 2
    done) > "$dir/ss.log" 2>&1 &
    SS_PID=$!
}

stop_profilers() {
    kill $MPSTAT_PID $VMSTAT_PID $SOFTNET_PID $SS_PID 2>/dev/null
    wait $MPSTAT_PID $VMSTAT_PID $SOFTNET_PID $SS_PID 2>/dev/null || true
}

run_config() {
    local label=$1
    local nprocs=$2
    local nworkers=$3
    local dir="$OUTDIR/$label"
    mkdir -p "$dir"

    echo ""
    echo "===== Config $label: ${nprocs} procs, ${nworkers} workers ====="

    start_server "$dir"
    start_profilers "$dir"

    # Compute T0 far enough in the future for all procs to load traces and be ready.
    # Each proc loads ~1.25M lines; give 10s headroom.
    local run_t0
    run_t0=$(python3 -c "import time; print(repr(time.time() + 10.0))")
    echo "  T0=$run_t0 (10s from now)"

    # Launch all Go clients. Each gets T0 piped to stdin immediately.
    # The client prints GO_CLI_READY, reads T0, then waits until T0 to start dispatching.
    local pids=()
    for ((p=0; p<nprocs; p++)); do
        local trace="$TRACE_DIR/p${nprocs}/trace_p${p}.jsonl"
        echo "$run_t0" | $CLIENT_BIN \
            --base-urls "http://127.0.0.1:$SERVER_PORT" \
            --generation-mode deterministic \
            --timeout 3600 \
            --max-active-requests $MAX_ACTIVE \
            --queue-capacity 0 \
            --max-conns-per-host 0 \
            --num-go-workers $nworkers \
            --worker-id "${label}_p${p}" \
            --trace-file "$trace" \
            --result-file "$dir/result_p${p}.jsonl" \
            --metrics-file "$dir/metrics_p${p}.json" \
            --phase-trace-file "$dir/phase_p${p}.jsonl" \
            --phase-trace-sample-rate 0.01 \
            --enable-httptrace \
            --sum-only \
            > "$dir/client_p${p}_stdout.log" \
            2>"$dir/client_p${p}_stderr.log" &
        pids+=($!)
    done

    echo "  Launched ${#pids[@]} client processes, dispatching (duration=${DURATION}s)..."

    # Wait for all client processes
    local any_failed=0
    for pid in "${pids[@]}"; do
        wait $pid || any_failed=1
    done

    stop_profilers

    # Fetch server metrics before stopping
    python3 -c "
from urllib.request import build_opener, ProxyHandler
import json
o = build_opener(ProxyHandler({}))
r = o.open('http://127.0.0.1:$SERVER_PORT/metrics', timeout=5)
data = json.loads(r.read().decode())
json.dump(data, open('$dir/target_metrics.json', 'w'), indent=2)
print(f'  Server metrics: {data.get(\"total_requests\", \"?\")} requests, max_active={data.get(\"max_active\", \"?\")}')
" 2>&1 || echo "  WARNING: could not fetch server metrics"

    stop_server

    # Analyze results
    echo "  --- Results ---"
    python3 << PYEOF
import json, glob, os, statistics

metrics = []
phase_records = []
for f in sorted(glob.glob("$dir/metrics_p*.json")):
    try:
        metrics.append(json.load(open(f)))
    except Exception as e:
        print(f"  WARNING: failed to load {f}: {e}")
for f in sorted(glob.glob("$dir/phase_p*.jsonl")):
    try:
        for line in open(f):
            phase_records.append(json.loads(line))
    except:
        pass

if not metrics:
    print("  WARNING: no metrics files found")
else:
    total_completed = sum(m.get("requests_completed", 0) for m in metrics)
    earliest_start = min(m.get("started_at", 1e18) for m in metrics)
    latest_done = max(m.get("last_body_done_at", 0) for m in metrics)
    duration = latest_done - earliest_start
    rps = total_completed / duration if duration > 0 else 0
    total_new = sum(m.get("new_connections", 0) for m in metrics)
    total_reused = sum(m.get("reused_connections", 0) for m in metrics)
    print(f"  completed={total_completed}  duration={duration:.3f}s  rps={rps:.1f}")
    print(f"  new_conns={total_new}  reused_conns={total_reused}")

if phase_records:
    tth = [r["time_to_headers_s"] for r in phase_records if "time_to_headers_s" in r]
    sloth = [r["slot_hold_s"] for r in phase_records if "slot_hold_s" in r]
    if tth:
        tth.sort()
        print(f"  tth:       mean={statistics.mean(tth)*1000:.3f}ms  p50={tth[len(tth)//2]*1000:.3f}ms  p99={tth[int(len(tth)*0.99)]*1000:.3f}ms")
    if sloth:
        sloth.sort()
        print(f"  slot_hold: mean={statistics.mean(sloth)*1000:.3f}ms  p50={sloth[len(sloth)//2]*1000:.3f}ms  p99={sloth[int(len(sloth)*0.99)]*1000:.3f}ms")
PYEOF

    # Summarize mpstat
    python3 << PYEOF
lines = open("$dir/mpstat.log").readlines()
usr_vals, sys_vals, soft_vals, idle_vals = [], [], [], []
for line in lines:
    parts = line.split()
    if len(parts) >= 12 and parts[1] == "all":
        try:
            usr_vals.append(float(parts[2]))
            sys_vals.append(float(parts[4]))
            soft_vals.append(float(parts[7]))
            idle_vals.append(float(parts[11]))
        except ValueError:
            pass
n = len(usr_vals)
if n > 2:
    # Skip first and last samples (ramp up/down)
    u = usr_vals[1:-1]
    s = sys_vals[1:-1]
    sf = soft_vals[1:-1]
    i = idle_vals[1:-1]
    import statistics
    print(f"  mpstat avg: %usr={statistics.mean(u):.1f}  %sys={statistics.mean(s):.1f}  %soft={statistics.mean(sf):.1f}  %idle={statistics.mean(i):.1f}  (over {len(u)} steady-state samples)")
    # Also compute total CPU utilization
    total_used = [100.0 - idle for idle in i]
    print(f"  mpstat total CPU used: {statistics.mean(total_used):.1f}%  (of $(nproc) cores)")
else:
    print(f"  mpstat: only {n} samples, insufficient")
PYEOF

    # Summarize vmstat
    python3 << PYEOF
lines = open("$dir/vmstat.log").readlines()
cs_vals, intr_vals = [], []
for line in lines[2:]:
    parts = line.split()
    if len(parts) >= 16:
        try:
            intr_vals.append(int(parts[10]))
            cs_vals.append(int(parts[11]))
        except (ValueError, IndexError):
            pass
n = len(cs_vals)
if n > 2:
    cs = cs_vals[1:-1]
    intr = intr_vals[1:-1]
    import statistics
    print(f"  vmstat avg: ctx_switches={statistics.mean(cs):.0f}/s  interrupts={statistics.mean(intr):.0f}/s")
else:
    print(f"  vmstat: only {n} samples")
PYEOF

    if [ "$any_failed" -ne 0 ]; then
        echo "  WARNING: some client processes failed"
    fi
    echo "  Done: $label"
}

# Run all three configs
run_config "A_1proc_4workers" 1 4
run_config "B_12procs_4workers" 12 4
run_config "C_12procs_1worker" 12 1

echo ""
echo "=========================================="
echo "=== All configs complete ==="
echo "=== Output: $OUTDIR ==="
echo "=== End: $(date -u) ==="
