#!/usr/bin/env bash
# dual_client_test.sh — run two independent replay_client instances against
# the same 16 stub servers and compare their individual and combined RPS.
#
# Interpretation:
#   A≈32K + B≈32K → total ~64K  →  go_dispatch timer IS the bottleneck
#                                   (each process has its own loop; they scale)
#   A≈16K + B≈16K → total ~32K  →  stub server OR loopback kernel is the
#                                   shared bottleneck (both clients compete)
#
# Usage:
#   bash benchmarks/dual_client_test.sh [options]
#
# Options:
#   --rps N           Target RPS *per client* (default: 32000)
#   --duration N      Probe duration in seconds (default: 10)
#   --payload TYPE    small|medium|large (default: medium)
#   --stub-workers N  How many stub processes to start (default: 16)
#   --go-concurrency N  go_dispatch concurrency (default: 4000)
#   --python PATH     Python interpreter (default: auto-detect)
#   --port N          Stub port (default: 8000)

set -euo pipefail
cd "$(dirname "$0")/.."

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
RPS=32000
DURATION=10
PAYLOAD=medium
STUB_WORKERS=16
GO_CONCURRENCY=4000
PORT=8000
PYTHON=""

# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --rps)            RPS="$2";            shift 2 ;;
        --duration)       DURATION="$2";       shift 2 ;;
        --payload)        PAYLOAD="$2";        shift 2 ;;
        --stub-workers)   STUB_WORKERS="$2";   shift 2 ;;
        --go-concurrency) GO_CONCURRENCY="$2"; shift 2 ;;
        --python)         PYTHON="$2";         shift 2 ;;
        --port)           PORT="$2";           shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Python auto-detect
# ---------------------------------------------------------------------------
if [[ -z "$PYTHON" ]]; then
    for candidate in \
        /home/wenyiw/agpt/venv/litellm/bin/python3 \
        /home/wenyiw/agpt/venv/litellm/bin/python \
        python3 python; do
        if command -v "$candidate" &>/dev/null && \
           "$candidate" -c "import starlette, uvicorn" &>/dev/null 2>&1; then
            PYTHON="$candidate"
            break
        fi
    done
    if [[ -z "$PYTHON" ]]; then
        echo "!!! ERROR: could not find a Python with starlette+uvicorn. Use --python." >&2
        exit 1
    fi
fi

# ---------------------------------------------------------------------------
# Output directory
# ---------------------------------------------------------------------------
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
OUTDIR=/home/wenyiw/agpt/data/bench_results/dual_test_${TIMESTAMP}
mkdir -p "$OUTDIR/client_A" "$OUTDIR/client_B"

echo "================================================================"
echo "  dual_client_test  $(date)"
echo "  python:           $PYTHON"
echo "  stub_workers:     $STUB_WORKERS  port=$PORT"
echo "  rps per client:   $RPS  (total target: $((RPS * 2)))"
echo "  duration:         ${DURATION}s"
echo "  payload:          $PAYLOAD"
echo "  go_concurrency:   $GO_CONCURRENCY"
echo "  outdir:           $OUTDIR"
echo "================================================================"

# ---------------------------------------------------------------------------
# Start stub servers
# ---------------------------------------------------------------------------
STUB_PIDS=()
echo "[stubs] Starting $STUB_WORKERS stub server(s) on port $PORT..."
for i in $(seq 1 "$STUB_WORKERS"); do
    "$PYTHON" benchmarks/stub_server.py \
        --port "$PORT" --reuse-port \
        > "$OUTDIR/stub_${i}.log" 2>&1 &
    STUB_PIDS+=($!)
done

# Wait for stubs to be ready
echo -n "[stubs] Waiting for stubs..."
for attempt in $(seq 1 30); do
    if curl -sf "http://127.0.0.1:${PORT}/health" > /dev/null 2>&1; then
        echo " ready (${attempt}s)"
        break
    fi
    sleep 1
    echo -n "."
    if [[ $attempt -eq 30 ]]; then
        echo ""
        echo "!!! ERROR: Stub server did not start within 30s. Check $OUTDIR/stub_1.log" >&2
        kill "${STUB_PIDS[@]}" 2>/dev/null || true
        exit 1
    fi
done

cleanup() {
    echo ""
    echo "[cleanup] Stopping stubs..."
    kill "${STUB_PIDS[@]}" 2>/dev/null || true
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Run two bench_client instances in parallel at fixed RPS
# ---------------------------------------------------------------------------
echo ""
echo "[clients] Launching client A and client B simultaneously..."
echo "          Each targets ${RPS} rps → combined target = $((RPS * 2)) rps"
echo ""

"$PYTHON" benchmarks/bench_client.py \
    --rps "$RPS" \
    --workers 4 \
    --payload "$PAYLOAD" \
    --stub-port "$PORT" \
    --duration "$DURATION" \
    --work-dir "$OUTDIR/client_A" \
    --go-concurrency "$GO_CONCURRENCY" \
    --python "$PYTHON" \
    > "$OUTDIR/client_A.log" 2>&1 &
PID_A=$!

"$PYTHON" benchmarks/bench_client.py \
    --rps "$RPS" \
    --workers 4 \
    --payload "$PAYLOAD" \
    --stub-port "$PORT" \
    --duration "$DURATION" \
    --work-dir "$OUTDIR/client_B" \
    --go-concurrency "$GO_CONCURRENCY" \
    --python "$PYTHON" \
    > "$OUTDIR/client_B.log" 2>&1 &
PID_B=$!

# Show a heartbeat while waiting
echo -n "[clients] Running"
while kill -0 $PID_A 2>/dev/null || kill -0 $PID_B 2>/dev/null; do
    sleep 2
    echo -n "."
done
echo " done"

# ---------------------------------------------------------------------------
# Extract actual_rps from result JSONs
# ---------------------------------------------------------------------------
extract_rps() {
    local label="$1"
    local work_dir="$2"
    local log="$3"

    # bench_client writes result JSON under work_dir/results/
    local json_file
    json_file=$(find "$work_dir" -name "result0.json" 2>/dev/null | head -1)

    if [[ -z "$json_file" ]]; then
        echo "  $label: result JSON not found — see $log"
        echo "0"
        return
    fi

    local actual ovhd
    actual=$("$PYTHON" -c "
import json, sys
d = json.load(open('$json_file'))
ov = d.get('overall', {})
req = d.get('requests', [])
# actual rps is in overall.rps for replay_client output
rps = ov.get('rps') or ov.get('actual_rps')
if rps is None and req:
    # fall back: compute from requests array
    lats = [r['latency'] for r in req if 'latency' in r]
    dur = d.get('summary', {}).get('trace_span_s') or (max(lats) if lats else 1)
    rps = len(req) / dur if dur else 0
ovhd = d.get('summary', {}).get('dispatch_overhead_s', 0) or 0
p50 = None
if req:
    lats = sorted(r['latency'] for r in req if 'latency' in r)
    n = len(lats)
    p50 = lats[n//2]*1000 if n else None
print(f'{rps:.0f} {ovhd:.3f} {p50:.2f}' if p50 is not None else f'{rps:.0f} {ovhd:.3f} -')
" 2>/dev/null || echo "0 0 -")

    read -r rps ovhd p50 <<< "$actual"
    printf "  %-10s actual_rps=%-8s  dispatch_ovhd=%-8ss  p50_latency=%sms\n" \
        "$label" "$rps" "$ovhd" "$p50"
    echo "$rps"
}

echo ""
echo "================================================================"
echo "  RESULTS"
echo "================================================================"
RPS_A=$(extract_rps "Client A" "$OUTDIR/client_A" "$OUTDIR/client_A.log")
RPS_B=$(extract_rps "Client B" "$OUTDIR/client_B" "$OUTDIR/client_B.log")

TOTAL=$("$PYTHON" -c "print(int('${RPS_A:-0}') + int('${RPS_B:-0}'))" 2>/dev/null || echo "?")

echo ""
echo "  Combined total:   ~${TOTAL} rps  (target was $((RPS * 2)) rps)"
echo ""

echo "----------------------------------------------------------------"
echo "  Interpretation:"
if [[ "$TOTAL" =~ ^[0-9]+$ ]]; then
    DOUBLE_RPS=$((RPS * 2))
    THRESHOLD=$(( DOUBLE_RPS * 75 / 100 ))
    if [[ "$TOTAL" -ge "$THRESHOLD" ]]; then
        echo "  ✓  Total (~${TOTAL}) is near the combined target ($DOUBLE_RPS)."
        echo "     → go_dispatch dispatch loop IS the bottleneck."
        echo "       Each process has its own timer and they scale independently."
        echo "       Fix: use a spin-wait (busy-loop) for sub-ms dispatch intervals"
        echo "            instead of time.NewTimer() in go_dispatch."
    else
        echo "  ✗  Total (~${TOTAL}) did NOT double — still capped near single-client max."
        echo "     → The bottleneck is SHARED: either the stub server"
        echo "       or the loopback kernel TCP stack."
        echo "       Next step: check stub CPU during run (htop/top -H)."
        echo "       If stubs are idle → loopback kernel softirq is the wall."
    fi
fi
echo "================================================================"
echo ""
echo "Full logs: $OUTDIR/"
