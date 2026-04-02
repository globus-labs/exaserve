#!/bin/bash
# Validate streaming/TTFT support on a compute node.
# Tests: (1) non-streaming baseline, (2) streaming with TTFT, (3) saturation with streaming.
exec 2>&1
cd /home/wenyiw/aurora_rayserver

module load frameworks 2>/dev/null || true
module load go 2>/dev/null || true
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

set -euo pipefail

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/stream_validate_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="
echo "=== Start: $(date -u) ==="

CLIENT_BIN="eval/go_client/bin/go_dispatch"
SERVER_PORT=18800

# Build Go client
echo "[build] Go client..."
(cd eval/go_client && bash build.sh 2>&1 | tail -1)

# Start streaming stub server (TTFT=50ms, token_delay=10ms, 16 tokens)
echo "[server] Starting streaming stub (ttft=50ms)..."
python3 clientlab/targets/stream_stub.py \
    --port $SERVER_PORT --ttft-delay 0.05 --token-delay 0.01 --num-tokens 16 &
SERVER_PID=$!
sleep 1

# Health check
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

echo ""
echo "=========================================="
echo "=== TEST 1: Non-streaming (baseline)   ==="
echo "=========================================="
# Use saturation-step to measure baseline (no stream)
$CLIENT_BIN \
    --mode saturation-step \
    --base-urls "http://127.0.0.1:$SERVER_PORT" \
    --max-active-requests 20 \
    --num-go-workers 2 \
    --sat-target-rate 50 \
    --sat-step-duration 5 \
    --sat-warmup-duration 1 \
    --sat-cooldown-pause 0 \
    --sat-output "$OUTDIR/test1_nostream.json" \
    2>"$OUTDIR/test1_stderr.log"

echo "  Result:"
python3 -c "
import json
r = json.load(open('$OUTDIR/test1_nostream.json'))
print(f'  achieved={r[\"achieved_rate\"]:.1f} rps, dur={r[\"duration_s\"]:.2f}s')
print(f'  p50_lat={r[\"p50_latency_s\"]*1000:.1f}ms, p99_lat={r[\"p99_latency_s\"]*1000:.1f}ms')
print(f'  p50_ttft={r.get(\"p50_ttft_s\", 0)*1000:.1f}ms (expect 0 — no streaming)')
print(f'  completed={r[\"completed\"]}, failed={r[\"failed\"]}')
"

echo ""
echo "=========================================="
echo "=== TEST 2: Streaming with TTFT        ==="
echo "=========================================="
$CLIENT_BIN \
    --mode saturation-step \
    --base-urls "http://127.0.0.1:$SERVER_PORT" \
    --max-active-requests 20 \
    --num-go-workers 2 \
    --sat-stream \
    --sat-target-rate 50 \
    --sat-step-duration 5 \
    --sat-warmup-duration 1 \
    --sat-cooldown-pause 0 \
    --sat-output "$OUTDIR/test2_stream.json" \
    2>"$OUTDIR/test2_stderr.log"

echo "  Result:"
python3 -c "
import json
r = json.load(open('$OUTDIR/test2_stream.json'))
print(f'  achieved={r[\"achieved_rate\"]:.1f} rps, dur={r[\"duration_s\"]:.2f}s')
print(f'  p50_lat={r[\"p50_latency_s\"]*1000:.1f}ms, p99_lat={r[\"p99_latency_s\"]*1000:.1f}ms')
print(f'  p50_ttft={r.get(\"p50_ttft_s\", 0)*1000:.1f}ms, p99_ttft={r.get(\"p99_ttft_s\", 0)*1000:.1f}ms')
print(f'  mean_ttft={r.get(\"mean_ttft_s\", 0)*1000:.1f}ms (expect ~50ms)')
print(f'  completed={r[\"completed\"]}, failed={r[\"failed\"]}')
"

echo ""
echo "=========================================="
echo "=== TEST 3: Saturation binary search    ==="
echo "===         with streaming + TTFT SLO   ==="
echo "=========================================="
$CLIENT_BIN \
    --mode saturation \
    --base-urls "http://127.0.0.1:$SERVER_PORT" \
    --max-active-requests 20 \
    --num-go-workers 2 \
    --sat-stream \
    --sat-initial-rate 5 \
    --sat-step-duration 3 \
    --sat-warmup-duration 1 \
    --sat-cooldown-pause 0.5 \
    --sat-tolerance 0.1 \
    --sat-max-p99-ttft 0.1 \
    --sat-verify=false \
    --sat-output "$OUTDIR/test3_sat.json" \
    2>"$OUTDIR/test3_stderr.log"

echo "  Saturation result:"
python3 -c "
import json
r = json.load(open('$OUTDIR/test3_sat.json'))
print(f'  saturation_rate={r[\"saturation_rate\"]} rps')
print(f'  search steps: {len(r[\"steps\"])}')
for s in r['steps']:
    ttft = s.get('p99_ttft_s', 0)
    print(f'    target={s[\"target_rate\"]:>5d}  achieved={s[\"achieved_rate\"]:>8.1f}  p99_ttft={ttft*1000:.1f}ms  healthy={s[\"healthy\"]}')
"

# Cleanup
kill $SERVER_PID 2>/dev/null
wait $SERVER_PID 2>/dev/null || true

echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
