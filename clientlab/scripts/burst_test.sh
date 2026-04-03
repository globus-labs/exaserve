#!/bin/bash
# Burst test: send N concurrent requests and measure throughput at different concurrency levels.
# Runs against a live Ray Serve deployment on the same node.
# Usage: bash burst_test.sh <server_url>
exec 2>&1
cd /home/wenyiw/aurora_rayserver

module load frameworks 2>/dev/null || true
module load go 2>/dev/null || true
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY

set -euo pipefail

SERVER_URL="${1:-http://127.0.0.1:8000}"
OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/burst_test_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Burst test: $SERVER_URL ==="
echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="

# Build Go client
echo "[build] Go client..."
(cd eval/go_client && bash build.sh 2>&1 | tail -1)
CLIENT_BIN="eval/go_client/bin/go_dispatch"

# Health check
echo "[health] Checking server..."
tries=0
while ! python3 -c "
from urllib.request import build_opener, ProxyHandler
o = build_opener(ProxyHandler({}))
o.open('${SERVER_URL}/health', timeout=5)
" 2>/dev/null; do
    tries=$((tries + 1))
    [ $tries -gt 30 ] && { echo "FATAL: server not healthy at $SERVER_URL"; exit 1; }
    sleep 2
done
echo "  Server healthy"

# Start a stats poller in background (hits /stats every 2s, logs to file)
echo "[stats] Starting stats poller..."
(while true; do
    ts=$(date -u +%Y-%m-%dT%H:%M:%S)
    stats=$(python3 -c "
from urllib.request import build_opener, ProxyHandler
import json
o = build_opener(ProxyHandler({}))
try:
    r = o.open('${SERVER_URL}/stats', timeout=2)
    print(r.read().decode())
except: print('{\"error\": \"unreachable\"}')
" 2>/dev/null)
    echo "$ts $stats" >> "$OUTDIR/stats_poll.jsonl"
    sleep 2
done) &
STATS_PID=$!
echo "  Stats poller PID=$STATS_PID"

echo ""
echo "============================================"
echo "=== Burst test: varying go_concurrency   ==="
echo "============================================"
echo ""
echo "Each test: saturation-step at a FIXED high rate (500 rps) for 15s."
echo "The rate is intentionally above capacity — we measure achieved throughput"
echo "as a function of max_active_requests (= concurrent in-flight requests)."
echo ""

printf "%-12s %12s %10s %10s %10s %10s %10s\n" "concurrency" "achieved_rps" "completed" "failed" "p50_lat_ms" "p99_lat_ms" "p50_ttft_ms"
printf "%s\n" "--------------------------------------------------------------------------------------------"

for CONC in 1 4 12 24 48 96 192; do
    $CLIENT_BIN \
        --mode saturation-step \
        --base-urls "$SERVER_URL" \
        --max-active-requests $CONC \
        --num-go-workers 4 \
        --sat-stream \
        --sat-target-rate 500 \
        --sat-step-duration 15 \
        --sat-warmup-duration 5 \
        --sat-cooldown-pause 1 \
        --sat-output "$OUTDIR/burst_conc${CONC}.json" \
        > /dev/null 2>"$OUTDIR/burst_conc${CONC}_stderr.log"

    python3 -c "
import json
r = json.load(open('$OUTDIR/burst_conc${CONC}.json'))
print(f'{$CONC:<12d} {r[\"achieved_rate\"]:>12.1f} {r[\"completed\"]:>10d} {r[\"failed\"]:>10d} {r[\"p50_latency_s\"]*1000:>10.0f} {r[\"p99_latency_s\"]*1000:>10.0f} {r.get(\"p50_ttft_s\",0)*1000:>10.0f}')
"
done

# Stop stats poller
kill $STATS_PID 2>/dev/null
wait $STATS_PID 2>/dev/null || true
echo ""
echo "[stats] Stats log: $OUTDIR/stats_poll.jsonl"
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
