#!/bin/bash
# Run 12-proc ceiling at auto-derived per_proc with error capture in summary.
exec 2>&1
cd /home/wenyiw/exaserve

source /opt/cray/pe/lmod/default/init/bash 2>/dev/null
source /etc/bash.bashrc 2>/dev/null
module load frameworks 2>/dev/null
module load go 2>/dev/null
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
set -uo pipefail

(cd eval/go_client && bash build.sh 2>&1 | tail -1)

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/catch_errors_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
PREV=/home/wenyiw/agpt/data/bench_results/clientlab/ceiling_clean_20260405T173309Z
cp "$PREV/trace.jsonl" "$PREV"/trace_p*.jsonl "$PREV/server_config.json" "$OUTDIR/"

clientlab/targets/cpp_server/bin/synthetic_server --config "$OUTDIR/server_config.json" &
SERVER_PID=$!
sleep 1

PORT_RANGE=$(python3 -c "lo,hi=map(int,open('/proc/sys/net/ipv4/ip_local_port_range').read().split()); print(hi-lo+1)")
PER_PROC=$(python3 -c "print(max(80, ($PORT_RANGE - 1024) // 12))")
echo "port_range=$PORT_RANGE  per_proc=$PER_PROC  total=$((PER_PROC * 12))"
echo "TIME_WAIT before: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

CLIENT=eval/go_client/bin/go_dispatch
PIDS=()
T0=$(python3 -c "import time; print(time.time()+1)")
for p in 0 1 2 3 4 5 6 7 8 9 10 11; do
    echo "$T0" | $CLIENT \
        --base-urls http://127.0.0.1:18500 \
        --max-active-requests "$PER_PROC" \
        --num-go-workers 4 \
        --trace-file "$OUTDIR/trace_p${p}.jsonl" \
        --result-file "$OUTDIR/res_p${p}.jsonl" \
        --sum-only \
        --worker-id "p${p}" \
        2>"$OUTDIR/stderr_p${p}.log" &
    PIDS+=($!)
done
for pid in "${PIDS[@]}"; do wait $pid 2>/dev/null; done

echo "TIME_WAIT after:  $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

echo ""
echo "=== Per-proc results ==="
export OUTDIR="$OUTDIR"
python3 << 'PYEOF'
import json, os

outdir = os.environ.get("OUTDIR", "/tmp")
tc = 0; te = 0; md = 0
all_error_counts = {}
all_error_samples = {}

for p in range(12):
    path = f"{outdir}/res_p{p}.jsonl"
    try:
        for line in open(path):
            d = json.loads(line.strip())
            if d.get("__type__") == "summary":
                completed = d["requests_completed"]
                errors = d["errors"]
                dur = d.get("last_body_done_at", 0) - d.get("adjusted_run_t0", 0)
                ec = d.get("error_counts", {})
                es = d.get("error_samples", {})
                tc += completed; te += errors; md = max(md, dur)

                if errors > 0:
                    print(f"  p{p}: completed={completed} errors={errors} error_counts={ec}")
                    for cls, msg in es.items():
                        print(f"      [{cls}] {msg}")
                else:
                    print(f"  p{p}: completed={completed} errors=0")

                for cls, cnt in ec.items():
                    all_error_counts[cls] = all_error_counts.get(cls, 0) + cnt
                    if cls not in all_error_samples:
                        all_error_samples[cls] = es.get(cls, "")
                break
    except FileNotFoundError:
        print(f"  p{p}: MISSING RESULT FILE")

rps = tc / md if md > 0 else 0
print()
print(f"=== Aggregate ===")
print(f"  completed={tc}  errors={te}  rps={rps:.0f}  dur={md:.1f}s")
if all_error_counts:
    print(f"  error_counts: {all_error_counts}")
    for cls, msg in all_error_samples.items():
        print(f"    [{cls}] {msg}")
else:
    print(f"  No errors!")
PYEOF

kill $SERVER_PID 2>/dev/null
echo ""
echo "=== Done ==="
