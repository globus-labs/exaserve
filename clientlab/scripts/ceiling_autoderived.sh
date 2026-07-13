#!/bin/bash
# Multi-proc ceiling: compare per_proc=80 vs per_proc=853 (10240/12, auto-derived budget).
# 70s cooldown between tests.
exec 2>&1
cd /home/wenyiw/exaserve

source /opt/cray/pe/lmod/default/init/bash 2>/dev/null
source /etc/bash.bashrc 2>/dev/null
module load frameworks 2>/dev/null
module load go 2>/dev/null
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
set -uo pipefail

(cd eval/go_client && bash build.sh 2>&1 | tail -1)
(cd clientlab/targets/cpp_server && bash build.sh 2>&1 | tail -1)

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/ceiling_autoderived_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="

# Reuse trace if available
PREV=/home/wenyiw/agpt/data/bench_results/clientlab/ceiling_clean_20260405T173309Z
if [ -f "$PREV/trace.jsonl" ]; then
    cp "$PREV/trace.jsonl" "$PREV"/trace_p*.jsonl "$PREV/server_config.json" "$OUTDIR/"
    echo "Reused trace from $PREV"
else
    python3 -c "
import json
cfg = {'target':{'host':'127.0.0.1','port':18500,'response_tokens':16},'client':{'model':'stub','prompt_words':32,'max_active_requests':0},'faults':{'service_time':{'distribution':'fixed','value_ms':0.0}}}
json.dump(cfg, open('$OUTDIR/server_config.json','w'))
"
    python3 -c "
import json, uuid
n = 10000000; rate = 1000000
with open('$OUTDIR/trace.jsonl', 'w') as f:
    f.write(json.dumps({'schema_version':'trace.v1','total_requests':n})+'\n')
    for i in range(n):
        f.write(json.dumps({'req_id':uuid.uuid4().hex,'timestamp':(i+1)/rate,'model':'stub','prompt':' '.join(['word']*32),'input_len':32,'output_len':16,'mode':'chat'})+'\n')
print(f'Generated {n} requests')
"
    python3 -c "
import json
lines = open('$OUTDIR/trace.jsonl').readlines()
header = lines[0]; reqs = lines[1:]
n = len(reqs)
for p in range(12):
    s = p * n // 12; e = (p+1) * n // 12
    with open(f'$OUTDIR/trace_p{p}.jsonl', 'w') as f:
        f.write(header)
        for r in reqs[s:e]: f.write(r)
"
fi

CLIENT=eval/go_client/bin/go_dispatch
clientlab/targets/cpp_server/bin/synthetic_server --config "$OUTDIR/server_config.json" &
SERVER_PID=$!
sleep 1

run_multi() {
    local LABEL="$1"
    local PER_PROC="$2"

    echo ""
    echo "--- 12-proc $LABEL: per_proc=$PER_PROC (total=$((PER_PROC * 12))) ---"
    echo "  TIME_WAIT before: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

    local PIDS=()
    local T0=$(python3 -c "import time; print(time.time()+1)")
    for p in 0 1 2 3 4 5 6 7 8 9 10 11; do
        echo "$T0" | $CLIENT \
            --base-urls http://127.0.0.1:18500 \
            --max-active-requests "$PER_PROC" \
            --num-go-workers 4 \
            --trace-file "$OUTDIR/trace_p${p}.jsonl" \
            --result-file "$OUTDIR/res_${LABEL}_p${p}.jsonl" \
            --sum-only \
            --worker-id "p${p}" \
            2>"$OUTDIR/stderr_${LABEL}_p${p}.log" &
        PIDS+=($!)
    done
    for pid in "${PIDS[@]}"; do wait $pid 2>/dev/null; done
    echo "  TIME_WAIT after:  $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

    python3 << PYEOF
import json
tc = 0; te = 0; md = 0; nf = 0
for p in range(12):
    path = "$OUTDIR/res_${LABEL}_p" + str(p) + ".jsonl"
    try:
        for l in open(path):
            d = json.loads(l.strip())
            if d.get("__type__") == "summary":
                tc += d["requests_completed"]; te += d["errors"]
                dur = d.get("last_body_done_at", 0) - d.get("adjusted_run_t0", 0)
                md = max(md, dur); nf += 1
    except FileNotFoundError:
        pass
rps = tc / md if md > 0 else 0
print(f"  procs_ok={nf}/12  completed={tc}  errors={te}  rps={rps:.0f}  dur={md:.1f}s")
PYEOF
}

PORT_RANGE=$(python3 -c "lo,hi=map(int,open('/proc/sys/net/ipv4/ip_local_port_range').read().split()); print(hi-lo+1)")
AUTO_PER_PROC=$(python3 -c "print(max(80, ($PORT_RANGE - 1024) // 12))")
echo "=== Multi-proc ceiling: 80 vs $AUTO_PER_PROC (port_range=$PORT_RANGE, /12) ==="

run_multi "a80" 80

echo "  [cooldown] 70s..."
sleep 70
echo "  TIME_WAIT after cooldown: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

run_multi "a_auto" "$AUTO_PER_PROC"

kill $SERVER_PID 2>/dev/null
echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
