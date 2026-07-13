#!/bin/bash
# Clean multi-proc ceiling test: 12 procs at per_proc=80 vs per_proc=10240
# 70s cooldown between tests to avoid TIME_WAIT contamination.
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

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/ceiling_clean_multiproc_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="

python3 -c "
import json
cfg = {'target':{'host':'127.0.0.1','port':18500,'response_tokens':16},'client':{'model':'stub','prompt_words':32,'max_active_requests':0},'faults':{'service_time':{'distribution':'fixed','value_ms':0.0}}}
json.dump(cfg, open('$OUTDIR/server_config.json','w'))
"

# 10M requests at 1M rps = 10s trace
python3 -c "
import json, uuid
n = 10000000; rate = 1000000
with open('$OUTDIR/trace.jsonl', 'w') as f:
    f.write(json.dumps({'schema_version':'trace.v1','total_requests':n})+'\n')
    for i in range(n):
        f.write(json.dumps({'req_id':uuid.uuid4().hex,'timestamp':(i+1)/rate,'model':'stub','prompt':' '.join(['word']*32),'input_len':32,'output_len':16,'mode':'chat'})+'\n')
print(f'Generated {n} requests at {rate} rps')
"

# Split for 12 procs
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

CLIENT=eval/go_client/bin/go_dispatch

run_multi() {
    local LABEL="$1" PER_PROC="$2"
    local DIR="$OUTDIR/$LABEL"
    mkdir -p "$DIR"
    echo ""
    echo "--- 12-proc $LABEL: per_proc=$PER_PROC (total=$((PER_PROC * 12))) ---"
    echo "  TIME_WAIT before: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"
    local PIDS=()
    local T0=$(python3 -c "import time; print(time.time()+1)")
    for ((p=0; p<12; p++)); do
        echo "$T0" | $CLIENT \
            --base-urls http://127.0.0.1:18500 \
            --max-active-requests "$PER_PROC" \
            --num-go-workers 4 \
            --trace-file "$OUTDIR/trace_p${p}.jsonl" \
            --result-file "$DIR/results_p${p}.jsonl" \
            --metrics-file "$DIR/metrics_p${p}.json" \
            --enable-httptrace \
            --sum-only \
            --worker-id "p${p}" \
            2>"$DIR/stderr_p${p}.log" &
        PIDS+=($!)
    done
    for pid in "${PIDS[@]}"; do wait $pid 2>/dev/null; done
    echo "  TIME_WAIT after:  $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"
    python3 << PYEOF
import json
tc=0; te=0; md=0; errors={}
for p in range(12):
    for l in open(f"$DIR/results_p{p}.jsonl"):
        d = json.loads(l.strip())
        if d.get("__type__") == "summary":
            tc += d["requests_completed"]; te += d["errors"]
            dur = d.get("last_body_done_at",0) - d.get("adjusted_run_t0",0)
            md = max(md, dur)
        ec = d.get("error_class", "")
        if ec: errors[ec] = errors.get(ec, 0) + 1
rps = tc / md if md > 0 else 0
print(f"  completed={tc}  errors={te}  rps={rps:.0f}  dur={md:.1f}s")
if errors: print(f"  error_classes: {errors}")
PYEOF
}

clientlab/targets/cpp_server/bin/synthetic_server --config "$OUTDIR/server_config.json" &
SERVER_PID=$!
sleep 1

echo ""
echo "=========================================="
echo "=== Clean 12-proc ceiling comparison   ==="
echo "=========================================="

run_multi "a80" 80

echo ""
echo "  [cooldown] Waiting 70s..."
sleep 70
echo "  [cooldown] TIME_WAIT: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

run_multi "a10240" 10240

echo ""
echo "  [cooldown] Waiting 70s..."
sleep 70
echo "  [cooldown] TIME_WAIT: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

# Also test auto-derived (no --max-active-requests)
echo ""
echo "--- 12-proc auto-derived (no flag) ---"
DIR="$OUTDIR/auto"
mkdir -p "$DIR"
echo "  TIME_WAIT before: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"
PIDS=()
T0=$(python3 -c "import time; print(time.time()+1)")
for ((p=0; p<12; p++)); do
    echo "$T0" | $CLIENT \
        --base-urls http://127.0.0.1:18500 \
        --num-go-workers 4 \
        --trace-file "$OUTDIR/trace_p${p}.jsonl" \
        --result-file "$DIR/results_p${p}.jsonl" \
        --metrics-file "$DIR/metrics_p${p}.json" \
        --sum-only \
        --worker-id "p${p}" \
        2>"$DIR/stderr_p${p}.log" &
    PIDS+=($!)
done
for pid in "${PIDS[@]}"; do wait $pid 2>/dev/null; done
echo "  TIME_WAIT after:  $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"
python3 << 'PYEOF'
import json, os
DIR = os.environ.get("DIR", "")
if not DIR:
    import sys; sys.exit(0)
tc=0; te=0; md=0; errors={}
for p in range(12):
    for l in open(f"{DIR}/results_p{p}.jsonl"):
        d = json.loads(l.strip())
        if d.get("__type__") == "summary":
            tc += d["requests_completed"]; te += d["errors"]
            dur = d.get("last_body_done_at",0) - d.get("adjusted_run_t0",0)
            md = max(md, dur)
        ec = d.get("error_class", "")
        if ec: errors[ec] = errors.get(ec, 0) + 1
rps = tc / md if md > 0 else 0
print(f"  completed={tc}  errors={te}  rps={rps:.0f}  dur={md:.1f}s")
if errors: print(f"  error_classes: {errors}")
PYEOF

kill $SERVER_PID 2>/dev/null
echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
