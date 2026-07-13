#!/bin/bash
# Investigation: why does multi-proc get port exhaustion when single-proc doesn't?
# 70s cooldown between tests to let TIME_WAIT expire.
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

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/conc_clean_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="

python3 -c "
import json
cfg = {'target':{'host':'127.0.0.1','port':18500,'response_tokens':16},'client':{'model':'stub','prompt_words':32,'max_active_requests':0},'faults':{'service_time':{'distribution':'fixed','value_ms':0.0}}}
json.dump(cfg, open('$OUTDIR/server_config.json','w'))
"

# 1M requests at 100K rps = 10s
python3 -c "
import json, uuid
n = 1000000; rate = 100000
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
PORT_RANGE=$(python3 -c "lo,hi=map(int,open('/proc/sys/net/ipv4/ip_local_port_range').read().split()); print(hi-lo+1)")
echo "Ephemeral port range: $PORT_RANGE"

clientlab/targets/cpp_server/bin/synthetic_server --config "$OUTDIR/server_config.json" &
SERVER_PID=$!
sleep 1

cooldown() {
    echo ""
    echo "  [cooldown] Waiting 70s for TIME_WAIT to expire..."
    echo "  [cooldown] Before: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"
    sleep 70
    echo "  [cooldown] After:  $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"
}

analyze_single() {
    local DIR="$OUTDIR/$1"
    python3 << PYEOF
import json
m = json.load(open("$DIR/metrics.json"))
agg = m.get("aggregate", {})
new_c = agg.get("new_connections", 0)
reused_c = agg.get("reused_connections", 0)
total = new_c + reused_c
reuse_pct = reused_c / total * 100 if total > 0 else 0
completed = 0; errs = 0; dur = 0; errors = {}
for line in open("$DIR/results.jsonl"):
    d = json.loads(line.strip())
    if d.get("__type__") == "summary":
        completed = d["requests_completed"]; errs = d["errors"]
        dur = d.get("last_body_done_at",0) - d.get("adjusted_run_t0",0)
    ec = d.get("error_class", "")
    if ec: errors[ec] = errors.get(ec, 0) + 1
rps = completed / dur if dur > 0 else 0
print(f"  completed={completed}  errors={errs}  rps={rps:.0f}  dur={dur:.1f}s")
print(f"  new_conns={new_c}  reused_conns={reused_c}  reuse={reuse_pct:.2f}%")
if errors: print(f"  error_classes: {errors}")
PYEOF
}

analyze_multi() {
    local DIR="$OUTDIR/$1" NPROCS="$2"
    python3 << PYEOF
import json
total_new = 0; total_reused = 0; total_completed = 0; total_errors = 0
max_dur = 0; per_proc_new = []; all_errors = {}
for p in range($NPROCS):
    m = json.load(open(f"$DIR/metrics_p{p}.json"))
    agg = m.get("aggregate", {})
    nc = agg.get("new_connections", 0)
    rc = agg.get("reused_connections", 0)
    total_new += nc; total_reused += rc
    per_proc_new.append(nc)
    for line in open(f"$DIR/results_p{p}.jsonl"):
        d = json.loads(line.strip())
        if d.get("__type__") == "summary":
            total_completed += d["requests_completed"]
            total_errors += d["errors"]
            dur = d.get("last_body_done_at",0) - d.get("adjusted_run_t0",0)
            max_dur = max(max_dur, dur)
        ec = d.get("error_class", "")
        if ec: all_errors[ec] = all_errors.get(ec, 0) + 1
total = total_new + total_reused
reuse_pct = total_reused / total * 100 if total > 0 else 0
rps = total_completed / max_dur if max_dur > 0 else 0
print(f"  completed={total_completed}  errors={total_errors}  rps={rps:.0f}  dur={max_dur:.1f}s")
print(f"  total_new={total_new}  total_reused={total_reused}  reuse={reuse_pct:.2f}%")
print(f"  per_proc_new={per_proc_new}")
if all_errors: print(f"  error_classes: {all_errors}")
PYEOF
}

run_single() {
    local LABEL="$1" ACTIVE="$2"
    local DIR="$OUTDIR/$LABEL"
    mkdir -p "$DIR"
    echo ""
    echo "--- Single-proc $LABEL: max_active=$ACTIVE ---"
    echo "  TIME_WAIT before: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"
    local T0=$(python3 -c "import time; print(time.time()+1)")
    echo "$T0" | $CLIENT \
        --base-urls http://127.0.0.1:18500 \
        --max-active-requests "$ACTIVE" \
        --num-go-workers 4 \
        --trace-file "$OUTDIR/trace.jsonl" \
        --result-file "$DIR/results.jsonl" \
        --metrics-file "$DIR/metrics.json" \
        --enable-httptrace \
        --sum-only \
        --worker-id "$LABEL" \
        2>"$DIR/stderr.log"
    echo "  TIME_WAIT after:  $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"
    analyze_single "$LABEL"
}

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
    analyze_multi "$LABEL" 12
}

echo ""
echo "=========================================="
echo "=== Test 1: Single-proc baseline       ==="
echo "=========================================="
run_single "single_80" 80
cooldown
run_single "single_1280" 1280
cooldown
run_single "single_10240" 10240
cooldown
run_single "single_27000" 27000
cooldown

echo ""
echo "=========================================="
echo "=== Test 2: 12-proc (clean starts)     ==="
echo "=========================================="
run_multi "multi_80" 80
cooldown
run_multi "multi_320" 320
cooldown
run_multi "multi_1280" 1280
cooldown
run_multi "multi_2267" 2267

kill $SERVER_PID 2>/dev/null
echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
