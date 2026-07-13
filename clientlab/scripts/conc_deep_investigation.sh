#!/bin/bash
# Deep investigation: why does higher max_active_requests degrade multi-proc throughput?
# Single-proc tests with full metrics to isolate the mechanism.
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

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/conc_investigation_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="

python3 -c "
import json
cfg = {'target':{'host':'127.0.0.1','port':18500,'response_tokens':16},'client':{'model':'stub','prompt_words':32,'max_active_requests':0},'faults':{'service_time':{'distribution':'fixed','value_ms':0.0}}}
json.dump(cfg, open('$OUTDIR/server_config.json','w'))
"

# Smaller trace for per-request analysis: 1M requests at 100K rps = 10s
python3 -c "
import json, uuid
n = 1000000; rate = 100000
with open('$OUTDIR/trace.jsonl', 'w') as f:
    f.write(json.dumps({'schema_version':'trace.v1','total_requests':n})+'\n')
    for i in range(n):
        f.write(json.dumps({'req_id':uuid.uuid4().hex,'timestamp':(i+1)/rate,'model':'stub','prompt':' '.join(['word']*32),'input_len':32,'output_len':16,'mode':'chat'})+'\n')
print(f'Generated {n} requests at {rate} rps')
"

CLIENT=eval/go_client/bin/go_dispatch

run_test() {
    local LABEL="$1" ACTIVE="$2"
    local DIR="$OUTDIR/$LABEL"
    mkdir -p "$DIR"

    echo ""
    echo "--- $LABEL: max_active=$ACTIVE ---"

    local T0=$(python3 -c "import time; print(time.time()+1)")
    echo "$T0" | $CLIENT \
        --base-urls http://127.0.0.1:18500 \
        --max-active-requests "$ACTIVE" \
        --num-go-workers 4 \
        --trace-file "$OUTDIR/trace.jsonl" \
        --result-file "$DIR/results.jsonl" \
        --metrics-file "$DIR/metrics.json" \
        --phase-trace-file "$DIR/phase_trace.jsonl" \
        --phase-trace-sample-rate 0.01 \
        --enable-httptrace \
        --worker-id "$LABEL" \
        2>"$DIR/stderr.log"

    python3 << PYEOF
import json, statistics

# Summary from results
for line in open("$DIR/results.jsonl"):
    d = json.loads(line.strip())
    if d.get("__type__") == "summary":
        c = d["requests_completed"]; e = d["errors"]
        dur = d.get("last_body_done_at",0) - d.get("adjusted_run_t0",0)
        rps = c / dur if dur > 0 else 0
        print(f"  completed={c}  errors={e}  rps={rps:.0f}  dur={dur:.1f}s")
        break

# Metrics: new_conns, reused_conns
m = json.load(open("$DIR/metrics.json"))
agg = m.get("aggregate", {})
print(f"  new_conns={agg.get('new_connections',0)}  reused_conns={agg.get('reused_connections',0)}  max_active={agg.get('max_observed_active',0)}")

# Phase trace analysis
traces = []
for line in open("$DIR/phase_trace.jsonl"):
    line = line.strip()
    if not line: continue
    try:
        t = json.loads(line)
        if t.get("req_id"): traces.append(t)
    except: pass

if traces:
    tth = [t["time_to_headers_s"] for t in traces if t.get("time_to_headers_s", 0) > 0]
    lag = [t["dispatch_lag_s"] for t in traces if "dispatch_lag_s" in t]
    new_c = sum(1 for t in traces if t.get("new_connection"))
    reused = sum(1 for t in traces if t.get("reused_connection"))

    print(f"  phase_traces={len(traces)}  sampled_new={new_c}  sampled_reused={reused}")
    if tth:
        print(f"  tth: p50={statistics.median(tth)*1e6:.0f}us  p99={sorted(tth)[int(len(tth)*0.99)]*1e6:.0f}us  mean={statistics.mean(tth)*1e6:.0f}us")
    if lag:
        print(f"  dispatch_lag: p50={statistics.median(lag)*1e6:.0f}us  p99={sorted(lag)[int(len(lag)*0.99)]*1e6:.0f}us  max={max(lag)*1e6:.0f}us")

# Error breakdown
errors = {}
for line in open("$DIR/results.jsonl"):
    d = json.loads(line.strip())
    ec = d.get("error_class", "")
    if ec:
        errors[ec] = errors.get(ec, 0) + 1
if errors:
    print(f"  error_classes: {errors}")
    # Sample first error of each class
    for line in open("$DIR/results.jsonl"):
        d = json.loads(line.strip())
        if d.get("error") and d.get("error_class"):
            print(f"    [{d['error_class']}] {d['error'][:200]}")
            break
PYEOF
}

clientlab/targets/cpp_server/bin/synthetic_server --config "$OUTDIR/server_config.json" &
SERVER_PID=$!
sleep 1

echo ""
echo "=== Single-proc investigation: why does high max_active degrade? ==="
echo "=== 1M requests at 100K rps, service_time=0 ==="

run_test "active_80" 80
run_test "active_320" 320
run_test "active_1280" 1280
run_test "active_5120" 5120
run_test "active_10240" 10240

echo ""
echo "=== Now 12-proc at key points with metrics ==="

# Split trace for 12 procs
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

run_multi() {
    local LABEL="$1" PER_PROC="$2"
    local DIR="$OUTDIR/multi_${LABEL}"
    mkdir -p "$DIR"

    echo ""
    echo "--- 12-proc $LABEL: per_proc=$PER_PROC ---"

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
            --worker-id "p${p}" \
            2>"$DIR/stderr_p${p}.log" &
        PIDS+=($!)
    done
    for pid in "${PIDS[@]}"; do wait $pid 2>/dev/null; done

    python3 << PYEOF
import json
tc=0; te=0; md=0; total_new=0; total_reused=0; max_active=0
errors = {}
for p in range(12):
    for l in open(f"$DIR/results_p{p}.jsonl"):
        d = json.loads(l.strip())
        if d.get("__type__") == "summary":
            tc += d["requests_completed"]; te += d["errors"]
            dur = d.get("last_body_done_at",0) - d.get("adjusted_run_t0",0)
            md = max(md, dur)
        ec = d.get("error_class", "")
        if ec:
            errors[ec] = errors.get(ec, 0) + 1
    m = json.load(open(f"$DIR/metrics_p{p}.json"))
    agg = m.get("aggregate", {})
    total_new += agg.get("new_connections", 0)
    total_reused += agg.get("reused_connections", 0)
    max_active = max(max_active, agg.get("max_observed_active", 0))
rps = tc / md if md > 0 else 0
print(f"  completed={tc}  errors={te}  rps={rps:.0f}  dur={md:.1f}s")
print(f"  total_new_conns={total_new}  total_reused={total_reused}  max_active_any_proc={max_active}")
if errors:
    print(f"  error_classes: {errors}")
    # Find a sample error
    for p in range(12):
        found = False
        for l in open(f"$DIR/results_p{p}.jsonl"):
            d = json.loads(l.strip())
            if d.get("error"):
                print(f"    sample: [{d.get('error_class','')}] {d['error'][:200]}")
                found = True
                break
        if found: break
PYEOF
}

run_multi "a80" 80
run_multi "a320" 320
run_multi "a1280" 1280
run_multi "a2267" 2267

kill $SERVER_PID 2>/dev/null
echo ""
echo "=== Done: $(date -u) ==="
echo "=== Output: $OUTDIR ==="
