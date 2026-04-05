#!/bin/bash
# Final sanity check: 12-proc with default max_active=max_conns=0
# Test 1: 300K rps (achievable) — expect healthy
# Test 2: 1M rps (above ceiling) — expect degraded diagnosis
exec 2>&1
cd /home/wenyiw/aurora_rayserver

source /opt/cray/pe/lmod/default/init/bash 2>/dev/null
source /etc/bash.bashrc 2>/dev/null
module load frameworks 2>/dev/null
module load go 2>/dev/null
unset http_proxy https_proxy HTTP_PROXY HTTPS_PROXY
set -uo pipefail

(cd eval/go_client && bash build.sh 2>&1 | tail -1)
(cd clientlab/targets/cpp_server && bash build.sh 2>&1 | tail -1)

OUTDIR="/home/wenyiw/agpt/data/bench_results/clientlab/safezone_final_$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$OUTDIR"
echo "=== Output: $OUTDIR ==="
echo "=== Node: $(hostname) ==="
echo "=== nproc: $(nproc) ==="

python3 -c "
import json
cfg = {'target':{'host':'127.0.0.1','port':18500,'response_tokens':16},'client':{'model':'stub','prompt_words':32,'max_active_requests':0},'faults':{'service_time':{'distribution':'fixed','value_ms':0.0}}}
json.dump(cfg, open('$OUTDIR/server_config.json','w'))
"

CLIENT=eval/go_client/bin/go_dispatch

# Compute per-proc budget: 80% of port range / 12 procs
PORT_RANGE=$(python3 -c "lo,hi=map(int,open('/proc/sys/net/ipv4/ip_local_port_range').read().split()); print(hi-lo+1)")
PER_PROC=$(python3 -c "print(max(80, int($PORT_RANGE * 0.8) // 12))")
echo "port_range=$PORT_RANGE  per_proc=$PER_PROC (0.8*$PORT_RANGE/12)"

gen_trace() {
    local RATE=$1 DUR=$2 TLABEL=$3
    local N=$((RATE * DUR))
    python3 << PYEOF
import json, uuid
n = $N; rate = $RATE; label = "$TLABEL"; outdir = "$OUTDIR"
with open(f'{outdir}/trace_{label}.jsonl', 'w') as f:
    f.write(json.dumps({'schema_version':'trace.v1','total_requests':n})+'\n')
    for i in range(n):
        f.write(json.dumps({'req_id':uuid.uuid4().hex,'timestamp':(i+1)/rate,'model':'stub','prompt':' '.join(['word']*32),'input_len':32,'output_len':16,'mode':'chat'})+'\n')
print(f'  Generated {n} requests at {rate} rps for {label}')
# Split for 12 procs
lines = open(f'{outdir}/trace_{label}.jsonl').readlines()
header = lines[0]; reqs = lines[1:]
nr = len(reqs)
for p in range(12):
    s = p * nr // 12; e = (p+1) * nr // 12
    with open(f'{outdir}/trace_{label}_p{p}.jsonl', 'w') as f:
        f.write(header)
        for r in reqs[s:e]: f.write(r)
PYEOF
}

run_multi() {
    local LABEL="$1" TRACE_LABEL="$2"
    local DIR="$OUTDIR/$LABEL"
    mkdir -p "$DIR"

    echo ""
    echo "--- $LABEL (auto-derived: --max-active-requests $PER_PROC per proc) ---"
    echo "  TIME_WAIT before: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

    local PIDS=()
    local T0=$(python3 -c "import time; print(time.time()+1)")
    for p in 0 1 2 3 4 5 6 7 8 9 10 11; do
        echo "$T0" | $CLIENT \
            --base-urls http://127.0.0.1:18500 \
            --max-active-requests "$PER_PROC" \
            --num-go-workers 4 \
            --trace-file "$OUTDIR/trace_${TRACE_LABEL}_p${p}.jsonl" \
            --result-file "$DIR/res_p${p}.jsonl" \
            --sum-only \
            --worker-id "p${p}" \
            2>"$DIR/stderr_p${p}.log" &
        PIDS+=($!)
    done
    for pid in "${PIDS[@]}"; do wait $pid 2>/dev/null; done
    echo "  TIME_WAIT after:  $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

    python3 << PYEOF
import json
tc = 0; te = 0; md = 0; nf = 0
all_ec = {}; all_es = {}; all_warnings = set()
health = "healthy"
max_lag = 0

for p in range(12):
    path = "$DIR/res_p" + str(p) + ".jsonl"
    try:
        for l in open(path):
            d = json.loads(l.strip())
            if d.get("__type__") == "summary":
                tc += d["requests_completed"]; te += d["errors"]
                dur = d.get("last_body_done_at", 0) - d.get("adjusted_run_t0", 0)
                md = max(md, dur); nf += 1
                for cls, cnt in d.get("error_counts", {}).items():
                    all_ec[cls] = all_ec.get(cls, 0) + cnt
                for cls, msg in d.get("error_samples", {}).items():
                    if cls not in all_es: all_es[cls] = msg
                for w in d.get("dispatch_warnings", []):
                    all_warnings.add(w)
                h = d.get("dispatch_health", "")
                if h in ("port_exhaustion", "degraded"):
                    health = h
                lag = d.get("dispatch_lag_p99_s", 0)
                max_lag = max(max_lag, lag)
    except FileNotFoundError:
        print(f"  p{p}: MISSING")

rps = tc / md if md > 0 else 0
print(f"  procs_ok={nf}/12  completed={tc}  errors={te}  rps={rps:.0f}  dur={md:.1f}s")
print(f"  dispatch_health={health}  max_dispatch_lag_p99={max_lag:.3f}s")
if all_ec:
    print(f"  error_counts: {all_ec}")
    for cls, msg in all_es.items():
        print(f"    [{cls}] {msg}")
for w in sorted(all_warnings):
    print(f"  WARNING: {w}")
PYEOF
}

clientlab/targets/cpp_server/bin/synthetic_server --config "$OUTDIR/server_config.json" &
SERVER_PID=$!
sleep 1

echo ""
echo "=========================================="
echo "=== Test 1: 300K rps (5s) — achievable ==="
echo "=========================================="
gen_trace 300000 5 "300k"
run_multi "test_300k" "300k"

echo ""
echo "  [cooldown] 70s..."
sleep 70
echo "  TIME_WAIT after cooldown: $(ss -s 2>/dev/null | grep -o 'timewait [0-9]*')"

echo ""
echo "=========================================="
echo "=== Test 2: 1M rps (5s) — above ceiling ==="
echo "=========================================="
gen_trace 1000000 5 "1m"
run_multi "test_1m" "1m"

kill $SERVER_PID 2>/dev/null
echo ""
echo "=== Done: $(date -u) ==="
