#!/bin/bash
# Multi-proc 12 procs: compare per_proc=80 vs per_proc=port_range//12
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

OUTDIR=/tmp/conc_investigation
mkdir -p "$OUTDIR"

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

CLIENT=eval/go_client/bin/go_dispatch

run_multi() {
    local LABEL="$1" PER_PROC="$2"
    local PIDS=()
    local T0=$(python3 -c "import time; print(time.time()+1)")
    for ((p=0; p<12; p++)); do
        echo "$T0" | $CLIENT \
            --base-urls http://127.0.0.1:18500 \
            --max-active-requests "$PER_PROC" \
            --num-go-workers 4 \
            --trace-file "$OUTDIR/trace_p${p}.jsonl" \
            --result-file "$OUTDIR/res_${LABEL}_p${p}.jsonl" \
            --sum-only --worker-id "p${p}" 2>/dev/null &
        PIDS+=($!)
    done
    for pid in "${PIDS[@]}"; do wait $pid 2>/dev/null; done
    python3 -c "
import json
tc=0;te=0;md=0
for p in range(12):
    for l in open(f'$OUTDIR/res_${LABEL}_p{p}.jsonl'):
        d=json.loads(l.strip())
        if d.get('__type__')=='summary':
            tc+=d['requests_completed'];te+=d['errors']
            dur=d.get('last_body_done_at',0)-d.get('adjusted_run_t0',0)
            md=max(md,dur)
rps=tc/md if md>0 else 0
print(f'  per_proc={\"$PER_PROC\":>6s}  completed={tc}  errors={te}  rps={rps:.0f}  dur={md:.1f}s')
"
}

clientlab/targets/cpp_server/bin/synthetic_server --config "$OUTDIR/server_config.json" &
SERVER_PID=$!
sleep 1

PORT_RANGE=$(python3 -c "lo,hi=map(int,open('/proc/sys/net/ipv4/ip_local_port_range').read().split()); print(hi-lo+1)")
PORT_DIV=$(( (PORT_RANGE - 1024) / 12 ))
echo "Port range: $PORT_RANGE, port_range//12: $PORT_DIV"
echo ""
echo "=== 12-proc sweep: per_proc max_active ==="

run_multi "a80" 80
run_multi "a160" 160
run_multi "a320" 320
run_multi "a640" 640
run_multi "a${PORT_DIV}" "$PORT_DIV"

kill $SERVER_PID 2>/dev/null
echo ""
echo "=== Done ==="
