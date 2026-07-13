#!/bin/bash
# Multi-proc dispatch ceiling: sweep num_go_procs with auto-derived concurrency.
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

OUTDIR=/tmp/ceiling_multiproc
mkdir -p "$OUTDIR"

# Server config
python3 -c "
import json
cfg = {'target':{'host':'127.0.0.1','port':18500,'response_tokens':16},'client':{'model':'stub','prompt_words':32,'max_active_requests':0},'faults':{'service_time':{'distribution':'fixed','value_ms':0.0}}}
json.dump(cfg, open('$OUTDIR/server_config.json','w'))
"

# Generate trace: 10M requests at 1M rps
python3 -c "
import json, uuid
n = 10000000; rate = 1000000
with open('$OUTDIR/trace.jsonl', 'w') as f:
    f.write(json.dumps({'schema_version':'trace.v1','total_requests':n})+'\n')
    for i in range(n):
        f.write(json.dumps({'req_id':uuid.uuid4().hex,'timestamp':(i+1)/rate,'model':'stub','prompt':' '.join(['word']*32),'input_len':32,'output_len':16,'mode':'chat'})+'\n')
print(f'Generated {n} requests')
"

clientlab/targets/cpp_server/bin/synthetic_server --config "$OUTDIR/server_config.json" &
SERVER_PID=$!
sleep 1

echo ""
echo "=== Multi-proc dispatch ceiling (auto-derived concurrency) ==="
printf "%-8s %12s %8s %8s\n" "Procs" "Achieved RPS" "Errors" "Dur (s)"
printf "%s\n" "----------------------------------------"

# Read ephemeral port range for per-proc budget calculation
PORT_RANGE=$(python3 -c "lo,hi=map(int,open('/proc/sys/net/ipv4/ip_local_port_range').read().split()); print(hi-lo+1)")
echo "Ephemeral port range: $PORT_RANGE"

for NPROCS in 1 2 4 8 12; do
    # Per-proc concurrency: 10240 / nprocs, floor 80. Avoids TIME_WAIT port exhaustion.
    PER_PROC=$(python3 -c "print(max(80, 10240 // $NPROCS))")
    echo "--- $NPROCS procs, per_proc_active=$PER_PROC ---"

    # Split trace into NPROCS partitions
    python3 -c "
import json
lines = open('$OUTDIR/trace.jsonl').readlines()
header = lines[0]
reqs = lines[1:]
n = len(reqs)
for p in range($NPROCS):
    start = p * n // $NPROCS
    end = (p + 1) * n // $NPROCS
    with open(f'$OUTDIR/trace_p{p}.jsonl', 'w') as f:
        f.write(header)
        for r in reqs[start:end]:
            f.write(r)
"

    # Launch NPROCS Go processes
    PIDS=()
    T0=$(python3 -c "import time; print(time.time()+1)")
    for ((p=0; p<NPROCS; p++)); do
        echo "$T0" | eval/go_client/bin/go_dispatch \
            --base-urls http://127.0.0.1:18500 \
            --max-active-requests "$PER_PROC" \
            --num-go-workers 4 \
            --trace-file "$OUTDIR/trace_p${p}.jsonl" \
            --result-file "$OUTDIR/results_${NPROCS}p_p${p}.jsonl" \
            --sum-only \
            --worker-id "p${p}" \
            2>/dev/null &
        PIDS+=($!)
    done

    for pid in "${PIDS[@]}"; do wait $pid 2>/dev/null; done

    # Aggregate
    python3 -c "
import json
total_c = 0; total_e = 0; max_dur = 0
for p in range($NPROCS):
    for line in open(f'$OUTDIR/results_${NPROCS}p_p{p}.jsonl'):
        d = json.loads(line.strip())
        if d.get('__type__') == 'summary':
            total_c += d['requests_completed']
            total_e += d['errors']
            dur = d.get('last_body_done_at',0) - d.get('adjusted_run_t0',0)
            max_dur = max(max_dur, dur)
rps = total_c / max_dur if max_dur > 0 else 0
print(f'$NPROCS        {rps:>12.0f} {total_e:>8d} {max_dur:>8.1f}')
"
done

echo ""
echo "Previous ceiling: ~625K at 12 procs"
kill $SERVER_PID 2>/dev/null
