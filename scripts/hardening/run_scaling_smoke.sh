#!/bin/bash
# Weak-scaling throughput smoke on the LIVE hardened code (staged via
# launch_cluster's MPI bcast of src/, not the eval HEAD snapshot).
# Deploys direct-mode tp=1 8B across all lease nodes, then runs the
# throughput probe. Records aggregate + per-node RPS for comparison against
# the recorded direct baseline (~110 rps/node offered, ~6.8k agg @64n).
#
# Weak-scaling pass criterion: per-node RPS stays ~flat as nodes grow.
set -o pipefail
source ~/script/env_aurora
cd ~/exaserve || exit 1
NODES=$(sort -u "$PBS_NODEFILE" | wc -l)
TAG="n${NODES}"
OUT=$PWD/artifacts/hardening/scaling-smoke/$TAG
mkdir -p "$OUT"
export EXASERVE_RUN_LOG_ROOT=$OUT/run_logs EXASERVE_VENDOR=xpu
export no_proxy="localhost,127.0.0.1,$(hostname)"
export EXASERVE_NODEFILE="$PBS_NODEFILE"
cp scripts/hardening/config.direct.8b.yaml "$OUT/config.yaml"
sed -i "s/^  num_nodes: .*/  num_nodes: $NODES/" "$OUT/config.yaml"
echo "=== scaling smoke: $NODES nodes ($(hostname)) ==="

bash src/exaserve/resources/launch_cluster.sh "$OUT/config.yaml" > "$OUT/launch.log" 2>&1 &
LPID=$!
ready=0
# larger clusters take longer to stage+deploy; scale the timeout with N, but
# CAP it well under the 1h debug-scaling walltime so the probe + teardown
# always get to run (leave ~15 min). 250 iters × 10s = ~42 min max wait.
MAXIT=$((60 + NODES * 6))
[ "$MAXIT" -gt 250 ] && MAXIT=250
# NB: do NOT break on a replica-level Traceback — vLLM EngineCore hits the
# transient A2 EADDRINUSE port race under many concurrent starts, and Ray
# Serve retries the replica (RAY_SERVE_MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT).
# Only a driver-level FATAL/Refusing (fail-closed readiness) or process exit
# is terminal.
for i in $(seq 1 $MAXIT); do
  grep -q "ALL SERVICES READY" "$OUT/launch.log" && { ready=1; break; }
  grep -qE "\[Driver\] FATAL|\[ExaServe\] .*Refusing to declare|Critical Error:" "$OUT/launch.log" && break
  kill -0 $LPID 2>/dev/null || break
  sleep 10
done
READY_S=$((i*10))
echo "ready=$ready after ${READY_S}s"
grep -m1 "All .* GPUs registered" "$OUT/launch.log" | tee "$OUT/gpus.txt"

if [ "$ready" = "1" ]; then
  # Warm up briefly, then measure. Ray Serve binds the HTTP proxy to the Ray
  # node IP (the PBS hostname:8000 returns 503) — use ray_node_ips.txt.
  # Distributed (Direct-MPI) load: one client rank per node via mpiexec -ppn 1,
  # each rank saturating its own node, so the measurement is NOT limited by a
  # single head-node client at large N (baseline findings #4).
  sleep 5
  IPS=$(ls "$OUT"/run_logs/*/ray_node_ips.txt 2>/dev/null | head -1)
  SHARDS="$OUT/probe_shards"
  rm -rf "$SHARDS"; mkdir -p "$SHARDS"
  echo "--- throughput probe (Direct-MPI, $NODES ranks, ips=$IPS) ---"
  head -3 "$IPS" 2>/dev/null
  ${EXASERVE_MPILAUNCH:-mpiexec -n $NODES -ppn 1} \
    python scripts/hardening/throughput_probe.py \
      --mpi --shard-dir "$SHARDS" --ips-file "$IPS" --route "" --duration 30 \
      --concurrency-per-node 32 --max-tokens 64 --prompt-len 64 \
      > "$OUT/probe_ranks.log" 2>&1
  echo "shards: $(ls "$SHARDS" | wc -l)/$NODES"
  python scripts/hardening/throughput_probe.py \
    --aggregate "$SHARDS" --out "$OUT/throughput.json" 2>&1 | tee "$OUT/probe.log"
fi

kill $LPID 2>/dev/null; pkill -f exaserve.driver 2>/dev/null; ray stop --force >/dev/null 2>&1
echo "=== SCALING VERDICT ($TAG) ==="
echo "deploy_ready=$([ "$ready" = 1 ] && echo PASS || echo FAIL) ready_s=${READY_S}"
if [ -f "$OUT/throughput.json" ]; then
  python - "$OUT/throughput.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"aggregate_rps={d['aggregate_rps']} per_node_rps={d['per_node_rps']} "
      f"err={d['error_rate']} p50={d['p50_latency_s']}s p99={d['p99_latency_s']}s")
PY
fi
echo "SCALING_SMOKE_DONE tag=$TAG nodes=$NODES"
