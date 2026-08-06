#!/bin/bash
# S02 readiness-authority probe (gate S02-2N). Run inside a 2-node lease.
# Brings up a minimal 2-node Ray cluster (no exaserve legacy stack), runs
# spike_s02.py against public APIs, tears down. Thread clamps per
# doc/KNOWN_ISSUES A5 / interactive-Ray notes: without them Ray fork-bombs
# node-local thread limits.
source ~/script/env_aurora
cd ~/exaserve || exit 1
mkdir -p artifacts/hardening/s02-2n

ulimit -s 8192
export RAYON_NUM_THREADS=1 TOKENIZERS_PARALLELISM=false
export RAY_num_server_call_thread=4 RAY_core_worker_num_server_call_thread=1
export RAY_num_grpc_internal_threads=1 RAY_worker_num_grpc_internal_threads=1
export RAY_enable_metrics_collection=0

HEAD=$(hostname)
WORKER=$(grep -v "$HEAD" "$PBS_NODEFILE" | head -1)
echo "head=$HEAD worker=$WORKER"
RAY_TMP=/tmp/s02_ray_$$

ray stop --force >/dev/null 2>&1
rm -rf "$RAY_TMP"
ray start --head --port=6379 --num-cpus=8 --num-gpus=0 --temp-dir="$RAY_TMP" \
  --disable-usage-stats > artifacts/hardening/s02-2n/ray_head.log 2>&1 || exit 2

ssh "$WORKER" "bash -l -c 'source ~/script/env_aurora; ulimit -s 8192; \
  export RAYON_NUM_THREADS=1 RAY_num_server_call_thread=4 \
  RAY_core_worker_num_server_call_thread=1 RAY_num_grpc_internal_threads=1 \
  RAY_worker_num_grpc_internal_threads=1 RAY_enable_metrics_collection=0; \
  ray stop --force >/dev/null 2>&1; \
  ray start --address=$HEAD:6379 --num-cpus=8 --num-gpus=0 --disable-usage-stats'" \
  > artifacts/hardening/s02-2n/ray_worker.log 2>&1 || { echo WORKER_START_FAILED; }

# wait for 2 alive nodes (bounded)
for i in $(seq 1 30); do
  n=$(python - <<'PY'
import ray
ray.init(address="auto", logging_level="ERROR")
print(sum(1 for x in ray.nodes() if x.get("Alive")))
PY
)
  n=$(echo "$n" | tail -1)
  [ "$n" = "2" ] && break
  sleep 2
done
echo "alive_nodes=$n"

timeout 300 python scripts/hardening/spike_s02.py 2>&1 | \
  tee artifacts/hardening/s02-2n/verdict.json
rc=${PIPESTATUS[0]}

ray stop --force >/dev/null 2>&1
ssh "$WORKER" "bash -l -c 'source ~/script/env_aurora; ray stop --force'" \
  >/dev/null 2>&1
rm -rf "$RAY_TMP"
echo "S02_DONE rc=$rc"
exit "$rc"
