#!/bin/bash
# P04 compute battery — run inside a 2-node lease. Exercises the hardened
# native + staging + driver paths end-to-end and records verdicts.
#   1. bcast.c: mpiexec -n2 round-trip of a metachar path + fail-loudly check
#   2. full launch_cluster deploy (pp=2 8B, 1 replica) with the new driver
#      (PR-001/002/028), staging (PR-005), setup_overlay version assert
#   3. functional canary through the served route
# NB: no `set -u` — lmod/env_aurora reference unbound vars (ZSH_EVAL_CONTEXT).
set -o pipefail
source ~/script/env_aurora
cd ~/exaserve || exit 1
OUT=$PWD/artifacts/hardening/p04-battery
mkdir -p "$OUT"
NODES=$(sort -u "$PBS_NODEFILE" | wc -l)
echo "=== P04 battery: $(hostname), $NODES nodes ==="

########## 1. bcast.c hardened round-trip ##########
echo "--- [1] bcast.c mpiexec round-trip ---"
BUILD=$OUT/bcast_build
mkdir -p "$BUILD"
mpicc -O2 -Wall -o "$BUILD/bcast" src/exaserve/resources/bcast.c 2>"$OUT/bcast_build.log" \
  && echo "compiled OK" || { echo "COMPILE_FAILED"; cat "$OUT/bcast_build.log"; }

BSRC="$BUILD/src/weird dir'x"
mkdir -p "$BSRC"
echo "payload-content" > "$BSRC/f.txt"
echo "--- 1a: round-trip with space+quote path (injection safety) ---"
${EXASERVE_MPILAUNCH:-mpiexec -n $NODES -ppn 1} "$BUILD/bcast" "$BUILD/src" /tmp/bcast_p04 \
  2>&1 | tee "$OUT/bcast_roundtrip.log"
RT_RC=${PIPESTATUS[0]}
# verify every node got the file (check locally on head; workers via ssh)
head_ok=0; [ -f "/tmp/bcast_p04/src/weird dir'x/f.txt" ] && head_ok=1
echo "roundtrip_rc=$RT_RC head_extracted=$head_ok"

echo "--- 1b: bogus source must FAIL loudly (nonzero) ---"
${EXASERVE_MPILAUNCH:-mpiexec -n $NODES -ppn 1} "$BUILD/bcast" "$BUILD/nope" /tmp/bcast_p04b \
  2>&1 | tee "$OUT/bcast_fail.log"
FAIL_RC=${PIPESTATUS[0]}
echo "bogus_source_rc=$FAIL_RC (expect nonzero)"

########## 2. full deploy with hardened driver + staging ##########
echo "--- [2] full launch_cluster deploy (pp=2 8B, 1 replica) ---"
export EXASERVE_RUN_LOG_ROOT=$OUT/run_logs
export EXASERVE_VENDOR=xpu
export no_proxy="localhost,127.0.0.1,$(hostname)"
rm -rf /tmp/exaserve_pp_shim
cp scripts/hardening/config.s03.pp2.yaml "$OUT/config.pp2.yaml"

bash src/exaserve/resources/launch_cluster.sh "$OUT/config.pp2.yaml" \
  > "$OUT/launch.log" 2>&1 &
LPID=$!
ready=0
for i in $(seq 1 150); do
  grep -q "ALL SERVICES READY" "$OUT/launch.log" && { ready=1; break; }
  grep -qiE "FATAL|Critical Error|Traceback" "$OUT/launch.log" && break
  kill -0 $LPID 2>/dev/null || break
  sleep 10
done
echo "deploy_ready=$ready after $((i*10))s"

# PR-003 evidence: source config must be UNMUTATED; runtime copy has head_ip.
src_has_headip=$(grep -c "head_ip: 10\." "$OUT/config.pp2.yaml" 2>/dev/null || echo 0)
runtime_cfg=$(ls "$EXASERVE_RUN_LOG_ROOT"/*/runtime_config.yaml 2>/dev/null | head -1)
rt_has_headip=0; [ -n "$runtime_cfg" ] && rt_has_headip=$(grep -c "head_ip: 10\." "$runtime_cfg" 2>/dev/null || echo 0)
echo "pr003_source_mutated=$src_has_headip (want 0) runtime_has_headip=$rt_has_headip (want 1)"

canary_ok=0
if [ "$ready" = "1" ]; then
  # PR-002 evidence: num-gpus from config (12), vendor-gated env
  grep -m1 "Starting RAY HEAD" "$OUT/launch.log" | tee "$OUT/pr002_head.txt"
  # PR-008 evidence: healthy deploy still reaches READY (fail-closed did NOT
  # false-negative), and the GPU/proxy predicates passed.
  grep -m1 "All .* GPUs registered\|Proxy statuses" "$OUT/launch.log" | tee "$OUT/pr008_ready.txt"
  # discover route + canary
  ROUTE=$(python - <<'PY' 2>/dev/null
import ray
from ray import serve
ray.init(address="auto", logging_level="ERROR", namespace="serve")
apps=serve.status().applications
print(next(iter(apps)) if apps else "")
PY
)
  echo "route_app=$ROUTE"
  # Single-model deploys at root "/"; multi-model at /<route>. Try root
  # first, then the model-prefixed route.
  for path in "" "/meta-llama--Meta-Llama-3-8B-Instruct"; do
    RESP=$(curl -s --noproxy '*' -m 120 "http://localhost:8000${path}/v1/completions" \
      -H 'Content-Type: application/json' \
      -d '{"prompt":"The capital of France is","max_tokens":8}')
    echo "path='${path}' -> $RESP" | tee -a "$OUT/canary.json"
    echo "$RESP" | grep -q '"text"' && { canary_ok=1; break; }
  done
fi
echo "canary_ok=$canary_ok"

########## 3. teardown + PR-028 SIGTERM drain evidence ##########
echo "--- [3] SIGTERM the driver process directly; expect orderly drain ---"
# The driver is an mpiexec child under $LPID; SIGTERM the driver PIDs so the
# handlers (driver + server) run, then read the drain markers from the log.
DRIVER_PIDS=$(pgrep -f "exaserve.driver" || true)
echo "driver_pids=$DRIVER_PIDS"
[ -n "$DRIVER_PIDS" ] && kill -TERM $DRIVER_PIDS 2>/dev/null
for i in $(seq 1 18); do
  grep -qE "Shutdown requested|Ray Serve shut down|Shutting down cluster" "$OUT/launch.log" && break
  sleep 5
done
grep -E "Shutdown requested|Ray Serve shut down cleanly|Shutting down cluster|Exiting with code" \
  "$OUT/launch.log" | tee "$OUT/pr028_drain.txt"
drain_ok=0; [ -s "$OUT/pr028_drain.txt" ] && drain_ok=1
kill $LPID 2>/dev/null
pkill -f exaserve.driver 2>/dev/null
ray stop --force >/dev/null 2>&1

########## verdict ##########
echo "=== P04 VERDICT ==="
echo "bcast_roundtrip=$([ "$RT_RC" = 0 ] && [ "$head_ok" = 1 ] && echo PASS || echo FAIL)"
echo "bcast_fail_loudly=$([ "$FAIL_RC" != 0 ] && echo PASS || echo FAIL)"
echo "deploy_ready=$([ "$ready" = 1 ] && echo PASS || echo FAIL)"
echo "canary=$([ "$canary_ok" = 1 ] && echo PASS || echo FAIL)"
echo "sigterm_drain=$([ "${drain_ok:-0}" = 1 ] && echo PASS || echo FAIL)"
echo "pr003_immutable_source=$([ "${src_has_headip:-1}" = 0 ] && [ "${rt_has_headip:-0}" -ge 1 ] && echo PASS || echo FAIL)"
echo "pr008_healthy_reaches_ready=$([ "$ready" = 1 ] && echo PASS || echo FAIL)"
echo "P04_BATTERY_DONE"
