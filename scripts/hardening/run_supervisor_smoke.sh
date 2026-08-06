#!/bin/bash
# IMP-B01/B02/B03/B04 wiring validation on real nodes:
#   - the SUPERVISED CLI path deploys to READY (no regression vs exec-bash),
#   - readiness comes from the GATE (readiness.json), not the stdout marker,
#   - a compatibility receipt is collected from every required role,
#   - SIGTERM to the supervisor reaps the whole launched tree (IMP-B03),
#   - exit status is typed.
set -o pipefail
source ~/script/env_aurora
cd ~/exaserve || exit 1
OUT=$PWD/artifacts/hardening/supervisor-smoke
rm -rf "$OUT"; mkdir -p "$OUT"
NODES=$(sort -u "$PBS_NODEFILE" | wc -l)
export EXASERVE_RUN_LOG_ROOT=$OUT/run_logs EXASERVE_VENDOR=xpu
export EXASERVE_USE_SUPERVISOR=1
export EXASERVE_DEPLOYMENT_ID="smoke$$"
export no_proxy="localhost,127.0.0.1,$(hostname)"
cp scripts/hardening/config.direct.8b.yaml "$OUT/config.yaml"
sed -i "s/^  num_nodes: .*/  num_nodes: $NODES/" "$OUT/config.yaml"
echo "=== supervisor smoke: $NODES nodes on $(hostname) ==="

PYTHONPATH=$PWD/src python -c "
import sys; sys.argv=['exaserve-launch-cluster','$OUT/config.yaml']
from exaserve.cli import launch_cluster; launch_cluster()
" > "$OUT/launch.log" 2>&1 &
SUPER_PID=$!
echo "supervisor_pid=$SUPER_PID"

ready=0
for i in $(seq 1 150); do
  [ -s "$(ls -t "$OUT"/run_logs/*/readiness.json 2>/dev/null | head -1)" ] && { ready=1; break; }
  grep -qE "\[Driver\] FATAL|Refusing to declare|refusing to declare|FIRST CAUSE" "$OUT/launch.log" && break
  kill -0 $SUPER_PID 2>/dev/null || break
  sleep 10
done
echo "ready_signal=$ready after $((i*10))s"

SNAP=$(ls -t "$OUT"/run_logs/*/readiness.json 2>/dev/null | head -1)
gate_ok=0; gate_ready=""
if [ -n "$SNAP" ]; then
  cp "$SNAP" "$OUT/readiness.json"
  gate_ready=$(python -c "import json;d=json.load(open('$SNAP'));print(d.get('ready'))")
  echo "--- readiness snapshot ---"; cat "$SNAP"
  [ "$gate_ready" = "True" ] && gate_ok=1
fi
# The marker must NOT appear before the gate passes.
marker_before_gate=0
if grep -q "CLUSTER FULLY READY" "$OUT/launch.log" && [ "$gate_ok" != "1" ]; then
  marker_before_gate=1
fi
grep -m1 "compatibility profile" "$OUT/launch.log" | tee "$OUT/compat_receipt.txt"
grep -m1 "supervising:" "$OUT/launch.log" | tee -a "$OUT/compat_receipt.txt"
grep -m1 "\[Readiness\] gate:" "$OUT/launch.log" | tee -a "$OUT/compat_receipt.txt"
receipts=$(python -c "
import json,sys
try: print(json.load(open('$SNAP')).get('receipts',0))
except Exception: print(0)" 2>/dev/null)
echo "receipts_collected=$receipts"
# EN-01: the engine must attest ITSELF, so it must not appear in the
# externally-attested list.
engine_self=$(python -c "
import json
try:
    d = json.load(open('$SNAP'))
    print('yes' if 'engine' not in (d.get('externally_attested_roles') or []) else 'no')
except Exception: print('unknown')" 2>/dev/null)
echo "engine_self_attested=$engine_self"

canary_ok=0; leftover=99
if [ "$gate_ok" = "1" ]; then
  IPS=$(ls -t "$OUT"/run_logs/*/ray_node_ips.txt 2>/dev/null | head -1)
  IP=$(head -1 "$IPS" 2>/dev/null)
  RESP=$(curl -s --noproxy '*' -m 60 "http://${IP:-localhost}:8000/v1/completions" \
    -H 'Content-Type: application/json' \
    -d '{"prompt":"The capital of France is","max_tokens":8}')
  echo "canary -> $RESP" | tee "$OUT/canary.json"
  echo "$RESP" | grep -q '"text"' && canary_ok=1

  # IMP-B03 reaping. The managed child runs in its OWN session (setsid), so
  # the supervisor's pgid is the WRONG thing to count — measure the launched
  # tree's group plus any surviving exaserve/ray processes on this node.
  CHILD=$(pgrep -P $SUPER_PID -f launch_cluster.sh | head -1)
  CHILD_PGID=$(ps -o pgid= -p "${CHILD:-0}" 2>/dev/null | tr -d ' ')
  before_group=$(pgrep -g "${CHILD_PGID:-0}" 2>/dev/null | wc -l)
  before_named=$(pgrep -c -f 'exaserve\.(driver|server)|raylet|ServeReplica' 2>/dev/null)
  echo "--- SIGTERM supervisor (child=$CHILD child_pgid=$CHILD_PGID group=$before_group named=$before_named) ---"
  kill -TERM $SUPER_PID
  for i in $(seq 1 36); do kill -0 $SUPER_PID 2>/dev/null || break; sleep 5; done
  wait $SUPER_PID 2>/dev/null; SUPER_RC=$?
  sleep 5
  after_group=$(pgrep -g "${CHILD_PGID:-0}" 2>/dev/null | wc -l)
  after_named=$(pgrep -c -f 'exaserve\.(driver|server)|raylet|ServeReplica' 2>/dev/null)
  leftover=$(( after_group + after_named ))
  echo "supervisor_exit=$SUPER_RC group_before=$before_group group_after=$after_group named_before=$before_named named_after=$after_named"
fi

pkill -f exaserve.driver 2>/dev/null; ray stop --force >/dev/null 2>&1
echo "=== SUPERVISOR SMOKE VERDICT ==="
echo "gate_ready=$([ "$gate_ok" = 1 ] && echo PASS || echo "FAIL ($gate_ready)")"
echo "marker_never_precedes_gate=$([ "$marker_before_gate" = 0 ] && echo PASS || echo FAIL)"
echo "receipts=$([ "${receipts:-0}" -ge 5 ] && echo "PASS ($receipts)" || echo "FAIL ($receipts)")"
echo "canary=$([ "$canary_ok" = 1 ] && echo PASS || echo FAIL)"
echo "tree_reaped=$([ "${leftover:-99}" -le 0 ] && echo PASS || echo "FAIL ($leftover left)")"
echo "engine_self_attested=$([ "$engine_self" = "yes" ] && echo PASS || echo "FAIL ($engine_self)")"
echo "SUPERVISOR_SMOKE_DONE"
