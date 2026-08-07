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

# Wait the way a CONSUMER is supposed to: through the shared status API, not
# by grepping a log. That also dogfoods the §3.4 boundary on every smoke run.
ready=0
for i in $(seq 1 240); do
  RUNDIR=$(ls -td "$OUT"/run_logs/*/ 2>/dev/null | head -1)
  if [ -n "$RUNDIR" ]; then
    state=$(PYTHONPATH=$PWD/src python -c "
from exaserve.status_api import read_deployment_status
s = read_deployment_status('$RUNDIR')
print(s.state if s else 'NONE')" 2>/dev/null)
    [ "$state" = "READY" ] && { ready=1; break; }
    case "$state" in FAILED|STOPPED|CANCELLED) break;; esac
  fi
  kill -0 $SUPER_PID 2>/dev/null || break
  sleep 10
done
echo "ready_signal=$ready after $((i*10))s (shared status state=${state:-NONE})"
status_ok=$([ "$state" = "READY" ] && echo 1 || echo 0)

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
# The ROOT's record now carries the exact receipt-slot reconciliation, so the
# check is "every PLANNED slot is covered", not "at least N receipts arrived".
receipts=$(python -c "
import json
try:
    d = json.load(open('$SNAP'))
    line = next((s for s in d.get('satisfied',[]) if s.startswith('receipts:')), '')
    print(line.split(':',1)[1].strip().split()[0] if line else '0/0')
except Exception: print('0/0')" 2>/dev/null)
echo "receipt_slots=$receipts"
receipts_ok=$(python -c "
a,_,b='$receipts'.partition('/')
print(1 if a and b and a==b and int(a)>0 else 0)" 2>/dev/null)

# The child publishes EVIDENCE under a different name; readiness.json is the
# root's verdict. Both must exist, and they must not be the same file.
EV=$(ls -t "$OUT"/run_logs/*/deployment_evidence.json 2>/dev/null | head -1)
evidence_ok=0
if [ -n "$EV" ]; then
  cp "$EV" "$OUT/deployment_evidence.json"
  evidence_ok=$(python -c "
import json;d=json.load(open('$EV'));print(1 if d.get('applications_running') and d.get('evidence_only') else 0)" 2>/dev/null)
fi
echo "evidence_published=$evidence_ok"

# IMP-B04: no detached Ray receipt actor may exist on the production path.
actor_absent=$(grep -qc "receipt collector ready" "$OUT/launch.log" 2>/dev/null && echo 0 || echo 1)
echo "ray_receipt_actor_absent=$actor_absent"

# EN-01: the engine attests ITSELF over the authenticated path. A replica
# forwarding a rebuilt copy would be an owner assertion, so what is checked is
# that an ENGINE-role evidence receipt reached the head.
engine_self=$(grep -m1 "evidence receipts:" "$OUT/launch.log" 2>/dev/null | grep -q "'engine'" && echo yes || echo no)
grep -m1 "evidence receipts:" "$OUT/launch.log" | tee -a "$OUT/compat_receipt.txt"
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
  # The composition root has no launch_cluster.sh child any more; its own
  # child is the rank launcher. Measuring a missing child's group meant
  # `pgrep -g 0`, which reported a meaningless count as "leftover".
  CHILD=$(pgrep -P $SUPER_PID 2>/dev/null | head -1)
  CHILD_PGID=$(ps -o pgid= -p "${CHILD:-0}" 2>/dev/null | tr -d ' ')
  if [ -n "$CHILD_PGID" ]; then
    before_group=$(pgrep -g "$CHILD_PGID" 2>/dev/null | wc -l)
  else
    before_group=0
  fi
  before_named=$(pgrep -c -f 'exaserve\.(driver|server)|raylet|ServeReplica' 2>/dev/null)
  echo "--- SIGTERM supervisor (child=$CHILD child_pgid=$CHILD_PGID group=$before_group named=$before_named) ---"
  kill -TERM $SUPER_PID
  for i in $(seq 1 36); do kill -0 $SUPER_PID 2>/dev/null || break; sleep 5; done
  wait $SUPER_PID 2>/dev/null; SUPER_RC=$?
  sleep 5
  if [ -n "$CHILD_PGID" ]; then
    after_group=$(pgrep -g "$CHILD_PGID" 2>/dev/null | wc -l)
  else
    after_group=0
  fi
  after_named=$(pgrep -c -f 'exaserve\.(driver|server)|raylet|ServeReplica' 2>/dev/null)
  leftover=$(( after_group + after_named ))
  echo "supervisor_exit=$SUPER_RC group_before=$before_group group_after=$after_group named_before=$before_named named_after=$after_named"
fi

pkill -f exaserve.driver 2>/dev/null; ray stop --force >/dev/null 2>&1
echo "=== SUPERVISOR SMOKE VERDICT ==="
echo "gate_ready=$([ "$gate_ok" = 1 ] && echo PASS || echo "FAIL ($gate_ready)")"
echo "shared_status_ready=$([ "${status_ok:-0}" = 1 ] && echo PASS || echo "FAIL (${state:-NONE})")"
echo "marker_never_precedes_gate=$([ "$marker_before_gate" = 0 ] && echo PASS || echo FAIL)"
echo "receipt_slots_exact=$([ "${receipts_ok:-0}" = 1 ] && echo "PASS ($receipts)" || echo "FAIL ($receipts)")"
echo "evidence_separate_from_verdict=$([ "${evidence_ok:-0}" = 1 ] && echo PASS || echo FAIL)"
echo "ray_receipt_actor_retired=$([ "${actor_absent:-0}" = 1 ] && echo PASS || echo FAIL)"
echo "engine_self_attested=$([ "$engine_self" = yes ] && echo PASS || echo "FAIL ($engine_self)")"
echo "canary=$([ "$canary_ok" = 1 ] && echo PASS || echo FAIL)"
echo "tree_reaped=$([ "${leftover:-99}" -le 0 ] && echo PASS || echo "FAIL ($leftover left)")"
echo "SUPERVISOR_SMOKE_DONE"
