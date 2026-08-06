#!/bin/bash
# S01 2-node scenario battery (gate S01-2N). Run inside a 2-node lease.
source ~/script/env_aurora
cd ~/exaserve || exit 1
mkdir -p artifacts/hardening/s01-2n
export EXASERVE_SPIKE_OUTDIR=$PWD/artifacts/hardening/s01-2n
echo "lease-head=$(hostname) nodes:"
cat "$PBS_NODEFILE"
overall=0
for s in clean child-death sup-death conn-drop; do
  echo "=== scenario $s ==="
  timeout 180 python scripts/hardening/spike_s01.py --mode head --ranks 2 \
    --scenario "$s" --duration 12 2>&1 | tee "artifacts/hardening/s01-2n/verdict_$s.json"
  rc=${PIPESTATUS[0]}
  echo "scenario_rc=$rc"
  [ "$rc" -ne 0 ] && [ "$s" != "clean" ] && overall=1
  [ "$rc" -ne 0 ] && [ "$s" == "clean" ] && overall=1
  # residue check: placeholder children must not survive a scenario
  leftover=$(pgrep -f "time[.]sleep(0.5)" | wc -l)
  echo "leftover_placeholder_procs=$leftover"
done
echo "BATTERY_DONE overall=$overall"
exit $overall
