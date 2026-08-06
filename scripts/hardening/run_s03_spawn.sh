#!/bin/bash
# S03 spawned-EngineCore compatibility proof (gate S03-1N-SPAWN).
# Run inside a 1-node (or head of 2-node) lease.
source ~/script/env_aurora
cd ~/exaserve || exit 1
OUT=$PWD/artifacts/hardening/s03-1n
mkdir -p "$OUT"
export EXASERVE_RUN_LOG_ROOT=$OUT/run_logs
cp scripts/hardening/config.s03.pp2.yaml "$OUT/config.pp2.yaml"

rm -rf /tmp/exaserve_pp_shim          # SH-28 interaction: force shim regen
export EXASERVE_VLLM_PATCH_VERBOSE=1 EXASERVE_PATCH_VERBOSE=1
export no_proxy="localhost,127.0.0.1,$(hostname)"

echo "[s03] launching cluster (pp=2, 8B) on $(hostname)"
bash src/exaserve/resources/launch_cluster.sh "$OUT/config.pp2.yaml" \
  > "$OUT/launch.log" 2>&1 &
LPID=$!

ready=0
for i in $(seq 1 180); do
  if grep -q "ALL SERVICES READY" "$OUT/launch.log"; then ready=1; break; fi
  grep -qiE "FATAL|Critical Error" "$OUT/launch.log" && break
  kill -0 $LPID 2>/dev/null || break
  sleep 10
done
if [ "$ready" != "1" ]; then
  echo "S03_NOT_READY"; tail -50 "$OUT/launch.log"
  kill $LPID 2>/dev/null; pkill -f exaserve.driver; ray stop --force >/dev/null 2>&1
  exit 3
fi
echo "[s03] READY after $((i*10))s"

# --- Evidence 1: shim dir generated (EN-01) ---
ls -la /tmp/exaserve_pp_shim/ | tee "$OUT/shim_dir.txt"
cat /tmp/exaserve_pp_shim/sitecustomize.py 2>/dev/null | tee -a "$OUT/shim_dir.txt"

# --- Evidence 2: which live processes carry the shim PYTHONPATH ---
# NB: EngineCore retitles itself "VLLM::EngineCore", so scan ALL of the
# user's processes, not `pgrep python` (attempt-3 lesson).
: > "$OUT/shim_reach.txt"
for pid in $(pgrep -u "$USER" .); do
  if tr '\0' '\n' < "/proc/$pid/environ" 2>/dev/null | grep -q "exaserve_pp_shim"; then
    cmd=$(tr '\0' ' ' < "/proc/$pid/cmdline" 2>/dev/null | cut -c1-120)
    ppid=$(awk '{print $4}' "/proc/$pid/stat" 2>/dev/null)
    echo "pid=$pid ppid=$ppid cmd=$cmd" >> "$OUT/shim_reach.txt"
  fi
done
cat "$OUT/shim_reach.txt"
ps -eo pid,ppid,cmd | grep -E "EngineCore|ray::" | grep -v grep | tee "$OUT/procs.txt"

# --- Evidence 3: patch application lines from worker/engine processes ---
grep -rh "exaserve" /tmp/ray/session_latest/logs/ 2>/dev/null | \
  grep -iE "patch|_sitecustomize" | sort -u | head -60 | tee "$OUT/patch_log_lines.txt"
grep -iE "patch|_sitecustomize" "$OUT/launch.log" | sort -u | head -30 | \
  tee -a "$OUT/patch_log_lines.txt"

# --- Evidence 4: functional pp=2 canary (PP alias patches are load-bearing) ---
# Discover the real route from public serve.status() (attempt-3: guessing
# the route 404'd).
python - <<'PY' | tee "$OUT/serve_routes.txt"
import ray
from ray import serve
ray.init(address="auto", logging_level="ERROR", namespace="serve")
for name, app in serve.status().applications.items():
    print(f"{name} {app.route_prefix}")
PY
ROUTE_PREFIX=$(awk 'NR==1{print $2}' "$OUT/serve_routes.txt")
[ -z "$ROUTE_PREFIX" ] && ROUTE_PREFIX=/
ROUTE_PREFIX=${ROUTE_PREFIX%/}
echo "[s03] canary via route prefix: '$ROUTE_PREFIX'"
curl -s --noproxy '*' -m 120 "http://localhost:8000$ROUTE_PREFIX/v1/completions" \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"The capital of France is","max_tokens":8}' | tee "$OUT/canary.json"
echo

# verdict summary
engine_reach=$(grep -c "EngineCore" "$OUT/shim_reach.txt" "$OUT/procs.txt" 2>/dev/null | awk -F: '{s+=$2} END {print s+0}')
canary_ok=$(grep -c '"text"' "$OUT/canary.json" 2>/dev/null | tr -d '[:space:]')
canary_ok=${canary_ok:-0}
echo "S03_SUMMARY shim_reach_lines=$(wc -l < "$OUT/shim_reach.txt") enginecore_evidence=$engine_reach canary_ok=$canary_ok"

kill $LPID 2>/dev/null; sleep 3
pkill -f exaserve.driver 2>/dev/null
ray stop --force >/dev/null 2>&1
echo S03_SPAWN_DONE
[ "$canary_ok" -ge 1 ] || exit 4
exit 0
