#!/bin/bash
# Submit all v2 weak-scaling runs in the background with minimal footprint.
#
# Usage:
#   nohup bash scripts/submit_v2.sh &> /tmp/submit_v2.log &
#   disown
#
# Monitor:
#   tail -f /tmp/submit_v2.log
#   qstat -u $USER
#
# The script submits haproxy and direct variants sequentially, respecting
# per-queue slot limits.  Between polls it sleeps (no CPU).  Safe to leave
# running on a login node.

set -eu
cd "$(dirname "$0")/.."

# Low scheduling priority — avoids drawing attention on shared login nodes.
renice +19 $$ >/dev/null 2>&1 || true

source ~/script/env_aurora 2>/dev/null || true

echo "[$(date -u +%FT%TZ)] Starting v2 weak-scaling submission"
echo "  PID=$$  host=$(hostname)"

for spec in weakscaling_haproxy_short_v2 weakscaling_direct_short_v2; do
    echo ""
    echo "[$(date -u +%FT%TZ)] Submitting $spec ..."
    python3 -m eval.cli run submit-all "$spec" --poll-interval 300
    echo "[$(date -u +%FT%TZ)] $spec submission complete"
done

echo ""
echo "[$(date -u +%FT%TZ)] All submissions complete"
