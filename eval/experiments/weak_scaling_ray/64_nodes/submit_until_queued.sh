#!/usr/bin/env bash
# Submit job.pbs every 5 minutes until the job is successfully queued.
# Use when queue allows only one job per user — run this to resubmit as soon as a slot opens.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

INTERVAL=300   # 5 minutes

while true; do
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Submitting job.pbs ..."
  output=$(qsub job.pbs 2>&1) && {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Job submitted successfully: $output"
    exit 0
  }
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Submit failed: $output"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Retrying in ${INTERVAL}s"
  sleep "$INTERVAL"
done
