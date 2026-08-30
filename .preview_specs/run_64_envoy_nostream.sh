#!/bin/bash
set -eo pipefail

test -n "${PBS_JOBID:-}"
test "${PBS_JOBID%%.*}" = "8750118"
test -r "${PBS_NODEFILE:-}"
compute_host=$(hostname -s)
awk -v host="$compute_host" 'index($0, host) == 1 { found=1 } END { exit !found }' "$PBS_NODEFILE"
node_count=$(sort -u "$PBS_NODEFILE" | wc -l)
test "$node_count" -eq 64

source /home/wenyiw/script/env_aurora
unset ONEAPI_DEVICE_SELECTOR
set -u

run_root=/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs-preview-97ab2a7-64
run_dir="$run_root/proxycmp_envoy_nostream_preview_b7c4f6b/run0/n64"
test -r "$run_dir/run.yaml"
test -r "$run_dir/runtime/run.plan.json"
test -r "$run_dir/runtime/deployment.plan.json"
test -x /home/wenyiw/agpt/data/snapshots/97ab2a7f0e6b67a9f921af9e85c39336ff698282-v2c/eval/go_client/bin/go_dispatch

echo "[preview] validated interactive PBS fallback $PBS_JOBID on $node_count nodes"
echo "[preview] executing $run_dir/run.yaml"
exec bash "$run_dir/job/job.pbs"
