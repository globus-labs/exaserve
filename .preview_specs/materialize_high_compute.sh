#!/bin/bash
set -eo pipefail

test -n "${PBS_JOBID:-}"
test "${AURORA_SUBJOB:-0}" = 1
test -r "${PBS_NODEFILE:-}"
compute_host=$(hostname -s)
awk -v host="$compute_host" 'index($0, host) == 1 { found=1 } END { exit !found }' "$PBS_NODEFILE"
node_count=$(sort -u "$PBS_NODEFILE" | wc -l)
test "$node_count" -eq 1

source /home/wenyiw/script/env_aurora
unset ONEAPI_DEVICE_SELECTOR
set -u
cd /home/wenyiw/exaserve-preview-candidate-20260811
export PYTHONPATH=src

python3 - <<'PY'
import os

from exaserve.state.atomic import ExclusiveLease

path = (
    "/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/traces/"
    "a351d9150da6cad9ddd8612e30779b1aba1c2ad91c989ecfa316e59dca2edb9b/"
    ".generate.lease"
)
if os.path.lexists(path):
    with ExclusiveLease(path, ttl_s=1800, owner_note="recover interrupted preview trace"):
        print(f"reclaimed {path}", flush=True)
PY

preview_root=/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs-preview-97ab2a7-high-compute
for spec in /home/wenyiw/exaserve/.preview_specs/*_high_two_pass.yaml; do
    python3 -m eval.cli run materialize \
        "$spec" \
        --experiments-root "$preview_root" \
        --timeout-s 3600
done
