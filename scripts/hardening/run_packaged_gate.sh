#!/bin/bash
# Run the installed-wheel release gate inside a validated Aurora compute lease.
# This is a test/CI boundary only; it is not part of deployment lifecycle.
set -eo pipefail

if [ "$#" -ne 2 ]; then
  echo "usage: $0 WHEEL OUTPUT_DIR" >&2
  exit 2
fi

wheel=$1
output_dir=$2
repo_root=$(cd "$(dirname "$0")/../.." && pwd -P)

if [ -z "${PBS_JOBID:-}" ]; then
  echo "packaged gate requires a validated Aurora PBS allocation" >&2
  exit 2
fi
current_host=$(hostname)
current_host=${current_host%%.*}
if [ ! -r "${PBS_NODEFILE:-}" ] || ! awk -v host="$current_host" '
  { node = $0; sub(/\..*$/, "", node); if (node == host) found = 1 }
  END { exit found ? 0 : 1 }
' "$PBS_NODEFILE"; then
  echo "packaged gate has no valid allocation nodefile" >&2
  exit 2
fi

source "$HOME/script/env_aurora"
unset ONEAPI_DEVICE_SELECTOR
set -u

wheel=$(realpath -e -- "$wheel")
case "$wheel" in
  "$repo_root"/artifacts/hardening/*.whl) ;;
  *)
    echo "wheel must be an immutable hardening artifact under $repo_root/artifacts/hardening" >&2
    exit 2
    ;;
esac

if [ -e "$output_dir" ]; then
  echo "output directory already exists: $output_dir" >&2
  exit 2
fi
mkdir -p -- "$output_dir"
output_dir=$(realpath -e -- "$output_dir")

scratch=$(mktemp -d /tmp/exaserve-packaged-gate.XXXXXX)
cleanup() {
  rm -rf -- "$scratch"
}
trap cleanup EXIT

python3 -m venv "$scratch/venv"
python_bin="$scratch/venv/bin/python"
"$python_bin" -m pip install pip==26.2.1
"$python_bin" -m pip install -r "$repo_root/requirements/ci.lock"
"$python_bin" -m pip install --no-deps "$wheel"

mkdir "$scratch/run"
cd "$scratch/run"
EXASERVE_TEST_INSTALLED_WHEEL=1 \
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 \
HF_HUB_OFFLINE=1 \
TRANSFORMERS_OFFLINE=1 \
  "$python_bin" -m pytest -q -p no:randomly "$repo_root" \
  2>&1 | tee "$output_dir/pytest.log"

"$python_bin" -c "
from importlib import resources
root = resources.files('exaserve') / 'resources'
for name in ('bcast.c', 'bcast.Makefile'):
    assert (root / name).is_file(), f'{name} missing from wheel'
assert not (root / 'launch_cluster.sh').is_file(), 'retired shell adapter shipped'
import exaserve.launcher
print('packaged resources OK')
" 2>&1 | tee "$output_dir/resources.log"

"$scratch/venv/bin/mypy" --ignore-missing-imports --follow-imports=skip \
  "$repo_root/src/exaserve/plan/contracts.py" \
  "$repo_root/src/exaserve/control/contracts.py" \
  "$repo_root/src/exaserve/telemetry.py" \
  "$repo_root/src/exaserve/state/results.py" \
  2>&1 | tee "$output_dir/mypy.log"

"$python_bin" - "$wheel" "$output_dir/receipt.json" <<'PY'
from __future__ import annotations

import hashlib
import json
import os
import platform
import socket
import sys
from pathlib import Path

wheel = Path(sys.argv[1])
receipt = Path(sys.argv[2])
payload = {
    "schema_version": 1,
    "status": "PASS",
    "wheel": str(wheel),
    "wheel_sha256": hashlib.sha256(wheel.read_bytes()).hexdigest(),
    "hostname": socket.getfqdn(),
    "pbs_job_id": os.environ["PBS_JOBID"],
    "pbs_nodefile": os.environ["PBS_NODEFILE"],
    "python": platform.python_version(),
    "python_executable": sys.executable,
}
with receipt.open("x", encoding="utf-8") as stream:
    json.dump(payload, stream, indent=2, sort_keys=True)
    stream.write("\n")
PY

echo "PACKAGED_GATE_PASS wheel=$wheel output=$output_dir"
