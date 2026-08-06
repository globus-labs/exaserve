#!/bin/bash
# PR-031 / AC-TST-01: run the release-gating suite as the hermetic CI lane sees
# it — no Ray, no vLLM, no torch, no transformers. A test that needs those must
# skip, not fail; anything that fails here would fail in CI.
set -o pipefail
cd "$(dirname "$0")/../.." || exit 1
HERM="$PWD/scripts/hardening/hermetic"
PY="${EXASERVE_PYTHON_EXEC:-python3}"
echo "=== hermetic check (blocked: ray, vllm, torch, transformers) ==="
PYTHONPATH="$HERM" "$PY" -c "
import sys
for mod in ('ray', 'vllm'):
    try:
        __import__(mod); print(f'FAIL: {mod} still importable'); sys.exit(1)
    except ImportError:
        pass
print('blocker active')" || exit 1
PYTHONPATH="$HERM" "$PY" -m pytest tests/ eval/tests/ clientlab/tests/ -q "$@"
rc=$?
echo "HERMETIC_CHECK_DONE rc=$rc"
exit $rc
