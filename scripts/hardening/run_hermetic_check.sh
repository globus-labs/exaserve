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
# A clean checkout has only our own dev deps. This developer machine has the
# Aurora frameworks stack, whose third-party pytest plugins (deepspeed) import
# torch during collection and would trip the blocker for reasons that have
# nothing to do with our code. Disabling plugin autoload reproduces the clean
# checkout; anything we actually need is passed explicitly.
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH="$HERM" \
  "$PY" -m pytest tests/ eval/tests/ clientlab/tests/ -q "$@"
rc=$?

# Second lane: randomized order, to catch tests that depend on execution
# order. pytest-randomly pulls third-party seeders from the frameworks env
# (deepspeed -> torch), so this lane blocks only ray/vllm. CI has neither
# problem and runs both lanes with the full block.
if [ "${EXASERVE_HERMETIC_RANDOM:-1}" = "1" ] && [ $rc -eq 0 ]; then
  echo "=== hermetic check (randomized order; blocked: ray, vllm) ==="
  EXASERVE_HERMETIC_BLOCK="ray,vllm" PYTHONPATH="$HERM" \
    "$PY" -m pytest tests/ eval/tests/ clientlab/tests/ -q -o addopts="" -p randomly
  rc=$?
fi

echo "HERMETIC_CHECK_DONE rc=$rc"
exit $rc
