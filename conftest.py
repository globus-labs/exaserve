"""Repository-wide pytest bootstrap.

Single source-layout contract (WP0 / AC-TST-01): every test subtree must be
independently collectible, so the ``src/`` package root (``exaserve``) and the
repository root (``eval``, ``clientlab``) are placed on ``sys.path`` here and
nowhere else. Do not add per-directory conftest path hacks; child processes
that need the same contract must receive it explicitly (see
``eval/lib/run_planner.py`` subprocess environment).
"""

import os
import sys

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))

_TEST_PATHS = [REPO_ROOT]
if os.environ.get("EXASERVE_TEST_INSTALLED_WHEEL") != "1":
    _TEST_PATHS.append(os.path.join(REPO_ROOT, "src"))

for _path in _TEST_PATHS:
    if _path not in sys.path:
        sys.path.insert(0, _path)

# Hermeticity guard (WP0/WP11): unit tests must never reach live Hugging Face
# services, including from forkserver/spawn children (env is inherited). An
# integration job may pre-set these to "0" before invoking pytest.
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TRANSFORMERS_OFFLINE", "1")
