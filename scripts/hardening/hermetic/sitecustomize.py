"""Make this interpreter look like the hermetic CI lane.

PR-031/AC-TST-01: the release-gating suite must pass on a clean checkout with
only the core + dev extras. Locally every developer has the Aurora frameworks
stack on PATH, so a test that quietly depends on Ray passes here and fails in
CI. Putting this directory on PYTHONPATH reproduces the CI lane.

EXASERVE_HERMETIC_BLOCK overrides what is blocked. The default blocks the whole
heavy stack. The randomized-order lane blocks only ray/vllm, because
pytest-randomly loads third-party seeders from the frameworks environment
(deepspeed) that import torch during startup — a property of this machine, not
of our code, and not present in CI.
"""

import os
import sys

_DEFAULT = "ray,vllm,torch,transformers"
_BLOCKED = tuple(
    name.strip()
    for name in os.environ.get("EXASERVE_HERMETIC_BLOCK", _DEFAULT).split(",")
    if name.strip()
)


class _Blocked:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in _BLOCKED:
            raise ImportError(f"No module named {name!r} (hermetic check)")
        return None


if _BLOCKED:
    sys.meta_path.insert(0, _Blocked())
