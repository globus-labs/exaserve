"""Make this interpreter look like the hermetic CI lane: no Ray, no vLLM.

PR-031/AC-TST-01: the release-gating suite must pass on a clean checkout with
only the core + dev extras. Locally every developer has the Aurora frameworks
stack on PATH, so a test that quietly depends on Ray passes here and fails in
CI. Putting this directory on PYTHONPATH reproduces the CI lane exactly.
"""
import sys

_BLOCKED = ("ray", "vllm", "torch", "transformers")


class _Blocked:
    def find_spec(self, name, path=None, target=None):
        if name.split(".")[0] in _BLOCKED:
            raise ImportError(f"No module named {name!r} (hermetic check)")
        return None


sys.meta_path.insert(0, _Blocked())
