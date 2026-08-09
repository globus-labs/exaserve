"""AC-RDY-01/WP13: stdout/file markers can never drive control flow."""

from __future__ import annotations

import ast
import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MARKERS = ("CLUSTER FULLY READY", "ALL SERVICES READY", "readiness.json")


def test_no_python_control_expression_consumes_a_readiness_marker():
    offenders = []
    for tree_root in (ROOT / "src", ROOT / "eval", ROOT / "clientlab", ROOT / "scripts"):
        for path in tree_root.rglob("*.py"):
            if "tests" in path.parts:
                continue
            source = path.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(path))
            for node in ast.walk(tree):
                if not isinstance(node, (ast.Call, ast.Compare, ast.If, ast.While)):
                    continue
                fragment = ast.get_source_segment(source, node) or ""
                if any(marker in fragment for marker in MARKERS):
                    offenders.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    assert offenders == []


def test_no_shell_control_path_greps_readiness_markers_or_private_files():
    pattern = re.compile(r"\bgrep\b.*(?:CLUSTER FULLY READY|ALL SERVICES READY|readiness\.json)")
    offenders = []
    for tree_root in (ROOT / "src", ROOT / "eval", ROOT / "clientlab", ROOT / "scripts"):
        for suffix in ("*.sh", "*.pbs"):
            for path in tree_root.rglob(suffix):
                if "tests" in path.parts:
                    continue
                for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                    if not line.lstrip().startswith("#") and pattern.search(line):
                        offenders.append(f"{path.relative_to(ROOT)}:{line_no}")
    assert offenders == []


def test_legacy_eval_and_clientlab_lifecycle_shells_are_absent():
    assert not (ROOT / "eval" / "templates" / "run_exp.sh").exists()
    assert not list((ROOT / "eval" / "scripts").glob("*.sh"))
    assert not list((ROOT / "eval" / "scripts").glob("*.pbs"))
    assert not list((ROOT / "clientlab" / "scripts").glob("*.sh"))


def test_retired_benchmark_control_plane_cannot_be_launched():
    retired = {
        "bench_client.py",
        "bench_proxy.py",
        "stub_server.py",
        "vllm_load.py",
        "bench_internode.sh",
        "bench_multi_stub.sh",
        "run_bench.sh",
        "run_vllm_load.sh",
    }
    assert not {path.name for path in (ROOT / "benchmarks").iterdir()} & retired
