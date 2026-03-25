from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

if __package__ in (None, ""):
    REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)

try:
    from eval.lib.run_executor import submit_run
except ImportError:  # pragma: no cover - direct script fallback
    from lib.run_executor import submit_run


def _discover_run_yamls(root: str) -> list[str]:
    search_root = Path(root)
    return sorted(str(path) for path in search_root.rglob("run.yaml"))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deprecated shim for submitting materialized run bundles.",
    )
    parser.add_argument("targets", nargs="*", help="run.yaml paths, run bundle dirs, or search roots")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    targets = list(args.targets)
    if not targets:
        targets = [os.getcwd()]

    run_yamls = []
    for target in targets:
        if os.path.isdir(target):
            candidate = os.path.join(target, "run.yaml")
            if os.path.isfile(candidate):
                run_yamls.append(candidate)
            else:
                run_yamls.extend(_discover_run_yamls(target))
        else:
            run_yamls.append(target)

    exit_code = 0
    for run_yaml in run_yamls:
        result = submit_run(run_yaml, dry_run=args.dry_run)
        if result != 0:
            exit_code = result
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
