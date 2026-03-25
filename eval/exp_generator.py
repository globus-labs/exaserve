from __future__ import annotations

import argparse
import os
import sys

if __package__ in (None, ""):
    REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)

try:
    from eval.lib.catalog import find_spec_path, list_spec_names
    from eval.lib.run_planner import materialize_run_bundles
except ImportError:  # pragma: no cover - direct script fallback
    from lib.catalog import find_spec_path, list_spec_names
    from lib.run_planner import materialize_run_bundles


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deprecated shim for the new eval run planner.",
    )
    parser.add_argument(
        "-e",
        "--experiment",
        default=None,
        help="Spec name or path. Pass 'list' to print available specs.",
    )
    parser.add_argument(
        "--backend",
        default=None,
        choices=["ray", "mock"],
    )
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--setup-only", action="store_true")
    parser.add_argument("--experiments-root", default=None)
    parser.add_argument("--trace-root", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.experiment or args.experiment == "list":
        for name in list_spec_names():
            print(name)
        return 0

    if args.setup_only:
        print("WARNING: --setup-only is deprecated and ignored in the new planner.")

    spec_path = find_spec_path(args.experiment)
    plans = materialize_run_bundles(
        spec_path,
        backend_name=args.backend,
        experiments_root=args.experiments_root,
        trace_root=args.trace_root,
    )
    for plan in plans:
        print(plan.bundle.run_yaml_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
