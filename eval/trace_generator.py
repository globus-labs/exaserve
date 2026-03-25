from __future__ import annotations

import argparse
import os
import sys

if __package__ in (None, ""):
    REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if REPO_ROOT not in sys.path:
        sys.path.insert(0, REPO_ROOT)

try:
    from eval.lib.catalog import find_spec_path
    from eval.lib.run_planner import materialize_traces
except ImportError:  # pragma: no cover - direct script fallback
    from lib.catalog import find_spec_path
    from lib.run_planner import materialize_traces


LEGACY_SPEC_BY_EXP_TYPE = {
    "peak": "peak_trace",
    "burst": "burst_trace",
    "sparse": "sparse_trace",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Deprecated shim for the new trace artifact materializer.",
    )
    parser.add_argument("--spec", default=None, help="Spec name or path")
    parser.add_argument(
        "--exp-type",
        choices=sorted(LEGACY_SPEC_BY_EXP_TYPE),
        default="sparse",
        help="Legacy shortcut for the old manual trace configs.",
    )
    parser.add_argument("--trace-root", default=None)
    return parser


def main(argv: list[str] | None = None) -> int:
    import warnings
    warnings.warn(
        "eval/trace_generator.py is deprecated. Use 'python -m eval.cli trace materialize' instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    parser = build_parser()
    args = parser.parse_args(argv)
    spec_name = args.spec or LEGACY_SPEC_BY_EXP_TYPE[args.exp_type]
    spec_path = find_spec_path(spec_name)
    artifacts = materialize_traces(spec_path, trace_root=args.trace_root)
    for artifact in artifacts:
        print(artifact.trace_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
