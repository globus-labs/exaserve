"""CLI entry point for the eval control plane.

Usage: python -m eval.cli <area> <command> [args]

Subcommands:
  spec validate <spec>              — parse and validate a spec file
  spec list                         — list available spec names in eval/specs/
  trace materialize <spec>          — generate cached trace artifacts for a spec
  run materialize <spec>            — create run bundles (traces + PBS jobs + run.yaml)
  run submit <target>               — qsub the PBS job for a materialized run bundle
  run execute <run.yaml>            — execute a run inside a PBS job (called by job.pbs)

This CLI replaces the old workflow of:
  python eval/exp_generator.py -> python eval/submit_all.py -> (PBS runs run_exp.sh)
"""

from __future__ import annotations

import argparse

try:
    from .lib.catalog import find_spec_path, list_spec_names
    from .lib.run_executor import execute_run, submit_run
    from .lib.run_planner import materialize_run_bundles, materialize_traces
    from .lib.spec_io import load_experiment_spec
except ImportError:  # pragma: no cover - script-mode fallback
    from eval.lib.catalog import find_spec_path, list_spec_names
    from eval.lib.run_executor import execute_run, submit_run
    from eval.lib.run_planner import materialize_run_bundles, materialize_traces
    from eval.lib.spec_io import load_experiment_spec


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Aurora eval control plane")
    subparsers = parser.add_subparsers(dest="area", required=True)

    spec_parser = subparsers.add_parser("spec", help="Spec inspection commands")
    spec_subparsers = spec_parser.add_subparsers(dest="command", required=True)
    validate_parser = spec_subparsers.add_parser("validate", help="Validate a spec file")
    validate_parser.add_argument("spec")
    spec_subparsers.add_parser("list", help="List available spec names")

    trace_parser = subparsers.add_parser("trace", help="Trace artifact commands")
    trace_subparsers = trace_parser.add_subparsers(dest="command", required=True)
    trace_materialize = trace_subparsers.add_parser("materialize", help="Materialize traces for a spec")
    trace_materialize.add_argument("spec")
    trace_materialize.add_argument("--trace-root", default=None)

    run_parser = subparsers.add_parser("run", help="Run bundle commands")
    run_subparsers = run_parser.add_subparsers(dest="command", required=True)
    run_materialize = run_subparsers.add_parser("materialize", help="Materialize run bundles")
    run_materialize.add_argument("spec")
    run_materialize.add_argument("--backend", default=None)
    run_materialize.add_argument("--experiments-root", default=None)
    run_materialize.add_argument("--trace-root", default=None)

    run_submit = run_subparsers.add_parser("submit", help="Submit a run bundle or run.yaml")
    run_submit.add_argument("target")
    run_submit.add_argument("--dry-run", action="store_true")

    run_execute = run_subparsers.add_parser("execute", help="Execute a materialized run.yaml")
    run_execute.add_argument("run_yaml")
    run_execute.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.area == "spec":
        if args.command == "list":
            for name in list_spec_names():
                print(name)
            return 0
        spec_path = find_spec_path(args.spec)
        spec = load_experiment_spec(spec_path)
        print(f"VALID {spec.name}: {spec_path}")
        return 0

    if args.area == "trace":
        spec_path = find_spec_path(args.spec)
        artifacts = materialize_traces(spec_path, trace_root=args.trace_root)
        for artifact in artifacts:
            print(artifact.trace_path)
        return 0

    if args.area == "run":
        if args.command == "materialize":
            spec_path = find_spec_path(args.spec)
            plans = materialize_run_bundles(
                spec_path,
                backend_name=args.backend,
                experiments_root=args.experiments_root,
                trace_root=args.trace_root,
            )
            for plan in plans:
                print(plan.bundle.run_yaml_path)
            return 0
        if args.command == "submit":
            return submit_run(args.target, dry_run=args.dry_run)
        if args.command == "execute":
            return execute_run(args.run_yaml, dry_run=args.dry_run)

    parser.error(f"Unsupported command: {args.area} {getattr(args, 'command', '')}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
