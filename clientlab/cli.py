import argparse
import json
from typing import List, Optional

from clientlab.runner.runtime import compare_studies, plan_study, report_study, run_smoke, run_study


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="ClientLab measurement platform for go_dispatch.")
    sub = parser.add_subparsers(dest="command")

    plan_parser = sub.add_parser("plan", help="Expand a study spec into concrete run points.")
    plan_parser.add_argument("spec")
    plan_parser.add_argument("--output-dir", default=None)

    run_parser = sub.add_parser("run", help="Execute a study spec and collect artifacts.")
    run_parser.add_argument("spec")
    run_parser.add_argument("--output-dir", default=None)
    run_parser.add_argument("--local", action="store_true", help="Force local execution mode.")
    run_parser.add_argument(
        "--pbs", action="store_true", help="Force PBS interactive execution mode."
    )

    report_parser = sub.add_parser(
        "report", help="Generate or refresh report artifacts for a study directory."
    )
    report_parser.add_argument("study_dir")

    compare_parser = sub.add_parser("compare", help="Compare two completed study directories.")
    compare_parser.add_argument("study_dir_a")
    compare_parser.add_argument("study_dir_b")

    smoke_parser = sub.add_parser("smoke", help="Run a lightweight built-in preset.")
    smoke_parser.add_argument("preset")
    smoke_parser.add_argument("--output-dir", default=None)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not getattr(args, "command", None):
        parser.print_help()
        return 2

    if args.command == "plan":
        plan = plan_study(args.spec, output_dir=args.output_dir)
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0
    if args.command == "run":
        study_dir = run_study(
            args.spec, output_dir=args.output_dir, force_local=args.local, force_pbs=args.pbs
        )
        print(study_dir)
        return 0
    if args.command == "report":
        report_path = report_study(args.study_dir)
        print(report_path)
        return 0
    if args.command == "compare":
        comparison = compare_studies(args.study_dir_a, args.study_dir_b)
        print(json.dumps(comparison, indent=2, sort_keys=True))
        return 0
    if args.command == "smoke":
        study_dir = run_smoke(args.preset, output_dir=args.output_dir)
        print(study_dir)
        return 0
    raise AssertionError(f"Unhandled command: {args.command}")
