"""CLI entry point for the eval control plane.

Usage: python -m eval.cli <area> <command> [args]

Subcommands:
  spec validate <spec>              — parse and validate a spec file
  spec list                         — list available spec names in eval/specs/
  trace materialize <spec>          — generate cached trace artifacts for a spec
  run materialize <spec>            — create run bundles (traces + PBS jobs + run.yaml)
  run submit <target>               — qsub the PBS job for a materialized run bundle
  run submit-all <spec_name>       — submit all pending runs for a spec, respecting queue limits
  run execute <run.yaml>            — execute a run inside a PBS job (called by job.pbs)

Scheduler job bodies are rendered by the shared scheduler backend and invoke
``run execute`` directly. There is no intermediate lifecycle shell or second
submission orchestrator.
"""

from __future__ import annotations

import argparse

try:
    from .lib.catalog import find_spec_path, list_spec_names
    from .lib.run_executor import execute_run, submit_all, submit_run
    from .lib.run_planner import materialize_run_bundles, materialize_traces
    from .lib.spec_io import load_experiment_spec
except ImportError:  # pragma: no cover - script-mode fallback
    from eval.lib.catalog import find_spec_path, list_spec_names
    from eval.lib.run_executor import execute_run, submit_all, submit_run
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
    trace_materialize = trace_subparsers.add_parser(
        "materialize", help="Materialize traces for a spec"
    )
    trace_materialize.add_argument("spec")
    trace_materialize.add_argument("--trace-root", default=None)
    trace_materialize.add_argument(
        "--force", action="store_true", help="Regenerate traces even if cached"
    )
    trace_materialize.add_argument(
        "--timeout-s",
        type=float,
        default=3600.0,
        help="Finite deadline for the complete trace materialization (default: 3600)",
    )

    run_parser = subparsers.add_parser("run", help="Run bundle commands")
    run_subparsers = run_parser.add_subparsers(dest="command", required=True)
    run_materialize = run_subparsers.add_parser("materialize", help="Materialize run bundles")
    run_materialize.add_argument("spec")
    run_materialize.add_argument("--backend", default=None)
    run_materialize.add_argument("--experiments-root", default=None)
    run_materialize.add_argument("--trace-root", default=None)
    run_materialize.add_argument(
        "--force-trace", action="store_true", help="Regenerate traces even if cached"
    )
    run_materialize.add_argument(
        "--allow-dirty",
        action="store_true",
        help="Explicitly run the committed-HEAD snapshot while recording excluded dirty files",
    )
    run_materialize.add_argument(
        "--timeout-s",
        type=float,
        default=3600.0,
        help="Finite deadline for parallel run-bundle materialization (default: 3600)",
    )

    run_submit = run_subparsers.add_parser("submit", help="Submit a run bundle or run.yaml")
    run_submit.add_argument("target")
    run_submit.add_argument("--dry-run", action="store_true")

    run_submit_all = run_subparsers.add_parser(
        "submit-all", help="Submit all pending runs for a spec name"
    )
    run_submit_all.add_argument(
        "spec_name", nargs="+", help="Spec name(s) (matches runs/<spec_name>/)"
    )
    run_submit_all.add_argument(
        "--run-group",
        required=True,
        help="Exact run group to submit (for example run0); 'latest' is intentionally unsupported.",
    )
    run_submit_all.add_argument("--experiments-root", default=None)
    run_submit_all.add_argument("--dry-run", action="store_true")
    run_submit_all.add_argument(
        "--poll-interval",
        type=int,
        default=300,
        help="Seconds between retry attempts when queues are full (default: 300)",
    )

    run_execute = run_subparsers.add_parser("execute", help="Execute a materialized run.yaml")
    run_execute.add_argument("run_yaml")
    run_execute.add_argument("--dry-run", action="store_true")

    # derive-params: compute client config from Phase 0 saturation results
    derive_parser = subparsers.add_parser(
        "derive-params",
        help="Derive client parameters from Phase 0 saturation results",
    )
    derive_parser.add_argument(
        "result_dir", help="Path to run variant dir containing results/saturation_output.json"
    )
    derive_parser.add_argument(
        "--headroom",
        type=float,
        default=0.7,
        help="Fraction of saturation rate to use as target (default: 0.7)",
    )

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
        artifacts = materialize_traces(
            spec_path,
            trace_root=args.trace_root,
            force=args.force,
            timeout_s=args.timeout_s,
        )
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
                force_trace=args.force_trace,
                allow_dirty=args.allow_dirty,
                timeout_s=args.timeout_s,
            )
            for plan in plans:
                print(plan.bundle.run_yaml_path)
            return 0
        if args.command == "submit":
            return submit_run(args.target, dry_run=args.dry_run)
        if args.command == "submit-all":
            rc = 0
            for spec in args.spec_name:
                ret = submit_all(
                    spec,
                    run_group=args.run_group,
                    experiments_root=args.experiments_root,
                    dry_run=args.dry_run,
                    poll_interval=args.poll_interval,
                )
                if ret != 0:
                    rc = ret
            return rc
        if args.command == "execute":
            return execute_run(args.run_yaml, dry_run=args.dry_run)

    if args.area == "derive-params":
        return _derive_params(args.result_dir, args.headroom)

    parser.error(f"Unsupported command: {args.area} {getattr(args, 'command', '')}")
    return 2


def _derive_params(result_dir: str, headroom: float) -> int:
    """Derive client config from Phase 0 saturation output."""
    import math
    from pathlib import Path

    sat_path = Path(result_dir) / "results" / "saturation_output.json"
    if not sat_path.exists():
        # Try direct path (if result_dir already points to results/)
        sat_path = Path(result_dir) / "saturation_output.json"
    if not sat_path.exists():
        print(f"ERROR: saturation_output.json not found in {result_dir}", flush=True)
        return 1

    from exaserve.state.atomic import strict_json_load_path

    try:
        sat_output = strict_json_load_path(sat_path)
    except (OSError, ValueError) as exc:
        print(f"ERROR: saturation output is invalid: {exc}", flush=True)
        return 1
    sat_rate = sat_output.get("saturation_rate", 0)
    if sat_rate <= 0:
        print("ERROR: saturation_rate is 0 — no saturation point found", flush=True)
        return 1

    steps = sat_output.get("steps", [])
    if not steps:
        print("ERROR: no steps found in saturation output", flush=True)
        return 1

    healthy_steps = [s for s in steps if s.get("healthy")]
    best = (
        healthy_steps[-1] if healthy_steps else max(steps, key=lambda s: s.get("achieved_rate", 0))
    )
    p99_latency = float(best.get("p99_latency_s", 0))
    p99_ttft = float(best.get("p99_ttft_s", 0))
    achieved_rps = float(best.get("achieved_rate", 0))

    rate_per_node = sat_rate * headroom
    latency_for_sizing = p99_latency if p99_latency > 0 else 0.1  # fallback
    concurrency_needed = rate_per_node * latency_for_sizing
    safe_per_proc = 1200  # from clientlab findings

    num_go_procs = max(1, math.ceil(concurrency_needed / safe_per_proc))
    go_concurrency = max(16, math.ceil(concurrency_needed / num_go_procs))

    print("# Derived client parameters from Phase 0 saturation results")
    print(f"# Source: {sat_path}")
    print(f"# Saturation rate: {sat_rate} rps")
    print(
        f"# Best healthy step: achieved={achieved_rps:.1f} rps, p99={p99_latency * 1000:.1f}ms"
        + (f", p99_ttft={p99_ttft * 1000:.1f}ms" if p99_ttft > 0 else "")
    )
    print(f"# Headroom: {headroom:.0%}")
    print()
    print(f"rate_per_node: {rate_per_node:.1f}")
    print(f"num_go_procs: {num_go_procs}")
    print(f"go_concurrency: {go_concurrency}")
    print("num_go_workers: 4")
    print()
    print("# Paste into your weak-scaling spec under 'client:' and 'workload:'")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
