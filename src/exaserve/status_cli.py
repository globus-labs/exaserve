"""Operator-facing, generation-bound deployment status commands."""

from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import asdict

from .status_api import InvalidDeploymentStatus, read_deployment_status

EXIT_READY = 0
EXIT_NOT_READY = 2
EXIT_TIMEOUT = 3
EXIT_INVALID = 4


def _identity_error(status, args) -> str:
    if args.generation is not None and status.generation != args.generation:
        return f"generation {status.generation} != expected {args.generation}"
    if args.plan_hash and status.deployment_plan_hash != args.plan_hash:
        return "deployment plan hash does not match the expected identity"
    if args.binding_hash and status.allocation_binding_hash != args.binding_hash:
        return "allocation binding hash does not match the expected identity"
    return ""


def _render(status, *, as_json: bool) -> None:
    if as_json:
        print(json.dumps(asdict(status), sort_keys=True, indent=2))
        return
    effective_state = (
        "READY_STALE" if status.state == "READY" and not status.ready else status.state
    )
    print(
        f"{effective_state} revision={status.revision} "
        f"deployment={status.deployment_id} generation={status.generation}"
    )
    print(f"plan={status.deployment_plan_hash} binding={status.allocation_binding_hash}")
    if status.advertised_endpoint:
        print(f"endpoint={status.advertised_endpoint}")
    if status.reason_code:
        print(f"reason={status.reason_code}: {status.detail or ''}")
    snapshot = status.readiness_snapshot
    for label in ("blockers", "missing_identities", "unhealthy_identities"):
        values = snapshot.get(label) or []
        if values:
            print(f"{label}:")
            for value in values:
                print(f"  - {value}")


def _read(args):
    try:
        status = read_deployment_status(args.run_dir)
    except (InvalidDeploymentStatus, OSError, ValueError) as exc:
        print(f"invalid deployment status: {exc}", file=sys.stderr)
        return None, EXIT_INVALID
    if status is None:
        return None, EXIT_NOT_READY
    mismatch = _identity_error(status, args)
    if mismatch:
        print(f"status identity mismatch: {mismatch}", file=sys.stderr)
        return None, EXIT_INVALID
    return status, None


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog="exaserve-status",
        description="Inspect or wait for the canonical DeploymentStatus record",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("show", "wait"):
        command = sub.add_parser(name)
        command.add_argument("--run-dir", required=True)
        command.add_argument("--generation", type=int)
        command.add_argument("--plan-hash", default="")
        command.add_argument("--binding-hash", default="")
        command.add_argument("--json", action="store_true")
        if name == "wait":
            command.add_argument("--timeout", type=float, default=1800.0)
            command.add_argument("--poll", type=float, default=1.0)
    args = parser.parse_args(argv)

    if args.command == "show":
        status, error = _read(args)
        if error is not None:
            if error == EXIT_NOT_READY:
                print(f"no deployment status under {args.run_dir}", file=sys.stderr)
            return error
        _render(status, as_json=args.json)
        return EXIT_READY if status.ready else EXIT_NOT_READY

    if args.generation is None or not args.plan_hash:
        parser.error("wait requires --generation and --plan-hash")
    if args.timeout <= 0 or args.poll <= 0:
        parser.error("--timeout and --poll must be positive")
    deadline = time.monotonic() + args.timeout
    last = None
    while True:
        status, error = _read(args)
        if error == EXIT_INVALID:
            return error
        if status is not None:
            last = status
            if status.ready:
                _render(status, as_json=args.json)
                return EXIT_READY
            if status.terminal:
                _render(status, as_json=args.json)
                return EXIT_NOT_READY
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            if last is not None:
                _render(last, as_json=args.json)
            else:
                print(f"no deployment status under {args.run_dir}", file=sys.stderr)
            return EXIT_TIMEOUT
        time.sleep(min(args.poll, remaining))


if __name__ == "__main__":
    raise SystemExit(main())
