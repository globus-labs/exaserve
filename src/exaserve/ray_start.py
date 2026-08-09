import argparse
import os
import sys

_MAX_STARTUP_ENV = "EXASERVE_RAYLET_MAX_STARTUP_CONCURRENCY"
_PRESTART_ENV = "EXASERVE_RAYLET_NUM_PRESTART_PYTHON_WORKERS"


def _replace_flag(command: list[str], prefix: str, value: int) -> list[str]:
    replacement = f"{prefix}{value}"
    for index, arg in enumerate(command):
        if arg.startswith(prefix):
            command[index] = replacement
            return command
    command.append(replacement)
    return command


def _patch_raylet_launch(
    *,
    max_startup_concurrency: int | None,
    num_prestart_python_workers: int | None,
) -> None:
    # This import belongs after exact profile/source verification. Importing
    # Ray at module scope used to mutate an unverified runtime before the
    # planned compatibility boundary had run.
    from ray._private import ray_constants, services

    original_start_ray_process = services.start_ray_process
    if getattr(original_start_ray_process, "_exaserve_raylet_fanout_patch", False):
        return

    def patched_start_ray_process(command, process_type, *args, **kwargs):
        if process_type == ray_constants.PROCESS_TYPE_RAYLET:
            command = list(command)
            if max_startup_concurrency is not None:
                command = _replace_flag(
                    command,
                    "--maximum_startup_concurrency=",
                    max_startup_concurrency,
                )
            if num_prestart_python_workers is not None:
                command = _replace_flag(
                    command,
                    "--num_prestart_python_workers=",
                    num_prestart_python_workers,
                )
        return original_start_ray_process(command, process_type, *args, **kwargs)

    patched_start_ray_process._exaserve_raylet_fanout_patch = True
    services.start_ray_process = patched_start_ray_process


def _optional_positive_environment(name: str) -> int | None:
    raw = os.environ.get(name)
    if raw in (None, ""):
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer") from exc
    if value < 1:
        raise RuntimeError(f"{name} must be positive")
    return value


def _patch_raylet_launch_from_environment() -> None:
    """Generated-overlay entry point for the pinned Ray services module."""
    _patch_raylet_launch(
        max_startup_concurrency=_optional_positive_environment(_MAX_STARTUP_ENV),
        num_prestart_python_workers=_optional_positive_environment(_PRESTART_ENV),
    )


def _attest_self() -> bool:
    """Deliver this instance's exact receipt over the bounded local hop.

    Failure is fatal before the daemon starts.  Letting an unproved Ray process
    run merely delays the same failure until the global readiness deadline and
    obscures the causal transport error.
    """
    slot = os.environ.get("EXASERVE_RECEIPT_SLOT", "")
    role = os.environ.get("EXASERVE_RECEIPT_ROLE", "")
    rank = os.environ.get("EXASERVE_RECEIPT_RANK", "")
    if not (slot and role and rank.isdigit()):
        raise RuntimeError("exact Ray receipt identity is missing or malformed")
    try:
        from exaserve.compat.producers import attest_self, deliver

        receipt = attest_self(
            requirement_id=slot,
            role=role,
            component_id="ray",
            owner_scope="RANK",
            owner_rank=int(rank),
            argv=list(sys.argv),
        )
        deliver(receipt)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"Ray self-attestation failed: {type(exc).__name__}: {exc}") from exc
    return True


def _build_ray_cli_args(args: argparse.Namespace) -> list[str]:
    cli_args = [
        "start",
        f"--node-ip-address={args.node_ip_address}",
        f"--num-cpus={args.num_cpus}",
        f"--num-gpus={args.num_gpus}",
    ]
    if args.head:
        cli_args.append("--head")
        cli_args.append(f"--port={args.port}")
    else:
        cli_args.append(f"--address={args.address}")
    if args.disable_usage_stats:
        cli_args.append("--disable-usage-stats")
    if args.include_dashboard is not None:
        cli_args.append(f"--include-dashboard={args.include_dashboard}")
    if args.block:
        cli_args.append("--block")
    return cli_args


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Wrapper around `ray start` that caps Raylet internal worker fan-out.",
    )
    parser.add_argument("--head", action="store_true")
    parser.add_argument("--address")
    parser.add_argument("--node-ip-address", required=True)
    parser.add_argument("--num-cpus", required=True, type=int)
    parser.add_argument("--num-gpus", required=True, type=int)
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--disable-usage-stats", action="store_true")
    parser.add_argument("--include-dashboard")
    parser.add_argument("--block", action="store_true")
    parser.add_argument("--max-startup-concurrency", type=int, default=None)
    parser.add_argument("--prestart-python-workers", type=int, default=None)
    args = parser.parse_args()

    if args.head == bool(args.address):
        parser.error("Specify exactly one of --head or --address.")
    if args.max_startup_concurrency is not None and args.max_startup_concurrency < 1:
        parser.error("--max-startup-concurrency must be >= 1")
    if args.prestart_python_workers is not None and args.prestart_python_workers < 1:
        parser.error("--prestart-python-workers must be >= 1")

    for name, value in (
        (_MAX_STARTUP_ENV, args.max_startup_concurrency),
        (_PRESTART_ENV, args.prestart_python_workers),
    ):
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = str(value)

    # This is an independently managed Python process. Verify and activate its
    # exact compatibility profile before its first Ray import; the parent's
    # verification cannot attest a different interpreter process.
    from exaserve.compat.activator import CompatibilityActivator

    role = "ray_head" if args.head else "ray_worker"
    CompatibilityActivator().activate(
        role,
    )
    # This process IS the planned ray_head/ray_worker component instance: it
    # imported ray and applied the in-process raylet patch above, so it can
    # attest to itself. Attesting here rather than from the supervisor matters
    # -- a supervisor cannot see inside a process it did not build, and §3.2.1
    # allows SUPERVISOR attestation only for UNMODIFIED external daemons.
    _attest_self()
    import ray.scripts.scripts as ray_scripts

    cli_args = _build_ray_cli_args(args)
    return ray_scripts.cli.main(args=cli_args, prog_name="ray", standalone_mode=False)


if __name__ == "__main__":
    raise SystemExit(main())
