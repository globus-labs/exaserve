import argparse
import sys

from ray._private import ray_constants, services
import ray.scripts.scripts as ray_scripts


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
    original_start_ray_process = services.start_ray_process

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

    services.start_ray_process = patched_start_ray_process


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

    _patch_raylet_launch(
        max_startup_concurrency=args.max_startup_concurrency,
        num_prestart_python_workers=args.prestart_python_workers,
    )
    cli_args = _build_ray_cli_args(args)
    return ray_scripts.cli.main(args=cli_args, prog_name="ray", standalone_mode=False)


if __name__ == "__main__":
    raise SystemExit(main())
