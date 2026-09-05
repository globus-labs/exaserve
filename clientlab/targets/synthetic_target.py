"""Owning launcher for one socket-activated ClientLab synthetic target."""

from __future__ import annotations

import argparse
import json
import os

_MAX_CONFIG_BYTES = 64 << 10


def _rank() -> int:
    for name in ("PALS_RANKID", "PMI_RANK", "SLURM_PROCID"):
        value = os.environ.get(name)
        if value is not None:
            return int(value)
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--config")
    group.add_argument("--config-template")
    group.add_argument("--config-json")
    parser.add_argument("--binary", required=True)
    args = parser.parse_args(argv)

    from exaserve.state.atomic import strict_json_load_path, strict_json_loads

    config_path = None
    if args.config_json is not None:
        if len(args.config_json.encode("utf-8")) > _MAX_CONFIG_BYTES:
            raise SystemExit("synthetic target inline config exceeds 64 KiB")
        payload = strict_json_loads(args.config_json)
    else:
        config_path = (
            args.config if args.config is not None else args.config_template.format(rank=_rank())
        )
        payload = strict_json_load_path(config_path)
    if not isinstance(payload, dict) or not isinstance(payload.get("target"), dict):
        raise SystemExit("synthetic target config is invalid")
    target = payload["target"]
    host = target.get("host")
    port = target.get("port")
    if not isinstance(host, str) or not host:
        raise SystemExit("synthetic target host must be non-empty text")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise SystemExit("synthetic target port must be in 1..65535")
    binary = os.path.abspath(args.binary)
    if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
        raise SystemExit(f"synthetic target binary is not executable: {binary}")

    from exaserve.state.ports import bind_listener

    listener = bind_listener(port, host=host)
    env = dict(os.environ)
    env["PYTHONNOUSERSITE"] = "1"
    env["CLIENTLAB_LISTEN_FD"] = str(listener.fileno())
    # exec preserves the process-group identity owned by ManagedComponent and
    # the explicitly inheritable listening descriptor.
    if config_path is None:
        child_argv = [
            binary,
            "--config-json",
            json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False),
        ]
    else:
        child_argv = [binary, "--config", os.path.realpath(config_path)]
    os.execve(binary, child_argv, env)
    return 127


if __name__ == "__main__":
    raise SystemExit(main())
