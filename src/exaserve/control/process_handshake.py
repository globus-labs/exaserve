"""Typed, bounded readiness handshake for finite helper processes.

This is deliberately not a deployment/readiness authority. It replaces a
fragile convention where Python blocked on the first stdout line of the Go
load generator and compared it with ``GO_CLI_READY``. Stdout is diagnostics;
an atomic, nonce-bound file proves only that this exact finite child finished
initialization and is ready for its start command.
"""

from __future__ import annotations

import json
import os
import secrets
import stat
import time
from pathlib import Path
from typing import Any

from ..state.atomic import regular_file_reader, strict_json_load


HANDSHAKE_SCHEMA_VERSION = 1


def prepare_ready_handshake(directory: str | os.PathLike, name: str) -> tuple[str, str]:
    if not isinstance(name, str) or not name or "/" in name or "\x00" in name:
        raise ValueError("handshake name must be one non-empty path component")
    from ..state.atomic import ensure_owned_directory

    root = Path(ensure_owned_directory(directory))
    token = secrets.token_hex(16)
    path = root / f".{name}.{token}.ready.json"
    return str(path), token


def ready_handshake_args(path: str, token: str) -> list[str]:
    return ["--ready-file", path, "--ready-token", token]


def wait_ready_handshake(
    process: Any,
    *,
    path: str,
    token: str,
    timeout_s: float = 30.0,
    poll_s: float = 0.02,
) -> dict:
    """Wait for one exact child PID/token record or fail with its exit code."""
    if timeout_s <= 0 or poll_s <= 0:
        raise ValueError("handshake timeout and poll interval must be positive")
    expected_pid = getattr(process, "pid", None)
    if type(expected_pid) is not int or expected_pid <= 0:
        raise ValueError("handshake requires a started child process with a PID")
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        returncode = process.poll()
        if returncode is not None:
            raise RuntimeError(
                f"helper process {expected_pid} exited {returncode} before readiness handshake"
            )
        try:
            initial = os.lstat(path)
        except FileNotFoundError:
            time.sleep(poll_s)
            continue
        except OSError as exc:
            raise RuntimeError(f"could not inspect helper readiness handshake: {exc}") from exc
        if not stat.S_ISREG(initial.st_mode):
            raise RuntimeError("helper readiness handshake must be a regular non-symlink file")
        try:
            with regular_file_reader(path) as handle:
                opened = os.fstat(handle.fileno())
                if (opened.st_dev, opened.st_ino) != (initial.st_dev, initial.st_ino):
                    raise RuntimeError("helper readiness handshake changed before it was read")
                payload = strict_json_load(handle)
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise RuntimeError(f"helper readiness handshake is invalid: {exc}") from exc
        if set(payload) != {"schema_version", "pid", "token"}:
            raise RuntimeError("helper readiness handshake has an invalid shape")
        if (
            type(payload["schema_version"]) is not int
            or payload["schema_version"] != HANDSHAKE_SCHEMA_VERSION
            or type(payload["pid"]) is not int
            or payload["pid"] != expected_pid
            or not isinstance(payload["token"], str)
            or payload["token"] != token
        ):
            raise RuntimeError("helper readiness handshake identity mismatch")
        try:
            current = os.lstat(path)
            if (current.st_dev, current.st_ino) != (opened.st_dev, opened.st_ino):
                raise RuntimeError("helper readiness handshake changed while it was consumed")
            os.unlink(path)
        except OSError as exc:
            raise RuntimeError(f"could not consume helper readiness handshake: {exc}") from exc
        return payload
    raise TimeoutError(
        f"helper process {expected_pid} did not publish readiness within {timeout_s:g}s"
    )
