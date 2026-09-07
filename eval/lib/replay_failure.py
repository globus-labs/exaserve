"""Strict, content-bound diagnostics for a replay process exit."""

from __future__ import annotations

import os
from typing import Any

from exaserve.state.results import ResultEntry, ResultManifestError


SCHEMA_VERSION = 1
COMMAND_ID = "mpi_replay_client"
REASON_CODE = "REPLAY_PROCESS_EXITED"
DETAIL = "replay client process exited with nonzero status"
_DIAGNOSTIC_LOGICAL_ID = "replay_failure_log"
_ALLOWED_TOPOLOGY_ARMS = frozenset({"local", "mesh", "paired"})


def _expected_log_name(topology_arm: str | None) -> str:
    if topology_arm is None:
        return "replay.log"
    if topology_arm not in _ALLOWED_TOPOLOGY_ARMS:
        raise ValueError("replay failure topology_arm is invalid")
    return f"replay_{topology_arm}.log"


def _configured_arms(run_plan: Any) -> tuple[str, ...]:
    raw = getattr(run_plan.client, "dispatch_topologies", ()) or ()
    if not isinstance(raw, (list, tuple)) or any(
        not isinstance(item, str) or item not in _ALLOWED_TOPOLOGY_ARMS for item in raw
    ):
        raise ValueError("RunPlan replay topology arms are invalid")
    return tuple(raw)


def _validate_arm(run_plan: Any, topology_arm: str | None) -> None:
    configured = _configured_arms(run_plan)
    if configured:
        if topology_arm is None or topology_arm not in configured:
            raise ValueError("replay failure topology_arm disagrees with RunPlan")
    elif topology_arm is not None:
        raise ValueError("replay failure unexpectedly names a topology arm")


def _bundle_log_entry(run_plan: Any, log_name: str) -> ResultEntry:
    root = os.path.abspath(run_plan.bundle.root_dir)
    logs_dir = os.path.abspath(run_plan.bundle.logs_dir)
    expected_logs_dir = os.path.join(root, "logs")
    if logs_dir != expected_logs_dir or os.path.islink(logs_dir):
        raise ResultManifestError("RunBundle logs directory is not the canonical logs directory")
    entry = ResultEntry.from_file(
        _DIAGNOSTIC_LOGICAL_ID,
        os.path.join(logs_dir, log_name),
        root=root,
    )
    expected_path = os.path.join("logs", log_name)
    if entry.path != expected_path:
        raise ResultManifestError("replay diagnostic path disagrees with RunBundle layout")
    return entry


def capture_replay_process_failure(
    run_plan: Any,
    *,
    exit_code: int,
    topology_arm: str | None,
    log_name: str,
) -> dict[str, Any]:
    """Capture the first replay exit without copying commands, environment, or log text."""
    if isinstance(exit_code, bool) or not isinstance(exit_code, int) or exit_code == 0:
        raise ValueError("replay failure exit_code must be a nonzero integer")
    _validate_arm(run_plan, topology_arm)
    expected_log_name = _expected_log_name(topology_arm)
    if log_name != expected_log_name:
        raise ValueError("replay failure log name disagrees with topology arm")
    entry = _bundle_log_entry(run_plan, log_name)
    failure: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "phase": "replay",
        "command_id": COMMAND_ID,
        "exit_code": exit_code,
        "diagnostic": {
            "path": entry.path,
            "size_bytes": entry.size_bytes,
            "sha256": entry.sha256,
        },
    }
    if topology_arm is not None:
        failure["topology_arm"] = topology_arm
    return failure


def validate_replay_process_failure(
    run_plan: Any,
    failure: Any,
    *,
    status_exit_code: int,
) -> None:
    """Validate descriptor shape, RunPlan coherence, containment, and current bytes."""
    required = {
        "schema_version",
        "phase",
        "command_id",
        "exit_code",
        "diagnostic",
    }
    optional = {"topology_arm"}
    if (
        not isinstance(failure, dict)
        or not required <= set(failure)
        or set(failure) - (required | optional)
    ):
        raise ValueError("replay failure descriptor shape mismatch")
    if type(failure["schema_version"]) is not int or failure["schema_version"] != SCHEMA_VERSION:
        raise ValueError("unsupported replay failure descriptor schema")
    if failure["phase"] != "replay" or failure["command_id"] != COMMAND_ID:
        raise ValueError("replay failure descriptor identity mismatch")
    exit_code = failure["exit_code"]
    if (
        isinstance(exit_code, bool)
        or not isinstance(exit_code, int)
        or exit_code == 0
        or exit_code != status_exit_code
    ):
        raise ValueError("replay failure exit_code disagrees with RunStatus")
    if "topology_arm" in failure and not isinstance(failure["topology_arm"], str):
        raise ValueError("replay failure topology_arm is invalid")
    topology_arm = failure.get("topology_arm")
    _validate_arm(run_plan, topology_arm)
    log_name = _expected_log_name(topology_arm)

    diagnostic = failure["diagnostic"]
    if not isinstance(diagnostic, dict) or set(diagnostic) != {
        "path",
        "size_bytes",
        "sha256",
    }:
        raise ValueError("replay failure diagnostic shape mismatch")
    try:
        declared = ResultEntry(logical_id=_DIAGNOSTIC_LOGICAL_ID, **diagnostic)
        observed = _bundle_log_entry(run_plan, log_name)
    except (TypeError, ResultManifestError) as exc:
        raise ValueError(f"replay failure diagnostic is invalid: {exc}") from exc
    if declared != observed:
        raise ValueError("replay failure diagnostic content identity mismatch")
