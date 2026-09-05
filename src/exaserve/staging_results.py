"""Bounded MPI result transport for finite staging collectives.

Workers must not publish rank-named files on the shared run filesystem. A
staging verifier therefore produces one small in-memory payload per MPI rank,
gathers those payloads to global rank zero, and emits exactly one attempt
envelope on rank zero's stdout. The owning head process validates that envelope
before it persists an aggregate result.

The enclosing :func:`~exaserve.control.finite_process.run_finite` deadline is
the failure bound for a lost rank. A rank that reaches this module always
participates in the gather, including when its local operation failed, so an
ordinary validation error does not strand peers in a collective.
"""

from __future__ import annotations

import contextlib
import hashlib
import io
import json
import re
import socket
import traceback
from collections.abc import Callable
from typing import Any

_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}(?:-[A-Za-z0-9_.-]+)?")
_RESULT_PREFIX = "EXASERVE_MPI_STAGE_RESULT="
_MAX_DIAGNOSTIC_BYTES = 16 * 1024


class StagingCollectiveError(RuntimeError):
    """A finite staging collective returned incomplete or failed evidence."""


def _same_node(left: str, right: str) -> bool:
    def canonical(value: str) -> str:
        return value.strip().lower().split(".", 1)[0]

    return bool(canonical(left)) and canonical(left) == canonical(right)


def _bounded(value: str) -> str:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= _MAX_DIAGNOSTIC_BYTES:
        return encoded.decode("utf-8", errors="replace")
    suffix = b"\n...[diagnostic truncated]"
    return (encoded[: _MAX_DIAGNOSTIC_BYTES - len(suffix)] + suffix).decode(
        "utf-8", errors="replace"
    )


def _result_id(attempt_id: str, rank: int, node: str, payload: object) -> str:
    canonical = json.dumps(
        [attempt_id, rank, node, payload],
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:32]


def run_collective_operation(
    operation: Callable[[], dict[str, Any]],
    *,
    attempt_id: str,
    expected_world_size: int,
    expected_root_node: str,
    mpi=None,
) -> bool:
    """Run one local operation per rank and gather its bounded result.

    Only global rank zero writes output. The returned boolean is broadcast to
    every participant so all ranks choose the same scheduler-visible exit
    status. ``mpi`` is injectable for unit tests; normal execution imports the
    site MPI runtime through mpi4py.
    """

    if not isinstance(attempt_id, str) or not _ATTEMPT_ID.fullmatch(attempt_id):
        raise ValueError(f"invalid staging attempt identity {attempt_id!r}")
    if type(expected_world_size) is not int or expected_world_size < 1:
        raise ValueError("expected_world_size must be a positive integer")
    if not isinstance(expected_root_node, str) or not expected_root_node.strip():
        raise ValueError("expected_root_node must be non-empty")
    if mpi is None:
        from mpi4py import MPI as mpi

    comm = mpi.COMM_WORLD
    rank = int(comm.Get_rank())
    world_size = int(comm.Get_size())
    node = str(mpi.Get_processor_name() or socket.gethostname())

    captured_out = io.StringIO()
    captured_err = io.StringIO()
    payload: dict[str, Any] | None = None
    error = ""
    try:
        with contextlib.redirect_stdout(captured_out), contextlib.redirect_stderr(captured_err):
            payload = operation()
        if not isinstance(payload, dict):
            raise TypeError("staging operation payload must be an object")
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except BaseException as exc:  # gather ordinary rank-local failure evidence
        payload = None
        error = _bounded("".join(traceback.format_exception_only(type(exc), exc)).strip())

    record = {
        "schema_version": 1,
        "attempt_id": attempt_id,
        "result_id": _result_id(attempt_id, rank, node, payload),
        "rank": rank,
        "node": node,
        "ok": not error,
        "payload": payload,
        "error": error,
        "stdout": _bounded(captured_out.getvalue()),
        "stderr": _bounded(captured_err.getvalue()),
    }
    records = comm.gather(record, root=0)

    aggregate_ok = True
    if rank == 0:
        assert records is not None
        ranks = [item.get("rank") for item in records]
        aggregate_ok = (
            world_size == expected_world_size
            and len(records) == expected_world_size
            and ranks == list(range(expected_world_size))
            and _same_node(node, expected_root_node)
            and all(item.get("ok") is True for item in records)
        )
        envelope = {
            "schema_version": 1,
            "attempt_id": attempt_id,
            "world_size": world_size,
            "root_node": node,
            "expected_root_node": expected_root_node,
            "ok": aggregate_ok,
            "results": records,
        }
        print(
            _RESULT_PREFIX
            + json.dumps(envelope, sort_keys=True, separators=(",", ":"), allow_nan=False),
            flush=True,
        )
    return bool(comm.bcast(aggregate_ok, root=0))


def load_collective_results(
    stdout: str,
    *,
    attempt_id: str,
    expected_world_size: int,
    expected_root_node: str,
) -> list[dict[str, Any]]:
    """Parse and validate the sole rank-zero aggregate from a finite command."""

    if not isinstance(stdout, str):
        raise TypeError("staging command stdout must be text")
    lines = [
        line[len(_RESULT_PREFIX) :]
        for line in stdout.splitlines()
        if line.startswith(_RESULT_PREFIX)
    ]
    if len(lines) != 1:
        raise StagingCollectiveError(
            f"expected one MPI staging result envelope, received {len(lines)}"
        )
    try:
        envelope = json.loads(lines[0])
    except (TypeError, json.JSONDecodeError) as exc:
        raise StagingCollectiveError(f"MPI staging result envelope is invalid JSON: {exc}") from exc
    expected_fields = {
        "schema_version",
        "attempt_id",
        "world_size",
        "root_node",
        "expected_root_node",
        "ok",
        "results",
    }
    if not isinstance(envelope, dict) or set(envelope) != expected_fields:
        raise StagingCollectiveError("MPI staging result envelope fields are invalid")
    if (
        type(envelope["schema_version"]) is not int
        or envelope["schema_version"] != 1
        or envelope["attempt_id"] != attempt_id
        or type(envelope["world_size"]) is not int
        or envelope["world_size"] != expected_world_size
        or envelope["expected_root_node"] != expected_root_node
        or not isinstance(envelope["root_node"], str)
        or not _same_node(envelope["root_node"], expected_root_node)
        or not isinstance(envelope["ok"], bool)
        or not isinstance(envelope["results"], list)
    ):
        raise StagingCollectiveError("MPI staging result envelope identity is invalid")
    records = envelope["results"]
    if len(records) != expected_world_size:
        raise StagingCollectiveError(
            f"MPI staging result is incomplete ({len(records)}/{expected_world_size} ranks)"
        )
    expected_record_fields = {
        "schema_version",
        "attempt_id",
        "result_id",
        "rank",
        "node",
        "ok",
        "payload",
        "error",
        "stdout",
        "stderr",
    }
    for rank, record in enumerate(records):
        if (
            not isinstance(record, dict)
            or set(record) != expected_record_fields
            or type(record["schema_version"]) is not int
            or record["schema_version"] != 1
            or record["attempt_id"] != attempt_id
            or type(record["rank"]) is not int
            or record["rank"] != rank
            or not isinstance(record["node"], str)
            or not record["node"]
            or not isinstance(record["result_id"], str)
            or not re.fullmatch(r"[0-9a-f]{32}", record["result_id"])
            or not isinstance(record["ok"], bool)
            or not isinstance(record["error"], str)
            or not isinstance(record["stdout"], str)
            or not isinstance(record["stderr"], str)
            or (record["ok"] and not isinstance(record["payload"], dict))
            or (not record["ok"] and record["payload"] is not None)
        ):
            raise StagingCollectiveError(f"MPI staging result rank {rank} is invalid")
        expected_id = _result_id(attempt_id, rank, record["node"], record["payload"])
        if record["result_id"] != expected_id:
            raise StagingCollectiveError(f"MPI staging result rank {rank} identity is invalid")
    failures = [
        f"rank {record['rank']} ({record['node']}): {record['error']}"
        for record in records
        if not record["ok"]
    ]
    if not envelope["ok"] or failures:
        detail = "; ".join(failures) or "rank-zero topology validation failed"
        raise StagingCollectiveError(f"MPI staging collective failed: {detail}")
    result_ids = [record["result_id"] for record in records]
    if len(result_ids) != len(set(result_ids)):
        raise StagingCollectiveError("MPI staging results contain duplicate identities")
    return [
        {
            "schema_version": record["schema_version"],
            "attempt_id": record["attempt_id"],
            "result_id": record["result_id"],
            **record["payload"],
        }
        for record in records
    ]


def result_line_count(stdout: str) -> int:
    """Count root aggregate protocol lines without parsing diagnostics."""

    return sum(line.startswith(_RESULT_PREFIX) for line in stdout.splitlines())
