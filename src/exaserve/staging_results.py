"""Structured, attempt-scoped result artifacts for finite MPI staging jobs.

MPI stdout/stderr is diagnostic output only.  Each rank atomically publishes a
small JSON result into a freshly created shared directory; the owning parent
then validates the complete set against its immutable allocation binding.
"""

from __future__ import annotations

import os
import re
import stat
import uuid
from pathlib import Path

from .state.atomic import atomic_create_json, strict_json_load_path

_RESULT_NAME = re.compile(r"rank-(\d{8})\.pid-(\d+)\.([0-9a-f]{32})\.json")
_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}(?:-[A-Za-z0-9_.-]+)?")
_RESERVED_FIELDS = {"schema_version", "attempt_id", "result_id"}


def _validate_result_directory(directory: Path) -> None:
    try:
        metadata = os.lstat(directory)
    except OSError as exc:
        raise RuntimeError(f"staging result directory is unavailable: {directory}: {exc}") from exc
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise RuntimeError(
            f"staging result directory must be a user-owned real directory: {directory}"
        )


def create_result_dir(root: Path, category: str, attempt_id: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", category):
        raise ValueError(f"unsafe staging result category {category!r}")
    if not isinstance(attempt_id, str) or not _ATTEMPT_ID.fullmatch(attempt_id):
        raise ValueError(f"invalid staging attempt identity {attempt_id!r}")
    directory = root.resolve() / "staging_rank_results" / f"{category}.{attempt_id}"
    directory.mkdir(mode=0o700, parents=True, exist_ok=False)
    return directory


def write_rank_result(
    result_dir: str | os.PathLike,
    *,
    attempt_id: str,
    payload: dict,
) -> Path:
    """Atomically publish one uniquely named rank result."""
    directory = Path(result_dir)
    _validate_result_directory(directory)
    if not isinstance(attempt_id, str) or not _ATTEMPT_ID.fullmatch(attempt_id):
        raise ValueError(f"invalid staging attempt identity {attempt_id!r}")
    if not isinstance(payload, dict):
        raise TypeError("staging rank payload must be a mapping")
    reserved = sorted(set(payload) & _RESERVED_FIELDS)
    if reserved:
        raise RuntimeError(f"staging rank payload overrides reserved fields: {reserved}")
    rank = payload.get("rank")
    if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
        raise RuntimeError(f"staging rank result has invalid rank {rank!r}")
    result_id = uuid.uuid4().hex
    result = {
        "schema_version": 1,
        "attempt_id": attempt_id,
        "result_id": result_id,
        **payload,
    }
    path = directory / f"rank-{rank:08d}.pid-{os.getpid()}.{result_id}.json"
    atomic_create_json(path, result)
    return path


def load_rank_results(result_dir: str | os.PathLike, *, attempt_id: str) -> list[dict]:
    """Load one complete command's results, rejecting debris and stale identity."""
    directory = Path(result_dir)
    if not isinstance(attempt_id, str) or not _ATTEMPT_ID.fullmatch(attempt_id):
        raise ValueError(f"invalid staging attempt identity {attempt_id!r}")
    _validate_result_directory(directory)
    try:
        entries = sorted(directory.iterdir(), key=lambda item: item.name)
    except OSError as exc:
        raise RuntimeError(f"could not inspect staging results at {directory}: {exc}") from exc
    results: list[dict] = []
    for path in entries:
        match = _RESULT_NAME.fullmatch(path.name)
        if match is None:
            raise RuntimeError(f"unexpected staging result entry: {path}")
        try:
            payload = strict_json_load_path(path)
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"unreadable staging result {path}: {exc}") from exc
        if not isinstance(payload, dict):
            raise RuntimeError(f"staging result {path} is not an object")
        filename_rank = int(match.group(1))
        if (
            type(payload.get("schema_version")) is not int
            or payload.get("schema_version") != 1
            or payload.get("attempt_id") != attempt_id
            or type(payload.get("rank")) is not int
            or payload.get("rank") != filename_rank
            or payload.get("result_id") != match.group(3)
        ):
            raise RuntimeError(f"staging result {path} has stale or inconsistent identity")
        results.append(payload)
    return results
