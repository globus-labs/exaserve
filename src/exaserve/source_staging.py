"""Transactional distribution of the immutable ExaServe rank artifact.

The Python composition root owns this finite helper.  It retains the native
MPI broadcast implementation, but replaces the former 200-line shell
orchestrator and hand-built Ray overlay with one checked transaction:

1. assemble source, eval, tools, and canonical run artifacts in head-local
   storage;
2. compute a complete path/size/mode/content manifest;
3. use one allocation-wide native collective whose global rank zero is bound
   to allocation rank zero;
4. verify and atomically publish a content-addressed capsule on every node;
5. gather in-memory receipts through MPI and let only the head persist the
   aggregate result.

MPI exit zero is necessary but not sufficient.  Missing, duplicate, stale, or
wrong-node receipts fail the step, so ranks can never launch from a partially
published source tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import socket
import stat
import sys
import tempfile
import time
import uuid
from collections.abc import Iterable
from importlib import util as importlib_util
from importlib import resources
from pathlib import Path
from pathlib import PurePosixPath

from .exception_notes import add_exception_note
from .plan.runtime_environment import RuntimePaths


class SourceStagingError(RuntimeError):
    """The source artifact could not be published on every planned rank."""


_SOURCE_RECEIPT_FIELDS = {
    "schema_version",
    "attempt_id",
    "result_id",
    "rank",
    "node",
    "generation",
    "source_manifest_hash",
    "file_count",
    "total_bytes",
    "published_path",
    "published_target",
    "runtime_device_id",
    "state_device_id",
    "runtime_fs_type",
    "state_fs_type",
    "qualified_python_path",
    "qualified_python_sha256",
    "qualified_python_device_id",
    "qualified_python_fs_type",
    "compatibility_profile_id",
    "compatibility_manifest_hash",
    "verification_duration_s",
}
_SOURCE_RESULT_FIELDS = {
    "schema_version",
    "deployment_id",
    "generation",
    "deployment_plan_hash",
    "site_profile_hash",
    "allocation_binding_hash",
    "source_manifest_hash",
    "capsule_manifest_hash",
    "local_runtime_root",
    "local_python_root",
    "local_plan_path",
    "local_site_profile_path",
    "local_binding_path",
    "local_bcast_path",
    "local_go_dispatch",
    "local_eval_manifest",
    "local_run_plan",
    "local_state_root",
    "qualified_python",
    "qualified_python_sha256",
    "compatibility_profile_id",
    "compatibility_manifest_hash",
    "file_count",
    "total_bytes",
    "files",
    "duration_s",
    "rank_receipts",
}
_SOURCE_FILE_FIELDS = {"path", "size", "mode", "sha256"}
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}")
_RESULT_ID = re.compile(r"[0-9a-f]{32}")
_SHARED_FS_TYPES = frozenset({"lustre", "nfs", "nfs4", "gpfs", "beegfs", "cifs"})

# This bootstrap is delivered as Python ``-c`` argv bytes. It imports no
# candidate code until stdlib-only verification has independently reproduced
# the complete content/mode manifest and local-filesystem containment proof.
# The subsequently imported candidate repeats verification before publication.
_QUALIFIED_VERIFIER_BOOTSTRAP = r"""
import hashlib
import json
import os
from pathlib import Path
import stat
import sys

candidate = Path(sys.argv[1])
expected_hash = sys.argv[2]
expected_files = int(sys.argv[3])
expected_bytes = int(sys.argv[4])
local_base = Path(sys.argv[5])
remaining = sys.argv[6:]

if not candidate.is_absolute() or not local_base.is_absolute():
    raise SystemExit("qualified verifier requires absolute local paths")
try:
    base_meta = os.lstat(local_base)
    candidate_meta = os.lstat(candidate)
except OSError as exc:
    raise SystemExit("qualified verifier input is unavailable: %s" % (exc,))
if not stat.S_ISDIR(base_meta.st_mode) or stat.S_ISLNK(base_meta.st_mode):
    raise SystemExit("qualified verifier local base is not a real directory")
if not stat.S_ISDIR(candidate_meta.st_mode) or stat.S_ISLNK(candidate_meta.st_mode):
    raise SystemExit("qualified verifier candidate is not a real directory")
base = Path(os.path.realpath(local_base))
resolved_candidate = Path(os.path.realpath(candidate))
try:
    resolved_candidate.relative_to(base)
except ValueError:
    raise SystemExit("qualified verifier candidate escapes the local base")
open_flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
directory_flags = open_flags | os.O_DIRECTORY
try:
    candidate_fd = os.open(candidate, directory_flags)
except OSError as exc:
    raise SystemExit("qualified verifier cannot open candidate safely: %s" % (exc,))
opened_candidate = os.fstat(candidate_fd)
if (opened_candidate.st_dev != candidate_meta.st_dev
        or opened_candidate.st_ino != candidate_meta.st_ino):
    os.close(candidate_fd)
    raise SystemExit("qualified verifier candidate changed before open")
if stat.S_IMODE(opened_candidate.st_mode) & 0o222:
    os.close(candidate_fd)
    raise SystemExit("qualified verifier candidate root is writable")

def mountinfo_unescape(value):
    return (value.replace("\\040", " ").replace("\\011", "\t")
            .replace("\\012", "\n").replace("\\134", "\\"))

try:
    with open("/proc/self/mountinfo", encoding="utf-8") as mountinfo:
        mount_points = []
        for line in mountinfo:
            fields = line.split()
            if len(fields) >= 6 and "-" in fields:
                value = Path(mountinfo_unescape(fields[4]))
                if value.is_absolute():
                    mount_points.append(value)
except OSError as exc:
    os.close(candidate_fd)
    raise SystemExit("qualified verifier cannot inspect mount boundaries: %s" % (exc,))
if not any(base == mount or mount in base.parents for mount in mount_points):
    os.close(candidate_fd)
    raise SystemExit("qualified verifier local base has no covering kernel mount")
nested_mounts = frozenset(
    mount for mount in mount_points
    if mount != base and base in mount.parents
)
if any(resolved_candidate == mount or mount in resolved_candidate.parents
       for mount in nested_mounts):
    os.close(candidate_fd)
    raise SystemExit("qualified verifier candidate is a nested mount")
files = []
total = 0

def walk(directory_fd, prefix):
    global total
    for name in sorted(os.listdir(directory_fd)):
        if not name or name in (".", "..") or "/" in name:
            raise SystemExit("qualified verifier encountered an unsafe name")
        relative = name if not prefix else prefix + "/" + name
        metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
        if stat.S_ISLNK(metadata.st_mode):
            raise SystemExit("qualified verifier rejects candidate symlink: %s" % (relative,))
        if resolved_candidate / relative in nested_mounts:
            raise SystemExit("qualified verifier rejects filesystem escape: %s" % (relative,))
        mode = stat.S_IMODE(metadata.st_mode)
        if mode & 0o222:
            raise SystemExit("qualified verifier rejects writable runtime entry: %s" % (relative,))
        if stat.S_ISDIR(metadata.st_mode):
            child_fd = os.open(name, directory_flags, dir_fd=directory_fd)
            opened = os.fstat(child_fd)
            if (opened.st_dev != metadata.st_dev or opened.st_ino != metadata.st_ino
                    or not stat.S_ISDIR(opened.st_mode)):
                os.close(child_fd)
                raise SystemExit("qualified verifier directory changed before open")
            try:
                walk(child_fd, relative)
            finally:
                os.close(child_fd)
            continue
        if not stat.S_ISREG(metadata.st_mode):
            raise SystemExit("qualified verifier rejects special entry: %s" % (relative,))
        descriptor = os.open(name, open_flags, dir_fd=directory_fd)
        opened = os.fstat(descriptor)
        if (opened.st_dev != metadata.st_dev or opened.st_ino != metadata.st_ino
                or opened.st_size != metadata.st_size or not stat.S_ISREG(opened.st_mode)):
            os.close(descriptor)
            raise SystemExit("qualified verifier file changed before open")
        digest = hashlib.sha256()
        size = 0
        try:
            while True:
                chunk = os.read(descriptor, 1 << 20)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
        finally:
            os.close(descriptor)
        if size != metadata.st_size:
            raise SystemExit("qualified verifier file changed while reading")
        total += size
        files.append({"path": relative, "size": size, "mode": mode,
                      "sha256": digest.hexdigest()})

walk(candidate_fd, "")
files.sort(key=lambda item: item["path"])
canonical = json.dumps(files, sort_keys=True, separators=(",", ":"))
observed_hash = hashlib.sha256(canonical.encode()).hexdigest()
if (observed_hash != expected_hash or len(files) != expected_files
        or total != expected_bytes):
    raise SystemExit("qualified verifier candidate manifest mismatch")
if remaining == ["--preverify-only"]:
    os.close(candidate_fd)
    raise SystemExit(0)
sys.path.insert(0, "/proc/self/fd/%d/python" % (candidate_fd,))
from exaserve.source_staging import main
try:
    status = main(remaining)
finally:
    os.close(candidate_fd)
raise SystemExit(status)
""".strip()


def runtime_paths(
    root: str | os.PathLike,
    *,
    deployment_id: str,
    generation: int,
    qualified_python: str,
) -> RuntimePaths:
    from types import SimpleNamespace

    from .plan.runtime_environment import default_local_state_root

    if not isinstance(deployment_id, str) or not deployment_id:
        raise SourceStagingError("runtime deployment identity must be non-empty")
    if type(generation) is not int or generation < 0:
        raise SourceStagingError("runtime generation must be a non-negative integer")
    root_path = Path(root)
    if not root_path.is_absolute() or ".." in root_path.parts or len(root_path.parents) < 3:
        raise SourceStagingError("local runtime root must be absolute and normalized")
    python = Path(qualified_python)
    if not python.is_absolute() or ".." in python.parts:
        raise SourceStagingError("qualified Python must be absolute and normalized")
    state = default_local_state_root(SimpleNamespace(deployment_id=deployment_id), generation)
    return RuntimePaths.from_roots(root_path, state, require_runtime=False)


def runtime_paths_from_result(result: object) -> RuntimePaths:
    """Recover the exact runtime layout without guessing a worker path."""

    validated = validate_source_staging_result(result)
    paths = runtime_paths(
        validated["local_runtime_root"],
        deployment_id=validated["deployment_id"],
        generation=validated["generation"],
        qualified_python=validated["qualified_python"],
    )
    expected = {
        "local_python_root": paths.python_root,
        "local_plan_path": paths.plan_path,
        "local_site_profile_path": paths.site_profile_path,
        "local_binding_path": paths.binding_path,
        "local_bcast_path": paths.bcast_path,
        "local_state_root": paths.state_root,
    }
    if validated["local_go_dispatch"] is not None:
        expected["local_go_dispatch"] = paths.go_dispatch_path
    if validated["local_eval_manifest"] is not None:
        expected["local_eval_manifest"] = paths.eval_manifest_path
    if validated["local_run_plan"] is not None:
        expected["local_run_plan"] = paths.run_plan_path
    mismatches = {
        name: (str(path), validated[name])
        for name, path in expected.items()
        if validated[name] != str(path)
    }
    if mismatches:
        raise SourceStagingError(f"source staging runtime layout is inconsistent: {mismatches}")
    return paths


def _validate_source_receipt(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != _SOURCE_RECEIPT_FIELDS:
        raise SourceStagingError("source receipt fields are invalid")
    duration = value["verification_duration_s"]
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or not isinstance(value["attempt_id"], str)
        or not _ATTEMPT_ID.fullmatch(value["attempt_id"])
        or not isinstance(value["result_id"], str)
        or not _RESULT_ID.fullmatch(value["result_id"])
        or type(value["rank"]) is not int
        or value["rank"] < 0
        or not isinstance(value["node"], str)
        or not value["node"]
        or type(value["generation"]) is not int
        or value["generation"] < 0
        or not isinstance(value["source_manifest_hash"], str)
        or not _SHA256.fullmatch(value["source_manifest_hash"])
        or type(value["file_count"]) is not int
        or value["file_count"] < 0
        or type(value["total_bytes"]) is not int
        or value["total_bytes"] < 0
        or not isinstance(value["published_path"], str)
        or not os.path.isabs(value["published_path"])
        or not isinstance(value["published_target"], str)
        or not os.path.isabs(value["published_target"])
        or type(value["runtime_device_id"]) is not int
        or value["runtime_device_id"] < 0
        or type(value["state_device_id"]) is not int
        or value["state_device_id"] < 0
        or not isinstance(value["runtime_fs_type"], str)
        or not value["runtime_fs_type"]
        or not isinstance(value["state_fs_type"], str)
        or not value["state_fs_type"]
        or not isinstance(value["qualified_python_path"], str)
        or not os.path.isabs(value["qualified_python_path"])
        or not isinstance(value["qualified_python_sha256"], str)
        or not _SHA256.fullmatch(value["qualified_python_sha256"])
        or type(value["qualified_python_device_id"]) is not int
        or value["qualified_python_device_id"] < 0
        or not isinstance(value["qualified_python_fs_type"], str)
        or not value["qualified_python_fs_type"]
        or not isinstance(value["compatibility_profile_id"], str)
        or not _SHA256.fullmatch(value["compatibility_profile_id"])
        or not isinstance(value["compatibility_manifest_hash"], str)
        or not _SHA256.fullmatch(value["compatibility_manifest_hash"])
        or isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
    ):
        raise SourceStagingError("source receipt values are invalid")
    return value


def _validate_source_files(value: object) -> tuple[list[dict], dict]:
    if not isinstance(value, list):
        raise SourceStagingError("source result files must be an array")
    files: list[dict] = []
    paths: list[str] = []
    total_bytes = 0
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != _SOURCE_FILE_FIELDS:
            raise SourceStagingError(f"source result file {index} fields are invalid")
        path = item["path"]
        if not isinstance(path, str) or not path or "\\" in path:
            raise SourceStagingError(f"source result file {index} path is invalid")
        parsed = PurePosixPath(path)
        if (
            parsed.is_absolute()
            or parsed.as_posix() != path
            or any(part in {"", ".", ".."} for part in parsed.parts)
        ):
            raise SourceStagingError(f"source result file {index} path is unsafe")
        if (
            type(item["size"]) is not int
            or item["size"] < 0
            or type(item["mode"]) is not int
            or not 0 <= item["mode"] <= 0o7777
            or not isinstance(item["sha256"], str)
            or not _SHA256.fullmatch(item["sha256"])
        ):
            raise SourceStagingError(f"source result file {index} values are invalid")
        paths.append(path)
        total_bytes += item["size"]
        files.append(item)
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise SourceStagingError("source result file paths must be unique and sorted")
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"))
    inventory = {
        "source_manifest_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        "file_count": len(files),
        "total_bytes": total_bytes,
    }
    return files, inventory


def validate_source_staging_result(
    value: object,
    *,
    expected_deployment_id: str | None = None,
    expected_generation: int | None = None,
    expected_plan_hash: str | None = None,
    expected_site_profile_hash: str | None = None,
    expected_binding_hash: str | None = None,
    expected_rank_to_node: Iterable[tuple[int, str]] | None = None,
    expected_run_dir: str | os.PathLike | None = None,
) -> dict:
    """Validate the complete immutable source-staging evidence boundary."""
    if not isinstance(value, dict) or set(value) != _SOURCE_RESULT_FIELDS:
        raise SourceStagingError("source staging result fields are invalid")
    duration = value["duration_s"]
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 2
        or not isinstance(value["deployment_id"], str)
        or not value["deployment_id"]
        or type(value["generation"]) is not int
        or value["generation"] < 0
        or any(
            not isinstance(value[name], str) or not _SHA256.fullmatch(value[name])
            for name in (
                "deployment_plan_hash",
                "site_profile_hash",
                "allocation_binding_hash",
                "source_manifest_hash",
            )
        )
        or not isinstance(value["qualified_python_sha256"], str)
        or not _SHA256.fullmatch(value["qualified_python_sha256"])
        or not isinstance(value["compatibility_profile_id"], str)
        or not _SHA256.fullmatch(value["compatibility_profile_id"])
        or not isinstance(value["compatibility_manifest_hash"], str)
        or not _SHA256.fullmatch(value["compatibility_manifest_hash"])
        or type(value["file_count"]) is not int
        or value["file_count"] < 0
        or type(value["total_bytes"]) is not int
        or value["total_bytes"] < 0
        or isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
        or not isinstance(value["capsule_manifest_hash"], str)
        or not _SHA256.fullmatch(value["capsule_manifest_hash"])
        or value["capsule_manifest_hash"] != value["source_manifest_hash"]
        or not isinstance(value["local_runtime_root"], str)
        or not os.path.isabs(value["local_runtime_root"])
        or any(
            not isinstance(value[name], str) or not os.path.isabs(value[name])
            for name in (
                "local_python_root",
                "local_plan_path",
                "local_site_profile_path",
                "local_binding_path",
                "local_bcast_path",
                "local_state_root",
                "qualified_python",
            )
        )
        or any(
            value[name] is not None
            and (not isinstance(value[name], str) or not os.path.isabs(value[name]))
            for name in ("local_go_dispatch", "local_eval_manifest", "local_run_plan")
        )
        or not isinstance(value["rank_receipts"], list)
        or not value["rank_receipts"]
    ):
        raise SourceStagingError("source staging result values are invalid")

    _, inventory = _validate_source_files(value["files"])
    if any(value[name] != inventory[name] for name in inventory):
        raise SourceStagingError("source staging result inventory hash/count/bytes mismatch")

    paths = runtime_paths(
        value["local_runtime_root"],
        deployment_id=value["deployment_id"],
        generation=value["generation"],
        qualified_python=value["qualified_python"],
    )
    exact_paths = {
        "local_python_root": paths.python_root,
        "local_plan_path": paths.plan_path,
        "local_site_profile_path": paths.site_profile_path,
        "local_binding_path": paths.binding_path,
        "local_bcast_path": paths.bcast_path,
        "local_state_root": paths.state_root,
    }
    if any(value[name] != str(path) for name, path in exact_paths.items()):
        raise SourceStagingError("source staging result runtime layout is inconsistent")
    optional_paths = {
        "local_go_dispatch": paths.go_dispatch_path,
        "local_eval_manifest": paths.eval_manifest_path,
        "local_run_plan": paths.run_plan_path,
    }
    if any(
        value[name] is not None and value[name] != str(path)
        for name, path in optional_paths.items()
    ):
        raise SourceStagingError("source staging optional runtime layout is inconsistent")
    root = Path(value["local_runtime_root"])
    if root.name != value["capsule_manifest_hash"] or root.parent.name != f"g{value['generation']}":
        raise SourceStagingError("local runtime root is not content/generation addressed")

    expected_identity = {
        "deployment_id": expected_deployment_id,
        "generation": expected_generation,
        "deployment_plan_hash": expected_plan_hash,
        "site_profile_hash": expected_site_profile_hash,
        "allocation_binding_hash": expected_binding_hash,
    }
    mismatches = {
        name: (expected, value[name])
        for name, expected in expected_identity.items()
        if expected is not None and value[name] != expected
    }
    if mismatches:
        raise SourceStagingError(f"source staging result identity mismatch: {mismatches}")

    receipts = [_validate_source_receipt(item) for item in value["rank_receipts"]]
    ranks = [item["rank"] for item in receipts]
    if ranks != list(range(len(receipts))):
        raise SourceStagingError("source receipts must be one ordered, contiguous rank set")
    attempt_ids = {item["attempt_id"] for item in receipts}
    result_ids = {item["result_id"] for item in receipts}
    if len(attempt_ids) != 1 or len(result_ids) != len(receipts):
        raise SourceStagingError("source receipts have inconsistent attempt/result identity")
    attempt_id = next(iter(attempt_ids))
    expected_target = value["local_runtime_root"]
    for receipt in receipts:
        if (
            receipt["generation"] != value["generation"]
            or receipt["source_manifest_hash"] != value["source_manifest_hash"]
            or receipt["file_count"] != value["file_count"]
            or receipt["total_bytes"] != value["total_bytes"]
            or receipt["published_path"] != expected_target
            or receipt["published_target"] != expected_target
            or receipt["runtime_device_id"] != receipt["state_device_id"]
            or receipt["runtime_fs_type"] != receipt["state_fs_type"]
            or receipt["runtime_fs_type"].lower() in _SHARED_FS_TYPES
            or receipt["qualified_python_path"] != value["qualified_python"]
            or receipt["qualified_python_sha256"] != value["qualified_python_sha256"]
            or receipt["qualified_python_fs_type"].lower() in _SHARED_FS_TYPES
            or receipt["compatibility_profile_id"] != value["compatibility_profile_id"]
            or receipt["compatibility_manifest_hash"] != value["compatibility_manifest_hash"]
        ):
            raise SourceStagingError(f"source receipt rank {receipt['rank']} disagrees with result")

    # ``expected_run_dir`` remains in the public validator signature for the
    # composition boundary, but workers no longer create anything beneath it.
    del expected_run_dir

    if expected_rank_to_node is not None:
        from .plan.contracts import same_node

        planned = tuple(expected_rank_to_node)
        if len(planned) != len(receipts) or [rank for rank, _ in planned] != ranks:
            raise SourceStagingError("source receipt rank set disagrees with allocation binding")
        for receipt, (_, node) in zip(receipts, planned):
            if not isinstance(node, str) or not node or not same_node(receipt["node"], node):
                raise SourceStagingError(
                    f"source receipt rank {receipt['rank']} came from the wrong node"
                )
    return value


def _rank() -> int:
    for name in ("PALS_RANKID", "PMI_RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK", "PMIX_RANK"):
        value = os.environ.get(name)
        if value is not None:
            try:
                rank = int(value)
            except ValueError:
                raise SourceStagingError(f"{name} is not an integer: {value!r}")
            if rank < 0:
                raise SourceStagingError(f"{name} must be non-negative: {value!r}")
            return rank
    raise SourceStagingError("source verifier has no MPI/srun rank identity")


def tree_manifest(root: Path) -> dict:
    """Return a descriptor-bound deterministic file manifest for ``root``."""

    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    directory_flags = flags | getattr(os, "O_DIRECTORY", 0)
    try:
        lexical = os.lstat(root)
        root_fd = os.open(root, directory_flags)
    except OSError as exc:
        raise SourceStagingError(f"source artifact is unavailable: {root}: {exc}") from exc
    opened_root = os.fstat(root_fd)
    if (
        not stat.S_ISDIR(opened_root.st_mode)
        or lexical.st_dev != opened_root.st_dev
        or lexical.st_ino != opened_root.st_ino
    ):
        os.close(root_fd)
        raise SourceStagingError(f"source artifact is not a stable real directory: {root}")
    from .state.mounts import MountBoundaryError, nested_mount_points

    resolved_root = Path(os.path.realpath(root))
    try:
        nested_mounts = nested_mount_points(resolved_root)
    except MountBoundaryError as exc:
        os.close(root_fd)
        raise SourceStagingError(str(exc)) from exc
    files: list[dict] = []
    total_bytes = 0

    def walk(directory_fd: int, prefix: str) -> None:
        nonlocal total_bytes
        try:
            names = sorted(os.listdir(directory_fd))
        except OSError as exc:
            raise SourceStagingError(f"source artifact directory is unreadable: {prefix}: {exc}")
        for name in names:
            if not name or name in {".", ".."} or "/" in name:
                raise SourceStagingError("source artifact contains an unsafe entry name")
            relative = name if not prefix else f"{prefix}/{name}"
            metadata = os.stat(name, dir_fd=directory_fd, follow_symlinks=False)
            if stat.S_ISLNK(metadata.st_mode):
                raise SourceStagingError(
                    f"source artifact contains an unresolved symlink: {relative}"
                )
            if resolved_root / relative in nested_mounts:
                raise SourceStagingError(f"source artifact crosses a filesystem: {relative}")
            if stat.S_ISDIR(metadata.st_mode):
                child_fd = os.open(name, directory_flags, dir_fd=directory_fd)
                opened = os.fstat(child_fd)
                if (
                    not stat.S_ISDIR(opened.st_mode)
                    or opened.st_dev != metadata.st_dev
                    or opened.st_ino != metadata.st_ino
                ):
                    os.close(child_fd)
                    raise SourceStagingError(
                        f"source artifact directory changed before open: {relative}"
                    )
                try:
                    walk(child_fd, relative)
                finally:
                    os.close(child_fd)
                continue
            if not stat.S_ISREG(metadata.st_mode):
                raise SourceStagingError(f"source artifact contains unsupported entry: {relative}")
            descriptor = os.open(name, flags, dir_fd=directory_fd)
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or opened.st_dev != metadata.st_dev
                or opened.st_ino != metadata.st_ino
                or opened.st_size != metadata.st_size
            ):
                os.close(descriptor)
                raise SourceStagingError(f"source artifact file changed before open: {relative}")
            digest = hashlib.sha256()
            size = 0
            try:
                while True:
                    chunk = os.read(descriptor, 1 << 20)
                    if not chunk:
                        break
                    digest.update(chunk)
                    size += len(chunk)
                final = os.fstat(descriptor)
            finally:
                os.close(descriptor)
            if (
                size != metadata.st_size
                or final.st_size != metadata.st_size
                or final.st_mtime_ns != metadata.st_mtime_ns
                or final.st_ctime_ns != metadata.st_ctime_ns
            ):
                raise SourceStagingError(f"source artifact file changed while reading: {relative}")
            total_bytes += size
            files.append(
                {
                    "path": relative,
                    "size": size,
                    "mode": stat.S_IMODE(opened.st_mode),
                    "sha256": digest.hexdigest(),
                }
            )

    try:
        walk(root_fd, "")
    finally:
        os.close(root_fd)
    files.sort(key=lambda item: item["path"])
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"))
    return {
        "source_manifest_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "files": files,
    }


def _ignore(directory: str, names: list[str]) -> set[str]:
    del directory
    return {
        name
        for name in names
        if name in {"__pycache__", ".git", ".pytest_cache"} or name.endswith((".pyc", ".pyo"))
    }


def _publish(candidate_root: Path, stable: Path, *, local_base: Path) -> None:
    """Atomically publish one verified immutable runtime capsule."""
    from .state.atomic import fsync_directory
    from .model_staging import ensure_node_local_directory, validate_node_local_tree

    try:
        ensure_node_local_directory(stable.parent)
    except (RuntimeError, ValueError) as exc:
        raise SourceStagingError(f"runtime publication parent is unsafe: {exc}") from exc
    quarantine = None
    stable_is_real_directory = False
    if os.path.lexists(stable):
        stable_metadata = os.lstat(stable)
        stable_is_real_directory = stat.S_ISDIR(stable_metadata.st_mode) and not stat.S_ISLNK(
            stable_metadata.st_mode
        )
        try:
            observed = tree_manifest(stable)
            validate_node_local_tree(stable, local_root=local_base)
            _validate_capsule_immutable(stable)
        except (OSError, ValueError, SourceStagingError):
            observed = None
        expected = tree_manifest(candidate_root)
        if observed == expected:
            _thaw_private_input(candidate_root)
            shutil.rmtree(candidate_root)
            return
        quarantine = stable.with_name(f".{stable.name}.invalid.{os.getpid()}.{time.time_ns()}")
        if stable_is_real_directory:
            stable.chmod(0o755)
        os.rename(stable, quarantine)
        if stable_is_real_directory:
            quarantine.chmod(0o555)
    try:
        # Some filesystems require owner-write on the moved directory so its
        # ``..`` entry can be updated. Payload files/subdirectories remain
        # read-only, and the published root is frozen before the directory
        # durability barrier or any child launch.
        candidate_root.chmod(0o755)
        os.replace(candidate_root, stable)
        stable.chmod(0o555)
        fsync_directory(stable.parent)
        if quarantine is not None:
            if stable_is_real_directory:
                _thaw_private_input(quarantine)
                shutil.rmtree(quarantine)
            else:
                quarantine.unlink()
    except BaseException as exc:
        try:
            if os.path.lexists(stable):
                stable.chmod(0o555)
            if quarantine is not None and not os.path.lexists(stable):
                if stable_is_real_directory:
                    quarantine.chmod(0o755)
                os.rename(quarantine, stable)
                if stable_is_real_directory:
                    stable.chmod(0o555)
                fsync_directory(stable.parent)
        except OSError as rollback_exc:
            add_exception_note(exc, f"source publication rollback also failed: {rollback_exc}")
        raise


def verify_and_publish(
    candidate_package: Path,
    expected_hash: str,
    expected_files: int,
    expected_bytes: int,
    generation: int,
    stable: Path,
    local_base: Path | None = None,
    state_root: Path | None = None,
    qualified_python: Path | None = None,
) -> dict:
    started = time.monotonic()
    from .model_staging import validate_node_local_root, validate_node_local_tree

    local_base = validate_node_local_root(local_base or stable.parents[2])
    validate_node_local_tree(candidate_package, local_root=local_base)
    _validate_capsule_immutable(candidate_package)
    observed = tree_manifest(candidate_package)
    if observed["source_manifest_hash"] != expected_hash:
        raise SourceStagingError(
            "node-local source manifest mismatch: "
            f"expected={expected_hash}, observed={observed['source_manifest_hash']}"
        )
    if observed["file_count"] != expected_files or observed["total_bytes"] != expected_bytes:
        raise SourceStagingError("node-local source inventory disagrees with declared count/bytes")
    _publish(candidate_package, stable, local_base=local_base)
    try:
        validate_node_local_tree(stable, local_root=local_base)
        _validate_capsule_immutable(stable)
    except (RuntimeError, ValueError) as exc:
        raise SourceStagingError(f"published runtime capsule is not node-local: {exc}") from exc
    if tree_manifest(stable) != observed:
        raise SourceStagingError(f"atomic source publication failed at {stable}")
    if state_root is not None:
        from .plan.runtime_environment import RuntimePaths

        local_paths = RuntimePaths.from_roots(stable, state_root, require_runtime=True)
        local_paths.prepare_state()
        try:
            validate_node_local_tree(state_root, local_root=local_base)
        except ValueError as exc:
            raise SourceStagingError(f"runtime state tree is not node-local: {exc}") from exc
    effective_state = state_root or stable.parent
    if qualified_python is None:
        raise SourceStagingError("qualified Python identity is required for source publication")
    from .plan.io import load_site_profile

    profile = load_site_profile(str(stable / "run" / "site.profile.json"))
    python_evidence = _qualified_executable_evidence(qualified_python, profile=profile)
    compatibility_evidence = _compatibility_evidence(stable)
    receipt = {
        "rank": _rank(),
        "node": socket.gethostname(),
        "generation": generation,
        "source_manifest_hash": expected_hash,
        "file_count": expected_files,
        "total_bytes": expected_bytes,
        "published_path": str(stable),
        "published_target": str(stable),
        "runtime_device_id": int(os.stat(stable, follow_symlinks=False).st_dev),
        "state_device_id": int(os.stat(effective_state, follow_symlinks=False).st_dev),
        "runtime_fs_type": _filesystem_type(stable),
        "state_fs_type": _filesystem_type(effective_state),
        **python_evidence,
        **compatibility_evidence,
        "verification_duration_s": round(time.monotonic() - started, 6),
    }
    _remove_source_candidate_parent(candidate_package, local_base=local_base, failed=False)
    return receipt


def _validate_capsule_immutable(root: Path) -> None:
    for path in [root, *root.rglob("*")]:
        metadata = os.lstat(path)
        if stat.S_IMODE(metadata.st_mode) & 0o222:
            raise SourceStagingError(f"runtime capsule entry is writable: {path}")


def _filesystem_type(path: Path) -> str:
    """Return the longest matching local mount's kernel filesystem type."""

    resolved = str(path.resolve(strict=True))
    matches: list[tuple[int, str]] = []
    try:
        with open("/proc/self/mountinfo", encoding="utf-8") as handle:  # noqa: PTH123
            for line in handle:
                left, separator, right = line.rstrip("\n").partition(" - ")
                if not separator:
                    continue
                fields = left.split()
                after = right.split()
                if len(fields) < 5 or not after:
                    continue
                mount = (
                    fields[4]
                    .replace("\\040", " ")
                    .replace("\\011", "\t")
                    .replace("\\012", "\n")
                    .replace("\\134", "\\")
                )
                if resolved == mount or resolved.startswith(mount.rstrip("/") + "/"):
                    matches.append((len(mount), after[0]))
    except OSError as exc:
        raise SourceStagingError(f"could not inspect runtime filesystem identity: {exc}") from exc
    if not matches:
        raise SourceStagingError(f"no mount identity covers node-local path: {resolved}")
    return max(matches)[1]


def _qualified_executable_evidence(path: Path, *, profile) -> dict:
    from .site import qualify_site_local_bootstrap

    try:
        resolved = Path(qualify_site_local_bootstrap(str(path), profile))
        lexical = os.lstat(resolved)
        descriptor = os.open(
            resolved,
            os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0),
        )
    except (OSError, RuntimeError) as exc:
        raise SourceStagingError(f"qualified Python is unavailable: {exc}") from exc
    try:
        opened = os.fstat(descriptor)
        if (
            not stat.S_ISREG(opened.st_mode)
            or opened.st_dev != lexical.st_dev
            or opened.st_ino != lexical.st_ino
            or not opened.st_mode & stat.S_IXUSR
        ):
            raise SourceStagingError("qualified Python changed before descriptor-bound hashing")
        digest = hashlib.sha256()
        while True:
            chunk = os.read(descriptor, 1 << 20)
            if not chunk:
                break
            digest.update(chunk)
        final = os.fstat(descriptor)
        if (
            final.st_size != opened.st_size
            or final.st_mtime_ns != opened.st_mtime_ns
            or final.st_ctime_ns != opened.st_ctime_ns
        ):
            raise SourceStagingError("qualified Python changed while hashing")
    finally:
        os.close(descriptor)
    return {
        "qualified_python_path": str(resolved),
        "qualified_python_sha256": digest.hexdigest(),
        "qualified_python_device_id": int(opened.st_dev),
        "qualified_python_fs_type": _filesystem_type(resolved),
    }


def _compatibility_evidence(runtime_root: Path) -> dict:
    import platform
    from importlib import metadata

    from .compat.producers import manifest_hash
    from .compat.profile import default_profile
    from .plan.io import load_deployment_plan

    plan = load_deployment_plan(str(runtime_root / "run" / "deployment.plan.json"))
    profile = default_profile(plan.vendor)
    observed = {"python": platform.python_version()}
    for distribution in ("ray", "vllm"):
        try:
            observed[distribution] = metadata.version(distribution)
        except metadata.PackageNotFoundError:
            pass
    profile.verify_environment(observed)
    profile.verify_installed_sources()
    resolved_manifest_hash = manifest_hash(profile)
    if (
        profile.profile_id != plan.compatibility_profile_hash
        or resolved_manifest_hash != plan.manifest_hash
    ):
        raise SourceStagingError("node compatibility proof disagrees with DeploymentPlan")
    return {
        "compatibility_profile_id": profile.profile_id,
        "compatibility_manifest_hash": resolved_manifest_hash,
    }


def _remove_source_candidate_parent(candidate: Path, *, local_base: Path, failed: bool) -> None:
    """Remove only this transaction's exact generation/attempt candidate."""

    parent = candidate.parent
    if (
        candidate.name != "capsule"
        or re.fullmatch(r"source\.[0-9a-f]{32}", parent.name) is None
        or re.fullmatch(r"g[0-9]+", parent.parent.name) is None
    ):
        return
    candidates_root = local_base / "candidates"
    try:
        from .model_staging import validate_node_local_tree

        generation_root = parent.parent
        if generation_root.parent != candidates_root:
            raise ValueError("candidate is outside the exact candidates namespace")
        for component in (candidates_root, generation_root, parent):
            metadata = os.lstat(component)
            if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
                raise ValueError(f"candidate ancestor is not a real directory: {component}")
        validate_node_local_tree(parent, local_root=local_base)
    except FileNotFoundError:
        return
    except (OSError, ValueError) as exc:
        raise SourceStagingError(f"source candidate cleanup path is unsafe: {parent}") from exc
    if failed:
        if candidate.exists():
            _thaw_private_input(candidate)
        shutil.rmtree(parent)
        return
    try:
        parent.rmdir()
    except OSError as exc:
        raise SourceStagingError(
            f"published source candidate parent is unexpectedly nonempty: {parent}: {exc}"
        ) from exc


def _rollback_source_candidate(candidate: Path, *, local_base: Path) -> None:
    """Remove only this attempt's candidate; immutable publications are reusable."""
    _remove_source_candidate_parent(candidate, local_base=local_base, failed=True)


def _clean_package_snapshot(destination: Path, *, vendor: str) -> Path:
    package = Path(str(resources.files("exaserve"))).resolve()
    if not package.is_dir():
        raise SourceStagingError("installed ExaServe package is not a filesystem artifact")
    for source in package.rglob("*"):
        if source.is_symlink():
            raise SourceStagingError(f"release source contains a symlink: {source}")
    clean = destination / "exaserve"
    shutil.copytree(package, clean, symlinks=False, ignore=_ignore)
    metadata_candidates = sorted(
        [
            *package.parent.glob("exaserve-*.dist-info"),
            *package.parent.glob("exaserve.egg-info"),
        ],
        key=lambda item: item.name,
    )
    if len(metadata_candidates) > 1:
        raise SourceStagingError("installed ExaServe package has ambiguous distribution metadata")
    if metadata_candidates:
        _copy_release_tree(metadata_candidates[0], destination / metadata_candidates[0].name)
    # Immutable release snapshots are intentionally read-only.  ``copytree``
    # preserves those directory modes, but this private transaction still has
    # to add its generated compatibility overlay and remove its input when the
    # broadcast finishes.  Add owner permissions only to directories in the
    # newly-created copy; never mutate the installed snapshot or broaden file
    # permissions in the artifact being broadcast.
    clean.chmod(clean.stat().st_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    for current, directories, _files in os.walk(clean, topdown=True, followlinks=False):
        current_path = Path(current)
        for name in directories:
            directory = current_path / name
            if directory.is_symlink():
                raise SourceStagingError(
                    f"clean source copy contains an unresolved directory symlink: {directory}"
                )
            directory.chmod(directory.stat().st_mode | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR)
    from .compat.generated_overlay import materialize
    from .compat.profile import default_profile

    profile = default_profile(vendor)
    materialize(profile, clean / "_compat_runtime" / profile.profile_id)
    return clean


def _copy_regular(source: Path, destination: Path, *, executable: bool = False) -> None:
    try:
        metadata = os.lstat(source)
    except OSError as exc:
        raise SourceStagingError(f"runtime capsule input is unavailable: {source}: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode):
        raise SourceStagingError(f"runtime capsule input must be a regular file: {source}")
    destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    shutil.copyfile(source, destination)
    destination.chmod(0o755 if executable else 0o644)


def _copy_release_tree(source: Path, destination: Path) -> None:
    if not source.is_dir() or source.is_symlink():
        raise SourceStagingError(f"runtime capsule tree must be a real directory: {source}")
    for entry in source.rglob("*"):
        if entry.is_symlink():
            raise SourceStagingError(f"runtime capsule tree contains a symlink: {entry}")
    shutil.copytree(source, destination, symlinks=False, ignore=_ignore)


def _eval_source() -> Path:
    configured = os.environ.get("EXASERVE_EVAL_SOURCE", "").strip()
    if configured:
        return Path(configured).resolve()
    spec = importlib_util.find_spec("eval")
    if spec is not None and spec.submodule_search_locations:
        locations = list(spec.submodule_search_locations)
        if len(locations) == 1:
            return Path(locations[0]).resolve()
    package = Path(str(resources.files("exaserve"))).resolve()
    repository = package.parents[1] if package.parent.name == "src" else package.parent
    return repository / "eval"


def _qualified_python(profile) -> Path:
    configured = os.environ.get("EXASERVE_QUALIFIED_PYTHON", "").strip()
    if not configured:
        raise SourceStagingError(
            "EXASERVE_QUALIFIED_PYTHON is required; the ambient interpreter is not authority"
        )
    candidate = Path(configured)
    if not candidate.is_absolute():
        raise SourceStagingError("EXASERVE_QUALIFIED_PYTHON must be absolute")
    from .site import qualify_site_local_bootstrap

    try:
        return Path(qualify_site_local_bootstrap(str(candidate), profile))
    except RuntimeError as exc:
        raise SourceStagingError(f"qualified Python failed SiteProfile evidence: {exc}") from exc


def _build_runtime_capsule(
    destination: Path,
    *,
    plan_path: Path,
    site_profile_path: Path,
    binding_path: Path,
    vendor: str,
    bcast_binary: Path,
    deployment_plan,
) -> Path:
    """Assemble the exact run capsule on allocation-head local storage."""

    capsule = destination / "capsule"
    (capsule / "python").mkdir(mode=0o700, parents=True)
    _clean_package_snapshot(capsule / "python", vendor=vendor)
    eval_source = _eval_source()
    run_plan_source = os.environ.get("EXASERVE_RUN_PLAN_PATH", "").strip()
    eval_manifest_source = os.environ.get("EXASERVE_EVAL_MANIFEST_PATH", "").strip()
    eval_declared = bool(run_plan_source or eval_manifest_source)
    if bool(run_plan_source) != bool(eval_manifest_source):
        raise SourceStagingError(
            "evaluation staging requires EXASERVE_RUN_PLAN_PATH and "
            "EXASERVE_EVAL_MANIFEST_PATH together"
        )
    if eval_source.is_dir():
        _copy_release_tree(eval_source, capsule / "python" / "eval")
    elif eval_declared:
        raise SourceStagingError(f"evaluation source package is unavailable: {eval_source}")
    _copy_regular(plan_path, capsule / "run" / "deployment.plan.json")
    _copy_regular(site_profile_path, capsule / "run" / "site.profile.json")
    _copy_regular(binding_path, capsule / "run" / "allocation_binding.json")
    if run_plan_source:
        from .plan.io import load_run_plan

        run_plan = load_run_plan(run_plan_source)
        if run_plan.deployment.deployment_plan_hash != deployment_plan.deployment_plan_hash:
            raise SourceStagingError("RunPlan embeds a different DeploymentPlan")
        _copy_regular(Path(run_plan_source), capsule / "run" / "run.plan.json")
    if eval_manifest_source:
        # The replay layer performs its own typed/hash validation; staging owns
        # only the exact-byte, head-reader transfer boundary.
        _copy_regular(Path(eval_manifest_source), capsule / "run" / "eval_manifest.yaml")
    _copy_regular(bcast_binary, capsule / "bin" / "bcast", executable=True)
    if eval_declared:
        configured_go = os.environ.get("EXASERVE_GO_DISPATCH_SOURCE", "").strip()
        go_dispatch = (
            Path(configured_go)
            if configured_go
            else eval_source / "go_client" / "bin" / "go_dispatch"
        )
        _copy_regular(go_dispatch, capsule / "bin" / "go_dispatch", executable=True)
    _freeze_capsule(capsule)
    return capsule


def _freeze_capsule(root: Path) -> None:
    """Make content-addressed runtime bytes immutable before hashing/transfer."""

    for current, directories, files in os.walk(root, topdown=False, followlinks=False):
        current_path = Path(current)
        for name in files:
            path = current_path / name
            mode = stat.S_IMODE(os.lstat(path).st_mode)
            path.chmod(0o555 if mode & 0o111 else 0o444)
        for name in directories:
            (current_path / name).chmod(0o555)
    root.chmod(0o555)


def _thaw_private_input(root: Path) -> None:
    """Restore owner directory permissions solely for head-local cleanup."""

    from .model_staging import chmod_real_directory

    if not root.exists():
        return
    for current, directories, _files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        if not chmod_real_directory(current_path, 0o700):
            continue
        for name in directories:
            directory = current_path / name
            metadata = os.lstat(directory)
            if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
                chmod_real_directory(directory, 0o700)


def _run_checked(argv: list[str], *, timeout_s: float, env: dict[str, str] | None = None):
    from .control.finite_process import FiniteProcessError, run_finite

    try:
        completed = run_finite(argv, timeout_s=timeout_s, env=env)
    except (OSError, FiniteProcessError) as exc:
        raise SourceStagingError(f"native staging command failed: {exc}") from exc
    if completed.stdout:
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n", flush=True)
    if completed.stderr:
        print(
            completed.stderr,
            end="" if completed.stderr.endswith("\n") else "\n",
            file=sys.stderr,
            flush=True,
        )
    if completed.returncode != 0:
        raise SourceStagingError(f"native staging command exited {completed.returncode}: {argv[0]}")
    return completed


def qualify_runtime_staging_base(root: Path, profile) -> Path:
    """Require the staging destination's declared fstype/RO identity."""

    from .model_staging import validate_node_local_root
    from .plan.runtime_environment import validate_declared_filesystem
    from .site import require_complete_filesystem_policy

    try:
        require_complete_filesystem_policy(profile)
        qualified = validate_node_local_root(root)
        identity = validate_declared_filesystem(
            qualified,
            policy=profile,
            root_kind="local_root",
        )
    except (RuntimeError, ValueError) as exc:
        raise SourceStagingError(f"runtime staging base is not proven node-local: {exc}") from exc
    if identity is None:
        raise SourceStagingError("runtime staging base has no declared local filesystem identity")
    return qualified


def stage(
    plan_path: str, binding_path: str, result_path: str, *, operation_timeout_s: float = 1800.0
) -> dict:
    from .model_bcast import (
        bootstrap_application_environment,
        cleanup_native_candidates,
        compile_bcast,
        mpi_launch_prefix,
        resolve_bcast_executable,
    )
    from .plan.io import load_allocation_binding, load_deployment_plan, load_site_profile
    from .plan.contracts import same_node
    from .state.atomic import atomic_create_json
    from .staging_results import StagingCollectiveError, load_collective_results
    from .site import require_complete_filesystem_policy, validate_local_stage_policy
    from .model_staging import ensure_node_local_directory
    from .plan.runtime_environment import shared_roots, validate_declared_filesystem

    plan = load_deployment_plan(plan_path)
    binding = load_allocation_binding(binding_path)
    if (
        binding.deployment_plan_hash != plan.deployment_plan_hash
        or binding.site_profile_hash != plan.site_profile_hash
        or len(binding.rank_to_node) != plan.num_nodes
    ):
        raise SourceStagingError("allocation binding does not belong to the source staging plan")

    site_profile_path = os.environ.get("EXASERVE_SITE_PROFILE_PATH", "").strip()
    if not site_profile_path:
        raise SourceStagingError("EXASERVE_SITE_PROFILE_PATH is required for runtime staging")
    site_profile = load_site_profile(site_profile_path)
    if (
        site_profile.site_profile_hash != plan.site_profile_hash
        or site_profile.site_id != plan.site_profile_id
    ):
        raise SourceStagingError("SiteProfile does not belong to the source staging plan")
    try:
        require_complete_filesystem_policy(site_profile)
        validate_local_stage_policy(plan, site_profile)
    except RuntimeError as exc:
        raise SourceStagingError(f"runtime filesystem policy is not executable: {exc}") from exc

    run_dir = Path(result_path).resolve().parent
    run_dir.mkdir(parents=True, exist_ok=True)
    local_base = Path(os.environ.get("EXASERVE_LOCAL_RUNTIME_BASE", "/tmp/exaserve"))
    try:
        if (
            validate_declared_filesystem(
                local_base,
                policy=site_profile,
                root_kind="local_root",
            )
            is None
        ):
            raise SourceStagingError(
                f"runtime staging base has no declared local filesystem identity: {local_base}"
            )
        local_base = ensure_node_local_directory(
            local_base,
            shared_roots=tuple(shared_roots(site_profile)),
        )
    except (RuntimeError, ValueError) as exc:
        raise SourceStagingError(f"runtime staging base is not proven node-local: {exc}") from exc
    local_base = qualify_runtime_staging_base(local_base, site_profile)
    qualified_python = _qualified_python(site_profile)
    scheduler = plan.scale_envelope.scheduler_type
    binary = (
        compile_bcast(site_profile=site_profile)
        if scheduler == "pbs"
        else resolve_bcast_executable(scheduler=scheduler, site_profile=site_profile)
    )
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="source-input.", dir=local_base) as temporary:
        source_root = Path(temporary)
        capsule = _build_runtime_capsule(
            source_root,
            plan_path=Path(plan_path),
            site_profile_path=Path(site_profile_path),
            binding_path=Path(binding_path),
            vendor=plan.vendor,
            bcast_binary=binary,
            deployment_plan=plan,
        )
        manifest = tree_manifest(capsule)
        attempt = uuid.uuid4().hex
        stable = (
            local_base / "runtime" / f"g{binding.generation}" / manifest["source_manifest_hash"]
        )
        paths = runtime_paths(
            stable,
            deployment_id=plan.deployment_id,
            generation=binding.generation,
            qualified_python=str(qualified_python),
        )
        candidate_parent = (
            local_base / "candidates" / f"g{binding.generation}" / f"source.{attempt}"
        )
        candidate_package = candidate_parent / "capsule"
        root_node = binding.rank_to_node[0][1]
        native_application_environment = bootstrap_application_environment(
            profile=site_profile,
            base_environment=os.environ,
        )
        transfer_prefix = mpi_launch_prefix(
            plan.num_nodes,
            scheduler=scheduler,
            application_cwd="/tmp",
            transfer_executable=(scheduler == "pbs"),
            application_environment=native_application_environment,
        )
        try:
            _run_checked(
                [
                    *transfer_prefix,
                    str(binary),
                    "--expected-root-host",
                    root_node,
                    "--expected-world-size",
                    str(plan.num_nodes),
                    str(capsule),
                    str(candidate_parent),
                ],
                timeout_s=operation_timeout_s,
            )
        except BaseException as exc:
            try:
                cleanup_native_candidates(
                    binary,
                    candidate_parent,
                    local_root=local_base,
                    num_nodes=plan.num_nodes,
                    binding=binding,
                    scheduler=scheduler,
                    application_cwd=Path("/tmp"),
                    application_environment=native_application_environment,
                    transfer_executable=(scheduler == "pbs"),
                    timeout_s=min(300.0, operation_timeout_s),
                )
            except BaseException as cleanup_exc:
                add_exception_note(exc, f"source native cleanup also failed: {cleanup_exc}")
            raise
        finally:
            _thaw_private_input(capsule)
        verify_env = os.environ.copy()
        for name in ("PYTHONHOME", "VIRTUAL_ENV", "CONDA_PREFIX"):
            verify_env.pop(name, None)
        verify_env.pop("PYTHONPATH", None)
        # The final state tree does not exist until this verifier creates it.
        # Its already-extracted candidate parent is the only pre-existing
        # node-local bootstrap scratch directory.
        bootstrap_python_additions = {
            "PYTHONNOUSERSITE": "1",
            "PYTHONSAFEPATH": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONPYCACHEPREFIX": str(candidate_parent),
            "HOME": str(candidate_parent),
            "TMPDIR": str(candidate_parent),
            "TMP": str(candidate_parent),
            "TEMP": str(candidate_parent),
            "XDG_CACHE_HOME": str(candidate_parent),
            "XDG_CONFIG_HOME": str(candidate_parent),
            "XDG_DATA_HOME": str(candidate_parent),
        }
        verifier_command = [
            *mpi_launch_prefix(
                plan.num_nodes,
                scheduler=scheduler,
                application_cwd=str(candidate_parent),
                application_environment=bootstrap_application_environment(
                    profile=site_profile,
                    base_environment=verify_env,
                    additions=bootstrap_python_additions,
                ),
            ),
            str(qualified_python),
            "-I",
            "-s",
            "-c",
            _QUALIFIED_VERIFIER_BOOTSTRAP,
            str(candidate_package),
            manifest["source_manifest_hash"],
            str(manifest["file_count"]),
            str(manifest["total_bytes"]),
            str(local_base),
            "--verify-and-publish",
            str(candidate_package),
            "--expected-hash",
            manifest["source_manifest_hash"],
            "--expected-files",
            str(manifest["file_count"]),
            "--expected-bytes",
            str(manifest["total_bytes"]),
            "--generation",
            str(binding.generation),
            "--stable-path",
            str(stable),
            "--local-base",
            str(local_base),
            "--state-root",
            str(paths.state_root),
            "--qualified-python",
            str(qualified_python),
            "--attempt-id",
            attempt,
            "--expected-world-size",
            str(plan.num_nodes),
            "--expected-root-node",
            root_node,
        ]
        try:
            verified = _run_checked(
                verifier_command,
                timeout_s=operation_timeout_s,
                env=verify_env,
            )
        except BaseException as exc:
            for cleanup_path in (candidate_parent,):
                try:
                    cleanup_native_candidates(
                        binary,
                        cleanup_path,
                        local_root=local_base,
                        num_nodes=plan.num_nodes,
                        binding=binding,
                        scheduler=scheduler,
                        application_cwd=Path("/tmp"),
                        application_environment=native_application_environment,
                        transfer_executable=(scheduler == "pbs"),
                        timeout_s=min(300.0, operation_timeout_s),
                    )
                except BaseException as cleanup_exc:
                    add_exception_note(
                        exc,
                        f"source verifier cleanup for {cleanup_path} also failed: {cleanup_exc}",
                    )
            raise
        try:
            receipts = [
                _validate_source_receipt(receipt)
                for receipt in load_collective_results(
                    verified.stdout,
                    attempt_id=attempt,
                    expected_world_size=plan.num_nodes,
                    expected_root_node=root_node,
                )
            ]
        except (RuntimeError, StagingCollectiveError) as exc:
            raise SourceStagingError(str(exc)) from exc
        if len(receipts) != plan.num_nodes:
            raise SourceStagingError(
                f"expected {plan.num_nodes} per-rank source receipts, received {len(receipts)}"
            )
        by_rank = {item.get("rank"): item for item in receipts}
        if len(by_rank) != plan.num_nodes or set(by_rank) != set(range(plan.num_nodes)):
            raise SourceStagingError("source receipts contain missing/duplicate rank IDs")
        for rank, planned_node in binding.rank_to_node:
            receipt = by_rank[rank]
            if not same_node(receipt["node"], planned_node):
                raise SourceStagingError(
                    f"source receipt rank {rank} came from {receipt.get('node')!r}, "
                    f"planned node is {planned_node!r}"
                )
            if (
                receipt.get("generation") != binding.generation
                or receipt.get("source_manifest_hash") != manifest["source_manifest_hash"]
            ):
                raise SourceStagingError(f"source receipt rank {rank} has stale/wrong identity")
        python_hashes = {receipt["qualified_python_sha256"] for receipt in receipts}
        python_paths = {receipt["qualified_python_path"] for receipt in receipts}
        if python_hashes != {by_rank[0]["qualified_python_sha256"]} or python_paths != {
            str(qualified_python)
        }:
            raise SourceStagingError("qualified Python identity differs across allocation ranks")
        if {receipt["compatibility_profile_id"] for receipt in receipts} != {
            plan.compatibility_profile_hash
        } or {receipt["compatibility_manifest_hash"] for receipt in receipts} != {
            plan.manifest_hash
        }:
            raise SourceStagingError("compatibility proof differs across allocation ranks")
        result = validate_source_staging_result(
            {
                "schema_version": 2,
                "deployment_id": plan.deployment_id,
                "generation": binding.generation,
                "deployment_plan_hash": plan.deployment_plan_hash,
                "site_profile_hash": plan.site_profile_hash,
                "allocation_binding_hash": binding.allocation_binding_hash,
                "source_manifest_hash": manifest["source_manifest_hash"],
                "capsule_manifest_hash": manifest["source_manifest_hash"],
                "local_runtime_root": str(paths.root),
                "local_python_root": str(paths.python_root),
                "local_plan_path": str(paths.plan_path),
                "local_site_profile_path": str(paths.site_profile_path),
                "local_binding_path": str(paths.binding_path),
                "local_bcast_path": str(paths.bcast_path),
                "local_go_dispatch": (
                    str(paths.go_dispatch_path)
                    if os.environ.get("EXASERVE_RUN_PLAN_PATH", "").strip()
                    else None
                ),
                "local_eval_manifest": (
                    str(paths.eval_manifest_path)
                    if os.environ.get("EXASERVE_EVAL_MANIFEST_PATH", "").strip()
                    else None
                ),
                "local_run_plan": (
                    str(paths.run_plan_path)
                    if os.environ.get("EXASERVE_RUN_PLAN_PATH", "").strip()
                    else None
                ),
                "local_state_root": str(paths.state_root),
                "qualified_python": str(qualified_python),
                "qualified_python_sha256": by_rank[0]["qualified_python_sha256"],
                "compatibility_profile_id": plan.compatibility_profile_hash,
                "compatibility_manifest_hash": plan.manifest_hash,
                "file_count": manifest["file_count"],
                "total_bytes": manifest["total_bytes"],
                "files": manifest["files"],
                "duration_s": round(time.monotonic() - started, 6),
                "rank_receipts": [by_rank[index] for index in range(plan.num_nodes)],
            },
            expected_deployment_id=plan.deployment_id,
            expected_generation=binding.generation,
            expected_plan_hash=plan.deployment_plan_hash,
            expected_site_profile_hash=plan.site_profile_hash,
            expected_binding_hash=binding.allocation_binding_hash,
            expected_rank_to_node=binding.rank_to_node,
            expected_run_dir=run_dir,
        )
    atomic_create_json(result_path, result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Transactional ExaServe source staging")
    parser.add_argument("--plan")
    parser.add_argument("--binding")
    parser.add_argument("--result")
    parser.add_argument("--operation-timeout-s", type=float, default=1800.0)
    parser.add_argument("--verify-and-publish")
    parser.add_argument("--expected-hash")
    parser.add_argument("--expected-files", type=int)
    parser.add_argument("--expected-bytes", type=int)
    parser.add_argument("--generation", type=int)
    parser.add_argument("--stable-path")
    parser.add_argument("--local-base")
    parser.add_argument("--state-root")
    parser.add_argument("--qualified-python")
    parser.add_argument("--attempt-id")
    parser.add_argument("--expected-world-size", type=int)
    parser.add_argument("--expected-root-node")
    args = parser.parse_args(argv)

    if args.verify_and_publish:
        required = (
            args.expected_hash,
            args.expected_files,
            args.expected_bytes,
            args.generation,
            args.stable_path,
            args.local_base,
            args.state_root,
            args.qualified_python,
            args.attempt_id,
            args.expected_world_size,
            args.expected_root_node,
        )
        if any(value is None for value in required):
            parser.error("verification mode requires expected identity fields")
        from .staging_results import run_collective_operation

        candidate = Path(args.verify_and_publish)
        local_base = Path(args.local_base)

        def operation() -> dict:
            try:
                return verify_and_publish(
                    candidate,
                    args.expected_hash,
                    args.expected_files,
                    args.expected_bytes,
                    args.generation,
                    Path(args.stable_path),
                    local_base=local_base,
                    state_root=Path(args.state_root),
                    qualified_python=Path(args.qualified_python),
                )
            except BaseException as exc:
                try:
                    _remove_source_candidate_parent(candidate, local_base=local_base, failed=True)
                except BaseException as cleanup_exc:
                    add_exception_note(exc, f"source candidate cleanup also failed: {cleanup_exc}")
                raise

        success = run_collective_operation(
            operation,
            attempt_id=args.attempt_id,
            expected_world_size=args.expected_world_size,
            expected_root_node=args.expected_root_node,
        )
        if not success:
            try:
                _rollback_source_candidate(
                    candidate,
                    local_base=local_base,
                )
            except BaseException as cleanup_exc:
                print(
                    f"[SourceStaging] rollback cleanup failed: {cleanup_exc}",
                    file=sys.stderr,
                    flush=True,
                )
        return 0 if success else 1
    if not args.plan or not args.binding or not args.result:
        parser.error("--plan, --binding, and --result are required")
    stage(args.plan, args.binding, args.result, operation_timeout_s=args.operation_timeout_s)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SourceStagingError as exc:
        print(f"[SourceStaging] ERROR: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1) from None
