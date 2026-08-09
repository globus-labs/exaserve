"""Bounded Python-owned node diagnostics archive publication (WP4/WP10)."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import stat
import tarfile
import tempfile
from pathlib import PurePosixPath

from ..exception_notes import add_exception_note
from .atomic import atomic_create_json


class DiagnosticsError(RuntimeError):
    pass


_SHA256 = re.compile(r"[0-9a-f]{64}")
_MANIFEST_FIELDS = {
    "schema_version",
    "deployment_id",
    "generation",
    "rank",
    "archive",
    "archive_sha256",
    "file_count",
    "source_bytes",
    "max_bytes",
    "max_files",
    "skipped_files",
    "complete",
    "files",
}


def _safe_relative_path(value: object) -> bool:
    if not isinstance(value, str) or not value or "\\" in value:
        return False
    path = PurePosixPath(value)
    return not path.is_absolute() and all(part not in {"", ".", ".."} for part in path.parts)


def validate_diagnostics_manifest(
    value: object,
    *,
    run_dir: str,
    expected_rank: int,
    expected_deployment_id: str,
    expected_generation: int,
) -> dict:
    if not isinstance(value, dict) or set(value) != _MANIFEST_FIELDS:
        raise DiagnosticsError("diagnostics manifest fields are invalid")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise DiagnosticsError("diagnostics manifest schema is unsupported")
    if (
        value["deployment_id"] != expected_deployment_id
        or type(value["generation"]) is not int
        or value["generation"] != expected_generation
        or type(value["rank"]) is not int
        or value["rank"] != expected_rank
    ):
        raise DiagnosticsError("diagnostics manifest identity is inconsistent")
    expected_archive = f"rank-{expected_rank:05d}.tar.gz"
    if value["archive"] != expected_archive:
        raise DiagnosticsError("diagnostics manifest archive name is invalid")
    if not isinstance(value["archive_sha256"], str) or not _SHA256.fullmatch(
        value["archive_sha256"]
    ):
        raise DiagnosticsError("diagnostics manifest archive hash is invalid")
    for field in (
        "file_count",
        "source_bytes",
        "max_bytes",
        "max_files",
        "skipped_files",
    ):
        if type(value[field]) is not int or value[field] < 0:
            raise DiagnosticsError(f"diagnostics manifest {field} is invalid")
    if value["max_bytes"] < 1 or value["max_files"] < 1:
        raise DiagnosticsError("diagnostics manifest bounds must be positive")
    if type(value["complete"]) is not bool or value["complete"] != (value["skipped_files"] == 0):
        raise DiagnosticsError("diagnostics manifest complete flag is inconsistent")
    files = value["files"]
    if not isinstance(files, list) or len(files) != value["file_count"]:
        raise DiagnosticsError("diagnostics manifest file inventory is inconsistent")
    paths: list[str] = []
    total = 0
    for index, entry in enumerate(files):
        if not isinstance(entry, dict) or set(entry) != {"path", "size_bytes"}:
            raise DiagnosticsError(f"diagnostics manifest file {index} fields are invalid")
        if not _safe_relative_path(entry["path"]):
            raise DiagnosticsError(f"diagnostics manifest file {index} path is unsafe")
        if type(entry["size_bytes"]) is not int or entry["size_bytes"] < 0:
            raise DiagnosticsError(f"diagnostics manifest file {index} size is invalid")
        paths.append(entry["path"])
        total += entry["size_bytes"]
    if paths != sorted(set(paths)):
        raise DiagnosticsError("diagnostics manifest file paths must be unique and sorted")
    if (
        total != value["source_bytes"]
        or value["file_count"] > value["max_files"]
        or total > value["max_bytes"]
    ):
        raise DiagnosticsError("diagnostics manifest inventory exceeds or disagrees with bounds")
    archive_path = os.path.join(os.path.abspath(run_dir), "per_node", expected_archive)
    try:
        archive_mode = os.lstat(archive_path).st_mode
    except OSError as exc:
        raise DiagnosticsError(f"diagnostics archive is unavailable: {exc}") from exc
    if not stat.S_ISREG(archive_mode) or _sha256(archive_path) != value["archive_sha256"]:
        raise DiagnosticsError("diagnostics archive type or content hash is invalid")
    return value


def _sha256(path: str) -> str:
    from .atomic import regular_file_reader

    digest = hashlib.sha256()
    with regular_file_reader(path, binary=True) as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_node_diagnostics(
    *,
    source_root: str,
    run_dir: str,
    rank: int,
    deployment_id: str,
    generation: int,
    max_bytes: int = 64 * 1024 * 1024,
    max_files: int = 10_000,
) -> dict:
    """Publish one bounded archive and manifest per rank without a shell/MPI hop."""
    for name, value in (
        ("source_root", source_root),
        ("run_dir", run_dir),
        ("deployment_id", deployment_id),
    ):
        if not isinstance(value, str) or not value:
            raise DiagnosticsError(f"diagnostics {name} must be non-empty text")
    if (
        isinstance(rank, bool)
        or not isinstance(rank, int)
        or rank < 0
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 0
    ):
        raise DiagnosticsError("diagnostics identity is invalid")
    if (
        isinstance(max_bytes, bool)
        or not isinstance(max_bytes, int)
        or max_bytes <= 0
        or isinstance(max_files, bool)
        or not isinstance(max_files, int)
        or max_files <= 0
    ):
        raise DiagnosticsError("diagnostics bounds/identity are invalid")
    source = os.path.abspath(source_root)
    destination = os.path.join(os.path.abspath(run_dir), "per_node")
    from .atomic import ensure_owned_directory

    ensure_owned_directory(destination)
    archive_path = os.path.join(destination, f"rank-{rank:05d}.tar.gz")
    manifest_path = os.path.join(destination, f"rank-{rank:05d}.manifest.json")
    if os.path.lexists(archive_path) or os.path.lexists(manifest_path):
        raise DiagnosticsError(f"diagnostics for rank {rank} were already published")

    selected: list[tuple[str, str, int, int]] = []
    skipped = 0
    total = 0
    if os.path.isdir(source):
        for root, dirs, names in os.walk(source, followlinks=False):
            dirs[:] = sorted(name for name in dirs if not os.path.islink(os.path.join(root, name)))
            for name in sorted(names):
                path = os.path.join(root, name)
                try:
                    metadata = os.lstat(path)
                except OSError as exc:
                    raise DiagnosticsError(
                        f"could not inspect diagnostics file {path}: {exc}"
                    ) from exc
                if not stat.S_ISREG(metadata.st_mode):
                    skipped += 1
                    continue
                size = metadata.st_size
                relative = os.path.relpath(path, source)
                if len(selected) >= max_files or total + size > max_bytes:
                    skipped += 1
                    continue
                selected.append((path, relative, metadata.st_dev, metadata.st_ino))
                total += size
    selected.sort(key=lambda item: item[1])

    fd, temporary = tempfile.mkstemp(
        prefix=f".rank-{rank:05d}.", suffix=".tar.gz.tmp", dir=destination
    )
    os.close(fd)
    actual_selected: list[tuple[str, int]] = []
    actual_total = 0
    try:
        from .atomic import atomic_create_bytes, regular_file_reader

        with tarfile.open(temporary, "w:gz") as archive:
            for path, relative, expected_dev, expected_ino in selected:
                try:
                    with regular_file_reader(path, binary=True) as source_handle:
                        metadata = os.fstat(source_handle.fileno())
                        if (metadata.st_dev, metadata.st_ino) != (expected_dev, expected_ino):
                            skipped += 1
                            continue
                        if actual_total + metadata.st_size > max_bytes:
                            skipped += 1
                            continue
                        info = tarfile.TarInfo(relative)
                        info.size = metadata.st_size
                        info.mode = stat.S_IMODE(metadata.st_mode)
                        info.mtime = int(metadata.st_mtime)
                        info.uid = 0
                        info.gid = 0
                        info.uname = ""
                        info.gname = ""
                        archive.addfile(info, source_handle)
                        actual_selected.append((relative, metadata.st_size))
                        actual_total += metadata.st_size
                except (OSError, ValueError):
                    skipped += 1
        with open(temporary, "rb") as handle:
            os.fsync(handle.fileno())
        with regular_file_reader(temporary, binary=True) as handle:
            atomic_create_bytes(archive_path, handle.read())
        os.unlink(temporary)
    except BaseException as exc:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            add_exception_note(exc, f"diagnostics temporary cleanup also failed: {cleanup_exc}")
        raise

    try:
        manifest = {
            "schema_version": 1,
            "deployment_id": deployment_id,
            "generation": generation,
            "rank": rank,
            "archive": os.path.basename(archive_path),
            "archive_sha256": _sha256(archive_path),
            "file_count": len(actual_selected),
            "source_bytes": actual_total,
            "max_bytes": max_bytes,
            "max_files": max_files,
            "skipped_files": skipped,
            "complete": skipped == 0,
            "files": [{"path": relative, "size_bytes": size} for relative, size in actual_selected],
        }
        validate_diagnostics_manifest(
            manifest,
            run_dir=run_dir,
            expected_rank=rank,
            expected_deployment_id=deployment_id,
            expected_generation=generation,
        )
        atomic_create_json(manifest_path, manifest)
    except BaseException as exc:
        # The archive is not a committed result until its integrity manifest
        # exists.  Remove an unpublished archive so a retry is possible and a
        # consumer cannot mistake an orphan for a complete diagnostic bundle.
        try:
            os.unlink(archive_path)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            add_exception_note(exc, f"unpublished diagnostics cleanup also failed: {cleanup_exc}")
        raise
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Bounded ExaServe node diagnostics collector")
    parser.add_argument("--source-root", required=True)
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--rank", required=True, type=int)
    parser.add_argument("--deployment-id", required=True)
    parser.add_argument("--generation", required=True, type=int)
    parser.add_argument("--max-bytes", type=int, default=64 * 1024 * 1024)
    parser.add_argument("--max-files", type=int, default=10_000)
    args = parser.parse_args(argv)
    collect_node_diagnostics(
        source_root=args.source_root,
        run_dir=args.run_dir,
        rank=args.rank,
        deployment_id=args.deployment_id,
        generation=args.generation,
        max_bytes=args.max_bytes,
        max_files=args.max_files,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
