"""
Model staging and hydration utilities for ExaServe.

This module handles downloading and caching models to a specified lustre path
before launching services, ensuring all vLLM instances can use local copies
instead of downloading from HuggingFace directly.
"""

import json
import hashlib
import errno
import os
import shutil
import stat
import sys
import time
import re
from pathlib import Path, PurePosixPath
from typing import Dict, List

from .exception_notes import add_exception_note
from .model_paths import (
    get_model_storage_path,
    get_model_storage_name,
    iter_unique_model_ids,
)
from .plan.contracts import ModelPlan
from .state.atomic import strict_json_load, strict_json_load_path


def print_red(message: str):
    """Print message in red color."""
    RED = "\033[91m"
    RESET = "\033[0m"
    print(f"{RED}{message}{RESET}", flush=True)


COMPLETION_MARKER = ".exaserve_complete.json"
MODEL_MANIFEST_VERSION = 2
MODEL_WEIGHT_SUFFIXES = (".safetensors", ".bin", ".pt", ".pth", ".gguf")
_SAMPLE_BYTES = 1 << 20
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_MANIFEST_FIELDS = {
    "version",
    "kind",
    "source_identity",
    "file_count",
    "total_bytes",
    "manifest_hash",
    "files",
}
_MANIFEST_FILE_FIELDS = {"path", "size", "hash_kind", "sha256"}
_VERIFIED_MANIFEST_CACHE: dict[str, tuple[tuple, dict]] = {}


def configured_shared_roots(value: str | None = None) -> tuple[Path, ...]:
    """Return normalized roots that managed workers may never traverse.

    The canonical runtime exports ``EXASERVE_SHARED_FILESYSTEM_ROOTS`` from the
    SiteProfile as an ``os.pathsep``-separated list.  Aurora's project and home
    roots are the fail-closed defaults while older plans are being migrated.
    """

    raw = (
        value
        if value is not None
        else os.environ.get("EXASERVE_SHARED_ROOTS", "")
        or os.environ.get("EXASERVE_SHARED_FILESYSTEM_ROOTS", "")
    )
    entries = [item for item in raw.split(os.pathsep) if item]
    if not entries:
        entries = ["/home", "/lus/flare"]
    roots: list[Path] = []
    for entry in entries:
        path = Path(entry)
        if not path.is_absolute() or ".." in path.parts:
            raise ValueError(f"shared filesystem root must be absolute and normalized: {entry!r}")
        roots.append(Path(os.path.realpath(path)))
    return tuple(dict.fromkeys(roots))


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def validate_node_local_root(
    root: str | os.PathLike,
    *,
    shared_roots: tuple[Path, ...] | None = None,
) -> Path:
    """Prove an existing root is real, local, and outside shared filesystems."""

    requested = Path(root)
    if not requested.is_absolute() or ".." in requested.parts:
        raise ValueError(f"node-local root must be absolute and normalized: {requested}")
    try:
        metadata = os.lstat(requested)
    except OSError as exc:
        raise ValueError(f"node-local root is unavailable: {requested}: {exc}") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValueError(f"node-local root must be a real directory: {requested}")
    resolved = Path(os.path.realpath(requested))
    declared_shared = shared_roots if shared_roots is not None else configured_shared_roots()
    if any(_is_relative_to(resolved, shared) for shared in declared_shared):
        raise ValueError(f"node-local root resolves beneath shared storage: {resolved}")
    shared_devices: set[int] = set()
    for shared in declared_shared:
        try:
            shared_devices.add(os.stat(shared).st_dev)
        except OSError:
            continue
    if metadata.st_dev in shared_devices:
        raise ValueError(
            f"node-local root {resolved} has the same filesystem identity as shared storage"
        )
    # Resolve alone is insufficient: explicitly reject a symlink in any
    # component of the configured root.
    cursor = Path(requested.anchor)
    for part in requested.parts[1:]:
        cursor /= part
        try:
            component = os.lstat(cursor)
        except OSError as exc:
            raise ValueError(f"node-local root component is unavailable: {cursor}: {exc}") from exc
        if stat.S_ISLNK(component.st_mode):
            raise ValueError(f"node-local root contains a symlink component: {cursor}")
    return resolved


def ensure_node_local_directory(
    root: str | os.PathLike,
    *,
    mode: int = 0o700,
    enforce_mode: bool = False,
    shared_roots: tuple[Path, ...] | None = None,
) -> Path:
    """Create a node-local directory without following an ancestor symlink.

    ``Path.mkdir(parents=True)`` is not a safe bootstrap primitive: an existing
    intermediate symlink can redirect the first write to shared storage before
    the completed path is validated.  Walk from ``/`` with directory file
    descriptors, open every existing component with ``O_NOFOLLOW``, and create
    each missing component relative to its already-verified parent.  The final
    pathname must still name the exact inode reached by the descriptor walk.
    """

    requested = Path(root)
    if (
        not requested.is_absolute()
        or ".." in requested.parts
        or "\x00" in os.fspath(requested)
        or requested == Path(requested.anchor)
    ):
        raise ValueError(f"node-local directory must be an absolute non-root path: {requested}")
    if not hasattr(os, "O_NOFOLLOW"):
        raise RuntimeError("node-local directory creation requires O_NOFOLLOW")
    declared_shared = shared_roots if shared_roots is not None else configured_shared_roots()
    normalized = Path(os.path.normpath(requested))
    if any(_is_relative_to(normalized, shared) for shared in declared_shared):
        raise ValueError(f"node-local directory is beneath shared storage: {normalized}")
    shared_devices: set[int] = set()
    for shared in declared_shared:
        try:
            shared_devices.add(os.stat(shared).st_dev)
        except OSError:
            continue

    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    current_fd = os.open(requested.anchor, flags)
    current_path = Path(requested.anchor)
    created_final = False
    try:
        for index, part in enumerate(requested.parts[1:]):
            current_path /= part
            if any(_is_relative_to(current_path, shared) for shared in declared_shared):
                raise ValueError(
                    f"node-local directory component is beneath shared storage: {current_path}"
                )
            created = False
            try:
                next_fd = os.open(part, flags, dir_fd=current_fd)
            except FileNotFoundError:
                parent_metadata = os.fstat(current_fd)
                parent_realpath = Path(os.path.realpath(f"/proc/self/fd/{current_fd}"))
                if parent_metadata.st_dev in shared_devices or any(
                    _is_relative_to(parent_realpath, shared) for shared in declared_shared
                ):
                    raise ValueError(
                        "refusing to create a node-local directory beneath shared storage: "
                        f"{current_path}"
                    )
                try:
                    os.mkdir(part, mode=mode, dir_fd=current_fd)
                    created = True
                except FileExistsError:
                    # A concurrent creator is acceptable only if the no-follow
                    # open below proves that it installed a real directory.
                    pass
                try:
                    next_fd = os.open(part, flags, dir_fd=current_fd)
                except OSError as exc:
                    raise ValueError(
                        f"node-local directory component is unsafe: {current_path}: {exc}"
                    ) from exc
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    raise ValueError(
                        f"node-local directory contains a symlink/non-directory: {current_path}"
                    ) from exc
                raise ValueError(
                    f"node-local directory component is unavailable: {current_path}: {exc}"
                ) from exc

            metadata = os.fstat(next_fd)
            descriptor_path = Path(os.path.realpath(f"/proc/self/fd/{next_fd}"))
            if not stat.S_ISDIR(metadata.st_mode):
                os.close(next_fd)
                raise ValueError(
                    f"node-local directory component is not a directory: {current_path}"
                )
            if metadata.st_dev in shared_devices or any(
                _is_relative_to(descriptor_path, shared) for shared in declared_shared
            ):
                os.close(next_fd)
                raise ValueError(
                    f"node-local directory component resolves beneath shared storage: {current_path}"
                )
            os.close(current_fd)
            current_fd = next_fd
            created_final = created and index == len(requested.parts[1:]) - 1

        metadata = os.fstat(current_fd)
        if metadata.st_uid != os.getuid():
            raise ValueError(f"node-local directory is not owned by this uid: {requested}")
        if created_final or enforce_mode:
            os.fchmod(current_fd, mode)
        try:
            pathname_metadata = os.lstat(requested)
        except OSError as exc:
            raise ValueError(
                f"node-local directory was replaced during creation: {requested}"
            ) from exc
        if stat.S_ISLNK(pathname_metadata.st_mode) or (
            pathname_metadata.st_dev,
            pathname_metadata.st_ino,
        ) != (metadata.st_dev, metadata.st_ino):
            raise ValueError(f"node-local directory was replaced during creation: {requested}")
    finally:
        os.close(current_fd)
    return validate_node_local_root(requested, shared_roots=declared_shared)


def chmod_real_directory(path: str | os.PathLike, mode: int) -> bool:
    """Apply a mode to one real directory inode without following a symlink.

    Return ``False`` for a symlink so cleanup walkers can unlink it without
    ever changing its target.  Descriptor-based ``fchmod`` is required because
    Aurora's Python does not implement ``chmod(..., follow_symlinks=False)``.
    """

    if type(mode) is not int or not 0 <= mode <= 0o7777:
        raise ValueError("directory mode must be an integer permission mask")
    metadata = os.lstat(path)
    if stat.S_ISLNK(metadata.st_mode):
        return False
    if not stat.S_ISDIR(metadata.st_mode):
        raise NotADirectoryError(os.fspath(path))
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    descriptor = os.open(path, flags)
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (metadata.st_dev, metadata.st_ino):
            raise OSError(f"directory changed before chmod: {path}")
        os.fchmod(descriptor, mode)
    finally:
        os.close(descriptor)
    return True


def validate_node_local_tree(
    path: str | os.PathLike,
    *,
    local_root: str | os.PathLike,
    shared_roots: tuple[Path, ...] | None = None,
    require_immutable: bool = False,
) -> Path:
    """Reject symlink, mount, special-file, and shared-root escapes in a tree."""

    root = validate_node_local_root(local_root, shared_roots=shared_roots)
    requested = Path(path)
    try:
        metadata = os.lstat(requested)
    except OSError as exc:
        raise ValueError(f"node-local candidate is unavailable: {requested}: {exc}") from exc
    if not stat.S_ISDIR(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode):
        raise ValueError(f"node-local candidate must be a real directory: {requested}")
    if require_immutable and stat.S_IMODE(metadata.st_mode) & 0o222:
        raise ValueError(f"node-local immutable tree root is writable: {requested}")
    resolved = Path(os.path.realpath(requested))
    if not _is_relative_to(resolved, root):
        raise ValueError(f"node-local candidate escapes its declared root: {resolved}")
    from .state.mounts import MountBoundaryError, nested_mount_points

    try:
        nested_mounts = nested_mount_points(root)
    except MountBoundaryError as exc:
        raise ValueError(str(exc)) from exc
    for current, directories, files in os.walk(resolved, topdown=True, followlinks=False):
        current_path = Path(current)
        if current_path in nested_mounts:
            raise ValueError(f"node-local tree crosses a filesystem boundary: {current_path}")
        for name in [*directories, *files]:
            entry = current_path / name
            entry_metadata = os.lstat(entry)
            if stat.S_ISLNK(entry_metadata.st_mode):
                raise ValueError(f"node-local tree contains a symlink escape: {entry}")
            if require_immutable and stat.S_IMODE(entry_metadata.st_mode) & 0o222:
                raise ValueError(f"node-local immutable tree entry is writable: {entry}")
            if entry in nested_mounts:
                raise ValueError(f"node-local tree crosses a filesystem boundary: {entry}")
            if not (stat.S_ISDIR(entry_metadata.st_mode) or stat.S_ISREG(entry_metadata.st_mode)):
                raise ValueError(f"node-local tree contains an unsupported entry: {entry}")
            if not _is_relative_to(Path(os.path.realpath(entry)), root):
                raise ValueError(f"node-local tree entry escapes its declared root: {entry}")
    return resolved


def content_addressed_model_path(
    model_id: str, storage_path: str | os.PathLike, manifest_hash: str
) -> Path:
    """Return the immutable local cache identity for one exact model manifest."""

    if not isinstance(manifest_hash, str) or not _SHA256.fullmatch(manifest_hash):
        raise ValueError("model manifest hash must be lowercase SHA-256")
    return Path(storage_path) / f"{get_model_storage_name(model_id)}.{manifest_hash}"


def _safe_manifest_path(value: object) -> bool:
    if not isinstance(value, str) or not value or "\\" in value:
        return False
    path = PurePosixPath(value)
    return not path.is_absolute() and all(part not in {"", ".", ".."} for part in path.parts)


def validate_model_manifest(value: object, *, allow_legacy_sampled: bool = False) -> dict:
    """Validate the v2 contract; production identities require full hashes."""
    if not isinstance(value, dict) or set(value) != _MANIFEST_FIELDS:
        raise ValueError("model completion manifest fields are invalid")
    if type(value["version"]) is not int or value["version"] != MODEL_MANIFEST_VERSION:
        raise ValueError("model completion manifest version is unsupported")
    if value["kind"] not in {"full_model", "tokenizer_only"}:
        raise ValueError("model completion manifest kind is invalid")
    if not isinstance(value["source_identity"], str) or not value["source_identity"]:
        raise ValueError("model completion manifest source_identity is invalid")
    for field in ("file_count", "total_bytes"):
        if type(value[field]) is not int or value[field] < 0:
            raise ValueError(f"model completion manifest {field} is invalid")
    if not isinstance(value["manifest_hash"], str) or not _SHA256.fullmatch(value["manifest_hash"]):
        raise ValueError("model completion manifest hash is invalid")
    files = value["files"]
    if not isinstance(files, list) or not files:
        raise ValueError("model completion manifest files must be a nonempty list")
    paths: list[str] = []
    total_bytes = 0
    for index, entry in enumerate(files):
        if not isinstance(entry, dict) or set(entry) != _MANIFEST_FILE_FIELDS:
            raise ValueError(f"model completion manifest file {index} fields are invalid")
        if not _safe_manifest_path(entry["path"]):
            raise ValueError(f"model completion manifest file {index} path is unsafe")
        if type(entry["size"]) is not int or entry["size"] < 0:
            raise ValueError(f"model completion manifest file {index} size is invalid")
        allowed_hash_kinds = {"sha256-full"}
        if allow_legacy_sampled:
            allowed_hash_kinds.add(f"sha256-first-last-{_SAMPLE_BYTES}")
        if entry["hash_kind"] not in allowed_hash_kinds:
            raise ValueError(f"model completion manifest file {index} hash_kind is invalid")
        if not isinstance(entry["sha256"], str) or not _SHA256.fullmatch(entry["sha256"]):
            raise ValueError(f"model completion manifest file {index} hash is invalid")
        paths.append(entry["path"])
        total_bytes += entry["size"]
    if len(paths) != len(set(paths)) or paths != sorted(paths):
        raise ValueError("model completion manifest file paths must be unique and sorted")
    if value["file_count"] != len(files) or value["total_bytes"] != total_bytes:
        raise ValueError("model completion manifest counts disagree with its files")
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if hashlib.sha256(canonical.encode()).hexdigest() != value["manifest_hash"]:
        raise ValueError("model completion manifest hash disagrees with its files")
    return value


def _open_model_input(path: Path):
    """Open a model input, following HF blob symlinks but rejecting special files."""
    handle = path.open("rb")
    try:
        metadata = os.fstat(handle.fileno())
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"model input is not a regular file: {path}")
        return handle, metadata.st_size
    except BaseException:
        handle.close()
        raise


def _model_repository_root(model_path: Path) -> Path:
    resolved = model_path.resolve(strict=True)
    if resolved.parent.name == "snapshots":
        return resolved.parent.parent
    return resolved


def _qualified_inventory_input(
    model_path: Path, path: Path, *, allowed_symlink_root: Path | None = None
) -> Path:
    """Allow HF blob links only while they remain inside one model repository."""

    try:
        resolved = path.resolve(strict=True)
        own_root = _model_repository_root(model_path)
        allowed = _model_repository_root(allowed_symlink_root) if allowed_symlink_root else None
        metadata = os.stat(resolved)
    except (OSError, RuntimeError) as exc:
        raise ValueError(f"model inventory entry cannot be resolved safely: {path}: {exc}") from exc
    if not stat.S_ISREG(metadata.st_mode) or not (
        _is_relative_to(resolved, own_root) or (allowed and _is_relative_to(resolved, allowed))
    ):
        raise ValueError(f"model inventory entry escapes its repository or is not regular: {path}")
    return resolved


def _model_tree_signature(model_path: Path) -> tuple:
    entries = []
    for path in sorted(model_path.rglob("*"), key=lambda item: item.as_posix()):
        if path == model_path / COMPLETION_MARKER:
            continue
        metadata = os.lstat(path)
        relative = path.relative_to(model_path).as_posix()
        if stat.S_ISDIR(metadata.st_mode):
            entries.append(
                (relative, "dir", metadata.st_dev, metadata.st_ino, metadata.st_mtime_ns)
            )
            continue
        qualified = _qualified_inventory_input(model_path, path)
        observed = os.stat(qualified)
        entries.append(
            (
                relative,
                "file",
                observed.st_dev,
                observed.st_ino,
                observed.st_size,
                observed.st_mtime_ns,
                observed.st_ctime_ns,
            )
        )
    return tuple(entries)


def _remember_verified_manifest(model_path: Path, manifest: dict) -> None:
    _VERIFIED_MANIFEST_CACHE[str(model_path.resolve())] = (
        _model_tree_signature(model_path),
        manifest,
    )


def verified_model_manifest(model_path: Path) -> dict | None:
    """Return this process's full-hash proof verified manifest if still unchanged."""

    try:
        cached = _VERIFIED_MANIFEST_CACHE.get(str(model_path.resolve()))
        if cached is None or cached[0] != _model_tree_signature(model_path):
            return None
        return cached[1]
    except (OSError, ValueError):
        return None


def _hash_inventory_file(path: Path) -> tuple[str, str, int]:
    """Hash every byte; sampled hashes cannot identify immutable model content."""
    digest = hashlib.sha256()
    handle, size = _open_model_input(path)
    with handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
    return "sha256-full", digest.hexdigest(), size


def _legacy_sample_hash(path: Path, hash_kind: str) -> tuple[str, int]:
    digest = hashlib.sha256()
    handle, size = _open_model_input(path)
    with handle:
        if hash_kind == f"sha256-first-last-{_SAMPLE_BYTES}" and size > 2 * _SAMPLE_BYTES:
            digest.update(handle.read(_SAMPLE_BYTES))
            handle.seek(-_SAMPLE_BYTES, os.SEEK_END)
            digest.update(handle.read(_SAMPLE_BYTES))
        else:
            for chunk in iter(lambda: handle.read(1 << 20), b""):
                digest.update(chunk)
    return digest.hexdigest(), size


def _legacy_sampled_manifest_matches(model_path: Path, manifest: dict) -> bool:
    try:
        for entry in manifest["files"]:
            source = _qualified_inventory_input(model_path, model_path / entry["path"])
            digest, size = _legacy_sample_hash(source, entry["hash_kind"])
            if digest != entry["sha256"] or size != entry["size"]:
                return False
    except (OSError, ValueError):
        return False
    return True


def build_model_manifest(
    model_path: Path,
    *,
    tokenizer_only: bool = False,
    source_identity: str = "",
    allowed_symlink_root: Path | None = None,
) -> dict:
    """Complete recursive inventory with full-file content identity."""
    if not isinstance(tokenizer_only, bool):
        raise TypeError("tokenizer_only must be a boolean")
    if not isinstance(source_identity, str):
        raise TypeError("source_identity must be text")
    entries = []
    for path in sorted(model_path.rglob("*"), key=lambda item: item.as_posix()):
        if path == model_path / COMPLETION_MARKER:
            continue
        metadata = os.lstat(path)
        if stat.S_ISDIR(metadata.st_mode):
            continue
        if not (stat.S_ISREG(metadata.st_mode) or stat.S_ISLNK(metadata.st_mode)):
            raise ValueError(f"model inventory contains an unsupported entry: {path}")
        qualified = _qualified_inventory_input(
            model_path, path, allowed_symlink_root=allowed_symlink_root
        )
        hash_kind, digest, size = _hash_inventory_file(qualified)
        entries.append(
            {
                "path": path.relative_to(model_path).as_posix(),
                "size": size,
                "hash_kind": hash_kind,
                "sha256": digest,
            }
        )
    return build_model_manifest_from_files(
        entries,
        tokenizer_only=tokenizer_only,
        source_identity=source_identity or str(model_path.resolve()),
    )


def build_model_manifest_from_files(
    entries: list[dict], *, tokenizer_only: bool = False, source_identity: str
) -> dict:
    """Build a strict full-hash manifest from already verified file entries."""

    if not isinstance(entries, list) or not entries:
        raise ValueError("model manifest entries must be a nonempty list")
    if any(entry.get("hash_kind") != "sha256-full" for entry in entries):
        raise ValueError("content-addressed model manifests require full-file SHA-256")
    entries = sorted((dict(entry) for entry in entries), key=lambda item: item["path"])
    canonical = json.dumps(entries, sort_keys=True, separators=(",", ":"))
    manifest = {
        "version": MODEL_MANIFEST_VERSION,
        "kind": "tokenizer_only" if tokenizer_only else "full_model",
        "source_identity": source_identity,
        "file_count": len(entries),
        "total_bytes": sum(item["size"] for item in entries),
        "manifest_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        "files": entries,
    }
    return validate_model_manifest(manifest)


def _validate_model_dir(model_path: Path) -> tuple[bool, str]:
    """PR-005: structural completeness check.

    Requires config.json and at least one weight file, and — when a
    safetensors/pytorch shard index is present — every shard the index
    references. This catches the "interrupted after shard k of N" case that
    the old any-one-weight-file check classified as complete.
    """
    if not (model_path / "config.json").is_file():
        return False, "missing config.json"

    names = {p.name for p in model_path.iterdir() if p.is_file()}
    weights = [n for n in names if n.endswith((".safetensors", ".bin", ".pt"))]
    if not weights:
        return False, "no weight files"

    for index_name in ("model.safetensors.index.json", "pytorch_model.bin.index.json"):
        index_path = model_path / index_name
        if index_path.is_file():
            try:
                handle, _size = _open_model_input(index_path)
                with handle:
                    index_payload = strict_json_load(handle)
            except (ValueError, OSError) as exc:
                return False, f"unreadable {index_name}: {exc}"
            if not isinstance(index_payload, dict):
                return False, f"{index_name} must be a JSON object"
            weight_map = index_payload.get("weight_map")
            if (
                not isinstance(weight_map, dict)
                or not weight_map
                or any(
                    not isinstance(key, str) or not key or not _safe_manifest_path(shard)
                    for key, shard in weight_map.items()
                )
            ):
                return False, f"{index_name} has an invalid weight_map"
            shards = set(weight_map.values())
            missing = sorted(s for s in shards if s not in names)
            if missing:
                return (
                    False,
                    f"{index_name} references {len(missing)} missing shard(s): {missing[:3]}",
                )
    return True, "ok"


def check_model_exists(model_path: Path) -> bool:
    """
    Check if a model already exists and appears complete.

    Prefers an explicit completion marker (written last after a validated
    download). For pre-existing directories staged before markers existed,
    falls back to structural validation and upgrades the marker in place so
    subsequent checks are cheap and unambiguous.
    """
    if not model_path.exists():
        return False
    # IMP-B05: the marker's EXISTENCE is not proof. Verify its recorded
    # inventory against what is actually on disk (every file present with the
    # recorded size) — deleting a weight file after the marker was written
    # previously still reported "complete". A marker that fails this check is
    # stale/corrupt and the directory is treated as incomplete.
    marker = model_path / COMPLETION_MARKER
    if marker.is_file():
        try:
            manifest = strict_json_load_path(marker)
            if isinstance(manifest, dict) and manifest.get("version") == MODEL_MANIFEST_VERSION:
                validate_model_manifest(manifest, allow_legacy_sampled=True)
                if manifest["kind"] == "tokenizer_only":
                    return False  # tokenizer staging never certifies a full model
                observed = build_model_manifest(
                    model_path, source_identity=manifest["source_identity"]
                )
                exact = (
                    observed["manifest_hash"] == manifest["manifest_hash"]
                    and observed["file_count"] == manifest["file_count"]
                    and observed["total_bytes"] == manifest["total_bytes"]
                    and observed["files"] == manifest["files"]
                )
                if exact:
                    _remember_verified_manifest(model_path, observed)
                    return True
                sampled = any(entry["hash_kind"] != "sha256-full" for entry in manifest["files"])
                if not sampled or not _legacy_sampled_manifest_matches(model_path, manifest):
                    return False
                if (
                    [entry["path"] for entry in observed["files"]]
                    != [entry["path"] for entry in manifest["files"]]
                    or observed["file_count"] != manifest["file_count"]
                    or observed["total_bytes"] != manifest["total_bytes"]
                ):
                    return False
                # Safe migration: the legacy boundary still matches every
                # declared sample/size/path, and ``observed`` now binds every
                # byte. Read-only sources remain usable through the run-owned
                # overlay even when their marker cannot be upgraded in place.
                try:
                    from .state.atomic import atomic_write_json

                    atomic_write_json(marker, observed)
                except OSError:
                    pass
                _remember_verified_manifest(model_path, observed)
                return True
            # Version-1 marker migration: validate the recorded flat sizes,
            # then structural content, and atomically upgrade below.
            if not isinstance(manifest, dict) or type(manifest.get("version")) is not int:
                return False
            if manifest["version"] != 1:
                return False
            files = manifest.get("files")
            if not isinstance(files, dict):
                return False
            for name, size in files.items():
                if not _safe_manifest_path(name) or type(size) is not int or size < 0:
                    return False
                candidate = model_path / name
                if not candidate.is_file() or candidate.stat().st_size != size:
                    return False
            complete, _reason = _validate_model_dir(model_path)
            if not complete:
                return False
            write_completion_marker(model_path)
            return True
        except (OSError, ValueError, TypeError):
            return False

    complete, _reason = _validate_model_dir(model_path)
    if complete:
        # Upgrade a legacy-complete directory to a marker-bearing one.
        try:
            write_completion_marker(model_path)
        except OSError:
            pass  # read-only store: still complete, just not upgradeable
    return complete


def write_completion_marker(
    model_path: Path,
    *,
    tokenizer_only: bool = False,
    source_identity: str = "",
    allowed_symlink_root: Path | None = None,
) -> dict:
    from exaserve.state.atomic import atomic_write_json

    manifest = build_model_manifest(
        model_path,
        tokenizer_only=tokenizer_only,
        source_identity=source_identity,
        allowed_symlink_root=allowed_symlink_root,
    )
    if allowed_symlink_root is None:
        _remember_verified_manifest(model_path, manifest)
    atomic_write_json(model_path / COMPLETION_MARKER, manifest)
    return manifest


def get_model_dir_state(model_path: Path) -> str:
    """
    Return 'missing', 'partial', or 'complete' for a model directory.
    """
    if not model_path.exists():
        return "missing"
    return "complete" if check_model_exists(model_path) else "partial"


def _resolve_hf_cache_snapshot(cache_dir: Path) -> Path | None:
    """
    Resolve a usable snapshot directory from a Hugging Face cache directory.
    """
    refs_main = cache_dir / "refs" / "main"
    snapshot_id = None
    if refs_main.is_file():
        handle, _size = _open_model_input(refs_main)
        with handle:
            snapshot_id = handle.read().decode("utf-8").strip()

    snapshots_dir = cache_dir / "snapshots"
    if snapshot_id:
        snapshot_dir = snapshots_dir / snapshot_id
        if snapshot_dir.is_dir():
            return snapshot_dir

    if snapshots_dir.is_dir():
        candidates = [p for p in snapshots_dir.iterdir() if p.is_dir()]
        if len(candidates) == 1:
            return candidates[0]
        if candidates:
            raise RuntimeError(
                f"Hugging Face cache {cache_dir} has {len(candidates)} snapshots "
                "but no valid refs/main; refusing to select a model revision by mtime"
            )
    return None


def resolve_existing_model_path(model_id: str, storage_path: str) -> Path | None:
    """
    Resolve an already-downloaded model directory under `storage_path`.

    Supports both the repo's flat cache layout (`org--name`) and the Hugging
    Face shared cache layout (`models--org--name/snapshots/<rev>`), including a
    nested `hub/` directory when present.
    """
    base_path = Path(storage_path)
    flat_dir = get_model_storage_path(model_id, base_path)
    if get_model_dir_state(flat_dir) == "complete":
        return flat_dir

    hf_cache_name = f"models--{get_model_storage_name(model_id)}"
    cache_roots = [base_path, base_path / "hub"]
    for root in cache_roots:
        cache_dir = root / hf_cache_name
        if not cache_dir.is_dir():
            continue
        snapshot_dir = _resolve_hf_cache_snapshot(cache_dir)
        if snapshot_dir and get_model_dir_state(snapshot_dir) == "complete":
            return snapshot_dir
    return None


def load_model_config(model_path: Path) -> dict:
    """
    Load the Hugging Face `config.json` for a resolved model directory.
    """
    config_path = model_path / "config.json"
    if not config_path.is_file():
        raise FileNotFoundError(f"Missing config.json for model at {model_path}")
    handle, _size = _open_model_input(config_path)
    with handle:
        payload = strict_json_load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"config.json for model at {model_path} must be a JSON object")
    return payload


def validate_tensor_parallel_compatibility(
    model_id: str,
    model_path: Path,
    tensor_parallel_size: int,
) -> None:
    """
    Fail fast when the model architecture cannot support the requested TP size.

    This catches obvious incompatibilities before we spend time staging the
    model to node-local storage and before Ray/vLLM startup.
    """
    config_data = load_model_config(model_path)
    num_attention_heads = config_data.get("num_attention_heads")
    if num_attention_heads is None:
        return
    if type(num_attention_heads) is not int or num_attention_heads < 1:
        raise RuntimeError(
            f"[ModelStaging] Model {model_id} at {model_path} declares invalid "
            f"num_attention_heads={num_attention_heads!r}."
        )
    if type(tensor_parallel_size) is not int or tensor_parallel_size < 1:
        raise RuntimeError(f"invalid tensor_parallel_size={tensor_parallel_size!r}")
    if num_attention_heads % tensor_parallel_size != 0:
        raise RuntimeError(
            f"[ModelStaging] Model {model_id} at {model_path} is incompatible with "
            f"tensor_parallel_size={tensor_parallel_size}: num_attention_heads="
            f"{num_attention_heads} is not divisible by {tensor_parallel_size}."
        )


def validate_vllm_modelinfo_seed_coverage(
    model_id: str,
    model_path: Path,
    supported_architectures: frozenset[str],
) -> tuple[str, ...]:
    """Reject a real vLLM model whose architecture lacks reviewed cache data.

    This runs only in the allocation-head staging process, before model bytes
    are broadcast. Workers receive the resulting model tree via MPI and never
    inspect the shared source path themselves.
    """

    if not isinstance(supported_architectures, frozenset) or any(
        not isinstance(value, str) or not value for value in supported_architectures
    ):
        raise TypeError("supported vLLM architectures must be a frozenset of text")
    config_data = load_model_config(model_path)
    architectures = config_data.get("architectures")
    if (
        not isinstance(architectures, list)
        or not architectures
        or any(not isinstance(value, str) or not value for value in architectures)
        or len(architectures) != len(set(architectures))
    ):
        raise RuntimeError(
            f"[ModelStaging] Model {model_id} at {model_path} must declare a unique, "
            "non-empty architectures list for VC-01"
        )
    unsupported = sorted(set(architectures) - supported_architectures)
    if unsupported:
        raise RuntimeError(
            f"[ModelStaging] Model {model_id} requires unreviewed vLLM model-info "
            f"architecture(s) {unsupported}; refusing a cache-miss subprocess before broadcast"
        )
    return tuple(architectures)


def download_model(model_id: str, local_path: Path, tokenizer_only: bool = False) -> str:
    """
    Download a model from HuggingFace to the specified local path.

    Args:
        model_id: HuggingFace model ID (e.g., "meta-llama/Meta-Llama-3-8B-Instruct")
        local_path: Local path where the model should be stored
        tokenizer_only: If True, only download tokenizer files

    Returns:
        str: Path to the downloaded model
    """
    print(f"[ModelStaging] Downloading {model_id} to {local_path}...", flush=True)
    start_time = time.time()

    from exaserve.state.atomic import (
        ExclusiveLease,
        LeaseHeartbeat,
        LeaseHeldError,
        fsync_directory,
    )

    try:
        from huggingface_hub import HfApi, snapshot_download

        local_path.parent.mkdir(parents=True, exist_ok=True)
        allow_patterns = ["*.json", "*.txt", "*.model", "tokenizer*"] if tokenizer_only else None

        # PR-005: transactional staging. One lease per model prevents
        # concurrent stagers from racing into the same tree; the download
        # lands in a sibling staging dir, is validated, marked complete, and
        # only then atomically published to the final path.
        lease_path = local_path.parent / f".{local_path.name}.download.lease"
        try:
            lease = ExclusiveLease(
                lease_path, ttl_s=7200, owner_note=f"download {model_id}"
            ).acquire()
        except LeaseHeldError:
            print(f"[ModelStaging] Another stager holds {model_id}; waiting...", flush=True)
            # Wait for the winner; then use its result if complete.
            deadline = time.time() + 7200
            while time.time() < deadline:
                time.sleep(5)
                if check_model_exists(local_path):
                    print(f"[ModelStaging] ✓ {model_id} completed by another stager", flush=True)
                    return str(local_path)
            raise RuntimeError(f"timed out waiting for concurrent download of {model_id}")

        try:
            if check_model_exists(local_path):  # re-check under lease
                return str(local_path)
            staging = local_path.parent / (
                f".{local_path.name}.staging.{os.getpid()}.{time.time_ns()}"
            )
            try:
                revision = str(HfApi().model_info(model_id).sha or "").strip()
                if not revision:
                    raise RuntimeError(
                        f"Hugging Face returned no immutable revision for {model_id}"
                    )
                with LeaseHeartbeat(lease, interval_s=60.0) as heartbeat:
                    snapshot_download(
                        repo_id=model_id,
                        revision=revision,
                        local_dir=str(staging),
                        local_dir_use_symlinks=False,
                        allow_patterns=allow_patterns,
                    )
                    heartbeat.ensure_held()
                    if not tokenizer_only:
                        complete, reason = _validate_model_dir(staging)
                        if not complete:
                            raise RuntimeError(f"downloaded {model_id} failed validation: {reason}")
                    write_completion_marker(
                        staging,
                        tokenizer_only=tokenizer_only,
                        source_identity=f"hf:{model_id}@{revision}",
                    )
                    heartbeat.ensure_held()
                    # Preserve a pre-existing partial tree for diagnosis.  The
                    # final name is absent only between two same-filesystem
                    # renames, and no reader can mistake either candidate for a
                    # completed final artifact.
                    quarantine = None
                    if local_path.exists():
                        quarantine = local_path.with_name(
                            f".{local_path.name}.invalid.{os.getpid()}.{time.time_ns()}"
                        )
                        os.rename(local_path, quarantine)
                    try:
                        os.replace(staging, local_path)
                        fsync_directory(local_path.parent)
                    except BaseException as publish_exc:
                        if quarantine is not None and not local_path.exists():
                            try:
                                os.rename(quarantine, local_path)
                            except OSError as rollback_exc:
                                add_exception_note(
                                    publish_exc,
                                    f"model publication rollback also failed: {rollback_exc}",
                                )
                        raise
            except BaseException as exc:
                try:
                    shutil.rmtree(staging)
                except FileNotFoundError:
                    pass
                except OSError as cleanup_exc:
                    add_exception_note(exc, f"model staging cleanup also failed: {cleanup_exc}")
                raise
        finally:
            active_error = sys.exc_info()[1]
            try:
                lease.release()
            except BaseException as release_exc:
                if active_error is None:
                    raise
                add_exception_note(
                    active_error, f"model download lease cleanup also failed: {release_exc}"
                )

        elapsed = time.time() - start_time
        print_red(f"[ModelStaging] ✓ Downloaded {model_id} in {elapsed:.2f}s")
        return str(local_path)
    except Exception as e:
        elapsed = time.time() - start_time
        print_red(f"[ModelStaging] ✗ Failed to download {model_id} after {elapsed:.2f}s: {e}")
        raise


def stage_models(model_configs: List[ModelPlan], storage_path: str) -> dict:
    """
    Stage all models specified in model_configs to the storage path.
    Downloads models if they don't already exist locally.

    Args:
        model_configs: List of ModelConfig objects
        storage_path: Base path for storing models (e.g., lustre path)

    Returns:
        dict: Mapping of model_id to local path
    """
    storage_path = Path(storage_path)
    storage_path.mkdir(parents=True, exist_ok=True)

    model_paths: Dict[str, str] = {}
    unique_models = list(iter_unique_model_ids(model_configs))

    print(
        f"[ModelStaging] Staging {len(unique_models)} unique model(s) to {storage_path}", flush=True
    )
    total_start = time.time()

    for model_id in unique_models:
        existing_path = resolve_existing_model_path(model_id, str(storage_path))
        local_path = existing_path or get_model_storage_path(model_id, storage_path)
        state = (
            "complete"
            if existing_path is not None and verified_model_manifest(local_path) is not None
            else get_model_dir_state(local_path)
        )

        if state == "complete":
            print(f"[ModelStaging] ✓ Model {model_id} already exists at {local_path}", flush=True)
            model_paths[model_id] = str(local_path)
        elif state == "partial":
            raise RuntimeError(
                f"[ModelStaging] Found partial/corrupt model directory for {model_id} at {local_path}. "
                "Refusing to overwrite it automatically."
            )
        else:
            print(f"[ModelStaging] Model {model_id} not found, downloading...", flush=True)
            try:
                downloaded_path = download_model(model_id, local_path)
                model_paths[model_id] = downloaded_path
            except Exception as e:
                print(f"[ModelStaging] Failed to stage {model_id}: {e}", flush=True)
                raise

    total_elapsed = time.time() - total_start
    print_red(f"[ModelStaging] ✓ All models staged in {total_elapsed:.2f}s")

    return model_paths
