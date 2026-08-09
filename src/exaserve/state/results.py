"""Immutable, checksum-backed run result completeness manifests (WP2/WP9).

The manifest is deliberately portable: entry paths are relative to the
directory containing the manifest.  A result bundle may therefore be moved or
archived without invalidating its identity.  Verification is also a security
boundary.  Absolute paths, ``..`` traversal, symlinks, special files, and
shape/type coercion are rejected before any content is trusted.
"""

from __future__ import annotations

import hashlib
import os
import re
from dataclasses import asdict, dataclass, replace
from datetime import datetime

from ..plan.contracts import canonical_hash
from .atomic import atomic_create_json, regular_file_reader, strict_json_load_path

SCHEMA_VERSION = 2
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_LOGICAL_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")


class ResultManifestError(ValueError):
    pass


def _regular_file_identity(path: str) -> tuple[int, str]:
    digest = hashlib.sha256()
    with regular_file_reader(path, binary=True) as handle:
        size = os.fstat(handle.fileno()).st_size
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return size, digest.hexdigest()


def file_sha256(path: str) -> str:
    return _regular_file_identity(path)[1]


@dataclass(frozen=True)
class ResultEntry:
    logical_id: str
    path: str
    size_bytes: int
    sha256: str

    def __post_init__(self) -> None:
        if not isinstance(self.logical_id, str) or not _LOGICAL_ID.fullmatch(self.logical_id):
            raise ResultManifestError(f"invalid result logical ID {self.logical_id!r}")
        if not isinstance(self.path, str) or not self.path:
            raise ResultManifestError("result entry path must be non-empty text")
        normalized = os.path.normpath(self.path)
        if (
            os.path.isabs(self.path)
            or normalized in ("", ".", "..")
            or normalized.startswith(".." + os.sep)
            or normalized != self.path
        ):
            raise ResultManifestError(
                f"result entry path must be normalized and relative: {self.path!r}"
            )
        if (
            isinstance(self.size_bytes, bool)
            or not isinstance(self.size_bytes, int)
            or self.size_bytes < 0
        ):
            raise ResultManifestError("result entry size_bytes must be a nonnegative integer")
        if not isinstance(self.sha256, str) or not _SHA256.fullmatch(self.sha256):
            raise ResultManifestError("result entry sha256 must be SHA-256")

    @classmethod
    def from_file(cls, logical_id: str, path: str, *, root: str) -> "ResultEntry":
        """Build an entry for a regular, non-symlink file inside ``root``."""
        absolute = os.path.abspath(path)
        root = os.path.abspath(root)
        try:
            common = os.path.commonpath((root, absolute))
        except ValueError as exc:
            raise ResultManifestError(
                f"result {logical_id!r} is on a different filesystem root"
            ) from exc
        if common != root:
            raise ResultManifestError(f"result {logical_id!r} escapes the result bundle")
        relative = os.path.relpath(absolute, root)
        try:
            size, digest = _regular_file_identity(absolute)
        except (OSError, ValueError) as exc:
            raise ResultManifestError(
                f"result {logical_id!r} must be an available regular, non-symlink file: {exc}"
            ) from exc
        return cls(
            logical_id=logical_id,
            path=relative,
            size_bytes=size,
            sha256=digest,
        )


@dataclass(frozen=True)
class ResultManifest:
    schema_version: int
    run_id: str
    run_semantic_hash: str
    deployment_plan_hash: str
    expected_ids: tuple[str, ...]
    entries: tuple[ResultEntry, ...]
    incomplete_reasons: tuple[str, ...]
    generated_at: str
    complete: bool
    manifest_hash: str = ""

    def __post_init__(self) -> None:
        if type(self.schema_version) is not int or self.schema_version != SCHEMA_VERSION:
            raise ResultManifestError("unsupported result manifest schema")
        if not isinstance(self.run_id, str) or not self.run_id:
            raise ResultManifestError("result manifest identity is incomplete")
        for name in ("run_semantic_hash", "deployment_plan_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not _SHA256.fullmatch(value):
                raise ResultManifestError(f"{name} must be SHA-256")
        if not isinstance(self.generated_at, str) or not self.generated_at:
            raise ResultManifestError("generated_at must be an ISO-8601 timestamp")
        try:
            parsed = datetime.fromisoformat(self.generated_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ResultManifestError("generated_at must be an ISO-8601 timestamp") from exc
        if parsed.tzinfo is None:
            raise ResultManifestError("generated_at must include a timezone")
        if not isinstance(self.complete, bool):
            raise ResultManifestError("complete must be a boolean")
        if not isinstance(self.manifest_hash, str) or (
            self.manifest_hash and not _SHA256.fullmatch(self.manifest_hash)
        ):
            raise ResultManifestError("manifest_hash must be empty or SHA-256")
        if not isinstance(self.expected_ids, (tuple, list)) or any(
            not isinstance(item, str) or not _LOGICAL_ID.fullmatch(item)
            for item in self.expected_ids
        ):
            raise ResultManifestError("expected_ids must contain valid logical IDs")
        if not isinstance(self.entries, (tuple, list)) or any(
            not isinstance(item, ResultEntry) for item in self.entries
        ):
            raise ResultManifestError("entries must contain ResultEntry values")
        if not isinstance(self.incomplete_reasons, (tuple, list)) or any(
            not isinstance(item, str) or not item.strip() for item in self.incomplete_reasons
        ):
            raise ResultManifestError("incomplete_reasons must contain non-empty strings")
        object.__setattr__(self, "expected_ids", tuple(self.expected_ids))
        object.__setattr__(self, "entries", tuple(self.entries))
        object.__setattr__(self, "incomplete_reasons", tuple(self.incomplete_reasons))
        if list(self.expected_ids) != sorted(set(self.expected_ids)):
            raise ResultManifestError("expected_ids must be sorted and unique")
        ids = [entry.logical_id for entry in self.entries]
        if ids != sorted(set(ids)):
            raise ResultManifestError("result entries must be sorted and have unique logical IDs")
        if list(self.incomplete_reasons) != sorted(set(self.incomplete_reasons)):
            raise ResultManifestError("incomplete_reasons must be sorted and unique")
        exact = set(ids) == set(self.expected_ids)
        if self.complete != (exact and not self.incomplete_reasons):
            raise ResultManifestError("complete flag disagrees with expected entries/reasons")

    def canonical(self) -> dict:
        payload = asdict(self)
        payload.pop("manifest_hash", None)
        return payload

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "ResultManifest":
        return replace(self, manifest_hash=self.compute_hash())


def write_result_manifest(path: str, manifest: ResultManifest) -> None:
    if manifest.manifest_hash != manifest.compute_hash():
        raise ResultManifestError("refusing unfinalized result manifest")
    # A completion manifest is immutable.  Idempotent publication of the same
    # object is allowed; silently replacing it with a different object would
    # make a previously consumed success change meaning after the fact.
    try:
        atomic_create_json(path, asdict(manifest))
        return
    except FileExistsError:
        existing = load_result_manifest(path, verify_files=False)
        if existing.manifest_hash == manifest.manifest_hash:
            return
        raise ResultManifestError("refusing to replace an immutable result manifest")


def load_result_manifest(path: str, *, verify_files: bool = True) -> ResultManifest:
    import json

    try:
        raw = strict_json_load_path(path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise ResultManifestError(f"could not read result manifest: {exc}") from exc
    expected = set(ResultManifest.__dataclass_fields__)
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ResultManifestError("result manifest shape mismatch")
    try:
        entries = tuple(ResultEntry(**entry) for entry in raw["entries"])
        manifest = ResultManifest(
            **{
                **raw,
                "expected_ids": tuple(raw["expected_ids"]),
                "entries": entries,
                "incomplete_reasons": tuple(raw["incomplete_reasons"]),
            }
        )
    except (TypeError, ValueError, KeyError) as exc:
        raise ResultManifestError(f"invalid result manifest: {exc}") from exc
    if manifest.manifest_hash != manifest.compute_hash():
        raise ResultManifestError("result manifest hash mismatch")
    if verify_files:
        root = os.path.abspath(os.path.dirname(path))
        for entry in manifest.entries:
            candidate = os.path.abspath(os.path.join(root, entry.path))
            try:
                if os.path.commonpath((root, candidate)) != root:
                    raise ResultManifestError(
                        f"result {entry.logical_id} escapes the result bundle"
                    )
                size, digest = _regular_file_identity(candidate)
            except (OSError, ValueError) as exc:
                raise ResultManifestError(f"result {entry.logical_id} unavailable: {exc}") from exc
            if size != entry.size_bytes or digest != entry.sha256:
                raise ResultManifestError(f"result {entry.logical_id} content mismatch")
    return manifest


def complete_result_manifest(path: str) -> bool:
    try:
        return load_result_manifest(path).complete
    except ResultManifestError:
        return False
