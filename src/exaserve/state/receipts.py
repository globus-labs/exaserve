"""Immutable manifest of the exact compatibility receipts behind READY."""

from __future__ import annotations

import copy
import json
import os
import re
from dataclasses import asdict, dataclass, replace
from typing import Any

from ..compat.receipt_v2 import ReceiptError, receipt_from_dict
from ..plan.contracts import canonical_hash
from .atomic import atomic_create_json, strict_json_load_path

_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ReceiptManifestError(ValueError):
    pass


@dataclass(frozen=True)
class ReceiptManifest:
    schema_version: int
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    allocation_binding_hash: str
    receipt_hashes: tuple[str, ...]
    receipts: tuple[dict[str, Any], ...]
    manifest_hash: str = ""

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ReceiptManifestError("unsupported receipt manifest schema")
        if not isinstance(self.deployment_id, str) or not self.deployment_id:
            raise ReceiptManifestError("receipt manifest identity is incomplete")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 0
        ):
            raise ReceiptManifestError("receipt manifest generation must be non-negative")
        for name in ("deployment_plan_hash", "allocation_binding_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not _SHA256.fullmatch(value):
                raise ReceiptManifestError(f"receipt manifest {name} must be SHA-256")
        if not isinstance(self.manifest_hash, str) or (
            self.manifest_hash and not _SHA256.fullmatch(self.manifest_hash)
        ):
            raise ReceiptManifestError("manifest_hash must be empty or SHA-256")
        if not isinstance(self.receipt_hashes, (tuple, list)) or any(
            not isinstance(item, str) or not _SHA256.fullmatch(item) for item in self.receipt_hashes
        ):
            raise ReceiptManifestError("receipt_hashes must contain SHA-256 values")
        if not isinstance(self.receipts, (tuple, list)) or any(
            not isinstance(item, dict) for item in self.receipts
        ):
            raise ReceiptManifestError("receipts must contain JSON objects")
        object.__setattr__(self, "receipt_hashes", tuple(self.receipt_hashes))
        object.__setattr__(self, "receipts", tuple(copy.deepcopy(item) for item in self.receipts))
        parsed = []
        try:
            for payload in self.receipts:
                receipt = receipt_from_dict(payload)
                if receipt.receipt_hash != receipt.compute_hash():
                    raise ReceiptManifestError("receipt content hash mismatch")
                parsed.append(receipt)
        except ReceiptError as exc:
            raise ReceiptManifestError(f"invalid embedded receipt: {exc}") from exc
        hashes = tuple(sorted(item.receipt_hash for item in parsed))
        if hashes != self.receipt_hashes:
            raise ReceiptManifestError(
                "receipt_hashes are not the exact sorted embedded receipt set"
            )
        slots = [item.receipt_requirement_id for item in parsed]
        if len(slots) != len(set(slots)):
            raise ReceiptManifestError("receipt manifest has duplicate planned slots")
        for item in parsed:
            if (
                item.deployment_id != self.deployment_id
                or item.generation != self.generation
                or item.deployment_plan_hash != self.deployment_plan_hash
                or item.allocation_binding_hash != self.allocation_binding_hash
            ):
                raise ReceiptManifestError("embedded receipt identity drift")

    def canonical(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.pop("manifest_hash", None)
        return payload

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "ReceiptManifest":
        return replace(self, manifest_hash=self.compute_hash())


def write_receipt_manifest(path: str, manifest: ReceiptManifest) -> None:
    if manifest.manifest_hash != manifest.compute_hash():
        raise ReceiptManifestError("refusing unfinalized receipt manifest")
    os.makedirs(os.path.dirname(os.path.abspath(path)), mode=0o700, exist_ok=True)
    try:
        atomic_create_json(path, asdict(manifest))
        return
    except FileExistsError:
        existing = load_receipt_manifest(path)
        if existing.manifest_hash == manifest.manifest_hash:
            return
        raise ReceiptManifestError("refusing to replace an immutable receipt manifest")


def load_receipt_manifest(path: str) -> ReceiptManifest:
    try:
        raw = strict_json_load_path(path)
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise ReceiptManifestError(f"could not read receipt manifest: {exc}") from exc
    expected = set(ReceiptManifest.__dataclass_fields__)
    if not isinstance(raw, dict) or set(raw) != expected:
        raise ReceiptManifestError("receipt manifest shape mismatch")
    try:
        manifest = ReceiptManifest(**raw)
    except (TypeError, ValueError, KeyError) as exc:
        raise ReceiptManifestError(f"invalid receipt manifest: {exc}") from exc
    if manifest.manifest_hash != manifest.compute_hash():
        raise ReceiptManifestError("receipt manifest hash mismatch")
    return manifest
