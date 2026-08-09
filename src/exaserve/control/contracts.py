"""Typed control-plane contracts (plan §3.1).

Pure data layer: lifecycle enums, component observations, protocol envelopes,
canonical serialization, and schema-version validation. This module must not
import Ray, engines, or transport internals, and it performs no I/O.

Design constraints enforced here (normative, plan §3.1–§3.2):
- Component states are a closed set; arbitrary strings are rejected.
- ``owner_rank`` must be absent for GLOBAL owners and present (>= 0) for RANK
  owners; every other combination is a validation error.
- Envelope kinds are exactly the seven protocol families.
- Serialization is canonical JSON (sorted keys, no whitespace variance) so a
  MAC over the encoded bytes is stable.
- Deduplication key: (deployment_id, generation, component_id, instance_id,
  sequence). Receivers reject unknown schema versions, wrong plan hashes,
  stale generations/instances, and sequence regressions (transport enforces
  the connection-level parts; ``validate_observation`` the shape).
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

SCHEMA_VERSION = 1

# Bound every control message (plan §3.2 "strict maximum message size").
MAX_MESSAGE_BYTES = 1 << 20  # 1 MiB
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class ContractError(ValueError):
    """A message or observation violates the control contract."""


class ComponentState(str, Enum):
    NEW = "NEW"
    STARTING = "STARTING"
    RUNNING = "RUNNING"
    READY = "READY"
    STOPPING = "STOPPING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class OwnerScope(str, Enum):
    GLOBAL = "GLOBAL"
    RANK = "RANK"


class EnvelopeKind(str, Enum):
    REGISTER = "REGISTER"
    OBSERVATION = "OBSERVATION"
    COMMAND = "COMMAND"
    COMMAND_RESULT = "COMMAND_RESULT"
    SNAPSHOT = "SNAPSHOT"
    RECEIPT = "RECEIPT"  # one exact v2 compatibility receipt
    HEARTBEAT = "HEARTBEAT"
    GOODBYE = "GOODBYE"


# Machine-readable rejection/failure reasons (audited fail-closed paths).
class RejectReason(str, Enum):
    BAD_MAC = "BAD_MAC"
    OVERSIZED = "OVERSIZED"
    UNKNOWN_SCHEMA = "UNKNOWN_SCHEMA"
    UNKNOWN_KIND = "UNKNOWN_KIND"
    WRONG_DEPLOYMENT = "WRONG_DEPLOYMENT"
    WRONG_PLAN_HASH = "WRONG_PLAN_HASH"
    STALE_GENERATION = "STALE_GENERATION"
    STALE_INSTANCE = "STALE_INSTANCE"
    SEQUENCE_REGRESSION = "SEQUENCE_REGRESSION"
    RANK_MISMATCH = "RANK_MISMATCH"
    NODE_MISMATCH = "NODE_MISMATCH"
    DUPLICATE_RANK = "DUPLICATE_RANK"
    NOT_REGISTERED = "NOT_REGISTERED"
    MALFORMED = "MALFORMED"


@dataclass(frozen=True)
class ComponentObservation:
    """One typed lifecycle observation (plan §3.1 field list)."""

    schema_version: int
    deployment_id: str
    plan_hash: str
    generation: int
    component_id: str
    instance_id: str
    sequence: int
    owner_scope: str
    role: str
    node_id: str
    state: str
    observed_at: float  # sender wall-clock; diagnostic evidence only
    owner_rank: int | None = None
    model_id: str | None = None
    replica_id: str | None = None
    reason_code: str | None = None
    detail: str | None = None
    compatibility_receipt_hash: str | None = None

    def dedup_key(self) -> tuple[str, int, str, str, int]:
        return (
            self.deployment_id,
            self.generation,
            self.component_id,
            self.instance_id,
            self.sequence,
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def validate_observation(data: dict[str, Any]) -> ComponentObservation:
    """Validate shape + invariants; raise ContractError with a reason."""

    if not isinstance(data, dict):
        raise ContractError(RejectReason.MALFORMED.value)
    if (
        isinstance(data.get("schema_version"), bool)
        or not isinstance(data.get("schema_version"), int)
        or data.get("schema_version") != SCHEMA_VERSION
    ):
        raise ContractError(RejectReason.UNKNOWN_SCHEMA.value)
    try:
        obs = ComponentObservation(**data)
    except TypeError as exc:
        raise ContractError(f"{RejectReason.MALFORMED.value}: {exc}") from exc

    try:
        scope = OwnerScope(obs.owner_scope)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{RejectReason.MALFORMED.value}: owner_scope") from exc
    try:
        ComponentState(obs.state)
    except (TypeError, ValueError) as exc:
        raise ContractError(f"{RejectReason.MALFORMED.value}: state") from exc

    # plan §3.1: schema rejects every other owner_scope/owner_rank combination
    if scope is OwnerScope.GLOBAL and obs.owner_rank is not None:
        raise ContractError(f"{RejectReason.MALFORMED.value}: GLOBAL owner_rank must be null")
    if scope is OwnerScope.RANK and (
        isinstance(obs.owner_rank, bool)
        or not isinstance(obs.owner_rank, int)
        or obs.owner_rank < 0
    ):
        raise ContractError(f"{RejectReason.MALFORMED.value}: RANK needs owner_rank >= 0")
    for name in (
        "deployment_id",
        "plan_hash",
        "component_id",
        "instance_id",
        "role",
        "node_id",
    ):
        if not isinstance(getattr(obs, name), str) or not getattr(obs, name):
            raise ContractError(f"{RejectReason.MALFORMED.value}: {name}")
    for name in ("sequence", "generation"):
        value = getattr(obs, name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ContractError(f"{RejectReason.MALFORMED.value}: {name}")
    if (
        isinstance(obs.observed_at, bool)
        or not isinstance(obs.observed_at, (int, float))
        or not math.isfinite(float(obs.observed_at))
        or obs.observed_at < 0
    ):
        raise ContractError(f"{RejectReason.MALFORMED.value}: observed_at")
    for name in ("model_id", "replica_id", "reason_code", "detail"):
        value = getattr(obs, name)
        if value is not None and not isinstance(value, str):
            raise ContractError(f"{RejectReason.MALFORMED.value}: {name}")
    if obs.compatibility_receipt_hash is not None and (
        not isinstance(obs.compatibility_receipt_hash, str)
        or not _SHA256_RE.fullmatch(obs.compatibility_receipt_hash)
    ):
        raise ContractError(f"{RejectReason.MALFORMED.value}: compatibility_receipt_hash")
    return obs


def _validate_json_value(value: Any, path: str = "payload") -> None:
    """Reject values that canonical JSON would coerce or serialize ambiguously."""
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ContractError(f"{RejectReason.MALFORMED.value}: {path} is non-finite")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _validate_json_value(item, f"{path}[{index}]")
        return
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str):
                raise ContractError(f"{RejectReason.MALFORMED.value}: {path} has a non-string key")
            _validate_json_value(item, f"{path}.{key}")
        return
    raise ContractError(f"{RejectReason.MALFORMED.value}: {path} contains {type(value).__name__}")


def _validate_envelope(env: Envelope) -> None:
    if isinstance(env.v, bool) or not isinstance(env.v, int) or env.v != SCHEMA_VERSION:
        raise ContractError(RejectReason.UNKNOWN_SCHEMA.value)
    try:
        EnvelopeKind(env.kind)
    except (TypeError, ValueError) as exc:
        raise ContractError(RejectReason.UNKNOWN_KIND.value) from exc
    for name in ("deployment_id", "plan_hash", "sender_node"):
        if not isinstance(getattr(env, name), str) or not getattr(env, name):
            raise ContractError(f"{RejectReason.MALFORMED.value}: {name}")
    for name, minimum in (("generation", 0), ("sender_rank", -1), ("seq", 0)):
        value = getattr(env, name)
        if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
            raise ContractError(f"{RejectReason.MALFORMED.value}: {name}")
    if not isinstance(env.payload, dict):
        raise ContractError(f"{RejectReason.MALFORMED.value}: payload")
    _validate_json_value(env.payload)


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant {value}")


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


@dataclass(frozen=True)
class Envelope:
    """Versioned wire envelope (plan §3.2). Payload semantics vary by kind."""

    v: int
    kind: str
    deployment_id: str
    plan_hash: str
    generation: int
    sender_rank: int  # -1 designates the global supervisor itself
    sender_node: str
    seq: int
    payload: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def encode_envelope(env: Envelope) -> bytes:
    """Canonical JSON bytes (sorted keys, compact separators)."""
    _validate_envelope(env)
    body = json.dumps(
        env.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    if len(body) > MAX_MESSAGE_BYTES:
        raise ContractError(RejectReason.OVERSIZED.value)
    return body


def decode_envelope(body: bytes) -> Envelope:
    if len(body) > MAX_MESSAGE_BYTES:
        raise ContractError(RejectReason.OVERSIZED.value)
    try:
        data = json.loads(
            body.decode(),
            parse_constant=_reject_json_constant,
            object_pairs_hook=_unique_json_object,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ContractError(RejectReason.MALFORMED.value) from exc
    if not isinstance(data, dict):
        raise ContractError(RejectReason.MALFORMED.value)
    try:
        env = Envelope(**data)
    except TypeError as exc:
        raise ContractError(f"{RejectReason.MALFORMED.value}: {exc}") from exc
    _validate_envelope(env)
    return env
