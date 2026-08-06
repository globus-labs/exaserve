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
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

SCHEMA_VERSION = 1

# Bound every control message (plan §3.2 "strict maximum message size").
MAX_MESSAGE_BYTES = 1 << 20  # 1 MiB


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
    if data.get("schema_version") != SCHEMA_VERSION:
        raise ContractError(RejectReason.UNKNOWN_SCHEMA.value)
    try:
        obs = ComponentObservation(**data)
    except TypeError as exc:
        raise ContractError(f"{RejectReason.MALFORMED.value}: {exc}") from exc

    try:
        scope = OwnerScope(obs.owner_scope)
    except ValueError as exc:
        raise ContractError(f"{RejectReason.MALFORMED.value}: owner_scope") from exc
    try:
        ComponentState(obs.state)
    except ValueError as exc:
        raise ContractError(f"{RejectReason.MALFORMED.value}: state") from exc

    # plan §3.1: schema rejects every other owner_scope/owner_rank combination
    if scope is OwnerScope.GLOBAL and obs.owner_rank is not None:
        raise ContractError(f"{RejectReason.MALFORMED.value}: GLOBAL owner_rank must be null")
    if scope is OwnerScope.RANK and (
        not isinstance(obs.owner_rank, int) or obs.owner_rank < 0
    ):
        raise ContractError(f"{RejectReason.MALFORMED.value}: RANK needs owner_rank >= 0")
    if obs.sequence < 0 or obs.generation < 0:
        raise ContractError(f"{RejectReason.MALFORMED.value}: negative counter")
    return obs


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
    body = json.dumps(env.to_dict(), sort_keys=True, separators=(",", ":")).encode()
    if len(body) > MAX_MESSAGE_BYTES:
        raise ContractError(RejectReason.OVERSIZED.value)
    return body


def decode_envelope(body: bytes) -> Envelope:
    if len(body) > MAX_MESSAGE_BYTES:
        raise ContractError(RejectReason.OVERSIZED.value)
    try:
        data = json.loads(body.decode())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractError(RejectReason.MALFORMED.value) from exc
    if not isinstance(data, dict):
        raise ContractError(RejectReason.MALFORMED.value)
    if data.get("v") != SCHEMA_VERSION:
        raise ContractError(RejectReason.UNKNOWN_SCHEMA.value)
    try:
        env = Envelope(**data)
    except TypeError as exc:
        raise ContractError(f"{RejectReason.MALFORMED.value}: {exc}") from exc
    try:
        EnvelopeKind(env.kind)
    except ValueError as exc:
        raise ContractError(RejectReason.UNKNOWN_KIND.value) from exc
    return env
