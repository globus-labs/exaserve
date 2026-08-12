"""Authenticated control transport (plan §3.2).

Framing: ``4-byte big-endian length | 32-byte HMAC-SHA256 | body`` where the
MAC is computed over the canonical-JSON body with the 256-bit per-deployment
secret. Length covers MAC+body and is bounded by MAX_MESSAGE_BYTES.

This module owns framing, session authentication, registration identity,
sequence/duplicate enforcement, snapshot-on-reconnect bookkeeping, and
rejection auditing. It contains NO readiness policy: validated observations
are handed to a caller-supplied sink; readiness projection lives in
``control.readiness`` (WP5).

Fail-closed rule (plan §3.2): unknown versions, bad authentication, wrong
deployment/plan hash/generation, rank/node disagreement, oversized messages,
and sequence regression terminate the offending session and are audited.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import math
import re
import secrets as _secrets
import struct
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Mapping, Sequence

from .contracts import (
    MAX_MESSAGE_BYTES,
    OwnerScope,
    SCHEMA_VERSION,
    ComponentObservation,
    ContractError,
    Envelope,
    EnvelopeKind,
    RejectReason,
    decode_envelope,
    encode_envelope,
    validate_observation,
)

_LEN = struct.Struct(">I")
# IMP-B06: bound listener memory (dedup keys retained per session).
_MAX_DEDUP_KEYS = 4096
_MAX_AUDIT_RECORDS = 1000
# Command identities are authoritative idempotency records.  Evicting old IDs
# would allow a delayed duplicate START/STOP to execute twice, so both ends
# retain a bounded deployment-lifetime set and fail closed before accepting a
# novel identity after the cap.  Pending commands are non-authoritative queue
# entries and have a smaller backpressure bound.
_MAX_COMMAND_RECORDS = 4096
_MAX_PENDING_HEAD_COMMANDS = 256
_MAX_COMPONENTS_PER_RANK = 64
_MAC_BYTES = 32
SUPERVISOR_RANK = -1
_SHA256_HEX = re.compile(r"[0-9a-f]{64}\Z")
_RESULT_OPERATIONS = {
    "START",
    "START_HEAD",
    "START_WORKER",
    "DRAIN",
    "STOP",
    "SNAPSHOT_ACCEPTED",
}


def _observation_fingerprint(observation: ComponentObservation) -> str:
    import json

    body = json.dumps(
        observation.to_dict(), sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def _is_payload_version_one(payload: Mapping[str, Any]) -> bool:
    value = payload.get("payload_version")
    return type(value) is int and value == 1


def _validate_command_result_payload(payload: object) -> dict[str, Any]:
    """Validate a rank-to-head result without JSON-type coercion."""
    if not isinstance(payload, dict) or not _is_payload_version_one(payload):
        raise ContractError("invalid COMMAND_RESULT payload/version")
    operation = payload.get("operation")
    expected_fields = {
        "payload_version",
        "command_id",
        "operation",
        "ok",
        "detail",
        "status",
    }
    if operation == "SNAPSHOT_ACCEPTED":
        expected_fields.update({"snapshot_id", "complete_set_hash"})
    if set(payload) != expected_fields:
        raise ContractError("COMMAND_RESULT fields do not match its operation contract")
    command_id = payload.get("command_id")
    if not isinstance(command_id, str) or not command_id:
        raise ContractError("COMMAND_RESULT command_id must be a nonempty string")
    if operation not in _RESULT_OPERATIONS:
        raise ContractError("COMMAND_RESULT operation is unsupported")
    if type(payload.get("ok")) is not bool:
        raise ContractError("COMMAND_RESULT ok must be a boolean")
    if not isinstance(payload.get("detail"), str):
        raise ContractError("COMMAND_RESULT detail must be a string")
    status = payload.get("status")
    if status not in {"SUCCEEDED", "FAILED"}:
        raise ContractError("COMMAND_RESULT status is unsupported")
    if (status == "SUCCEEDED") is not payload["ok"]:
        raise ContractError("COMMAND_RESULT status and ok disagree")
    if operation == "SNAPSHOT_ACCEPTED":
        snapshot_id = payload.get("snapshot_id")
        complete_hash = payload.get("complete_set_hash")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ContractError("snapshot result snapshot_id must be a nonempty string")
        if not isinstance(complete_hash, str) or _SHA256_HEX.fullmatch(complete_hash) is None:
            raise ContractError("snapshot result complete_set_hash must be lowercase SHA-256")
    return payload


def _validate_command_payload(payload: object) -> dict[str, Any]:
    if not isinstance(payload, dict) or not _is_payload_version_one(payload):
        raise ContractError("unknown COMMAND payload_version")
    command_id = payload.get("command_id")
    operation = payload.get("operation")
    if not isinstance(command_id, str) or not command_id:
        raise ContractError("COMMAND command_id must be a nonempty string")
    if not isinstance(operation, str) or not operation:
        raise ContractError("COMMAND operation must be a nonempty string")
    base_fields = {"payload_version", "command_id", "operation"}
    expected_fields = (
        base_fields | {"snapshot_id", "complete_set_hash"}
        if operation == "SNAPSHOT_ACCEPTED"
        else base_fields
    )
    if set(payload) != expected_fields:
        raise ContractError("COMMAND fields do not match its operation contract")
    if operation == "SNAPSHOT_ACCEPTED":
        snapshot_id = payload.get("snapshot_id")
        complete_hash = payload.get("complete_set_hash")
        if not isinstance(snapshot_id, str) or not snapshot_id:
            raise ContractError("snapshot command snapshot_id must be a nonempty string")
        if not isinstance(complete_hash, str) or _SHA256_HEX.fullmatch(complete_hash) is None:
            raise ContractError("snapshot command hash must be lowercase SHA-256")
    return payload


def canonical_snapshot_items(items: list) -> list[dict]:
    """Collapse byte-identical duplicates, reject conflicts, and sort exactly."""
    import json

    unique: dict[tuple, tuple[str, dict]] = {}
    for item in items:
        if not isinstance(item, Mapping) or set(item) != {"kind", "body"}:
            raise ContractError("snapshot item must contain exactly kind/body")
        kind = item.get("kind")
        body = item.get("body")
        if kind not in {"observation", "receipt"} or not isinstance(body, Mapping):
            raise ContractError("snapshot item kind/body is invalid")
        if kind == "observation":
            fields = (body.get("component_id"), body.get("instance_id"))
        else:
            fields = (
                body.get("receipt_requirement_id"),
                body.get("component_id"),
                body.get("instance_id"),
            )
        if any(not isinstance(part, str) or not part for part in fields):
            raise ContractError(f"snapshot {kind} key is incomplete")
        key = (kind, *fields)
        normalized = {"kind": kind, "body": dict(body)}
        try:
            encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError) as exc:
            raise ContractError(f"snapshot item is not canonical JSON: {exc}") from exc
        existing = unique.get(key)
        if existing is not None and existing[0] != encoded:
            raise ContractError(f"conflicting duplicate snapshot key {key}")
        unique[key] = (encoded, normalized)
    return [unique[key][1] for key in sorted(unique)]


def snapshot_set_hash(items: list) -> str:
    import json

    canonical = canonical_snapshot_items(items)
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def new_deployment_secret() -> bytes:
    """256-bit per-deployment authentication secret (plan §3.2)."""
    return _secrets.token_bytes(32)


def _mac(secret: bytes, body: bytes) -> bytes:
    return hmac.new(secret, body, hashlib.sha256).digest()


async def write_frame(
    writer: asyncio.StreamWriter,
    secret: bytes,
    env: Envelope,
    *,
    max_frame_bytes: int = MAX_MESSAGE_BYTES,
) -> None:
    body = encode_envelope(env)
    if len(body) > max_frame_bytes:
        raise ContractError(RejectReason.OVERSIZED.value)
    frame = _mac(secret, body) + body
    writer.write(_LEN.pack(len(frame)) + frame)
    await writer.drain()


async def read_frame(
    reader: asyncio.StreamReader, secret: bytes, *, max_frame_bytes: int = MAX_MESSAGE_BYTES
) -> Envelope:
    """Read one authenticated envelope; raise ContractError on violation."""
    header = await reader.readexactly(_LEN.size)
    (length,) = _LEN.unpack(header)
    if length > max_frame_bytes + _MAC_BYTES or length < _MAC_BYTES:
        raise ContractError(RejectReason.OVERSIZED.value)
    frame = await reader.readexactly(length)
    mac, body = frame[:_MAC_BYTES], frame[_MAC_BYTES:]
    if not hmac.compare_digest(mac, _mac(secret, body)):
        raise ContractError(RejectReason.BAD_MAC.value)
    return decode_envelope(body)


@dataclass
class SessionInfo:
    rank: int
    node_id: str
    registered: bool = False
    established: bool = False
    instance_id: str = ""
    pending_snapshot_id: str = ""
    pending_snapshot_hash: str = ""
    pending_snapshot_command_id: str = ""
    expected_goodbye: bool = False
    last_seq: int = -1
    # dedup memory for at-least-once observation delivery. IMP-B06: bounded —
    # keep only the most recent keys so listener state stays O(planned
    # components) rather than O(events) over a long deployment.
    seen_obs: "OrderedDict[tuple[str, int, str, str, int], str]" = field(
        default_factory=OrderedDict
    )
    # highest sequence seen per (component_id, instance_id)
    last_component_seq: dict[tuple[str, str], int] = field(default_factory=dict)
    # One current instance per component.  Instance changes cross only through
    # a complete replacement snapshot; ordinary incrementals from a retired or
    # invented instance fail closed as STALE_INSTANCE.
    component_instances: dict[str, str] = field(default_factory=dict)
    last_seen_at: float = 0.0  # receiver-side heartbeat/lease timestamp

    def remember(self, key, fingerprint: str) -> bool:
        """Record a dedup key; reject a same-key payload conflict."""
        existing = self.seen_obs.get(key)
        if existing is not None:
            if existing != fingerprint:
                raise ContractError("conflicting duplicate observation payload")
            return False
        self.seen_obs[key] = fingerprint
        while len(self.seen_obs) > _MAX_DEDUP_KEYS:
            self.seen_obs.popitem(last=False)
        return True


@dataclass
class AuditRecord:
    reason: str
    detail: str
    rank: int | None = None
    node_id: str | None = None


@dataclass(frozen=True)
class _DefaultLimits:
    """Transport-only defaults used by isolated tests and tools.

    Production always passes the immutable plan's ``ControlLimits``. Keeping a
    local default avoids importing plan policy into the transport module.
    """

    registration_deadline_s: float = 300.0
    heartbeat_interval_s: float = 5.0
    reconnect_grace_s: float = 60.0
    lease_timeout_s: float = 30.0
    snapshot_assembly_deadline_s: float = 60.0
    watchdog_cleanup_deadline_s: float = 120.0
    max_frame_bytes: int = MAX_MESSAGE_BYTES
    max_snapshot_chunks: int = 256
    max_snapshot_bytes: int = 64 << 20
    max_snapshot_items: int = 65536


class ControlListener:
    """Allocation-head listener owned by the RuntimeSupervisor.

    Callers provide:
    - ``expected_ranks``: exact planned rank count (plan §3.2: rank count must
      equal planned node count; hostnames unique).
    - ``on_observation(rank, ComponentObservation)`` sink.
    - ``on_session_change(rank, connected: bool)`` for lease/readiness removal.
    """

    def __init__(
        self,
        *,
        deployment_id: str,
        plan_hash: str,
        generation: int,
        expected_ranks: int,
        secret: bytes,
        on_observation: Callable[[int, ComponentObservation], Awaitable[None] | None],
        on_session_change: Callable[[int, bool], Awaitable[None] | None] | None = None,
        host: str = "0.0.0.0",
        on_receipt: Callable[[int, dict], Awaitable[None] | None] | None = None,
        on_register: Callable[[int, str, str], Any] | None = None,
        on_snapshot: Callable[[int, str, str, list], Any] | None = None,
        on_snapshot_ack: Callable[[int, dict], Any] | None = None,
        on_protocol_violation: Callable[[int, str], Any] | None = None,
        on_heartbeat: Callable[[int], Any] | None = None,
        expected_nodes: Mapping[int, str] | None = None,
        limits: object | None = None,
    ) -> None:
        for name, value in (("deployment_id", deployment_id), ("plan_hash", plan_hash)):
            if not isinstance(value, str) or not value:
                raise ValueError(f"control listener {name} must be non-empty text")
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise ValueError("control listener generation must be a non-negative integer")
        if (
            isinstance(expected_ranks, bool)
            or not isinstance(expected_ranks, int)
            or expected_ranks < 1
        ):
            raise ValueError("control listener expected_ranks must be a positive integer")
        if not isinstance(secret, bytes) or len(secret) != 32:
            raise ValueError("control listener secret must be exactly 256 bits")
        if not callable(on_observation):
            raise ValueError("control listener observation sink must be callable")
        for name, callback in (
            ("on_session_change", on_session_change),
            ("on_receipt", on_receipt),
            ("on_register", on_register),
            ("on_snapshot", on_snapshot),
            ("on_snapshot_ack", on_snapshot_ack),
            ("on_protocol_violation", on_protocol_violation),
            ("on_heartbeat", on_heartbeat),
        ):
            if callback is not None and not callable(callback):
                raise ValueError(f"control listener {name} must be null or callable")
        if not isinstance(host, str) or not host:
            raise ValueError("control listener host must be non-empty text")
        if expected_nodes is not None and not isinstance(expected_nodes, Mapping):
            raise ValueError("control listener expected_nodes must be a mapping or null")
        for rank, node in (expected_nodes or {}).items():
            if (
                isinstance(rank, bool)
                or not isinstance(rank, int)
                or not 0 <= rank < expected_ranks
                or not isinstance(node, str)
                or not node
            ):
                raise ValueError("control listener expected_nodes contains an invalid binding")
        self._on_receipt = on_receipt
        self.deployment_id = deployment_id
        self.plan_hash = plan_hash
        self.generation = generation
        self.expected_ranks = expected_ranks
        self._secret = secret
        self._on_observation = on_observation
        self._on_session_change = on_session_change
        self._on_register = on_register
        self._on_snapshot = on_snapshot
        self._on_snapshot_ack = on_snapshot_ack
        self._on_protocol_violation = on_protocol_violation
        self._on_heartbeat = on_heartbeat
        self._host = host
        self._limits = limits or _DefaultLimits()
        self._expected_nodes = dict(expected_nodes or {})
        self._server: asyncio.AbstractServer | None = None
        self.port: int | None = None
        self.sessions: dict[int, SessionInfo] = {}
        # Writer per authenticated session, so the head can PUSH commands.
        # Without this, START was computed and never delivered.
        self._writers: dict[int, Any] = {}
        self._command_results: dict[str, dict] = {}
        self._command_result_events: dict[str, asyncio.Event] = {}
        # Exact command request indexed by its stable id.  A result is
        # authoritative only when it comes from the rank to which this exact
        # request was issued.  Retrying the same request is idempotent;
        # reusing an id for a different rank or payload is a supervisor bug.
        self._issued_commands: dict[str, tuple[int, dict[str, Any]]] = {}
        self._snapshots: dict[int, dict] = {}
        self._nodes_by_rank: dict[int, str] = {}
        self.audit: list[AuditRecord] = []
        self._registered_event: dict[int, asyncio.Event] = {}
        self._all_authenticated = asyncio.Event()
        self._all_established = asyncio.Event()
        self._out_seq: dict[int, int] = {}
        self._handler_tasks: set[asyncio.Task] = set()
        # Track every accepted socket, including connections which have not yet
        # authenticated.  ``asyncio.Server.close()`` only stops accepting new
        # sockets; it deliberately leaves existing clients open.  Without this
        # set an idle or malicious pre-REGISTER client could keep its handler
        # alive past listener teardown.
        self._connection_writers: set[asyncio.StreamWriter] = set()
        self._received_goodbyes: set[int] = set()
        self._goodbye_lock = threading.Lock()
        self._handler_failure_lock = threading.Lock()
        self._handler_failures: list[str] = []

    def _remember_issued_command(
        self, command_id: str, request: tuple[int, dict[str, Any]]
    ) -> None:
        issued = self._issued_commands.get(command_id)
        if issued is not None:
            if issued != request:
                raise ContractError(f"command_id {command_id!r} was already issued differently")
            return
        if len(self._issued_commands) >= _MAX_COMMAND_RECORDS:
            raise ContractError(
                "control command identity capacity exhausted; refusing a novel command"
            )
        self._issued_commands[command_id] = request

    # -- lifecycle ---------------------------------------------------------
    async def start(self) -> None:
        # Bind an ephemeral port before rank launch (plan §3.2 step 2).
        self._server = await asyncio.start_server(self._handle, self._host, 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        server = self._server
        if server is not None:
            # On Python 3.12+, wait_closed() waits for accepted connections too.
            # Stop accepting first, but do not wait until the client transports
            # and their handlers below have been closed.
            server.close()
            self._server = None
        for writer in list(self._connection_writers):
            try:
                writer.close()
            except RuntimeError:
                # Loop teardown may race an already-cancelled connection task;
                # all authoritative session state has been cleared above.
                pass
        tasks = [task for task in self._handler_tasks if task is not asyncio.current_task()]
        # Closing a transport normally wakes readexactly(), but cancellation is
        # also required for protocol callbacks or transport implementations
        # which do not unblock promptly.  Each handler's finally block still
        # revokes its authenticated session before the task completes.
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if server is not None:
            await server.wait_closed()
        self._writers.clear()
        self._connection_writers.clear()

    async def wait_all_registered(self, timeout: float) -> bool:
        """Wait for the full snapshot/receipt/ACK barrier, not bare sockets."""
        try:
            await asyncio.wait_for(self._all_established.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def wait_all_established(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._all_established.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def wait_all_authenticated(self, timeout: float) -> bool:
        """Transport diagnostic only; this is never a START/readiness gate."""
        try:
            await asyncio.wait_for(self._all_authenticated.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    def is_established(self, rank: int) -> bool:
        session = self.sessions.get(rank)
        return bool(session and session.registered and session.established)

    # -- internals ---------------------------------------------------------
    def _audit(
        self, reason: str, detail: str, rank: int | None = None, node_id: str | None = None
    ) -> None:
        self.audit.append(AuditRecord(reason=reason, detail=detail, rank=rank, node_id=node_id))
        if len(self.audit) > _MAX_AUDIT_RECORDS:  # IMP-B06: bounded
            del self.audit[: len(self.audit) - _MAX_AUDIT_RECORDS]

    async def _emit_session_change(self, rank: int, connected: bool) -> None:
        if self._on_session_change is None:
            return
        result = self._on_session_change(rank, connected)
        if asyncio.iscoroutine(result):
            await result

    async def _call_decision(
        self, callback, *args, default: tuple[bool, str] = (True, "accepted")
    ) -> tuple[bool, str]:
        if callback is None:
            return default
        result = callback(*args)
        if asyncio.iscoroutine(result):
            result = await result
        if isinstance(result, tuple) and len(result) == 2:
            accepted, detail = result
            if type(accepted) is not bool or not isinstance(detail, str):
                raise ContractError("control decision callback returned an invalid result")
            return accepted, detail
        if type(result) is bool:
            return result, "accepted" if result else "rejected"
        if result is None:
            return default
        raise ContractError("control decision callback returned an unsupported result")

    async def _protocol_violation(
        self, session: SessionInfo | None, reason: str, detail: str
    ) -> None:
        """Audit and escalate only after a planned rank authenticated."""
        rank = None if session is None else session.rank
        node = None if session is None else session.node_id
        self._audit(reason, detail, rank=rank, node_id=node)
        if session is None or not session.registered or self._on_protocol_violation is None:
            return
        result = self._on_protocol_violation(session.rank, f"{reason}: {detail}")
        if asyncio.iscoroutine(result):
            await result

    def _refresh_established_event(self) -> None:
        if all(self.is_established(rank) for rank in range(self.expected_ranks)):
            self._all_established.set()
        else:
            self._all_established.clear()

    def _next_out_seq(self, rank: int) -> int:
        seq = self._out_seq.get(rank, 0) + 1
        self._out_seq[rank] = seq
        return seq

    async def _write_session_frame(
        self, writer: asyncio.StreamWriter, envelope: Envelope
    ) -> bool:
        """Write to one rank, treating socket loss as a recoverable disconnect.

        EOF/reset has the same session semantics in either direction.  A read
        reset was already handled locally, but a reset raised by ``drain()``
        escaped to the listener-wide handler-failure boundary and made one
        transient rank loss generation-fatal before reconnect grace applied.
        Keep callback/contract exceptions visible while containing only
        transport I/O loss here.
        """
        try:
            await write_frame(
                writer,
                self._secret,
                envelope,
                max_frame_bytes=self._limits.max_frame_bytes,
            )
            return True
        except (ConnectionError, OSError):
            return False

    def _check_scope(self, env: Envelope) -> str | None:
        if env.deployment_id != self.deployment_id:
            return RejectReason.WRONG_DEPLOYMENT.value
        if env.plan_hash != self.plan_hash:
            return RejectReason.WRONG_PLAN_HASH.value
        if env.generation != self.generation:
            return RejectReason.STALE_GENERATION.value
        return None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        task = asyncio.current_task()
        if task is not None:
            self._handler_tasks.add(task)
        self._connection_writers.add(writer)
        session: SessionInfo | None = None
        try:
            while True:
                try:
                    env = await read_frame(
                        reader, self._secret, max_frame_bytes=self._limits.max_frame_bytes
                    )
                except ContractError as exc:
                    self._audit(
                        str(exc), "frame rejected", rank=None if session is None else session.rank
                    )
                    break  # fail closed: terminate session
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break

                reason = self._check_scope(env)
                if reason is not None:
                    self._audit(
                        reason, f"kind={env.kind}", rank=env.sender_rank, node_id=env.sender_node
                    )
                    break

                if env.kind == EnvelopeKind.REGISTER.value:
                    if session is not None and session.registered:
                        await self._protocol_violation(
                            session,
                            RejectReason.DUPLICATE_RANK.value,
                            "REGISTER repeated on an authenticated connection",
                        )
                        break
                    registered_session, err = self._register(env)
                    if err is not None:
                        self._audit(
                            err,
                            "registration rejected",
                            rank=env.sender_rank,
                            node_id=env.sender_node,
                        )
                        break
                    session = registered_session
                    ok, detail = await self._call_decision(
                        self._on_register, session.rank, session.node_id, session.instance_id
                    )
                    if not ok:
                        self._audit(
                            RejectReason.NODE_MISMATCH.value,
                            detail,
                            rank=session.rank,
                            node_id=session.node_id,
                        )
                        session.registered = False
                        break
                    if not await self._write_session_frame(
                        writer,
                        Envelope(
                            v=SCHEMA_VERSION,
                            kind=EnvelopeKind.COMMAND_RESULT.value,
                            deployment_id=self.deployment_id,
                            plan_hash=self.plan_hash,
                            generation=self.generation,
                            sender_rank=SUPERVISOR_RANK,
                            sender_node="head",
                            seq=self._next_out_seq(session.rank),
                            payload={
                                "payload_version": 1,
                                "command_id": "register",
                                "operation": "REGISTER",
                                "ok": True,
                                "control_limits": {
                                    "registration_deadline_s": self._limits.registration_deadline_s,
                                    "heartbeat_interval_s": self._limits.heartbeat_interval_s,
                                    "lease_timeout_s": self._limits.lease_timeout_s,
                                    "reconnect_grace_s": self._limits.reconnect_grace_s,
                                    "snapshot_assembly_deadline_s": self._limits.snapshot_assembly_deadline_s,
                                    "watchdog_cleanup_deadline_s": self._limits.watchdog_cleanup_deadline_s,
                                    "max_frame_bytes": self._limits.max_frame_bytes,
                                    "max_snapshot_chunks": self._limits.max_snapshot_chunks,
                                    "max_snapshot_bytes": self._limits.max_snapshot_bytes,
                                    "max_snapshot_items": self._limits.max_snapshot_items,
                                },
                            },
                        ),
                    ):
                        break
                    self._writers[session.rank] = writer
                    await self._emit_session_change(session.rank, True)
                    continue

                if session is None or not session.registered:
                    self._audit(
                        RejectReason.NOT_REGISTERED.value,
                        f"kind={env.kind}",
                        rank=env.sender_rank,
                        node_id=env.sender_node,
                    )
                    break

                if env.sender_rank != session.rank or env.sender_node != session.node_id:
                    await self._protocol_violation(
                        session,
                        RejectReason.RANK_MISMATCH.value,
                        f"claimed {env.sender_rank}/{env.sender_node}",
                    )
                    break

                if env.seq <= session.last_seq:
                    await self._protocol_violation(
                        session,
                        RejectReason.SEQUENCE_REGRESSION.value,
                        f"seq={env.seq} last={session.last_seq}",
                    )
                    break
                session.last_seq = env.seq

                if env.kind == EnvelopeKind.SNAPSHOT.value:
                    # §3.2.1 chunking: accept the COMPLETE set or nothing. A
                    # partial or mixed snapshot must never become the rank's
                    # projection, so chunks are buffered and verified against
                    # the declared hash before any item is applied.
                    accepted, reason, items = self._assemble_snapshot(session.rank, env.payload)
                    if reason:
                        await self._protocol_violation(
                            session, RejectReason.MALFORMED.value, f"snapshot rejected: {reason}"
                        )
                        return
                    if not accepted:
                        continue  # more chunks owed
                    # `_assemble_snapshot` has validated both as exact strings.
                    snapshot_id = env.payload["snapshot_id"]
                    complete_hash = env.payload["complete_set_hash"]
                    try:
                        observations, receipts = self._validate_snapshot_items(session, items)
                    except ContractError as exc:
                        await self._protocol_violation(
                            session, RejectReason.MALFORMED.value, f"snapshot item rejected: {exc}"
                        )
                        return
                    ok, detail = await self._call_decision(
                        self._on_snapshot, session.rank, snapshot_id, complete_hash, items
                    )
                    if not ok:
                        await self._protocol_violation(
                            session,
                            RejectReason.MALFORMED.value,
                            f"snapshot projection rejected: {detail}",
                        )
                        return
                    # The production callback atomically replaces the rank's
                    # projection.  Isolated transport users without that
                    # callback retain the simple sinks for compatibility.
                    if self._on_snapshot is None:
                        for receipt in receipts:
                            if self._on_receipt is not None:
                                result = self._on_receipt(session.rank, receipt)
                                if asyncio.iscoroutine(result):
                                    await result
                        for obs in observations:
                            result = self._on_observation(session.rank, obs)
                            if asyncio.iscoroutine(result):
                                await result
                    session.last_component_seq = {
                        (obs.component_id, obs.instance_id): obs.sequence for obs in observations
                    }
                    session.component_instances = {
                        obs.component_id: obs.instance_id for obs in observations
                    }
                    session.seen_obs.clear()
                    for obs in observations:
                        session.remember(obs.dedup_key(), _observation_fingerprint(obs))
                    session.pending_snapshot_id = snapshot_id
                    session.pending_snapshot_hash = complete_hash
                    session.pending_snapshot_command_id = (
                        f"SNAPSHOT_ACCEPTED:{session.rank}:{snapshot_id}"
                    )
                    sent = await self._send_command(
                        session.rank,
                        session.pending_snapshot_command_id,
                        "SNAPSHOT_ACCEPTED",
                        {
                            "snapshot_id": snapshot_id,
                            "complete_set_hash": complete_hash,
                        },
                    )
                    if not sent:
                        return
                    payloads = None
                elif env.kind == EnvelopeKind.OBSERVATION.value:
                    if not session.established:
                        await self._protocol_violation(
                            session,
                            RejectReason.NOT_REGISTERED.value,
                            "incremental observation before snapshot ACK barrier",
                        )
                        return
                    if (
                        not isinstance(env.payload, dict)
                        or not _is_payload_version_one(env.payload)
                        or "observation" not in env.payload
                        or not set(env.payload).issubset(
                            {"payload_version", "observation", "compatibility_receipt"}
                        )
                    ):
                        await self._protocol_violation(
                            session,
                            RejectReason.MALFORMED.value,
                            "invalid OBSERVATION payload/version",
                        )
                        return
                    attached = env.payload.get("compatibility_receipt")
                    if attached is not None:
                        try:
                            receipt = self._validate_rank_receipt(session, attached)
                        except ContractError as exc:
                            await self._protocol_violation(
                                session,
                                RejectReason.MALFORMED.value,
                                f"attached receipt rejected: {exc}",
                            )
                            return
                        if self._on_receipt is not None:
                            result = self._on_receipt(session.rank, receipt)
                            if asyncio.iscoroutine(result):
                                await result
                    payloads = [env.payload["observation"]]
                else:
                    payloads = None

                if payloads is not None:
                    for item in payloads:
                        try:
                            obs = validate_observation(item)
                        except ContractError as exc:
                            # IMP-B06: fail CLOSED. A malformed observation on
                            # an authenticated session is a protocol violation,
                            # not something to skip.
                            await self._protocol_violation(
                                session, str(exc), "observation rejected — closing session"
                            )
                            return
                        if (
                            obs.deployment_id != self.deployment_id
                            or obs.plan_hash != self.plan_hash
                            or obs.generation != self.generation
                        ):
                            await self._protocol_violation(
                                session,
                                RejectReason.STALE_GENERATION.value,
                                f"obs {obs.component_id}",
                            )
                            return
                        # IMP-B06: bind observation identity to the
                        # AUTHENTICATED session. Without this an authenticated
                        # rank could publish another rank's state, or forge a
                        # GLOBAL (supervisor-owned) observation such as
                        # "gateway READY".
                        if obs.owner_scope != OwnerScope.RANK.value:
                            await self._protocol_violation(
                                session,
                                RejectReason.RANK_MISMATCH.value,
                                f"rank {session.rank} sent owner_scope={obs.owner_scope} "
                                "(only the supervisor may publish GLOBAL observations)",
                            )
                            return
                        if obs.owner_rank != session.rank:
                            await self._protocol_violation(
                                session,
                                RejectReason.RANK_MISMATCH.value,
                                f"rank {session.rank} sent owner_rank={obs.owner_rank}",
                            )
                            return
                        from ..plan.contracts import same_node

                        if not same_node(obs.node_id, session.node_id):
                            await self._protocol_violation(
                                session,
                                RejectReason.NODE_MISMATCH.value,
                                f"rank {session.rank} ({session.node_id}) sent "
                                f"node_id={obs.node_id}",
                            )
                            return
                        active_instance = session.component_instances.get(obs.component_id)
                        if active_instance is None:
                            if len(session.component_instances) >= _MAX_COMPONENTS_PER_RANK:
                                await self._protocol_violation(
                                    session,
                                    RejectReason.MALFORMED.value,
                                    "rank component identity capacity exhausted",
                                )
                                return
                            session.component_instances[obs.component_id] = obs.instance_id
                        elif active_instance != obs.instance_id:
                            await self._protocol_violation(
                                session,
                                RejectReason.STALE_INSTANCE.value,
                                f"component {obs.component_id!r} active instance "
                                f"{active_instance!r}, received {obs.instance_id!r}",
                            )
                            return
                        # Per-component sequence must advance. Exact
                        # at-least-once duplicates are dropped; a same-key body
                        # conflict or a duplicate older than retained dedup
                        # memory is rejected rather than re-applied.
                        comp_key = (obs.component_id, obs.instance_id)
                        last = session.last_component_seq.get(comp_key, -1)
                        if obs.sequence < last:
                            await self._protocol_violation(
                                session,
                                RejectReason.SEQUENCE_REGRESSION.value,
                                f"{obs.component_id} seq {obs.sequence} < {last}",
                            )
                            return
                        key = obs.dedup_key()
                        try:
                            novel = session.remember(key, _observation_fingerprint(obs))
                        except ContractError as exc:
                            await self._protocol_violation(
                                session, RejectReason.MALFORMED.value, str(exc)
                            )
                            return
                        if not novel:
                            continue  # at-least-once duplicate: idempotent drop
                        if obs.sequence == last:
                            await self._protocol_violation(
                                session,
                                RejectReason.SEQUENCE_REGRESSION.value,
                                f"{obs.component_id} sequence {obs.sequence} is no longer "
                                "inside dedup retention",
                            )
                            return
                        session.last_component_seq[comp_key] = obs.sequence
                        try:
                            result = self._on_observation(session.rank, obs)
                        except ContractError as exc:
                            await self._protocol_violation(
                                session, RejectReason.MALFORMED.value, str(exc)
                            )
                            return
                        if asyncio.iscoroutine(result):
                            await result
                elif env.kind == EnvelopeKind.HEARTBEAT.value:
                    if not session.established:
                        await self._protocol_violation(
                            session,
                            RejectReason.NOT_REGISTERED.value,
                            "HEARTBEAT before snapshot ACK barrier",
                        )
                        return
                    if (
                        not isinstance(env.payload, dict)
                        or set(env.payload) != {"payload_version", "heartbeat_id"}
                        or not _is_payload_version_one(env.payload)
                        or not isinstance(env.payload.get("heartbeat_id"), str)
                        or not env.payload["heartbeat_id"]
                    ):
                        await self._protocol_violation(
                            session,
                            RejectReason.MALFORMED.value,
                            "invalid HEARTBEAT payload/version",
                        )
                        return
                    # IMP-B06: record receiver-side arrival so a watchdog can
                    # expire a silent rank's lease.
                    session.last_seen_at = time.monotonic()
                    if self._on_heartbeat is not None:
                        result = self._on_heartbeat(session.rank)
                        if asyncio.iscoroutine(result):
                            await result
                    if not await self._write_session_frame(
                        writer,
                        Envelope(
                            v=SCHEMA_VERSION,
                            kind=EnvelopeKind.HEARTBEAT.value,
                            deployment_id=self.deployment_id,
                            plan_hash=self.plan_hash,
                            generation=self.generation,
                            sender_rank=SUPERVISOR_RANK,
                            sender_node="head",
                            seq=self._next_out_seq(session.rank),
                            payload={
                                "payload_version": 1,
                                "heartbeat_id": env.payload["heartbeat_id"],
                            },
                        ),
                    ):
                        break
                elif env.kind == EnvelopeKind.RECEIPT.value:
                    if not session.established:
                        await self._protocol_violation(
                            session,
                            RejectReason.NOT_REGISTERED.value,
                            "incremental receipt before snapshot ACK barrier",
                        )
                        return
                    # A rank session may submit only RANK-scoped receipts for
                    # its own authenticated rank; the validator enforces the
                    # rest (§3.2.1).
                    if (
                        not isinstance(env.payload, dict)
                        or not _is_payload_version_one(env.payload)
                        or set(env.payload) != {"payload_version", "receipt"}
                    ):
                        await self._protocol_violation(
                            session, RejectReason.MALFORMED.value, "invalid RECEIPT payload/version"
                        )
                        return
                    try:
                        payload = self._validate_rank_receipt(session, env.payload["receipt"])
                    except ContractError as exc:
                        await self._protocol_violation(
                            session, RejectReason.MALFORMED.value, f"receipt rejected: {exc}"
                        )
                        return
                    if self._on_receipt is not None:
                        result = self._on_receipt(session.rank, payload)
                        if asyncio.iscoroutine(result):
                            await result
                elif env.kind == EnvelopeKind.COMMAND_RESULT.value:
                    try:
                        result_payload = _validate_command_result_payload(env.payload)
                    except ContractError as exc:
                        await self._protocol_violation(
                            session,
                            RejectReason.MALFORMED.value,
                            str(exc),
                        )
                        return
                    command_id = result_payload["command_id"]
                    issued = self._issued_commands.get(command_id)
                    if issued is None:
                        await self._protocol_violation(
                            session,
                            RejectReason.MALFORMED.value,
                            f"COMMAND_RESULT names unissued command {command_id!r}",
                        )
                        return
                    issued_rank, issued_payload = issued
                    if issued_rank != session.rank:
                        await self._protocol_violation(
                            session,
                            RejectReason.RANK_MISMATCH.value,
                            f"rank {session.rank} returned COMMAND_RESULT for rank "
                            f"{issued_rank} command {command_id!r}",
                        )
                        return
                    if result_payload.get("operation") != issued_payload.get("operation"):
                        await self._protocol_violation(
                            session,
                            RejectReason.MALFORMED.value,
                            f"COMMAND_RESULT operation disagrees with command {command_id!r}",
                        )
                        return
                    if (
                        not session.established
                        and command_id != session.pending_snapshot_command_id
                    ):
                        await self._protocol_violation(
                            session,
                            RejectReason.NOT_REGISTERED.value,
                            "COMMAND_RESULT before snapshot ACK barrier",
                        )
                        return
                    result_record = dict(result_payload)
                    result_record["rank"] = env.sender_rank
                    prior = self._command_results.get(command_id)
                    if prior is not None and prior != result_record:
                        await self._protocol_violation(
                            session,
                            RejectReason.MALFORMED.value,
                            f"conflicting duplicate COMMAND_RESULT {command_id}",
                        )
                        return
                    self._command_results[command_id] = result_record
                    # A rank is allowed to close as soon as it has sent a
                    # successful DRAIN/STOP acknowledgement. Authorize that
                    # GOODBYE in the same listener turn that accepts the ACK,
                    # before waking a sequential head-side waiter. Otherwise
                    # a fast rank can close while the head is still waiting on
                    # an earlier rank and be falsely rejected as unsolicited.
                    if (
                        result_payload["operation"] in {"DRAIN", "STOP"}
                        and result_payload["ok"] is True
                    ):
                        self.expect_goodbye(session.rank)
                    self._command_result_events.setdefault(command_id, asyncio.Event()).set()
                    if command_id == session.pending_snapshot_command_id:
                        expected = {
                            "payload_version": 1,
                            "operation": "SNAPSHOT_ACCEPTED",
                            "snapshot_id": session.pending_snapshot_id,
                            "complete_set_hash": session.pending_snapshot_hash,
                            "status": "SUCCEEDED",
                        }
                        mismatch = [
                            key
                            for key, value in expected.items()
                            if result_payload.get(key) != value
                        ]
                        if mismatch:
                            await self._protocol_violation(
                                session,
                                RejectReason.MALFORMED.value,
                                f"snapshot COMMAND_RESULT mismatch: {mismatch}",
                            )
                            return
                        ok, detail = await self._call_decision(
                            self._on_snapshot_ack, session.rank, dict(result_payload)
                        )
                        if not ok:
                            await self._protocol_violation(
                                session,
                                RejectReason.MALFORMED.value,
                                f"snapshot acknowledgment rejected: {detail}",
                            )
                            return
                        session.established = True
                        session.pending_snapshot_command_id = ""
                        self._refresh_established_event()
                elif env.kind == EnvelopeKind.GOODBYE.value:
                    if set(env.payload) != {"payload_version"} or not _is_payload_version_one(
                        env.payload
                    ):
                        await self._protocol_violation(
                            session, RejectReason.MALFORMED.value, "invalid GOODBYE payload/version"
                        )
                        break
                    if not session.expected_goodbye:
                        await self._protocol_violation(
                            session,
                            RejectReason.MALFORMED.value,
                            "unsolicited GOODBYE from required rank",
                        )
                    else:
                        with self._goodbye_lock:
                            self._received_goodbyes.add(session.rank)
                    break
                # COMMAND/COMMAND_RESULT handling is added with the command
                # dispatcher in WP4's supervisor slice.
        except asyncio.CancelledError:
            raise
        except BaseException as exc:
            failure = (
                f"control handler failed for rank "
                f"{getattr(session, 'rank', 'unauthenticated')}: "
                f"{type(exc).__name__}: {exc}"
            )
            with self._handler_failure_lock:
                self._handler_failures.append(failure)
                if len(self._handler_failures) > _MAX_AUDIT_RECORDS:
                    del self._handler_failures[: len(self._handler_failures) - _MAX_AUDIT_RECORDS]
            self._audit(
                RejectReason.MALFORMED.value,
                failure,
                rank=None if session is None else session.rank,
                node_id=None if session is None else session.node_id,
            )
        finally:
            if session is not None and session.registered:
                session.registered = False
                session.established = False
                # IMP-B06: registration is no longer "all present" once a rank
                # drops; readiness must not keep believing the set is complete.
                self._all_authenticated.clear()
                self._all_established.clear()
                await self._emit_session_change(session.rank, False)
            self._writers.pop(getattr(session, "rank", -1), None)
            self._snapshots.pop(getattr(session, "rank", -1), None)
            try:
                writer.close()
            except RuntimeError:
                # Test/embedding teardown can close the event loop before the
                # handler's finalizer runs.  State has already been revoked;
                # there is no remaining transport action to schedule.
                pass
            self._connection_writers.discard(writer)
            if task is not None:
                self._handler_tasks.discard(task)

    def _validate_rank_receipt(self, session: SessionInfo, payload: Any) -> dict:
        if not isinstance(payload, Mapping):
            raise ContractError("rank receipt must be an object")
        receipt = dict(payload)
        if (
            not isinstance(receipt.get("owner_scope"), str)
            or receipt.get("owner_scope") != OwnerScope.RANK.value
        ):
            raise ContractError("rank session may submit only RANK receipts")
        submitted_rank = receipt.get("owner_rank")
        if type(submitted_rank) is not int or submitted_rank != session.rank:
            raise ContractError(
                f"receipt owner_rank {submitted_rank!r} does not match session {session.rank}"
            )
        from ..plan.contracts import same_node

        node_id = receipt.get("node_id")
        if node_id is not None:
            if not isinstance(node_id, str) or not node_id:
                raise ContractError("receipt node_id must be a nonempty string")
            if not same_node(node_id, session.node_id):
                raise ContractError("receipt node_id does not match session")
        return receipt

    def _validate_snapshot_items(
        self, session: SessionInfo, items: list[dict]
    ) -> tuple[list[ComponentObservation], list[dict]]:
        """Validate the complete set before any production projection changes."""
        observations: list[ComponentObservation] = []
        receipts: list[dict] = []
        from ..plan.contracts import same_node

        for item in items:
            if item["kind"] == "receipt":
                receipts.append(self._validate_rank_receipt(session, item["body"]))
                continue
            obs = validate_observation(item["body"])
            if (
                obs.deployment_id != self.deployment_id
                or obs.plan_hash != self.plan_hash
                or obs.generation != self.generation
            ):
                raise ContractError("snapshot observation identity is stale")
            if obs.owner_scope != OwnerScope.RANK.value:
                raise ContractError("rank snapshot contains a GLOBAL observation")
            if obs.owner_rank != session.rank:
                raise ContractError("snapshot observation owner_rank mismatch")
            if not same_node(obs.node_id, session.node_id):
                raise ContractError("snapshot observation node_id mismatch")
            observations.append(obs)
        component_instances: dict[str, str] = {}
        for obs in observations:
            previous = component_instances.get(obs.component_id)
            if previous is not None:
                raise ContractError(f"snapshot repeats component_id {obs.component_id!r}")
            component_instances[obs.component_id] = obs.instance_id
        if len(component_instances) > _MAX_COMPONENTS_PER_RANK:
            raise ContractError("snapshot component identity capacity exceeded")
        return observations, receipts

    def _assemble_snapshot(self, rank: int, payload: dict):
        """Buffer one chunk. Returns (complete, reject_reason, items)."""
        import json as _json

        expected_fields = {
            "payload_version",
            "snapshot_id",
            "chunk_index",
            "chunk_count",
            "complete_set_hash",
            "observations",
            "compatibility_receipts",
        }
        if not isinstance(payload, dict) or not _is_payload_version_one(payload):
            return False, "unknown snapshot payload_version", []
        if set(payload) != expected_fields:
            return False, "snapshot fields do not match the version-1 contract", []
        snapshot_id = payload.get("snapshot_id")
        complete_hash = payload.get("complete_set_hash")
        total = payload.get("chunk_count")
        index = payload.get("chunk_index")
        if type(total) is not int or type(index) is not int:
            return False, "snapshot chunk_count/chunk_index is invalid", []
        if not isinstance(snapshot_id, str) or not snapshot_id:
            return False, "snapshot id is missing or invalid", []
        if not isinstance(complete_hash, str) or _SHA256_HEX.fullmatch(complete_hash) is None:
            return False, "snapshot id/hash is missing or invalid", []
        if total < 1 or index < 0 or index >= total:
            return False, f"chunk {index} out of range 0..{total - 1}", []
        if total > self._limits.max_snapshot_chunks:
            return False, f"{total} chunks exceeds the cap", []

        state = self._snapshots.get(rank)
        if state is None:
            state = {
                "snapshot_id": snapshot_id,
                "total": total,
                "hash": complete_hash,
                "chunks": {},
                "bytes": 0,
                "started_at": time.monotonic(),
            }
            self._snapshots[rank] = state
        elif (
            state["snapshot_id"] != snapshot_id
            or state["total"] != total
            or state["hash"] != complete_hash
        ):
            return False, "snapshot id/count/hash changed while one is in flight", []
        if time.monotonic() - state["started_at"] > self._limits.snapshot_assembly_deadline_s:
            del self._snapshots[rank]
            return False, "snapshot assembly deadline expired", []
        existing = state["chunks"].get(index)
        observations = payload.get("observations")
        receipts = payload.get("compatibility_receipts")
        if not isinstance(observations, list) or not isinstance(receipts, list):
            return False, "snapshot observations/compatibility_receipts must be lists", []
        raw_items = [{"kind": "observation", "body": body} for body in observations] + [
            {"kind": "receipt", "body": body} for body in receipts
        ]
        try:
            encoded = _json.dumps(raw_items, sort_keys=True, separators=(",", ":"), allow_nan=False)
        except (TypeError, ValueError):
            return False, "snapshot chunk is not canonical JSON", []
        items = list(raw_items)
        if existing is not None and existing != items:
            return False, f"conflicting duplicate for chunk {index}", []
        if existing is None:
            state["bytes"] += len(encoded.encode("utf-8"))
            if state["bytes"] > self._limits.max_snapshot_bytes:
                return False, "snapshot exceeds max_snapshot_bytes", []
        state["chunks"][index] = items
        item_count = sum(len(chunk) for chunk in state["chunks"].values())
        if item_count > self._limits.max_snapshot_items:
            return False, "snapshot exceeds max_snapshot_items", []
        if len(state["chunks"]) < state["total"]:
            return False, None, []

        assembled: list = []
        for i in sorted(state["chunks"]):
            assembled.extend(state["chunks"][i])
        try:
            assembled = canonical_snapshot_items(assembled)
            actual = snapshot_set_hash(assembled)
        except ContractError as exc:
            del self._snapshots[rank]
            return False, str(exc), []
        del self._snapshots[rank]
        if actual != complete_hash:
            return False, "snapshot content hash mismatch", []
        return True, None, assembled

    async def _send_command(
        self, rank: int, command_id: str, operation: str, payload: dict | None = None
    ) -> bool:
        """Push one command to a rank. Returns False if it is unreachable."""
        if not isinstance(command_id, str) or not command_id:
            raise ContractError("command_id must be a nonempty string")
        if not isinstance(operation, str) or not operation:
            raise ContractError("operation must be a nonempty string")
        if payload is not None and not isinstance(payload, dict):
            raise ContractError("command payload must be an object")
        reserved = {"payload_version", "command_id", "operation"}
        if payload is not None and reserved.intersection(payload):
            raise ContractError("command payload cannot replace reserved fields")
        writer = self._writers.get(rank)
        if writer is None:
            return False
        body = {"payload_version": 1, "command_id": command_id, "operation": operation}
        body.update(payload or {})
        request = (rank, dict(body))
        self._remember_issued_command(command_id, request)
        try:
            await write_frame(
                writer,
                self._secret,
                Envelope(
                    v=SCHEMA_VERSION,
                    kind=EnvelopeKind.COMMAND.value,
                    deployment_id=self.deployment_id,
                    plan_hash=self.plan_hash,
                    generation=self.generation,
                    sender_rank=SUPERVISOR_RANK,
                    sender_node="head",
                    seq=self._next_out_seq(rank),
                    payload=body,
                ),
                max_frame_bytes=self._limits.max_frame_bytes,
            )
            return True
        except (ConnectionError, OSError, ContractError):
            return False

    async def broadcast_command_targets(
        self, command_id: str, operation: str, payload: dict | None = None
    ) -> tuple[int, ...]:
        """Send to every established session and freeze the delivered ranks.

        A successful shutdown recipient may acknowledge, send GOODBYE, and
        cease to be established before the head inspects its result.  Returning
        the immutable delivery set prevents that correct fast close from being
        mistaken for a rank that never received the command.
        """
        sent: list[int] = []
        for rank in range(self.expected_ranks):
            if not self.is_established(rank):
                continue
            if await self._send_command(rank, f"{command_id}:{rank}", operation, payload):
                sent.append(rank)
        return tuple(sent)

    async def broadcast_command(
        self, command_id: str, operation: str, payload: dict | None = None
    ) -> int:
        """Compatibility count for callers that do not need delivery identity."""
        return len(await self.broadcast_command_targets(command_id, operation, payload))

    async def send_command(
        self, rank: int, command_id: str, operation: str, payload: dict | None = None
    ) -> bool:
        """Send one command to one established planned rank.

        Startup is deliberately phased (head, then workers), so it cannot use
        the broadcast-only surface without accidentally releasing every rank.
        The listener remains the sole writer and retains the same framing,
        authentication, sequence, and result-correlation rules.
        """
        if isinstance(rank, bool) or not isinstance(rank, int):
            raise ContractError("command rank must be an integer")
        if not 0 <= rank < self.expected_ranks:
            raise ContractError(f"command rank {rank} is not planned")
        if not self.is_established(rank):
            return False
        return await self._send_command(rank, command_id, operation, payload)

    def command_result(self, command_id: str) -> dict | None:
        return self._command_results.get(command_id)

    def expect_goodbye(self, rank: int) -> None:
        session = self.sessions.get(rank)
        if session is None:
            raise ContractError(f"rank {rank} is not established")
        # Listener-side ACK handling authorizes shutdown before it wakes the
        # head's result waiter. The later head-side call is intentionally
        # idempotent and must not erase an already received GOODBYE.
        if session.expected_goodbye:
            return
        if not session.established:
            raise ContractError(f"rank {rank} is not established")
        session.expected_goodbye = True
        with self._goodbye_lock:
            self._received_goodbyes.discard(rank)

    def received_goodbye(self, rank: int) -> bool:
        with self._goodbye_lock:
            return rank in self._received_goodbyes

    def unexpected_failure(self) -> str | None:
        with self._handler_failure_lock:
            return self._handler_failures[0] if self._handler_failures else None

    async def wait_command_result(self, command_id: str, timeout: float) -> dict | None:
        if not isinstance(command_id, str) or not command_id:
            raise ContractError("command result wait requires a nonempty command_id")
        if command_id not in self._issued_commands:
            raise ContractError(f"cannot wait for unissued command {command_id!r}")
        result = self.command_result(command_id)
        if result is not None:
            return result
        event = self._command_result_events.setdefault(command_id, asyncio.Event())
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except asyncio.TimeoutError:
            return None
        return self.command_result(command_id)

    async def wait_command_results(
        self, command_ids: Sequence[str], timeout: float
    ) -> dict[str, dict | None]:
        """Wait for a fixed command set without rank-order head-of-line blocking."""
        identifiers = tuple(command_ids)
        if len(set(identifiers)) != len(identifiers):
            raise ContractError("command result wait requires unique command IDs")
        results = await asyncio.gather(
            *(self.wait_command_result(command_id, timeout) for command_id in identifiers)
        )
        return dict(zip(identifiers, results, strict=True))

    def _register(self, env: Envelope) -> tuple[SessionInfo | None, str | None]:
        rank, node = env.sender_rank, env.sender_node
        if (
            not isinstance(env.payload, dict)
            or set(env.payload) != {"payload_version", "instance_id"}
            or not _is_payload_version_one(env.payload)
            or not isinstance(env.payload.get("instance_id"), str)
            or not env.payload["instance_id"]
        ):
            return None, RejectReason.MALFORMED.value
        if not (0 <= rank < self.expected_ranks):
            return None, RejectReason.RANK_MISMATCH.value
        if env.seq < 0:
            return None, RejectReason.SEQUENCE_REGRESSION.value
        from ..plan.contracts import same_node

        planned_node = self._expected_nodes.get(rank)
        if planned_node is not None and not same_node(planned_node, node):
            return None, RejectReason.NODE_MISMATCH.value
        existing = self.sessions.get(rank)
        if existing is not None and existing.registered:
            return None, RejectReason.DUPLICATE_RANK.value
        expected_node = self._nodes_by_rank.get(rank)
        if (
            existing is not None
            and expected_node is not None
            and not same_node(expected_node, node)
        ):
            # A rank must re-register from the same node identity; a moved
            # rank is a topology change, not a reconnect.
            return None, RejectReason.NODE_MISMATCH.value
        if any(
            other_rank != rank and same_node(other_node, node)
            for other_rank, other_node in self._nodes_by_rank.items()
        ):
            return None, RejectReason.DUPLICATE_RANK.value
        session = SessionInfo(
            rank=rank,
            node_id=node,
            registered=True,
            instance_id=env.payload["instance_id"],
            last_seq=env.seq,
            last_seen_at=time.monotonic(),
        )
        self.sessions[rank] = session
        self._nodes_by_rank[rank] = node
        if all(
            r in self.sessions and self.sessions[r].registered for r in range(self.expected_ranks)
        ):
            self._all_authenticated.set()
        return session, None


class NodeChannel:
    """Rank-side client: register, then push observations/heartbeats."""

    def __init__(
        self,
        *,
        host: str,
        port: int,
        secret: bytes,
        deployment_id: str,
        plan_hash: str,
        generation: int,
        rank: int,
        node_id: str,
        limits: object | None = None,
    ) -> None:
        if not isinstance(host, str) or not host:
            raise ValueError("rank control host must be non-empty text")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
            raise ValueError("rank control port must be in 1..65535")
        if not isinstance(secret, bytes) or len(secret) != 32:
            raise ValueError("rank control secret must be exactly 256 bits")
        for name, value in (("deployment_id", deployment_id), ("plan_hash", plan_hash)):
            if not isinstance(value, str) or not value:
                raise ValueError(f"rank control {name} must be non-empty text")
        for name, value in (("generation", generation), ("rank", rank)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"rank control {name} must be a non-negative integer")
        if not isinstance(node_id, str) or not node_id:
            raise ValueError("rank control node_id must be non-empty text")
        self._host, self._port, self._secret = host, port, secret
        self.deployment_id, self.plan_hash = deployment_id, plan_hash
        self.generation, self.rank, self.node_id = generation, rank, node_id
        self._seq = 0
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._head_seq = 0
        self._limits = limits or _DefaultLimits()
        self.control_limits: dict[str, Any] = {}
        self._snapshot_hashes: dict[str, str] = {}
        self._pending_head_commands: list[dict] = []
        self._received_commands: dict[str, dict[str, Any]] = {}
        self._command_results: dict[str, dict[str, Any]] = {}
        self._heartbeat_counter = 0

    def _remember_received_command(self, command: dict[str, Any]) -> dict[str, Any]:
        command_id = command["command_id"]
        previous = self._received_commands.get(command_id)
        if previous is not None:
            if previous != command:
                raise ContractError(f"conflicting duplicate COMMAND {command_id!r}")
            return previous
        if len(self._received_commands) >= _MAX_COMMAND_RECORDS:
            raise ContractError(
                "rank command identity capacity exhausted; refusing a novel command"
            )
        remembered = dict(command)
        self._received_commands[command_id] = remembered
        return remembered

    def _queue_head_command(self, payload: object) -> None:
        command = dict(_validate_command_payload(payload))
        if len(self._pending_head_commands) >= _MAX_PENDING_HEAD_COMMANDS:
            raise ContractError("pending supervisor command queue capacity exhausted")
        self._pending_head_commands.append(command)

    def _env(self, kind: str, payload: dict[str, Any]) -> Envelope:
        self._seq += 1
        return Envelope(
            v=SCHEMA_VERSION,
            kind=kind,
            deployment_id=self.deployment_id,
            plan_hash=self.plan_hash,
            generation=self.generation,
            sender_rank=self.rank,
            sender_node=self.node_id,
            seq=self._seq,
            payload=payload,
        )

    async def connect_and_register(self, timeout: float = 30.0, *, instance_id: str = "") -> None:
        if self._writer is not None:
            previous = self._writer
            previous.close()
            try:
                await previous.wait_closed()
            except (ConnectionError, OSError) as exc:
                raise ConnectionError(f"previous control connection cleanup failed: {exc}") from exc
            finally:
                self._writer = None
                self._reader = None
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port), timeout
        )
        await write_frame(
            self._writer,
            self._secret,
            self._env(
                EnvelopeKind.REGISTER.value,
                {
                    "payload_version": 1,
                    "instance_id": instance_id or f"{self.node_id}:supervisor",
                },
            ),
            max_frame_bytes=self._limits.max_frame_bytes,
        )
        ack = await asyncio.wait_for(
            read_frame(self._reader, self._secret, max_frame_bytes=self._limits.max_frame_bytes),
            timeout,
        )
        self._validate_head_envelope(ack)
        expected_ack_fields = {
            "payload_version",
            "command_id",
            "operation",
            "ok",
            "control_limits",
        }
        if not (
            ack.kind == EnvelopeKind.COMMAND_RESULT.value
            and set(ack.payload) == expected_ack_fields
            and _is_payload_version_one(ack.payload)
            and ack.payload.get("operation") == "REGISTER"
            and ack.payload.get("command_id") == "register"
            and ack.payload.get("ok") is True
        ):
            raise ContractError("registration not acknowledged")
        limits = ack.payload.get("control_limits")
        if not isinstance(limits, dict):
            raise ContractError("registration acknowledgment omitted control limits")
        required = {
            "registration_deadline_s",
            "heartbeat_interval_s",
            "lease_timeout_s",
            "reconnect_grace_s",
            "snapshot_assembly_deadline_s",
            "watchdog_cleanup_deadline_s",
            "max_frame_bytes",
            "max_snapshot_chunks",
            "max_snapshot_bytes",
            "max_snapshot_items",
        }
        timing = required - {
            "max_frame_bytes",
            "max_snapshot_chunks",
            "max_snapshot_bytes",
            "max_snapshot_items",
        }
        counts = required - timing
        if (
            set(limits) != required
            or any(
                isinstance(limits[key], bool)
                or not isinstance(limits[key], (int, float))
                or not math.isfinite(float(limits[key]))
                or limits[key] <= 0
                for key in timing
            )
            or any(type(limits[key]) is not int or limits[key] <= 0 for key in counts)
        ):
            raise ContractError("registration acknowledgment has invalid control limits")
        self.control_limits = dict(limits)
        self._limits = _DefaultLimits(**limits)

    def _validate_head_envelope(self, env: Envelope) -> None:
        if (
            env.deployment_id != self.deployment_id
            or env.plan_hash != self.plan_hash
            or env.generation != self.generation
            or env.sender_rank != SUPERVISOR_RANK
        ):
            raise ContractError("supervisor envelope identity mismatch")
        if env.seq <= self._head_seq:
            raise ContractError(RejectReason.SEQUENCE_REGRESSION.value)
        self._head_seq = env.seq

    async def _write(self, kind: str, payload: dict[str, Any]) -> None:
        if self._writer is None:
            raise ConnectionError("control channel is not connected")
        await write_frame(
            self._writer,
            self._secret,
            self._env(kind, payload),
            max_frame_bytes=self._limits.max_frame_bytes,
        )

    async def send_observation(
        self, obs: ComponentObservation, receipt: dict | None = None
    ) -> None:
        payload: dict[str, Any] = {"payload_version": 1, "observation": obs.to_dict()}
        if receipt is not None:
            payload["compatibility_receipt"] = dict(receipt)
        await self._write(EnvelopeKind.OBSERVATION.value, payload)

    async def send_snapshot(
        self,
        observations: list[ComponentObservation],
        receipts: list | None = None,
        *,
        snapshot_id: str = "",
        chunk_items: int = 256,
    ) -> str:
        """Send a complete replacement snapshot, chunked and hashed.

        The §3.2.1 contract: payload version 1, zero-based contiguous chunks, a
        canonical SHA-256 over the COMPLETE item set, and one in-flight
        snapshot. The head accepts only the whole set, so a partial or mixed
        snapshot cannot silently become the rank's projection.
        """
        items = [{"kind": "observation", "body": o.to_dict()} for o in observations]
        items += [{"kind": "receipt", "body": dict(r)} for r in (receipts or [])]
        items = canonical_snapshot_items(items)
        complete_hash = snapshot_set_hash(items)
        snapshot_id = snapshot_id or f"snap-{self.rank}-{complete_hash[:12]}"
        if type(chunk_items) is not int or chunk_items < 1:
            raise ContractError("chunk_items must be positive")
        if len(items) > self._limits.max_snapshot_items:
            raise ContractError("snapshot exceeds max_snapshot_items")
        import json as _json

        encoded = _json.dumps(items, sort_keys=True, separators=(",", ":"), allow_nan=False)
        if len(encoded.encode("utf-8")) > self._limits.max_snapshot_bytes:
            raise ContractError("snapshot exceeds max_snapshot_bytes")
        chunks = [items[i : i + chunk_items] for i in range(0, len(items), chunk_items)] or [[]]
        if len(chunks) > self._limits.max_snapshot_chunks:
            raise ContractError("snapshot exceeds max_snapshot_chunks")
        previous_hash = self._snapshot_hashes.get(snapshot_id)
        if previous_hash is not None and previous_hash != complete_hash:
            raise ContractError(f"snapshot_id {snapshot_id!r} was already used differently")
        if previous_hash is None:
            if self._snapshot_hashes:
                raise ContractError("only one local snapshot may be in flight")
            self._snapshot_hashes[snapshot_id] = complete_hash
        for index, chunk in enumerate(chunks):
            await self._write(
                EnvelopeKind.SNAPSHOT.value,
                {
                    "payload_version": 1,
                    "snapshot_id": snapshot_id,
                    "chunk_index": index,
                    "chunk_count": len(chunks),
                    "complete_set_hash": complete_hash,
                    "observations": [i["body"] for i in chunk if i["kind"] == "observation"],
                    "compatibility_receipts": [i["body"] for i in chunk if i["kind"] == "receipt"],
                },
            )
        return snapshot_id

    async def await_snapshot_accepted(self, snapshot_id: str, timeout: float) -> bool:
        expected_hash = self._snapshot_hashes.get(snapshot_id)
        if expected_hash is None:
            raise ContractError(f"unknown local snapshot {snapshot_id!r}")
        command = await self.receive_command(timeout)
        if command is None:
            return False
        expected = {
            "payload_version": 1,
            "operation": "SNAPSHOT_ACCEPTED",
            "snapshot_id": snapshot_id,
            "complete_set_hash": expected_hash,
        }
        mismatches = [key for key, value in expected.items() if command.get(key) != value]
        command_id = command.get("command_id")
        if mismatches or not isinstance(command_id, str) or not command_id:
            raise ContractError(
                f"SNAPSHOT_ACCEPTED command mismatch: {mismatches or ['command_id']}"
            )
        await self.send_command_result(
            command_id,
            True,
            operation="SNAPSHOT_ACCEPTED",
            snapshot_id=snapshot_id,
            complete_set_hash=expected_hash,
            status="SUCCEEDED",
        )
        del self._snapshot_hashes[snapshot_id]
        return True

    async def send_heartbeat(self) -> None:
        self._heartbeat_counter += 1
        await self._write(
            EnvelopeKind.HEARTBEAT.value,
            {
                "payload_version": 1,
                "heartbeat_id": f"{self.rank}:{self._heartbeat_counter}",
            },
        )

    async def heartbeat_round_trip(self, timeout: float) -> bool:
        self._heartbeat_counter += 1
        heartbeat_id = f"{self.rank}:{self._heartbeat_counter}"
        await self._write(
            EnvelopeKind.HEARTBEAT.value, {"payload_version": 1, "heartbeat_id": heartbeat_id}
        )
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            env = await self._read_head_envelope(remaining)
            if env.kind == EnvelopeKind.COMMAND.value:
                self._queue_head_command(env.payload)
                continue
            if (
                env.kind == EnvelopeKind.HEARTBEAT.value
                and set(env.payload) == {"payload_version", "heartbeat_id"}
                and _is_payload_version_one(env.payload)
                and env.payload.get("heartbeat_id") == heartbeat_id
            ):
                return True
            raise ContractError(f"unexpected {env.kind} during heartbeat")

    async def _read_head_envelope(self, timeout: float) -> Envelope:
        if self._reader is None:
            raise ConnectionError("control channel is not connected")
        try:
            env = await asyncio.wait_for(
                read_frame(
                    self._reader, self._secret, max_frame_bytes=self._limits.max_frame_bytes
                ),
                timeout,
            )
        except asyncio.TimeoutError:
            raise
        except (ConnectionError, asyncio.IncompleteReadError) as exc:
            raise ConnectionError("supervisor control channel closed") from exc
        self._validate_head_envelope(env)
        return env

    async def receive_command(self, timeout: float = 1.0):
        """One COMMAND from the head, or None on timeout."""
        if self._pending_head_commands:
            command = _validate_command_payload(self._pending_head_commands.pop(0))
        else:
            try:
                env = await self._read_head_envelope(timeout)
            except asyncio.TimeoutError:
                return None
            if env.kind != EnvelopeKind.COMMAND.value:
                raise ContractError(f"expected COMMAND, received {env.kind}")
            command = _validate_command_payload(env.payload)
        return self._remember_received_command(dict(command))

    async def replay_command_result(self, command: Mapping[str, Any]) -> bool:
        """Re-send the exact cached result for an idempotent command retry."""
        command_id = command.get("command_id")
        if not isinstance(command_id, str) or not command_id:
            raise ContractError("command replay requires a nonempty command_id")
        result = self._command_results.get(command_id)
        if result is None:
            return False
        previous = self._received_commands.get(command_id)
        if previous != dict(command):
            raise ContractError(f"conflicting duplicate COMMAND {command_id!r}")
        await self._write(EnvelopeKind.COMMAND_RESULT.value, dict(result))
        return True

    async def send_receipt(self, payload: dict) -> None:
        await self._write(
            EnvelopeKind.RECEIPT.value, {"payload_version": 1, "receipt": dict(payload)}
        )

    async def send_command_result(
        self,
        command_id: str,
        ok: bool,
        detail: str = "",
        *,
        operation: str = "",
        status: str | None = None,
        **payload: Any,
    ) -> None:
        if not isinstance(command_id, str) or not command_id:
            raise ContractError("command result command_id must be a nonempty string")
        if type(ok) is not bool:
            raise ContractError("command result ok must be a boolean")
        if not isinstance(detail, str):
            raise ContractError("command result detail must be a string")
        if not isinstance(operation, str) or not operation:
            raise ContractError("command result operation must be a nonempty string")
        if status is not None and status not in {"SUCCEEDED", "FAILED"}:
            raise ContractError("command result status is unsupported")
        reserved = {
            "payload_version",
            "command_id",
            "operation",
            "ok",
            "detail",
            "status",
        }
        if reserved.intersection(payload):
            raise ContractError("command result payload cannot replace reserved fields")
        body: dict[str, Any] = {
            "payload_version": 1,
            "command_id": command_id,
            "operation": operation,
            "ok": ok,
            "detail": detail,
            "status": status if status is not None else ("SUCCEEDED" if ok else "FAILED"),
        }
        body.update(payload)
        request = self._received_commands.get(command_id)
        if request is None:
            raise ContractError(f"cannot answer unreceived command {command_id!r}")
        if request.get("operation") != operation:
            raise ContractError("command result operation disagrees with received command")
        if operation == "SNAPSHOT_ACCEPTED" and any(
            request.get(key) != body.get(key) for key in ("snapshot_id", "complete_set_hash")
        ):
            raise ContractError("snapshot command result identity disagrees with received command")
        validated = dict(_validate_command_result_payload(body))
        previous = self._command_results.get(command_id)
        if previous is not None and previous != validated:
            raise ContractError(f"conflicting result for command {command_id!r}")
        if previous is None and len(self._command_results) >= _MAX_COMMAND_RECORDS:
            raise ContractError("rank command-result capacity exhausted")
        await self._write(
            EnvelopeKind.COMMAND_RESULT.value,
            validated,
        )
        self._command_results[command_id] = validated

    async def wait_disconnected(self) -> None:
        """Return when the supervisor connection is gone (watchdog input).

        The server never sends unsolicited frames after the register ack, so
        an EOF/reset here means the control lease is lost (plan §3.2: node
        supervisors must then drain/terminate local children by a deadline).
        """
        assert self._reader is not None
        while True:
            try:
                chunk = await self._reader.read(4096)
            except (ConnectionError, asyncio.IncompleteReadError):
                return
            if not chunk:
                return

    def drop_connection(self) -> None:
        """Abruptly close without GOODBYE (test/failure-injection helper)."""
        if self._writer is not None:
            self._writer.close()
            self._writer = None

    async def close(self, *, expected: bool = False) -> None:
        errors = []
        if self._writer is not None:
            if expected:
                try:
                    await self._write(EnvelopeKind.GOODBYE.value, {"payload_version": 1})
                except (ConnectionError, ContractError) as exc:
                    errors.append(f"GOODBYE delivery failed: {type(exc).__name__}: {exc}")
            writer = self._writer
            writer.close()
            self._writer = None
            self._reader = None
            try:
                await writer.wait_closed()
            except (ConnectionError, OSError, RuntimeError) as exc:
                errors.append(f"connection close failed: {type(exc).__name__}: {exc}")
        if errors:
            raise ConnectionError("; ".join(errors))
