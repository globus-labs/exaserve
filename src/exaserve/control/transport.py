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
import secrets as _secrets
import struct
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from .contracts import (
    MAX_MESSAGE_BYTES,
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
_MAC_BYTES = 32
SUPERVISOR_RANK = -1


def new_deployment_secret() -> bytes:
    """256-bit per-deployment authentication secret (plan §3.2)."""
    return _secrets.token_bytes(32)


def _mac(secret: bytes, body: bytes) -> bytes:
    return hmac.new(secret, body, hashlib.sha256).digest()


async def write_frame(writer: asyncio.StreamWriter, secret: bytes, env: Envelope) -> None:
    body = encode_envelope(env)
    frame = _mac(secret, body) + body
    writer.write(_LEN.pack(len(frame)) + frame)
    await writer.drain()


async def read_frame(reader: asyncio.StreamReader, secret: bytes) -> Envelope:
    """Read one authenticated envelope; raise ContractError on violation."""
    header = await reader.readexactly(_LEN.size)
    (length,) = _LEN.unpack(header)
    if length > MAX_MESSAGE_BYTES + _MAC_BYTES or length < _MAC_BYTES:
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
    last_seq: int = -1
    # dedup memory for at-least-once observation delivery
    seen_obs: set[tuple[str, int, str, str, int]] = field(default_factory=set)


@dataclass
class AuditRecord:
    reason: str
    detail: str
    rank: int | None = None
    node_id: str | None = None


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
    ) -> None:
        self.deployment_id = deployment_id
        self.plan_hash = plan_hash
        self.generation = generation
        self.expected_ranks = expected_ranks
        self._secret = secret
        self._on_observation = on_observation
        self._on_session_change = on_session_change
        self._host = host
        self._server: asyncio.AbstractServer | None = None
        self.port: int | None = None
        self.sessions: dict[int, SessionInfo] = {}
        self._nodes_by_rank: dict[int, str] = {}
        self.audit: list[AuditRecord] = []
        self._registered_event: dict[int, asyncio.Event] = {}
        self._all_registered = asyncio.Event()

    # -- lifecycle ---------------------------------------------------------
    async def start(self) -> None:
        # Bind an ephemeral port before rank launch (plan §3.2 step 2).
        self._server = await asyncio.start_server(self._handle, self._host, 0)
        self.port = self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    async def wait_all_registered(self, timeout: float) -> bool:
        try:
            await asyncio.wait_for(self._all_registered.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    # -- internals ---------------------------------------------------------
    def _audit(self, reason: str, detail: str, rank: int | None = None,
               node_id: str | None = None) -> None:
        self.audit.append(AuditRecord(reason=reason, detail=detail, rank=rank, node_id=node_id))

    async def _emit_session_change(self, rank: int, connected: bool) -> None:
        if self._on_session_change is None:
            return
        result = self._on_session_change(rank, connected)
        if asyncio.iscoroutine(result):
            await result

    def _check_scope(self, env: Envelope) -> str | None:
        if env.deployment_id != self.deployment_id:
            return RejectReason.WRONG_DEPLOYMENT.value
        if env.plan_hash != self.plan_hash:
            return RejectReason.WRONG_PLAN_HASH.value
        if env.generation != self.generation:
            return RejectReason.STALE_GENERATION.value
        return None

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        session: SessionInfo | None = None
        try:
            while True:
                try:
                    env = await read_frame(reader, self._secret)
                except ContractError as exc:
                    self._audit(str(exc), "frame rejected",
                                rank=None if session is None else session.rank)
                    break  # fail closed: terminate session
                except (asyncio.IncompleteReadError, ConnectionResetError):
                    break

                reason = self._check_scope(env)
                if reason is not None:
                    self._audit(reason, f"kind={env.kind}", rank=env.sender_rank,
                                node_id=env.sender_node)
                    break

                if env.kind == EnvelopeKind.REGISTER.value:
                    session, err = self._register(env)
                    if err is not None:
                        self._audit(err, "registration rejected", rank=env.sender_rank,
                                    node_id=env.sender_node)
                        break
                    await write_frame(
                        writer, self._secret,
                        Envelope(v=SCHEMA_VERSION, kind=EnvelopeKind.COMMAND_RESULT.value,
                                 deployment_id=self.deployment_id, plan_hash=self.plan_hash,
                                 generation=self.generation, sender_rank=SUPERVISOR_RANK,
                                 sender_node="head", seq=0,
                                 payload={"command_id": "register", "ok": True}),
                    )
                    await self._emit_session_change(session.rank, True)
                    continue

                if session is None or not session.registered:
                    self._audit(RejectReason.NOT_REGISTERED.value, f"kind={env.kind}",
                                rank=env.sender_rank, node_id=env.sender_node)
                    break

                if env.sender_rank != session.rank or env.sender_node != session.node_id:
                    self._audit(RejectReason.RANK_MISMATCH.value,
                                f"claimed {env.sender_rank}/{env.sender_node}",
                                rank=session.rank, node_id=session.node_id)
                    break

                if env.seq <= session.last_seq:
                    self._audit(RejectReason.SEQUENCE_REGRESSION.value,
                                f"seq={env.seq} last={session.last_seq}", rank=session.rank)
                    break
                session.last_seq = env.seq

                if env.kind in (EnvelopeKind.OBSERVATION.value, EnvelopeKind.SNAPSHOT.value):
                    payloads = (
                        env.payload.get("observations", [])
                        if env.kind == EnvelopeKind.SNAPSHOT.value
                        else [env.payload]
                    )
                    for item in payloads:
                        try:
                            obs = validate_observation(item)
                        except ContractError as exc:
                            self._audit(str(exc), "observation rejected", rank=session.rank)
                            continue
                        if (obs.deployment_id != self.deployment_id
                                or obs.plan_hash != self.plan_hash
                                or obs.generation != self.generation):
                            self._audit(RejectReason.STALE_GENERATION.value,
                                        f"obs {obs.component_id}", rank=session.rank)
                            continue
                        key = obs.dedup_key()
                        if key in session.seen_obs:
                            continue  # at-least-once duplicate: idempotent drop
                        session.seen_obs.add(key)
                        result = self._on_observation(session.rank, obs)
                        if asyncio.iscoroutine(result):
                            await result
                elif env.kind == EnvelopeKind.HEARTBEAT.value:
                    pass  # arrival time is the receiver-side liveness signal
                elif env.kind == EnvelopeKind.GOODBYE.value:
                    break
                # COMMAND/COMMAND_RESULT handling is added with the command
                # dispatcher in WP4's supervisor slice.
        finally:
            if session is not None and session.registered:
                session.registered = False
                await self._emit_session_change(session.rank, False)
            writer.close()

    def _register(self, env: Envelope) -> tuple[SessionInfo | None, str | None]:
        rank, node = env.sender_rank, env.sender_node
        if not (0 <= rank < self.expected_ranks):
            return None, RejectReason.RANK_MISMATCH.value
        existing = self.sessions.get(rank)
        if existing is not None and existing.registered:
            return None, RejectReason.DUPLICATE_RANK.value
        expected_node = self._nodes_by_rank.get(rank)
        if existing is not None and expected_node is not None and expected_node != node:
            # A rank must re-register from the same node identity; a moved
            # rank is a topology change, not a reconnect.
            return None, RejectReason.NODE_MISMATCH.value
        if node in self._nodes_by_rank.values() and self._nodes_by_rank.get(rank) != node:
            return None, RejectReason.DUPLICATE_RANK.value  # hostname uniqueness
        session = SessionInfo(rank=rank, node_id=node, registered=True)
        self.sessions[rank] = session
        self._nodes_by_rank[rank] = node
        if all(
            r in self.sessions and self.sessions[r].registered
            for r in range(self.expected_ranks)
        ):
            self._all_registered.set()
        return session, None


class NodeChannel:
    """Rank-side client: register, then push observations/heartbeats."""

    def __init__(self, *, host: str, port: int, secret: bytes, deployment_id: str,
                 plan_hash: str, generation: int, rank: int, node_id: str) -> None:
        self._host, self._port, self._secret = host, port, secret
        self.deployment_id, self.plan_hash = deployment_id, plan_hash
        self.generation, self.rank, self.node_id = generation, rank, node_id
        self._seq = 0
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    def _env(self, kind: str, payload: dict[str, Any]) -> Envelope:
        self._seq += 1
        return Envelope(v=SCHEMA_VERSION, kind=kind, deployment_id=self.deployment_id,
                        plan_hash=self.plan_hash, generation=self.generation,
                        sender_rank=self.rank, sender_node=self.node_id,
                        seq=self._seq, payload=payload)

    async def connect_and_register(self, timeout: float = 30.0) -> None:
        self._reader, self._writer = await asyncio.wait_for(
            asyncio.open_connection(self._host, self._port), timeout)
        await write_frame(self._writer, self._secret,
                          self._env(EnvelopeKind.REGISTER.value, {}))
        ack = await asyncio.wait_for(read_frame(self._reader, self._secret), timeout)
        if not (ack.kind == EnvelopeKind.COMMAND_RESULT.value and ack.payload.get("ok")):
            raise ContractError("registration not acknowledged")

    async def send_observation(self, obs: ComponentObservation) -> None:
        assert self._writer is not None
        await write_frame(self._writer, self._secret,
                          self._env(EnvelopeKind.OBSERVATION.value, obs.to_dict()))

    async def send_snapshot(self, observations: list[ComponentObservation]) -> None:
        assert self._writer is not None
        await write_frame(
            self._writer, self._secret,
            self._env(EnvelopeKind.SNAPSHOT.value,
                      {"observations": [o.to_dict() for o in observations]}))

    async def send_heartbeat(self) -> None:
        assert self._writer is not None
        await write_frame(self._writer, self._secret,
                          self._env(EnvelopeKind.HEARTBEAT.value, {}))

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

    async def close(self) -> None:
        if self._writer is not None:
            try:
                await write_frame(self._writer, self._secret,
                                  self._env(EnvelopeKind.GOODBYE.value, {}))
            except (ConnectionError, ContractError):
                pass
            self._writer.close()
            self._writer = None
