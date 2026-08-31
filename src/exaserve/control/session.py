"""Control-session state machine (plan §3.2.1 Q4, packet P03, IMP-B06).

The transport could frame and authenticate messages; what it could not do was
decide anything. Registration was fail-open, a reconnecting rank could resume
with an incremental, a clean `GOODBYE` was reported as a lost lease, and the
heartbeat/lease machinery existed only in tests. This module is the missing
decision layer.

Three phases, deliberately distinct because they fail differently:

1. **Listener bind** — bounded transient retry is allowed *before* any rank
   launches. Ultimate failure is terminal and launches nothing: there is no
   "degrade to launcher-only" path, because a run nobody can observe is not a
   run we should start.
2. **Initial registration** — every planned rank must authenticate *and* land a
   complete snapshot with its supervisor receipt before anything starts. START
   is the gate: no rank starts Ray or any other long-lived child until the head
   has accepted all of them. One missing rank at the deadline is terminal.
3. **Established-session reconnect** — readiness is revoked the moment a
   session is lost. The same planned identity may recover within a bounded
   grace, and only by replacing its whole projection with a fresh snapshot.

Loss timing is anchored precisely, because double-counting a lease on top of a
grace silently doubles every recovery window:

    EOF/reset       -> loss_time = now
    silent loss     -> loss_time = last_heartbeat + lease_timeout_s
    readiness revoked at loss_time
    reconnect grace ends at loss_time + reconnect_grace_s
    node-local cleanup starts only after grace, bounded by
    watchdog_cleanup_deadline_s
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional


class SessionState(str, Enum):
    EXPECTED = "EXPECTED"  # planned, never connected
    AUTHENTICATED = "AUTHENTICATED"  # REGISTER accepted, snapshot owed
    SNAPSHOT_PENDING = "SNAPSHOT_PENDING"  # snapshot assembling
    SNAPSHOT_ACK_PENDING = "SNAPSHOT_ACK_PENDING"  # exact ACK/result owed
    ESTABLISHED = "ESTABLISHED"  # snapshot ACK round trip accepted
    LOST = "LOST"  # connection/lease gone, in grace
    TERMINAL = "TERMINAL"  # grace expired / fatal violation


class GenerationState(str, Enum):
    REGISTERING = "REGISTERING"
    STARTED = "STARTED"
    TERMINAL = "TERMINAL"


class ProtocolViolation(RuntimeError):
    """A deterministic violation by an authenticated rank: generation-fatal."""


@dataclass
class RankSession:
    rank: int
    node_id: str
    state: str = SessionState.EXPECTED.value
    instance_id: str = ""
    last_heartbeat_at: Optional[float] = None
    loss_time: Optional[float] = None
    snapshot_id: str = ""
    complete_set_hash: str = ""
    snapshot_command_id: str = ""
    snapshot_accepted: bool = False
    supervisor_receipt_accepted: bool = False
    reconnects: int = 0

    def is_established(self) -> bool:
        return self.state == SessionState.ESTABLISHED.value


@dataclass
class SnapshotAssembly:
    """Bounded chunked snapshot reassembly (§3.2.1 chunking contract)."""

    snapshot_id: str
    total_chunks: int
    complete_hash: str
    limits: object
    started_at: float
    chunks: dict = field(default_factory=dict)

    def add(self, index: int, payload: list) -> tuple[bool, str]:
        if index < 0 or index >= self.total_chunks:
            return False, f"chunk index {index} out of range 0..{self.total_chunks - 1}"
        if self.total_chunks > self.limits.max_snapshot_chunks:
            return False, (
                f"snapshot declares {self.total_chunks} chunks, limit is "
                f"{self.limits.max_snapshot_chunks}"
            )
        existing = self.chunks.get(index)
        if existing is not None and existing != payload:
            return False, f"conflicting duplicate for chunk {index}"
        self.chunks[index] = payload
        items = sum(len(c) for c in self.chunks.values())
        if items > self.limits.max_snapshot_items:
            return False, f"snapshot exceeds max_snapshot_items ({items})"
        return True, "accepted"

    def is_complete(self) -> bool:
        return len(self.chunks) == self.total_chunks

    def expired(self, now: float) -> bool:
        return now - self.started_at > self.limits.snapshot_assembly_deadline_s

    def items(self) -> list:
        out: list = []
        for index in sorted(self.chunks):
            out.extend(self.chunks[index])
        return out


class SessionCoordinator:
    """Head-side authority over rank sessions for ONE generation."""

    def __init__(
        self,
        *,
        plan,
        binding,
        clock: Callable[[], float] = time.monotonic,
        log: Callable[[str], None] = print,
    ) -> None:
        self.plan = plan
        self.binding = binding
        self.limits = plan.control
        self._clock = clock
        self._log = log
        self.generation_state = GenerationState.REGISTERING.value
        self.terminal_reason: Optional[str] = None
        # Binding the listener happens before potentially long source/model
        # staging. The registration lease begins only when RankLauncher is
        # about to start; otherwise large model broadcasts consume a deadline
        # for ranks that do not exist yet.
        self.started_at: Optional[float] = None
        self.sessions: dict[int, RankSession] = {
            rank: RankSession(rank=rank, node_id=binding.node_for(rank) or "")
            for rank in binding.ranks()
        }
        self._assemblies: dict[int, SnapshotAssembly] = {}
        self._drain_requested: set = set()

    # -- helpers -----------------------------------------------------------
    def _fatal(self, reason: str) -> None:
        if self.generation_state != GenerationState.TERMINAL.value:
            self.generation_state = GenerationState.TERMINAL.value
            self.terminal_reason = reason
            self._log(f"[Session] GENERATION TERMINAL: {reason}")

    def expected_ranks(self) -> tuple:
        return tuple(sorted(self.sessions))

    def begin_registration(self) -> float:
        """Start the one registration clock immediately before rank launch."""
        if self.generation_state != GenerationState.REGISTERING.value:
            raise RuntimeError(
                f"cannot begin registration while generation is {self.generation_state}"
            )
        if self.started_at is None:
            self.started_at = self._clock()
            self._log("[Session] rank registration deadline started")
        return self.started_at

    def registration_deadline_at(self) -> float:
        if self.started_at is None:
            raise RuntimeError("rank registration deadline has not started")
        return self.started_at + self.limits.registration_deadline_s

    def registration_remaining_s(self) -> float:
        if self.started_at is None:
            raise RuntimeError("rank registration deadline has not started")
        return max(0.0, self.registration_deadline_at() - self._clock())

    # -- phase 2: registration --------------------------------------------
    def register(self, rank: int, node_id: str, instance_id: str) -> tuple[bool, str]:
        """Authenticated REGISTER. Does NOT by itself count as registered."""
        session = self.sessions.get(rank)
        if session is None:
            # Unplanned rank: session-local rejection, not generation-fatal.
            return False, f"rank {rank} is not in the allocation binding"
        if self.started_at is None:
            # A planned connection before RankLauncher exists is stale traffic
            # or a composition wiring defect. Never shift the global deadline
            # to first arrival, which would hide both conditions.
            return False, "rank registration has not started"
        bound = self.binding.node_for(rank)
        if bound and not self.binding.is_bound_node(rank, node_id):
            # An authenticated rank asserting the wrong node is deterministic
            # and therefore generation-fatal.
            self._fatal(f"rank {rank} registered from {node_id!r}, bound to {bound!r}")
            session.state = SessionState.TERMINAL.value
            return False, "node does not match the allocation binding"
        if session.state == SessionState.ESTABLISHED.value and instance_id == session.instance_id:
            return True, "already established"
        session.instance_id = instance_id
        session.state = SessionState.AUTHENTICATED.value
        session.last_heartbeat_at = self._clock()
        session.loss_time = None
        session.snapshot_id = ""
        session.complete_set_hash = ""
        session.snapshot_command_id = ""
        session.snapshot_accepted = False
        session.supervisor_receipt_accepted = False
        return True, "authenticated; complete snapshot owed"

    def begin_snapshot(
        self, rank: int, snapshot_id: str, total_chunks: int, complete_hash: str
    ) -> tuple[bool, str]:
        session = self.sessions.get(rank)
        if session is None or session.state == SessionState.EXPECTED.value:
            return False, "snapshot before authentication"
        if total_chunks < 1:
            return False, "snapshot must declare at least one chunk"
        if total_chunks > self.limits.max_snapshot_chunks:
            return False, (
                f"{total_chunks} chunks exceeds max_snapshot_chunks "
                f"{self.limits.max_snapshot_chunks}"
            )
        existing = self._assemblies.get(rank)
        if existing is not None:
            if (
                existing.snapshot_id != snapshot_id
                or existing.total_chunks != total_chunks
                or existing.complete_hash != complete_hash
            ):
                return False, "a different snapshot is already in flight"
            return True, "identical snapshot already assembling"
        # One in-flight snapshot per rank.
        self._assemblies[rank] = SnapshotAssembly(
            snapshot_id=snapshot_id,
            total_chunks=total_chunks,
            complete_hash=complete_hash,
            limits=self.limits,
            started_at=self._clock(),
        )
        session.state = SessionState.SNAPSHOT_PENDING.value
        session.snapshot_accepted = False
        return True, "assembling"

    def add_snapshot_chunk(self, rank: int, index: int, payload: list) -> tuple[bool, str]:
        assembly = self._assemblies.get(rank)
        if assembly is None:
            return False, "no snapshot in flight"
        if assembly.expired(self._clock()):
            del self._assemblies[rank]
            return False, "snapshot assembly deadline expired"
        return assembly.add(index, payload)

    def complete_snapshot(
        self, rank: int, *, verify_hash: Optional[str] = None, supervisor_receipt: bool = False
    ) -> tuple[bool, str]:
        """Atomic replacement: an incomplete or mixed snapshot is refused."""
        assembly = self._assemblies.get(rank)
        session = self.sessions.get(rank)
        if assembly is None or session is None:
            return False, "no snapshot in flight"
        if not assembly.is_complete():
            missing = sorted(set(range(assembly.total_chunks)) - set(assembly.chunks))
            return False, f"incomplete snapshot, missing chunks {missing[:8]}"
        expected = verify_hash or assembly.complete_hash
        from ..compat.receipt_v2 import canonical_hash

        actual = canonical_hash(assembly.items())
        if expected and actual != expected:
            del self._assemblies[rank]
            return False, "snapshot content hash mismatch"
        del self._assemblies[rank]
        session.snapshot_id = assembly.snapshot_id
        session.complete_set_hash = actual
        session.snapshot_command_id = f"SNAPSHOT_ACCEPTED:{rank}:{assembly.snapshot_id}"
        # The complete set has been validated, but the barrier does not cross
        # until the rank durably processes the exact SNAPSHOT_ACCEPTED command
        # and returns the matching idempotent COMMAND_RESULT.
        session.snapshot_accepted = False
        session.supervisor_receipt_accepted = (
            supervisor_receipt or session.supervisor_receipt_accepted
        )
        if not session.supervisor_receipt_accepted:
            return False, "snapshot accepted but supervisor receipt is missing"
        session.state = SessionState.SNAPSHOT_ACK_PENDING.value
        session.last_heartbeat_at = self._clock()
        session.loss_time = None
        return True, "snapshot validated; acknowledgment round trip pending"

    def acknowledge_snapshot(
        self,
        rank: int,
        *,
        command_id: str,
        snapshot_id: str,
        complete_set_hash: str,
        succeeded: bool,
    ) -> tuple[bool, str]:
        """Cross the registration barrier only for the exact command/result."""
        session = self.sessions.get(rank)
        if session is None:
            return False, "unknown rank"
        if session.state != SessionState.SNAPSHOT_ACK_PENDING.value:
            return False, "no snapshot acknowledgment is pending"
        if not succeeded:
            return False, "SNAPSHOT_ACCEPTED command failed"
        if command_id != session.snapshot_command_id:
            return False, "snapshot acknowledgment command_id mismatch"
        if snapshot_id != session.snapshot_id:
            return False, "snapshot acknowledgment snapshot_id mismatch"
        if complete_set_hash != session.complete_set_hash:
            return False, "snapshot acknowledgment complete_set_hash mismatch"
        session.snapshot_accepted = True
        session.state = SessionState.ESTABLISHED.value
        session.last_heartbeat_at = self._clock()
        return True, "established"

    def all_registered(self) -> bool:
        """Every planned rank established WITH its snapshot and receipt."""
        return all(
            s.is_established() and s.snapshot_accepted and s.supervisor_receipt_accepted
            for s in self.sessions.values()
        )

    def may_start(self) -> tuple[bool, str]:
        if self.generation_state == GenerationState.TERMINAL.value:
            return False, self.terminal_reason or "generation terminal"
        if not self.all_registered():
            pending = sorted(
                r
                for r, s in self.sessions.items()
                if not (
                    s.is_established() and s.snapshot_accepted and s.supervisor_receipt_accepted
                )
            )
            return False, f"ranks not established: {pending[:8]}"
        return True, "all planned ranks established"

    def start(self) -> tuple[bool, str]:
        ok, reason = self.may_start()
        if ok:
            self.generation_state = GenerationState.STARTED.value
        return ok, reason

    def check_registration_deadline(self) -> Optional[str]:
        """One missing rank at the deadline makes the generation terminal."""
        if self.generation_state != GenerationState.REGISTERING.value:
            return None
        if self.started_at is None:
            return None
        elapsed = self._clock() - self.started_at
        if elapsed < self.limits.registration_deadline_s:
            return None
        pending = sorted(r for r, s in self.sessions.items() if not s.is_established())
        if pending:
            reason = (
                f"registration deadline ({self.limits.registration_deadline_s}s) "
                f"expired with ranks {pending[:8]} not established"
            )
            self._fatal(reason)
            return reason
        return None

    # -- phase 3: loss / reconnect ----------------------------------------
    def on_disconnect(
        self, rank: int, *, expected: bool = False, now: Optional[float] = None
    ) -> str:
        """EOF/reset. `expected` only after an acknowledged DRAIN/STOP."""
        session = self.sessions.get(rank)
        if session is None:
            return "unknown rank"
        now = self._clock() if now is None else now
        was_established = session.is_established()
        if expected:
            session.state = SessionState.TERMINAL.value
            return "expected goodbye"
        self._assemblies.pop(rank, None)
        session.snapshot_accepted = False
        # Before START, transport retries remain bounded by the initial
        # registration deadline; reconnect grace applies only to an established
        # generation. A partial registration must begin a fresh snapshot.
        if not was_established:
            session.state = SessionState.EXPECTED.value
            session.loss_time = None
            return "registration interrupted"
        session.state = SessionState.LOST.value
        session.loss_time = now
        return "lost"

    def request_drain(self, rank: int) -> None:
        """After this, a GOODBYE from that rank is normal rather than a loss."""
        self._drain_requested.add(rank)

    def on_heartbeat(self, rank: int, now: Optional[float] = None) -> None:
        session = self.sessions.get(rank)
        if session is not None and session.is_established():
            session.last_heartbeat_at = self._clock() if now is None else now

    def poll_leases(self, now: Optional[float] = None) -> list:
        """Silent loss: anchor loss_time at last heartbeat + lease timeout."""
        now = self._clock() if now is None else now
        newly_lost = []
        for session in self.sessions.values():
            if session.state != SessionState.ESTABLISHED.value:
                continue
            last = session.last_heartbeat_at
            if last is None:
                continue
            if now - last > self.limits.lease_timeout_s:
                session.state = SessionState.LOST.value
                session.loss_time = last + self.limits.lease_timeout_s
                newly_lost.append(session.rank)
        return newly_lost

    def readiness_revoked_ranks(self) -> tuple:
        """Any non-established session revokes readiness immediately."""
        return tuple(sorted(r for r, s in self.sessions.items() if not s.is_established()))

    def reconnect(
        self, rank: int, node_id: str, instance_id: str, now: Optional[float] = None
    ) -> tuple[bool, str]:
        """Recover inside grace, and only via a complete replacement snapshot."""
        session = self.sessions.get(rank)
        if session is None:
            return False, "unknown rank"
        if not self.binding.is_bound_node(rank, node_id):
            self._fatal(f"rank {rank} reconnected from unbound node {node_id!r}")
            session.state = SessionState.TERMINAL.value
            return False, "node does not match the allocation binding"
        now = self._clock() if now is None else now
        if session.state == SessionState.TERMINAL.value:
            return False, "session is terminal; a later reconnect cannot resurrect it"
        if session.loss_time is not None:
            if now > session.loss_time + self.limits.reconnect_grace_s:
                self._fatal(f"rank {rank} reconnect grace expired")
                session.state = SessionState.TERMINAL.value
                return False, "reconnect grace expired"
        session.reconnects += 1
        session.snapshot_accepted = False
        session.supervisor_receipt_accepted = False
        session.snapshot_id = ""
        session.complete_set_hash = ""
        session.snapshot_command_id = ""
        session.instance_id = instance_id
        session.state = SessionState.AUTHENTICATED.value
        return True, "reconnected; complete snapshot required before incrementals"

    def protocol_violation(self, rank: int, reason: str) -> None:
        """A deterministic violation after planned authentication is fatal."""
        session = self.sessions.get(rank)
        if session is not None:
            session.state = SessionState.TERMINAL.value
        self._assemblies.pop(rank, None)
        self._fatal(f"rank {rank} protocol violation: {reason}")

    def accept_incremental(self, rank: int) -> tuple[bool, str]:
        """An incremental before the replacement snapshot is refused."""
        session = self.sessions.get(rank)
        if session is None:
            return False, "unknown rank"
        if not session.is_established() or not session.snapshot_accepted:
            return False, (
                "incremental observation before a complete snapshot; "
                "reconnect must replace the whole projection first"
            )
        return True, "accepted"

    def check_grace_deadlines(self, now: Optional[float] = None) -> list:
        """Grace expiry is terminal; it is also when node cleanup may begin."""
        now = self._clock() if now is None else now
        expired = []
        for session in self.sessions.values():
            if session.state != SessionState.LOST.value or session.loss_time is None:
                continue
            if now > session.loss_time + self.limits.reconnect_grace_s:
                session.state = SessionState.TERMINAL.value
                expired.append(session.rank)
        if expired:
            self._fatal(f"reconnect grace expired for ranks {expired}")
        return expired

    def cleanup_deadline_for(self, rank: int) -> Optional[float]:
        """When node-local cleanup must be finished by. Grace is NOT doubled."""
        session = self.sessions.get(rank)
        if session is None or session.loss_time is None:
            return None
        return (
            session.loss_time
            + self.limits.reconnect_grace_s
            + self.limits.watchdog_cleanup_deadline_s
        )

    # -- reporting ---------------------------------------------------------
    def snapshot_state(self) -> dict:
        return {
            "generation_state": self.generation_state,
            "terminal_reason": self.terminal_reason,
            "sessions": {r: s.state for r, s in sorted(self.sessions.items())},
            "established": sum(1 for s in self.sessions.values() if s.is_established()),
            "planned": len(self.sessions),
        }
