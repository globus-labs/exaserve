"""Single-event-loop runtime for the §3.2 control channel (plan WP4.4).

`transport.py` implements the authenticated channel; this module is the part
that makes it *run* inside processes that are not asyncio applications. The
head and each rank are ordinary blocking programs, so each side bridges its
synchronous owner to one private asyncio loop. Heartbeat, reconnect, command,
and transport work are tasks on that loop; no second thread makes control
decisions and no stdout parser exists.

This closes WP4.4's second failure signal. Before it, the head learned of a
rank failure only through launcher exit aggregation — correct but coarse: it
cannot distinguish "rank 3's raylet died" from "mpiexec returned nonzero", and
it cannot act until the whole launch unwinds. A rank that reports a fatal
observation now fails the run immediately, and the head terminates the launcher
group explicitly rather than waiting.

The channel is mandatory. Missing configuration, failed authentication, or an
unreachable listener prevents a rank from starting long-lived children.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import math
import os
import socket
import threading
import time
from collections.abc import Iterable
from typing import Callable, Optional, TypeVar

from .contracts import (
    SCHEMA_VERSION,
    ComponentObservation,
    ComponentState,
    ContractError,
    OwnerScope,
)
from .transport import ControlListener, NodeChannel, new_deployment_secret

HOST_ENV = "EXASERVE_CONTROL_HOST"
PORT_ENV = "EXASERVE_CONTROL_PORT"
SECRET_ENV = "EXASERVE_CONTROL_SECRET"  # hex; per-deployment, never logged
DEPLOYMENT_ENV = "EXASERVE_DEPLOYMENT_ID"
GENERATION_ENV = "EXASERVE_GENERATION"
PLAN_HASH_ENV = "EXASERVE_PLAN_HASH"

# Diagnostic records are useful after a failure, but they are not authoritative
# state and must not grow with uptime or reconnect count.  The exact current
# projection lives in the indexed dictionaries below; these lists retain a
# bounded forensic tail.  Failure logs preserve their first item because it is
# the causal record consumed by the outer supervisor.
_MAX_HEAD_DIAGNOSTIC_RECORDS = 4096


_DiagnosticItem = TypeVar("_DiagnosticItem")


class _BoundedDiagnosticList(list[_DiagnosticItem]):
    def __init__(self, *, preserve_first: bool = False) -> None:
        super().__init__()
        self.preserve_first = preserve_first
        self.dropped = 0

    def append(self, item: _DiagnosticItem) -> None:
        if len(self) < _MAX_HEAD_DIAGNOSTIC_RECORDS:
            super().append(item)
            return
        self.dropped += 1
        if self.preserve_first:
            del self[1]
        else:
            del self[0]
        super().append(item)

    def extend(self, items: Iterable[_DiagnosticItem]) -> None:
        for item in items:
            self.append(item)


def _resolve_deadline(*, timeout: float, deadline: Optional[float], label: str) -> float:
    value = deadline if deadline is not None else timeout
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise ValueError(f"{label} must be finite and nonnegative")
    return float(value) if deadline is not None else time.monotonic() + float(value)


def _require_positive_timeout(value: float, *, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value <= 0
    ):
        raise ValueError(f"{label} must be finite and positive")
    return float(value)


def _completion_timeout(protocol_timeout: float) -> float:
    """Bound the thread handoff outside an independently bounded coroutine.

    A coroutine such as ``receive_command(t)`` enforces the protocol deadline
    with ``asyncio.wait_for``. Giving ``Future.result`` the exact same ``t``
    creates a race: the caller can time out just before the event-loop thread
    returns the coroutine's ordinary timeout result. The capped allowance here
    is only for cross-thread result delivery; it does not extend the protocol
    operation's own deadline.
    """
    timeout = _require_positive_timeout(protocol_timeout, label="protocol timeout")
    return timeout + min(0.5, max(0.05, timeout * 0.05))


def _required_payload_text(payload: dict, key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ContractError(f"{key} must be a nonempty string")
    return value


def _receipt_key(payload: dict) -> tuple[str, str, str]:
    requirement_id = _required_payload_text(payload, "receipt_requirement_id")
    component_id = payload.get("component_id", "")
    instance_id = payload.get("instance_id", "")
    if not isinstance(component_id, str) or not isinstance(instance_id, str):
        raise ContractError("receipt component_id/instance_id must be strings when present")
    return requirement_id, component_id, instance_id


class _LoopThread:
    """A private event loop on its own thread."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._failure: Optional[BaseException] = None
        self._thread = threading.Thread(target=self._run, daemon=True, name="exaserve-control")
        self._thread.start()

    def _run(self) -> None:
        try:
            asyncio.set_event_loop(self.loop)
            self.loop.run_forever()
        except BaseException as exc:
            self._failure = exc

    def call(
        self,
        coro,
        timeout: float = 30.0,
        *,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ):
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or timeout < 0
        ):
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            raise ValueError("control call timeout must be finite and nonnegative")
        if self._failure is not None:
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            raise RuntimeError(f"control event loop failed: {self._failure}") from self._failure
        if not self._thread.is_alive():
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            raise RuntimeError("control event-loop thread is not running")
        if cancel_requested is not None and not callable(cancel_requested):
            close = getattr(coro, "close", None)
            if callable(close):
                close()
            raise ValueError("cancel_requested must be callable or null")
        future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        try:
            deadline = time.monotonic() + float(timeout)
            while True:
                if cancel_requested is not None:
                    cancelled = cancel_requested()
                    if not isinstance(cancelled, bool):
                        raise ValueError("cancel_requested must return a bool")
                    if cancelled:
                        raise RuntimeError("control call cancelled by its owner")
                remaining = max(0.0, deadline - time.monotonic())
                try:
                    return future.result(timeout=min(0.1, remaining))
                except concurrent.futures.TimeoutError:
                    # If the coroutine itself completed by raising TimeoutError,
                    # retrieve that exception instead of mistaking it for this
                    # cross-thread polling slice.
                    if future.done():
                        return future.result()
                    if time.monotonic() >= deadline:
                        raise
        except BaseException:
            # A synchronous deadline must also cancel the scheduled work; a
            # late START/STOP command after the caller failed is unsafe.
            future.cancel()
            raise

    def stop(self, timeout: float = 5.0) -> None:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or timeout < 0
        ):
            raise ValueError("control loop stop timeout must be finite and nonnegative")
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=float(timeout))
        if self._thread.is_alive():
            raise RuntimeError(
                f"control event-loop thread did not stop within {float(timeout):g} seconds"
            )
        self.loop.close()
        if self._failure is not None:
            raise RuntimeError(f"control event loop failed: {self._failure}") from self._failure


class HeadChannel:
    """Head-side listener. Records rank failures and lost sessions."""

    def __init__(
        self,
        *,
        deployment_id: str,
        generation: int,
        plan_hash: str,
        expected_ranks: int,
        host: str = "0.0.0.0",
        sessions=None,
        receipts=None,
    ) -> None:
        for name, value in (("deployment_id", deployment_id), ("plan_hash", plan_hash)):
            if not isinstance(value, str) or not value:
                raise ValueError(f"head channel {name} must be non-empty text")
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise ValueError("head channel generation must be a non-negative integer")
        if (
            isinstance(expected_ranks, bool)
            or not isinstance(expected_ranks, int)
            or expected_ranks < 1
        ):
            raise ValueError("head channel expected_ranks must be a positive integer")
        if not isinstance(host, str) or not host:
            raise ValueError("head channel host must be non-empty text")
        # The SessionCoordinator is the authority over registration/START; the
        # listener feeds it. Without this wiring the coordinator was a separate
        # object nothing informed, so all_registered() could never become true.
        self.sessions_coordinator = sessions
        # Same defect one layer over: receipts arrived and were appended to a
        # list nothing adjudicated, so every planned slot stayed missing.
        self.ledger = receipts
        self.evidence_receipts: list[tuple[int, dict]] = _BoundedDiagnosticList()
        self.receipt_rejections: list[str] = _BoundedDiagnosticList(preserve_first=True)
        self.deployment_id = deployment_id
        self.generation = generation
        self.plan_hash = plan_hash
        self.expected_ranks = expected_ranks
        self.secret = new_deployment_secret()
        self._observations_by_rank: dict[int, dict[tuple[str, str], ComponentObservation]] = {}
        # Sender wall time is diagnostic only. Freshness is evaluated from the
        # head's monotonic receive clock, as required by the control contract.
        self._observation_arrivals: dict[tuple[int, str, str], float] = {}
        self.receipt_payloads: list = _BoundedDiagnosticList()
        self.failures: list[str] = _BoundedDiagnosticList(preserve_first=True)
        self.disconnected: list[int] = _BoundedDiagnosticList()
        # A launcher can aggregate a remote rank death before the ordinary
        # reconnect grace expires.  Preserve the authenticated transport fact
        # separately so the launcher's unexpected-exit callback can report the
        # rank-local cause instead of replacing it with a generic MPI status.
        # Order is causal.  PALS terminates sibling ranks after one rank dies;
        # sorting a set of the resulting disconnects incorrectly promoted a
        # collateral rank-0 teardown over the injected rank-1 failure.
        self._unexpected_disconnects: list[int] = []
        self._shutdown_acknowledged_ranks: set[int] = set()
        self._state_lock = threading.RLock()
        self._stop_lock = threading.Lock()
        self._stopped = False
        self._stop_clean = True
        self._planned_receipt_ids = frozenset(
            requirement.receipt_requirement_id
            for requirement in (() if receipts is None else receipts.plan.receipt_requirements)
        )
        self._enforce_planned_observations = sessions is not None
        expected_nodes = None
        limits = None
        if sessions is not None:
            expected_nodes = {
                rank: sessions.binding.node_for(rank) or "" for rank in sessions.expected_ranks()
            }
            limits = sessions.limits
        self._listener = ControlListener(
            deployment_id=deployment_id,
            plan_hash=plan_hash,
            generation=generation,
            expected_ranks=expected_ranks,
            secret=self.secret,
            on_observation=self._on_observation,
            on_session_change=self._on_session_change,
            host=host,
            on_receipt=self._on_receipt,
            on_register=self._on_register,
            on_snapshot=self._on_snapshot,
            on_snapshot_ack=self._on_snapshot_ack,
            on_protocol_violation=self._on_protocol_violation,
            on_heartbeat=self._on_heartbeat,
            expected_nodes=expected_nodes,
            limits=limits,
        )
        self._loop = _LoopThread()
        try:
            self._loop.call(self._listener.start())
        except BaseException as exc:
            try:
                self._loop.stop()
            except BaseException as cleanup_exc:
                from ..exception_notes import add_exception_note

                add_exception_note(
                    exc,
                    f"control loop cleanup after listener startup failure also failed: "
                    f"{type(cleanup_exc).__name__}: {cleanup_exc}",
                )
            raise
        self.port = self._listener.port

    # -- sinks -------------------------------------------------------------
    def _on_receipt(self, rank: int, payload: dict) -> None:
        """Rank-owned receipts arrive here and nowhere else.

        The AUTHENTICATED session's rank is what authorizes the payload — the
        rank claimed inside it is only a claim, and `accept()` rejects the two
        disagreeing. Payloads that name no planned slot are rejected: an
        authenticated transport does not turn unplanned evidence into an
        authoritative compatibility claim.
        """
        self.receipt_payloads.append((rank, payload))
        if self.ledger is None:
            return
        raw_requirement_id = payload.get("receipt_requirement_id")
        requirement_id = raw_requirement_id if isinstance(raw_requirement_id, str) else "<invalid>"
        planned_receipt_ids = getattr(self, "_planned_receipt_ids", None)
        if planned_receipt_ids is None:
            # Some isolated ledger tests construct the sink without starting a
            # listener. Cache the projection on first use; production eagerly
            # initializes it in __init__, so ordinary receipt delivery is O(1).
            planned_receipt_ids = frozenset(
                requirement.receipt_requirement_id
                for requirement in self.ledger.plan.receipt_requirements
            )
            self._planned_receipt_ids = planned_receipt_ids
        if requirement_id not in planned_receipt_ids:
            self.receipt_rejections.append(
                f"rank {rank} {requirement_id}: no such planned requirement"
            )
            return
        from ..compat.receipt_v2 import ReceiptError, receipt_from_dict

        try:
            receipt = receipt_from_dict(payload)
        except ReceiptError as exc:
            self.receipt_rejections.append(f"rank {rank} {requirement_id}: {exc}")
            return
        node = None
        coordinator = getattr(self, "sessions_coordinator", None)
        if coordinator is not None:
            session = coordinator.sessions.get(rank)
            node = getattr(session, "node_id", None) if session else None
        ok, detail = self.ledger.accept(
            receipt,
            session_rank=rank,
            session_node=node,
        )
        if not ok:
            self.receipt_rejections.append(f"rank {rank} {requirement_id}: {detail}")
            print(
                f"[Control] receipt rejected from rank {rank} ({requirement_id}): {detail}",
                flush=True,
            )

    def _on_observation(self, rank: int, obs: ComponentObservation) -> None:
        reason = self._observation_contract_reason(rank, obs)
        if reason is not None:
            raise ContractError(reason)
        with self._state_lock:
            projection = self._observations_by_rank.setdefault(rank, {})
            projection[(obs.component_id, obs.instance_id)] = obs
            self._observation_arrivals[(rank, obs.component_id, obs.instance_id)] = time.monotonic()
            self._record_observation_failure(rank, obs)

    def _observation_contract_reason(self, rank: int, obs: ComponentObservation) -> Optional[str]:
        """Return why a rank observation is outside the production plan."""
        if not getattr(self, "_enforce_planned_observations", False):
            return None
        allowed = {
            f"rank{rank}": "rank",
            "ray": "ray",
            "serve_proxy": "serve_proxy",
            "node_diagnostics": "diagnostics",
        }
        if rank == 0:
            allowed["ray_head_endpoint"] = "ray_head_endpoint"
        expected_role = allowed.get(obs.component_id)
        if expected_role is None:
            return f"rank {rank} published unplanned component {obs.component_id!r}"
        if obs.role != expected_role:
            return (
                f"rank {rank} component {obs.component_id!r} has role {obs.role!r}; "
                f"expected {expected_role!r}"
            )
        return None

    @property
    def observations(self) -> list[ComponentObservation]:
        """Materialize the compatibility projection only when it is read.

        Ordinary authenticated observations update one indexed entry in O(1).
        Rebuilding this full list inside ``_on_observation`` made every event
        O(K), and a K-event reconciliation O(K²).  Production consumers use
        ``current_observations``; this property remains for diagnostics and
        compatibility tests and performs the expected O(K) full projection.
        """
        with self._state_lock:
            return [
                obs
                for rank in sorted(self._observations_by_rank)
                for _, obs in sorted(self._observations_by_rank[rank].items())
            ]

    def _record_observation_failure(self, rank: int, obs: ComponentObservation) -> None:
        if obs.state == ComponentState.FAILED.value:
            detail = obs.detail or obs.reason_code or "no detail"
            self.failures.append(f"rank {rank} component {obs.component_id}: {detail}")

    def _on_register(self, rank: int, node_id: str, instance_id: str) -> tuple[bool, str]:
        coordinator = self.sessions_coordinator
        if coordinator is None:
            return True, "standalone transport registration"
        session = coordinator.sessions.get(rank)
        if session is not None and session.state == "LOST":
            return coordinator.reconnect(rank, node_id, instance_id)
        return coordinator.register(rank, node_id, instance_id)

    def _decode_snapshot_receipts(self, rank: int, payloads: list[dict]):
        if self.ledger is None:
            return True, "standalone receipts", None
        from ..compat.receipt_v2 import ReceiptError, receipt_from_dict

        receipts = []
        for payload in payloads:
            raw_requirement_id = payload.get("receipt_requirement_id")
            requirement_id = (
                raw_requirement_id if isinstance(raw_requirement_id, str) else "<invalid>"
            )
            try:
                receipts.append(receipt_from_dict(payload))
            except ReceiptError as exc:
                return False, f"{requirement_id}: {exc}", None
        session = self.sessions_coordinator.sessions.get(rank)
        node = session.node_id if session is not None else ""

        return self.ledger.stage_rank_snapshot(rank, receipts, session_node=node)

    def _on_snapshot(
        self, rank: int, snapshot_id: str, complete_hash: str, items: list
    ) -> tuple[bool, str]:
        observation_payloads = [item["body"] for item in items if item["kind"] == "observation"]
        receipt_payloads = [item["body"] for item in items if item["kind"] == "receipt"]
        supervisor_id = f"rank{rank}/node_supervisor"
        if not any(
            payload.get("receipt_requirement_id") == supervisor_id for payload in receipt_payloads
        ):
            return False, f"required supervisor receipt {supervisor_id!r} is missing"

        ok, detail, staged_ledger = self._decode_snapshot_receipts(rank, receipt_payloads)
        if not ok:
            self.receipt_rejections.append(f"rank {rank}: {detail}")
            return False, detail

        coordinator = self.sessions_coordinator
        if coordinator is not None:
            ok, detail = coordinator.begin_snapshot(rank, snapshot_id, 1, complete_hash)
            if ok:
                ok, detail = coordinator.add_snapshot_chunk(rank, 0, items)
            if ok:
                ok, detail = coordinator.complete_snapshot(
                    rank, verify_hash=complete_hash, supervisor_receipt=True
                )
            if not ok:
                return False, detail

        from .contracts import validate_observation

        staged_observations = {
            (obs.component_id, obs.instance_id): obs
            for obs in (validate_observation(payload) for payload in observation_payloads)
        }
        for obs in staged_observations.values():
            reason = self._observation_contract_reason(rank, obs)
            if reason is not None:
                return False, reason
        with self._state_lock:
            if staged_ledger is not None:
                self.ledger.commit_staged(staged_ledger)
            for key in [key for key in self._observation_arrivals if key[0] == rank]:
                self._observation_arrivals.pop(key, None)
            self._observations_by_rank[rank] = staged_observations
            received_at = time.monotonic()
            for obs in staged_observations.values():
                self._observation_arrivals[(rank, obs.component_id, obs.instance_id)] = received_at
            self.receipt_payloads.extend((rank, dict(payload)) for payload in receipt_payloads)
            for obs in staged_observations.values():
                self._record_observation_failure(rank, obs)
        return True, "complete rank snapshot atomically accepted"

    def _on_snapshot_ack(self, rank: int, payload: dict) -> tuple[bool, str]:
        coordinator = self.sessions_coordinator
        if coordinator is None:
            return True, "standalone snapshot acknowledgment"
        try:
            command_id = _required_payload_text(payload, "command_id")
            snapshot_id = _required_payload_text(payload, "snapshot_id")
            complete_set_hash = _required_payload_text(payload, "complete_set_hash")
        except ContractError as exc:
            return False, str(exc)
        return coordinator.acknowledge_snapshot(
            rank,
            command_id=command_id,
            snapshot_id=snapshot_id,
            complete_set_hash=complete_set_hash,
            succeeded=(payload.get("status") == "SUCCEEDED" and payload.get("ok") is True),
        )

    def _on_protocol_violation(self, rank: int, reason: str) -> None:
        if self.sessions_coordinator is not None:
            self.sessions_coordinator.protocol_violation(rank, reason)
        self.failures.append(f"rank {rank}: protocol violation: {reason}")

    def _on_heartbeat(self, rank: int) -> None:
        if self.sessions_coordinator is not None:
            self.sessions_coordinator.on_heartbeat(rank)

    def _on_session_change(self, rank: int, connected: bool) -> None:
        coordinator = getattr(self, "sessions_coordinator", None)
        expected_goodbye = False
        if not connected:
            expected_goodbye = self._listener.received_goodbye(rank)
        if coordinator is not None and not connected:
            coordinator.on_disconnect(
                rank,
                expected=expected_goodbye,
            )
            if self.ledger is not None:
                self.ledger.drop_rank(rank)
            with self._state_lock:
                self._observations_by_rank.pop(rank, None)
                for key in [key for key in self._observation_arrivals if key[0] == rank]:
                    self._observation_arrivals.pop(key, None)
        with self._state_lock:
            if connected:
                self._unexpected_disconnects = [
                    owner_rank for owner_rank in self._unexpected_disconnects if owner_rank != rank
                ]
            else:
                self.disconnected.append(rank)
                if not expected_goodbye and rank not in self._unexpected_disconnects:
                    self._unexpected_disconnects.append(rank)
                if coordinator is None:
                    self.failures.append(f"rank {rank}: control lease lost")

    # -- surface -----------------------------------------------------------
    def rank_failure(self) -> Optional[str]:
        self.poll()
        with self._state_lock:
            return self.failures[0] if self.failures else None

    def launcher_exit_evidence(self, exit_code: Optional[int], *, wait_s: float = 1.0) -> str:
        """Resolve a coarse launcher exit to the first typed rank evidence.

        MPI/PALS can return as soon as one rank dies.  Its status proves the
        allocation launch failed, but does not identify why.  Give the already
        authenticated control channel one small, bounded handoff window to
        deliver the rank's fatal component observation or disconnect event.
        This callback runs before the global first cause is assigned.
        """
        if exit_code is not None and (
            isinstance(exit_code, bool) or not isinstance(exit_code, int)
        ):
            raise ValueError("rank launcher exit code must be null or an integer")
        wait_s = _require_positive_timeout(wait_s, label="launcher evidence wait")
        deadline = time.monotonic() + wait_s
        while True:
            self.poll()
            with self._state_lock:
                failure = self.failures[0] if self.failures else None
                first_disconnect = (
                    self._unexpected_disconnects[0] if self._unexpected_disconnects else None
                )
            if failure is not None:
                return f"{failure}; rank launcher exit={exit_code}"
            if first_disconnect is not None:
                return (
                    "authenticated rank control session disappeared without "
                    f"GOODBYE for rank(s) [{first_disconnect}]; "
                    f"rank launcher exit={exit_code}"
                )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return (
                    "rank launcher exited without typed rank evidence within "
                    f"{wait_s:g}s; exit={exit_code}"
                )
            time.sleep(min(0.02, remaining))

    def current_observations(
        self,
        *,
        rank: Optional[int] = None,
        role: Optional[str] = None,
        max_age_s: Optional[float] = None,
    ) -> list[ComponentObservation]:
        """Return a lock-consistent projection, optionally freshness-bounded."""
        if max_age_s is not None:
            _require_positive_timeout(max_age_s, label="observation max_age_s")
        now = time.monotonic()
        with self._state_lock:
            result = []
            ranks = [rank] if rank is not None else sorted(self._observations_by_rank)
            for owner_rank in ranks:
                projection = self._observations_by_rank.get(owner_rank, {})
                for (_component_id, _instance_id), obs in sorted(projection.items()):
                    if role is not None and obs.role != role:
                        continue
                    if max_age_s is not None:
                        arrived = self._observation_arrivals.get(
                            (owner_rank, obs.component_id, obs.instance_id)
                        )
                        if arrived is None or now - arrived > max_age_s:
                            continue
                    result.append(obs)
            return result

    def poll(self) -> Optional[str]:
        transport_failure = self._listener.unexpected_failure()
        if transport_failure and transport_failure not in self.failures:
            self.failures.append(transport_failure)
        coordinator = self.sessions_coordinator
        if coordinator is None:
            return self.failures[0] if self.failures else None
        coordinator.poll_leases()
        coordinator.check_registration_deadline()
        coordinator.check_grace_deadlines()
        if coordinator.terminal_reason:
            reason = coordinator.terminal_reason
            if reason not in self.failures:
                self.failures.append(reason)
            return reason
        return self.failures[0] if self.failures else None

    def env(self, *, reachable_host: str) -> dict:
        """Environment a rank needs to reach this listener."""
        if not isinstance(reachable_host, str) or not reachable_host:
            raise ValueError("a resolved control-listener address is required")
        return {
            HOST_ENV: reachable_host,
            PORT_ENV: str(self.port),
            SECRET_ENV: self.secret.hex(),
            DEPLOYMENT_ENV: self.deployment_id,
            GENERATION_ENV: str(self.generation),
            PLAN_HASH_ENV: self.plan_hash,
        }

    def wait_all_registered(
        self,
        timeout: float,
        *,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ) -> bool:
        timeout = _require_positive_timeout(timeout, label="registration barrier timeout")
        try:
            return bool(
                self._loop.call(
                    self._listener.wait_all_registered(timeout),
                    timeout=_completion_timeout(timeout),
                    cancel_requested=cancel_requested,
                )
            )
        except Exception as exc:  # synchronous boundary records the transport cause
            self.failures.append(f"registration barrier failed: {type(exc).__name__}: {exc}")
            return False

    def _send_start_phase(
        self,
        operation: str,
        ranks: tuple[int, ...],
        *,
        timeout: float,
        finalize: bool,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ) -> int:
        timeout = _require_positive_timeout(timeout, label=f"{operation} timeout")
        if operation not in {"START", "START_HEAD", "START_WORKER"}:
            raise ValueError(f"unsupported startup operation {operation!r}")
        if not ranks:
            if finalize and self.sessions_coordinator is not None:
                ok, reason = self.sessions_coordinator.start()
                if not ok:
                    self.failures.append(f"startup state transition refused: {reason}")
                    return -1
            if finalize:
                self._started = True
            return 0
        deadline = time.monotonic() + timeout
        sent = 0
        for rank in ranks:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.failures.append(f"{operation} delivery timed out at rank {rank}")
                return sent
            command_id = f"{operation}:{rank}"
            delivered = self._loop.call(
                self._listener.send_command(rank, command_id, operation),
                timeout=min(remaining, 30.0),
                cancel_requested=cancel_requested,
            )
            if not delivered:
                self.failures.append(f"{operation} could not reach established rank {rank}")
                return sent
            sent += 1
        for rank in ranks:
            command_id = f"{operation}:{rank}"
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self.failures.append(f"{operation} acknowledgment timed out at rank {rank}")
                return 0
            result = self._loop.call(
                self._listener.wait_command_result(command_id, remaining),
                timeout=_completion_timeout(remaining),
                cancel_requested=cancel_requested,
            )
            if not (
                result
                and result.get("payload_version") == 1
                and result.get("operation") == operation
                and result.get("status") == "SUCCEEDED"
                and result.get("ok") is True
            ):
                self.failures.append(f"rank {rank} returned an invalid {operation} result")
                return 0
        if finalize and self.sessions_coordinator is not None:
            ok, reason = self.sessions_coordinator.start()
            if not ok:
                self.failures.append(f"startup state transition refused: {reason}")
                return 0
        if finalize:
            self._started = True
        return sent

    def start_head(
        self,
        timeout: float = 30.0,
        *,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ) -> bool:
        """Release only rank zero after the complete registration barrier."""
        timeout = _require_positive_timeout(timeout, label="START_HEAD timeout")
        try:
            deadline = time.monotonic() + timeout
            if not self.wait_all_registered(
                max(0.0, deadline - time.monotonic()),
                cancel_requested=cancel_requested,
            ):
                self.failures.append("START_HEAD refused: full snapshot ACK barrier did not close")
                return False
            if self.sessions_coordinator is not None:
                ok, reason = self.sessions_coordinator.may_start()
                if not ok:
                    self.failures.append(f"START_HEAD refused: {reason}")
                    return False
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                self.failures.append("START_HEAD deadline exhausted before command delivery")
                return False
            return (
                self._send_start_phase(
                    "START_HEAD",
                    (0,),
                    timeout=remaining,
                    finalize=False,
                    cancel_requested=cancel_requested,
                )
                == 1
            )
        except Exception as exc:  # noqa: BLE001
            self.failures.append(f"START_HEAD failed: {type(exc).__name__}: {exc}")
            return False

    def start_workers(
        self,
        timeout: float = 30.0,
        *,
        cancel_requested: Optional[Callable[[], bool]] = None,
    ) -> int:
        """Release worker ranks and close the generation startup barrier."""
        timeout = _require_positive_timeout(timeout, label="START_WORKER timeout")
        try:
            ranks = tuple(range(1, self.expected_ranks))
            sent = self._send_start_phase(
                "START_WORKER",
                ranks,
                timeout=timeout,
                finalize=True,
                cancel_requested=cancel_requested,
            )
            return sent
        except Exception as exc:  # noqa: BLE001
            self.failures.append(f"START_WORKER failed: {type(exc).__name__}: {exc}")
            return -1

    def broadcast_start(self, timeout: float = 30.0) -> int:
        """Release the START gate by actually SENDING it (§3.2.1 Q4).

        This used to set a head-side flag that no rank could observe, so the
        gate was computed and never enforced.
        """
        timeout = _require_positive_timeout(timeout, label="START timeout")
        try:
            deadline = time.monotonic() + timeout
            # A client has only flushed its SNAPSHOT_ACCEPTED result when its
            # local ``establish`` returns; the listener may still be one event
            # loop turn away from committing that ACK.  START is the next
            # protocol phase, so make this method itself cross the complete
            # listener-side barrier instead of racing that final write.
            if not self.wait_all_registered(max(0.0, deadline - time.monotonic())):
                self.failures.append("START refused: full snapshot ACK barrier did not close")
                return 0
            if self.sessions_coordinator is not None:
                ok, reason = self.sessions_coordinator.may_start()
                if not ok:
                    self.failures.append(f"START refused: {reason}")
                    return 0
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                self.failures.append("START deadline exhausted before command delivery")
                return 0
            return self._send_start_phase(
                "START", tuple(range(self.expected_ranks)), timeout=remaining, finalize=True
            )
        except Exception as exc:  # noqa: BLE001
            self.failures.append(f"START broadcast failed: {type(exc).__name__}: {exc}")
            return 0

    def start_broadcast(self) -> bool:
        return getattr(self, "_started", False)

    def broadcast_shutdown(
        self,
        operation: str = "DRAIN",
        timeout: float = 30.0,
        *,
        deadline: Optional[float] = None,
    ) -> int:
        """Request ordered rank cleanup and authorize GOODBYE only after ACK."""
        if operation not in {"DRAIN", "STOP"}:
            raise ValueError(f"unsupported shutdown operation {operation!r}")
        deadline = _resolve_deadline(
            timeout=timeout,
            deadline=deadline,
            label="shutdown broadcast deadline",
        )
        try:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                self.failures.append(f"{operation} broadcast cleanup deadline exhausted")
                return 0
            sent = int(
                self._loop.call(
                    self._listener.broadcast_command(operation, operation),
                    timeout=min(remaining, 30.0),
                )
            )
            acknowledged = 0
            acknowledged_ranks: set[int] = set()
            for rank in range(self.expected_ranks):
                if not self._listener.is_established(rank):
                    continue
                command_id = f"{operation}:{rank}"
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                result = self._loop.call(
                    self._listener.wait_command_result(command_id, remaining),
                    timeout=_completion_timeout(remaining),
                )
                if not (
                    result
                    and result.get("operation") == operation
                    and result.get("status") == "SUCCEEDED"
                    and result.get("ok") is True
                ):
                    self.failures.append(f"rank {rank} returned an invalid {operation} result")
                    continue
                if self.sessions_coordinator is not None:
                    self.sessions_coordinator.request_drain(rank)
                self._listener.expect_goodbye(rank)
                acknowledged += 1
                acknowledged_ranks.add(rank)
            self._shutdown_acknowledged_ranks = acknowledged_ranks
            if acknowledged != sent:
                self.failures.append(
                    f"{operation} acknowledged by {acknowledged}/{sent} reachable ranks"
                )
            return acknowledged
        except Exception as exc:  # noqa: BLE001
            self.failures.append(f"{operation} broadcast failed: {type(exc).__name__}: {exc}")
            return 0

    def wait_shutdown_goodbyes(self, *, deadline: float) -> int:
        """Wait until every DRAIN-acknowledging rank closes with GOODBYE."""
        deadline = _resolve_deadline(
            timeout=0.0,
            deadline=deadline,
            label="GOODBYE wait deadline",
        )
        expected = set(self._shutdown_acknowledged_ranks)
        while time.monotonic() < deadline:
            remaining = {rank for rank in expected if not self._listener.received_goodbye(rank)}
            if not remaining:
                return len(expected)
            time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
        received = sum(1 for rank in expected if self._listener.received_goodbye(rank))
        if received != len(expected):
            self.failures.append(
                f"GOODBYE received from {received}/{len(expected)} DRAIN-acknowledging ranks"
            )
        return received

    def stop(self, *, timeout: float = 15.0, deadline: Optional[float] = None) -> bool:
        with self._stop_lock:
            if self._stopped:
                return self._stop_clean
            deadline = _resolve_deadline(
                timeout=timeout,
                deadline=deadline,
                label="head-channel stop deadline",
            )
            # Claim cleanup before performing it so concurrent callers cannot
            # schedule work onto an event loop another caller is closing.
            self._stopped = True

            def remaining() -> float:
                return max(0.0, deadline - time.monotonic())

            clean = True
            try:
                budget = remaining()
                if budget <= 0:
                    raise TimeoutError("control listener cleanup deadline exhausted")
                self._loop.call(self._listener.stop(), timeout=budget)
            except Exception as exc:
                clean = False
                self.failures.append(
                    f"control listener cleanup failed: {type(exc).__name__}: {exc}"
                )
            try:
                self._loop.stop(timeout=remaining())
            except Exception as exc:
                clean = False
                self.failures.append(f"control loop cleanup failed: {type(exc).__name__}: {exc}")
            self._stop_clean = clean
            return clean


class RankClient:
    """Mandatory rank-side control client and START barrier participant."""

    def __init__(self, *, rank: int, node_id: Optional[str] = None) -> None:
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
            raise ValueError("rank client rank must be a non-negative integer")
        if node_id is not None and (not isinstance(node_id, str) or not node_id):
            raise ValueError("rank client node_id must be null or non-empty text")
        self.rank = rank
        self.node_id = node_id or socket.gethostname()
        self.enabled = all(
            os.environ.get(key)
            for key in (
                HOST_ENV,
                PORT_ENV,
                SECRET_ENV,
                DEPLOYMENT_ENV,
                GENERATION_ENV,
                PLAN_HASH_ENV,
            )
        )
        self._loop: Optional[_LoopThread] = None
        self._channel: Optional[NodeChannel] = None
        self._seq = 0
        self.connected = False
        self.established = False
        self._start_received = False
        self._state_lock = threading.RLock()
        self._snapshot_observations: dict[tuple[str, str], ComponentObservation] = {}
        self._snapshot_receipts: dict[tuple[str, str, str], dict] = {}
        self._maintenance_stop = threading.Event()
        self._maintenance_future: Optional[concurrent.futures.Future] = None
        self._control_failure: Optional[str] = None
        self._shutdown_operation: Optional[str] = None

    def connect(self, timeout: float = 30.0) -> bool:
        """Authenticate with the head; failure is fail-closed for this rank."""
        if not self.enabled:
            return False
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or timeout <= 0
        ):
            raise ValueError("control connection timeout must be finite and positive")
        try:
            self._loop = _LoopThread()
            self._channel = NodeChannel(
                host=os.environ[HOST_ENV],
                port=int(os.environ[PORT_ENV]),
                secret=bytes.fromhex(os.environ[SECRET_ENV]),
                deployment_id=os.environ[DEPLOYMENT_ENV],
                plan_hash=os.environ[PLAN_HASH_ENV],
                generation=int(os.environ[GENERATION_ENV]),
                rank=self.rank,
                node_id=self.node_id,
            )
            self._loop.call(
                self._channel.connect_and_register(
                    timeout, instance_id=f"{self.node_id}:{os.getpid()}"
                ),
                _completion_timeout(timeout),
            )
            self.connected = True
            return True
        except Exception as exc:
            print(
                f"[Rank {self.rank}] mandatory control channel unavailable "
                f"({type(exc).__name__}: {exc}); refusing to start children",
                flush=True,
            )
            if self._loop is not None:
                cleanup_deadline = time.monotonic() + min(5.0, float(timeout))

                def cleanup_remaining() -> float:
                    return max(0.0, cleanup_deadline - time.monotonic())

                if self._channel is not None:
                    try:
                        budget = cleanup_remaining()
                        if budget <= 0:
                            raise TimeoutError("failed connection cleanup deadline exhausted")
                        self._loop.call(self._channel.close(expected=False), timeout=budget)
                    except Exception as cleanup_exc:
                        print(
                            f"[Rank {self.rank}] failed control connection cleanup also failed: "
                            f"{cleanup_exc}",
                            flush=True,
                        )
                try:
                    self._loop.stop(timeout=cleanup_remaining())
                except RuntimeError as cleanup_exc:
                    print(
                        f"[Rank {self.rank}] control loop cleanup also failed: {cleanup_exc}",
                        flush=True,
                    )
                self._loop = None
            self._channel = None
            self.connected = False
            return False

    @property
    def control_limits(self) -> dict:
        if self._channel is None:
            return {}
        return dict(self._channel.control_limits)

    def make_observation(
        self,
        component_id: str,
        state: str,
        *,
        role: str = "component",
        reason_code: Optional[str] = None,
        detail: Optional[str] = None,
        instance_id: Optional[str] = None,
        model_id: Optional[str] = None,
        replica_id: Optional[str] = None,
    ) -> ComponentObservation:
        if self._channel is None:
            raise RuntimeError("control channel has not authenticated")
        # NodeSupervisor lifecycle events and deployment-child application
        # events are forwarded by separate threads. Sequence allocation is a
        # single sender concern and therefore must be serialized.
        with self._state_lock:
            self._seq += 1
            sequence = self._seq
        return ComponentObservation(
            schema_version=SCHEMA_VERSION,
            deployment_id=self._channel.deployment_id,
            plan_hash=self._channel.plan_hash,
            generation=self._channel.generation,
            component_id=component_id,
            instance_id=instance_id or f"{self.node_id}:{os.getpid()}",
            sequence=sequence,
            owner_scope=OwnerScope.RANK.value,
            role=role,
            node_id=self.node_id,
            state=state,
            observed_at=time.time(),
            owner_rank=self.rank,
            model_id=model_id,
            replica_id=replica_id,
            reason_code=reason_code,
            detail=detail,
        )

    def establish(self, supervisor_receipt, *, observations=(), timeout: float = 30.0) -> bool:
        """Send the complete initial snapshot and cross its exact ACK barrier."""
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or timeout <= 0
        ):
            raise ValueError("snapshot establishment timeout must be finite and positive")
        if not self.connected or self._channel is None or self._loop is None:
            return False
        try:
            payload = (
                supervisor_receipt.to_dict()
                if hasattr(supervisor_receipt, "to_dict")
                else dict(supervisor_receipt)
            )
            _receipt_key(payload)
            deadline = time.monotonic() + float(timeout)
            snapshot_id = self._loop.call(
                self._channel.send_snapshot(list(observations), receipts=[payload]),
                timeout=max(0.0, deadline - time.monotonic()),
            )
            remaining = max(0.0, deadline - time.monotonic())
            if remaining <= 0:
                raise TimeoutError("snapshot establishment deadline exhausted before acceptance")
            accepted = self._loop.call(
                self._channel.await_snapshot_accepted(snapshot_id, remaining),
                timeout=_completion_timeout(remaining),
            )
            self.established = bool(accepted)
            if self.established:
                with self._state_lock:
                    self._snapshot_observations = {
                        (obs.component_id, obs.instance_id): obs for obs in observations
                    }
                    self._snapshot_receipts[_receipt_key(payload)] = payload
            return self.established
        except Exception as exc:  # noqa: BLE001
            print(
                f"[Rank {self.rank}] initial snapshot barrier failed ({type(exc).__name__}: {exc})",
                flush=True,
            )
            self.connected = False
            self.established = False
            return False

    def observe(
        self,
        component_id: str,
        state: str,
        *,
        role: str = "component",
        reason_code: Optional[str] = None,
        detail: Optional[str] = None,
        instance_id: Optional[str] = None,
        model_id: Optional[str] = None,
        replica_id: Optional[str] = None,
        timeout: float = 10.0,
    ) -> bool:
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or timeout <= 0
        ):
            raise ValueError("observation timeout must be finite and positive")
        if (
            not self.connected
            or not self.established
            or self._channel is None
            or self._loop is None
        ):
            return False
        obs = self.make_observation(
            component_id,
            state,
            role=role,
            reason_code=reason_code,
            detail=detail,
            instance_id=instance_id,
            model_id=model_id,
            replica_id=replica_id,
        )
        try:
            self._loop.call(self._channel.send_observation(obs), timeout=timeout)
            with self._state_lock:
                self._snapshot_observations[(obs.component_id, obs.instance_id)] = obs
            return True
        except Exception as exc:  # maintenance loop owns reconnect policy
            self.connected = False
            self._control_failure = f"observation delivery failed: {type(exc).__name__}: {exc}"
            return False

    def start_gate_available(self) -> bool:
        """True when the head can deliver START over the wire."""
        return self.connected and self.established

    def submit_receipt(self, receipt) -> bool:
        """Send one exact v2 receipt over the AUTHENTICATED channel (§3.2.1).

        A detached Ray actor, stdout, or a node-local file is not an
        authoritative readiness source. This is the only path a rank-owned
        receipt may take to the head.
        """
        if (
            not self.connected
            or not self.established
            or self._channel is None
            or self._loop is None
        ):
            return False
        try:
            payload = receipt.to_dict() if hasattr(receipt, "to_dict") else dict(receipt)
            _receipt_key(payload)
            self._loop.call(self._channel.send_receipt(payload), timeout=15)
            with self._state_lock:
                self._snapshot_receipts[_receipt_key(payload)] = payload
            return True
        except Exception as exc:  # noqa: BLE001 - maintenance loop owns reconnect policy
            self._control_failure = f"receipt delivery failed: {type(exc).__name__}: {exc}"
            return False

    def poll_start(self, timeout: float = 1.0, *, expected_operation: str = "START") -> bool:
        """Check for one exact startup phase and acknowledge it.

        A pre-start rank also heartbeats after an idle receive.  Worker ranks
        can therefore remain fenced while the head becomes ready without
        losing their control leases or needing a second command-reading thread.
        """
        if expected_operation not in {"START", "START_HEAD", "START_WORKER"}:
            raise ValueError(f"unsupported startup operation {expected_operation!r}")
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not math.isfinite(float(timeout))
            or timeout <= 0
        ):
            raise ValueError("START polling timeout must be finite and positive")
        if self._start_received:
            return True
        if not self.connected or self._channel is None or self._loop is None:
            return False
        deadline = time.monotonic() + float(timeout)
        receive_budget = min(float(timeout) * 0.75, deadline - time.monotonic())
        if receive_budget <= 0:
            return False
        try:
            payload = self._loop.call(
                self._channel.receive_command(receive_budget),
                timeout=_completion_timeout(receive_budget),
            )
        except Exception as exc:  # noqa: BLE001
            self.connected = False
            self.established = False
            self._control_failure = (
                f"{expected_operation} receive failed: {type(exc).__name__}: {exc}"
            )
            return False
        if not payload:
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._loop.call(
                    self._channel.heartbeat_round_trip(remaining),
                    timeout=_completion_timeout(remaining),
                )
            except Exception as exc:  # noqa: BLE001
                self.connected = False
                self.established = False
                self._control_failure = f"pre-start heartbeat failed: {type(exc).__name__}: {exc}"
            return False
        operation = payload.get("operation")
        if operation != expected_operation:
            self._control_failure = f"expected {expected_operation}, received {operation!r}"
            self.connected = False
            self.established = False
            return False
        try:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"{expected_operation} acknowledgment deadline exhausted")
            self._loop.call(
                self._channel.send_command_result(
                    _required_payload_text(payload, "command_id"),
                    True,
                    operation=expected_operation,
                    status="SUCCEEDED",
                ),
                timeout=remaining,
            )
        except Exception as exc:  # noqa: BLE001
            self.connected = False
            self.established = False
            print(
                f"[Rank {self.rank}] {expected_operation} acknowledgment failed: {exc}", flush=True
            )
            return False
        self._start_received = True
        self._start_maintenance()
        return True

    def start_received(self) -> bool:
        """True once the head has released the START gate.

        Missing transport never opens the mandatory gate.
        """
        return self.connected and self.established and self._start_received

    def note_start(self) -> None:
        self._start_received = True

    def _start_maintenance(self) -> None:
        if self._maintenance_future is not None:
            return
        try:
            limits = (
                float(self.control_limits.get("heartbeat_interval_s", 0)),
                float(self.control_limits.get("lease_timeout_s", 0)),
                float(self.control_limits.get("reconnect_grace_s", 0)),
            )
        except (TypeError, ValueError) as exc:
            self._control_failure = f"resolved control lease limits are invalid: {exc}"
            return
        if any(not math.isfinite(value) or value <= 0 for value in limits):
            self._control_failure = "resolved control lease limits are invalid"
            return
        if self._loop is None:
            self._control_failure = "control event loop is unavailable"
            return
        future = asyncio.run_coroutine_threadsafe(self._maintain_control_lease(), self._loop.loop)

        def maintenance_done(done: concurrent.futures.Future) -> None:
            try:
                done.result()
            except concurrent.futures.CancelledError:
                return
            except BaseException as exc:
                if self._control_failure is None:
                    self._control_failure = (
                        f"control maintenance task failed: {type(exc).__name__}: {exc}"
                    )

        future.add_done_callback(maintenance_done)
        self._maintenance_future = future

    async def _maintain_control_lease(self) -> None:
        interval = float(self.control_limits.get("heartbeat_interval_s", 0))
        lease = float(self.control_limits.get("lease_timeout_s", 0))
        grace = float(self.control_limits.get("reconnect_grace_s", 0))
        if min(interval, lease, grace) <= 0:
            self._control_failure = "resolved control lease limits are invalid"
            return
        last_ack = time.monotonic()
        while not self._maintenance_stop.is_set():
            await asyncio.sleep(interval)
            if self._maintenance_stop.is_set():
                return
            connection_reset = False
            try:
                assert self._channel is not None
                remaining_lease = max(0.001, last_ack + lease - time.monotonic())
                alive = await self._channel.heartbeat_round_trip(remaining_lease)
                if not alive:
                    raise TimeoutError("heartbeat acknowledgment timed out")
                last_ack = time.monotonic()
                command = await self._channel.receive_command(0.001)
                if command is not None and await self._handle_runtime_command(command):
                    return
                continue
            except ContractError as exc:
                self._control_failure = f"control protocol violation: {exc}"
                return
            except Exception as exc:  # noqa: BLE001 - reconnect below
                connection_reset = isinstance(exc, ConnectionError)
                self.connected = False
                self.established = False

            # EOF/reset anchors immediately. A silent timeout anchors at the
            # last accepted heartbeat acknowledgment plus the resolved lease.
            loss_time = (
                time.monotonic() if connection_reset else max(time.monotonic(), last_ack + lease)
            )
            backoff = min(0.25, max(0.05, interval / 4))
            reconnect_failure = "no reconnect attempt completed"
            while not self._maintenance_stop.is_set() and time.monotonic() <= loss_time + grace:
                try:
                    assert self._channel is not None
                    reconnect_deadline = loss_time + grace
                    remaining = reconnect_deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    connect_budget = min(remaining, 5.0)
                    await self._channel.connect_and_register(
                        connect_budget, instance_id=f"{self.node_id}:{os.getpid()}"
                    )
                    with self._state_lock:
                        observations = list(self._snapshot_observations.values())
                        receipts = list(self._snapshot_receipts.values())
                    remaining = reconnect_deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    snapshot_id = await self._channel.send_snapshot(observations, receipts=receipts)
                    remaining = reconnect_deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    acceptance_budget = min(remaining, 5.0)
                    accepted = await self._channel.await_snapshot_accepted(
                        snapshot_id, acceptance_budget
                    )
                    if accepted:
                        self.connected = True
                        self.established = True
                        last_ack = time.monotonic()
                        break
                except Exception as exc:  # noqa: BLE001 - bounded retry
                    reconnect_failure = f"{type(exc).__name__}: {exc}"
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, max(0.25, interval))
            if not self.established:
                self._control_failure = (
                    f"outer-supervisor lease lost; reconnect grace {grace}s expired; "
                    f"last reconnect failure: {reconnect_failure}"
                )
                return

    async def _handle_runtime_command(self, payload: dict) -> bool:
        assert self._channel is not None
        if await self._channel.replay_command_result(payload):
            return False
        operation = payload.get("operation")
        command_id = payload.get("command_id")
        if operation not in {"DRAIN", "STOP"} or not isinstance(command_id, str) or not command_id:
            self._control_failure = f"unexpected control command {operation!r}"
            return True
        try:
            await asyncio.wait_for(
                self._channel.send_command_result(
                    command_id, True, operation=operation, status="SUCCEEDED"
                ),
                timeout=10,
            )
        except Exception as exc:  # noqa: BLE001
            self._control_failure = f"{operation} acknowledgment failed: {exc}"
            return True
        self._shutdown_operation = operation
        return True

    def control_failure(self) -> Optional[str]:
        return self._control_failure

    def shutdown_requested(self) -> bool:
        return self._shutdown_operation in {"DRAIN", "STOP"}

    def close(
        self,
        *,
        expected: bool = False,
        timeout: float = 15.0,
        deadline: Optional[float] = None,
    ) -> None:
        deadline = _resolve_deadline(
            timeout=timeout,
            deadline=deadline,
            label="rank-channel close deadline",
        )

        def remaining() -> float:
            return max(0.0, deadline - time.monotonic())

        errors = []
        self._maintenance_stop.set()
        maintenance = self._maintenance_future
        if maintenance is not None and not maintenance.done():
            maintenance.cancel()
        if maintenance is not None:
            try:
                maintenance.result(timeout=remaining())
            except concurrent.futures.CancelledError:
                pass
            except concurrent.futures.TimeoutError:
                errors.append("control maintenance task did not stop by cleanup deadline")
            except BaseException as exc:
                errors.append(
                    f"control maintenance task failed during cleanup: {type(exc).__name__}: {exc}"
                )
        channel, loop = self._channel, self._loop
        if channel is not None and loop is not None:
            try:
                budget = remaining()
                if budget <= 0:
                    raise TimeoutError("control channel close deadline exhausted")
                loop.call(channel.close(expected=expected), timeout=budget)
            except Exception as exc:  # cleanup is best-effort but never silent
                errors.append(f"control channel close failed: {type(exc).__name__}: {exc}")
                if self._control_failure is None:
                    self._control_failure = errors[-1]
        if loop is not None:
            try:
                loop.stop(timeout=remaining())
            except RuntimeError as exc:
                errors.append(f"control loop close failed: {exc}")
        self._channel = None
        self._loop = None
        self._maintenance_future = None
        self.connected = False
        self.established = False
        if errors:
            raise RuntimeError("; ".join(errors))
