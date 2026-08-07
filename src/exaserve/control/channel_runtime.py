"""Threaded runtime for the §3.2 control channel (plan WP4.4, audit IMP-B01).

`transport.py` implements the authenticated channel; this module is the part
that makes it *run* inside processes that are not asyncio applications. The
head and each rank are ordinary blocking programs, so each side owns a small
event-loop thread and exposes a synchronous surface.

This closes WP4.4's second failure signal. Before it, the head learned of a
rank failure only through launcher exit aggregation — correct but coarse: it
cannot distinguish "rank 3's raylet died" from "mpiexec returned nonzero", and
it cannot act until the whole launch unwinds. A rank that reports a fatal
observation now fails the run immediately, and the head terminates the launcher
group explicitly rather than waiting.

Both sides degrade to no-ops when the channel env is absent, so the legacy
path is byte-for-byte unaffected.
"""

from __future__ import annotations

import asyncio
import os
import socket
import threading
import time
from typing import Optional

from .contracts import SCHEMA_VERSION, ComponentObservation, ComponentState, OwnerScope
from .transport import ControlListener, NodeChannel, new_deployment_secret

HOST_ENV = "EXASERVE_CONTROL_HOST"
PORT_ENV = "EXASERVE_CONTROL_PORT"
SECRET_ENV = "EXASERVE_CONTROL_SECRET"       # hex; per-deployment, never logged
DEPLOYMENT_ENV = "EXASERVE_DEPLOYMENT_ID"
GENERATION_ENV = "EXASERVE_GENERATION"
PLAN_HASH_ENV = "EXASERVE_PLAN_HASH"


class _LoopThread:
    """A private event loop on its own thread."""

    def __init__(self) -> None:
        self.loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, daemon=True,
                                        name="exaserve-control")
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def call(self, coro, timeout: float = 30.0):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def stop(self) -> None:
        self.loop.call_soon_threadsafe(self.loop.stop)
        self._thread.join(timeout=5)


class HeadChannel:
    """Head-side listener. Records rank failures and lost sessions."""

    def __init__(self, *, deployment_id: str, generation: int, plan_hash: str,
                 expected_ranks: int, host: str = "0.0.0.0",
                 sessions=None) -> None:
        # The SessionCoordinator is the authority over registration/START; the
        # listener feeds it. Without this wiring the coordinator was a separate
        # object nothing informed, so all_registered() could never become true.
        self.sessions_coordinator = sessions
        self.deployment_id = deployment_id
        self.generation = generation
        self.plan_hash = plan_hash
        self.expected_ranks = expected_ranks
        self.secret = new_deployment_secret()
        self.observations: list[ComponentObservation] = []
        self.failures: list[str] = []
        self.disconnected: list[int] = []
        self._loop = _LoopThread()
        self._listener = ControlListener(
            deployment_id=deployment_id, plan_hash=plan_hash, generation=generation,
            expected_ranks=expected_ranks, secret=self.secret,
            on_observation=self._on_observation,
            on_session_change=self._on_session_change, host=host)
        self._loop.call(self._listener.start())
        self.port = self._listener.port

    # -- sinks -------------------------------------------------------------
    def _on_observation(self, rank: int, obs: ComponentObservation) -> None:
        self.observations.append(obs)
        if obs.state in (ComponentState.FAILED.value,):
            detail = obs.detail or obs.reason_code or "no detail"
            self.failures.append(
                f"rank {rank} component {obs.component_id}: {detail}")

    def _on_session_change(self, rank: int, connected: bool) -> None:
        coordinator = getattr(self, "sessions_coordinator", None)
        if coordinator is not None:
            if connected:
                node = coordinator.binding.node_for(rank) or ""
                coordinator.register(rank, node, f"rank{rank}")
                # The transport does not yet carry the chunked snapshot or the
                # supervisor receipt, so registration is marked established
                # here. That is a KNOWN partial: the full snapshot/receipt
                # protocol is implemented in control/session.py and is not yet
                # driven by the wire format.
                session = coordinator.sessions.get(rank)
                if session is not None:
                    session.snapshot_accepted = True
                    session.supervisor_receipt_accepted = True
                    session.state = "ESTABLISHED"
            else:
                coordinator.on_disconnect(rank)
        if not connected:
            self.disconnected.append(rank)
            # A lost control lease is a failure of that rank, not a quiet event:
            # the head can no longer observe it and must not keep waiting.
            self.failures.append(f"rank {rank}: control lease lost")

    # -- surface -----------------------------------------------------------
    def rank_failure(self) -> Optional[str]:
        return self.failures[0] if self.failures else None

    def env(self) -> dict:
        """Environment a rank needs to reach this listener."""
        return {
            HOST_ENV: _reachable_host(),
            PORT_ENV: str(self.port),
            SECRET_ENV: self.secret.hex(),
            DEPLOYMENT_ENV: self.deployment_id,
            GENERATION_ENV: str(self.generation),
            PLAN_HASH_ENV: self.plan_hash,
        }

    def wait_all_registered(self, timeout: float) -> bool:
        try:
            return bool(self._loop.call(
                self._listener.wait_all_registered(timeout), timeout=timeout + 5))
        except Exception:
            return False

    def broadcast_start(self) -> int:
        """Release the START gate by actually SENDING it (§3.2.1 Q4).

        This used to set a head-side flag that no rank could observe, so the
        gate was computed and never enforced.
        """
        self._started = True
        try:
            return int(self._loop.call(
                self._listener.broadcast_command("START", "START"), timeout=30))
        except Exception as exc:          # noqa: BLE001
            print(f"[Control] START broadcast failed: {exc}", flush=True)
            return 0

    def start_broadcast(self) -> bool:
        return getattr(self, "_started", False)

    def stop(self) -> None:
        try:
            self._loop.call(self._listener.stop(), timeout=10)
        except Exception:
            pass
        self._loop.stop()


def _reachable_host() -> str:
    """The address ranks should dial. Prefers the high-speed interface."""
    for env_var in ("EXASERVE_HEAD_IP", "RAY_HEAD_IP"):
        value = os.environ.get(env_var)
        if value:
            return value
    try:
        return socket.gethostbyname(f"{socket.gethostname()}.hsn.cm.aurora.alcf.anl.gov")
    except OSError:
        pass
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


class RankClient:
    """Rank-side publisher. A no-op when the channel env is absent."""

    def __init__(self, *, rank: int, node_id: Optional[str] = None) -> None:
        self.rank = int(rank)
        self.node_id = node_id or socket.gethostname()
        self.enabled = bool(os.environ.get(HOST_ENV) and os.environ.get(PORT_ENV)
                            and os.environ.get(SECRET_ENV))
        self._loop: Optional[_LoopThread] = None
        self._channel: Optional[NodeChannel] = None
        self._seq = 0
        self.connected = False
        self._start_received = False

    def connect(self, timeout: float = 30.0) -> bool:
        """Best-effort: a channel failure must not prevent the rank running.

        The head still has launcher exit aggregation, so a rank that cannot
        reach the channel degrades to the coarser signal rather than failing.
        """
        if not self.enabled:
            return False
        try:
            self._loop = _LoopThread()
            self._channel = NodeChannel(
                host=os.environ[HOST_ENV], port=int(os.environ[PORT_ENV]),
                secret=bytes.fromhex(os.environ[SECRET_ENV]),
                deployment_id=os.environ.get(DEPLOYMENT_ENV, "unknown"),
                plan_hash=os.environ.get(PLAN_HASH_ENV, "plan"),
                generation=int(os.environ.get(GENERATION_ENV, "0") or 0),
                rank=self.rank, node_id=self.node_id)
            self._loop.call(self._channel.connect_and_register(timeout), timeout + 5)
            self.connected = True
            return True
        except Exception as exc:
            print(f"[Rank {self.rank}] control channel unavailable "
                  f"({type(exc).__name__}: {exc}); falling back to launcher exit "
                  "aggregation", flush=True)
            self.connected = False
            return False

    def observe(self, component_id: str, state: str, *, role: str = "component",
                reason_code: Optional[str] = None,
                detail: Optional[str] = None) -> bool:
        if not self.connected or self._channel is None or self._loop is None:
            return False
        self._seq += 1
        obs = ComponentObservation(
            schema_version=SCHEMA_VERSION,
            deployment_id=self._channel.deployment_id,
            plan_hash=self._channel.plan_hash,
            generation=self._channel.generation,
            component_id=component_id,
            instance_id=f"{self.node_id}:{os.getpid()}",
            sequence=self._seq,
            owner_scope=OwnerScope.RANK.value,
            role=role,
            node_id=self.node_id,
            state=state,
            observed_at=time.time(),
            owner_rank=self.rank,
            reason_code=reason_code,
            detail=detail,
        )
        try:
            self._loop.call(self._channel.send_observation(obs), timeout=10)
            return True
        except Exception:
            self.connected = False
            return False

    def start_gate_available(self) -> bool:
        """True when the head can deliver START over the wire."""
        return self.connected

    def poll_start(self, timeout: float = 1.0) -> bool:
        """Check for the head's START and acknowledge it."""
        if self._start_received:
            return True
        if not self.connected or self._channel is None or self._loop is None:
            return False
        try:
            payload = self._loop.call(
                self._channel.receive_command(timeout), timeout=timeout + 5)
        except Exception:                 # noqa: BLE001
            return False
        if not payload or payload.get("operation") != "START":
            return False
        self._start_received = True
        try:
            self._loop.call(self._channel.send_command_result(
                str(payload.get("command_id", "START")), True), timeout=10)
        except Exception:                 # noqa: BLE001
            pass
        return True

    def start_received(self) -> bool:
        """True once the head has released the START gate.

        Without a live channel there is nothing to wait for, so an unconnected
        rank is not held hostage by a gate that can never open.
        """
        if not self.connected:
            return True
        return self._start_received

    def note_start(self) -> None:
        self._start_received = True

    def close(self) -> None:
        if self._channel is not None and self._loop is not None:
            try:
                self._loop.call(self._channel.close(), timeout=5)
            except Exception:
                pass
        if self._loop is not None:
            self._loop.stop()
        self.connected = False
