"""Per-rank supervision of node-local children (plan WP4.3, audit IMP-B01).

The S01 ownership topology is: one `RuntimeSupervisor` on the allocation head
owns a single `RankLauncher`; each launched rank runs a `NodeSupervisor` that
owns **its own node's** children and nothing else.

The invariant this exists to enforce, stated plainly: *no process manages a PID
it did not create.* Previously the head process reasoned about the whole
allocation through one MPI launch and a shared filesystem, so a "cleanup" on
the head had no way to distinguish a local child from a remote one — the only
reason it was safe is that it never really tried. Here ownership is explicit
and typed, and a remote rank is reached by the control channel or by the
launcher's own termination, never by signalling a PID.

Each rank reports typed `ComponentObservation`s (never stdout) with
`owner_scope=RANK` and its own rank/node identity, which is exactly what the
readiness authority accepts.
"""

from __future__ import annotations

import os
import socket
import time
from typing import Callable, Iterable, Optional

from .contracts import SCHEMA_VERSION, ComponentObservation, ComponentState, OwnerScope
from .supervisor import ManagedComponent, RuntimeSupervisor


class NodeSupervisor:
    """Owns the node-local components of exactly one rank."""

    def __init__(self, *, deployment_id: str, generation: int, plan_hash: str,
                 rank: int, node_id: Optional[str] = None,
                 instance_id: Optional[str] = None,
                 poll_interval_s: float = 2.0,
                 publish: Optional[Callable[[ComponentObservation], None]] = None) -> None:
        self.deployment_id = deployment_id
        self.generation = generation
        self.plan_hash = plan_hash
        self.rank = int(rank)
        self.node_id = node_id or socket.gethostname()
        self.instance_id = instance_id or f"{self.node_id}:{os.getpid()}"
        self.publish = publish
        self._supervisor = RuntimeSupervisor(poll_interval_s=poll_interval_s)
        self._sequence = 0

    # -- ownership ---------------------------------------------------------
    def adopt(self, component: ManagedComponent) -> ManagedComponent:
        """Take ownership of a LOCAL child.

        A component is only adoptable if this supervisor will be the process
        that started it; adopting a component that already has a process means
        somebody else created that PID.
        """
        if component.process is not None:
            raise ValueError(
                f"refusing to adopt already-started component "
                f"{component.component_id!r}: a rank must not manage a PID it "
                "did not create")
        component.owner_scope = OwnerScope.RANK.value
        component.owner_rank = self.rank
        return self._supervisor.register(component)

    @property
    def components(self) -> dict:
        return self._supervisor.components

    def install_signal_handlers(self) -> None:
        self._supervisor.install_signal_handlers()

    # -- lifecycle ---------------------------------------------------------
    def start_all(self) -> None:
        self._supervisor.start_all()
        for component in self._supervisor.components.values():
            self._emit(component, ComponentState.RUNNING.value)

    def observe_once(self) -> Optional[object]:
        """Poll every owned component and publish its state. Returns a cause."""
        cause = None
        for component in self._supervisor.components.values():
            state, code = component.observe()
            self._emit(component, state, exit_code=code)
            if state == ComponentState.FAILED.value and cause is None:
                self._supervisor.record_cause(
                    component.component_id, "UNEXPECTED_EXIT",
                    f"rank {self.rank} component exited unexpectedly", code)
                cause = self._supervisor.first_cause
        return cause

    def supervise(self, *, until: Optional[Callable[[], bool]] = None,
                  timeout_s: Optional[float] = None) -> Optional[object]:
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while True:
            cause = self.observe_once()
            if cause is not None:
                return cause
            if until is not None and until():
                return None
            if deadline is not None and time.monotonic() > deadline:
                self._supervisor.record_cause(
                    f"rank{self.rank}", "DEADLINE", f"exceeded {timeout_s}s")
                return self._supervisor.first_cause
            time.sleep(self._supervisor.poll_interval_s)

    def shutdown(self, *, drain_s: float = 30.0) -> None:
        """Bounded, idempotent cleanup of THIS node's children only."""
        self._supervisor.shutdown(drain_s=drain_s)
        for component in self._supervisor.components.values():
            self._emit(component, ComponentState.STOPPED.value)

    def record_cause(self, component_id: str, reason_code: str, detail: str,
                     exit_code: Optional[int] = None) -> None:
        self._supervisor.record_cause(component_id, reason_code, detail, exit_code)

    @property
    def first_cause(self):
        return self._supervisor.first_cause

    def exit_code(self) -> int:
        return self._supervisor.exit_code()

    # -- observations ------------------------------------------------------
    def _emit(self, component: ManagedComponent, state: str,
              exit_code: Optional[int] = None) -> ComponentObservation:
        self._sequence += 1
        observation = ComponentObservation(
            schema_version=SCHEMA_VERSION,
            deployment_id=self.deployment_id,
            plan_hash=self.plan_hash,
            generation=self.generation,
            component_id=component.component_id,
            instance_id=self.instance_id,
            sequence=self._sequence,
            owner_scope=OwnerScope.RANK.value,
            role=component.component_id.split("@")[0],
            node_id=self.node_id,
            state=state,
            observed_at=time.time(),
            owner_rank=self.rank,
            reason_code=(None if exit_code is None else "EXIT"),
            detail=(None if exit_code is None else f"exit={exit_code}"),
        )
        if self.publish is not None:
            self.publish(observation)
        return observation


def ray_component(argv: Iterable[str], *, env: Optional[dict] = None,
                  stdout=None) -> ManagedComponent:
    """The node-local Ray daemon as a supervised long-lived component.

    Ray head/worker is a real process boundary (WP4.5), so it stays a
    subprocess — but an owned one: argument array, its own process group,
    and an unexpected exit is fatal even with status 0.
    """
    return ManagedComponent(
        component_id="ray", argv=list(argv), env=env, stdout=stdout,
        long_lived=True)


def deployment_component(argv: Iterable[str], *, env: Optional[dict] = None,
                         stdout=None) -> ManagedComponent:
    """The rank-0 deployment child (`python -m exaserve.server`)."""
    return ManagedComponent(
        component_id="deployment", argv=list(argv), env=env, stdout=stdout,
        long_lived=True)
