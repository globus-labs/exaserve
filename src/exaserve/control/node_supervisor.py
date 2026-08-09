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
import math
import socket
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Callable, Iterable, Optional

from .contracts import SCHEMA_VERSION, ComponentObservation, ComponentState, OwnerScope
from .supervisor import ManagedComponent, RuntimeSupervisor


class NodeSupervisor:
    """Owns the node-local components of exactly one rank."""

    def __init__(
        self,
        *,
        deployment_id: str,
        generation: int,
        plan_hash: str,
        rank: int,
        node_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        poll_interval_s: float = 2.0,
        publish: Optional[Callable[[ComponentObservation], None]] = None,
    ) -> None:
        for name, value in (
            ("deployment_id", deployment_id),
            ("plan_hash", plan_hash),
        ):
            if not isinstance(value, str) or not value:
                raise ValueError(f"node supervisor {name} must be non-empty text")
        if (
            isinstance(generation, bool)
            or not isinstance(generation, int)
            or generation < 0
            or isinstance(rank, bool)
            or not isinstance(rank, int)
            or rank < 0
        ):
            raise ValueError("node supervisor generation/rank must be non-negative integers")
        if node_id is not None and (not isinstance(node_id, str) or not node_id):
            raise ValueError("node supervisor node_id must be null or non-empty text")
        if instance_id is not None and (not isinstance(instance_id, str) or not instance_id):
            raise ValueError("node supervisor instance_id must be null or non-empty text")
        if (
            isinstance(poll_interval_s, bool)
            or not isinstance(poll_interval_s, (int, float))
            or not math.isfinite(float(poll_interval_s))
            or poll_interval_s <= 0
        ):
            raise ValueError("node supervisor poll interval must be finite and positive")
        if publish is not None and not callable(publish):
            raise ValueError("node supervisor publish must be null or callable")
        self.deployment_id = deployment_id
        self.generation = generation
        self.plan_hash = plan_hash
        self.rank = rank
        self.node_id = node_id or socket.gethostname()
        self.instance_id = instance_id or f"{self.node_id}:{os.getpid()}"
        self.publish = publish
        self._supervisor = RuntimeSupervisor(poll_interval_s=poll_interval_s)
        self._probes: dict[str, HealthProbe] = {}
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
                "did not create"
            )
        component.owner_scope = OwnerScope.RANK.value
        component.owner_rank = self.rank
        return self._supervisor.register(component)

    @property
    def components(self) -> dict:
        return self._supervisor.components

    def add_probe(self, probe: "HealthProbe") -> "HealthProbe":
        if probe.component_id in self._probes:
            raise ValueError(f"duplicate local probe {probe.component_id!r}")
        self._probes[probe.component_id] = probe
        return probe

    def install_signal_handlers(self) -> None:
        self._supervisor.install_signal_handlers()

    # -- lifecycle ---------------------------------------------------------
    def start_all(self, *, rollback_s: float = 30.0) -> None:
        self._supervisor.start_all(rollback_s=rollback_s)
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
                    component.component_id,
                    "UNEXPECTED_EXIT",
                    f"rank {self.rank} component exited unexpectedly",
                    code,
                )
                cause = self._supervisor.first_cause
        for probe in self._probes.values():
            state, detail = probe.observe()
            self._emit(probe, state, detail=detail)
        return cause

    def supervise(
        self, *, until: Optional[Callable[[], bool]] = None, timeout_s: Optional[float] = None
    ) -> Optional[object]:
        deadline = None if timeout_s is None else time.monotonic() + timeout_s
        while True:
            # A SIGTERM sets the flag on the owned RuntimeSupervisor; this loop
            # ignored it, so a rank never drained and its children survived as
            # orphans (caught on 2 nodes: tree_reaped FAIL, 15 left).
            if self._supervisor._shutdown_requested:  # noqa: SLF001
                return self._supervisor.first_cause
            cause = self.observe_once()
            if cause is not None:
                return cause
            if until is not None and until():
                return None
            if deadline is not None and time.monotonic() > deadline:
                self._supervisor.record_cause(
                    f"rank{self.rank}", "DEADLINE", f"exceeded {timeout_s}s"
                )
                return self._supervisor.first_cause
            time.sleep(self._supervisor.poll_interval_s)

    def shutdown(
        self,
        *,
        drain_s: float = 30.0,
        deadline: Optional[float] = None,
        publish_observations: bool = True,
    ) -> bool:
        """Bounded, idempotent cleanup of THIS node's children only."""
        clean = self._supervisor.shutdown(drain_s=drain_s, deadline=deadline)
        if publish_observations:
            for component in self._supervisor.components.values():
                self._emit(component, component.state)
            for probe in self._probes.values():
                self._emit(probe, ComponentState.STOPPED.value)
        return clean

    def record_cause(
        self, component_id: str, reason_code: str, detail: str, exit_code: Optional[int] = None
    ) -> None:
        self._supervisor.record_cause(component_id, reason_code, detail, exit_code)

    @property
    def first_cause(self):
        return self._supervisor.first_cause

    def exit_code(self) -> int:
        return self._supervisor.exit_code()

    # -- observations ------------------------------------------------------
    def _emit(
        self, component, state: str, exit_code: Optional[int] = None, detail: Optional[str] = None
    ) -> ComponentObservation:
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
            role=getattr(component, "role", component.component_id.split("@")[0]),
            node_id=self.node_id,
            state=state,
            observed_at=time.time(),
            owner_rank=self.rank,
            reason_code=(None if exit_code is None else "EXIT"),
            detail=(
                detail if detail is not None else None if exit_code is None else f"exit={exit_code}"
            ),
        )
        if self.publish is not None:
            self.publish(observation)
        return observation


@dataclass
class HealthProbe:
    """A node-local observation source; it owns no process or remote PID."""

    component_id: str
    role: str
    check: Callable[[], tuple[bool, str]]

    def __post_init__(self) -> None:
        for name in ("component_id", "role"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"health probe {name} must be non-empty text")
        if not callable(self.check):
            raise ValueError("health probe check must be callable")

    def observe(self) -> tuple[str, str]:
        try:
            healthy, detail = self.check()
        except (OSError, ValueError, urllib.error.URLError) as exc:
            return ComponentState.STARTING.value, f"{type(exc).__name__}: {exc}"[:400]
        if not isinstance(healthy, bool) or not isinstance(detail, str):
            raise ValueError("health probe result must be (bool, str)")
        return (ComponentState.READY.value if healthy else ComponentState.STARTING.value), detail[
            :400
        ]


def serve_proxy_probe(port: int, *, host: str = "127.0.0.1", timeout_s: float = 1.0) -> HealthProbe:
    """Observe THIS node's Serve proxy with one bounded local health request."""
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("Serve proxy probe port must be in 1..65535")
    if not isinstance(host, str) or not host:
        raise ValueError("Serve proxy probe host must be non-empty")
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s <= 0
    ):
        raise ValueError("Serve proxy probe timeout must be finite and positive")

    def _check() -> tuple[bool, str]:
        request = urllib.request.Request(f"http://{host}:{port}/-/healthz")
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=timeout_s) as response:
                status = int(getattr(response, "status", response.getcode()))
        except (urllib.error.URLError, OSError) as exc:
            return False, f"{type(exc).__name__}: {exc}"
        return status == 200, f"HTTP {status}"

    return HealthProbe(component_id="serve_proxy", role="serve_proxy", check=_check)


def ray_head_endpoint_probe(port: int, *, host: str, timeout_s: float = 1.0) -> HealthProbe:
    """Typed availability observation for the planned Ray GCS endpoint."""
    if not isinstance(host, str) or not host.strip():
        raise ValueError("Ray head probe host must be non-empty")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("Ray head probe port must be in 1..65535")
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s <= 0
    ):
        raise ValueError("Ray head probe timeout must be finite and positive")

    def _check() -> tuple[bool, str]:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.settimeout(timeout_s)
            code = probe.connect_ex((host, port))
        return code == 0, (
            f"tcp://{host}:{port} accepts connections" if code == 0 else f"connect_ex={code}"
        )

    return HealthProbe(component_id="ray_head_endpoint", role="ray_head_endpoint", check=_check)


def ray_component(
    argv: Iterable[str], *, env: Optional[dict] = None, stdout=None
) -> ManagedComponent:
    """The node-local Ray daemon as a supervised long-lived component.

    Ray head/worker is a real process boundary (WP4.5), so it stays a
    subprocess — but an owned one: argument array, its own process group,
    and an unexpected exit is fatal even with status 0.
    """
    return ManagedComponent(component_id="ray", argv=argv, env=env, stdout=stdout, long_lived=True)
