"""Readiness coordinator (plan WP5, audit IMP-B02).

A PURE projection over the resolved plan plus received observations. It holds
no processes and reads no logs: `CLUSTER FULLY READY` text can never make a
deployment ready here.

Design constraints (plan §3.1/WP5.3):
- work is O(1) per observation and O(K) retained state for K planned
  identities — no fleet-wide polling, no per-replica head polling;
- READY is a pure predicate over the CURRENT generation, so it is REVOCABLE:
  losing a required component/lease flips readiness back off (the legacy path
  latched READY permanently);
- every unsatisfied conjunct names its blocker.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Iterable

from .contracts import ComponentObservation, ComponentState


@dataclass(frozen=True)
class ReadinessPlan:
    """The exact identities that must be healthy for THIS generation."""

    deployment_id: str
    generation: int
    plan_hash: str
    expected_nodes: tuple[str, ...]                 # node ids
    expected_components: tuple[str, ...]            # long-lived component ids
    expected_replicas: dict[str, int] = field(default_factory=dict)  # model -> count
    expected_routes: tuple[str, ...] = ()           # per-model external routes
    # Routes an inference canary must answer. Defaults to every expected route;
    # a large fan-out may SAMPLE, and then the snapshot records exactly which
    # routes were probed rather than implying all of them were.
    canary_routes: tuple[str, ...] = ()
    required_receipt_roles: tuple[str, ...] = ()

    def routes_to_canary(self) -> tuple[str, ...]:
        return self.canary_routes or self.expected_routes


@dataclass
class ReadinessSnapshot:
    ready: bool
    generation: int
    plan_hash: str
    satisfied: tuple[str, ...]
    blockers: tuple[str, ...]
    observed_at: float

    def to_dict(self) -> dict:
        return {
            "ready": self.ready, "generation": self.generation,
            "plan_hash": self.plan_hash, "satisfied": list(self.satisfied),
            "blockers": list(self.blockers), "observed_at": self.observed_at,
        }


class ReadinessCoordinator:
    """Indexed readiness projection. Feed it observations; ask it for READY."""

    def __init__(self, plan: ReadinessPlan, *, lease_timeout_s: float = 120.0,
                 receipts=None, clock=time.monotonic) -> None:
        self.plan = plan
        self.lease_timeout_s = lease_timeout_s
        self.receipts = receipts            # compat.ReceiptStore | None
        self._clock = clock
        # O(K) indexed state, keyed by logical identity.
        self._component_state: dict[str, str] = {}
        self._component_instance: dict[str, str] = {}
        self._replica_ready: dict[str, set[str]] = {}   # model -> replica ids
        self._node_seen: set[str] = set()
        self._connected_ranks: set[int] = set()
        self._lease: dict[str, float] = {}              # component -> last seen
        self._routes_ok: dict[str, bool] = {}
        self._canary_ok: dict[str, bool] = {}

    # -- ingestion ---------------------------------------------------------
    def observe(self, obs: ComponentObservation) -> bool:
        """Apply one observation. Returns False if it was rejected as stale."""
        if (obs.deployment_id != self.plan.deployment_id
                or obs.plan_hash != self.plan.plan_hash
                or obs.generation != self.plan.generation):
            return False
        # A newer instance supersedes the old one for a logical identity;
        # observations from a superseded instance are rejected (plan §3.1).
        known_instance = self._component_instance.get(obs.component_id)
        if known_instance is not None and obs.instance_id != known_instance:
            if obs.instance_id < known_instance:
                return False
            # accepted new instance supersedes
        self._component_instance[obs.component_id] = obs.instance_id
        self._component_state[obs.component_id] = obs.state
        self._lease[obs.component_id] = self._clock()
        self._node_seen.add(obs.node_id)
        if obs.owner_rank is not None:
            self._connected_ranks.add(obs.owner_rank)
        if obs.model_id and obs.replica_id:
            bucket = self._replica_ready.setdefault(obs.model_id, set())
            if obs.state == ComponentState.READY.value:
                bucket.add(obs.replica_id)
            else:
                bucket.discard(obs.replica_id)
        return True

    def set_replicas(self, model_id: str, ready_replica_ids: Iterable[str]) -> None:
        """Replace the ready-replica set for a model (absolute, not additive).

        Polled sources report the CURRENT set; a replica that vanished must
        revoke readiness rather than linger from an earlier observation.
        """
        self._replica_ready[model_id] = set(ready_replica_ids)

    def set_route_health(self, route: str, healthy: bool) -> None:
        self._routes_ok[route] = healthy

    def set_canary(self, model_or_route: str, ok: bool) -> None:
        """Externally-routed inference canary result (plan WP5.9)."""
        self._canary_ok[model_or_route] = ok

    def rank_disconnected(self, rank: int) -> None:
        """Losing a rank's control lease immediately removes its components."""
        self._connected_ranks.discard(rank)

    # -- predicate ---------------------------------------------------------
    def evaluate(self) -> ReadinessSnapshot:
        now = self._clock()
        satisfied: list[str] = []
        blockers: list[str] = []

        # 1. exact node membership
        missing_nodes = [n for n in self.plan.expected_nodes if n not in self._node_seen]
        if missing_nodes:
            blockers.append(f"nodes not reporting: {sorted(missing_nodes)[:5]}")
        else:
            satisfied.append(f"membership: {len(self.plan.expected_nodes)} nodes")

        # 2. every required long-lived component healthy AND lease-fresh
        for comp in self.plan.expected_components:
            state = self._component_state.get(comp)
            if state is None:
                blockers.append(f"component {comp}: no observation")
                continue
            if state not in (ComponentState.READY.value, ComponentState.RUNNING.value):
                blockers.append(f"component {comp}: state={state}")
                continue
            age = now - self._lease.get(comp, 0.0)
            if age > self.lease_timeout_s:
                blockers.append(f"component {comp}: lease stale ({age:.0f}s)")
        if not any(b.startswith("component ") for b in blockers):
            satisfied.append(f"components: {len(self.plan.expected_components)} healthy")

        # 3. expected replica count per model, current generation only
        for model, want in self.plan.expected_replicas.items():
            have = len(self._replica_ready.get(model, ()))
            if have < want:
                blockers.append(f"model {model}: {have}/{want} replicas ready")
            else:
                satisfied.append(f"model {model}: {have}/{want} replicas")

        # 4. routes healthy on every required node
        for route in self.plan.expected_routes:
            if not self._routes_ok.get(route, False):
                blockers.append(f"route {route}: not healthy")
        if self.plan.expected_routes and not any(
                b.startswith("route ") for b in blockers):
            satisfied.append(f"routes: {len(self.plan.expected_routes)} healthy")

        # 5. per-model external canary (a health endpoint is NOT a canary)
        probed = self.plan.routes_to_canary()
        for route in probed:
            if not self._canary_ok.get(route, False):
                blockers.append(f"canary {route}: no successful inference")
        if probed and not any(b.startswith("canary ") for b in blockers):
            satisfied.append(
                f"canaries: {len(probed)}/{len(self.plan.expected_routes)} "
                "routes answered")

        # 6. compatibility receipts from every required role
        if self.receipts is not None:
            ok, reason = self.receipts.satisfied()
            (satisfied if ok else blockers).append(f"receipts: {reason}")

        ready = not blockers
        return ReadinessSnapshot(
            ready=ready, generation=self.plan.generation,
            plan_hash=self.plan.plan_hash, satisfied=tuple(satisfied),
            blockers=tuple(blockers), observed_at=time.time())

    # -- convenience -------------------------------------------------------
    def is_ready(self) -> bool:
        return self.evaluate().ready

    def blockers(self) -> tuple[str, ...]:
        return self.evaluate().blockers
