"""Readiness derived from the compiled plan (plan §3.2.1 Q3, packet P03, IMP-B02).

The previous gate inferred what it expected from what it found: expected nodes
came from `ray.nodes()`, expected applications and replica targets from
whatever Serve currently reported. A planned node that never appeared, or an
application that never deployed, therefore *shrank the expectation* instead of
blocking READY — the check could not fail for the one reason it existed.

Here the expectation is the compiled `DeploymentPlan` and its
`AllocationBinding`. Nothing observed can enlarge or shrink it.

The sequence is fixed (§3.2.1 Q3):

    1. deploy Serve, collect typed current-generation evidence
    2. VALIDATING: establish the compiled advertised endpoint
       - PROXIED_INTERNAL -> the global gateway process
       - DIRECT_VALIDATION -> the declared Serve endpoint (validation only)
    3. verify endpoint, gateway process/health/routes, exact receipts, and a
       per-model canary *through that endpoint*
    4. atomically persist the single READY transition

After READY, an unexpected gateway-process exit goes straight to terminal
FAILED. A live gateway that fails health/route/canary goes READY -> VALIDATING
with the persisted record revoked, and may return to READY on successful
revalidation within the recovery deadline.
"""

from __future__ import annotations

import math
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from enum import Enum
from typing import Callable, Optional


class DeploymentPhase(str, Enum):
    DEPLOYING = "DEPLOYING"
    VALIDATING = "VALIDATING"
    READY = "READY"
    FAILED = "FAILED"


PROXY_ANCHOR_APP_PREFIX = "_exaserve_proxy_anchor_r"
_UNPLANNED_EVIDENCE_MARGIN = 64


def _required_text(value, *, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"readiness {label} must be non-empty text")
    return value


def _required_bool(value, *, label: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"readiness {label} must be a boolean")
    return value


def _optional_bool(value, *, label: str) -> Optional[bool]:
    if value is not None and type(value) is not bool:
        raise ValueError(f"readiness {label} must be null or a boolean")
    return value


def _nonnegative_int(value, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"readiness {label} must be a non-negative integer")
    return value


def _finite_resource(value, *, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise ValueError(f"readiness {label} must be finite and non-negative")
    return float(value)


def planned_proxy_anchor_names(plan) -> frozenset[str]:
    """Return one internal proxy-anchor application for every planned rank.

    Ray's public ``ProxyLocation.EveryNode`` means every node that hosts at
    least one *Serve replica*, not literally every Ray node. Pipeline stages
    and otherwise idle allocation ranks therefore need a tiny, route-less,
    node-pinned Serve application to make the plan's per-node proxy topology
    real. The names are derived from the immutable node count and are part of
    the exact application predicate; observed applications can never invent or
    remove them.
    """
    return frozenset(f"{PROXY_ANCHOR_APP_PREFIX}{rank}" for rank in range(plan.num_nodes))


def planned_application_names(plan) -> frozenset[str]:
    """Return the exact model and infrastructure Serve application set."""
    names: set[str] = set(planned_proxy_anchor_names(plan))
    for model in plan.models:
        if model.num_replicas > 1:
            names.update(f"{model.route_name}_r{index}" for index in range(model.num_replicas))
        elif len(plan.models) == 1:
            # A named root-route application coexists with the internal proxy
            # anchors. Ray's reserved default application has replace-all
            # semantics and could delete those anchors on deployment.
            names.add(model.route_name)
        else:
            names.add(model.route_name)
    return frozenset(names)


@dataclass
class ReadinessVerdict:
    ready: bool
    phase: str
    blockers: tuple
    satisfied: tuple
    advertised_endpoint: str
    generation: int
    deployment_plan_hash: str
    allocation_binding_hash: str
    missing_identities: tuple = ()
    unhealthy_identities: tuple = ()
    receipt_hashes: tuple = ()
    model_map: dict = field(default_factory=dict)
    capability_map: dict = field(default_factory=dict)
    nodes: tuple = ()
    proxies: tuple = ()
    observed_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "ready": self.ready,
            "phase": self.phase,
            "blockers": list(self.blockers),
            "satisfied": list(self.satisfied),
            "advertised_endpoint": self.advertised_endpoint,
            "generation": self.generation,
            "deployment_plan_hash": self.deployment_plan_hash,
            "allocation_binding_hash": self.allocation_binding_hash,
            "missing_identities": list(self.missing_identities),
            "unhealthy_identities": list(self.unhealthy_identities),
            "receipt_hashes": list(self.receipt_hashes),
            "model_map": self.model_map,
            "capability_map": self.capability_map,
            "nodes": list(self.nodes),
            "proxies": list(self.proxies),
            "observed_at": self.observed_at,
        }


class ReadinessCoordinator:
    """Readiness as a predicate over the PLAN, not over the survivors."""

    def __init__(
        self, *, plan, binding, receipts, sessions=None, log: Callable[[str], None] = print
    ) -> None:
        self.plan = plan
        self.binding = binding
        self.receipts = receipts  # ExactReceiptLedger
        self.sessions = sessions  # SessionCoordinator | None
        self._log = log
        self._planned_ranks = frozenset(binding.ranks())
        self._planned_model_ids = frozenset(model.model_id for model in plan.models)
        self._planned_routes = frozenset(model.route_name for model in plan.models)
        self._planned_applications = planned_application_names(plan)
        self.phase = DeploymentPhase.DEPLOYING.value
        self.advertised_endpoint = ""
        self._gateway_alive: Optional[bool] = None
        self._gateway_healthy: Optional[bool] = None
        self._routes_ok: dict = {}
        self._canary_ok: dict = {}
        self._replicas_running: dict = {}
        self._nodes: tuple[dict, ...] = ()
        self._proxies: tuple[dict, ...] = ()
        self._rank_components: dict[int, bool] = {}
        self._owned_components: dict[str, bool] = {}
        self._application_names: frozenset[str] = frozenset()
        self.first_failure: Optional[str] = None

    # -- inputs ------------------------------------------------------------
    def set_advertised_endpoint(self, endpoint: str) -> None:
        self.advertised_endpoint = _required_text(endpoint, label="advertised endpoint")

    def set_gateway(self, *, alive: Optional[bool], healthy: Optional[bool]) -> None:
        self._gateway_alive = _optional_bool(alive, label="gateway alive state")
        self._gateway_healthy = _optional_bool(healthy, label="gateway health state")

    def set_route(self, route: str, healthy: bool) -> None:
        route = _required_text(route, label="route")
        if route not in self._planned_routes:
            raise ValueError(f"readiness route {route!r} is not planned")
        self._routes_ok[route] = _required_bool(healthy, label=f"route {route!r} health")

    def set_canary(self, model_id: str, ok: bool) -> None:
        model_id = _required_text(model_id, label="canary model_id")
        if model_id not in self._planned_model_ids:
            raise ValueError(f"readiness canary model {model_id!r} is not planned")
        self._canary_ok[model_id] = _required_bool(ok, label=f"canary {model_id!r} result")

    def set_replicas(self, model_id: str, running: int, target: int) -> None:
        model_id = _required_text(model_id, label="replica model_id")
        if model_id not in self._planned_model_ids:
            raise ValueError(f"readiness replica model {model_id!r} is not planned")
        self._replicas_running[model_id] = (
            _nonnegative_int(running, label=f"model {model_id!r} running replicas"),
            _nonnegative_int(target, label=f"model {model_id!r} target replicas"),
        )

    def set_cluster(self, *, nodes, proxies) -> None:
        """Replace the absolute current cluster/proxy projection."""
        if not isinstance(nodes, Sequence) or isinstance(nodes, (str, bytes)):
            raise ValueError("readiness nodes must be a sequence")
        if not isinstance(proxies, Sequence) or isinstance(proxies, (str, bytes)):
            raise ValueError("readiness proxies must be a sequence")
        limit = int(self.plan.num_nodes) + _UNPLANNED_EVIDENCE_MARGIN
        if len(nodes) > limit or len(proxies) > limit:
            raise ValueError("readiness cluster projection exceeds its plan-relative bound")
        node_fields = {"node_id", "node_name", "node_address", "alive", "cpu", "gpu"}
        node_projection = []
        node_ids: set[str] = set()
        for index, item in enumerate(nodes):
            if not isinstance(item, Mapping) or set(item) != node_fields:
                raise ValueError(f"readiness node {index} has invalid fields")
            node = dict(item)
            for field_name in ("node_id", "node_name", "node_address"):
                _required_text(node[field_name], label=f"node {index} {field_name}")
            if node["node_id"] in node_ids:
                raise ValueError(f"readiness node_id {node['node_id']!r} is duplicated")
            node_ids.add(node["node_id"])
            _required_bool(node["alive"], label=f"node {index} alive state")
            _finite_resource(node["cpu"], label=f"node {index} CPU")
            _finite_resource(node["gpu"], label=f"node {index} GPU")
            node_projection.append(node)
        proxy_projection = []
        proxy_nodes: set[str] = set()
        for index, item in enumerate(proxies):
            if not isinstance(item, Mapping) or set(item) != {"node_id", "status"}:
                raise ValueError(f"readiness proxy {index} has invalid fields")
            proxy = dict(item)
            node_id = _required_text(proxy["node_id"], label=f"proxy {index} node_id")
            _required_text(proxy["status"], label=f"proxy {index} status")
            if node_id in proxy_nodes:
                raise ValueError(f"readiness proxy node_id {node_id!r} is duplicated")
            proxy_nodes.add(node_id)
            proxy_projection.append(proxy)
        self._nodes = tuple(node_projection)
        self._proxies = tuple(proxy_projection)

    def set_rank_component(self, rank: int, healthy: bool) -> None:
        rank = _nonnegative_int(rank, label="rank component owner")
        if rank not in self._planned_ranks:
            raise ValueError(f"readiness rank {rank} is not planned")
        self._rank_components[rank] = _required_bool(healthy, label=f"rank {rank} component health")

    def set_owned_component(self, component_id: str, healthy: bool) -> None:
        component_id = _required_text(component_id, label="owned component_id")
        if component_id not in {"rank_launcher", "deployment"}:
            raise ValueError(f"readiness global component {component_id!r} is not planned")
        self._owned_components[component_id] = _required_bool(
            healthy, label=f"component {component_id!r} health"
        )

    def set_applications(self, names) -> None:
        if isinstance(names, (str, bytes)) or not isinstance(names, Iterable):
            raise ValueError("readiness applications must be an iterable of names")
        application_names = []
        limit = len(self._planned_applications) + _UNPLANNED_EVIDENCE_MARGIN
        for index, name in enumerate(names):
            if index >= limit:
                raise ValueError("readiness application projection exceeds its plan-relative bound")
            application_names.append(
                _required_text(name, label=f"application name at index {index}")
            )
        if len(application_names) != len(set(application_names)):
            raise ValueError("readiness application projection contains duplicate names")
        self._application_names = frozenset(application_names)

    # -- predicate ---------------------------------------------------------
    def evaluate(self) -> ReadinessVerdict:
        blockers: list = []
        satisfied: list = []
        missing: set[str] = set()
        unhealthy: set[str] = set()

        # 1. Every PLANNED rank session established. Observed set cannot shrink
        #    the expectation, because the expectation is the binding.
        planned_ranks = set(self.binding.ranks())
        if self.sessions is not None:
            revoked = set(self.sessions.readiness_revoked_ranks())
            if revoked:
                blockers.append(f"ranks not established: {sorted(revoked)[:8]}")
                missing.update(f"session/rank/{rank}" for rank in revoked)
            else:
                satisfied.append(f"sessions: {len(planned_ranks)} planned ranks established")
            if self.sessions.generation_state == "TERMINAL":
                blockers.append(f"generation terminal: {self.sessions.terminal_reason}")
                unhealthy.add("generation")

        # 1b. Exact live Ray membership/resources and every rank-owned Ray
        # process observation.  The allocation binding—not observed survivors—
        # supplies the expected set.
        from ..plan.contracts import canonical_node_id

        alive_nodes = [node for node in self._nodes if node.get("alive") is True]
        nodes_by_name: dict[str, list[dict]] = {}
        for node in alive_nodes:
            nodes_by_name.setdefault(canonical_node_id(node.get("node_name", "")), []).append(node)
        matched_ids: set[str] = set()
        for rank, expected_name in self.binding.rank_to_node:
            matches = nodes_by_name.get(canonical_node_id(expected_name), ())
            if len(matches) != 1:
                blockers.append(
                    f"ray membership rank {rank} node {expected_name}: {len(matches)} live matches"
                )
                missing.add(f"ray-node/rank/{rank}")
                continue
            node = matches[0]
            matched_ids.add(str(node["node_id"]))
            cpu, gpu = float(node["cpu"]), float(node["gpu"])
            wanted_cpu = float(self.plan.node_cpus)
            wanted_gpu = float(self.plan.num_gpus_per_node)
            if cpu < wanted_cpu or gpu < wanted_gpu:
                blockers.append(
                    f"ray resources rank {rank}: CPU {cpu:g}/{wanted_cpu:g}, "
                    f"GPU {gpu:g}/{wanted_gpu:g}"
                )
                unhealthy.add(f"ray-node/rank/{rank}/resources")
            elif not self.plan.readiness.allow_excess_resources and (
                cpu != wanted_cpu or gpu != wanted_gpu
            ):
                blockers.append(
                    f"ray resources rank {rank}: excess resources forbidden "
                    f"(CPU {cpu:g}, GPU {gpu:g})"
                )
                unhealthy.add(f"ray-node/rank/{rank}/resources")
        extra_ids = sorted(
            str(node["node_id"]) for node in alive_nodes if str(node["node_id"]) not in matched_ids
        )
        if extra_ids:
            blockers.append(f"ray membership has unplanned live nodes: {extra_ids[:6]}")
            unhealthy.update(f"ray-node/unplanned/{item}" for item in extra_ids)
        if not any(blocker.startswith(("ray membership", "ray resources")) for blocker in blockers):
            satisfied.append(f"ray membership/resources: {len(matched_ids)} exact nodes")
        for rank in self.binding.ranks():
            if not self._rank_components.get(rank, False):
                blockers.append(f"rank {rank} ray component: not fresh/running")
                unhealthy.add(f"component/rank/{rank}/ray")
        for component_id in ("rank_launcher", "deployment"):
            if not self._owned_components.get(component_id, False):
                blockers.append(f"owned component {component_id}: not running")
                unhealthy.add(f"component/global/{component_id}")

        expected_applications = planned_application_names(self.plan)
        missing_applications = sorted(expected_applications - self._application_names)
        unexpected_applications = sorted(self._application_names - expected_applications)
        if missing_applications:
            blockers.append(f"serve applications missing: {missing_applications[:6]}")
            missing.update(f"serve-application/{name}" for name in missing_applications)
        if unexpected_applications:
            blockers.append(f"serve applications unplanned: {unexpected_applications[:6]}")
            unhealthy.update(
                f"serve-application/unplanned/{name}" for name in unexpected_applications
            )
        if not missing_applications and not unexpected_applications:
            satisfied.append(f"serve applications: {len(expected_applications)} exact")

        # Serve EveryNode proxies must cover exactly the matched Ray node IDs.
        proxies = {str(item["node_id"]): str(item["status"]).upper() for item in self._proxies}
        for node_id in sorted(matched_ids):
            status = proxies.get(node_id, "MISSING")
            if "HEALTHY" not in status or "UNHEALTHY" in status:
                blockers.append(f"serve proxy {node_id}: status={status}")
                (missing if status == "MISSING" else unhealthy).add(f"serve-proxy/{node_id}")
        unplanned_proxies = sorted(set(proxies) - matched_ids)
        if unplanned_proxies:
            blockers.append(f"serve proxies on unplanned nodes: {unplanned_proxies[:6]}")
            unhealthy.update(f"serve-proxy/unplanned/{item}" for item in unplanned_proxies)
        if matched_ids and not any(blocker.startswith("serve prox") for blocker in blockers):
            satisfied.append(f"serve proxies: {len(matched_ids)} healthy")

        # 2. Exact receipt set equality against the plan's slots.
        ok, detail = self.receipts.satisfied()
        if ok:
            satisfied.append(f"receipts: {detail['accepted']}/{detail['planned']} exact slots")
        else:
            if detail["missing"]:
                blockers.append(f"receipts missing: {detail['missing'][:6]}")
                missing.update(f"receipt/{item}" for item in detail["missing"])
            if detail["unexpected"]:
                blockers.append(f"receipts unexpected: {detail['unexpected'][:6]}")
                unhealthy.update(f"receipt/unplanned/{item}" for item in detail["unexpected"])

        # 3. Every planned model's replicas, against the PLAN's target.
        for model in self.plan.models:
            running, observed_target = self._replicas_running.get(model.model_id, (0, 0))
            planned_target = model.num_replicas
            if observed_target != planned_target:
                blockers.append(
                    f"model {model.model_id}: observed target {observed_target}, "
                    f"planned {planned_target}"
                )
                unhealthy.add(f"model/{model.model_id}/replica-target")
            if running != planned_target:
                blockers.append(f"model {model.model_id}: {running}/{planned_target} replicas")
                (missing if running < planned_target else unhealthy).add(
                    f"model/{model.model_id}/replicas"
                )
            else:
                satisfied.append(f"model {model.model_id}: {running}/{planned_target}")

        # 4. The advertised endpoint must exist and, for production, be a live
        #    healthy gateway process.
        if not self.advertised_endpoint:
            blockers.append("no advertised endpoint established")
            missing.add("advertised-endpoint")
        elif self.plan.is_production_exposure():
            if self._gateway_alive is False:
                blockers.append("gateway process is not running")
                unhealthy.add("gateway/process")
            elif self._gateway_alive is None:
                blockers.append("gateway process state unknown")
                missing.add("gateway/process-observation")
            elif not self._gateway_healthy:
                blockers.append("gateway health check failed")
                unhealthy.add("gateway/health")
            else:
                satisfied.append(f"gateway {self.plan.gateway.kind} healthy")

        # 5. Routes + a real canary through the ADVERTISED endpoint.
        for model in self.plan.models:
            if not self._routes_ok.get(model.route_name, False):
                blockers.append(f"route {model.route_name}: not healthy")
                unhealthy.add(f"route/{model.route_name}")
            if not self._canary_ok.get(model.model_id, False):
                blockers.append(
                    f"canary {model.model_id}: no completion via the advertised endpoint"
                )
                missing.add(f"canary/{model.model_id}")
        if self.plan.models and not any(b.startswith("canary ") for b in blockers):
            satisfied.append(f"canaries: {len(self.plan.models)} model(s) answered")

        receipts = (
            self.receipts.accepted_receipts() if hasattr(self.receipts, "accepted_receipts") else ()
        )
        capability_map = {receipt.component_id: list(receipt.capabilities) for receipt in receipts}
        model_map = {}
        for model in self.plan.models:
            running, observed_target = self._replicas_running.get(model.model_id, (0, 0))
            model_map[model.model_id] = {
                "route_name": model.route_name,
                "expected_replicas": model.num_replicas,
                "observed_replicas": running,
                "observed_target": observed_target,
            }

        ready = not blockers
        return ReadinessVerdict(
            ready=ready,
            phase=self.phase,
            blockers=tuple(blockers),
            satisfied=tuple(satisfied),
            advertised_endpoint=self.advertised_endpoint,
            generation=self.binding.generation,
            deployment_plan_hash=self.plan.deployment_plan_hash,
            allocation_binding_hash=self.binding.allocation_binding_hash,
            missing_identities=tuple(sorted(missing)),
            unhealthy_identities=tuple(sorted(unhealthy)),
            receipt_hashes=tuple(sorted(r.receipt_hash for r in receipts)),
            model_map=model_map,
            capability_map=capability_map,
            nodes=tuple(dict(item) for item in self._nodes),
            proxies=tuple(dict(item) for item in self._proxies),
        )

    # -- transitions -------------------------------------------------------
    def enter_validating(self) -> None:
        if self.phase in (DeploymentPhase.FAILED.value,):
            return
        self.phase = DeploymentPhase.VALIDATING.value

    def commit_ready(self) -> ReadinessVerdict:
        """Commit the in-memory phase after the caller publishes status.

        Durable publication belongs to the sole ``DeploymentStatus`` writer.
        Keeping file I/O here previously created a second authority and a
        window in which ``readiness.json`` and ``deployment_status.json``
        disagreed.  The composition root now writes the verdict and READY state
        in one CAS transition, then calls this method.
        """
        verdict = self.evaluate()
        if not verdict.ready:
            return verdict
        self.phase = DeploymentPhase.READY.value
        verdict = replace(
            verdict, phase=self.phase, blockers=(), missing_identities=(), unhealthy_identities=()
        )
        return verdict

    def revoke(self, reason: str, *, gateway_dead: bool = False) -> str:
        """Post-READY loss. A dead gateway process is immediately terminal."""
        if gateway_dead:
            self.phase = DeploymentPhase.FAILED.value
            self.first_failure = self.first_failure or reason
            return DeploymentPhase.FAILED.value
        if self.phase == DeploymentPhase.READY.value:
            self.phase = DeploymentPhase.VALIDATING.value
            self._log(f"[Readiness] READY revoked -> VALIDATING: {reason}")
        return self.phase

    def fail(self, reason: str) -> None:
        self.phase = DeploymentPhase.FAILED.value
        self.first_failure = self.first_failure or reason


# Transitional import name only.  There is one implementation and production
# code imports the canonical ``ReadinessCoordinator`` from ``control.readiness``.
PlanReadiness = ReadinessCoordinator
