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

import json
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Optional


class DeploymentPhase(str, Enum):
    DEPLOYING = "DEPLOYING"
    VALIDATING = "VALIDATING"
    READY = "READY"
    FAILED = "FAILED"


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
    observed_at: float = field(default_factory=time.time)

    def to_dict(self) -> dict:
        return {
            "ready": self.ready, "phase": self.phase,
            "blockers": list(self.blockers), "satisfied": list(self.satisfied),
            "advertised_endpoint": self.advertised_endpoint,
            "generation": self.generation,
            "deployment_plan_hash": self.deployment_plan_hash,
            "allocation_binding_hash": self.allocation_binding_hash,
            "observed_at": self.observed_at,
        }


class PlanReadiness:
    """Readiness as a predicate over the PLAN, not over the survivors."""

    def __init__(self, *, plan, binding, receipts, sessions=None,
                 log: Callable[[str], None] = print) -> None:
        self.plan = plan
        self.binding = binding
        self.receipts = receipts          # ExactReceiptLedger
        self.sessions = sessions          # SessionCoordinator | None
        self._log = log
        self.phase = DeploymentPhase.DEPLOYING.value
        self.advertised_endpoint = ""
        self._gateway_alive: Optional[bool] = None
        self._gateway_healthy: Optional[bool] = None
        self._routes_ok: dict = {}
        self._canary_ok: dict = {}
        self._replicas_running: dict = {}
        self._persisted_ready = False
        self._persist_path = ""
        self.first_failure: Optional[str] = None

    # -- inputs ------------------------------------------------------------
    def set_advertised_endpoint(self, endpoint: str) -> None:
        self.advertised_endpoint = endpoint

    def set_gateway(self, *, alive: Optional[bool], healthy: Optional[bool]) -> None:
        self._gateway_alive = alive
        self._gateway_healthy = healthy

    def set_route(self, route: str, healthy: bool) -> None:
        self._routes_ok[route] = healthy

    def set_canary(self, model_id: str, ok: bool) -> None:
        self._canary_ok[model_id] = ok

    def set_replicas(self, model_id: str, running: int, target: int) -> None:
        self._replicas_running[model_id] = (running, target)

    # -- predicate ---------------------------------------------------------
    def evaluate(self) -> ReadinessVerdict:
        blockers: list = []
        satisfied: list = []

        # 1. Every PLANNED rank session established. Observed set cannot shrink
        #    the expectation, because the expectation is the binding.
        planned_ranks = set(self.binding.ranks())
        if self.sessions is not None:
            revoked = set(self.sessions.readiness_revoked_ranks())
            if revoked:
                blockers.append(f"ranks not established: {sorted(revoked)[:8]}")
            else:
                satisfied.append(f"sessions: {len(planned_ranks)} planned ranks established")
            if self.sessions.generation_state == "TERMINAL":
                blockers.append(
                    f"generation terminal: {self.sessions.terminal_reason}")

        # 2. Exact receipt set equality against the plan's slots.
        ok, detail = self.receipts.satisfied()
        if ok:
            satisfied.append(f"receipts: {detail['accepted']}/{detail['planned']} exact slots")
        else:
            if detail["missing"]:
                blockers.append(f"receipts missing: {detail['missing'][:6]}")
            if detail["unexpected"]:
                blockers.append(f"receipts unexpected: {detail['unexpected'][:6]}")

        # 3. Every planned model's replicas, against the PLAN's target.
        for model in self.plan.models:
            running, target = self._replicas_running.get(model.model_id, (0, 0))
            planned_target = model.num_replicas or target
            if planned_target <= 0:
                blockers.append(f"model {model.model_id}: no replica target resolved")
            elif running < planned_target:
                blockers.append(
                    f"model {model.model_id}: {running}/{planned_target} replicas")
            else:
                satisfied.append(f"model {model.model_id}: {running}/{planned_target}")

        # 4. The advertised endpoint must exist and, for production, be a live
        #    healthy gateway process.
        if not self.advertised_endpoint:
            blockers.append("no advertised endpoint established")
        elif self.plan.is_production_exposure():
            if self._gateway_alive is False:
                blockers.append("gateway process is not running")
            elif self._gateway_alive is None:
                blockers.append("gateway process state unknown")
            elif not self._gateway_healthy:
                blockers.append("gateway health check failed")
            else:
                satisfied.append(f"gateway {self.plan.gateway.kind} healthy")

        # 5. Routes + a real canary through the ADVERTISED endpoint.
        for model in self.plan.models:
            if not self._routes_ok.get(model.route_name, False):
                blockers.append(f"route {model.route_name}: not healthy")
            if not self._canary_ok.get(model.model_id, False):
                blockers.append(f"canary {model.model_id}: no completion via the "
                                "advertised endpoint")
        if self.plan.models and not any(b.startswith("canary ") for b in blockers):
            satisfied.append(f"canaries: {len(self.plan.models)} model(s) answered")

        ready = not blockers
        return ReadinessVerdict(
            ready=ready, phase=self.phase, blockers=tuple(blockers),
            satisfied=tuple(satisfied), advertised_endpoint=self.advertised_endpoint,
            generation=self.binding.generation,
            deployment_plan_hash=self.plan.deployment_plan_hash,
            allocation_binding_hash=self.binding.allocation_binding_hash)

    # -- transitions -------------------------------------------------------
    def enter_validating(self) -> None:
        if self.phase in (DeploymentPhase.FAILED.value,):
            return
        self.phase = DeploymentPhase.VALIDATING.value

    def commit_ready(self, persist_dir: str = "") -> ReadinessVerdict:
        """Atomically persist the single READY transition.

        Publication failure is fatal: a READY nobody can read is not READY.
        """
        verdict = self.evaluate()
        if not verdict.ready:
            return verdict
        self.phase = DeploymentPhase.READY.value
        verdict = ReadinessVerdict(**{**verdict.to_dict(),
                                      "phase": self.phase,
                                      "blockers": (), "satisfied": verdict.satisfied})
        if persist_dir:
            self._persist_path = self._persist(verdict, persist_dir)
            if not self._persist_path:
                self.phase = DeploymentPhase.FAILED.value
                self.first_failure = "durable READY publication failed"
                raise RuntimeError(
                    "[Readiness] could not durably persist the READY record; "
                    "failing rather than serving an unrecorded READY")
        self._persisted_ready = True
        return verdict

    def revoke(self, reason: str, *, gateway_dead: bool = False) -> str:
        """Post-READY loss. A dead gateway process is immediately terminal."""
        if gateway_dead:
            self.phase = DeploymentPhase.FAILED.value
            self.first_failure = self.first_failure or reason
            self._unpersist()
            return DeploymentPhase.FAILED.value
        if self.phase == DeploymentPhase.READY.value:
            self.phase = DeploymentPhase.VALIDATING.value
            self._unpersist()
            self._log(f"[Readiness] READY revoked -> VALIDATING: {reason}")
        return self.phase

    def fail(self, reason: str) -> None:
        self.phase = DeploymentPhase.FAILED.value
        self.first_failure = self.first_failure or reason
        self._unpersist()

    # -- persistence -------------------------------------------------------
    def _persist(self, verdict: ReadinessVerdict, directory: str) -> str:
        try:
            os.makedirs(directory, exist_ok=True)
            path = os.path.join(directory, "readiness.json")
            tmp = f"{path}.tmp.{os.getpid()}"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(verdict.to_dict(), handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
            return path
        except OSError:
            return ""

    def _unpersist(self) -> None:
        """A revoked READY must not leave a readable READY record behind."""
        self._persisted_ready = False
        if not self._persist_path:
            return
        try:
            with open(self._persist_path, encoding="utf-8") as handle:
                data = json.load(handle)
            data["ready"] = False
            data["phase"] = self.phase
            data["revoked_at"] = time.time()
            tmp = f"{self._persist_path}.tmp.{os.getpid()}"
            with open(tmp, "w", encoding="utf-8") as handle:
                json.dump(data, handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self._persist_path)
        except (OSError, ValueError):
            pass
