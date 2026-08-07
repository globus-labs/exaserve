"""The shared deployment-status surface (plan §3.4 boundary table, WP9).

The plan's boundary table is explicit about how evaluation and ClientLab may
learn what a deployment is doing:

    Eval/ClientLab -> deployment | Shared `DeploymentStatus`/event API |
    Generation-specific terminal/readiness state; no private process monitor
    or log grep

`state/status.py` already implemented the durable, lease-guarded, CAS-checked
record that boundary needs — and nothing wrote to it or read from it. The
composition root persisted its own `readiness.json`, so every consumer that
wanted deployment state either parsed that file's private shape or fell back to
grepping a log line, which is the untyped coupling the whole migration exists
to remove.

This module is the two halves of that boundary:

* `DeploymentStatusPublisher` — the writer. Only the composition root holds
  one; each transition is CAS-guarded, so a stale writer fails closed instead
  of overwriting a newer generation's state.
* `read_deployment_status` / `require_ready_endpoint` — the reader. Consumers
  get a typed record and the *compiled advertised endpoint*, and a deployment
  that is not READY raises rather than handing back an endpoint that may be
  serving nothing.

The record is generation-specific by construction: the generation, plan hash
and binding hash live in its provenance, so a consumer that reads a status
file left over from a previous generation can detect it rather than trusting
a filename.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

from .state.status import (
    DeploymentState,
    StatusConflict,
    StatusRecord,
    StatusStore,
)

STATUS_FILENAME = "deployment_status.json"


class DeploymentNotReady(RuntimeError):
    """The deployment exists but is not in a state a client may use."""


def status_path(run_dir: str) -> str:
    return os.path.join(run_dir, STATUS_FILENAME)


class DeploymentStatusPublisher:
    """Writer side. One per generation, owned by the composition root."""

    def __init__(self, run_dir: str, *, plan, binding=None, generation: int = 0,
                 log=print) -> None:
        self.path = status_path(run_dir)
        self.store = StatusStore.deployment(self.path)
        self.plan = plan
        self.binding = binding
        self.generation = generation
        self._log = log
        self._state = DeploymentState.PLANNED
        self._revision = -1

    # -- lifecycle ---------------------------------------------------------
    def initialize(self) -> Optional[StatusRecord]:
        """Create the record. A pre-existing one is NOT silently reused.

        Two generations writing one status file is how a stale READY survives
        into a run that never reached it; the conflict is reported and the
        publisher goes inert rather than taking over somebody else's record.
        """
        provenance = {
            "deployment_id": self.plan.deployment_id,
            "generation": self.generation,
            "deployment_plan_hash": self.plan.deployment_plan_hash,
            "site_profile_hash": self.plan.site_profile_hash,
            "allocation_binding_hash": (
                self.binding.allocation_binding_hash if self.binding else ""),
        }
        try:
            record = self.store.initialize(
                f"{self.plan.deployment_id}/gen{self.generation}",
                DeploymentState.PLANNED, provenance=provenance,
                data={"exposure_mode": self.plan.exposure.mode,
                      "num_nodes": self.plan.num_nodes})
        except (StatusConflict, OSError) as exc:
            self._log(f"[Status] not publishing: {exc}")
            self._revision = -1
            return None
        self._state = DeploymentState.PLANNED
        self._revision = record.revision
        return record

    def advance(self, new: DeploymentState, *, reason_code: str,
                detail: str = "", **data: Any) -> Optional[StatusRecord]:
        """One CAS-guarded step. Returns None when the record is not ours."""
        if self._revision < 0:
            return None
        try:
            record = self.store.transition(
                self._state, new, reason_code=reason_code, detail=detail or None,
                data_update=data or None, expected_revision=self._revision)
        except (StatusConflict, OSError) as exc:
            self._log(f"[Status] {self._state.value} -> {new.value} refused: {exc}")
            return None
        except Exception as exc:              # noqa: BLE001 - illegal transition
            self._log(f"[Status] {self._state.value} -> {new.value} illegal: {exc}")
            return None
        self._state = DeploymentState(new)
        self._revision = record.revision
        return record

    def advance_through(self, *states: DeploymentState, reason_code: str,
                        detail: str = "", **data: Any) -> Optional[StatusRecord]:
        """Walk several legal steps, e.g. PLANNED -> ... -> DEPLOYING."""
        record = None
        for state in states:
            record = self.advance(state, reason_code=reason_code,
                                  detail=detail, **data)
            if record is None:
                return None
        return record

    @property
    def state(self) -> str:
        return self._state.value


# -- reader side ----------------------------------------------------------
@dataclass(frozen=True)
class DeploymentStatus:
    """What a consumer is allowed to know, typed."""

    state: str
    revision: int
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    allocation_binding_hash: str
    advertised_endpoint: str
    exposure_mode: str
    reason_code: Optional[str]
    detail: Optional[str]

    @property
    def ready(self) -> bool:
        return self.state == DeploymentState.READY.value

    @property
    def terminal(self) -> bool:
        return self.state in (DeploymentState.FAILED.value,
                              DeploymentState.STOPPED.value,
                              DeploymentState.CANCELLED.value)


def read_deployment_status(run_dir: str) -> Optional[DeploymentStatus]:
    """Read the shared record. None when no deployment published one."""
    record = StatusStore.deployment(status_path(run_dir)).load()
    if record is None:
        return None
    provenance = record.provenance or {}
    data = record.data or {}
    return DeploymentStatus(
        state=record.state, revision=record.revision,
        deployment_id=str(provenance.get("deployment_id", "")),
        generation=int(provenance.get("generation", 0) or 0),
        deployment_plan_hash=str(provenance.get("deployment_plan_hash", "")),
        allocation_binding_hash=str(provenance.get("allocation_binding_hash", "")),
        advertised_endpoint=str(data.get("advertised_endpoint", "")),
        exposure_mode=str(data.get("exposure_mode", "")),
        reason_code=record.reason_code, detail=record.detail)


def require_ready_endpoint(run_dir: str, *, expected_generation: Optional[int] = None,
                           expected_plan_hash: str = "") -> str:
    """The advertised endpoint of a deployment that is READY *right now*.

    Every failure mode raises with the reason instead of returning a URL a
    client would then hammer: no record, wrong generation, wrong plan, not
    READY, or READY with no endpoint recorded.
    """
    status = read_deployment_status(run_dir)
    if status is None:
        raise DeploymentNotReady(
            f"no deployment status published under {run_dir}; a client must not "
            "guess an endpoint or grep a log for one")
    if expected_generation is not None and status.generation != expected_generation:
        raise DeploymentNotReady(
            f"status is for generation {status.generation}, expected "
            f"{expected_generation}; this record is from another run")
    if expected_plan_hash and status.deployment_plan_hash != expected_plan_hash:
        raise DeploymentNotReady(
            f"status plan {status.deployment_plan_hash[:12]} != expected "
            f"{expected_plan_hash[:12]}")
    if not status.ready:
        raise DeploymentNotReady(
            f"deployment is {status.state}"
            + (f" ({status.reason_code}: {status.detail})" if status.reason_code else ""))
    if not status.advertised_endpoint:
        raise DeploymentNotReady("deployment is READY but published no endpoint")
    return status.advertised_endpoint
