"""Versioned deployment/run status with compare-and-set transitions (WP2.3).

One durable JSON record per deployment/run, published atomically. Writers
must hold the record's lease and name the state they believe is current;
a mismatch is a typed conflict, never a silent overwrite. The state machines
are the plan §WP5.1 deployment lifecycle and the eval run lifecycle
(WP9.6 distinguishes partial/cancelled/invalid from failed).
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from .atomic import ExclusiveLease, LeaseHeldError, atomic_write_json

SCHEMA_VERSION = 1

# Lease contention on a status record is transient (writers hold it for one
# read-modify-publish); wait bounded, then surface a typed conflict.
_LEASE_WAIT_S = 10.0
_LEASE_POLL_S = 0.05


class DeploymentState(str, Enum):
    PLANNED = "PLANNED"
    STAGING = "STAGING"
    CLUSTER_STARTING = "CLUSTER_STARTING"
    DEPLOYING = "DEPLOYING"
    VALIDATING = "VALIDATING"
    READY = "READY"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


_ACTIVE = {
    DeploymentState.PLANNED, DeploymentState.STAGING,
    DeploymentState.CLUSTER_STARTING, DeploymentState.DEPLOYING,
    DeploymentState.VALIDATING, DeploymentState.READY, DeploymentState.DRAINING,
}

DEPLOYMENT_TRANSITIONS: dict[DeploymentState, set[DeploymentState]] = {
    DeploymentState.PLANNED: {DeploymentState.STAGING},
    DeploymentState.STAGING: {DeploymentState.CLUSTER_STARTING},
    DeploymentState.CLUSTER_STARTING: {DeploymentState.DEPLOYING},
    DeploymentState.DEPLOYING: {DeploymentState.VALIDATING},
    DeploymentState.VALIDATING: {DeploymentState.READY},
    # Post-READY loss re-validates (plan WP5.1); explicit drain also legal.
    DeploymentState.READY: {DeploymentState.VALIDATING, DeploymentState.DRAINING},
    DeploymentState.DRAINING: {DeploymentState.STOPPED},
    DeploymentState.STOPPED: set(),
    DeploymentState.FAILED: set(),
    DeploymentState.CANCELLED: set(),
}
for _state in _ACTIVE:
    DEPLOYMENT_TRANSITIONS[_state] |= {DeploymentState.FAILED, DeploymentState.CANCELLED}


class RunState(str, Enum):
    PLANNED = "PLANNED"
    SUBMITTED = "SUBMITTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    INVALID = "INVALID"


RUN_TRANSITIONS: dict[RunState, set[RunState]] = {
    RunState.PLANNED: {RunState.SUBMITTED, RunState.CANCELLED, RunState.INVALID},
    RunState.SUBMITTED: {RunState.RUNNING, RunState.FAILED, RunState.CANCELLED,
                         # scheduler may report terminal without an observed RUNNING
                         RunState.SUCCEEDED, RunState.PARTIAL, RunState.INVALID},
    RunState.RUNNING: {RunState.SUCCEEDED, RunState.PARTIAL, RunState.FAILED,
                       RunState.CANCELLED, RunState.INVALID},
    RunState.SUCCEEDED: set(),
    RunState.PARTIAL: set(),
    RunState.FAILED: {RunState.SUBMITTED},  # explicit resubmission path
    RunState.CANCELLED: set(),
    RunState.INVALID: set(),
}


class StatusConflict(RuntimeError):
    """CAS failure: current state/revision differs from the caller's view."""


class IllegalTransition(ValueError):
    pass


@dataclass
class StatusRecord:
    schema_version: int
    kind: str  # "deployment" | "run"
    record_id: str
    state: str
    revision: int
    updated_at: float
    reason_code: str | None = None
    detail: str | None = None
    provenance: dict[str, Any] = field(default_factory=dict)
    data: dict[str, Any] = field(default_factory=dict)
    history: list[dict[str, Any]] = field(default_factory=list)


class StatusStore:
    """Durable single-record store with lease-guarded CAS transitions."""

    def __init__(self, path: str | os.PathLike, kind: str,
                 transitions: dict[Any, set[Any]], state_enum: type[Enum]) -> None:
        self.path = os.fspath(path)
        self.kind = kind
        self._transitions = transitions
        self._enum = state_enum

    # -- constructors --------------------------------------------------------
    @classmethod
    def deployment(cls, path: str | os.PathLike) -> "StatusStore":
        return cls(path, "deployment", DEPLOYMENT_TRANSITIONS, DeploymentState)

    @classmethod
    def run(cls, path: str | os.PathLike) -> "StatusStore":
        return cls(path, "run", RUN_TRANSITIONS, RunState)

    # -- I/O ------------------------------------------------------------------
    def load(self) -> StatusRecord | None:
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except FileNotFoundError:
            return None
        if raw.get("schema_version") != SCHEMA_VERSION:
            raise StatusConflict(
                f"unknown status schema_version={raw.get('schema_version')}")
        return StatusRecord(**raw)

    def _acquire_lease(self) -> ExclusiveLease:
        deadline = time.monotonic() + _LEASE_WAIT_S
        while True:
            try:
                return ExclusiveLease(self.path + ".lease", ttl_s=60).acquire()
            except LeaseHeldError as exc:
                if time.monotonic() >= deadline:
                    raise StatusConflict(
                        f"status lease busy beyond {_LEASE_WAIT_S}s: {exc}") from exc
                time.sleep(_LEASE_POLL_S)

    def initialize(self, record_id: str, state: Enum,
                   provenance: dict[str, Any] | None = None,
                   data: dict[str, Any] | None = None) -> StatusRecord:
        with self._acquire_lease():
            if self.load() is not None:
                raise StatusConflict(f"{self.path} already initialized")
            record = StatusRecord(
                schema_version=SCHEMA_VERSION, kind=self.kind, record_id=record_id,
                state=self._enum(state).value, revision=0, updated_at=time.time(),
                provenance=provenance or {}, data=data or {},
                history=[{"state": self._enum(state).value, "at": time.time(),
                          "reason_code": "INIT"}])
            atomic_write_json(self.path, asdict(record))
            return record

    def transition(self, expected: Enum | str, new: Enum | str, *,
                   reason_code: str, detail: str | None = None,
                   data_update: dict[str, Any] | None = None) -> StatusRecord:
        expected_state = self._enum(expected)
        new_state = self._enum(new)
        if new_state not in self._transitions[expected_state]:
            raise IllegalTransition(
                f"{self.kind}: {expected_state.value} -> {new_state.value}")
        with self._acquire_lease():
            record = self.load()
            if record is None:
                raise StatusConflict(f"{self.path} not initialized")
            if record.state != expected_state.value:
                raise StatusConflict(
                    f"expected {expected_state.value}, found {record.state}")
            record.state = new_state.value
            record.revision += 1
            record.updated_at = time.time()
            record.reason_code = reason_code
            record.detail = detail
            if data_update:
                record.data.update(data_update)
            record.history.append({"state": new_state.value, "at": record.updated_at,
                                   "reason_code": reason_code})
            atomic_write_json(self.path, asdict(record))
            return record
