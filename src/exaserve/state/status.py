"""Versioned deployment/run status with compare-and-set transitions (WP2.3).

One durable JSON record per deployment/run, published atomically. Writers
must hold the record's lease and name the state they believe is current;
a mismatch is a typed conflict, never a silent overwrite. The state machines
are the plan §WP5.1 deployment lifecycle and the eval run lifecycle
(WP9.6 distinguishes partial/cancelled/invalid from failed).
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any

from .atomic import ExclusiveLease, LeaseHeldError, atomic_write_json, strict_json_load_path

SCHEMA_VERSION = 1

# Lease contention on a status record is transient (writers hold it for one
# read-modify-publish); wait bounded, then surface a typed conflict.
_LEASE_WAIT_S = 10.0
_LEASE_POLL_S = 0.05
MAX_STATUS_HISTORY_EVENTS = 256
_HISTORY_DROPPED_KEY = "status_history_events_dropped"
_HISTORY_OMITTED_KEY = "status_history_events_omitted"
_INTERNAL_DATA_KEYS = frozenset({_HISTORY_DROPPED_KEY, _HISTORY_OMITTED_KEY})


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
    DeploymentState.PLANNED,
    DeploymentState.STAGING,
    DeploymentState.CLUSTER_STARTING,
    DeploymentState.DEPLOYING,
    DeploymentState.VALIDATING,
    DeploymentState.READY,
    DeploymentState.DRAINING,
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
    RunState.SUBMITTED: {
        RunState.RUNNING,
        RunState.FAILED,
        RunState.CANCELLED,
        # scheduler may report terminal without an observed RUNNING
        RunState.SUCCEEDED,
        RunState.PARTIAL,
        RunState.INVALID,
    },
    RunState.RUNNING: {
        RunState.SUCCEEDED,
        RunState.PARTIAL,
        RunState.FAILED,
        RunState.CANCELLED,
        RunState.INVALID,
    },
    RunState.SUCCEEDED: set(),
    RunState.PARTIAL: set(),
    # A terminal attempt owns immutable result and scheduler identities. A
    # retry is a new materialization, never a transition back into this run.
    RunState.FAILED: set(),
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

    def __init__(
        self,
        path: str | os.PathLike,
        kind: str,
        transitions: dict[Any, set[Any]],
        state_enum: type[Enum],
    ) -> None:
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
            raw = strict_json_load_path(self.path)
        except FileNotFoundError:
            return None
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            raise StatusConflict(f"status record is unreadable: {exc}") from exc
        expected = set(StatusRecord.__dataclass_fields__)
        if not isinstance(raw, dict) or set(raw) != expected:
            unknown = sorted(set(raw) - expected) if isinstance(raw, dict) else []
            missing = sorted(expected - set(raw)) if isinstance(raw, dict) else sorted(expected)
            raise StatusConflict(
                f"status record shape mismatch: unknown={unknown}, missing={missing}"
            )
        if (
            type(raw.get("schema_version")) is not int
            or raw.get("schema_version") != SCHEMA_VERSION
        ):
            raise StatusConflict(f"unknown status schema_version={raw.get('schema_version')}")
        if raw.get("kind") != self.kind:
            raise StatusConflict(f"status kind {raw.get('kind')!r} != expected {self.kind!r}")
        if not isinstance(raw.get("record_id"), str) or not raw["record_id"]:
            raise StatusConflict("status record_id must be a non-empty string")
        try:
            self._enum(raw.get("state"))
        except (TypeError, ValueError):
            raise StatusConflict(f"unknown {self.kind} state {raw.get('state')!r}") from None
        revision = raw.get("revision")
        updated_at = raw.get("updated_at")
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < 0:
            raise StatusConflict("status revision must be a non-negative integer")
        if (
            isinstance(updated_at, bool)
            or not isinstance(updated_at, (int, float))
            or not math.isfinite(float(updated_at))
            or updated_at < 0
        ):
            raise StatusConflict("status updated_at must be finite and non-negative")
        for name in ("reason_code", "detail"):
            if raw[name] is not None and not isinstance(raw[name], str):
                raise StatusConflict(f"status {name} must be null or a string")
        if raw["reason_code"] == "":
            raise StatusConflict("status reason_code must be null or non-empty")
        if not isinstance(raw["provenance"], dict) or not isinstance(raw["data"], dict):
            raise StatusConflict("status provenance and data must be objects")
        history = raw["history"]
        if (
            not isinstance(history, list)
            or not history
            or len(history) > revision + 1
            or len(history) > MAX_STATUS_HISTORY_EVENTS
        ):
            raise StatusConflict(
                "status history must be nonempty and within its revision/storage bounds"
            )
        dropped = raw["data"].get(_HISTORY_DROPPED_KEY, 0)
        if isinstance(dropped, bool) or not isinstance(dropped, int) or dropped < 0:
            raise StatusConflict(f"status {_HISTORY_DROPPED_KEY} is invalid")
        omitted = raw["data"].get(_HISTORY_OMITTED_KEY, 0)
        if isinstance(omitted, bool) or not isinstance(omitted, int) or omitted < 0:
            raise StatusConflict(f"status {_HISTORY_OMITTED_KEY} is invalid")
        if len(history) + dropped + omitted != revision + 1:
            raise StatusConflict("status history accounting disagrees with revision")
        if dropped and len(history) != MAX_STATUS_HISTORY_EVENTS:
            raise StatusConflict("status dropped history does not retain the full bounded window")
        previous_at = -1.0
        for index, event in enumerate(history):
            if not isinstance(event, dict) or set(event) != {"state", "at", "reason_code"}:
                raise StatusConflict(f"status history event {index} shape mismatch")
            try:
                self._enum(event["state"])
            except (TypeError, ValueError):
                raise StatusConflict(f"status history event {index} has unknown state") from None
            at = event["at"]
            if (
                isinstance(at, bool)
                or not isinstance(at, (int, float))
                or not math.isfinite(float(at))
                or at < previous_at
            ):
                raise StatusConflict(f"status history event {index} timestamp is invalid")
            if not isinstance(event["reason_code"], str) or not event["reason_code"]:
                raise StatusConflict(f"status history event {index} reason_code is invalid")
            previous_at = float(at)
        initial_state = next(iter(self._INITIAL_STATES[self.kind])).value
        if history[0]["state"] != initial_state or history[0]["reason_code"] != "INIT":
            raise StatusConflict("status history does not begin at the required initial state")
        for index in range(1, len(history)):
            # Once old records were dropped, history[0] and history[1] are not
            # adjacent. Every retained tail edge remains fully checkable.
            if index == 1 and dropped:
                continue
            previous_state = self._enum(history[index - 1]["state"])
            current_state = self._enum(history[index]["state"])
            if (
                current_state != previous_state
                and current_state not in self._transitions[previous_state]
            ):
                raise StatusConflict(
                    f"status history contains illegal transition "
                    f"{previous_state.value} -> {current_state.value}"
                )
        if history[-1]["state"] != raw["state"]:
            raise StatusConflict("status history tail disagrees with current state")
        try:
            return StatusRecord(**raw)
        except TypeError as exc:
            raise StatusConflict(f"status record is invalid: {exc}") from exc

    def _acquire_lease(self) -> ExclusiveLease:
        deadline = time.monotonic() + _LEASE_WAIT_S
        while True:
            try:
                return ExclusiveLease(self.path + ".lease", ttl_s=60).acquire()
            except LeaseHeldError as exc:
                if time.monotonic() >= deadline:
                    raise StatusConflict(
                        f"status lease busy beyond {_LEASE_WAIT_S}s: {exc}"
                    ) from exc
                time.sleep(_LEASE_POLL_S)

    @staticmethod
    def _validate_data_update(data: dict[str, Any] | None) -> None:
        if data is not None and not isinstance(data, dict):
            raise ValueError("status data must be an object or null")
        if data is not None and _INTERNAL_DATA_KEYS.intersection(data):
            reserved = sorted(_INTERNAL_DATA_KEYS.intersection(data))
            raise ValueError(f"status history accounting keys are owned by StatusStore: {reserved}")

    @staticmethod
    def _validate_reason_detail(reason_code: str, detail: str | None) -> None:
        if not isinstance(reason_code, str) or not reason_code:
            raise ValueError("status reason_code must be a non-empty string")
        if detail is not None and not isinstance(detail, str):
            raise ValueError("status detail must be null or a string")

    @staticmethod
    def _validate_expected_revision(expected_revision: int | None) -> None:
        if expected_revision is not None and (
            isinstance(expected_revision, bool)
            or not isinstance(expected_revision, int)
            or expected_revision < 0
        ):
            raise ValueError("status expected_revision must be null or non-negative integer")

    @staticmethod
    def _append_history(record: StatusRecord, event: dict[str, Any]) -> None:
        if len(record.history) >= MAX_STATUS_HISTORY_EVENTS:
            # Preserve the creation event and the newest bounded tail.  The
            # exact number omitted remains visible without allowing a flapping
            # READY/VALIDATING deployment to grow its durable status forever.
            del record.history[1]
            dropped = record.data.get(_HISTORY_DROPPED_KEY, 0)
            if isinstance(dropped, bool) or not isinstance(dropped, int) or dropped < 0:
                raise StatusConflict(f"status {_HISTORY_DROPPED_KEY} is invalid")
            record.data[_HISTORY_DROPPED_KEY] = dropped + 1
        record.history.append(event)

    # IMP-B07: a record may only be created in a legitimate INITIAL state.
    # Initializing directly as READY/SUCCEEDED would fabricate a terminal or
    # ready deployment without traversing its state machine.
    _INITIAL_STATES = {"deployment": {DeploymentState.PLANNED}, "run": {RunState.PLANNED}}

    def initialize(
        self,
        record_id: str,
        state: Enum,
        provenance: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
    ) -> StatusRecord:
        if not isinstance(record_id, str) or not record_id:
            raise ValueError("status record_id must be a non-empty string")
        if provenance is not None and not isinstance(provenance, dict):
            raise ValueError("status provenance must be an object or null")
        self._validate_data_update(data)
        allowed = self._INITIAL_STATES[self.kind]
        if self._enum(state) not in allowed:
            raise IllegalTransition(
                f"{self.kind} records must start in "
                f"{sorted(s.value for s in allowed)}, got {self._enum(state).value}"
            )
        with self._acquire_lease():
            if self.load() is not None:
                raise StatusConflict(f"{self.path} already initialized")
            now = time.time()
            record = StatusRecord(
                schema_version=SCHEMA_VERSION,
                kind=self.kind,
                record_id=record_id,
                state=self._enum(state).value,
                revision=0,
                updated_at=now,
                provenance=dict(provenance or {}),
                data=dict(data or {}),
                history=[{"state": self._enum(state).value, "at": now, "reason_code": "INIT"}],
            )
            atomic_write_json(self.path, asdict(record))
            return record

    def transition(
        self,
        expected: Enum | str,
        new: Enum | str,
        *,
        reason_code: str,
        detail: str | None = None,
        data_update: dict[str, Any] | None = None,
        expected_revision: int | None = None,
    ) -> StatusRecord:
        """Compare-and-set a lifecycle transition.

        IMP-B07: comparing only the state enum admits an ABA race — a writer
        that observed READY at revision 5 could still commit after the record
        went READY→VALIDATING→READY at revision 7. Callers that read a record
        should pass ``expected_revision=record.revision``; the transition then
        fails closed if anything changed in between.
        """
        expected_state = self._enum(expected)
        new_state = self._enum(new)
        self._validate_reason_detail(reason_code, detail)
        self._validate_expected_revision(expected_revision)
        self._validate_data_update(data_update)
        if new_state not in self._transitions[expected_state]:
            raise IllegalTransition(f"{self.kind}: {expected_state.value} -> {new_state.value}")
        with self._acquire_lease():
            record = self.load()
            if record is None:
                raise StatusConflict(f"{self.path} not initialized")
            if record.state != expected_state.value:
                raise StatusConflict(f"expected {expected_state.value}, found {record.state}")
            if expected_revision is not None and record.revision != expected_revision:
                raise StatusConflict(
                    f"stale writer: expected revision {expected_revision}, "
                    f"found {record.revision} (state cycled back to "
                    f"{record.state}; ABA)"
                )
            record.state = new_state.value
            record.revision += 1
            record.updated_at = time.time()
            record.reason_code = reason_code
            record.detail = detail
            if data_update:
                record.data.update(data_update)
            self._append_history(
                record,
                {"state": new_state.value, "at": record.updated_at, "reason_code": reason_code},
            )
            atomic_write_json(self.path, asdict(record))
            return record

    def update(
        self,
        expected: Enum | str,
        *,
        reason_code: str,
        detail: str | None = None,
        data_update: dict[str, Any] | None = None,
        expected_revision: int | None = None,
        record_history: bool = True,
    ) -> StatusRecord:
        """CAS-update data without fabricating a lifecycle transition."""
        if type(record_history) is not bool:
            raise ValueError("record_history must be a boolean")
        self._validate_reason_detail(reason_code, detail)
        self._validate_expected_revision(expected_revision)
        self._validate_data_update(data_update)
        expected_state = self._enum(expected)
        with self._acquire_lease():
            record = self.load()
            if record is None:
                raise StatusConflict(f"{self.path} not initialized")
            if record.state != expected_state.value:
                raise StatusConflict(f"expected {expected_state.value}, found {record.state}")
            if expected_revision is not None and record.revision != expected_revision:
                raise StatusConflict(
                    f"stale writer: expected revision {expected_revision}, found {record.revision}"
                )
            record.revision += 1
            record.updated_at = time.time()
            record.reason_code = reason_code
            record.detail = detail
            if data_update:
                record.data.update(data_update)
            if record_history:
                self._append_history(
                    record,
                    {
                        "state": expected_state.value,
                        "at": record.updated_at,
                        "reason_code": reason_code,
                    },
                )
            else:
                omitted = record.data.get(_HISTORY_OMITTED_KEY, 0)
                if isinstance(omitted, bool) or not isinstance(omitted, int) or omitted < 0:
                    raise StatusConflict(f"status {_HISTORY_OMITTED_KEY} is invalid")
                record.data[_HISTORY_OMITTED_KEY] = omitted + 1
            atomic_write_json(self.path, asdict(record))
            return record
