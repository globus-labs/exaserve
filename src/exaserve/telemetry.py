"""Versioned, generation-fenced telemetry contracts (WP9/WP10).

Telemetry is not readiness authority, but requested telemetry is part of run
completeness.  These small pure-Python contracts keep Ray actor transport from
turning into an untyped dictionary side channel.
"""

from __future__ import annotations

import atexit
import copy
import ipaddress
import json
import math
import os
import re
import threading
import time
from dataclasses import asdict, dataclass
from typing import Any

from .plan.contracts import canonical_hash

SCHEMA_VERSION = 1
MAX_ENVELOPE_BYTES = 2 * 1024 * 1024
MAX_SAMPLES_PER_REPLICA = 10_000
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class TelemetryContractError(ValueError):
    pass


class OwnedTelemetryActors:
    """Driver-side ownership for named telemetry actors.

    Named actors are job-owned (never detached), and this registry additionally
    gives normal, failure, and interpreter-exit paths one idempotent explicit
    cleanup operation. Failed kills remain registered so a later cleanup can
    retry instead of silently losing ownership.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._actors: dict[str, Any] = {}

    def register(self, kind: str, handle: Any) -> Any:
        if kind not in {"replica_init", "serving", "lifecycle"}:
            raise TelemetryContractError(f"unknown telemetry actor kind {kind!r}")
        with self._lock:
            previous = self._actors.get(kind)
            if previous is not None and previous is not handle:
                raise TelemetryContractError(f"telemetry actor {kind!r} already has an owner")
            self._actors[kind] = handle
        return handle

    def release(self, kind: str, handle: Any | None = None) -> None:
        with self._lock:
            current = self._actors.get(kind)
            if current is not None and (handle is None or current is handle):
                self._actors.pop(kind, None)

    def cleanup(self, kill_fn) -> dict[str, str]:
        with self._lock:
            actors = tuple(self._actors.items())
        errors: dict[str, str] = {}
        for kind, handle in actors:
            try:
                kill_fn(handle)
            except Exception as exc:  # cleanup boundary: retain and report exact cause
                errors[kind] = f"{type(exc).__name__}: {exc}"
            else:
                self.release(kind, handle)
        return errors

    def snapshot(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._actors))


OWNED_TELEMETRY_ACTORS = OwnedTelemetryActors()


def cleanup_owned_telemetry_actors() -> dict[str, str]:
    """Kill every driver-owned telemetry actor; safe to call repeatedly."""
    if not OWNED_TELEMETRY_ACTORS.snapshot():
        return {}
    try:
        import ray
    except ImportError as exc:
        return {"runtime": f"ImportError: {exc}"}
    return OWNED_TELEMETRY_ACTORS.cleanup(lambda actor: ray.kill(actor, no_restart=True))


def _cleanup_owned_telemetry_actors_at_exit() -> None:
    errors = cleanup_owned_telemetry_actors()
    if errors:
        print(f"[Telemetry] actor cleanup failed: {errors}", flush=True)


atexit.register(_cleanup_owned_telemetry_actors_at_exit)


@dataclass(frozen=True)
class TelemetryIdentity:
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    allocation_binding_hash: str

    def __post_init__(self) -> None:
        if not isinstance(self.deployment_id, str) or not self.deployment_id:
            raise TelemetryContractError("telemetry deployment_id is required")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 0
        ):
            raise TelemetryContractError("telemetry generation is invalid")
        for field in ("deployment_plan_hash", "allocation_binding_hash"):
            value = getattr(self, field)
            if not isinstance(value, str) or not _SHA256.fullmatch(value):
                raise TelemetryContractError(f"telemetry {field} is not SHA-256")

    @classmethod
    def from_environment(cls) -> "TelemetryIdentity":
        try:
            generation = int(os.environ["EXASERVE_GENERATION"])
        except (KeyError, TypeError, ValueError) as exc:
            raise TelemetryContractError("EXASERVE_GENERATION is required for telemetry") from exc
        try:
            return cls(
                deployment_id=os.environ["EXASERVE_DEPLOYMENT_ID"],
                generation=generation,
                deployment_plan_hash=os.environ["EXASERVE_PLAN_HASH"],
                allocation_binding_hash=os.environ["EXASERVE_ALLOCATION_BINDING_HASH"],
            )
        except KeyError as exc:
            raise TelemetryContractError(f"{exc.args[0]} is required for telemetry") from exc

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def telemetry_actor_name(kind: str, identity: TelemetryIdentity) -> str:
    if kind not in {"serving", "replica_init", "lifecycle"}:
        raise TelemetryContractError(f"unknown telemetry actor kind {kind!r}")
    digest = canonical_hash({"kind": kind, **identity.to_dict()})[:24]
    return f"ExaServeTelemetry:{kind}:{digest}"


def serving_envelope(
    *,
    identity: TelemetryIdentity,
    replica_id: str,
    sequence: int,
    model_id: str,
    node_ip: str,
    pid: int,
    summary: dict,
    sample: list,
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        **identity.to_dict(),
        "replica_id": replica_id,
        "sequence": sequence,
        "observed_at": time.time(),
        "payload": {
            "model_id": model_id,
            "node_ip": node_ip,
            "pid": pid,
            "summary": summary,
            "sample": sample,
        },
    }


def validate_serving_envelope(raw: Any) -> dict[str, Any]:
    fields = {
        "schema_version",
        "deployment_id",
        "generation",
        "deployment_plan_hash",
        "allocation_binding_hash",
        "replica_id",
        "sequence",
        "observed_at",
        "payload",
    }
    if not isinstance(raw, dict) or set(raw) != fields:
        raise TelemetryContractError("serving stats envelope shape mismatch")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != SCHEMA_VERSION:
        raise TelemetryContractError("unsupported serving stats schema")
    identity = TelemetryIdentity(
        deployment_id=raw["deployment_id"],
        generation=raw["generation"],
        deployment_plan_hash=raw["deployment_plan_hash"],
        allocation_binding_hash=raw["allocation_binding_hash"],
    )
    if (
        not isinstance(raw["replica_id"], str)
        or not raw["replica_id"]
        or len(raw["replica_id"]) > 256
    ):
        raise TelemetryContractError("serving stats replica_id is invalid")
    if (
        isinstance(raw["sequence"], bool)
        or not isinstance(raw["sequence"], int)
        or raw["sequence"] < 0
    ):
        raise TelemetryContractError("serving stats sequence is invalid")
    observed = raw["observed_at"]
    if (
        isinstance(observed, bool)
        or not isinstance(observed, (int, float))
        or not math.isfinite(float(observed))
        or observed <= 0
    ):
        raise TelemetryContractError("serving stats observed_at is invalid")
    payload = raw["payload"]
    if not isinstance(payload, dict) or set(payload) != {
        "model_id",
        "node_ip",
        "pid",
        "summary",
        "sample",
    }:
        raise TelemetryContractError("serving stats payload shape mismatch")
    if not isinstance(payload["model_id"], str) or not payload["model_id"]:
        raise TelemetryContractError("serving stats model_id is invalid")
    if not isinstance(payload["node_ip"], str) or not payload["node_ip"]:
        raise TelemetryContractError("serving stats node_ip is invalid")
    try:
        ipaddress.ip_address(payload["node_ip"])
    except ValueError as exc:
        raise TelemetryContractError("serving stats node_ip is not an IP address") from exc
    if (
        isinstance(payload["pid"], bool)
        or not isinstance(payload["pid"], int)
        or payload["pid"] <= 0
    ):
        raise TelemetryContractError("serving stats pid is invalid")
    _validate_summary(payload["summary"])
    _validate_sample(payload["sample"])
    try:
        size = len(json.dumps(raw, allow_nan=False, separators=(",", ":")))
    except (TypeError, ValueError) as exc:
        raise TelemetryContractError("serving stats payload is not finite JSON") from exc
    if size > MAX_ENVELOPE_BYTES:
        raise TelemetryContractError(f"serving stats envelope exceeds {MAX_ENVELOPE_BYTES} bytes")
    return {**raw, "_identity": identity}


def _finite_number(value: Any, name: str, *, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise TelemetryContractError(f"{name} must be a finite nonnegative number")


def _validate_distribution(value: Any, name: str) -> None:
    if not isinstance(value, dict) or "n" not in value:
        raise TelemetryContractError(f"{name} must be a distribution object")
    count = value["n"]
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise TelemetryContractError(f"{name}.n is invalid")
    expected = {"n"} if count == 0 else {"n", "mean", "p50", "p90", "p99", "max"}
    if set(value) != expected:
        raise TelemetryContractError(f"{name} distribution fields are invalid")
    for field in expected - {"n"}:
        _finite_number(value[field], f"{name}.{field}")


def _validate_summary(value: Any) -> None:
    fields = {
        "total_requests",
        "server_ttft",
        "server_tbt",
        "e2e",
        "queued_time",
        "prefill_time",
        "decode_time",
        "mean_batch_size",
        "max_batch_size",
        "kv_cache_peak",
    }
    if not isinstance(value, dict) or set(value) != fields:
        raise TelemetryContractError("serving stats summary shape mismatch")
    total = value["total_requests"]
    if isinstance(total, bool) or not isinstance(total, int) or total < 0:
        raise TelemetryContractError("serving stats total_requests is invalid")
    for field in ("server_ttft", "server_tbt", "e2e", "queued_time", "prefill_time", "decode_time"):
        _validate_distribution(value[field], f"summary.{field}")
    for field in ("mean_batch_size", "kv_cache_peak"):
        _finite_number(value[field], f"summary.{field}")
    if (
        isinstance(value["max_batch_size"], bool)
        or not isinstance(value["max_batch_size"], int)
        or value["max_batch_size"] < 0
    ):
        raise TelemetryContractError("summary.max_batch_size must be a nonnegative integer")


def _validate_sample(value: Any) -> None:
    if not isinstance(value, list) or len(value) > MAX_SAMPLES_PER_REPLICA:
        raise TelemetryContractError("serving stats sample is invalid or unbounded")
    fields = {"finished_at", "ttft", "tbt", "e2e"}
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != fields:
            raise TelemetryContractError(f"serving stats sample[{index}] shape mismatch")
        _finite_number(item["finished_at"], f"sample[{index}].finished_at")
        _finite_number(item["ttft"], f"sample[{index}].ttft")
        _finite_number(item["tbt"], f"sample[{index}].tbt", nullable=True)
        _finite_number(item["e2e"], f"sample[{index}].e2e")


class ServingStatsStore:
    """Bounded actor state with generation fencing and sequence deduplication."""

    def __init__(self, identity: dict[str, Any], expected_replicas: int) -> None:
        self.identity = TelemetryIdentity(**identity)
        if (
            isinstance(expected_replicas, bool)
            or not isinstance(expected_replicas, int)
            or expected_replicas <= 0
        ):
            raise TelemetryContractError("expected_replicas must be positive")
        self.expected_replicas = expected_replicas
        self._latest: dict[str, dict[str, Any]] = {}
        self._duplicates = 0
        self._rejected = 0

    def report(self, envelope: Any) -> bool:
        try:
            validated = validate_serving_envelope(envelope)
            if validated.pop("_identity") != self.identity:
                raise TelemetryContractError("stale or foreign telemetry identity")
            replica_id = validated["replica_id"]
            previous = self._latest.get(replica_id)
            if previous is not None:
                if validated["sequence"] < previous["sequence"]:
                    raise TelemetryContractError("serving stats sequence regressed")
                if validated["sequence"] == previous["sequence"]:
                    if validated != previous:
                        raise TelemetryContractError("conflicting serving stats duplicate sequence")
                    self._duplicates += 1
                    return False
            if previous is None and len(self._latest) >= self.expected_replicas:
                raise TelemetryContractError("telemetry contains more replicas than the plan")
            self._latest[replica_id] = copy.deepcopy(validated)
            return True
        except TelemetryContractError:
            self._rejected += 1
            raise

    def snapshot(self) -> dict[str, Any]:
        received = len(self._latest)
        return {
            "schema_version": SCHEMA_VERSION,
            "identity": self.identity.to_dict(),
            "expected_replicas": self.expected_replicas,
            "received_replicas": received,
            "complete": received == self.expected_replicas,
            "duplicates_ignored": self._duplicates,
            "rejected_reports": self._rejected,
            "replicas": copy.deepcopy(dict(sorted(self._latest.items()))),
        }


def _validate_snapshot_header(
    raw: Any,
    *,
    fields: set[str],
    expected_identity: TelemetryIdentity | None,
    expected_replicas: int | None,
) -> tuple[TelemetryIdentity, int, int]:
    if not isinstance(raw, dict) or set(raw) != fields:
        raise TelemetryContractError("telemetry snapshot shape mismatch")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != SCHEMA_VERSION:
        raise TelemetryContractError("unsupported telemetry snapshot schema")
    identity_value = raw["identity"]
    if not isinstance(identity_value, dict) or set(identity_value) != {
        "deployment_id",
        "generation",
        "deployment_plan_hash",
        "allocation_binding_hash",
    }:
        raise TelemetryContractError("telemetry snapshot identity shape mismatch")
    identity = TelemetryIdentity(**identity_value)
    if expected_identity is not None and identity != expected_identity:
        raise TelemetryContractError("telemetry snapshot has stale or foreign identity")
    expected = raw["expected_replicas"]
    received = raw["received_replicas"]
    if type(expected) is not int or expected <= 0:
        raise TelemetryContractError("telemetry snapshot expected_replicas is invalid")
    if expected_replicas is not None and expected != expected_replicas:
        raise TelemetryContractError("telemetry snapshot expected replica count disagrees")
    if type(received) is not int or received < 0 or received > expected:
        raise TelemetryContractError("telemetry snapshot received_replicas is invalid")
    for name in ("duplicates_ignored", "rejected_reports"):
        if type(raw[name]) is not int or raw[name] < 0:
            raise TelemetryContractError(f"telemetry snapshot {name} is invalid")
    if not isinstance(raw["complete"], bool) or raw["complete"] != (received == expected):
        raise TelemetryContractError("telemetry snapshot complete flag is inconsistent")
    return identity, expected, received


def validate_serving_snapshot(
    raw: Any,
    *,
    expected_identity: TelemetryIdentity | None = None,
    expected_replicas: int | None = None,
) -> dict[str, Any]:
    """Validate the exact actor snapshot after Ray transport."""
    fields = {
        "schema_version",
        "identity",
        "expected_replicas",
        "received_replicas",
        "complete",
        "duplicates_ignored",
        "rejected_reports",
        "replicas",
    }
    identity, _expected, received = _validate_snapshot_header(
        raw,
        fields=fields,
        expected_identity=expected_identity,
        expected_replicas=expected_replicas,
    )
    replicas = raw["replicas"]
    if not isinstance(replicas, dict) or len(replicas) != received:
        raise TelemetryContractError("serving snapshot replica inventory is inconsistent")
    for replica_id, envelope in replicas.items():
        if not isinstance(replica_id, str) or not replica_id:
            raise TelemetryContractError("serving snapshot replica key is invalid")
        validated = validate_serving_envelope(envelope)
        if validated.pop("_identity") != identity or validated["replica_id"] != replica_id:
            raise TelemetryContractError("serving snapshot replica identity is inconsistent")
    return copy.deepcopy(raw)


def replica_init_envelope(
    *, identity: TelemetryIdentity, replica_id: str, payload: dict[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        **identity.to_dict(),
        "replica_id": replica_id,
        "observed_at": time.time(),
        "payload": payload,
    }


class ReplicaInitStatsStore:
    """One bounded init record per planned replica, fenced to one generation."""

    def __init__(self, identity: dict[str, Any], expected_replicas: int) -> None:
        self.identity = TelemetryIdentity(**identity)
        if (
            isinstance(expected_replicas, bool)
            or not isinstance(expected_replicas, int)
            or expected_replicas <= 0
        ):
            raise TelemetryContractError("expected_replicas must be positive")
        self.expected_replicas = expected_replicas
        self._replicas: dict[str, dict[str, Any]] = {}
        self._fingerprints: dict[str, str] = {}
        self._duplicates = 0
        self._rejected = 0

    def report(self, raw: Any) -> bool:
        try:
            fields = {
                "schema_version",
                "deployment_id",
                "generation",
                "deployment_plan_hash",
                "allocation_binding_hash",
                "replica_id",
                "observed_at",
                "payload",
            }
            if not isinstance(raw, dict) or set(raw) != fields:
                raise TelemetryContractError("replica init envelope shape mismatch")
            if type(raw["schema_version"]) is not int or raw["schema_version"] != SCHEMA_VERSION:
                raise TelemetryContractError("unsupported replica init schema")
            identity = TelemetryIdentity(
                deployment_id=raw["deployment_id"],
                generation=raw["generation"],
                deployment_plan_hash=raw["deployment_plan_hash"],
                allocation_binding_hash=raw["allocation_binding_hash"],
            )
            if identity != self.identity:
                raise TelemetryContractError("stale or foreign telemetry identity")
            replica_id = raw["replica_id"]
            if not isinstance(replica_id, str) or not replica_id or len(replica_id) > 256:
                raise TelemetryContractError("replica init replica_id is invalid")
            observed = raw["observed_at"]
            if (
                isinstance(observed, bool)
                or not isinstance(observed, (int, float))
                or not math.isfinite(float(observed))
                or observed <= 0
            ):
                raise TelemetryContractError("replica init observed_at is invalid")
            payload = raw["payload"]
            if not isinstance(payload, dict):
                raise TelemetryContractError("replica init payload must be an object")
            try:
                encoded = json.dumps(payload, allow_nan=False, separators=(",", ":"))
            except (TypeError, ValueError) as exc:
                raise TelemetryContractError("replica init payload is not finite JSON") from exc
            if len(encoded) > 256 * 1024:
                raise TelemetryContractError("replica init payload is too large")
            if replica_id in self._replicas:
                fingerprint = canonical_hash(raw)
                if self._fingerprints[replica_id] != fingerprint:
                    raise TelemetryContractError("conflicting replica init duplicate")
                self._duplicates += 1
                return False
            if len(self._replicas) >= self.expected_replicas:
                raise TelemetryContractError("replica init stats exceed planned replicas")
            self._replicas[replica_id] = copy.deepcopy(payload)
            self._fingerprints[replica_id] = canonical_hash(raw)
            return True
        except TelemetryContractError:
            self._rejected += 1
            raise

    def snapshot(self) -> dict[str, Any]:
        received = len(self._replicas)
        return {
            "schema_version": SCHEMA_VERSION,
            "identity": self.identity.to_dict(),
            "expected_replicas": self.expected_replicas,
            "received_replicas": received,
            "complete": received == self.expected_replicas,
            "duplicates_ignored": self._duplicates,
            "rejected_reports": self._rejected,
            "replicas": copy.deepcopy(dict(sorted(self._replicas.items()))),
        }


def validate_replica_init_snapshot(
    raw: Any,
    *,
    expected_identity: TelemetryIdentity | None = None,
    expected_replicas: int | None = None,
) -> dict[str, Any]:
    fields = {
        "schema_version",
        "identity",
        "expected_replicas",
        "received_replicas",
        "complete",
        "duplicates_ignored",
        "rejected_reports",
        "replicas",
    }
    _identity, _expected, received = _validate_snapshot_header(
        raw,
        fields=fields,
        expected_identity=expected_identity,
        expected_replicas=expected_replicas,
    )
    replicas = raw["replicas"]
    if not isinstance(replicas, dict) or len(replicas) != received:
        raise TelemetryContractError("replica-init snapshot inventory is inconsistent")
    for replica_id, payload in replicas.items():
        if not isinstance(replica_id, str) or not replica_id or not isinstance(payload, dict):
            raise TelemetryContractError("replica-init snapshot entry is invalid")
        try:
            encoded = json.dumps(payload, allow_nan=False, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            raise TelemetryContractError(
                "replica-init snapshot payload is not finite JSON"
            ) from exc
        if len(encoded) > 256 * 1024:
            raise TelemetryContractError("replica-init snapshot payload is too large")
    return copy.deepcopy(raw)


_LIFECYCLE_EVENTS = {"STARTED", "STOPPED", "FAILED"}


def lifecycle_event_envelope(
    *,
    identity: TelemetryIdentity,
    component_id: str,
    instance_id: str,
    event: str,
    model_id: str,
    node_id: str,
    pid: int,
    detail: str = "",
) -> dict[str, Any]:
    """Build one generation-fenced replica lifecycle event."""
    return {
        "schema_version": SCHEMA_VERSION,
        **identity.to_dict(),
        "component_id": component_id,
        "instance_id": instance_id,
        "event": event,
        "model_id": model_id,
        "node_id": node_id,
        "pid": pid,
        "observed_at": time.time(),
        "detail": detail,
    }


class ReplicaLifecycleStore:
    """Exact, O(K) proof that every planned replica teardown completed.

    Ray Serve awaits an async replica destructor but logs and suppresses an
    exception raised by it. This driver-owned actor makes the result observable
    to the deployment child after ``serve.shutdown()`` returns. A missing final
    event is itself a failure, covering destructors that were never invoked.
    """

    def __init__(self, identity: dict[str, Any], expected_components: list[str]) -> None:
        self.identity = TelemetryIdentity(**identity)
        if (
            not isinstance(expected_components, list)
            or not expected_components
            or any(not isinstance(item, str) or not item for item in expected_components)
            or len(set(expected_components)) != len(expected_components)
        ):
            raise TelemetryContractError("lifecycle expected components must be unique strings")
        self.expected_components = frozenset(expected_components)
        self._current: dict[str, dict[str, Any]] = {}
        self._duplicates = 0
        self._rejected = 0
        self._replacements: dict[str, int] = {}

    def report(self, raw: Any) -> bool:
        try:
            fields = {
                "schema_version",
                "deployment_id",
                "generation",
                "deployment_plan_hash",
                "allocation_binding_hash",
                "component_id",
                "instance_id",
                "event",
                "model_id",
                "node_id",
                "pid",
                "observed_at",
                "detail",
            }
            if not isinstance(raw, dict) or set(raw) != fields:
                raise TelemetryContractError("lifecycle event shape mismatch")
            if type(raw["schema_version"]) is not int or raw["schema_version"] != SCHEMA_VERSION:
                raise TelemetryContractError("unsupported lifecycle schema")
            identity = TelemetryIdentity(
                deployment_id=raw["deployment_id"],
                generation=raw["generation"],
                deployment_plan_hash=raw["deployment_plan_hash"],
                allocation_binding_hash=raw["allocation_binding_hash"],
            )
            if identity != self.identity:
                raise TelemetryContractError("stale or foreign lifecycle identity")
            component_id = raw["component_id"]
            if component_id not in self.expected_components:
                raise TelemetryContractError("lifecycle event names an unplanned component")
            for field in ("instance_id", "model_id", "node_id"):
                value = raw[field]
                if not isinstance(value, str) or not value or len(value) > 256:
                    raise TelemetryContractError(f"lifecycle {field} is invalid")
            if raw["event"] not in _LIFECYCLE_EVENTS:
                raise TelemetryContractError("lifecycle event is invalid")
            if isinstance(raw["pid"], bool) or not isinstance(raw["pid"], int) or raw["pid"] <= 0:
                raise TelemetryContractError("lifecycle pid is invalid")
            observed = raw["observed_at"]
            if (
                isinstance(observed, bool)
                or not isinstance(observed, (int, float))
                or not math.isfinite(float(observed))
                or observed <= 0
            ):
                raise TelemetryContractError("lifecycle observed_at is invalid")
            if not isinstance(raw["detail"], str) or len(raw["detail"]) > 400:
                raise TelemetryContractError("lifecycle detail is invalid")

            previous = self._current.get(component_id)
            if raw["event"] == "STARTED":
                if previous is not None and previous["event"] == "STARTED":
                    if previous["instance_id"] == raw["instance_id"]:
                        if previous != raw:
                            raise TelemetryContractError("conflicting lifecycle STARTED duplicate")
                        self._duplicates += 1
                        return False
                    raise TelemetryContractError("lifecycle component has two active instances")
                if previous is not None:
                    self._replacements[component_id] = self._replacements.get(component_id, 0) + 1
            elif previous is None or previous["event"] != "STARTED":
                raise TelemetryContractError("lifecycle terminal event has no active instance")
            elif previous["instance_id"] != raw["instance_id"]:
                raise TelemetryContractError("lifecycle terminal event has the wrong instance")

            self._current[component_id] = dict(raw)
            return True
        except TelemetryContractError:
            self._rejected += 1
            raise

    def snapshot(self) -> dict[str, Any]:
        missing = sorted(self.expected_components - set(self._current))
        active = sorted(
            component for component, event in self._current.items() if event["event"] == "STARTED"
        )
        failed = sorted(
            component for component, event in self._current.items() if event["event"] == "FAILED"
        )
        stopped = sorted(
            component for component, event in self._current.items() if event["event"] == "STOPPED"
        )
        return {
            "schema_version": SCHEMA_VERSION,
            "identity": self.identity.to_dict(),
            "expected_components": sorted(self.expected_components),
            "missing_components": missing,
            "active_components": active,
            "failed_components": failed,
            "stopped_components": stopped,
            "complete": not missing and not active,
            "clean": not missing and not active and not failed,
            "duplicates_ignored": self._duplicates,
            "rejected_reports": self._rejected,
            "replacements": dict(sorted(self._replacements.items())),
            "components": {key: dict(value) for key, value in sorted(self._current.items())},
        }


def _validate_snapshot_lifecycle_event(
    raw: Any, *, identity: TelemetryIdentity, expected_components: set[str]
) -> None:
    fields = {
        "schema_version",
        "deployment_id",
        "generation",
        "deployment_plan_hash",
        "allocation_binding_hash",
        "component_id",
        "instance_id",
        "event",
        "model_id",
        "node_id",
        "pid",
        "observed_at",
        "detail",
    }
    if not isinstance(raw, dict) or set(raw) != fields:
        raise TelemetryContractError("lifecycle snapshot event shape mismatch")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != SCHEMA_VERSION:
        raise TelemetryContractError("unsupported lifecycle snapshot event schema")
    observed_identity = TelemetryIdentity(
        deployment_id=raw["deployment_id"],
        generation=raw["generation"],
        deployment_plan_hash=raw["deployment_plan_hash"],
        allocation_binding_hash=raw["allocation_binding_hash"],
    )
    if observed_identity != identity or raw["component_id"] not in expected_components:
        raise TelemetryContractError("lifecycle snapshot event identity is inconsistent")
    for field in ("instance_id", "model_id", "node_id"):
        if not isinstance(raw[field], str) or not raw[field] or len(raw[field]) > 256:
            raise TelemetryContractError(f"lifecycle snapshot {field} is invalid")
    if raw["event"] not in _LIFECYCLE_EVENTS:
        raise TelemetryContractError("lifecycle snapshot event is invalid")
    if type(raw["pid"]) is not int or raw["pid"] <= 0:
        raise TelemetryContractError("lifecycle snapshot pid is invalid")
    observed_at = raw["observed_at"]
    if (
        isinstance(observed_at, bool)
        or not isinstance(observed_at, (int, float))
        or not math.isfinite(float(observed_at))
        or observed_at <= 0
    ):
        raise TelemetryContractError("lifecycle snapshot observed_at is invalid")
    if not isinstance(raw["detail"], str) or len(raw["detail"]) > 400:
        raise TelemetryContractError("lifecycle snapshot detail is invalid")


def validate_lifecycle_snapshot(
    raw: Any, *, expected_identity: TelemetryIdentity | None = None
) -> dict[str, Any]:
    fields = {
        "schema_version",
        "identity",
        "expected_components",
        "missing_components",
        "active_components",
        "failed_components",
        "stopped_components",
        "complete",
        "clean",
        "duplicates_ignored",
        "rejected_reports",
        "replacements",
        "components",
    }
    if not isinstance(raw, dict) or set(raw) != fields:
        raise TelemetryContractError("lifecycle snapshot shape mismatch")
    if type(raw["schema_version"]) is not int or raw["schema_version"] != SCHEMA_VERSION:
        raise TelemetryContractError("unsupported lifecycle snapshot schema")
    identity_value = raw["identity"]
    if not isinstance(identity_value, dict) or set(identity_value) != {
        "deployment_id",
        "generation",
        "deployment_plan_hash",
        "allocation_binding_hash",
    }:
        raise TelemetryContractError("lifecycle snapshot identity shape mismatch")
    identity = TelemetryIdentity(**identity_value)
    if expected_identity is not None and identity != expected_identity:
        raise TelemetryContractError("lifecycle snapshot has stale or foreign identity")
    expected_list = raw["expected_components"]
    if (
        not isinstance(expected_list, list)
        or not expected_list
        or any(not isinstance(item, str) or not item for item in expected_list)
        or expected_list != sorted(set(expected_list))
    ):
        raise TelemetryContractError("lifecycle snapshot expected components are invalid")
    expected = set(expected_list)
    components = raw["components"]
    if not isinstance(components, dict) or not set(components) <= expected:
        raise TelemetryContractError("lifecycle snapshot component inventory is invalid")
    for component_id, event in components.items():
        _validate_snapshot_lifecycle_event(event, identity=identity, expected_components=expected)
        if event["component_id"] != component_id:
            raise TelemetryContractError("lifecycle snapshot component key is inconsistent")
    observed_groups = {
        "missing_components": sorted(expected - set(components)),
        "active_components": sorted(
            key for key, event in components.items() if event["event"] == "STARTED"
        ),
        "failed_components": sorted(
            key for key, event in components.items() if event["event"] == "FAILED"
        ),
        "stopped_components": sorted(
            key for key, event in components.items() if event["event"] == "STOPPED"
        ),
    }
    for field, observed in observed_groups.items():
        if raw[field] != observed:
            raise TelemetryContractError(f"lifecycle snapshot {field} is inconsistent")
    complete = (
        not observed_groups["missing_components"] and not observed_groups["active_components"]
    )
    clean = complete and not observed_groups["failed_components"]
    if type(raw["complete"]) is not bool or raw["complete"] != complete:
        raise TelemetryContractError("lifecycle snapshot complete flag is inconsistent")
    if type(raw["clean"]) is not bool or raw["clean"] != clean:
        raise TelemetryContractError("lifecycle snapshot clean flag is inconsistent")
    for name in ("duplicates_ignored", "rejected_reports"):
        if type(raw[name]) is not int or raw[name] < 0:
            raise TelemetryContractError(f"lifecycle snapshot {name} is invalid")
    replacements = raw["replacements"]
    if not isinstance(replacements, dict) or any(
        key not in expected or type(count) is not int or count <= 0
        for key, count in replacements.items()
    ):
        raise TelemetryContractError("lifecycle snapshot replacements are invalid")
    return copy.deepcopy(raw)
