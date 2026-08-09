"""Typed, generation-fenced gateway health and failure evidence."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from .atomic import atomic_write_json


CLASSIFICATIONS = frozenset({"healthy", "degraded", "process_dead"})
PROCESS_STATES = frozenset(
    {"NEW", "STARTING", "RUNNING", "READY", "STOPPING", "STOPPED", "FAILED", "UNKNOWN"}
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


@dataclass(frozen=True)
class GatewayHealthEvidence:
    schema_version: int
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    allocation_binding_hash: str
    gateway_kind: str
    classification: str
    process_state: str
    health_ok: bool
    observed_at: str
    observed_monotonic: float
    exit_code: Optional[int]
    signal: Optional[int]
    detail: str
    log_tail: str
    log_total_bytes: int
    log_dropped_bytes: int
    log_truncated: bool

    def __post_init__(self) -> None:
        if isinstance(self.schema_version, bool) or self.schema_version != 1:
            raise ValueError("unsupported gateway evidence schema")
        for name in ("deployment_id", "gateway_kind", "observed_at", "detail", "log_tail"):
            value = getattr(self, name)
            if not isinstance(value, str) or (name != "log_tail" and not value):
                raise ValueError(f"gateway evidence {name} is invalid")
        for name in ("deployment_plan_hash", "allocation_binding_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not _SHA256.fullmatch(value):
                raise ValueError(f"gateway evidence {name} must be SHA-256")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 0
        ):
            raise ValueError("gateway evidence generation must be non-negative")
        if self.classification not in CLASSIFICATIONS:
            raise ValueError("invalid gateway evidence classification")
        if self.process_state not in PROCESS_STATES:
            raise ValueError("invalid gateway evidence process_state")
        if not isinstance(self.health_ok, bool) or not isinstance(self.log_truncated, bool):
            raise ValueError("gateway evidence boolean fields are invalid")
        if (
            isinstance(self.observed_monotonic, bool)
            or not isinstance(self.observed_monotonic, (int, float))
            or not math.isfinite(float(self.observed_monotonic))
            or self.observed_monotonic < 0
        ):
            raise ValueError("gateway evidence monotonic time is invalid")
        try:
            parsed = datetime.fromisoformat(self.observed_at)
        except ValueError as exc:
            raise ValueError("gateway evidence timestamp is invalid") from exc
        if parsed.tzinfo is None:
            raise ValueError("gateway evidence timestamp requires a timezone")
        for name in ("exit_code", "signal"):
            value = getattr(self, name)
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 0
            ):
                raise ValueError(f"gateway evidence {name} must be null or nonnegative")
        for name in ("log_total_bytes", "log_dropped_bytes"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.log_dropped_bytes > self.log_total_bytes:
            raise ValueError("gateway dropped-byte count exceeds total bytes")
        if self.classification == "process_dead":
            if self.process_state != "FAILED" or self.health_ok:
                raise ValueError("process_dead evidence is internally inconsistent")
        elif self.exit_code is not None or self.signal is not None:
            raise ValueError("live gateway evidence cannot carry exit information")

    def to_dict(self) -> dict:
        return dict(self.__dict__)


def classify_gateway_evidence(
    *,
    deployment_id: str,
    generation: int,
    deployment_plan_hash: str,
    allocation_binding_hash: str,
    gateway_kind: str,
    process_state: str,
    returncode: Optional[int],
    health_ok: bool,
    detail: str,
    capture: Optional[dict] = None,
) -> GatewayHealthEvidence:
    if not isinstance(health_ok, bool):
        raise ValueError("gateway health_ok must be a boolean")
    if not isinstance(detail, str):
        raise ValueError("gateway detail must be text")
    if capture is not None and not isinstance(capture, dict):
        raise ValueError("gateway output capture must be an object")
    if returncode is not None and (isinstance(returncode, bool) or not isinstance(returncode, int)):
        raise ValueError("gateway returncode must be null or an integer")
    dead = process_state == "FAILED" or returncode is not None
    classification = "process_dead" if dead else "healthy" if health_ok else "degraded"
    capture = capture or {}
    tail = capture.get("tail", "")
    total_bytes = capture.get("total_bytes", 0)
    dropped_bytes = capture.get("dropped_bytes", 0)
    truncated = capture.get("truncated", False)
    if not isinstance(tail, str):
        raise ValueError("gateway output tail must be text")
    for name, value in (("total_bytes", total_bytes), ("dropped_bytes", dropped_bytes)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"gateway output {name} must be nonnegative")
    if not isinstance(truncated, bool):
        raise ValueError("gateway output truncated must be a boolean")
    exit_code = returncode if returncode is not None and returncode >= 0 else None
    signal = -returncode if returncode is not None and returncode < 0 else None
    return GatewayHealthEvidence(
        schema_version=1,
        deployment_id=deployment_id,
        generation=generation,
        deployment_plan_hash=deployment_plan_hash,
        allocation_binding_hash=allocation_binding_hash,
        gateway_kind=gateway_kind,
        classification=classification,
        process_state=process_state,
        health_ok=health_ok and not dead,
        observed_at=datetime.now(timezone.utc).isoformat(),
        observed_monotonic=__import__("time").monotonic(),
        exit_code=exit_code,
        signal=signal,
        detail=detail[:2000],
        log_tail=tail[-65536:],
        log_total_bytes=total_bytes,
        log_dropped_bytes=dropped_bytes,
        log_truncated=truncated,
    )


def write_gateway_evidence(path: str, evidence: GatewayHealthEvidence) -> None:
    atomic_write_json(path, evidence.to_dict())
