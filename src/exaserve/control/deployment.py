"""Callable deployment lifecycle (plan WP4.1, audit IMP-B01).

`server.py` grew a 600-line `main()` that staged models, started Serve, waited
for proxies, decided readiness, and handled shutdown — all in one straight line
with no addressable operations. Nothing else could drive a deployment, and a
caller could not ask "is it still healthy?" without re-reading stdout.

`DeploymentManager` gives that lifecycle the five operations WP4.1 requires —
``prepare``, ``deploy``, ``observe``, ``drain``, ``stop`` — with typed failures
and an explicit state machine. It deliberately *delegates* to the existing,
validated deployment internals rather than reimplementing them: this is a
contract around proven code, not a rewrite of it.

State machine (plan WP5.1), a strict subset of the full deployment lifecycle
that this component owns:

    PLANNED -> STAGING -> DEPLOYING -> VALIDATING -> READY -> DRAINING -> STOPPED

`FAILED` is reachable from every active state and preserves the first cause.
Post-READY loss of the readiness predicate returns the deployment to
`VALIDATING`, so READY is revocable here too, not latched.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Optional


class DeploymentError(RuntimeError):
    """Base for typed deployment failures (plan WP4.9: no broad catches)."""


class PrepareError(DeploymentError):
    """Staging/planning failed; nothing was deployed."""


class DeployError(DeploymentError):
    """The applications could not be brought up."""


class ValidationError(DeploymentError):
    """Deployed, but the readiness predicate is not satisfied."""


class DrainError(DeploymentError):
    """Drain exceeded its deadline or failed to quiesce."""


class DeploymentState:
    PLANNED = "PLANNED"
    STAGING = "STAGING"
    DEPLOYING = "DEPLOYING"
    VALIDATING = "VALIDATING"
    READY = "READY"
    DRAINING = "DRAINING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"


# state -> states reachable from it. FAILED/STOPPED are terminal for this
# component; re-deploying means constructing a new manager for a new generation.
_ALLOWED = {
    DeploymentState.PLANNED: {DeploymentState.STAGING, DeploymentState.FAILED},
    DeploymentState.STAGING: {DeploymentState.DEPLOYING, DeploymentState.FAILED},
    # The isolated deployment child is a witness, not the global readiness
    # authority. It may receive shutdown while global validation is pending,
    # so DEPLOYING -> DRAINING is a legitimate bounded teardown edge.
    DeploymentState.DEPLOYING: {
        DeploymentState.VALIDATING,
        DeploymentState.DRAINING,
        DeploymentState.FAILED,
    },
    DeploymentState.VALIDATING: {
        DeploymentState.READY,
        DeploymentState.VALIDATING,
        DeploymentState.DRAINING,
        DeploymentState.FAILED,
    },
    # READY -> VALIDATING is the revocation edge: losing the predicate after
    # READY must not be silently tolerated, and must not be terminal either.
    DeploymentState.READY: {
        DeploymentState.VALIDATING,
        DeploymentState.DRAINING,
        DeploymentState.FAILED,
    },
    DeploymentState.DRAINING: {DeploymentState.STOPPED, DeploymentState.FAILED},
    DeploymentState.STOPPED: set(),
    DeploymentState.FAILED: set(),
}

MAX_DEPLOYMENT_HISTORY_EVENTS = 256


@dataclass
class DeploymentCause:
    operation: str
    reason_code: str
    detail: str
    at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        for name in ("operation", "reason_code", "detail"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"deployment cause {name} must be non-empty text")
        if (
            isinstance(self.at, bool)
            or not isinstance(self.at, (int, float))
            or not math.isfinite(float(self.at))
            or self.at < 0
        ):
            raise ValueError("deployment cause time must be finite and non-negative")

    def __str__(self) -> str:
        return f"{self.operation}: {self.reason_code}: {self.detail}"


class DeploymentManager:
    """Owns one deployment generation's lifecycle.

    Every operation is injected, so the manager is testable without Ray and the
    production wiring stays in `server.py` where the deployment internals live.
    """

    def __init__(
        self,
        *,
        deployment_id: str,
        generation: int,
        prepare_fn: Optional[Callable[[], Any]] = None,
        deploy_fn: Optional[Callable[[], Any]] = None,
        validate_fn: Optional[Callable[[], Any]] = None,
        observe_fn: Optional[Callable[[], Any]] = None,
        drain_fn: Optional[Callable[[float], None]] = None,
        stop_fn: Optional[Callable[[], None]] = None,
    ) -> None:
        if not isinstance(deployment_id, str) or not deployment_id:
            raise ValueError("deployment manager deployment_id must be non-empty text")
        if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
            raise ValueError("deployment manager generation must be non-negative")
        for name, callback in (
            ("prepare_fn", prepare_fn),
            ("deploy_fn", deploy_fn),
            ("validate_fn", validate_fn),
            ("observe_fn", observe_fn),
            ("drain_fn", drain_fn),
            ("stop_fn", stop_fn),
        ):
            if callback is not None and not callable(callback):
                raise ValueError(f"deployment manager {name} must be null or callable")
        self.deployment_id = deployment_id
        self.generation = generation
        self._prepare_fn = prepare_fn
        self._deploy_fn = deploy_fn
        self._validate_fn = validate_fn
        self._observe_fn = observe_fn
        self._drain_fn = drain_fn
        self._stop_fn = stop_fn

        self.state = DeploymentState.PLANNED
        self.first_cause: Optional[DeploymentCause] = None
        self.secondary_causes: list[DeploymentCause] = []
        self.history: list[tuple[str, float]] = [(self.state, time.time())]
        self.history_events_dropped = 0
        self.last_snapshot: Any = None
        self._stop_complete = False

    # -- state ------------------------------------------------------------
    def _append_history(self, state: str) -> None:
        if len(self.history) >= MAX_DEPLOYMENT_HISTORY_EVENTS:
            del self.history[1]
            self.history_events_dropped += 1
        self.history.append((state, time.time()))

    def _transition(self, target: str) -> None:
        if target not in _ALLOWED.get(self.state, set()):
            raise DeploymentError(
                f"illegal transition {self.state} -> {target} "
                f"(deployment {self.deployment_id} gen {self.generation})"
            )
        self.state = target
        self._append_history(target)

    def _fail(
        self, operation: str, reason_code: str, detail: str, error: type[DeploymentError]
    ) -> DeploymentError:
        """Record the FIRST cause and move to FAILED. Later errors never win."""
        detail = detail or "operation failed without diagnostic detail"
        if self.first_cause is None:
            self.first_cause = DeploymentCause(operation, reason_code, detail)
        if self.state not in (DeploymentState.FAILED, DeploymentState.STOPPED):
            self.state = DeploymentState.FAILED
            self._append_history(self.state)
        return error(f"{operation}: {detail}")

    # -- operations --------------------------------------------------------
    def prepare(self) -> Any:
        self._transition(DeploymentState.STAGING)
        if self._prepare_fn is None:
            return None
        try:
            return self._prepare_fn()
        except Exception as exc:
            raise self._fail("prepare", "STAGING_FAILED", str(exc), PrepareError) from exc

    def deploy(self) -> Any:
        self._transition(DeploymentState.DEPLOYING)
        if self._deploy_fn is None:
            return None
        try:
            return self._deploy_fn()
        except Exception as exc:
            raise self._fail("deploy", "DEPLOY_FAILED", str(exc), DeployError) from exc

    def validate(self) -> Any:
        """Run the readiness predicate. READY only if it is satisfied."""
        self._transition(DeploymentState.VALIDATING)
        if self._validate_fn is None:
            self._transition(DeploymentState.READY)
            return None
        try:
            snapshot = self._validate_fn()
        except Exception as exc:
            raise self._fail("validate", "NOT_READY", str(exc), ValidationError) from exc
        self.last_snapshot = snapshot
        ready = getattr(snapshot, "ready", None)
        if type(ready) is not bool:
            raise self._fail(
                "validate",
                "INVALID_VERDICT",
                "readiness snapshot must expose an exact boolean ready field",
                ValidationError,
            )
        if ready is False:
            # An explicitly degraded start stays in VALIDATING: it is running,
            # but it has NOT satisfied the predicate and must not read as READY.
            return snapshot
        self._transition(DeploymentState.READY)
        return snapshot

    def observe(self) -> Any:
        """Re-evaluate health. READY is revocable (plan WP5.1)."""
        if self._observe_fn is None:
            return self.last_snapshot
        try:
            snapshot = self._observe_fn()
        except Exception as exc:
            raise self._fail("observe", "OBSERVE_FAILED", str(exc), DeploymentError) from exc
        self.last_snapshot = snapshot
        ready = getattr(snapshot, "ready", None)
        if type(ready) is not bool:
            raise self._fail(
                "observe",
                "INVALID_VERDICT",
                "observation snapshot must expose an exact boolean ready field",
                DeploymentError,
            )
        if ready is False and self.state == DeploymentState.READY:
            self._transition(DeploymentState.VALIDATING)
        elif ready is True and self.state == DeploymentState.VALIDATING:
            self._transition(DeploymentState.READY)
        return snapshot

    def drain(self, deadline_s: float = 30.0) -> None:
        if (
            isinstance(deadline_s, bool)
            or not isinstance(deadline_s, (int, float))
            or not math.isfinite(float(deadline_s))
            or deadline_s < 0
        ):
            raise ValueError("deployment drain deadline must be finite and non-negative")
        if self.state in (DeploymentState.STOPPED, DeploymentState.FAILED):
            return
        self._transition(DeploymentState.DRAINING)
        if self._drain_fn is None:
            return
        try:
            self._drain_fn(deadline_s)
        except Exception as exc:
            raise self._fail("drain", "DRAIN_FAILED", str(exc), DrainError) from exc

    def stop(self) -> None:
        """Idempotent; publish STOPPED only after cleanup succeeds."""
        if self._stop_complete or self.state == DeploymentState.STOPPED:
            return
        if self._stop_fn is not None:
            try:
                self._stop_fn()
            except Exception as exc:
                cause = DeploymentCause(
                    "stop",
                    "CLEANUP_ERROR",
                    str(exc) or type(exc).__name__,
                )
                if self.first_cause is None:
                    self.first_cause = cause
                else:
                    self.secondary_causes.append(cause)
                if self.state != DeploymentState.FAILED:
                    self.state = DeploymentState.FAILED
                    self._append_history(self.state)
                raise DrainError(f"stop: cleanup failed: {exc}") from exc
        self._stop_complete = True
        if self.state == DeploymentState.DRAINING:
            self._transition(DeploymentState.STOPPED)
        elif self.state not in (DeploymentState.FAILED, DeploymentState.STOPPED):
            self.state = DeploymentState.STOPPED
            self._append_history(self.state)

    # -- reporting ---------------------------------------------------------
    def is_ready(self) -> bool:
        return self.state == DeploymentState.READY

    def to_dict(self) -> dict:
        return {
            "deployment_id": self.deployment_id,
            "generation": self.generation,
            "state": self.state,
            "first_cause": str(self.first_cause) if self.first_cause else None,
            "secondary_causes": [str(cause) for cause in self.secondary_causes],
            "history": [{"state": s, "at": t} for s, t in self.history],
            "history_events_dropped": self.history_events_dropped,
        }
