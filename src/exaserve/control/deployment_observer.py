"""Outer-supervisor ownership of the isolated deployment child's evidence.

The fallback deployment child is a GLOBAL component owned by the allocation-
head supervisor.  Its local Unix socket is therefore terminated here—not in
rank zero—and kernel peer credentials bind every accepted snapshot to the
exact child PID the supervisor created.  The bridge stores one bounded current
snapshot plus a receiver-monotonic arrival time; it creates no second control
or readiness authority.
"""

from __future__ import annotations

import math
from copy import deepcopy
import threading
import time
from typing import Callable, Optional

from .deployment_ipc import DeploymentIPCError, validate_snapshot


class DeploymentObserver:
    """Validate and retain current evidence from one owned local child."""

    def __init__(
        self,
        *,
        ingress,
        owned_pid: Callable[[], Optional[int]],
        plan,
        binding,
        poll_s: float = 0.2,
        log: Callable[[str], None] = print,
    ) -> None:
        if not callable(owned_pid) or not callable(log):
            raise ValueError("deployment observer callbacks must be callable")
        if not callable(getattr(ingress, "drain_with_peer", None)):
            raise ValueError("deployment observer ingress lacks drain_with_peer")
        if (
            isinstance(poll_s, bool)
            or not isinstance(poll_s, (int, float))
            or not math.isfinite(float(poll_s))
            or poll_s <= 0
        ):
            raise ValueError("deployment observer poll interval must be finite and positive")
        self.ingress = ingress
        self.owned_pid = owned_pid
        self.plan = plan
        self.binding = binding
        self.poll_s = poll_s
        self.log = log
        self._lock = threading.RLock()
        self._snapshot: Optional[dict] = None
        self._received_at: Optional[float] = None
        self._failure: Optional[str] = None
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, daemon=True, name="exaserve-deployment-observer"
        )
        self._thread.start()

    def stop(self, timeout_s: float = 5.0) -> bool:
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(float(timeout_s))
            or timeout_s < 0
        ):
            raise ValueError("deployment observer stop timeout must be finite and non-negative")
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout=max(0.0, timeout_s))
        return thread is None or not thread.is_alive()

    def failure(self) -> Optional[str]:
        with self._lock:
            return self._failure

    def current(self, *, max_age_s: float) -> Optional[dict]:
        if (
            isinstance(max_age_s, bool)
            or not isinstance(max_age_s, (int, float))
            or not math.isfinite(float(max_age_s))
            or max_age_s < 0
        ):
            raise ValueError("deployment observer max age must be finite and non-negative")
        with self._lock:
            if self._snapshot is None or self._received_at is None:
                return None
            if time.monotonic() - self._received_at > max_age_s:
                return None
            # Callers receive no mutable aliases into the authoritative
            # receiver-owned projection.
            return deepcopy(self._snapshot)

    def _record_failure(self, detail: str) -> None:
        with self._lock:
            if self._failure is None:
                self._failure = detail
                self.log(f"[DeploymentIPC] owned-child protocol failure: {detail}")

    def _drain(self) -> None:
        expected_pid = self.owned_pid()
        for peer_pid, payload in self.ingress.drain_with_peer():
            if expected_pid is None or peer_pid != expected_pid:
                # Same-user processes can discover a /tmp pathname. Kernel
                # credential mismatch makes their message non-authoritative,
                # but cannot let them fail a healthy generation.
                self.log(
                    f"[DeploymentIPC] rejected unowned pid {peer_pid}; expected {expected_pid}"
                )
                continue
            try:
                snapshot = validate_snapshot(
                    payload,
                    deployment_id=self.plan.deployment_id,
                    generation=self.binding.generation,
                    deployment_plan_hash=self.plan.deployment_plan_hash,
                    site_profile_hash=self.plan.site_profile_hash,
                    allocation_binding_hash=(self.binding.allocation_binding_hash),
                )
            except (DeploymentIPCError, TypeError, ValueError) as exc:
                self._record_failure(str(exc))
                continue
            with self._lock:
                self._snapshot = snapshot
                self._received_at = time.monotonic()

    def _run(self) -> None:
        while not self._stop.wait(self.poll_s):
            self._drain()
        self._drain()
