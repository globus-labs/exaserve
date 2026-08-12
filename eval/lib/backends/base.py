"""Base classes and utilities for backend adapters.

BackendAdapter defines the lifecycle contract that every backend must implement:

  validate()                — check the RunMaterialization/backend combination.
  build_runtime_manifest()  — write a backend-specific config (e.g., ExpConfig YAML
                              for the ray backend) that the serving code reads at launch.
  runtime_env()             — return env script path + env vars for the PBS job.
  launch()                  — start the serving cluster, return a LaunchedBackend handle.
  wait_ready()              — wait on canonical DeploymentStatus readiness.
  discover_targets()        — return base URLs the replay client should hit.
  stop()                    — tear down the cluster.

``BackendProcessHandle`` tails diagnostics and observes process death. Serving
readiness comes only from the shared generation-specific DeploymentStatus API.
"""

from __future__ import annotations

import abc
import collections
import math
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from ..models import RunMaterialization


@dataclass
class RuntimeEnvSpec:
    env_script: str
    exports: dict[str, str] = field(default_factory=dict)


@dataclass
class BackendProcessHandle:
    process: subprocess.Popen[str] | None
    log_path: str
    status_dir: str = ""
    expected_generation: int | None = None
    expected_plan_hash: str = ""
    readiness_source: str = ""
    recent_lines: collections.deque[str] = field(
        default_factory=lambda: collections.deque(maxlen=40)
    )
    _thread: threading.Thread | None = None
    _reader_error: str | None = field(default=None, init=False, repr=False)
    process_group: int | None = field(default=None, init=False)

    def start(self) -> "BackendProcessHandle":
        if self.process is None:
            return self
        try:
            self.process_group = os.getpgid(self.process.pid)
        except OSError:
            # launch uses start_new_session=True, so the immutable intended
            # group identity is the child PID even if it exited immediately.
            self.process_group = self.process.pid
        if self.process.stdout is None:
            return self

        def reader() -> None:
            try:
                os.makedirs(os.path.dirname(os.path.abspath(self.log_path)), exist_ok=True)
                with open(self.log_path, "a", encoding="utf-8") as log_handle:
                    for line in self.process.stdout:
                        log_handle.write(line)
                        log_handle.flush()
                        print(line, end="", flush=True)
                        self.recent_lines.append(line.rstrip())
            except Exception as exc:
                self._reader_error = f"{type(exc).__name__}: {exc}"
                print(
                    f"[eval backend] diagnostic reader failed: {self._reader_error}",
                    file=sys.stderr,
                    flush=True,
                )

        self._thread = threading.Thread(target=reader, daemon=True)
        self._thread.start()
        return self

    def wait_for_ready(self, timeout_s: float) -> bool:
        """Wait for this generation's canonical status or fail fast on exit."""
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(float(timeout_s))
            or timeout_s <= 0
        ):
            raise ValueError("backend readiness timeout must be finite and positive")
        from exaserve.status_api import read_deployment_status

        if not self.status_dir:
            raise RuntimeError(
                "backend has no DeploymentStatus directory; stdout is not a readiness fallback"
            )
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            if self._reader_error is not None:
                raise RuntimeError(f"backend diagnostic reader failed: {self._reader_error}")
            status = read_deployment_status(self.status_dir)
            if status is not None:
                identity_matches = (
                    self.expected_generation is None
                    or status.generation == self.expected_generation
                ) and (
                    not self.expected_plan_hash
                    or status.deployment_plan_hash == self.expected_plan_hash
                )
                if identity_matches and status.ready:
                    self.readiness_source = "deployment_status"
                    return True
                if identity_matches and status.terminal:
                    return False
            if self.process is not None and self.process.poll() is not None:
                return False  # exited before becoming ready → launch failed
            time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))
        if self._reader_error is not None:
            raise RuntimeError(f"backend diagnostic reader failed: {self._reader_error}")
        return False

    def close(self, *, deadline_s: float = 12.0, deadline: float | None = None) -> None:
        for name, value in (("deadline_s", deadline_s), ("deadline", deadline)):
            if value is None:
                continue
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise ValueError(f"backend monitor {name} must be finite and nonnegative")
        deadline = float(deadline) if deadline is not None else time.monotonic() + float(deadline_s)

        def remaining() -> float:
            return max(0.0, deadline - time.monotonic())

        if self._thread is not None:
            self._thread.join(timeout=remaining())
            if self._thread.is_alive():
                raise RuntimeError("backend diagnostic reader did not stop by cleanup deadline")
        if (
            self.process is not None
            and self.process.stdout is not None
            and not self.process.stdout.closed
        ):
            self.process.stdout.close()
        if self._reader_error is not None:
            raise RuntimeError(f"backend diagnostic reader failed: {self._reader_error}")


@dataclass
class LaunchedBackend:
    monitor: BackendProcessHandle
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BackendRunContext:
    run_plan: RunMaterialization

    @property
    def repo_root(self) -> str:
        return self.run_plan.repo_root


class BackendAdapter(abc.ABC):
    name: str = ""

    @abc.abstractmethod
    def validate(self, run_plan: RunMaterialization) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def build_runtime_manifest(self, run_plan: RunMaterialization) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def runtime_env(self, run_plan: RunMaterialization) -> RuntimeEnvSpec:
        raise NotImplementedError

    def job_env_exports(self, run_plan: RunMaterialization) -> dict[str, str]:
        """Env vars exported at the PBS job-script level, visible to the WHOLE
        job: the server launch, discover_targets, and the replay client. This is
        distinct from runtime_env().exports, which are applied only to the launch
        subprocess. Default: none."""
        return {}

    @abc.abstractmethod
    def launch(self, run_ctx: BackendRunContext) -> LaunchedBackend:
        raise NotImplementedError

    @abc.abstractmethod
    def wait_ready(self, run_ctx: BackendRunContext, launched: LaunchedBackend) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def discover_targets(
        self,
        run_ctx: BackendRunContext,
        launched: LaunchedBackend,
    ) -> list[str]:
        raise NotImplementedError

    @abc.abstractmethod
    def stop(self, run_ctx: BackendRunContext, launched: LaunchedBackend) -> None:
        raise NotImplementedError


def process_group_exists(process_group: int | None) -> bool:
    if process_group is None:
        return False
    try:
        os.killpg(process_group, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # A group we launched should never become unowned.  Treating this as
        # absent would allow cleanup to report success while live processes
        # remain outside our signalling authority.
        return True


def terminate_process_tree(
    process: subprocess.Popen[str] | None,
    *,
    process_group: int | None = None,
    deadline_s: float = 20.0,
    deadline: float | None = None,
    graceful_s: float | None = None,
) -> None:
    """Boundedly reap an owned process group, even after its leader exits."""
    if process is None:
        return
    for name, value in (
        ("deadline_s", deadline_s),
        ("deadline", deadline),
        ("graceful_s", graceful_s),
    ):
        if value is None:
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value < 0
        ):
            raise ValueError(f"process cleanup {name} must be finite and nonnegative")
    if process_group is None:
        try:
            process_group = os.getpgid(process.pid)
        except OSError:
            process_group = process.pid
    deadline = float(deadline) if deadline is not None else time.monotonic() + float(deadline_s)

    def signal_owned(sig: signal.Signals) -> None:
        try:
            os.killpg(process_group, sig)
        except ProcessLookupError:
            return
        except OSError:
            if process.poll() is None:
                process.send_signal(sig)

    signal_owned(signal.SIGTERM)
    now = time.monotonic()
    term_deadline = (
        now + max(0.0, deadline - now) / 2
        if graceful_s is None
        else min(deadline, now + float(graceful_s))
    )
    while time.monotonic() < term_deadline and process_group_exists(process_group):
        # ``killpg(..., 0)`` continues to report a process group whose leader
        # has exited but remains an unreaped zombie. Poll the Popen owner in
        # the graceful phase so waitpid can reap that leader immediately. A
        # clean ExaServe shutdown otherwise burns the entire watchdog despite
        # having no live owned process, delaying every accepted experiment by
        # up to two minutes and consuming scale-allocation walltime.
        process.poll()
        if not process_group_exists(process_group):
            break
        time.sleep(min(0.05, max(0.0, term_deadline - time.monotonic())))
    if process_group_exists(process_group):
        signal_owned(signal.SIGKILL)
    while time.monotonic() < deadline and process_group_exists(process_group):
        if process.poll() is None:
            try:
                process.wait(timeout=min(0.05, max(0.0, deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                pass
        else:
            time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
    if process.poll() is None:
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"process pid={process.pid} survived SIGKILL") from exc
    if process_group_exists(process_group):
        raise RuntimeError(
            f"process group {process_group} retained members after its cleanup deadline"
        )
