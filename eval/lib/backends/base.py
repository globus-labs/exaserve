"""Base classes and utilities for backend adapters.

BackendAdapter defines the lifecycle contract that every backend must implement:

  validate()                — check that the RunPlan is valid for this backend.
  build_runtime_manifest()  — write a backend-specific config (e.g., ExpConfig YAML
                              for the ray backend) that the serving code reads at launch.
  runtime_env()             — return env script path + env vars for the PBS job.
  launch()                  — start the serving cluster, return a LaunchedBackend handle.
  wait_ready()              — block until the cluster signals readiness.
  discover_targets()        — return base URLs the replay client should hit.
  stop()                    — tear down the cluster.

ProcessMonitor is a reusable helper that tails a subprocess's stdout into a
log file while scanning for a readiness marker string.
"""

from __future__ import annotations

import abc
import collections
import glob
import json
import os
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Any

from ..models import RunPlan


@dataclass
class RuntimeEnvSpec:
    env_script: str
    exports: dict[str, str] = field(default_factory=dict)


@dataclass
class ProcessMonitor:
    process: subprocess.Popen[str] | None
    log_path: str
    ready_marker: str = ""
    # IMP-B02 cutover: the AUTHORITATIVE readiness signal is a structured
    # snapshot written by the readiness gate, not a line of stdout. When the
    # directory is provided we consume the file; the marker survives only as a
    # fallback for a backend that predates the gate, and taking that path is
    # logged so the migration is visible rather than silent.
    readiness_dir: str = ""
    readiness_marker_grace_s: float = 120.0
    readiness_source: str = ""
    ready_event: threading.Event = field(default_factory=threading.Event)
    recent_lines: collections.deque[str] = field(
        default_factory=lambda: collections.deque(maxlen=40)
    )
    _thread: threading.Thread | None = None

    def start(self) -> "ProcessMonitor":
        if self.process is None or self.process.stdout is None:
            self.ready_event.set()
            return self

        def reader() -> None:
            os.makedirs(os.path.dirname(os.path.abspath(self.log_path)), exist_ok=True)
            with open(self.log_path, "a", encoding="utf-8") as log_handle:
                for line in self.process.stdout:
                    log_handle.write(line)
                    log_handle.flush()
                    print(line, end="", flush=True)
                    self.recent_lines.append(line.rstrip())
                    if self.ready_marker and self.ready_marker in line:
                        self.ready_event.set()
            if not self.ready_marker:
                self.ready_event.set()

        self._thread = threading.Thread(target=reader, daemon=True)
        self._thread.start()
        return self

    def readiness_snapshot(self) -> dict | None:
        """Newest readiness.json under readiness_dir, or None."""
        if not self.readiness_dir:
            return None
        newest = None
        for path in glob.glob(os.path.join(self.readiness_dir, "**", "readiness.json"),
                              recursive=True):
            try:
                stamp = os.path.getmtime(path)
            except OSError:
                continue
            if newest is None or stamp > newest[0]:
                newest = (stamp, path)
        if newest is None:
            return None
        try:
            with open(newest[1], encoding="utf-8") as fh:
                data = json.load(fh)
        except (OSError, ValueError):
            return None
        return data if isinstance(data, dict) else None

    def wait_for_ready(self, timeout_s: float) -> bool:
        # Poll for the process dying DURING the wait: when launch_cluster crashes
        # without emitting the ready marker, the marker event is never set, so a
        # plain ready_event.wait(timeout_s) would block the ENTIRE timeout (i.e.
        # the walltime) instead of failing fast. Check poll() every couple seconds
        # so a failed launch aborts the job in seconds, not hours.
        deadline = time.monotonic() + timeout_s
        marker_at: float | None = None
        while time.monotonic() < deadline:
            snapshot = self.readiness_snapshot()
            if snapshot is not None:
                if snapshot.get("ready") is True:
                    self.readiness_source = "snapshot"
                    return True
                # A snapshot that says NOT ready overrides the marker: the text
                # can outrun the fact, the file cannot.
                marker_at = None
            elif self.ready_event.is_set():
                now = time.monotonic()
                marker_at = now if marker_at is None else marker_at
                if not self.readiness_dir or now - marker_at >= self.readiness_marker_grace_s:
                    self.readiness_source = "marker"
                    if self.readiness_dir:
                        print("[ProcessMonitor] WARNING: readiness accepted from the "
                              "stdout marker; no readiness.json appeared under "
                              f"{self.readiness_dir} within "
                              f"{self.readiness_marker_grace_s:.0f}s (legacy backend).",
                              flush=True)
                    return True
            if self.process is not None and self.process.poll() is not None:
                return False  # exited before becoming ready → launch failed
            time.sleep(min(2.0, max(0.0, deadline - time.monotonic())))
        return False

    def close(self) -> None:
        if self.process is not None and self.process.stdout is not None:
            try:
                self.process.stdout.close()
            except Exception:
                pass


@dataclass
class LaunchedBackend:
    monitor: ProcessMonitor
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BackendRunContext:
    run_plan: RunPlan

    @property
    def repo_root(self) -> str:
        return self.run_plan.repo_root


class BackendAdapter(abc.ABC):
    name: str = ""

    @abc.abstractmethod
    def validate(self, run_plan: RunPlan) -> None:
        raise NotImplementedError

    @abc.abstractmethod
    def build_runtime_manifest(self, run_plan: RunPlan) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def runtime_env(self, run_plan: RunPlan) -> RuntimeEnvSpec:
        raise NotImplementedError

    def job_env_exports(self, run_plan: RunPlan) -> dict[str, str]:
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


def terminate_process_tree(process: subprocess.Popen[str] | None) -> None:
    if process is None:
        return
    if process.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    except Exception:
        process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGKILL)
        except Exception:
            process.kill()
        process.wait()
