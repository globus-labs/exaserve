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
import os
import signal
import subprocess
import threading
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

    def wait_for_ready(self, timeout_s: float) -> bool:
        if self.ready_event.wait(timeout_s):
            return True
        if self.process is not None and self.process.poll() is not None:
            return False
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
