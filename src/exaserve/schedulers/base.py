"""One typed scheduler boundary shared by serving and evaluation.

Schedulers are real external process boundaries, so this module deliberately
keeps shell rendering, submission, observation, cancellation, and recovery in
one contract.  Callers provide a structured :class:`JobSpec`; they never paste
an ad-hoc command into a scheduler template.
"""

from __future__ import annotations

import re
import shlex
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Optional, Sequence

from ..control.finite_process import FiniteProcessError, run_finite


_SAFE_DIRECTIVE = re.compile(r"^[A-Za-z0-9 ._:/@=+%-]*$")
_SAFE_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_directive_field(value: str | Path, field_name: str) -> str:
    if not isinstance(value, (str, Path)):
        raise ValueError(f"scheduler field {field_name!r} must be text or a path")
    text = str(value)
    if "\n" in text or "\r" in text or not _SAFE_DIRECTIVE.fullmatch(text):
        raise ValueError(f"scheduler field {field_name!r} has disallowed characters: {text!r}")
    return text


@dataclass(frozen=True)
class JobSpec:
    """Scheduler-independent description of one batch job.

    ``bootstrap_script`` is the sole deliberately raw shell seam.  It is site
    administrator code (for example ``module load``), never experiment data.
    Every other caller-controlled value is rendered with ``shlex.quote``.
    """

    job_name: str
    num_nodes: int
    walltime: str
    account: str
    stdout_dir: Path
    stderr_dir: Path
    command_argv: tuple[str, ...]
    queue: str = ""
    cwd: Optional[Path] = None
    environment: Mapping[str, str] = field(default_factory=dict)
    bootstrap_script: str = ""
    source_env_script: Optional[Path] = None
    pythonpath: tuple[Path, ...] = ()
    environment_unset: tuple[str, ...] = ()
    filesystems: Optional[str] = None
    keep_flag: Optional[str] = None
    gpus_per_node: Optional[int] = None
    mail_user: str = ""
    mail_events: str = ""
    run_identity: str = ""
    exclusive: bool = True

    def __post_init__(self) -> None:
        if (
            not isinstance(self.command_argv, tuple)
            or not self.command_argv
            or any(not isinstance(item, str) for item in self.command_argv)
        ):
            raise ValueError("JobSpec.command_argv must be a non-empty argv tuple")
        if (
            isinstance(self.num_nodes, bool)
            or not isinstance(self.num_nodes, int)
            or self.num_nodes <= 0
        ):
            raise ValueError("JobSpec.num_nodes must be positive")
        if self.gpus_per_node is not None and (
            isinstance(self.gpus_per_node, bool)
            or not isinstance(self.gpus_per_node, int)
            or self.gpus_per_node < 0
        ):
            raise ValueError("JobSpec.gpus_per_node must be non-negative")
        for name in (
            "walltime",
            "account",
            "job_name",
            "queue",
            "filesystems",
            "keep_flag",
            "mail_user",
            "mail_events",
            "run_identity",
        ):
            value = getattr(self, name)
            if value is not None:
                if not isinstance(value, str):
                    raise ValueError(f"scheduler field {name!r} must be text")
                validate_directive_field(value, name)
        for name in ("walltime", "account", "job_name"):
            if not getattr(self, name):
                raise ValueError(f"scheduler field {name!r} must be non-empty")
        if not re.fullmatch(r"\d+:[0-5]\d:[0-5]\d", self.walltime):
            raise ValueError("scheduler walltime must use HH:MM:SS")
        for name in ("stdout_dir", "stderr_dir", "cwd", "source_env_script"):
            value = getattr(self, name)
            if value is None:
                continue
            if not isinstance(value, (str, Path)):
                raise ValueError(f"JobSpec.{name} must be a path")
            if name in {"stdout_dir", "stderr_dir"}:
                validate_directive_field(value, name)
        if not isinstance(self.pythonpath, tuple) or any(
            not isinstance(path, (str, Path)) for path in self.pythonpath
        ):
            raise ValueError("JobSpec.pythonpath must be a tuple of paths")
        if not isinstance(self.environment_unset, tuple) or any(
            not isinstance(name, str) or not _SAFE_ENV_NAME.fullmatch(name)
            for name in self.environment_unset
        ):
            raise ValueError("JobSpec.environment_unset must be a tuple of environment names")
        if len(self.environment_unset) != len(set(self.environment_unset)):
            raise ValueError("JobSpec.environment_unset must not contain duplicates")
        if not isinstance(self.bootstrap_script, str):
            raise ValueError("JobSpec.bootstrap_script must be text")
        if not isinstance(self.exclusive, bool):
            raise ValueError("JobSpec.exclusive must be boolean")
        if not isinstance(self.environment, Mapping):
            raise ValueError("JobSpec.environment must be a mapping")
        for key, value in self.environment.items():
            if not isinstance(key, str) or not _SAFE_ENV_NAME.fullmatch(key):
                raise ValueError(f"invalid job environment variable: {key!r}")
            if not isinstance(value, str):
                raise ValueError(f"job environment value for {key!r} must be text")
            if "\x00" in value:
                raise ValueError(f"job environment value for {key!r} contains NUL")
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))
        for name in ("stdout_dir", "stderr_dir", "cwd", "source_env_script"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, Path(value))
        object.__setattr__(self, "pythonpath", tuple(Path(path) for path in self.pythonpath))
        for arg in self.command_argv:
            if "\x00" in arg:
                raise ValueError("JobSpec.command_argv contains NUL")


class SchedulerState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    HELD = "HELD"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class Submission:
    job_id: str
    raw_output: str

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, str) or not self.job_id:
            raise ValueError("scheduler submission job_id must be non-empty text")
        if not isinstance(self.raw_output, str):
            raise ValueError("scheduler submission raw_output must be text")


class SubmissionError(RuntimeError):
    """Base error for the scheduler's submit ownership boundary."""


class SubmissionRejected(SubmissionError):
    """The scheduler command definitively did not report acceptance."""


class SubmissionAmbiguous(SubmissionError):
    """The scheduler may own a job; exact reconciliation is required."""


@dataclass(frozen=True)
class JobObservation:
    job_id: str
    state: SchedulerState
    raw_state: str = ""
    job_name: str = ""
    head_node: Optional[str] = None
    reason: str = ""
    observed_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def __post_init__(self) -> None:
        if not isinstance(self.job_id, str) or not self.job_id:
            raise ValueError("scheduler observation job_id must be non-empty text")
        if not isinstance(self.state, SchedulerState):
            raise ValueError("scheduler observation state must be a SchedulerState")
        for name in ("raw_state", "job_name", "reason", "observed_at"):
            if not isinstance(getattr(self, name), str):
                raise ValueError(f"scheduler observation {name} must be text")
        if self.head_node is not None and (
            not isinstance(self.head_node, str) or not self.head_node
        ):
            raise ValueError("scheduler observation head_node must be null or non-empty text")
        try:
            observed_at = datetime.fromisoformat(self.observed_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("scheduler observation observed_at must be ISO-8601") from exc
        if observed_at.tzinfo is None:
            raise ValueError("scheduler observation observed_at must include a timezone")


@dataclass(frozen=True)
class AllocationMetadata:
    job_id: str
    head_node: str
    nodes: tuple[str, ...]

    def __post_init__(self) -> None:
        for name in ("job_id", "head_node"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise ValueError(f"scheduler allocation {name} must be non-empty text")
        if not isinstance(self.nodes, (tuple, list)) or any(
            not isinstance(node, str) or not node for node in self.nodes
        ):
            raise ValueError("scheduler allocation nodes must contain non-empty strings")
        object.__setattr__(self, "nodes", tuple(self.nodes))
        if len(self.nodes) != len(set(self.nodes)):
            raise ValueError("scheduler allocation nodes must be unique")
        if self.head_node not in self.nodes:
            raise ValueError("scheduler allocation head_node must be in nodes")


def run_cmd(
    cmd: Sequence[str],
    timeout_s: int,
    *,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    if isinstance(cmd, (str, bytes)) or not cmd or any(not isinstance(item, str) for item in cmd):
        raise ValueError("scheduler command must be a non-empty string argument vector")
    return run_finite(
        list(cmd),
        timeout_s=timeout_s,
        check=check,
    )


def run_submission_cmd(cmd: Sequence[str], timeout_s: int) -> subprocess.CompletedProcess[str]:
    """Run a submit command while preserving accepted-vs-unknown semantics."""
    try:
        return run_cmd(cmd, timeout_s)
    except (subprocess.CalledProcessError, OSError) as exc:
        raise SubmissionRejected(str(exc)) from exc
    except FiniteProcessError as exc:
        # Timeout or a process-group contract failure can happen after the
        # scheduler accepted the request. Treating it as rejection permits a
        # duplicate submit and is therefore unsafe.
        raise SubmissionAmbiguous(str(exc)) from exc


def default_queue_and_walltime(num_nodes: int) -> tuple[str, str]:
    """Conservative Aurora defaults for a topology without explicit values.

    The site's capacity queue covers 1--16 nodes (and permits longer jobs),
    while debug-scaling covers 2--256 nodes with a one-hour maximum.  Production
    starts at 256 nodes.  At the overlapping 256-node boundary we choose prod
    so a default is not silently constrained to a debug reservation.
    """
    if isinstance(num_nodes, bool) or not isinstance(num_nodes, int) or num_nodes <= 0:
        raise ValueError("num_nodes must be a positive integer")
    if num_nodes <= 16:
        return "capacity", "01:00:00"
    if num_nodes < 256:
        return "debug-scaling", "01:00:00"
    return "prod", "02:00:00"


class SchedulerBackend(ABC):
    name = "base"
    submit_timeout_s = 600
    status_timeout_s = 600

    @abstractmethod
    def render_job(self, spec: JobSpec) -> str:
        """Return the complete, inspectable job script."""

    @abstractmethod
    def submit(self, job_path: Path) -> Submission:
        """Submit once or raise; never encode errors as an ambiguous string."""

    @abstractmethod
    def observe(self, job_id: str) -> JobObservation:
        """Return one scheduler observation; observation failure is UNKNOWN."""

    @abstractmethod
    def cancel(self, job_id: str) -> None:
        """Request cancellation or raise if the scheduler rejects it."""

    def attach(self, job_id: str) -> JobObservation:
        """Recover/attach to a known native identity."""
        return self.observe(job_id)

    def find_by_run_identity(
        self,
        run_identity: str,
        *,
        user: Optional[str] = None,
    ) -> tuple[JobObservation, ...]:
        """Reconcile an ambiguous submit intent.  Empty means no exact match.

        Backends must not return fuzzy matches.  A backend that cannot search
        exact scheduler-visible identities fails closed via ``NotImplementedError``.
        """
        raise NotImplementedError(f"{self.name} cannot reconcile by exact run identity")

    def allocation_metadata(self, job_id: str) -> Optional[AllocationMetadata]:
        observation = self.observe(job_id)
        if observation.state is not SchedulerState.RUNNING or not observation.head_node:
            return None
        return AllocationMetadata(job_id, observation.head_node, (observation.head_node,))

    def count_queued(self, user: str) -> Optional[dict[str, int]]:
        """Return active jobs by queue, or None when observation failed."""
        return None

    def slot_limits(self) -> dict[str, Optional[int]]:
        return {}

    def _body(self, spec: JobSpec) -> str:
        lines = [
            # Site bootstrap (notably Lmod on Aurora) is not nounset-clean and
            # may inspect shell-family variables that are legitimately absent.
            # Keep exit/pipe failures strict, then enable nounset immediately
            # after the trusted site initialization boundary.
            "set -eo pipefail",
            "unset VIRTUAL_ENV PYTHONHOME CONDA_DEFAULT_ENV CONDA_PREFIX "
            "CONDA_PROMPT_MODIFIER _CE_CONDA _CE_M PYTHONPYCACHEPREFIX",
            # A run imports its source snapshot before the snapshot validator
            # executes.  Bytecode writes would otherwise mutate that supposedly
            # immutable tree and make its first import fail its own inventory.
            "export PYTHONDONTWRITEBYTECODE=1",
        ]
        if spec.bootstrap_script:
            lines.append(spec.bootstrap_script.rstrip("\n"))
        if spec.cwd is not None:
            lines.append(f"cd {shlex.quote(str(spec.cwd))}")
        if spec.pythonpath:
            joined = ":".join(str(path) for path in spec.pythonpath)
            lines.append("unset PYTHONPATH")
            lines.append(f"_ES_PYTHONPATH={shlex.quote(joined)}")
            lines.append('export PYTHONPATH="$_ES_PYTHONPATH"')
        if spec.source_env_script is not None:
            lines.append(f"source {shlex.quote(str(spec.source_env_script))}")
        for key in spec.environment_unset:
            lines.append(f"unset {key}")
        lines.append("set -u")
        for key, value in sorted(spec.environment.items()):
            lines.append(f"export {key}={shlex.quote(str(value))}")
        command = " ".join(shlex.quote(str(arg)) for arg in spec.command_argv)
        lines.append(f"exec {command}")
        return "\n".join(lines) + "\n"
