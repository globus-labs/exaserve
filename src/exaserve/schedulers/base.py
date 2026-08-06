"""
Abstract interface for pluggable batch schedulers (package side).

Backs ``exaserve-serve-submit`` / ``exaserve-serve-url``: render a one-shot job
script that runs ``exaserve-launch-cluster <config>``, submit it, and resolve the
running job to a head node. Selected by ``EXASERVE_SCHEDULER`` (default ``pbs``,
so Aurora is unchanged). The launcher itself is scheduler-agnostic via the
runtime seam (EXASERVE_NODEFILE / EXASERVE_MPILAUNCH); this layer only handles
submission + status.

Design doc: doc/design/scheduler_abstraction.md

Job states are normalized to a small set so callers don't special-case:
``R`` running, ``Q`` queued/pending, ``H`` held, ``D`` done, or the raw string.
"""

from __future__ import annotations

import shlex
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass
class JobSpec:
    config_path: Path
    launch_script: Path
    package_root: Path
    package_parent: Path
    num_nodes: int
    walltime: str
    account: str
    job_name: str
    log_dir: Path
    queue: str = ""                    # PBS queue / Slurm partition
    env_setup: str = ""                # shell that puts the runtime env on PATH
    vendor: Optional[str] = None       # EXASERVE_VENDOR for the job
    gpus_per_node: Optional[int] = None  # Slurm --gpus-per-node
    filesystems: Optional[str] = None  # PBS only
    keep_flag: Optional[str] = None    # PBS only

    def __post_init__(self) -> None:
        # PR-015: fields that land in #PBS/#SBATCH directive lines must not
        # carry newlines or shell metacharacters (env_setup is intentionally
        # raw operator shell and is exempt; it is treated as privileged code).
        import re as _re

        safe = _re.compile(r"^[A-Za-z0-9 ._:/@=+-]*$")
        for name in ("walltime", "account", "job_name", "queue",
                     "filesystems", "keep_flag", "vendor"):
            value = getattr(self, name)
            if value is None:
                continue
            text = str(value)
            if "\n" in text or "\r" in text or not safe.match(text):
                raise ValueError(
                    f"JobSpec.{name} has disallowed characters (newline or shell "
                    f"metacharacter): {text!r}"
                )


def run_cmd(cmd: List[str], timeout_s: int, check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        cmd, check=check, text=True, capture_output=True, timeout=timeout_s
    )


class SchedulerBackend(ABC):
    name: str = "base"
    submit_timeout_s: int = 600
    status_timeout_s: int = 600

    @abstractmethod
    def render_job(self, spec: JobSpec) -> str:
        """Return the full job-script text (header + body)."""

    @abstractmethod
    def submit(self, job_path: Path) -> str:
        """Submit the job script; return the scheduler's job id."""

    @abstractmethod
    def job_state(self, job_id: str) -> Optional[str]:
        """Normalized state: R / Q / H / D, or None if unknown."""

    @abstractmethod
    def head_node(self, job_id: str) -> Optional[str]:
        """First (head) node hostname once running, else None."""

    # ---- shared job-script body ---------------------------------------------

    def _body(self, spec: JobSpec) -> str:
        launch_q = shlex.quote(str(spec.launch_script))
        config_q = shlex.quote(str(spec.config_path.resolve()))
        root_q = shlex.quote(str(spec.package_root))
        parent_q = shlex.quote(str(spec.package_parent))
        vendor_line = (
            f"export EXASERVE_VENDOR={shlex.quote(spec.vendor)}\n" if spec.vendor else ""
        )
        return (
            "set -e\n"
            "unset VIRTUAL_ENV PYTHONHOME CONDA_DEFAULT_ENV CONDA_PREFIX "
            "CONDA_PROMPT_MODIFIER _CE_CONDA _CE_M\n"
            f"{spec.env_setup}\n"
            f"export EXASERVE_PACKAGE_ROOT={root_q}\n"
            f"export EXASERVE_PACKAGE_PARENT={parent_q}\n"
            f"{vendor_line}"
            f"exec bash {launch_q} {config_q}\n"
        )
