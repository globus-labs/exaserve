"""Slurm scheduler (e.g. NCSA Delta, Cray/Slurm sites). UNTESTED — implemented
from standard Slurm conventions; validate on a Slurm system before trusting.

Notes:
- ``--ntasks-per-node=1`` in the header matches the launcher's per-node srun.
- ``--gpus-per-node`` is Delta's documented idiom (``--gres=gpu:N`` also works);
  set it from the deployment's ``num_gpus_per_node``.
- Node sharing is the Slurm default; add ``--exclusive`` via the site env or a
  custom header if a site needs whole nodes.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from .base import JobSpec, SchedulerBackend, run_cmd

_SUBMIT_RE = re.compile(r"Submitted batch job (\d+)")
_STATE_MAP = {
    "RUNNING": "R", "COMPLETING": "R",
    "PENDING": "Q", "CONFIGURING": "Q", "REQUEUED": "Q", "RESIZING": "Q",
    "SUSPENDED": "H",
    "COMPLETED": "D", "CANCELLED": "D", "FAILED": "D", "TIMEOUT": "D",
    "NODE_FAIL": "D", "PREEMPTED": "D", "BOOT_FAIL": "D", "OUT_OF_MEMORY": "D",
}


class SlurmScheduler(SchedulerBackend):
    name = "slurm"

    def render_job(self, spec: JobSpec) -> str:
        part = f"#SBATCH --partition={spec.queue}\n" if spec.queue else ""
        gpn = (
            f"#SBATCH --gpus-per-node={spec.gpus_per_node}\n"
            if spec.gpus_per_node
            else ""
        )
        header = (
            "#!/bin/bash -l\n"
            f"#SBATCH --job-name={spec.job_name}\n"
            f"#SBATCH --account={spec.account}\n"
            f"{part}"
            f"#SBATCH --nodes={spec.num_nodes}\n"
            "#SBATCH --ntasks-per-node=1\n"
            f"{gpn}"
            f"#SBATCH --time={spec.walltime}\n"
            f"#SBATCH --output={spec.log_dir}/%x-%j.out\n"
            f"#SBATCH --error={spec.log_dir}/%x-%j.err\n\n"
        )
        return header + self._body(spec)

    def submit(self, job_path: Path) -> str:
        proc = run_cmd(["sbatch", str(job_path)], self.submit_timeout_s)
        m = _SUBMIT_RE.search(proc.stdout)
        if not m:
            raise RuntimeError(f"could not parse job id from sbatch output: {proc.stdout!r}")
        return m.group(1)

    def _squeue(self, job_id: str, fmt: str) -> Optional[str]:
        proc = run_cmd(
            ["squeue", "-h", "-j", job_id, "-o", fmt],
            self.status_timeout_s,
            check=False,
        )
        if proc.returncode != 0:
            # PR-014: a failed squeue is "unknown", never "completed". An
            # invalid/expired job id also lands here on most Slurm builds.
            return None
        return proc.stdout.strip()

    def job_state(self, job_id: str) -> Optional[str]:
        raw = self._squeue(job_id, "%T")
        if raw is None:
            return None  # scheduler unobservable — caller must not conclude
        if not raw:
            # squeue succeeded and the job is gone from the queue -> finished
            return "D"
        return _STATE_MAP.get(raw.splitlines()[0].strip(), raw.splitlines()[0].strip())

    def head_node(self, job_id: str) -> Optional[str]:
        nodelist = self._squeue(job_id, "%N")
        if not nodelist:
            return None
        hosts = run_cmd(
            ["scontrol", "show", "hostnames", nodelist.splitlines()[0].strip()],
            self.status_timeout_s,
            check=False,
        ).stdout.strip()
        return hosts.splitlines()[0].strip() if hosts else None
