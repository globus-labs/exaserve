"""PBS Pro scheduler (Aurora). VALIDATED — the rendered header + qsub/qstat
behavior matches the pre-abstraction submit.py."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from .base import JobSpec, SchedulerBackend, run_cmd

_JOB_ID_RE = re.compile(r"^(\d+\.[\w\-.]+)")


class PBSScheduler(SchedulerBackend):
    name = "pbs"

    def render_job(self, spec: JobSpec) -> str:
        keep = f"#PBS -k {spec.keep_flag}\n" if spec.keep_flag else ""
        fs = f"#PBS -l filesystems={spec.filesystems}\n" if spec.filesystems else ""
        header = (
            "#!/bin/bash -l\n"
            f"#PBS -N {spec.job_name}\n"
            f"#PBS -A {spec.account}\n"
            f"{keep}{fs}"
            f"#PBS -l select={spec.num_nodes}\n"
            f"#PBS -l walltime={spec.walltime}\n"
            f"#PBS -q {spec.queue}\n"
            f"#PBS -o {spec.log_dir}/\n"
            f"#PBS -e {spec.log_dir}/\n\n"
        )
        return header + self._body(spec)

    def submit(self, job_path: Path) -> str:
        proc = run_cmd(["qsub", str(job_path)], self.submit_timeout_s)
        job_id = proc.stdout.strip().splitlines()[-1].strip()
        if not _JOB_ID_RE.match(job_id):
            raise RuntimeError(f"could not parse job id from qsub output: {proc.stdout!r}")
        return job_id

    # ---- status via qstat -f -------------------------------------------------

    def _qstat_field(self, job_id: str, field: str) -> Optional[str]:
        proc = run_cmd(["qstat", "-f", job_id], self.status_timeout_s, check=False)
        if proc.returncode != 0:
            proc = run_cmd(["qstat", "-fx", job_id], self.status_timeout_s, check=False)
            if proc.returncode != 0:
                raise RuntimeError(f"qstat failed for {job_id}: {proc.stderr.strip()}")
        for line in proc.stdout.splitlines():
            line = line.strip()
            if line.startswith(f"{field} ="):
                return line.split("=", 1)[1].strip()
        return None

    def job_state(self, job_id: str) -> Optional[str]:
        # PBS already uses R / Q / H / E / F.
        return self._qstat_field(job_id, "job_state")

    def head_node(self, job_id: str) -> Optional[str]:
        exec_host = self._qstat_field(job_id, "exec_host")
        if not exec_host:
            return None
        # "host1/0*208+host2/0*208+..." -> host1
        return exec_host.split("+", 1)[0].split("/", 1)[0]
