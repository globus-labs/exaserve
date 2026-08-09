"""Slurm implementation of the shared scheduler contract."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from .base import (
    JobObservation,
    JobSpec,
    SchedulerBackend,
    SchedulerState,
    Submission,
    SubmissionAmbiguous,
    run_cmd,
    run_submission_cmd,
)

_SUBMIT_RE = re.compile(r"Submitted batch job (\d+)")
_STATE_MAP = {
    "RUNNING": SchedulerState.RUNNING,
    "COMPLETING": SchedulerState.RUNNING,
    "PENDING": SchedulerState.PENDING,
    "CONFIGURING": SchedulerState.PENDING,
    "REQUEUED": SchedulerState.PENDING,
    "RESIZING": SchedulerState.PENDING,
    "SUSPENDED": SchedulerState.HELD,
    "COMPLETED": SchedulerState.COMPLETED,
    "CANCELLED": SchedulerState.CANCELLED,
    "FAILED": SchedulerState.FAILED,
    "TIMEOUT": SchedulerState.FAILED,
    "NODE_FAIL": SchedulerState.FAILED,
    "PREEMPTED": SchedulerState.FAILED,
    "BOOT_FAIL": SchedulerState.FAILED,
    "OUT_OF_MEMORY": SchedulerState.FAILED,
}
_ACTIVE_SHORT = {"R", "PD", "CF", "CG", "RD", "RQ", "RS", "S", "RF"}


def _map_mail(events: str) -> str:
    values = []
    if "b" in events:
        values.append("BEGIN")
    if "e" in events:
        values.append("END")
    if "a" in events:
        values.append("FAIL")
    return ",".join(values) or "NONE"


class SlurmScheduler(SchedulerBackend):
    name = "slurm"

    def render_job(self, spec: JobSpec) -> str:
        part = f"#SBATCH --partition={spec.queue}\n" if spec.queue else ""
        gpn = f"#SBATCH --gpus-per-node={spec.gpus_per_node}\n" if spec.gpus_per_node else ""
        mail = (
            f"#SBATCH --mail-user={spec.mail_user}\n"
            f"#SBATCH --mail-type={_map_mail(spec.mail_events)}\n"
            if spec.mail_user
            else ""
        )
        identity = f"# EXASERVE-RUN-IDENTITY: {spec.run_identity}\n" if spec.run_identity else ""
        header = (
            "#!/bin/bash -l\n"
            f"{identity}#SBATCH --job-name={spec.job_name}\n"
            f"#SBATCH --account={spec.account}\n{part}"
            f"#SBATCH --nodes={spec.num_nodes}\n"
            "#SBATCH --ntasks-per-node=1\n"
            f"{gpn}#SBATCH --time={spec.walltime}\n"
            f"#SBATCH --output={spec.stdout_dir}/%x-%j.out\n"
            f"#SBATCH --error={spec.stderr_dir}/%x-%j.err\n{mail}\n"
        )
        return header + self._body(spec)

    def submit(self, job_path: Path) -> Submission:
        proc = run_submission_cmd(["sbatch", str(job_path)], self.submit_timeout_s)
        match = _SUBMIT_RE.search(proc.stdout)
        if not match:
            raise SubmissionAmbiguous(
                f"sbatch exited zero but its job id could not be parsed: {proc.stdout!r}"
            )
        return Submission(match.group(1), proc.stdout.strip())

    def _squeue(self, *args: str) -> Optional[str]:
        proc = run_cmd(["squeue", "-h", *args], self.status_timeout_s, check=False)
        return proc.stdout.strip() if proc.returncode == 0 else None

    def observe(self, job_id: str) -> JobObservation:
        raw = self._squeue("-j", job_id, "-o", "%i|%T|%j|%N|%R")
        if raw is None:
            return JobObservation(
                job_id, SchedulerState.UNKNOWN, reason="squeue observation failed"
            )
        if not raw:
            # Resolve final state through accounting; disappearance from squeue
            # alone is not evidence of success.
            proc = run_cmd(
                [
                    "sacct",
                    "-n",
                    "-P",
                    "-j",
                    job_id,
                    "--format=JobIDRaw,State,JobName,NodeList,Reason",
                ],
                self.status_timeout_s,
                check=False,
            )
            if proc.returncode != 0 or not proc.stdout.strip():
                return JobObservation(
                    job_id, SchedulerState.UNKNOWN, reason="job absent from squeue and sacct"
                )
            raw = proc.stdout.strip().splitlines()[0]
        parts = raw.split("|")
        if len(parts) < 5:
            return JobObservation(
                job_id, SchedulerState.UNKNOWN, reason=f"malformed Slurm observation: {raw!r}"
            )
        native_id, raw_state, name, nodelist, reason = parts[:5]
        raw_state = raw_state.split("+", 1)[0]
        head = None
        if nodelist and nodelist not in {"(null)", "N/A"}:
            hosts = run_cmd(
                ["scontrol", "show", "hostnames", nodelist], self.status_timeout_s, check=False
            )
            if hosts.returncode == 0 and hosts.stdout.strip():
                head = hosts.stdout.strip().splitlines()[0]
        return JobObservation(
            native_id or job_id,
            _STATE_MAP.get(raw_state, SchedulerState.UNKNOWN),
            raw_state,
            name,
            head,
            reason,
        )

    def cancel(self, job_id: str) -> None:
        run_cmd(["scancel", job_id], self.submit_timeout_s)

    def find_by_run_identity(
        self,
        run_identity: str,
        *,
        user: Optional[str] = None,
    ) -> tuple[JobObservation, ...]:
        raise NotImplementedError(
            "Slurm exact reconciliation is not qualified: active-only squeue "
            "search can miss an accepted job that already entered accounting"
        )

    def count_queued(self, user: str) -> Optional[dict[str, int]]:
        raw = self._squeue("-u", user, "-o", "%P|%t")
        if raw is None:
            return None
        counts: dict[str, int] = {}
        for line in raw.splitlines():
            partition, sep, state = line.partition("|")
            if sep and state in _ACTIVE_SHORT:
                counts[partition] = counts.get(partition, 0) + 1
        return counts
