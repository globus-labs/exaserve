"""PBS Pro implementation of the shared scheduler contract."""

from __future__ import annotations

import getpass
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

_JOB_ID_RE = re.compile(r"^(\d+\.[\w.-]+)")
_STATE_MAP = {
    "Q": SchedulerState.PENDING,
    "W": SchedulerState.PENDING,
    "B": SchedulerState.PENDING,
    "R": SchedulerState.RUNNING,
    "E": SchedulerState.RUNNING,
    "H": SchedulerState.HELD,
    "S": SchedulerState.HELD,
    "F": SchedulerState.COMPLETED,
    "X": SchedulerState.CANCELLED,
}


def _parse_records(text: str) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    current: dict[str, str] = {}
    last_key: Optional[str] = None
    for raw in text.splitlines():
        line = raw.rstrip()
        if line.startswith("Job Id:"):
            if current:
                records.append(current)
            current = {"job_id": line.split(":", 1)[1].strip()}
            last_key = None
        elif " = " in line:
            key, value = line.strip().split(" = ", 1)
            current[key] = value
            last_key = key
        elif last_key and line[:1].isspace():
            current[last_key] += line.strip()
    if current:
        records.append(current)
    return records


class PBSScheduler(SchedulerBackend):
    name = "pbs"

    def render_job(self, spec: JobSpec) -> str:
        if not spec.queue:
            raise ValueError("PBS jobs require an explicit queue")
        keep = f"#PBS -k {spec.keep_flag}\n" if spec.keep_flag else ""
        fs = f"#PBS -l filesystems={spec.filesystems}\n" if spec.filesystems else ""
        mail = f"#PBS -m {spec.mail_events}\n#PBS -M {spec.mail_user}\n" if spec.mail_user else ""
        identity = f"# EXASERVE-RUN-IDENTITY: {spec.run_identity}\n" if spec.run_identity else ""
        header = (
            "#!/bin/bash -l\n"
            f"{identity}#PBS -N {spec.job_name}\n"
            f"#PBS -A {spec.account}\n{mail}{keep}{fs}"
            f"#PBS -l select={spec.num_nodes}\n"
            f"#PBS -l walltime={spec.walltime}\n"
            f"#PBS -q {spec.queue}\n"
            f"#PBS -o {spec.stdout_dir}/\n"
            f"#PBS -e {spec.stderr_dir}/\n\n"
        )
        return header + self._body(spec)

    def submit(self, job_path: Path) -> Submission:
        proc = run_submission_cmd(["qsub", str(job_path)], self.submit_timeout_s)
        output = proc.stdout.strip()
        job_id = output.splitlines()[-1].strip() if output else ""
        if not _JOB_ID_RE.fullmatch(job_id):
            raise SubmissionAmbiguous(
                f"qsub exited zero but its job id could not be parsed: {proc.stdout!r}"
            )
        return Submission(job_id=job_id, raw_output=output)

    def _query(self, *args: str) -> list[dict[str, str]]:
        proc = run_cmd(["qstat", *args], self.status_timeout_s, check=False)
        if proc.returncode != 0 and "-x" not in args:
            proc = run_cmd(["qstat", "-x", *args], self.status_timeout_s, check=False)
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or f"qstat {' '.join(args)} failed")
        return _parse_records(proc.stdout)

    def observe(self, job_id: str) -> JobObservation:
        try:
            records = self._query("-f", job_id)
        except Exception as exc:
            return JobObservation(job_id, SchedulerState.UNKNOWN, reason=str(exc))
        if not records:
            return JobObservation(
                job_id, SchedulerState.UNKNOWN, reason="scheduler returned no job record"
            )
        record = records[0]
        raw = record.get("job_state", "")
        state = _STATE_MAP.get(raw, SchedulerState.UNKNOWN)
        # PBS records a non-zero exit status only after completion.
        if raw == "F" and record.get("Exit_status", "0") not in ("", "0"):
            state = SchedulerState.FAILED
        exec_host = record.get("exec_host", "")
        head = exec_host.split("+", 1)[0].split("/", 1)[0] if exec_host else None
        return JobObservation(
            job_id=record.get("job_id", job_id),
            state=state,
            raw_state=raw,
            job_name=record.get("Job_Name", ""),
            head_node=head,
            reason=record.get("comment", ""),
        )

    def cancel(self, job_id: str) -> None:
        run_cmd(["qdel", job_id], self.submit_timeout_s)

    def find_by_run_identity(
        self,
        run_identity: str,
        *,
        user: Optional[str] = None,
    ) -> tuple[JobObservation, ...]:
        # Job_Name is scheduler-visible and exact within PBS's configured name
        # limit.  Materialization uses the same validated identity as job_name.
        try:
            active = self._query("-f", "-u", user or getpass.getuser())
            historical = self._query("-x", "-f", "-u", user or getpass.getuser())
        except Exception as exc:
            raise RuntimeError(f"cannot reconcile PBS submissions: {exc}") from exc
        records = []
        seen = set()
        for record in [*active, *historical]:
            job_id = record.get("job_id", "")
            if job_id and job_id not in seen:
                seen.add(job_id)
                records.append(record)
        matches = []
        for record in records:
            if record.get("Job_Name") != run_identity:
                continue
            matches.append(self.observe(record["job_id"]))
        return tuple(matches)

    def count_queued(self, user: str) -> Optional[dict[str, int]]:
        try:
            records = self._query("-f", "-u", user)
        except Exception:
            return None
        counts: dict[str, int] = {}
        for record in records:
            if _STATE_MAP.get(record.get("job_state", "")) not in {
                SchedulerState.PENDING,
                SchedulerState.RUNNING,
                SchedulerState.HELD,
            }:
                continue
            queue = record.get("queue", "")
            if queue:
                counts[queue] = counts.get(queue, 0) + 1
        return counts

    def slot_limits(self) -> dict[str, Optional[int]]:
        # ALCF publishes queue/node/walltime constraints, but no stable
        # per-user active-job quotas that this client can truthfully encode.
        # Submission orchestration therefore applies its own conservative
        # anti-flood fallback instead of presenting observations as policy.
        return {}
