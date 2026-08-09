"""ExaWorks PSI/J scheduler backend — the portable submit layer.

Uses psij-python (https://github.com/ExaWorks/psij-python) to render + submit
jobs on any scheduler PSI/J supports (PBS Pro, Slurm, LSF, Flux, Cobalt, ...),
so new sites need no hand-written backend. It is registered as the optional
``psij``/``exawork`` backend. The first production SiteProfile selects the
native PBS backend; PSI/J remains gated until its executor is qualified.

Division of labor (see doc/exawork_psij_notes.md for the friction log):
- PSI/J: job-script rendering + submission (qsub/sbatch/bsub/... quirks).
- Ours:  job *body* (identical to the native backends), site defaults
  (queue/walltime/account), and status/head-node resolution — PSI/J cannot
  report the allocated nodelist client-side, and its poller is asynchronous,
  so for PBS/Slurm we answer status with the native one-shot qstat/squeue
  helpers. Other executors fall back to PSI/J ``attach`` for state.

The rendered artifact is a self-describing script: a ``# PSIJ-SPEC: {json}``
header line carries everything ``submit()`` needs, so render and submit can
happen in different processes (eval materialize vs submit_all) and the file
stays inspectable/re-submittable.

psij-python is required only where submission happens (login node); job
scripts and compute nodes never import it.
"""

from __future__ import annotations

import json
import shutil
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from .base import (
    JobObservation,
    JobSpec,
    SchedulerBackend,
    SchedulerState,
    Submission,
    SubmissionRejected,
)

_SPEC_MARKER = "# PSIJ-SPEC: "


def detect_psij_executor() -> str:
    """Pick the PSI/J executor from installed scheduler commands."""
    for cmd, executor in (
        ("qsub", "pbs"),
        ("sbatch", "slurm"),
        ("bsub", "lsf"),
        ("flux", "flux"),
    ):
        if shutil.which(cmd):
            return executor
    raise RuntimeError("could not detect a batch scheduler (no qsub/sbatch/bsub/flux on PATH)")


def _parse_walltime(walltime: str) -> timedelta:
    parts = [int(p) for p in walltime.split(":")]
    while len(parts) < 3:
        parts.insert(0, 0)
    h, m, s = parts[-3], parts[-2], parts[-1]
    return timedelta(hours=h, minutes=m, seconds=s)


class PSIJScheduler(SchedulerBackend):
    name = "psij"

    # ---- render --------------------------------------------------------------

    def _executor_name(self) -> str:
        return detect_psij_executor()

    def _custom_attributes(self, spec: JobSpec, executor: str) -> Dict[str, Any]:
        """Site/scheduler extras PSI/J has no first-class field for."""
        attrs: Dict[str, Any] = {}
        if executor.startswith("pbs"):
            if spec.filesystems:
                attrs[f"{executor}.l"] = f"filesystems={spec.filesystems}"
            if spec.keep_flag:
                attrs[f"{executor}.k"] = spec.keep_flag
        elif executor == "slurm" and spec.gpus_per_node:
            # PSI/J's Slurm template only maps gpus-per-task; per-node is the
            # HPC-site idiom (e.g. Delta), so inject it as a custom directive.
            attrs["slurm.gpus-per-node"] = spec.gpus_per_node
        return attrs

    def render_job(self, spec: JobSpec) -> str:
        executor = self._executor_name()
        header = {
            "executor": executor,
            "job_name": spec.job_name,
            "num_nodes": spec.num_nodes,
            "queue": spec.queue,
            "walltime": spec.walltime,
            "account": spec.account,
            "custom_attributes": self._custom_attributes(spec, executor),
            # Absolutized: PSI/J resolves relative stdout paths against the JOB's
            # working directory (not the submit cwd, which is what #PBS -o does).
            "stdout_path": str((Path(spec.stdout_dir) / f"{spec.job_name}.out").resolve()),
            "stderr_path": str((Path(spec.stderr_dir) / f"{spec.job_name}.err").resolve()),
            "exclusive": spec.exclusive,
            "run_identity": spec.run_identity,
        }
        return (
            "#!/bin/bash -l\n"
            f"{_SPEC_MARKER}{json.dumps(header)}\n"
            "# Rendered for the ExaWorks PSI/J backend: the header above is parsed by\n"
            "# exaserve.schedulers.psij_backend.submit(); the body below is the same\n"
            "# scheduler-agnostic body the native backends use.\n\n" + self._body(spec)
        )

    # ---- submit --------------------------------------------------------------

    @staticmethod
    def _parse_spec_header(job_path: Path) -> Dict[str, Any]:
        from ..state.atomic import regular_file_reader

        with regular_file_reader(job_path) as fh:
            for line in fh:
                if line.startswith(_SPEC_MARKER):
                    from ..state.atomic import strict_json_loads

                    return strict_json_loads(line[len(_SPEC_MARKER) :])
        raise RuntimeError(
            f"{job_path} has no '{_SPEC_MARKER.strip()}' header — was it rendered "
            "by the psij backend? (EXASERVE_SCHEDULER=pbs for native scripts)"
        )

    def submit(self, job_path: Path) -> Submission:
        try:
            from psij import (  # noqa: PLC0415 — lazy: only submit hosts need psij
                Job,
                JobAttributes,
                JobExecutor,
                JobSpec as PJobSpec,
                ResourceSpecV1,
            )
        except ImportError as exc:
            raise SubmissionRejected(
                "the optional PSI/J scheduler backend is not installed; install "
                "psij-python or select a qualified native scheduler backend"
            ) from exc

        # Absolutize: PSI/J executes the payload from the JOB's cwd ($HOME on
        # PBS), so a relative script path is simply not found (measured: job
        # 8731123 died with "No such file or directory"). And run bash with -l:
        # PSI/J invokes `/bin/bash <script>`, which ignores the script's
        # `#!/bin/bash -l` shebang — without a login shell there is no `module`
        # function for env_setup (native qsub honors the shebang).
        job_path = Path(job_path).resolve()
        try:
            h = self._parse_spec_header(job_path)
            pspec = PJobSpec(
                executable="/bin/bash",
                arguments=["-l", str(job_path)],
                name=h["job_name"],
                # False matches the native backends (no -V / --export=ALL):
                # job setup is explicit in the body and login-shell variables
                # must not leak into the allocation.
                inherit_environment=False,
                stdout_path=Path(h["stdout_path"]),
                stderr_path=Path(h["stderr_path"]),
                resources=ResourceSpecV1(
                    node_count=int(h["num_nodes"]),
                    processes_per_node=1,
                    exclusive_node_use=bool(h.get("exclusive", True)),
                ),
                attributes=JobAttributes(
                    # Always explicit: PSI/J defaults to a 10-minute walltime.
                    duration=_parse_walltime(h["walltime"]),
                    queue_name=h["queue"] or None,
                    account=h["account"] or None,
                    custom_attributes=h.get("custom_attributes") or {},
                ),
            )
            executor = JobExecutor.get_instance(h["executor"])
            job = Job(pspec)
        except Exception as exc:
            # No external submit call has happened yet, so retry is safe.
            raise SubmissionRejected(f"PSI/J submit preflight failed: {exc}") from exc
        from .base import SubmissionAmbiguous

        try:
            executor.submit(job)
        except Exception as exc:
            raise SubmissionAmbiguous(f"PSI/J submit ownership is unknown: {exc}") from exc
        if not job.native_id:
            raise SubmissionAmbiguous(f"PSI/J submit returned without a native id for {job_path}")
        return Submission(str(job.native_id), str(job.native_id))

    # ---- status: native one-shot helpers where we have them ------------------

    def _native(self) -> Optional[SchedulerBackend]:
        executor = self._executor_name()
        if executor.startswith("pbs"):
            from .pbs import PBSScheduler

            return PBSScheduler()
        if executor == "slurm":
            from .slurm import SlurmScheduler

            return SlurmScheduler()
        return None

    def observe(self, job_id: str) -> JobObservation:
        native = self._native()
        if native is not None:
            return native.observe(job_id)
        # Generic fallback: PSI/J attach. Asynchronous poller (~30s interval),
        # and cannot distinguish FAILED from COMPLETED without its .ec file.
        from psij import Job, JobExecutor

        executor = JobExecutor.get_instance(self._executor_name())
        job = Job()
        executor.attach(job, job_id)
        status = job.wait(timeout=timedelta(seconds=60))
        state = str(status.state) if status else ""
        normalized = {
            "JobState.ACTIVE": SchedulerState.RUNNING,
            "JobState.QUEUED": SchedulerState.PENDING,
            "JobState.HELD": SchedulerState.HELD,
            "JobState.COMPLETED": SchedulerState.COMPLETED,
            "JobState.FAILED": SchedulerState.FAILED,
            "JobState.CANCELED": SchedulerState.CANCELLED,
        }.get(state, SchedulerState.UNKNOWN)
        return JobObservation(job_id, normalized, raw_state=state)

    def cancel(self, job_id: str) -> None:
        native = self._native()
        if native is not None:
            native.cancel(job_id)
            return
        from psij import Job, JobExecutor

        executor = JobExecutor.get_instance(self._executor_name())
        job = Job()
        executor.attach(job, job_id)
        executor.cancel(job)

    def find_by_run_identity(self, run_identity: str, *, user=None):
        native = self._native()
        if native is not None:
            return native.find_by_run_identity(run_identity, user=user)
        raise NotImplementedError(
            f"exact run-identity reconciliation is not implemented for PSI/J "
            f"executor {self._executor_name()!r}"
        )

    def count_queued(self, user: str):
        native = self._native()
        return native.count_queued(user) if native is not None else None

    def slot_limits(self):
        native = self._native()
        return native.slot_limits() if native is not None else {}
