"""ExaWorks PSI/J scheduler backend — the portable submit layer.

Uses psij-python (https://github.com/ExaWorks/psij-python) to render + submit
jobs on any scheduler PSI/J supports (PBS Pro, Slurm, LSF, Flux, Cobalt, ...),
so new sites need no hand-written backend. Registered as ``psij`` (alias
``exawork``) and the **default** scheduler; the hand-rolled ``pbs``/``slurm``
backends remain as alternatives (``EXASERVE_SCHEDULER=pbs``).

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
import os
import shutil
from datetime import timedelta
from pathlib import Path
from typing import Any, Dict, Optional

from .base import JobSpec, SchedulerBackend

_SPEC_MARKER = "# PSIJ-SPEC: "


def detect_psij_executor() -> str:
    """Pick the PSI/J executor for this host: env override, else PATH probing."""
    override = os.environ.get("EXASERVE_PSIJ_EXECUTOR")
    if override:
        return override.lower()
    for cmd, executor in (
        ("qsub", "pbs"),
        ("sbatch", "slurm"),
        ("bsub", "lsf"),
        ("flux", "flux"),
    ):
        if shutil.which(cmd):
            return executor
    raise RuntimeError(
        "could not detect a batch scheduler (no qsub/sbatch/bsub/flux on PATH); "
        "set EXASERVE_PSIJ_EXECUTOR explicitly"
    )


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
        extra = os.environ.get("EXASERVE_PSIJ_ATTRS")
        if extra:
            attrs.update(json.loads(extra))
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
            "stdout_path": str((Path(spec.log_dir) / f"{spec.job_name}.out").resolve()),
            "stderr_path": str((Path(spec.log_dir) / f"{spec.job_name}.err").resolve()),
            "exclusive": os.environ.get("EXASERVE_PSIJ_EXCLUSIVE", "1") == "1",
        }
        return (
            "#!/bin/bash -l\n"
            f"{_SPEC_MARKER}{json.dumps(header)}\n"
            "# Rendered for the ExaWorks PSI/J backend: the header above is parsed by\n"
            "# exaserve.schedulers.psij_backend.submit(); the body below is the same\n"
            "# scheduler-agnostic body the native backends use.\n\n"
            + self._body(spec)
        )

    # ---- submit --------------------------------------------------------------

    @staticmethod
    def _parse_spec_header(job_path: Path) -> Dict[str, Any]:
        with open(job_path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(_SPEC_MARKER):
                    return json.loads(line[len(_SPEC_MARKER):])
        raise RuntimeError(
            f"{job_path} has no '{_SPEC_MARKER.strip()}' header — was it rendered "
            "by the psij backend? (EXASERVE_SCHEDULER=pbs for native scripts)"
        )

    def submit(self, job_path: Path) -> str:
        try:
            from psij import (  # noqa: PLC0415 — lazy: only submit hosts need psij
                Job,
                JobAttributes,
                JobExecutor,
                JobSpec as PJobSpec,
                ResourceSpecV1,
            )
        except ImportError as exc:
            raise RuntimeError(
                "the default scheduler backend uses ExaWorks PSI/J; install it "
                "(`pip install --user psij-python`) or select the native backend "
                "(EXASERVE_SCHEDULER=pbs|slurm)"
            ) from exc

        # Absolutize: PSI/J executes the payload from the JOB's cwd ($HOME on
        # PBS), so a relative script path is simply not found (measured: job
        # 8731123 died with "No such file or directory"). And run bash with -l:
        # PSI/J invokes `/bin/bash <script>`, which ignores the script's
        # `#!/bin/bash -l` shebang — without a login shell there is no `module`
        # function for env_setup (native qsub honors the shebang).
        job_path = Path(job_path).resolve()
        h = self._parse_spec_header(job_path)
        pspec = PJobSpec(
            executable="/bin/bash",
            arguments=["-l", str(job_path)],
            name=h["job_name"],
            # False matches the native backends (no -V / --export=ALL): the job
            # env comes from the body's env_setup, and login-shell variables
            # (e.g. EXASERVE_SCHEDULER=psij itself) must not leak into the job.
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
        executor.submit(job)
        if not job.native_id:
            raise RuntimeError(f"psij submitted {job_path} but returned no native id")
        return str(job.native_id)

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

    def job_state(self, job_id: str) -> Optional[str]:
        native = self._native()
        if native is not None:
            return native.job_state(job_id)
        # Generic fallback: PSI/J attach. Asynchronous poller (~30s interval),
        # and cannot distinguish FAILED from COMPLETED without its .ec file.
        from psij import Job, JobExecutor
        executor = JobExecutor.get_instance(self._executor_name())
        job = Job()
        executor.attach(job, job_id)
        status = job.wait(timeout=timedelta(seconds=60))
        state = str(status.state) if status else None
        return {
            "JobState.ACTIVE": "R",
            "JobState.QUEUED": "Q",
            "JobState.HELD": "H",
            "JobState.COMPLETED": "D",
            "JobState.FAILED": "D",
            "JobState.CANCELED": "D",
        }.get(state, state)

    def head_node(self, job_id: str) -> Optional[str]:
        native = self._native()
        if native is not None:
            return native.head_node(job_id)
        raise NotImplementedError(
            f"head-node resolution is not implemented for psij executor "
            f"{self._executor_name()!r} (PSI/J does not expose the allocated "
            "nodelist client-side); use a scheduler with a native helper "
            "(pbs/slurm) or resolve the URL from the job's run logs"
        )
