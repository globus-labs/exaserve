"""ExaWorks PSI/J backend for the eval harness (default; native pbs/slurm remain).

Same design as the package-side ``exaserve.schedulers.psij_backend``: the
rendered job script is self-describing (``# PSIJ-SPEC: {json}`` header + the
standard eval body), so materialize (render) and submit_all (submit) can run in
different processes. Submission goes through PSI/J; queue counting and slot
limits — which PSI/J does not do — delegate to the native backend for the
detected executor (our site policy rides along).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Optional, Tuple

from .base import EvalScheduler

_SPEC_MARKER = "# PSIJ-SPEC: "


def _psij_helpers():
    """Import the package-side helpers (src/ is on PYTHONPATH for eval runs)."""
    from exaserve.schedulers.psij_backend import _parse_walltime, detect_psij_executor
    return detect_psij_executor, _parse_walltime


class PSIJEvalScheduler(EvalScheduler):
    name = "psij"

    def render_job(
        self,
        *,
        job_name: str,
        num_nodes: int,
        queue: str,
        walltime: str,
        project: str,
        filesystems: str,
        keep_output: str,
        stdout_dir: str,
        stderr_dir: str,
        mail_user: str,
        mail_events: str,
        code_root: str,
        env_script: str,
        run_yaml_path: str,
        job_exports: Optional[Dict[str, str]] = None,
        gpus_per_node: Optional[int] = None,
    ) -> str:
        detect, _ = _psij_helpers()
        executor = detect()
        custom: Dict[str, object] = {}
        if executor.startswith("pbs"):
            if filesystems:
                custom[f"{executor}.l"] = f"filesystems={filesystems}"
            if keep_output:
                custom[f"{executor}.k"] = keep_output
        elif executor == "slurm" and gpus_per_node:
            custom["slurm.gpus-per-node"] = gpus_per_node
        header = {
            "executor": executor,
            "job_name": job_name,
            "num_nodes": num_nodes,
            "queue": queue,
            "walltime": walltime,
            "account": project,
            "custom_attributes": custom,
            "stdout_path": f"{stdout_dir}/{job_name}.out",
            "stderr_path": f"{stderr_dir}/{job_name}.err",
            "exclusive": True,
        }
        return (
            "#!/bin/bash -l\n"
            f"{_SPEC_MARKER}{json.dumps(header)}\n\n"
            + self._body(
                code_root=code_root,
                env_script=env_script,
                run_yaml_path=run_yaml_path,
                job_exports=job_exports,
            )
        )

    def submit(self, job_path: str) -> Tuple[bool, str]:
        try:
            from psij import (
                Job,
                JobAttributes,
                JobExecutor,
                JobSpec,
                ResourceSpecV1,
            )
        except ImportError:
            return False, (
                "psij-python not installed (pip install --user psij-python), "
                "or set scheduler.type: pbs|slurm in the spec"
            )
        _, parse_walltime = _psij_helpers()
        header = None
        with open(job_path, "r", encoding="utf-8") as fh:
            for line in fh:
                if line.startswith(_SPEC_MARKER):
                    header = json.loads(line[len(_SPEC_MARKER):])
                    break
        if header is None:
            return False, f"{job_path}: no PSIJ-SPEC header (not a psij-rendered script)"
        try:
            # Absolutize + bash -l: PSI/J runs the payload from the job's cwd
            # and ignores the script shebang (see exaserve.schedulers.psij_backend).
            spec = JobSpec(
                executable="/bin/bash",
                arguments=["-l", str(Path(job_path).resolve())],
                name=header["job_name"],
                inherit_environment=False,
                stdout_path=Path(header["stdout_path"]),
                stderr_path=Path(header["stderr_path"]),
                resources=ResourceSpecV1(
                    node_count=int(header["num_nodes"]),
                    processes_per_node=1,
                    exclusive_node_use=bool(header.get("exclusive", True)),
                ),
                attributes=JobAttributes(
                    duration=parse_walltime(header["walltime"]),
                    queue_name=header["queue"] or None,
                    account=header["account"] or None,
                    custom_attributes=header.get("custom_attributes") or {},
                ),
            )
            job = Job(spec)
            JobExecutor.get_instance(header["executor"]).submit(job)
        except Exception as exc:  # submission errors -> defer/retry in submit_all
            return False, f"psij submit failed: {exc}"
        return True, str(job.native_id)

    # ---- queue accounting: PSI/J can't see foreign jobs; use native tools ----

    def _native(self) -> Optional[EvalScheduler]:
        detect, _ = _psij_helpers()
        executor = detect()
        if executor.startswith("pbs"):
            from .pbs import PBSScheduler
            return PBSScheduler()
        if executor == "slurm":
            from .slurm import SlurmScheduler
            return SlurmScheduler()
        return None

    def count_queued(self, user: str) -> Dict[str, int]:
        native = self._native()
        return native.count_queued(user) if native else {}

    def slot_limits(self) -> Dict[str, Optional[int]]:
        native = self._native()
        return native.slot_limits() if native else {}
