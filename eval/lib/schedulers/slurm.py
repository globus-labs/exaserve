from __future__ import annotations

import re
from typing import Dict, Optional, Tuple

from .base import EvalScheduler

_SUBMIT_RE = re.compile(r"Submitted batch job (\d+)")
# squeue -t state codes counted as occupying a slot (running or waiting)
_ACTIVE = {"R", "PD", "CF", "CG", "RD", "RQ", "RS", "S", "RF"}


def _map_mail(pbs_events: str) -> str:
    m = []
    if "b" in pbs_events:
        m.append("BEGIN")
    if "e" in pbs_events:
        m.append("END")
    if "a" in pbs_events:
        m.append("FAIL")
    return ",".join(m) or "NONE"


class SlurmScheduler(EvalScheduler):
    """Slurm (e.g. NCSA Delta). UNTESTED — from standard Slurm conventions."""

    name = "slurm"

    def render_job(
        self,
        *,
        job_name: str,
        num_nodes: int,
        queue: str,
        walltime: str,
        project: str,
        filesystems: str,       # PBS-only; ignored
        keep_output: str,       # PBS-only; ignored
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
        # PR-015: validate every field that lands in an #SBATCH directive.
        from .base import validate_directive_field as _v

        job_name = _v(job_name, "job_name")
        project = _v(project, "project")
        walltime = _v(walltime, "walltime")
        stdout_dir = _v(stdout_dir, "stdout_dir")
        stderr_dir = _v(stderr_dir, "stderr_dir")
        if queue:
            queue = _v(queue, "queue")
        if mail_user:
            mail_user = _v(mail_user, "mail_user")
        part = f"#SBATCH --partition={queue}\n" if queue else ""
        gpn = f"#SBATCH --gpus-per-node={gpus_per_node}\n" if gpus_per_node else ""
        mail = (
            f"#SBATCH --mail-user={mail_user}\n#SBATCH --mail-type={_map_mail(mail_events)}\n"
            if mail_user
            else ""
        )
        header = (
            "#!/bin/bash -l\n"
            f"#SBATCH --job-name={job_name}\n"
            f"#SBATCH --account={project}\n"
            f"{part}"
            f"#SBATCH --nodes={num_nodes}\n"
            "#SBATCH --ntasks-per-node=1\n"
            f"{gpn}"
            f"#SBATCH --time={walltime}\n"
            f"#SBATCH --output={stdout_dir}/%x-%j.out\n"
            f"#SBATCH --error={stderr_dir}/%x-%j.err\n"
            f"{mail}\n"
        )
        return header + self._body(
            code_root=code_root, env_script=env_script,
            run_yaml_path=run_yaml_path, job_exports=job_exports,
        )

    def submit(self, job_path: str) -> Tuple[bool, str]:
        try:
            r = self._run(["sbatch", job_path])
        except Exception as exc:
            return False, f"sbatch failed: {exc}"
        if r.returncode != 0:
            return False, (r.stderr.strip() or r.stdout.strip())
        m = _SUBMIT_RE.search(r.stdout)
        return True, (m.group(1) if m else r.stdout.strip())

    def count_queued(self, user: str) -> Optional[Dict[str, int]]:
        # PR-014: fail closed — an unobservable scheduler is None, never {}.
        try:
            r = self._run(["squeue", "-h", "-u", user, "-o", "%P %t"])
        except Exception:
            return None
        if r.returncode != 0:
            return None
        counts: Dict[str, int] = {}
        for line in r.stdout.splitlines():
            parts = line.split()
            if len(parts) < 2:
                continue
            partition, state = parts[0], parts[1]
            if state in _ACTIVE:
                counts[partition] = counts.get(partition, 0) + 1
        return counts

    def slot_limits(self) -> Dict[str, Optional[int]]:
        # Slurm per-partition MaxSubmitJobs is site-defined; default unlimited and
        # let submit_all fall back to its conservative default per partition.
        return {}
