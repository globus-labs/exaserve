from __future__ import annotations

from typing import Dict, Optional, Tuple

from .base import EvalScheduler


def default_queue_and_walltime(num_nodes: int) -> tuple[str, str]:
    if num_nodes <= 2:
        return "debug", "01:00:00"
    if num_nodes < 256:
        return "debug-scaling", "01:00:00"
    return "prod", "02:00:00"


class PBSScheduler(EvalScheduler):
    """PBS Pro (Aurora). VALIDATED — render output byte-identical to the prior
    render_pbs_job; qsub/qstat submit + queue count moved in from run_executor."""

    name = "pbs"

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
        gpus_per_node: Optional[int] = None,  # PBS ignores; select= is per-node
    ) -> str:
        # PR-015: validate every field that lands in a #PBS directive.
        from .base import validate_directive_field as _v

        job_name = _v(job_name, "job_name")
        queue = _v(queue, "queue")
        walltime = _v(walltime, "walltime")
        project = _v(project, "project")
        filesystems = _v(filesystems, "filesystems")
        keep_output = _v(keep_output, "keep_output")
        stdout_dir = _v(stdout_dir, "stdout_dir")
        stderr_dir = _v(stderr_dir, "stderr_dir")
        if mail_user:
            mail_user = _v(mail_user, "mail_user")
            mail_events = _v(mail_events, "mail_events")
        mail_lines = ""
        if mail_user:
            mail_lines = f"#PBS -m {mail_events}\n#PBS -M {mail_user}\n"
        header = (
            "#!/bin/bash -l\n"
            f"#PBS -N {job_name}\n"
            f"{mail_lines}"
            f"#PBS -l filesystems={filesystems}\n"
            f"#PBS -A {project}\n"
            f"#PBS -k {keep_output}\n"
            f"#PBS -l select={num_nodes}\n"
            f"#PBS -l walltime={walltime}\n"
            f"#PBS -q {queue}\n"
            f"#PBS -o {stdout_dir}/\n"
            f"#PBS -e {stderr_dir}/\n\n"
        )
        return header + self._body(
            code_root=code_root, env_script=env_script,
            run_yaml_path=run_yaml_path, job_exports=job_exports,
        )

    def submit(self, job_path: str) -> Tuple[bool, str]:
        try:
            r = self._run(["qsub", job_path])
        except Exception as exc:
            return False, f"qsub failed: {exc}"
        return r.returncode == 0, (r.stdout.strip() or r.stderr.strip())

    def count_queued(self, user: str) -> Optional[Dict[str, int]]:
        # PR-014: fail closed — an unobservable scheduler is None, never {}.
        try:
            r = self._run(["qstat", "-u", user])
        except Exception:
            return None
        if r.returncode != 0:
            return None
        counts: Dict[str, int] = {}
        for line in r.stdout.splitlines():
            if user not in line:
                continue
            parts = line.split()
            if len(parts) < 6:
                continue
            state = queue = None
            for i, part in enumerate(parts):
                if part in ("R", "Q", "H", "B", "E", "W", "S"):
                    state = part
                    if i >= 3:
                        queue = parts[2]
                    break
            if state in ("R", "Q", "H", "B") and queue:
                counts[queue] = counts.get(queue, 0) + 1
        return counts

    def slot_limits(self) -> Dict[str, Optional[int]]:
        # 1 running + 1 queued on debug/debug-scaling; prod = unlimited queued.
        return {"debug": 2, "debug-scaling": 2, "prod": None}


# Back-compat module function (thin wrapper; some callers import it directly).
def render_pbs_job(**kw) -> str:
    return PBSScheduler().render_job(**kw)
