"""Pluggable schedulers for the eval/benchmark harness.

Parallel to the package-side ``exaserve.schedulers`` but with the harness's own
job body (cd into the repo snapshot, fix PYTHONPATH, source the env, run
``eval.cli run execute``) and its queue-throttling needs (submit_all). Selected
by ``scheduler.type`` in a spec (default ``pbs``). The cluster launch inside the
job goes through ``launch_cluster.sh``, which is already scheduler-aware, so only
the header + submit + queue-count differ here.

Design doc: doc/design/scheduler_abstraction.md
"""

from __future__ import annotations

import subprocess
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple

# Shared job-body snippet: sanitize a venv-polluted PYTHONPATH inherited from the
# submitting shell (identical to the pre-abstraction PBS body).
_PYTHONPATH_SANITIZE = """unset VIRTUAL_ENV PYTHONHOME CONDA_DEFAULT_ENV CONDA_PREFIX CONDA_PROMPT_MODIFIER _CE_CONDA _CE_M
if [ -n "$PYTHONPATH" ]; then
    CLEAN_PYTHONPATH=""
    OLD_IFS="$IFS"
    IFS=':'
    for entry in $PYTHONPATH; do
        case "$entry" in
            *"/venv/"*"/site-packages"*|*"/.venv/"*"/site-packages"*)
                continue
                ;;
        esac
        CLEAN_PYTHONPATH="${CLEAN_PYTHONPATH:+$CLEAN_PYTHONPATH:}$entry"
    done
    IFS="$OLD_IFS"
    export PYTHONPATH="$CLEAN_PYTHONPATH"
fi"""


class EvalScheduler(ABC):
    name: str = "base"
    _timeout_s: int = 600

    @abstractmethod
    def render_job(self, **kw) -> str:
        """Return the full job-script text. kwargs match the fields resolved in
        run_planner (job_name, num_nodes, queue, walltime, project, filesystems,
        keep_output, stdout_dir, stderr_dir, mail_user, mail_events, code_root,
        env_script, run_yaml_path, job_exports, gpus_per_node)."""

    @abstractmethod
    def submit(self, job_path: str) -> Tuple[bool, str]:
        """Submit; return (ok, job_id-or-error-message)."""

    @abstractmethod
    def count_queued(self, user: str) -> Dict[str, int]:
        """Per-queue/partition count of the user's running+queued jobs."""

    def slot_limits(self) -> Dict[str, Optional[int]]:
        """Max (running+queued) jobs per queue the scheduler accepts, for
        submit_all throttling. None = unlimited."""
        return {}

    # ---- shared body ---------------------------------------------------------

    def _body(self, *, code_root: str, env_script: str, run_yaml_path: str,
              job_exports: Optional[Dict[str, str]]) -> str:
        # The launcher auto-detects the scheduler from the allocation env
        # ($PBS_NODEFILE / $SLURM_JOB_NODELIST), so nothing extra is injected —
        # the PBS body stays byte-identical to the pre-abstraction template.
        export_block = "".join(f"export {k}={v}\n" for k, v in (job_exports or {}).items())
        return (
            f"cd {code_root}\n"
            f"{_PYTHONPATH_SANITIZE}\n"
            "# Prepend the snapshot repo root + its src/ so 'from eval.X' and\n"
            "# 'from exaserve.X' both resolve when eval.cli imports backends.\n"
            f'export PYTHONPATH="{code_root}:{code_root}/src${{PYTHONPATH:+:$PYTHONPATH}}"\n'
            f'source "{env_script}"\n'
            f'{export_block}python3 -m eval.cli run execute "{run_yaml_path}"\n'
        )

    def _run(self, cmd: list, check: bool = False) -> subprocess.CompletedProcess:
        return subprocess.run(
            cmd, capture_output=True, text=True, check=check, timeout=self._timeout_s
        )
