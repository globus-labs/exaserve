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

import re
import shlex
import subprocess
from abc import ABC, abstractmethod
from typing import Dict, Optional, Tuple

# PR-015: scheduler directive fields (job name, account, queue, walltime,
# paths) are interpolated into #PBS/#SBATCH lines. A newline or shell
# metacharacter there injects arbitrary directives/commands. Validate them
# against a conservative allowlist and reject anything else.
_SAFE_DIRECTIVE = re.compile(r"^[A-Za-z0-9 ._:/@=+-]*$")


def validate_directive_field(value: str, field: str) -> str:
    text = str(value)
    if "\n" in text or "\r" in text:
        raise ValueError(f"scheduler field {field!r} must not contain newlines: {text!r}")
    if not _SAFE_DIRECTIVE.match(text):
        raise ValueError(
            f"scheduler field {field!r} has disallowed characters: {text!r} "
            "(allowed: alphanumerics and . _ : / @ = + - space)"
        )
    return text

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
    def count_queued(self, user: str) -> Optional[Dict[str, int]]:
        """Per-queue/partition count of the user's running+queued jobs.

        Returns ``None`` when the scheduler could not be observed (command
        failure/nonzero exit). PR-014: observation failure must be
        distinguishable from zero jobs — callers fail closed on ``None``.
        """

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
        # PR-015: shell-quote every interpolated data field. Export values are
        # quoted; keys are validated as identifiers. env_setup-style raw shell
        # is not accepted on this path.
        code_q = shlex.quote(str(code_root))
        env_q = shlex.quote(str(env_script))
        run_q = shlex.quote(str(run_yaml_path))
        export_lines = []
        for k, v in (job_exports or {}).items():
            if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", str(k)):
                raise ValueError(f"invalid export variable name: {k!r}")
            export_lines.append(f"export {k}={shlex.quote(str(v))}\n")
        export_block = "".join(export_lines)
        return (
            f"cd {code_q}\n"
            f"{_PYTHONPATH_SANITIZE}\n"
            "# Prepend the snapshot repo root + its src/ so 'from eval.X' and\n"
            "# 'from exaserve.X' both resolve when eval.cli imports backends.\n"
            f'export PYTHONPATH="{code_q}:{code_q}/src${{PYTHONPATH:+:$PYTHONPATH}}"\n'
            f"source {env_q}\n"
            f"{export_block}python3 -m eval.cli run execute {run_q}\n"
        )

    def _run(self, cmd: list, check: bool = False) -> subprocess.CompletedProcess:
        return subprocess.run(
            cmd, capture_output=True, text=True, check=check, timeout=self._timeout_s
        )
