from __future__ import annotations

import os

from . import __doc__  # noqa: F401


def default_queue_and_walltime(num_nodes: int) -> tuple[str, str]:
    if num_nodes <= 2:
        return "debug", "01:00:00"
    if num_nodes < 256:
        return "debug-scaling", "01:00:00"
    return "prod", "02:00:00"


def render_pbs_job(
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
) -> str:
    mail_lines = ""
    if mail_user:
        mail_lines = f"#PBS -m {mail_events}\n#PBS -M {mail_user}\n"

    return f"""#!/bin/bash -l
#PBS -N {job_name}
{mail_lines}#PBS -l filesystems={filesystems}
#PBS -A {project}
#PBS -k {keep_output}
#PBS -l select={num_nodes}
#PBS -l walltime={walltime}
#PBS -q {queue}
#PBS -o {stdout_dir}/
#PBS -e {stderr_dir}/

cd {code_root}
unset VIRTUAL_ENV PYTHONHOME CONDA_DEFAULT_ENV CONDA_PREFIX CONDA_PROMPT_MODIFIER _CE_CONDA _CE_M
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
        CLEAN_PYTHONPATH="${{CLEAN_PYTHONPATH:+$CLEAN_PYTHONPATH:}}$entry"
    done
    IFS="$OLD_IFS"
    export PYTHONPATH="$CLEAN_PYTHONPATH"
fi
# Prepend the snapshot repo root + its src/ subdir so 'from eval.X' and
# 'from aurora_rayserver.X' both resolve when eval.cli imports backends.
export PYTHONPATH="{code_root}:{code_root}/src${{PYTHONPATH:+:$PYTHONPATH}}"
source "{env_script}"
python3 -m eval.cli run execute "{run_yaml_path}"
"""
