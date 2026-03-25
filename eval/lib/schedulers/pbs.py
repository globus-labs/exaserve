from __future__ import annotations

import os

from . import __doc__  # noqa: F401


def default_queue_and_walltime(num_nodes: int) -> tuple[str, str]:
    if num_nodes <= 2:
        return "debug", "01:00:00"
    if num_nodes <= 256:
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
source "{env_script}"
python -m eval.cli run execute "{run_yaml_path}"
"""
