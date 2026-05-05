"""Ad-hoc serve submission helpers.

This module backs the ``aurora-serve-submit`` and ``aurora-serve-url``
console scripts (PI feedback #4). They give a casual user a two-step
workflow without going through the eval pipeline:

    $ aurora-serve-submit my_config.yaml --wait
    8470000.aurora-pbs-0001
    $ aurora-serve-url 8470000.aurora-pbs-0001
    http://x4709c1s5b0n0.hsn.cm.aurora.alcf.anl.gov:4001

Source: aurora-serve-submit reads a deployment config YAML, generates a
one-shot job.pbs that calls ``aurora-launch-cluster``, and ``qsub``s it.
The job_id is written to the path passed via ``--job-id-file`` (default:
<config-stem>.jobid in the config's directory) and printed to stdout.

aurora-serve-url polls ``qstat -f`` for the given job id and prints
``http://<head_node>:<proxy_port>`` once the job is running. With
``--wait``, it keeps polling until the job reaches state R; otherwise
it returns immediately or fails if the job hasn't been placed.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

from .config import get_site_defaults
from .schemas import load_deployment_config, load_proxy_config


_QSUB_TIMEOUT_S = 600
_QSTAT_TIMEOUT_S = 60
_JOB_ID_RE = re.compile(r"^(\d+\.[\w\-.]+)")


# ---------------------------------------------------------------------------
# aurora-serve-submit
# ---------------------------------------------------------------------------


def _render_job_pbs(
    config_path: Path,
    *,
    num_nodes: int,
    walltime: str,
    queue: str,
    project_account: str,
    filesystems: str,
    keep_flag: str,
    job_name: str,
    log_dir: Path,
) -> str:
    """Render a one-shot PBS script that calls aurora-launch-cluster <config>."""
    # We ship the launcher inside the package; ``aurora-launch-cluster``
    # is the console script that resolves the .sh via importlib.resources.
    # PYTHONPATH must include the source tree so ``aurora_rayserver`` resolves
    # at runtime; the launcher itself uses AURORA_PROJECT_ROOT to locate
    # tools/ and eval/ when running outside a source tree.
    return f"""#!/bin/bash -l
#PBS -N {job_name}
#PBS -A {project_account}
#PBS -k {keep_flag}
#PBS -l filesystems={filesystems}
#PBS -l select={num_nodes}
#PBS -l walltime={walltime}
#PBS -q {queue}
#PBS -o {log_dir}/
#PBS -e {log_dir}/

set -e
unset VIRTUAL_ENV PYTHONHOME CONDA_DEFAULT_ENV CONDA_PREFIX CONDA_PROMPT_MODIFIER _CE_CONDA _CE_M

# aurora-launch-cluster is a console script installed by the package.
# It resolves the bash launcher via importlib.resources and execs into it.
exec aurora-launch-cluster "{config_path.resolve()}"
"""


def submit_serve(
    config_path: str | os.PathLike,
    *,
    job_id_file: Optional[str | os.PathLike] = None,
    queue: Optional[str] = None,
    walltime: Optional[str] = None,
    project_account: Optional[str] = None,
    job_name: Optional[str] = None,
    log_dir: Optional[str | os.PathLike] = None,
    dry_run: bool = False,
) -> str:
    """Submit a deployment config as a one-shot PBS job.

    Returns the qsub'd job id. Writes it to ``job_id_file`` (default:
    <config>.jobid in the config's directory).

    Site values (queue, walltime, account, filesystems, keep flag) come from
    explicit kwargs first, then env vars (AURORA_DEFAULT_QUEUE etc.), then
    built-in defaults. See ``aurora_rayserver.config.get_site_defaults``.
    """
    cfg_path = Path(config_path).resolve()
    if not cfg_path.is_file():
        raise FileNotFoundError(f"deployment config not found: {cfg_path}")

    deploy_cfg = load_deployment_config(str(cfg_path))
    proxy_cfg = load_proxy_config(str(cfg_path))

    defaults = get_site_defaults()
    queue = queue or defaults.queue
    walltime = walltime or defaults.walltime
    project_account = project_account or defaults.project_account
    job_name = job_name or f"aurora-serve-{cfg_path.stem}"
    log_dir = Path(log_dir) if log_dir else cfg_path.parent / "pbs_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    job_pbs_text = _render_job_pbs(
        cfg_path,
        num_nodes=deploy_cfg.num_nodes,
        walltime=walltime,
        queue=queue,
        project_account=project_account,
        filesystems=defaults.filesystems,
        keep_flag=defaults.keep_flag,
        job_name=job_name,
        log_dir=log_dir,
    )

    job_pbs_path = log_dir / f"{cfg_path.stem}.pbs"
    job_pbs_path.write_text(job_pbs_text)

    if dry_run:
        sys.stderr.write(f"[aurora-serve-submit] dry-run; would qsub {job_pbs_path}\n")
        sys.stderr.write(f"[aurora-serve-submit] proxy port (from config): {proxy_cfg.port}\n")
        return ""

    qsub_path = shutil.which("qsub")
    if qsub_path is None:
        raise RuntimeError("qsub not found on PATH; are you on a PBS-enabled host?")

    proc = subprocess.run(
        [qsub_path, str(job_pbs_path)],
        check=True,
        text=True,
        capture_output=True,
        timeout=_QSUB_TIMEOUT_S,
    )
    job_id = proc.stdout.strip().splitlines()[-1].strip()
    if not _JOB_ID_RE.match(job_id):
        raise RuntimeError(f"could not parse job id from qsub output: {proc.stdout!r}")

    job_id_path = Path(job_id_file) if job_id_file else cfg_path.with_suffix(".jobid")
    job_id_path.write_text(job_id + "\n")
    return job_id


def _serve_submit_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="aurora-serve-submit")
    p.add_argument("config", help="Deployment config YAML")
    p.add_argument("--job-id-file", default=None, help="Write job id here (default: <config>.jobid)")
    p.add_argument("--queue", default=None, help="PBS queue (default: $AURORA_DEFAULT_QUEUE or 'debug-scaling')")
    p.add_argument("--walltime", default=None, help="PBS walltime hh:mm:ss (default: $AURORA_DEFAULT_WALLTIME or '01:00:00')")
    p.add_argument("--project-account", default=None, help="PBS account (default: $AURORA_PROJECT_ACCOUNT or 'AuroraGPT')")
    p.add_argument("--job-name", default=None)
    p.add_argument("--log-dir", default=None, help="Where to write the .pbs and PBS stdout/stderr")
    p.add_argument("--dry-run", action="store_true", help="Render the .pbs but don't qsub")
    return p


def serve_submit_main() -> int:
    args = _serve_submit_argparser().parse_args()
    job_id = submit_serve(
        args.config,
        job_id_file=args.job_id_file,
        queue=args.queue,
        walltime=args.walltime,
        project_account=args.project_account,
        job_name=args.job_name,
        log_dir=args.log_dir,
        dry_run=args.dry_run,
    )
    if job_id:
        print(job_id)
    return 0


# ---------------------------------------------------------------------------
# aurora-serve-url
# ---------------------------------------------------------------------------


def _qstat_field(job_id: str, field: str) -> Optional[str]:
    """Return the named scalar field from ``qstat -f``, or None if absent."""
    qstat_path = shutil.which("qstat")
    if qstat_path is None:
        raise RuntimeError("qstat not found on PATH")
    proc = subprocess.run(
        [qstat_path, "-f", job_id],
        check=False,
        text=True,
        capture_output=True,
        timeout=_QSTAT_TIMEOUT_S,
    )
    if proc.returncode != 0:
        # PBS stops listing finished jobs in -f without -x.
        proc = subprocess.run(
            [qstat_path, "-fx", job_id],
            check=False,
            text=True,
            capture_output=True,
            timeout=_QSTAT_TIMEOUT_S,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"qstat failed for {job_id}: {proc.stderr.strip()}")
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line.startswith(f"{field} ="):
            return line.split("=", 1)[1].strip()
    return None


def _parse_head_node(exec_host: str) -> Optional[str]:
    """exec_host looks like ``host1/0*208+host2/0*208+...`` — return host1."""
    if not exec_host:
        return None
    head = exec_host.split("+", 1)[0]
    return head.split("/", 1)[0]


def serve_url(
    job_id: str,
    config_path: Optional[str | os.PathLike] = None,
    *,
    wait: bool = False,
    poll_interval_s: float = 5.0,
    timeout_s: float = 1800.0,
    port: Optional[int] = None,
) -> str:
    """Resolve a job id to ``http://<head_node>:<port>``.

    Args:
        job_id: PBS job id (e.g. ``8470000.aurora-pbs-0001``).
        config_path: optional deployment config; we read ``proxy_config.port``
            from it to populate the port. Falls back to ``port`` kwarg, then 4001.
        wait: if True, poll until the job reaches state R.
        poll_interval_s, timeout_s: poll loop control.
        port: explicit override (skips reading the config).
    """
    if port is None and config_path is not None:
        proxy_cfg = load_proxy_config(str(config_path))
        port = proxy_cfg.port
    if port is None:
        port = 4001

    deadline = time.monotonic() + timeout_s
    while True:
        state = _qstat_field(job_id, "job_state")
        exec_host = _qstat_field(job_id, "exec_host")
        if state == "R" and exec_host:
            head = _parse_head_node(exec_host)
            if head:
                return f"http://{head}:{port}"
        if not wait:
            if state in (None, "Q", "H"):
                raise RuntimeError(
                    f"job {job_id} state={state!r}; not running yet. "
                    f"Pass --wait to poll until ready."
                )
        if time.monotonic() > deadline:
            raise TimeoutError(f"job {job_id} did not reach state R within {timeout_s}s")
        time.sleep(poll_interval_s)


def _serve_url_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="aurora-serve-url")
    p.add_argument("job_id_or_file", help="PBS job id, or path to a file containing one")
    p.add_argument("--config", default=None, help="Deployment config YAML (used to read proxy port)")
    p.add_argument("--port", type=int, default=None, help="Override proxy port")
    p.add_argument("--wait", action="store_true", help="Poll until the job is running")
    p.add_argument("--poll-interval", type=float, default=5.0)
    p.add_argument("--timeout", type=float, default=1800.0)
    return p


def _resolve_job_id(token: str) -> str:
    """token may be a literal jobid or a path to a file containing one."""
    candidate = Path(token)
    if candidate.is_file():
        return candidate.read_text().strip().splitlines()[0].strip()
    return token


def serve_url_main() -> int:
    args = _serve_url_argparser().parse_args()
    job_id = _resolve_job_id(args.job_id_or_file)
    url = serve_url(
        job_id,
        config_path=args.config,
        wait=args.wait,
        poll_interval_s=args.poll_interval,
        timeout_s=args.timeout,
        port=args.port,
    )
    print(url)
    return 0
