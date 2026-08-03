"""Ad-hoc serve submission helpers.

This module backs the ``exaserve-serve-submit`` and ``exaserve-serve-url``
console scripts (PI feedback #4). They give a casual user a two-step
workflow without going through the eval pipeline:

    $ exaserve-serve-submit my_config.yaml --wait
    8470000.aurora-pbs-0001
    http://x4709c1s5b0n0.hsn.cm.aurora.alcf.anl.gov:4001
    $ exaserve-serve-url 8470000.aurora-pbs-0001
    http://x4709c1s5b0n0.hsn.cm.aurora.alcf.anl.gov:4001

Source: exaserve-serve-submit reads a deployment config YAML, generates a
one-shot job.pbs that calls ``exaserve-launch-cluster``, and ``qsub``s it.
The job_id is written to the path passed via ``--job-id-file`` (default:
<config-stem>.jobid in the config's directory) and printed to stdout.

exaserve-serve-url polls ``qstat -f`` for the given job id and prints
``http://<head_node>:<proxy_port>`` once the job is running. With
``--wait``, it keeps polling until the job reaches state R; otherwise
it returns immediately or fails if the job hasn't been placed.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from importlib import resources
from pathlib import Path
from typing import Optional

from .config import get_site_defaults
from .schedulers import JobSpec, get_scheduler
from .schemas import load_deployment_config, load_proxy_config


# Default runtime-env setup for the job script. Aurora sources env_aurora (or
# falls back to `module load frameworks`); other sites override via
# EXASERVE_ENV_SETUP (e.g. `module load <...>; source <venv>/bin/activate`).
_DEFAULT_ENV_SETUP = """if [ -f "$HOME/script/env_aurora" ]; then
    source "$HOME/script/env_aurora"
else
    module load frameworks
fi"""


# ---------------------------------------------------------------------------
# exaserve-serve-submit
# ---------------------------------------------------------------------------


def _package_paths() -> tuple[Path, Path, Path]:
    package_root = Path(str(resources.files("exaserve")))
    package_parent = package_root.parent
    launch_script = package_root / "resources" / "launch_cluster.sh"
    return package_root, package_parent, launch_script


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
    explicit kwargs first, then env vars (EXASERVE_DEFAULT_QUEUE etc.), then
    built-in defaults. See ``exaserve.config.get_site_defaults``.
    """
    cfg_path = Path(config_path).resolve()
    if not cfg_path.is_file():
        raise FileNotFoundError(f"deployment config not found: {cfg_path}")

    deploy_cfg = load_deployment_config(str(cfg_path))
    proxy_cfg = load_proxy_config(str(cfg_path))

    defaults = get_site_defaults()
    package_root, package_parent, launch_script = _package_paths()
    queue = queue or defaults.queue
    walltime = walltime or defaults.walltime
    project_account = project_account or defaults.project_account
    job_name = job_name or f"exaserve-{cfg_path.stem}"
    log_dir = Path(log_dir) if log_dir else cfg_path.parent / "pbs_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    scheduler = get_scheduler()
    env_setup = os.environ.get("EXASERVE_ENV_SETUP", _DEFAULT_ENV_SETUP)
    job_spec = JobSpec(
        config_path=cfg_path,
        launch_script=launch_script,
        package_root=package_root,
        package_parent=package_parent,
        num_nodes=deploy_cfg.num_nodes,
        walltime=walltime,
        account=project_account,
        job_name=job_name,
        log_dir=log_dir,
        queue=queue,
        env_setup=env_setup,
        vendor=os.environ.get("EXASERVE_VENDOR"),
        gpus_per_node=getattr(deploy_cfg, "num_gpus_per_node", None),
        filesystems=defaults.filesystems,   # PBS only; ignored by Slurm
        keep_flag=defaults.keep_flag,        # PBS only; ignored by Slurm
    )
    job_text = scheduler.render_job(job_spec)

    ext = {"slurm": "sbatch", "psij": "psij.sh"}.get(scheduler.name, "pbs")
    job_script_path = log_dir / f"{cfg_path.stem}.{ext}"
    job_script_path.write_text(job_text)

    if dry_run:
        sys.stderr.write(
            f"[exaserve-serve-submit] dry-run ({scheduler.name}); would submit {job_script_path}\n"
        )
        sys.stderr.write(f"[exaserve-serve-submit] proxy port (from config): {proxy_cfg.port}\n")
        return ""

    job_id = scheduler.submit(job_script_path)
    job_id_path = Path(job_id_file) if job_id_file else cfg_path.with_suffix(".jobid")
    job_id_path.write_text(job_id + "\n")
    return job_id


def _serve_submit_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="exaserve-serve-submit")
    p.add_argument("config", help="Deployment config YAML")
    p.add_argument("--job-id-file", default=None, help="Write job id here (default: <config>.jobid)")
    p.add_argument("--queue", default=None, help="PBS queue (default: $EXASERVE_DEFAULT_QUEUE or 'debug-scaling')")
    p.add_argument("--walltime", default=None, help="PBS walltime hh:mm:ss (default: $EXASERVE_DEFAULT_WALLTIME or '01:00:00')")
    p.add_argument("--project-account", default=None, help="PBS account (default: $EXASERVE_PROJECT_ACCOUNT or 'AuroraGPT')")
    p.add_argument("--job-name", default=None)
    p.add_argument("--log-dir", default=None, help="Where to write the .pbs and PBS stdout/stderr")
    p.add_argument("--dry-run", action="store_true", help="Render the .pbs but don't qsub")
    p.add_argument("--wait", action="store_true", help="After qsub, wait for the job to run and print the service URL")
    p.add_argument("--poll-interval", type=float, default=5.0)
    p.add_argument("--timeout", type=float, default=1800.0)
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
        if args.wait:
            print(
                serve_url(
                    job_id,
                    config_path=args.config,
                    wait=True,
                    poll_interval_s=args.poll_interval,
                    timeout_s=args.timeout,
                )
            )
    return 0


# ---------------------------------------------------------------------------
# exaserve-serve-url
# ---------------------------------------------------------------------------


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
        job_id: scheduler job id (PBS ``8470000.aurora-pbs-0001`` or Slurm ``12345``).
        config_path: optional deployment config; we read ``proxy_config.port``
            from it to populate the port. Falls back to ``port`` kwarg, then 4001.
        wait: if True, poll until the job reaches the running state.
        poll_interval_s, timeout_s: poll loop control.
        port: explicit override (skips reading the config).
    """
    if port is None and config_path is not None:
        proxy_cfg = load_proxy_config(str(config_path))
        port = proxy_cfg.port
    if port is None:
        port = 4001

    scheduler = get_scheduler()
    deadline = time.monotonic() + timeout_s
    while True:
        state = scheduler.job_state(job_id)
        if state == "R":
            head = scheduler.head_node(job_id)
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
    p = argparse.ArgumentParser(prog="exaserve-serve-url")
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
