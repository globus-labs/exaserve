"""Allocation-head supervisor entry point (plan WP4/WP13, audit IMP-B01).

This is the process the site adapter `exec`s into. It owns the S01 topology:

    RuntimeSupervisor (here, one per allocation)
      └── RankLauncher            — one mpiexec/srun, the head's only child
            └── NodeSupervisor    — one per rank, owns that node's Ray child
                  └── rank 0 also owns the deployment child

What moves here from the shell: deciding the launch, owning the launch,
deciding terminal state, and producing the exit code. What stays in the shell:
site environment and preflight — the things that genuinely belong to the site.

`EXASERVE_PYTHON_RANK_LAUNCH=0` restores the shell's own `mpiexec` line for a
run-to-run comparison; the switch is registered in
`doc/hardening/MIGRATION_LOG.md` and is removed at the WP13 cutover.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Optional, Sequence

from .control.rank_launcher import RankLauncher, rank_result_check
from .control.supervisor import RuntimeSupervisor


def use_python_rank_launch() -> bool:
    return os.environ.get("EXASERVE_PYTHON_RANK_LAUNCH", "1") != "0"


def _node_count(nodefile: Optional[str]) -> int:
    if not nodefile or not os.path.exists(nodefile):
        raise SystemExit(f"[supervisor] no nodefile at {nodefile!r}; the site "
                         "adapter must export EXASERVE_NODEFILE")
    with open(nodefile, encoding="utf-8") as handle:
        nodes = {line.strip() for line in handle if line.strip()}
    if not nodes:
        raise SystemExit(f"[supervisor] nodefile {nodefile} is empty")
    return len(nodes)


def build(config_path: str, *, argv_extra: Sequence[str] = ()) -> tuple:
    """Build the supervisor and its single launcher component."""
    nodes = _node_count(os.environ.get("EXASERVE_NODEFILE"))
    scheduler = os.environ.get("EXASERVE_SCHEDULER", "pbs")
    rank_argv = [sys.executable, "-m", "exaserve.driver",
                 "--config", config_path, *argv_extra]
    launcher = RankLauncher(node_count=nodes, rank_argv=rank_argv,
                            scheduler=scheduler, env=os.environ.copy())

    supervisor = RuntimeSupervisor(poll_interval_s=2.0)
    # Handlers before any child exists: a SIGTERM during bring-up must drain,
    # not kill the head mid-launch and orphan an allocation-wide MPI job.
    supervisor.install_signal_handlers()
    component = supervisor.register(launcher.component())
    # The second, independent failure signal (WP4.4). Today no rank failure can
    # arrive except through launcher exit aggregation, so this reports no
    # failure; wiring the control-channel listener in replaces this callable
    # without touching the ownership structure.
    component.result_check = rank_result_check(lambda: None)
    return supervisor, launcher, component


def run(config_path: str, *, argv_extra: Sequence[str] = ()) -> int:
    supervisor, launcher, component = build(config_path, argv_extra=argv_extra)
    print(f"[supervisor] owning 1 rank launcher over {launcher.node_count} node(s): "
          f"{' '.join(launcher.argv())}", flush=True)
    supervisor.start_all()

    cause = supervisor.supervise(
        until=lambda: component.process is not None
        and component.process.poll() is not None)
    supervisor.shutdown(drain_s=30.0)

    launch_rc = component.process.returncode if component.process else 1
    if cause is not None:
        print(f"[supervisor] FIRST CAUSE: {cause}", flush=True)
        return supervisor.exit_code()
    if launch_rc:
        print(f"[supervisor] rank launcher exited {launch_rc}", flush=True)
    return launch_rc or 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="exaserve-supervisor")
    parser.add_argument("--config", required=True,
                        help="Path to the run-scoped runtime config YAML")
    args, extra = parser.parse_known_args(list(argv) if argv is not None
                                          else sys.argv[1:])
    return run(args.config, argv_extra=extra)


if __name__ == "__main__":
    raise SystemExit(main())
