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

from .control.channel_runtime import HeadChannel
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


def build(config_path: str, *, argv_extra: Sequence[str] = (),
          channel: Optional[HeadChannel] = None) -> tuple:
    """Build the supervisor and its single launcher component."""
    nodes = _node_count(os.environ.get("EXASERVE_NODEFILE"))
    scheduler = os.environ.get("EXASERVE_SCHEDULER", "pbs")
    # IMP-B01: the rank entry point is the NodeSupervisor, not the legacy
    # driver. Until this line changed, NodeSupervisor had no production
    # consumer and the documented ownership tree was a design, not the
    # process tree that ran.
    rank_module = ("exaserve.driver"
                   if os.environ.get("EXASERVE_RANK_ENTRY") == "driver"
                   else "exaserve.rank_main")
    rank_argv = [sys.executable, "-m", rank_module,
                 "--config", config_path, *argv_extra]

    env = os.environ.copy()
    if channel is not None:
        # Ranks learn where to report BEFORE they are launched (plan §3.2:
        # bind an ephemeral port first, then hand the address to the ranks).
        env.update(channel.env())
    launcher = RankLauncher(node_count=nodes, rank_argv=rank_argv,
                            scheduler=scheduler, env=env)

    supervisor = RuntimeSupervisor(poll_interval_s=2.0)
    # Handlers before any child exists: a SIGTERM during bring-up must drain,
    # not kill the head mid-launch and orphan an allocation-wide MPI job.
    supervisor.install_signal_handlers()
    component = supervisor.register(launcher.component())
    # WP4.4's second, independent failure signal: a zero exit from the launcher
    # is not sufficient if a rank reported a fatal observation.
    component.result_check = rank_result_check(
        (lambda: None) if channel is None else channel.rank_failure)
    return supervisor, launcher, component


def use_control_channel() -> bool:
    return os.environ.get("EXASERVE_CONTROL_CHANNEL", "1") != "0"


def run(config_path: str, *, argv_extra: Sequence[str] = ()) -> int:
    channel = None
    if use_control_channel():
        try:
            channel = HeadChannel(
                deployment_id=os.environ.get("EXASERVE_DEPLOYMENT_ID", "unknown"),
                generation=int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
                plan_hash=os.environ.get("EXASERVE_PLAN_HASH", "plan"),
                expected_ranks=_node_count(os.environ.get("EXASERVE_NODEFILE")))
            print(f"[supervisor] control channel listening on port {channel.port} "
                  f"for {channel.expected_ranks} rank(s)", flush=True)
        except Exception as exc:
            # Launcher exit aggregation still covers rank failure, so a channel
            # that cannot bind degrades the signal rather than failing the run.
            print(f"[supervisor] WARNING: control channel unavailable "
                  f"({type(exc).__name__}: {exc}); rank failure will be detected "
                  "by launcher exit aggregation only", flush=True)
            channel = None

    supervisor, launcher, component = build(config_path, argv_extra=argv_extra,
                                            channel=channel)
    print(f"[supervisor] owning 1 rank launcher over {launcher.node_count} node(s): "
          f"{' '.join(launcher.argv())}", flush=True)
    supervisor.start_all()

    def _done() -> bool:
        if component.process is not None and component.process.poll() is not None:
            return True
        # A rank reporting a fatal observation ends the run NOW; we do not wait
        # for the launch to unwind on its own (WP4.4).
        if channel is not None and channel.rank_failure():
            supervisor.record_cause(component.component_id, "RANK_FAILURE",
                                    channel.rank_failure() or "rank failure")
            return True
        return False

    cause = supervisor.supervise(until=_done)
    if cause is None and channel is not None and channel.rank_failure():
        cause = supervisor.first_cause
    supervisor.shutdown(drain_s=30.0)
    if channel is not None:
        if channel.observations:
            print(f"[supervisor] {len(channel.observations)} rank observation(s) "
                  f"received; failures={len(channel.failures)}", flush=True)
        channel.stop()

    launch_rc = component.process.returncode if component.process else 1
    if cause is not None:
        print(f"[supervisor] FIRST CAUSE: {cause}", flush=True)
        return supervisor.exit_code() or 1
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
