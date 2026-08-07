"""Per-rank entry point (plan WP4.3, audit IMP-B01 §4.1.1).

This is the process `RankLauncher` starts on every node. Until now that was
`exaserve.driver`, which meant `NodeSupervisor` had no production consumer at
all — the documented ownership tree described a design, not the process tree
that ran. The completion-claim audit was right to call that out.

Here the tree is real:

    RuntimeSupervisor (allocation head)
      └── RankLauncher              — one mpiexec/srun
            └── NodeSupervisor      — THIS process, one per rank
                  ├── ray           — the node-local Ray daemon
                  └── deployment    — rank 0 only

`NodeSupervisor` creates every child it owns (it refuses to adopt a PID it did
not create), so no process here manages anything remote, and an unexpected exit
of any owned child is fatal for this rank **regardless of exit status** — which
closes the "a long-lived Ray worker exiting zero becomes a successful rank"
hole the audit identified.

`EXASERVE_RANK_ENTRY=driver` restores the legacy driver for a run-to-run
comparison. Removed at the WP13 cutover.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from typing import Optional, Sequence


def use_node_supervisor() -> bool:
    return os.environ.get("EXASERVE_RANK_ENTRY", "node_supervisor") != "driver"


def _server_ready(port: int = 8000, host: str = "127.0.0.1") -> bool:
    """Liveness only. READY is the readiness gate's decision, not this."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(1.0)
        return probe.connect_ex((host, port)) == 0


def run(config_path: str) -> int:
    from .control.channel_runtime import RankClient
    from .control.contracts import ComponentState
    from .control.node_supervisor import (
        NodeSupervisor,
        deployment_component,
        ray_component,
    )
    from .driver import (
        get_rank,
        get_ray_env,
        load_ray_cluster_config,
        ray_head_argv,
        ray_worker_argv,
        resolve_num_gpus,
        resolve_vendor,
        server_argv,
    )

    rank = get_rank()
    hostname = socket.gethostname()
    cluster = load_ray_cluster_config(config_path)
    vendor = resolve_vendor()
    num_gpus = resolve_num_gpus(config_path)

    print(f"[Rank {rank}] NodeSupervisor on {hostname} "
          f"({'HEAD' if rank == 0 else 'WORKER'})", flush=True)

    channel = RankClient(rank=rank, node_id=hostname)
    channel.connect()

    node = NodeSupervisor(
        deployment_id=os.environ.get("EXASERVE_DEPLOYMENT_ID", "unknown"),
        generation=int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
        plan_hash=os.environ.get("EXASERVE_PLAN_HASH", "plan"),
        rank=rank, node_id=hostname,
        publish=lambda obs: channel.observe(
            obs.component_id, obs.state, role=obs.role,
            reason_code=obs.reason_code, detail=obs.detail),
    )
    # Handlers before any child exists.
    node.install_signal_handlers()

    # §3.2.1 Q4 phase 2: a rank may run enough of NodeSupervisor to REGISTER
    # and publish its snapshot + supervisor receipt, but it must NOT start Ray
    # or any other long-lived child until the head has accepted every planned
    # rank and sent START. Starting Ray first is what made registration
    # decorative.
    if channel.connected:
        channel.observe(f"rank{rank}", ComponentState.RUNNING.value, role="rank",
                        detail="registered")
        if channel.start_gate_available():
            started = time.monotonic()
            deadline = float(os.environ.get("EXASERVE_START_GATE_TIMEOUT_S", "600"))
            while not channel.start_received():
                if time.monotonic() - started > deadline:
                    print(f"[Rank {rank}] START never arrived within "
                          f"{deadline:.0f}s; refusing to start children",
                          flush=True)
                    channel.close()
                    return 1
                time.sleep(1.0)
            print(f"[Rank {rank}] START received after "
                  f"{time.monotonic() - started:.1f}s", flush=True)
        else:
            # KNOWN GAP, stated rather than hidden: COMMAND/COMMAND_RESULT
            # dispatch is not implemented in the transport, so the head cannot
            # deliver START over the wire. The gate exists and is unit-tested
            # in control/session.py; until dispatch lands, a rank proceeds
            # after registration. Blocking here instead would hang every run
            # on a message that cannot arrive.
            print(f"[Rank {rank}] START gate not deliverable (COMMAND dispatch "
                  "unimplemented); proceeding after registration", flush=True)

    ray_env = get_ray_env(vendor)
    argv = (ray_head_argv(cluster, num_gpus) if rank == 0
            else ray_worker_argv(cluster, num_gpus))
    node.adopt(ray_component(argv, env=ray_env))

    if rank == 0:
        server_env = dict(ray_env)
        server_env["RAY_ADDRESS"] = f"{cluster.head_ip}:{cluster.port}"
        node.adopt(deployment_component(server_argv(config_path), env=server_env))

    node.start_all()
    try:
        # Every owned child is polled every pass, including the Ray daemon --
        # the legacy loop watched Serve and the gateway but never the Ray head.
        cause = node.supervise()
        if cause is not None:
            print(f"[Rank {rank}] FIRST CAUSE: {cause}", flush=True)
    finally:
        node.shutdown(drain_s=30.0)
        try:
            channel.observe(f"rank{rank}", ComponentState.STOPPED.value, role="rank")
            channel.close()
        except Exception:
            pass

    code = node.exit_code()
    print(f"[Rank {rank}] exit {code}", flush=True)
    return code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="exaserve-rank")
    parser.add_argument("--config", required=True)
    args, _ = parser.parse_known_args(list(argv) if argv is not None
                                      else sys.argv[1:])
    if not use_node_supervisor():
        from .driver import main as _driver_main

        return _driver_main() or 0
    return run(args.config)


if __name__ == "__main__":
    raise SystemExit(main())
