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

WP13 removed the `EXASERVE_RANK_ENTRY=driver` fallback: two per-rank entry
points meant two ownership trees, and only one of them was the one the
documentation described.
"""

from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from typing import Optional, Sequence


def use_node_supervisor() -> bool:
    """WP13: NodeSupervisor is the only per-rank entry point."""
    return True


def _server_ready(port: int = 8000, host: str = "127.0.0.1") -> bool:
    """Liveness only. READY is the readiness gate's decision, not this."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(1.0)
        return probe.connect_ex((host, port)) == 0


def _clear_stale_ray_state(rank: int, *, timeout_s: float = 60.0) -> bool:
    """Clear a PRIOR generation's node-local Ray state before starting ours.

    Ray persists its session name on the node. A generation that was killed
    before it could stop Ray leaves that behind, and the next `ray start --head`
    dies with

        AssertionError: Session name ... does not match persisted value ...

    which reads as a Redis fault and is really an inherited-state fault. §3.2.1
    is explicit that a generation must not inherit a prior generation's state,
    and this is the node-local half of that.

    Safe by ordering, not by luck: this runs after START and before this rank
    creates ANY child, so nothing belonging to this generation exists on the
    node yet. The node is exclusively allocated, so there is no third party's
    cluster to stop.
    """
    if os.environ.get("EXASERVE_SKIP_RAY_PREFLIGHT") == "1":
        return False
    import glob
    import shutil
    import subprocess

    try:
        subprocess.run([sys.executable, "-m", "ray.scripts.scripts", "stop",
                        "--force"], timeout=timeout_s, check=False,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except (subprocess.TimeoutExpired, OSError) as exc:
        print(f"[Rank {rank}] ray preflight stop did not complete: {exc}",
              flush=True)
    # `ray stop` reaps raylets and Ray workers, but NOT the engine processes a
    # replica spawns -- and those hold device memory. A node reused after a
    # killed generation therefore comes back with its GPUs occupied, and this
    # generation dies with `torch.OutOfMemoryError: XPU out of memory`, which
    # reads as a capacity problem and is really inherited state.
    orphans = 0
    for pattern in ("EngineCore", "ServeReplica", "exaserve.server",
                    "VLLM::EngineCore"):
        try:
            found = subprocess.run(["pkill", "-9", "-f", pattern], timeout=15,
                                   check=False, stdout=subprocess.DEVNULL,
                                   stderr=subprocess.DEVNULL)
            orphans += 1 if found.returncode == 0 else 0
        except (subprocess.TimeoutExpired, OSError):
            pass

    removed = 0
    for path in glob.glob("/tmp/ray/session_*") + ["/tmp/ray/ray_current_cluster"]:
        try:
            if os.path.isdir(path):
                shutil.rmtree(path, ignore_errors=True)
            elif os.path.exists(path):
                os.unlink(path)
            removed += 1
        except OSError:
            pass
    if removed or orphans:
        print(f"[Rank {rank}] cleared {removed} stale Ray state path(s) and "
              f"{orphans} orphaned engine process group(s)", flush=True)
    return True


def _attest_node_supervisor(channel, rank: int, hostname: str) -> bool:
    """This process's own planned slot, SELF-attested and sent over the channel.

    The NodeSupervisor is the one thing here that can honestly attest to
    itself: it is the process, so pid, executable, prepared environment and
    node identity are first-hand rather than inferred.
    """
    try:
        from .compat.producers import attest_self

        receipt = attest_self(
            requirement_id=f"rank{rank}/node_supervisor",
            role="node_supervisor", component_id="node_supervisor",
            owner_scope="RANK", owner_rank=rank, node_id=hostname,
            argv=list(sys.argv))
    except Exception as exc:              # noqa: BLE001 - reported, not fatal
        print(f"[Rank {rank}] could not build the node_supervisor receipt: "
              f"{type(exc).__name__}: {exc}", flush=True)
        return False
    if not channel.submit_receipt(receipt):
        print(f"[Rank {rank}] node_supervisor receipt NOT delivered; the head "
              "will block on that slot by name", flush=True)
        return False
    return True


def _forward_receipts(channel, ingress, rank: int, *, poll_s: float = 1.0):
    """Drain the local hop and forward each payload UNCHANGED (§3.2.1).

    Unchanged is the whole contract: the supervisor is a transport for
    somebody else's attestation, not a co-author of it. Anything this thread
    rewrote would be evidence the head cannot attribute.
    """
    import threading

    def _pump() -> None:
        while True:
            for payload in ingress.drain():
                channel.submit_receipt(payload)
            if _pump_stop.wait(poll_s):
                # One final drain so a receipt produced during shutdown is not
                # lost between the last poll and the stop.
                for payload in ingress.drain():
                    channel.submit_receipt(payload)
                return

    _pump_stop = threading.Event()
    thread = threading.Thread(target=_pump, daemon=True,
                              name=f"exaserve-receipt-fwd-{rank}")
    thread.start()
    return _pump_stop


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
    # The bounded local hop and this rank's own supervisor receipt belong to
    # REGISTRATION, not to bring-up: §3.2.1 says REGISTER "does not count until
    # a bounded replacement SNAPSHOT plus supervisor receipt ... are atomically
    # accepted". Producing the receipt after START would mean the head released
    # the gate on evidence it did not yet have.
    from .compat.local_ingress import SOCKET_ENV, LocalReceiptIngress, socket_path_for

    deployment_id = os.environ.get("EXASERVE_DEPLOYMENT_ID", "unknown")
    generation = int(os.environ.get("EXASERVE_GENERATION", "0") or 0)
    socket_path = socket_path_for(deployment_id, generation)
    ingress = LocalReceiptIngress(socket_path, log=print)
    if ingress.start():
        os.environ[SOCKET_ENV] = socket_path
        print(f"[Rank {rank}] receipt ingress at {socket_path}", flush=True)
    forwarder_stop = _forward_receipts(channel, ingress, rank)
    _attest_node_supervisor(channel, rank, hostname)

    if channel.connected:
        channel.observe(f"rank{rank}", ComponentState.RUNNING.value, role="rank",
                        detail="registered")
        if channel.start_gate_available():
            started = time.monotonic()
            deadline = float(os.environ.get("EXASERVE_START_GATE_TIMEOUT_S", "600"))
            while not channel.poll_start(timeout=2.0):
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
            # No channel: the head has launcher exit aggregation and there is
            # no gate to wait for.
            print(f"[Rank {rank}] no control channel; proceeding after "
                  "registration", flush=True)

    _clear_stale_ray_state(rank)

    ray_env = get_ray_env(vendor)
    ray_env[SOCKET_ENV] = os.environ.get(SOCKET_ENV, "")
    ray_env["EXASERVE_RECEIPT_SLOT"] = (f"rank{rank}/ray_head" if rank == 0
                                        else f"rank{rank}/ray_worker")
    ray_env["EXASERVE_RECEIPT_ROLE"] = "ray_head" if rank == 0 else "ray_worker"
    ray_env["EXASERVE_RECEIPT_RANK"] = str(rank)
    for key in ("EXASERVE_PLAN_HASH", "EXASERVE_SITE_PROFILE_HASH",
                "EXASERVE_ALLOCATION_BINDING_HASH", "EXASERVE_DEPLOYMENT_ID",
                "EXASERVE_GENERATION"):
        value = os.environ.get(key)
        if value:
            ray_env[key] = value
    argv = (ray_head_argv(cluster, num_gpus) if rank == 0
            else ray_worker_argv(cluster, num_gpus))
    node.adopt(ray_component(argv, env=ray_env))

    if rank == 0:
        server_env = dict(ray_env)
        server_env["RAY_ADDRESS"] = f"{cluster.head_ip}:{cluster.port}"
        # get_ray_env builds a Ray-focused environment, so the deployment
        # identity and run directory have to be carried explicitly. Without
        # this the readiness snapshot lands in a /tmp fallback and every
        # consumer reports not-ready for a deployment that IS ready.
        for key in ("EXASERVE_RUN_LOG_DIR", "EXASERVE_RUN_LOG_ROOT",
                    "EXASERVE_DEPLOYMENT_ID", "EXASERVE_GENERATION",
                    "EXASERVE_PLAN_HASH", "EXASERVE_ALLOCATION_BINDING_HASH",
                    "EXASERVE_COMPAT_PROFILE_ID", "EXASERVE_HEAD_IP",
                    "EXASERVE_CONTROL_HOST", "EXASERVE_CONTROL_PORT",
                    "EXASERVE_CONTROL_SECRET"):
            value = os.environ.get(key)
            if value:
                server_env[key] = value
        server_env[SOCKET_ENV] = os.environ.get(SOCKET_ENV, "")
        server_env["EXASERVE_SITE_PROFILE_HASH"] = os.environ.get(
            "EXASERVE_SITE_PROFILE_HASH", "")
        # Stated explicitly, not inferred from the socket: if the hop failed to
        # bind, the child must STILL not run its own enforcing gate, or two
        # processes would each be deciding readiness and the child would block
        # on receipts that now travel a different path.
        server_env["EXASERVE_ROOT_OWNS_READINESS"] = "1"
        server_env["EXASERVE_RECEIPT_RANK"] = str(rank)
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
            forwarder_stop.set()      # forwards once more, then exits
            ingress.stop()
        except Exception:
            pass
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
    return run(args.config)


if __name__ == "__main__":
    raise SystemExit(main())
