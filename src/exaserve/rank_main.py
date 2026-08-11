"""Per-rank entry point (plan WP4.3, audit IMP-B01 §4.1.1).

This is the process `RankLauncher` starts on every node. Until now that was
`exaserve.driver`, which meant `NodeSupervisor` had no production consumer at
all — the documented ownership tree described a design, not the process tree
that ran. The completion-claim audit was right to call that out.

Here the tree is real:

    RuntimeSupervisor (allocation head)
      └── RankLauncher              — one mpiexec/srun
            └── NodeSupervisor      — THIS process, one per rank
                  └── ray           — the node-local Ray daemon

The allocation-head RuntimeSupervisor separately owns the isolated deployment
child, gateway, readiness, status persistence, and global failure policy.

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
import math
import os
import socket
import sys
import threading
import time
from typing import Any, Optional, Sequence


_CONTROL_CREDENTIAL_KEYS = (
    "EXASERVE_CONTROL_HOST",
    "EXASERVE_CONTROL_PORT",
    "EXASERVE_CONTROL_SECRET",
)


def _without_control_credentials(environment: dict) -> dict:
    """Return a child environment without the NodeSupervisor-only channel."""
    sanitized = dict(environment)
    for key in _CONTROL_CREDENTIAL_KEYS:
        sanitized.pop(key, None)
    return sanitized


def use_node_supervisor() -> bool:
    """WP13: NodeSupervisor is the only per-rank entry point."""
    return True


def _server_ready(port: int = 8000, host: str = "127.0.0.1") -> bool:
    """Liveness only. READY is the readiness gate's decision, not this."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.settimeout(1.0)
        return probe.connect_ex((host, port)) == 0


def _clear_stale_ray_state(rank: int, *, timeout_s: float = 60.0) -> bool:
    """Reap only prior process groups carrying a valid ownership receipt."""
    from .state.process_ownership import cleanup_stale_owned_processes

    deployment_id = os.environ.get("EXASERVE_DEPLOYMENT_ID", "")
    generation_raw = os.environ.get("EXASERVE_GENERATION", "")
    if not deployment_id or not generation_raw.isdigit():
        raise RuntimeError("stale cleanup requires deployment and generation identity")
    removed = cleanup_stale_owned_processes(
        deployment_id=deployment_id, generation=int(generation_raw), deadline_s=timeout_s
    )
    if removed:
        print(f"[Rank {rank}] reaped {removed} prior owned process group(s)", flush=True)
    return True


def _build_node_supervisor_receipt(rank: int, hostname: str):
    """Build this process's SELF attestation for the initial snapshot.

    The NodeSupervisor is the one thing here that can honestly attest to
    itself: it is the process, so pid, executable, prepared environment and
    node identity are first-hand rather than inferred.
    """
    try:
        from .compat.producers import attest_self

        receipt = attest_self(
            requirement_id=f"rank{rank}/node_supervisor",
            role="node_supervisor",
            component_id="node_supervisor",
            owner_scope="RANK",
            owner_rank=rank,
            node_id=hostname,
            argv=list(sys.argv),
        )
    except Exception as exc:  # noqa: BLE001 - reported, not fatal
        print(
            f"[Rank {rank}] could not build the node_supervisor receipt: "
            f"{type(exc).__name__}: {exc}",
            flush=True,
        )
        return None
    return receipt


class _ReceiptForwarder:
    """Owned, bounded bridge from the local receipt socket to head control."""

    def __init__(self, channel: Any, ingress: Any, rank: int, *, poll_s: float) -> None:
        if poll_s <= 0:
            raise ValueError("receipt-forwarding poll interval must be positive")
        self._channel = channel
        self._ingress = ingress
        self._rank = rank
        self._poll_s = poll_s
        self._stop = threading.Event()
        self._failure_lock = threading.Lock()
        self._failure: Optional[str] = None
        self._thread = threading.Thread(
            target=self._pump,
            daemon=True,
            name=f"exaserve-receipt-fwd-{rank}",
        )

    @property
    def failure(self) -> Optional[str]:
        with self._failure_lock:
            return self._failure

    def _fail(self, detail: str) -> None:
        with self._failure_lock:
            if self._failure is None:
                self._failure = detail
                print(f"[Rank {self._rank}] receipt forwarder failed: {detail}", flush=True)
        self._stop.set()

    def _forward_pending(self) -> bool:
        for payload in self._ingress.drain():
            # The payload is deliberately forwarded unchanged.  The
            # supervisor transports another process's attestation; it must
            # never reconstruct, re-sign, or reinterpret it.
            if not self._channel.submit_receipt(payload):
                self._fail(
                    self._channel.control_failure()
                    or "authenticated control channel rejected a receipt"
                )
                return False
        return True

    def _pump(self) -> None:
        try:
            while not self._stop.wait(self._poll_s):
                if not self._forward_pending():
                    return
            # One final drain prevents a receipt produced during child
            # shutdown from being lost between the last poll and stop().
            if self.failure is None:
                self._forward_pending()
        except Exception as exc:  # noqa: BLE001 - terminal thread boundary
            self._fail(f"{type(exc).__name__}: {exc}")

    def start(self) -> None:
        self._thread.start()

    def stop(self, *, timeout_s: float) -> bool:
        """Request the final drain, join, and report exact termination."""
        self._stop.set()
        self._thread.join(timeout=max(0.0, timeout_s))
        if self._thread.is_alive():
            self._fail(f"thread did not stop within {max(0.0, timeout_s):g}s")
            return False
        return self.failure is None


def _forward_receipts(
    channel: Any, ingress: Any, rank: int, *, poll_s: float = 1.0
) -> _ReceiptForwarder:
    """Drain the local hop and forward each payload UNCHANGED (§3.2.1).

    Unchanged is the whole contract: the supervisor is a transport for
    somebody else's attestation, not a co-author of it. Anything this thread
    rewrote would be evidence the head cannot attribute.
    """
    forwarder = _ReceiptForwarder(channel, ingress, rank, poll_s=poll_s)
    forwarder.start()
    return forwarder


def _cleanup_registration_resources(
    *,
    channel: Any,
    ingress: Any,
    rank: int,
    forwarder: Optional[_ReceiptForwarder] = None,
    timeout_s: float = 5.0,
) -> bool:
    """Close pre-child registration resources under one absolute deadline.

    These failure paths used to grant the forwarder, ingress, and control
    channel a fresh five-second timeout each.  That made a nominal five-second
    rollback last roughly fifteen seconds.  The same canonical cleanup budget
    now flows through every owner, including the control event-loop thread.
    """
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s < 0
    ):
        raise ValueError("registration cleanup timeout must be finite and nonnegative")
    deadline = time.monotonic() + float(timeout_s)

    def remaining() -> float:
        return max(0.0, deadline - time.monotonic())

    clean = True
    if forwarder is not None and not forwarder.stop(timeout_s=remaining()):
        clean = False
        print(
            f"[Rank {rank}] receipt forwarder cleanup failed: "
            f"{forwarder.failure or 'thread did not stop'}",
            flush=True,
        )
    if not ingress.stop(timeout_s=remaining()):
        clean = False
        print(f"[Rank {rank}] receipt ingress cleanup failed", flush=True)
    try:
        channel.close(expected=False, deadline=deadline)
    except (OSError, RuntimeError) as exc:
        clean = False
        print(f"[Rank {rank}] control cleanup failed: {exc}", flush=True)
    if remaining() <= 0:
        clean = False
        print(
            f"[Rank {rank}] registration cleanup exhausted its {float(timeout_s):g}s deadline",
            flush=True,
        )
    return clean


def _collect_diagnostics_finite(
    *,
    source_root: str,
    run_dir: str,
    rank: int,
    deployment_id: str,
    generation: int,
    timeout_s: float,
) -> dict:
    """Collect optional diagnostics in a killable finite subprocess."""
    from .control.finite_process import FiniteProcessError, run_finite

    if timeout_s <= 0:
        raise RuntimeError("diagnostics cleanup budget is exhausted")
    command = [
        sys.executable,
        "-m",
        "exaserve.state.diagnostics",
        "--source-root",
        source_root,
        "--run-dir",
        run_dir,
        "--rank",
        str(rank),
        "--deployment-id",
        deployment_id,
        "--generation",
        str(generation),
    ]
    child_env = _without_control_credentials(dict(os.environ))
    package_parent = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    inherited_pythonpath = child_env.get("PYTHONPATH", "")
    child_env["PYTHONPATH"] = os.pathsep.join(
        item for item in (package_parent, inherited_pythonpath) if item
    )
    try:
        result = run_finite(command, timeout_s=timeout_s, env=child_env)
    except (OSError, FiniteProcessError) as exc:
        raise RuntimeError(f"diagnostics process failed: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"diagnostics process exited {result.returncode}: {detail[:400]}")
    manifest_path = os.path.join(run_dir, "per_node", f"rank-{rank:05d}.manifest.json")
    from .state.atomic import strict_json_load_path
    from .state.diagnostics import DiagnosticsError, validate_diagnostics_manifest

    try:
        manifest = strict_json_load_path(manifest_path)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"diagnostics manifest is unreadable: {exc}") from exc
    try:
        return validate_diagnostics_manifest(
            manifest,
            run_dir=run_dir,
            expected_rank=rank,
            expected_deployment_id=deployment_id,
            expected_generation=generation,
        )
    except DiagnosticsError as exc:
        raise RuntimeError(f"diagnostics manifest is invalid: {exc}") from exc


def run(plan_path: str) -> int:
    from .control.channel_runtime import RankClient
    from .control.contracts import ComponentState
    from .control.node_supervisor import (
        NodeSupervisor,
        ray_component,
        ray_head_endpoint_probe,
        serve_proxy_probe,
    )
    from .control.ray_runtime import (
        RayClusterConfig,
        get_rank,
        ray_child_environment,
        ray_head_argv,
        ray_node_ip,
        ray_worker_argv,
    )
    from .plan.io import load_deployment_plan

    rank = get_rank()
    hostname = socket.gethostname()
    try:
        plan = load_deployment_plan(plan_path)
    except Exception as exc:  # noqa: BLE001 - startup boundary
        print(f"[Rank {rank}] canonical plan rejected: {exc}", flush=True)
        return 1
    os.environ["EXASERVE_PLAN_PATH"] = plan_path
    expected_hash = os.environ.get("EXASERVE_PLAN_HASH", "")
    if expected_hash != plan.deployment_plan_hash:
        print(
            f"[Rank {rank}] plan hash mismatch: launcher={expected_hash!r}, "
            f"artifact={plan.deployment_plan_hash!r}",
            flush=True,
        )
        return 1
    try:
        from .site import prepare_runtime_site

        prepare_runtime_site(plan)
    except Exception as exc:  # noqa: BLE001 - startup boundary
        print(f"[Rank {rank}] SiteProfile preparation failed: {exc}", flush=True)
        return 1
    head_ip = os.environ.get("EXASERVE_HEAD_IP", "").strip()
    if not head_ip:
        print(f"[Rank {rank}] EXASERVE_HEAD_IP is missing from allocation binding", flush=True)
        return 1
    cluster = RayClusterConfig(head_ip=head_ip, port=plan.ray_port, node_cpus=plan.node_cpus)
    num_gpus = plan.num_gpus_per_node

    print(
        f"[Rank {rank}] NodeSupervisor on {hostname} ({'HEAD' if rank == 0 else 'WORKER'})",
        flush=True,
    )

    channel = RankClient(rank=rank, node_id=hostname)
    if not channel.connect():
        return 1

    node = NodeSupervisor(
        deployment_id=os.environ.get("EXASERVE_DEPLOYMENT_ID", "unknown"),
        generation=int(os.environ.get("EXASERVE_GENERATION", "0") or 0),
        plan_hash=os.environ.get("EXASERVE_PLAN_HASH", "plan"),
        rank=rank,
        node_id=hostname,
        publish=lambda obs: channel.observe(
            obs.component_id,
            obs.state,
            role=obs.role,
            reason_code=obs.reason_code,
            detail=obs.detail,
        ),
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
    socket_path = socket_path_for(deployment_id, generation, owner_rank=rank)
    ingress = LocalReceiptIngress(socket_path, log=print)
    if not ingress.start():
        print(
            f"[Rank {rank}] required receipt ingress could not bind; refusing to start children",
            flush=True,
        )
        _cleanup_registration_resources(channel=channel, ingress=ingress, rank=rank)
        return 1
    os.environ[SOCKET_ENV] = socket_path
    print(f"[Rank {rank}] receipt ingress at {socket_path}", flush=True)

    supervisor_receipt = _build_node_supervisor_receipt(rank, hostname)
    if supervisor_receipt is None:
        _cleanup_registration_resources(channel=channel, ingress=ingress, rank=rank)
        return 1
    initial = channel.make_observation(
        f"rank{rank}",
        ComponentState.RUNNING.value,
        role="rank",
        detail="authenticated; awaiting START",
    )
    registration_timeout = float(channel.control_limits.get("registration_deadline_s", 0))
    if registration_timeout <= 0 or not channel.establish(
        supervisor_receipt, observations=[initial], timeout=registration_timeout
    ):
        print(
            f"[Rank {rank}] initial control snapshot was not accepted; refusing to start children",
            flush=True,
        )
        _cleanup_registration_resources(channel=channel, ingress=ingress, rank=rank)
        return 1
    forwarder_stop = _forward_receipts(channel, ingress, rank)

    if not channel.start_gate_available():
        print(f"[Rank {rank}] mandatory START gate is unavailable", flush=True)
        _cleanup_registration_resources(
            channel=channel,
            ingress=ingress,
            rank=rank,
            forwarder=forwarder_stop,
        )
        return 1
    started = time.monotonic()
    startup_timeout = float(plan.readiness.initial_deadline_s)
    start_operation = "START_HEAD" if rank == 0 else "START_WORKER"
    while not channel.poll_start(timeout=2.0, expected_operation=start_operation):
        forwarder_failure = forwarder_stop.failure
        if channel.control_failure() or forwarder_failure:
            print(
                f"[Rank {rank}] startup control protocol failed: "
                f"{channel.control_failure() or forwarder_failure}",
                flush=True,
            )
            _cleanup_registration_resources(
                channel=channel,
                ingress=ingress,
                rank=rank,
                forwarder=forwarder_stop,
            )
            return 1
        if not channel.connected or time.monotonic() - started > startup_timeout:
            print(
                f"[Rank {rank}] {start_operation} never arrived within the resolved "
                f"{startup_timeout:.0f}s startup deadline; refusing "
                "to start children",
                flush=True,
            )
            _cleanup_registration_resources(
                channel=channel,
                ingress=ingress,
                rank=rank,
                forwarder=forwarder_stop,
            )
            return 1
        time.sleep(1.0)
    print(
        f"[Rank {rank}] {start_operation} received after {time.monotonic() - started:.1f}s",
        flush=True,
    )

    try:
        _clear_stale_ray_state(rank)

        from .state.process_ownership import (
            ProcessOwnershipRegistry,
            generation_runtime_root,
            process_start_ticks,
        )
        from .control.process_guardian import guardian_argv

        # Resolve one address and bind both Ray and vLLM to it.  Resolving the
        # worker address separately in either consumer can select a different
        # interface on multi-fabric HPC nodes and make vLLM observe more IPs
        # than Ray node IDs.
        node_ip = ray_node_ip(cluster, rank)
        ray_env = _without_control_credentials(ray_child_environment(plan, node_ip=node_ip))
        # The per-deployment control secret belongs only to NodeSupervisor.  Ray,
        # Serve, engines, and deployment children receive the bounded local receipt
        # socket but never the head control endpoint or authentication secret.
        ray_env[SOCKET_ENV] = os.environ.get(SOCKET_ENV, "")
        ray_env["EXASERVE_RECEIPT_SLOT"] = (
            f"rank{rank}/ray_head" if rank == 0 else f"rank{rank}/ray_worker"
        )
        ray_env["EXASERVE_RECEIPT_ROLE"] = "ray_head" if rank == 0 else "ray_worker"
        ray_env["EXASERVE_RECEIPT_RANK"] = str(rank)
        ray_env["EXASERVE_COMPAT_ROLE"] = "ray_head" if rank == 0 else "ray_worker"
        for key in (
            "EXASERVE_PLAN_HASH",
            "EXASERVE_SITE_PROFILE_HASH",
            "EXASERVE_ALLOCATION_BINDING_HASH",
            "EXASERVE_DEPLOYMENT_ID",
            "EXASERVE_GENERATION",
            "EXASERVE_PLAN_PATH",
            "EXASERVE_ALLOCATION_BINDING_PATH",
        ):
            value = os.environ.get(key)
            if value:
                ray_env[key] = value
        runtime_root = generation_runtime_root(plan.deployment_id, generation, rank)
        ray_temp = os.path.join(runtime_root, "ray")
        ray_env["RAY_TMPDIR"] = ray_temp
        ray_argv = (
            ray_head_argv(cluster, num_gpus)
            if rank == 0
            else ray_worker_argv(cluster, num_gpus, worker_ip=node_ip)
        )
        cleanup_budget = float(channel.control_limits.get("watchdog_cleanup_deadline_s", 30.0))
        argv = guardian_argv(
            ray_argv,
            owner_pid=os.getpid(),
            owner_start_ticks=process_start_ticks(os.getpid()),
            deployment_id=plan.deployment_id,
            generation=generation,
            rank=rank,
            cleanup_deadline_s=cleanup_budget,
            receipt_wait_s=min(30.0, cleanup_budget),
        )
        ownership = ProcessOwnershipRegistry(
            deployment_id=plan.deployment_id, generation=generation, rank=rank
        )
        ray = ray_component(argv, env=ray_env)
        ray.on_started = lambda identity: ownership.record(
            "ray", pid=identity["pid"], pgid=identity["pgid"], argv=argv, temp_paths=(runtime_root,)
        )
        node.adopt(ray)
        if rank == 0:
            node.add_probe(ray_head_endpoint_probe(plan.ray_port, host=cluster.head_ip))
        serve_port = (
            plan.gateway.backend_port if plan.gateway is not None else plan.exposure.serve_port
        )
        # HeadOnly intentionally has no worker-node proxy. Requiring its local
        # health probe on every rank would make the benchmark topology
        # permanently unready.
        if rank == 0 or not plan.uses_head_only_serve_proxy():
            node.add_probe(serve_proxy_probe(serve_port))
    except Exception as exc:  # no child exists yet; close every registration resource
        node.record_cause("startup", "PREFLIGHT_FAILED", str(exc))
        print(f"[Rank {rank}] pre-child startup failed: {exc}", flush=True)
        _cleanup_registration_resources(
            channel=channel,
            ingress=ingress,
            rank=rank,
            forwarder=forwarder_stop,
        )
        return 1

    cleanup_clean = False
    try:
        node.start_all(
            rollback_s=float(channel.control_limits.get("watchdog_cleanup_deadline_s", 30.0))
        )

        def control_requires_stop() -> bool:
            if channel.shutdown_requested():
                return True
            failure = channel.control_failure() or forwarder_stop.failure
            if failure:
                node.record_cause("control", "CONTROL_LEASE_LOST", failure)
                return True
            return False

        # Every owned child is polled every pass, including the Ray daemon --
        # the legacy loop watched Serve and the gateway but never the Ray head.
        cause = node.supervise(until=control_requires_stop)
        if cause is not None:
            print(f"[Rank {rank}] FIRST CAUSE: {cause}", flush=True)
    except Exception as exc:  # startup transaction boundary
        node.record_cause("startup", "START_FAILED", str(exc))
        print(f"[Rank {rank}] startup transaction failed: {exc}", flush=True)
    finally:
        cleanup_budget = float(channel.control_limits.get("watchdog_cleanup_deadline_s", 30.0))
        cleanup_deadline = time.monotonic() + cleanup_budget

        def remaining() -> float:
            return max(0.0, cleanup_deadline - time.monotonic())

        cleanup_clean = node.shutdown(
            deadline=cleanup_deadline,
            publish_observations=False,
        )
        if cleanup_clean:
            try:
                diagnostics_budget = min(10.0, remaining() / 4.0)
                diagnostics = _collect_diagnostics_finite(
                    source_root=runtime_root,
                    run_dir=os.environ["EXASERVE_RUN_LOG_DIR"],
                    rank=rank,
                    deployment_id=plan.deployment_id,
                    generation=generation,
                    timeout_s=diagnostics_budget,
                )
                if remaining() > 0:
                    channel.observe(
                        "node_diagnostics",
                        ComponentState.READY.value,
                        role="diagnostics",
                        detail=(
                            f"files={diagnostics['file_count']} "
                            f"bytes={diagnostics['source_bytes']} "
                            f"skipped={diagnostics['skipped_files']}"
                        ),
                        timeout=remaining(),
                    )
            except (KeyError, OSError, RuntimeError, ValueError) as exc:
                # Diagnostics are explicitly optional. Loss is observable but
                # does not change serving correctness or overwrite first cause.
                print(f"[Rank {rank}] optional diagnostics failed: {exc}", flush=True)
                try:
                    if remaining() > 0:
                        channel.observe(
                            "node_diagnostics",
                            ComponentState.FAILED.value,
                            role="diagnostics",
                            reason_code="DIAGNOSTICS_FAILED",
                            detail=str(exc)[:400],
                            timeout=remaining(),
                        )
                except (OSError, RuntimeError) as report_exc:
                    print(
                        f"[Rank {rank}] diagnostics failure observation also failed: {report_exc}",
                        flush=True,
                    )
            try:
                ownership.release("ray")
            except RuntimeError as exc:
                cleanup_clean = False
                node.record_cause("ray", "OWNERSHIP_RELEASE_FAILED", str(exc))
                print(f"[Rank {rank}] ownership release failed: {exc}", flush=True)
        try:
            if not forwarder_stop.stop(timeout_s=remaining()):
                cleanup_clean = False
                node.record_cause(
                    "receipt_forwarder",
                    "CLEANUP_INCOMPLETE",
                    forwarder_stop.failure or "receipt forwarder thread did not stop",
                )
            if not ingress.stop(timeout_s=remaining()):
                cleanup_clean = False
                node.record_cause(
                    "receipt_ingress",
                    "CLEANUP_INCOMPLETE",
                    "receipt ingress thread did not stop",
                )
        except OSError as exc:
            cleanup_clean = False
            node.record_cause("receipt_ingress", "CLEANUP_INCOMPLETE", str(exc))
            print(f"[Rank {rank}] IPC cleanup error: {exc}", flush=True)
        try:
            if remaining() > 0:
                channel.observe(
                    f"rank{rank}",
                    (
                        ComponentState.STOPPED.value
                        if cleanup_clean
                        else ComponentState.FAILED.value
                    ),
                    role="rank",
                    reason_code=(None if cleanup_clean else "CLEANUP_INCOMPLETE"),
                    timeout=remaining(),
                )
            channel.close(
                expected=channel.shutdown_requested(),
                deadline=cleanup_deadline,
            )
        except (OSError, RuntimeError) as exc:
            cleanup_clean = False
            node.record_cause("control", "CLEANUP_INCOMPLETE", str(exc))
            print(f"[Rank {rank}] control close error: {exc}", flush=True)
        if remaining() <= 0:
            cleanup_clean = False
            node.record_cause(
                f"rank{rank}",
                "CLEANUP_DEADLINE_EXHAUSTED",
                f"node cleanup exceeded {cleanup_budget:g}s",
            )

    code = node.exit_code()
    print(f"[Rank {rank}] exit {code}", flush=True)
    return code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(prog="exaserve-rank")
    parser.add_argument("--plan", required=True, help="Verified canonical DeploymentPlan artifact")
    args = parser.parse_args(list(argv) if argv is not None else sys.argv[1:])
    return run(args.plan)


if __name__ == "__main__":
    raise SystemExit(main())
