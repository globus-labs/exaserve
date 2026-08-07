"""The allocation-head composition root (plan §3.2.1, packet P04, IMP-H03/B01).

This is the process the site adapter `exec`s into, and the only place that
turns configuration into a running deployment. The previous arrangement had two
nested supervisors around a 557-line shell that still owned staging,
distribution, Copper, logs, collection, cleanup and the final launch — so the
"Python supervisor" supervised Bash, and Bash owned the lifecycle.

Ownership here, in order:

    1. load/compile ONE plan; never reinterpret YAML again
    2. activate the compatibility profile before any Ray/engine import
    3. resolve the nodefile and atomically persist the AllocationBinding
    4. bind the control listener FAIL-CLOSED — no listener, no ranks
    5. run staging/distribution as finite supervised subprocesses with
       deadlines and validated result manifests
    6. launch ranks; hold START until every planned rank is established
    7. own the gateway as a GLOBAL component and establish the advertised
       endpoint
    8. drive plan-bound readiness, persist exactly one READY
    9. supervise; on any failure produce a typed first cause, bounded
       reverse-order cleanup, and a nonzero scheduler-visible exit

Step 4 is the one that changes behaviour most: a run nobody can observe is not
a run worth starting, so listener failure launches nothing at all.
"""

from __future__ import annotations

import os
import socket
import time
from dataclasses import dataclass
from typing import Callable, Optional

from .control.supervisor import ManagedComponent, RuntimeSupervisor


class CompositionError(RuntimeError):
    """A typed first cause from the composition root."""


@dataclass
class StagingStep:
    """A finite supervised subprocess with a validated result manifest.

    Sound native/MPI algorithms are kept as subprocesses rather than rewritten
    in Python (§3.2.1). What changes is that they are *owned*: argv vector,
    sanitized environment, bounded deadline, and a declared result that must
    exist. Zero exit without the declared result is failure.
    """

    name: str
    argv: list
    result_paths: tuple = ()
    deadline_s: float = 1800.0
    env: Optional[dict] = None

    def validate_result(self) -> tuple[bool, str]:
        missing = [p for p in self.result_paths if not os.path.exists(p)]
        if missing:
            return False, f"{self.name}: declared result(s) missing: {missing}"
        return True, f"{self.name}: results present"


class CompositionRoot:
    """Owns one deployment generation end to end."""

    def __init__(self, *, plan, generation: int, run_dir: str,
                 log: Callable[[str], None] = print) -> None:
        self.plan = plan
        self.generation = generation
        self.run_dir = run_dir
        self._log = log
        self.supervisor = RuntimeSupervisor(poll_interval_s=2.0)
        self.binding = None
        self.head_channel = None
        self.sessions = None
        self.receipts = None
        self.readiness = None
        self.gateway_component: Optional[ManagedComponent] = None
        self.first_cause: Optional[str] = None
        self._staging_done: list = []
        # Receipts must name the site profile they were produced under; without
        # it every producer would emit an empty hash and the strict validator
        # would reject the whole set for a reason that is really a plumbing gap.
        os.environ["EXASERVE_SITE_PROFILE_HASH"] = plan.site_profile_hash
        os.environ["EXASERVE_PLAN_HASH"] = plan.deployment_plan_hash
        os.environ["EXASERVE_GENERATION"] = str(generation)

    # -- 3. allocation binding --------------------------------------------
    def bind_allocation(self, nodes: list, scheduler_allocation_id: str):
        from .plan.contracts import build_allocation_binding
        from .state.atomic import atomic_write_json

        self.binding = build_allocation_binding(
            plan=self.plan, generation=self.generation,
            scheduler_allocation_id=scheduler_allocation_id, nodes=nodes)
        path = os.path.join(self.run_dir, "allocation_binding.json")
        try:
            os.makedirs(self.run_dir, exist_ok=True)
            atomic_write_json(path, {
                "schema_version": self.binding.schema_version,
                "deployment_id": self.binding.deployment_id,
                "generation": self.binding.generation,
                "deployment_plan_hash": self.binding.deployment_plan_hash,
                "site_profile_hash": self.binding.site_profile_hash,
                "scheduler_allocation_id": self.binding.scheduler_allocation_id,
                "rank_to_node": [list(x) for x in self.binding.rank_to_node],
                "allocation_binding_hash": self.binding.allocation_binding_hash,
            })
        except OSError as exc:
            raise CompositionError(
                f"could not persist the allocation binding: {exc}") from exc
        self._log(f"[Composition] binding {self.binding.allocation_binding_hash[:12]} "
                  f"for {len(nodes)} node(s), generation {self.generation}")
        return self.binding

    # -- 4. fail-closed listener ------------------------------------------
    def bind_control_listener(self, *, retries: int = 3, backoff_s: float = 1.0):
        """Bind before any rank exists. Ultimate failure launches nothing."""
        from .compat.receipt_v2 import ExactReceiptLedger
        from .control.channel_runtime import HeadChannel
        from .control.session import SessionCoordinator

        self.sessions = SessionCoordinator(plan=self.plan, binding=self.binding,
                                           log=self._log)
        # The ledger has to exist BEFORE the listener: a receipt that arrives
        # during registration would otherwise land in a list nothing
        # adjudicates, and the slot would read as missing for the whole run.
        self.receipts = ExactReceiptLedger(self.plan, self.binding)
        last: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                self.head_channel = HeadChannel(
                    deployment_id=self.plan.deployment_id,
                    generation=self.generation,
                    plan_hash=self.plan.deployment_plan_hash,
                    expected_ranks=self.plan.num_nodes,
                    sessions=self.sessions, receipts=self.receipts)
                break
            except Exception as exc:      # noqa: BLE001 - reported, then fatal
                last = exc
                self._log(f"[Composition] control listener bind attempt "
                          f"{attempt}/{retries} failed: {exc}")
                time.sleep(backoff_s * attempt)
        if self.head_channel is None:
            raise CompositionError(
                f"control listener could not bind after {retries} attempts "
                f"({last}); refusing to launch ranks. A run nobody can observe "
                "is not a run worth starting.")

        self._log(f"[Composition] control listener on port {self.head_channel.port} "
                  f"for {self.plan.num_nodes} planned rank(s)")
        return self.head_channel

    # -- 4b. the GLOBAL receipts only this process may issue ----------------
    def attest_global(self) -> int:
        """Issue the GLOBAL slots from the in-process supervisor authority.

        §3.2.1: GLOBAL receipts "are never accepted from a rank session or an
        unauthenticated IPC source" — they enter here, in-process, or not at
        all. `from_global_authority=True` is the flag that says this call site
        *is* that authority; nothing reachable over the wire can set it.
        """
        import sys as _sys

        from .compat.producers import (
            attest_self,
            attest_supervisor,
            required_patch_ids,
            resolved_not_required,
        )
        from .compat.receipt_v2 import ReceiptError

        issued = 0
        candidates = []
        for requirement in self.plan.receipt_requirements:
            if requirement.owner_scope != "GLOBAL":
                continue
            if requirement.role == "gateway":
                argv = self.gateway_component.argv if self.gateway_component else None
                pid = (self.gateway_component.process.pid
                       if self.gateway_component and self.gateway_component.process
                       else None)
                if pid is None:
                    # No attestation for a daemon that is not running: an
                    # unbacked receipt is worse than a missing one, because the
                    # ledger would then report the slot covered.
                    self._log("[Composition] gateway receipt withheld: "
                              "no running gateway process to attest")
                    continue
                candidates.append(attest_supervisor(
                    requirement_id=requirement.receipt_requirement_id,
                    role=requirement.role,
                    component_id=requirement.component_slot,
                    executable=(argv[0] if argv else ""), argv=argv, pid=pid))
            else:
                candidates.append(attest_self(
                    requirement_id=requirement.receipt_requirement_id,
                    role=requirement.role,
                    component_id=requirement.component_slot,
                    owner_scope="GLOBAL", executable=_sys.executable))

        for receipt in candidates:
            try:
                ok, detail = self.receipts.accept(
                    receipt,
                    required_patch_ids=required_patch_ids(receipt.role),
                    resolved_not_required=resolved_not_required(receipt.role),
                    from_global_authority=True)
            except ReceiptError as exc:
                self._log(f"[Composition] GLOBAL receipt "
                          f"{receipt.receipt_requirement_id} rejected: {exc}")
                continue
            if ok:
                issued += 1
            else:
                self._log(f"[Composition] GLOBAL receipt "
                          f"{receipt.receipt_requirement_id} rejected: {detail}")
        return issued

    # -- 5. staging as owned finite components -----------------------------
    def run_staging(self, steps: list) -> None:
        """Each step must exit zero AND produce its declared result."""
        import subprocess

        for step in steps:
            self._log(f"[Composition] staging: {step.name}")
            started = time.monotonic()
            try:
                completed = subprocess.run(  # noqa: S603 - argv vector, no shell
                    list(step.argv), env=step.env or os.environ.copy(),
                    timeout=step.deadline_s, check=False)
            except subprocess.TimeoutExpired as exc:
                raise CompositionError(
                    f"staging step {step.name!r} exceeded its "
                    f"{step.deadline_s}s deadline") from exc
            if completed.returncode != 0:
                raise CompositionError(
                    f"staging step {step.name!r} exited {completed.returncode}")
            ok, detail = step.validate_result()
            if not ok:
                # Zero exit without the declared result is failure (§3.2.1).
                raise CompositionError(
                    f"staging step {step.name!r} exited 0 but {detail}")
            self._staging_done.append(step.name)
            self._log(f"[Composition] staging: {step.name} ok "
                      f"({time.monotonic() - started:.1f}s)")

    def default_staging_steps(self, config_path: str, *,
                              python_exec: Optional[str] = None) -> list:
        """The staging the shell used to own, as OWNED finite components.

        The native/MPI algorithms are unchanged and still run as subprocesses
        (§3.2.1 is explicit that a sound algorithm should not be rewritten in
        Python to avoid a subprocess). What changes is ownership: argv vector,
        deadline, and a declared result that must exist for the step to count
        as successful.
        """
        import sys as _sys
        from importlib import resources

        python_exec = python_exec or _sys.executable
        resources_dir = resources.files("exaserve") / "resources"
        env = os.environ.copy()
        env["EXASERVE_RUN_LOG_DIR"] = self.run_dir

        steps: list = []
        if os.environ.get("EXASERVE_NULL_COMPUTE", "0") != "1":
            steps.append(StagingStep(
                name="model_bcast",
                argv=[python_exec, "-m", "exaserve.model_bcast",
                      "--config", config_path,
                      "--num-nodes", str(self.plan.num_nodes)],
                result_paths=(os.path.join(self.run_dir,
                                           "model_bcast_timing.json"),),
                deadline_s=float(os.environ.get(
                    "EXASERVE_STAGING_DEADLINE_S", "3600")),
                env=env))
        steps.append(StagingStep(
            name="distribute_source",
            argv=["bash", str(resources_dir / "distribute_to_nodes.sh")],
            deadline_s=float(os.environ.get("EXASERVE_DISTRIBUTE_DEADLINE_S", "1800")),
            env=env))
        return steps

    # -- 6. ranks + START gate --------------------------------------------
    def launch_ranks(self, rank_argv: list, *, scheduler: str = "pbs"):
        from .control.rank_launcher import RankLauncher

        env = os.environ.copy()
        if self.head_channel is not None:
            env.update(self.head_channel.env())
        env["EXASERVE_PLAN_HASH"] = self.plan.deployment_plan_hash
        # §3.2.1: the head IP is derived from the binding and passed to ranks.
        # The shell used to resolve it and MUTATE the runtime config to
        # communicate it, which made a config file a channel between processes.
        env.setdefault("EXASERVE_HEAD_IP", head_ip())
        # The run directory is where every durable artifact of this generation
        # lands (readiness snapshot, traces, per-node archives). The shell used
        # to export it; the root owns it now, so it must pass it on or the
        # snapshot ends up in /tmp where no consumer looks for it.
        env["EXASERVE_RUN_LOG_DIR"] = self.run_dir
        env.setdefault("EXASERVE_RUN_LOG_ROOT", os.path.dirname(self.run_dir)
                       or self.run_dir)
        env["EXASERVE_ALLOCATION_BINDING_HASH"] = self.binding.allocation_binding_hash
        env["EXASERVE_SITE_PROFILE_HASH"] = self.plan.site_profile_hash
        launcher = RankLauncher(node_count=self.plan.num_nodes,
                                rank_argv=rank_argv, scheduler=scheduler, env=env)
        component = self.supervisor.register(launcher.component())
        self.supervisor.install_signal_handlers()
        self.supervisor.start_all()
        self._log(f"[Composition] launched {self.plan.num_nodes} rank(s)")
        return component

    def await_all_registered(self, *, poll_s: float = 1.0) -> None:
        """Hold START until every planned rank is established (§3.2.1 Q4)."""
        deadline = time.monotonic() + self.plan.control.registration_deadline_s
        while time.monotonic() < deadline:
            reason = self.sessions.check_registration_deadline()
            if reason:
                raise CompositionError(reason)
            if self.sessions.all_registered():
                ok, why = self.sessions.start()
                if not ok:
                    raise CompositionError(f"START refused: {why}")
                if self.head_channel is not None:
                    released = self.head_channel.broadcast_start()
                    self._log(f"[Composition] START released to {released} session(s)")
                self._log("[Composition] START: every planned rank established")
                return
            time.sleep(poll_s)
        pending = self.sessions.readiness_revoked_ranks()
        raise CompositionError(
            f"registration deadline expired with ranks {list(pending)[:8]} "
            "not established")

    # -- 7. gateway as a GLOBAL component ---------------------------------
    def start_gateway(self, argv: list, *, env: Optional[dict] = None,
                      log_file=None) -> Optional[ManagedComponent]:
        """The allocation head owns the gateway. Rank zero must not."""
        if self.plan.gateway is None:
            return None
        component = self.supervisor.register(ManagedComponent(
            component_id=f"gateway/{self.plan.gateway.kind}",
            argv=list(argv), env=env, stdout=log_file, long_lived=True))
        component.start()
        self.gateway_component = component
        self._log(f"[Composition] gateway {self.plan.gateway.kind} started "
                  f"(pid={component.process.pid if component.process else '?'})")
        return component

    def advertised_endpoint(self, head_ip: str) -> str:
        """The one endpoint readiness verifies and canaries go through."""
        exposure = self.plan.exposure
        if self.plan.gateway is not None:
            port = self.plan.gateway.port
        else:
            port = exposure.serve_port
        return f"{exposure.advertised_scheme}://{head_ip}:{port}"

    def gateway_alive(self) -> Optional[bool]:
        if self.gateway_component is None:
            return None
        state, _ = self.gateway_component.observe()
        return state == "RUNNING"

    # -- 7b. persistent site services + result collection ------------------
    def start_copper(self, log_dir: Optional[str] = None) -> Optional[ManagedComponent]:
        """Copper as a typed supervised component (§3.2.1).

        Loading a Copper module is site setup and stays in the adapter; STARTING
        and later stopping a Copper process is lifecycle, so it belongs here as
        an owned component that shutdown reaps in reverse order.
        """
        if os.environ.get("EXASERVE_AURORA_USE_COPPER", "0") != "1":
            return None
        if self.plan.num_nodes < 2:
            self._log("[Composition] copper skipped (single-node run)")
            return None
        log_dir = log_dir or os.path.join(self.run_dir, "copper")
        os.makedirs(log_dir, exist_ok=True)
        mount = f"/tmp/{os.environ.get('USER', 'exaserve')}/copper_mount"
        component = self.supervisor.register(ManagedComponent(
            component_id="copper",
            argv=["launch_copper_aurora.sh", "-d", log_dir, "-v", mount],
            long_lived=True))
        try:
            component.start()
        except (OSError, FileNotFoundError) as exc:
            self._log(f"[Composition] copper unavailable ({exc}); continuing")
            self.supervisor.components.pop("copper", None)
            return None
        self._log(f"[Composition] copper started (logs {log_dir})")
        return component

    def collect_results(self, *, deadline_s: float = 300.0) -> bool:
        """Per-node archive collection, owned and bounded.

        The shell ran this from an EXIT trap, which meant collection outlived
        the thing that was supposed to own it and could not report a typed
        failure. Failure is reported, not fatal: losing diagnostics must not
        turn a good run into a failed one.
        """
        from importlib import resources

        per_node = os.path.join(self.run_dir, "per_node")
        os.makedirs(per_node, exist_ok=True)
        script = resources.files("exaserve") / "resources" / "gather_run_logs.sh"
        if not os.path.exists(str(script)):
            return False
        step = StagingStep(name="collect_results",
                           argv=["bash", str(script), self.run_dir, per_node],
                           deadline_s=deadline_s)
        try:
            self.run_staging([step])
            return True
        except CompositionError as exc:
            self._log(f"[Composition] result collection failed (not fatal): {exc}")
            return False

    # -- 8. readiness over the advertised endpoint -------------------------
    def build_readiness(self):
        """Plan-bound readiness bound to THIS generation's evidence."""
        from .control.plan_readiness import PlanReadiness

        self.readiness = PlanReadiness(
            plan=self.plan, binding=self.binding, receipts=self.receipts,
            sessions=self.sessions, log=self._log)
        return self.readiness

    def establish_advertised_endpoint(self, ip: Optional[str] = None) -> str:
        """§3.2.1 Q3 step 2: enter VALIDATING and fix the advertised endpoint.

        For production that means the gateway process the head owns; for an
        explicit validation plan it is the declared Serve endpoint. Either way
        readiness verifies THAT endpoint and canaries go through it.
        """
        if self.readiness is None:
            self.build_readiness()
        self.readiness.enter_validating()
        endpoint = self.advertised_endpoint(ip or head_ip())
        self.readiness.set_advertised_endpoint(endpoint)
        if self.plan.gateway is not None:
            self.readiness.set_gateway(alive=self.gateway_alive(), healthy=None)
        self._log(f"[Composition] advertised endpoint {endpoint} "
                  f"({self.plan.exposure.mode})")
        return endpoint

    def await_deployment_serving(self, *, timeout_s: float = 3600.0,
                                 poll_s: float = 5.0) -> bool:
        """Wait for the deployment child to report its applications running.

        The deployment-side snapshot is EVIDENCE, not the decision: the root
        still verifies the advertised endpoint and commits READY itself.
        """
        import json

        from .control.serve_readiness import EVIDENCE_FILENAME

        deadline = time.monotonic() + timeout_s
        path = os.path.join(self.run_dir, EVIDENCE_FILENAME)
        while time.monotonic() < deadline:
            try:
                with open(path, encoding="utf-8") as handle:
                    data = json.load(handle)
                if data.get("applications_running") or data.get("satisfied"):
                    return True
            except (OSError, ValueError):
                pass
            if self.sessions is not None and \
                    self.sessions.generation_state == "TERMINAL":
                return False
            time.sleep(poll_s)
        return False

    def gateway_argv(self, config_dir: str) -> Optional[list]:
        """Render the gateway config and return its argv, or None."""
        if self.plan.gateway is None:
            return None
        if self.plan.gateway.kind != "haproxy":
            raise CompositionError(
                f"gateway {self.plan.gateway.kind} has no supervised launcher yet")
        return ["haproxy", "-f", os.path.join(config_dir, "haproxy.cfg"), "-db"]

    def canary_advertised_endpoint(self, model, *, timeout_s: float = 60.0
                                   ) -> tuple:
        """One real completion through the COMPILED advertised endpoint."""
        import json
        import urllib.error
        import urllib.request

        route = "" if len(self.plan.models) == 1 else f"/{model.route_name}"
        url = f"{self.readiness.advertised_endpoint}{route}/v1/completions"
        body = json.dumps({"prompt": "The capital of France is",
                           "max_tokens": 4}).encode()
        request = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"})
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(request, timeout=timeout_s) as response:
                payload = json.loads(response.read().decode())
        except (urllib.error.URLError, OSError, ValueError) as exc:
            return False, f"{type(exc).__name__}: {exc}"
        choices = payload.get("choices") or []
        if not choices or "text" not in choices[0]:
            return False, f"no completion: {str(payload)[:120]}"
        return True, str(choices[0]["text"])[:60]

    def observe_gateway(self) -> None:
        """Post-READY: a dead gateway is terminal, an unhealthy one revokes."""
        if self.plan.gateway is None or self.readiness is None:
            return
        alive = self.gateway_alive()
        if alive is False:
            self.readiness.revoke("gateway process exited", gateway_dead=True)
            self.fail("gateway process exited after READY")

    # -- 9. termination ----------------------------------------------------
    def fail(self, reason: str) -> None:
        if self.first_cause is None:
            self.first_cause = reason
        self._log(f"[Composition] FIRST CAUSE: {reason}")

    def shutdown(self, *, drain_s: float = 30.0) -> None:
        """Bounded reverse-order cleanup; a cleanup error never hides the cause."""
        try:
            self.supervisor.shutdown(drain_s=drain_s)
        except Exception as exc:          # noqa: BLE001
            self._log(f"[Composition] cleanup error (first cause preserved): {exc}")
        if self.head_channel is not None:
            try:
                self.head_channel.stop()
            except Exception:             # noqa: BLE001
                pass

    def exit_code(self) -> int:
        """Typed exit. A requested shutdown is 143, not a generic failure."""
        supervised = self.supervisor.exit_code()
        if self.first_cause is not None:
            # A SIGTERM-driven shutdown is not a fault; the supervisor already
            # classifies it, and flattening it to 1 loses that distinction.
            if supervised == 143 or "SHUTDOWN_REQUESTED" in str(self.first_cause):
                return 143
            return supervised or 1
        return supervised


def head_ip() -> str:
    for env_var in ("EXASERVE_HEAD_IP", "RAY_HEAD_IP"):
        value = os.environ.get(env_var)
        if value:
            return value
    try:
        return socket.gethostbyname(
            f"{socket.gethostname()}.hsn.cm.aurora.alcf.anl.gov")
    except OSError:
        pass
    try:
        return socket.gethostbyname(socket.gethostname())
    except OSError:
        return "127.0.0.1"


def read_nodefile(path: Optional[str] = None) -> list:
    """Discover the allocation.

    The site adapter normally exports EXASERVE_NODEFILE, but a direct CLI
    launch is legitimate, so the scheduler's own variables are consulted too
    rather than demanding a wrapper that may not have run.
    """
    candidates = [path, os.environ.get("EXASERVE_NODEFILE"),
                  os.environ.get("PBS_NODEFILE")]
    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            path = candidate
            break
    else:
        nodelist = os.environ.get("SLURM_JOB_NODELIST")
        if nodelist:
            import subprocess

            out = subprocess.run(["scontrol", "show", "hostnames", nodelist],
                                 capture_output=True, text=True, check=False)
            nodes = [n.strip() for n in out.stdout.splitlines() if n.strip()]
            if nodes:
                return list(dict.fromkeys(nodes))
        raise CompositionError(
            "no nodefile found (checked EXASERVE_NODEFILE, PBS_NODEFILE, "
            "SLURM_JOB_NODELIST); cannot bind an allocation")
    with open(path, encoding="utf-8") as handle:
        nodes = [line.strip() for line in handle if line.strip()]
    unique: list = []
    for node in nodes:
        if node not in unique:
            unique.append(node)
    if not unique:
        raise CompositionError(f"nodefile {path} is empty")
    return unique
