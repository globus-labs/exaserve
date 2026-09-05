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

import math
import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import Callable, Optional

from .exception_notes import add_exception_note
from .control.supervisor import (
    BoundedOutputCapture,
    FirstCause,
    ManagedComponent,
    RuntimeSupervisor,
)


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
    cwd: Optional[str] = None
    result_validator: Optional[Callable[[], tuple[bool, str]]] = None

    def validate_result(self) -> tuple[bool, str]:
        missing = [p for p in self.result_paths if not os.path.exists(p)]
        if missing:
            return False, f"{self.name}: declared result(s) missing: {missing}"
        if self.result_validator is not None:
            return self.result_validator()
        return True, f"{self.name}: results present"


class CompositionRoot:
    """Owns one deployment generation end to end."""

    def __init__(
        self,
        *,
        plan,
        generation: int,
        run_dir: str,
        log: Callable[[str], None] = print,
        production_qualified: bool = False,
        site_profile=None,
    ) -> None:
        if not isinstance(production_qualified, bool):
            raise TypeError("production_qualified must be a bool")
        if production_qualified and plan.validation_mode:
            raise ValueError("a validation-mode plan cannot be production-qualified")
        self.plan = plan
        self.generation = generation
        self.run_dir = run_dir
        self._log = log
        self.production_qualified = production_qualified
        self.site_profile = site_profile
        self.supervisor = RuntimeSupervisor(poll_interval_s=2.0)
        self.binding = None
        self.binding_path = ""
        self.binding_store = None
        self.run_provenance = None
        self.head_channel = None
        self.sessions = None
        self.receipts = None
        self.readiness = None
        self.gateway_component: Optional[ManagedComponent] = None
        self.deployment_component: Optional[ManagedComponent] = None
        self._deployment_ingress = None
        self._deployment_observer = None
        self._gateway_port_lease = None
        self._gateway_listener = None
        self._gateway_environment: Optional[dict] = None
        self._gateway_capture: Optional[BoundedOutputCapture] = None
        self._last_gateway_health = None
        self._head_address: Optional[str] = None
        self.first_cause: Optional[str] = None
        self._staging_done: list = []
        self.local_runtime_paths = None
        self.status = None  # the shared DeploymentStatus writer
        self._validation_loss_started: Optional[float] = None
        self._last_live_validation = 0.0
        # Receipts must name the site profile they were produced under; without
        # it every producer would emit an empty hash and the strict validator
        # would reject the whole set for a reason that is really a plumbing gap.
        os.environ["EXASERVE_DEPLOYMENT_ID"] = plan.deployment_id
        os.environ["EXASERVE_SITE_PROFILE_HASH"] = plan.site_profile_hash
        os.environ["EXASERVE_PLAN_HASH"] = plan.deployment_plan_hash
        os.environ["EXASERVE_GENERATION"] = str(generation)
        from .plan.runtime_environment import runtime_environment

        # Legacy native helpers still read environment variables, but the root
        # overwrites them from the hash-bearing plan before any helper starts.
        # Ambient submission-shell values therefore cannot alter this run.
        os.environ.update(runtime_environment(plan))

    # -- 3. allocation binding --------------------------------------------
    def bind_allocation(self, nodes: list, scheduler_allocation_id: str):
        from .plan.contracts import build_allocation_binding
        from .plan.io import write_allocation_binding

        self.binding = build_allocation_binding(
            plan=self.plan,
            generation=self.generation,
            scheduler_allocation_id=scheduler_allocation_id,
            nodes=nodes,
        )
        path = os.path.join(self.run_dir, "allocation_binding.json")
        try:
            from .state.atomic import ensure_owned_directory

            ensure_owned_directory(self.run_dir)
            write_allocation_binding(path, self.binding)
            self.binding_path = path
        except OSError as exc:
            raise CompositionError(f"could not persist the allocation binding: {exc}") from exc
        self._log(
            f"[Composition] binding {self.binding.allocation_binding_hash[:12]} "
            f"for {len(nodes)} node(s), generation {self.generation}"
        )
        # The root's OWN receipts are built from the environment, like every
        # other producer's — so the binding hash has to be in this process's
        # environment, not only in the one it hands to ranks. Without this the
        # root's GLOBAL receipt carries an empty allocation_binding_hash and the
        # strict validator rejects it, blocking readiness on `global/supervisor`.
        os.environ["EXASERVE_ALLOCATION_BINDING_HASH"] = self.binding.allocation_binding_hash

        # Persist execution provenance only after the allocation identity is
        # known. It is separate from both semantic hashes by construction.
        run_id = os.environ.get("EXASERVE_RUN_ID", "").strip()
        if run_id:
            from datetime import datetime, timezone

            from .compat.producers import prepared_environment_hash
            from .plan.contracts import RunProvenance
            from .plan.io import (
                load_run_plan,
                write_run_provenance,
            )

            run_plan_path = os.environ.get("EXASERVE_RUN_PLAN_PATH", "").strip()
            source_hash = os.environ.get("EXASERVE_SOURCE_SNAPSHOT_HASH", "").strip()
            declared_run_hash = os.environ.get("EXASERVE_RUN_SEMANTIC_HASH", "").strip()
            if not run_plan_path or not source_hash or not declared_run_hash:
                raise CompositionError(
                    "eval execution provenance is incomplete: run plan, source "
                    "snapshot hash, and run semantic hash are required together"
                )
            semantic_run = load_run_plan(run_plan_path)
            if semantic_run.run_semantic_hash != declared_run_hash:
                raise CompositionError("runtime run semantic hash disagrees with RunPlan artifact")
            if semantic_run.deployment.deployment_plan_hash != self.plan.deployment_plan_hash:
                raise CompositionError("runtime RunPlan embeds a different DeploymentPlan")
            output_locations = tuple(
                item
                for item in os.environ.get("EXASERVE_OUTPUT_LOCATIONS", self.run_dir).split(
                    os.pathsep
                )
                if item
            )
            self.run_provenance = RunProvenance(
                schema_version=self.plan.schema_version,
                run_id=run_id,
                deployment_id=self.plan.deployment_id,
                generation=self.generation,
                deployment_plan_hash=self.plan.deployment_plan_hash,
                allocation_binding_hash=self.binding.allocation_binding_hash,
                run_semantic_hash=semantic_run.run_semantic_hash,
                source_snapshot_hash=source_hash,
                resolved_input_paths=(
                    os.path.abspath(run_plan_path),
                    os.path.abspath(self.binding_path),
                ),
                argv=tuple(str(item) for item in sys.argv),
                prepared_environment_hash=prepared_environment_hash(),
                started_at=datetime.now(timezone.utc).isoformat(),
                output_locations=output_locations or (self.run_dir,),
            ).finalize()
            write_run_provenance(
                os.path.join(self.run_dir, "run_provenance.json"), self.run_provenance
            )

        # The shared status record opens here, once the generation has a real
        # identity to publish. Eval and ClientLab read THIS, not a log line.
        from .status_api import DeploymentStatusPublisher

        self.status = DeploymentStatusPublisher(
            self.run_dir,
            plan=self.plan,
            binding=self.binding,
            generation=self.generation,
            run_provenance=self.run_provenance,
            log=self._log,
        )
        self.status.initialize()
        from .state.bindings import ComponentBindingStore

        self.binding_store = ComponentBindingStore(
            self.run_dir,
            plan=self.plan,
            binding=self.binding,
            current_publish_batch_size=64,
        )
        return self.binding

    # -- 4. fail-closed listener ------------------------------------------
    def bind_control_listener(self, *, retries: int = 3, backoff_s: float = 1.0):
        """Bind before any rank exists. Ultimate failure launches nothing."""
        from .compat.receipt_v2 import ExactReceiptLedger
        from .control.channel_runtime import HeadChannel
        from .control.session import SessionCoordinator

        self.sessions = SessionCoordinator(plan=self.plan, binding=self.binding, log=self._log)
        # The ledger has to exist BEFORE the listener: a receipt that arrives
        # during registration would otherwise land in a list nothing
        # adjudicates, and the slot would read as missing for the whole run.
        self.receipts = ExactReceiptLedger(
            self.plan, self.binding, binding_store=self.binding_store
        )
        last: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            self.raise_if_termination("control-listener setup interrupted")
            try:
                self.head_channel = HeadChannel(
                    deployment_id=self.plan.deployment_id,
                    generation=self.generation,
                    plan_hash=self.plan.deployment_plan_hash,
                    expected_ranks=self.plan.num_nodes,
                    sessions=self.sessions,
                    receipts=self.receipts,
                )
                break
            except Exception as exc:  # noqa: BLE001 - reported, then fatal
                last = exc
                self._log(
                    f"[Composition] control listener bind attempt {attempt}/{retries} failed: {exc}"
                )
                self.raise_if_termination("control-listener retry interrupted")
                time.sleep(backoff_s * attempt)
        if self.head_channel is None:
            raise CompositionError(
                f"control listener could not bind after {retries} attempts "
                f"({last}); refusing to launch ranks. A run nobody can observe "
                "is not a run worth starting."
            )

        self._log(
            f"[Composition] control listener on port {self.head_channel.port} "
            f"for {self.plan.num_nodes} planned rank(s)"
        )
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
        )
        from .compat.receipt_v2 import ReceiptError

        issued = 0
        candidates = []
        for requirement in self.plan.receipt_requirements:
            if requirement.owner_scope != "GLOBAL":
                continue
            if requirement.role == "gateway":
                argv = self.gateway_component.argv if self.gateway_component else None
                pid = (
                    self.gateway_component.process.pid
                    if self.gateway_component and self.gateway_component.process
                    else None
                )
                if pid is None:
                    # No attestation for a daemon that is not running: an
                    # unbacked receipt is worse than a missing one, because the
                    # ledger would then report the slot covered.
                    self._log(
                        "[Composition] gateway receipt withheld: "
                        "no running gateway process to attest"
                    )
                    continue
                candidates.append(
                    attest_supervisor(
                        requirement_id=requirement.receipt_requirement_id,
                        role=requirement.role,
                        component_id=requirement.component_slot,
                        executable=(argv[0] if argv else ""),
                        argv=argv,
                        pid=pid,
                    )
                )
            else:
                candidates.append(
                    attest_self(
                        requirement_id=requirement.receipt_requirement_id,
                        role=requirement.role,
                        component_id=requirement.component_slot,
                        owner_scope="GLOBAL",
                        executable=_sys.executable,
                    )
                )

        for receipt in candidates:
            try:
                ok, detail = self.receipts.accept(
                    receipt,
                    from_global_authority=True,
                )
            except ReceiptError as exc:
                self._log(
                    f"[Composition] GLOBAL receipt {receipt.receipt_requirement_id} rejected: {exc}"
                )
                continue
            if ok:
                issued += 1
            else:
                self._log(
                    f"[Composition] GLOBAL receipt "
                    f"{receipt.receipt_requirement_id} rejected: {detail}"
                )
        return issued

    def raise_if_termination(self, context: str, *, poll_components: bool = True) -> None:
        """Abort a synchronous phase when supervision already has a cause.

        The main deployment thread performs bounded waits before it enters
        ``RuntimeSupervisor.supervise()``. Signal handlers and the authenticated
        control listener remain live during those waits, so each wait must
        consume their typed cause instead of continuing toward READY. Control
        failures are assigned here before raising, which preserves first-cause
        identity and phase-aware cleanup when the failure removed a rank session.
        """
        if not isinstance(context, str) or not context:
            raise ValueError("termination context must be non-empty text")
        if not isinstance(poll_components, bool):
            raise TypeError("poll_components must be a bool")
        if poll_components:
            self.supervisor.poll_once()
        if self.termination_requested():
            raise CompositionError(f"{context}: {self.supervisor.first_cause}")

    def termination_requested(self) -> bool:
        """Poll non-process failure sources for interruptible boundaries."""
        if self.head_channel is not None:
            channel_failure = self.head_channel.poll()
            if channel_failure:
                self.supervisor.record_cause("control", "CONTROL_FAILURE", channel_failure)
        return self.supervisor.first_cause is not None

    # -- 5. staging as owned finite components -----------------------------
    def run_staging(self, steps: list) -> None:
        """Each step must exit zero AND produce its declared result."""
        from .state.status import DeploymentState

        if self.status is not None:
            self.status.advance(DeploymentState.STAGING, reason_code="STAGING")
        for step in steps:
            self._log(f"[Composition] staging: {step.name}")
            before = {
                path: (os.stat(path).st_ino, os.stat(path).st_mtime_ns, os.stat(path).st_size)
                for path in step.result_paths
                if os.path.exists(path)
            }
            started = time.monotonic()
            component_id = f"staging/{step.name}"
            step_env = step.env or os.environ.copy()
            step_cwd = step.cwd
            if step.name != "distribute_source" and self.local_runtime_paths is not None:
                # Later staging phases execute local code and launch local
                # verifier children, while this explicitly head-owned process
                # retains the one shared result directory it publishes into.
                from .plan.runtime_environment import closed_runtime_environment

                closed_utility_env = closed_runtime_environment(
                    self.plan,
                    paths=self.local_runtime_paths,
                    base_environment=step_env,
                    policy=self.site_profile or self.plan,
                )
                # The utility itself is allocation-head-owned and needs the
                # live PBS/PALS state to create its nested MPI transaction.
                # mpi_launch_prefix separately exports NONE plus its explicit
                # local application environment to remote ranks.
                head_utility_env = dict(step_env)
                head_utility_env.update(closed_utility_env)
                step_env = head_utility_env
                step_env["EXASERVE_RUN_LOG_DIR"] = self.run_dir
                step_env["EXASERVE_COMPAT_ROLE"] = "utility"
                step_cwd = str(self.local_runtime_paths.python_root)
            component = self.supervisor.register(
                ManagedComponent(
                    component_id=component_id,
                    argv=list(step.argv),
                    env=step_env,
                    cwd=step_cwd,
                    long_lived=False,
                )
            )
            returncode = None
            boundary_error: Optional[BaseException] = None
            rollback_budget = min(30.0, self.plan.control.watchdog_cleanup_deadline_s)
            startup_rollback_deadline = time.monotonic() + rollback_budget
            started_cleanly = False
            try:
                component.start(rollback_deadline=startup_rollback_deadline)
                started_cleanly = True
                step_deadline = time.monotonic() + step.deadline_s
                while True:
                    # This loop owns the finite component's exit semantics: a
                    # nonzero result must remain the precise ``exited N``
                    # staging error below.  Poll pre-existing signal/control
                    # causes here, but observe this component locally before
                    # the general supervisor can classify its finite exit as
                    # an unexpected long-lived-process failure.
                    self.raise_if_termination(
                        f"staging step {step.name!r} interrupted",
                        poll_components=False,
                    )
                    state, returncode = component.observe()
                    if state != "RUNNING":
                        break
                    remaining = step_deadline - time.monotonic()
                    if remaining <= 0:
                        raise CompositionError(
                            f"staging step {step.name!r} exceeded its {step.deadline_s}s deadline"
                        )
                    time.sleep(min(0.1, remaining))
            except Exception as exc:
                if isinstance(exc, CompositionError):
                    boundary_error = exc
                else:
                    boundary_error = CompositionError(
                        f"staging step {step.name!r} failed at its process boundary: {exc}"
                    )
                    boundary_error.__cause__ = exc
            except BaseException as exc:
                # Preserve cancellation/system-exit identity while still
                # unwinding the owned process group in ``finally``.
                boundary_error = exc
            finally:
                cleanup_deadline = (
                    time.monotonic() + rollback_budget
                    if started_cleanly
                    else startup_rollback_deadline
                )
                try:
                    component.stop("finite staging completion", deadline=cleanup_deadline)
                except BaseException as exc:
                    if boundary_error is None:
                        boundary_error = CompositionError(
                            f"staging step {step.name!r} left a process group: {exc}"
                        )
                        boundary_error.__cause__ = exc
                    else:
                        add_exception_note(
                            boundary_error,
                            f"staging process cleanup also failed: {type(exc).__name__}: {exc}",
                        )
                self.supervisor.components.pop(component_id, None)
            if boundary_error is not None:
                raise boundary_error
            if returncode != 0:
                raise CompositionError(f"staging step {step.name!r} exited {returncode}")
            unchanged = [
                path
                for path, signature in before.items()
                if os.path.exists(path)
                and (os.stat(path).st_ino, os.stat(path).st_mtime_ns, os.stat(path).st_size)
                == signature
            ]
            if unchanged:
                raise CompositionError(
                    f"staging step {step.name!r} exited 0 but did not publish "
                    f"a fresh result: {unchanged}"
                )
            ok, detail = step.validate_result()
            if not ok:
                # Zero exit without the declared result is failure (§3.2.1).
                raise CompositionError(f"staging step {step.name!r} exited 0 but {detail}")
            self._staging_done.append(step.name)
            if step.name == "distribute_source":
                self.activate_runtime_capsule(step.result_paths[0])
            self._log(f"[Composition] staging: {step.name} ok ({time.monotonic() - started:.1f}s)")

    def activate_runtime_capsule(self, manifest_path: str) -> None:
        """Bind children to the already-validated node-local capsule layout."""

        from .plan.io import (
            load_allocation_binding,
            load_deployment_plan,
            load_site_profile,
        )
        from .plan.runtime_environment import (
            COMPAT_SOURCE_MANIFEST_ENV,
            COMPAT_SOURCE_PROFILE_ENV,
            LOCAL_RUNTIME_ROOT_ENV,
            LOCAL_STATE_ROOT_ENV,
            QUALIFIED_PYTHON_ENV,
            QUALIFIED_PYTHON_HASH_ENV,
            QUALIFIED_PYTHON_PROFILE_ENV,
            RuntimePathError,
        )
        from .source_staging import SourceStagingError, runtime_paths_from_result
        from .state.atomic import strict_json_load_path

        try:
            manifest = strict_json_load_path(manifest_path)
            paths = runtime_paths_from_result(manifest)
            qualified = os.environ.get(QUALIFIED_PYTHON_ENV, "")
            if not qualified or os.path.realpath(manifest["qualified_python"]) != os.path.realpath(
                qualified
            ):
                raise RuntimePathError(
                    "capsule qualified Python disagrees with the SiteProfile-qualified executable"
                )
            paths.verify_capsule(policy=self.site_profile or self.plan)
            paths.prepare_state(policy=self.site_profile or self.plan)
            local_plan = load_deployment_plan(paths.plan_path)
            local_site = load_site_profile(paths.site_profile_path)
            local_binding = load_allocation_binding(paths.binding_path)
            if local_plan.deployment_plan_hash != self.plan.deployment_plan_hash:
                raise RuntimePathError("capsule DeploymentPlan identity mismatch")
            if local_site.site_profile_hash != self.plan.site_profile_hash:
                raise RuntimePathError("capsule SiteProfile identity mismatch")
            if (
                self.binding is None
                or local_binding.allocation_binding_hash != self.binding.allocation_binding_hash
            ):
                raise RuntimePathError("capsule AllocationBinding identity mismatch")
        except (OSError, TypeError, ValueError, RuntimePathError, SourceStagingError) as exc:
            raise CompositionError(f"runtime capsule rejected: {exc}") from exc
        self.local_runtime_paths = paths
        os.environ[LOCAL_RUNTIME_ROOT_ENV] = str(paths.root)
        os.environ[LOCAL_STATE_ROOT_ENV] = str(paths.state_root)
        os.environ[QUALIFIED_PYTHON_HASH_ENV] = manifest["qualified_python_sha256"]
        os.environ[QUALIFIED_PYTHON_PROFILE_ENV] = self.plan.site_profile_hash
        os.environ[COMPAT_SOURCE_PROFILE_ENV] = manifest["compatibility_profile_id"]
        os.environ[COMPAT_SOURCE_MANIFEST_ENV] = manifest["compatibility_manifest_hash"]
        self._log(f"[Composition] node-local runtime capsule {paths.root}")

    def default_staging_steps(self, plan_path: str, *, python_exec: Optional[str] = None) -> list:
        """The staging the shell used to own, as OWNED finite components.

        The native/MPI algorithms are unchanged and still run as subprocesses
        (§3.2.1 is explicit that a sound algorithm should not be rewritten in
        Python to avoid a subprocess). What changes is ownership: argv vector,
        deadline, and a declared result that must exist for the step to count
        as successful.
        """
        import sys as _sys

        python_exec = python_exec or _sys.executable
        if self.binding is None or not self.binding_path:
            raise CompositionError("source staging requires a persisted AllocationBinding")
        clean_env = os.environ.copy()
        from .plan.runtime_environment import runtime_environment

        clean_env.update(runtime_environment(self.plan))
        clean_env["EXASERVE_RUN_LOG_DIR"] = self.run_dir
        # The transaction must boot from the clean shared package, never a
        # stale node-local tree or the removed hand-built Ray overlay.
        python_path = [
            entry
            for entry in clean_env.get("PYTHONPATH", "").split(os.pathsep)
            if entry and entry not in {"/tmp/exaserve_src", "/tmp/exaserve_overlay"}
        ]
        clean_env["PYTHONPATH"] = os.pathsep.join(python_path)
        source_result = os.path.join(self.run_dir, "source_staging_manifest.json")
        steps: list = [
            StagingStep(
                name="distribute_source",
                argv=[
                    python_exec,
                    "-m",
                    "exaserve.source_staging",
                    "--plan",
                    plan_path,
                    "--binding",
                    self.binding_path,
                    "--result",
                    source_result,
                    "--operation-timeout-s",
                    "1800",
                ],
                result_paths=(source_result,),
                result_validator=lambda: self._validate_staging_manifest(
                    source_result, kind="source"
                ),
                deadline_s=1900.0,
                env=clean_env,
            )
        ]
        if not self.plan.runtime.null_compute:
            steps.append(
                StagingStep(
                    name="model_bcast",
                    argv=[
                        python_exec,
                        "-m",
                        "exaserve.model_bcast",
                        "--plan",
                        plan_path,
                        "--binding",
                        self.binding_path,
                        "--num-nodes",
                        str(self.plan.num_nodes),
                    ],
                    result_paths=(os.path.join(self.run_dir, "model_bcast_timing.json"),),
                    result_validator=lambda: self._validate_staging_manifest(
                        os.path.join(self.run_dir, "model_bcast_timing.json"), kind="model"
                    ),
                    deadline_s=3600.0,
                )
            )
        return steps

    def _validate_staging_manifest(self, path: str, *, kind: str) -> tuple[bool, str]:
        """Validate result identity and every planned rank's stage receipt."""
        from .state.atomic import strict_json_load_path

        try:
            data = strict_json_load_path(path)
        except (OSError, ValueError) as exc:
            return False, f"{kind} staging manifest unreadable: {exc}"
        if not isinstance(data, dict):
            return False, f"{kind} staging manifest must be an object"
        identity = {
            "deployment_id": self.plan.deployment_id,
            "generation": self.generation,
            "deployment_plan_hash": self.plan.deployment_plan_hash,
            "site_profile_hash": self.plan.site_profile_hash,
            "allocation_binding_hash": self.binding.allocation_binding_hash,
        }
        mismatches = {
            key: (value, data.get(key)) for key, value in identity.items() if data.get(key) != value
        }
        if mismatches:
            return False, f"{kind} staging manifest identity mismatch: {mismatches}"
        if kind == "source":
            from .source_staging import SourceStagingError, validate_source_staging_result

            try:
                validate_source_staging_result(
                    data,
                    expected_deployment_id=self.plan.deployment_id,
                    expected_generation=self.generation,
                    expected_plan_hash=self.plan.deployment_plan_hash,
                    expected_site_profile_hash=self.plan.site_profile_hash,
                    expected_binding_hash=self.binding.allocation_binding_hash,
                    expected_rank_to_node=self.binding.rank_to_node,
                    expected_run_dir=self.run_dir,
                )
            except SourceStagingError as exc:
                return False, str(exc)
        elif kind == "model":
            from .model_bcast import validate_model_bcast_result

            try:
                validate_model_bcast_result(data, plan=self.plan, binding=self.binding)
            except RuntimeError as exc:
                return False, str(exc)
        else:
            return False, f"unknown staging manifest kind {kind!r}"
        return True, f"{kind} staging manifest identity and receipts verified"

    # -- 6. ranks + START gate --------------------------------------------
    def head_address(self) -> str:
        """Resolve the allocation-bound rank-zero address once, fail closed."""
        if self._head_address is not None:
            return self._head_address
        if self.binding is None:
            raise CompositionError("head address cannot be resolved before allocation binding")
        from .site import resolve_allocation_node_address

        try:
            self._head_address = resolve_allocation_node_address(
                self.binding.node_for(0) or "", site_id=self.plan.site_profile_id
            )
        except RuntimeError as exc:
            raise CompositionError(str(exc)) from exc
        return self._head_address

    def launch_ranks(
        self,
        rank_argv: list,
        *,
        scheduler: str = "pbs",
        launch_prefix: Optional[list[str]] = None,
    ):
        from .control.rank_launcher import RankLauncher, rank_result_check

        if self.local_runtime_paths is None:
            raise CompositionError("rank launch requires a validated node-local runtime capsule")
        from .plan.runtime_environment import (
            RuntimePathError,
            assert_worker_launch_is_local,
            closed_runtime_environment,
        )

        self.local_runtime_paths.prepare_state(policy=self.site_profile or self.plan)
        env = closed_runtime_environment(
            self.plan,
            paths=self.local_runtime_paths,
            base_environment=os.environ,
            policy=self.site_profile or self.plan,
        )
        env["EXASERVE_COMPAT_ROLE"] = "node_supervisor"
        if self.head_channel is not None:
            env.update(self.head_channel.env(reachable_host=self.head_address()))
        env["EXASERVE_PLAN_HASH"] = self.plan.deployment_plan_hash
        # §3.2.1: the head IP is derived from the binding and passed to ranks.
        # The shell used to resolve it and MUTATE the runtime config to
        # communicate it, which made a config file a channel between processes.
        env["EXASERVE_HEAD_IP"] = self.head_address()
        env["EXASERVE_ALLOCATION_BINDING_HASH"] = self.binding.allocation_binding_hash
        env["EXASERVE_SITE_PROFILE_HASH"] = self.plan.site_profile_hash
        env["EXASERVE_VENDOR"] = self.plan.vendor
        env["EXASERVE_NUM_GPUS_PER_NODE"] = str(self.plan.num_gpus_per_node)
        rank_argv = list(rank_argv)
        try:
            assert_worker_launch_is_local(
                argv=rank_argv,
                cwd=self.local_runtime_paths.python_root,
                environment=env,
                policy=self.site_profile or self.plan,
            )
        except RuntimePathError as exc:
            raise CompositionError(f"rank launch descriptor rejected: {exc}") from exc
        launcher_env = os.environ.copy()
        launcher_env.update(env)
        launcher = RankLauncher(
            node_count=self.plan.num_nodes,
            rank_argv=rank_argv,
            scheduler=scheduler,
            launch_prefix=launch_prefix,
            env=launcher_env,
            application_env=env,
            cwd=str(self.local_runtime_paths.python_root),
        )
        component = self.supervisor.register(launcher.component())
        # Launcher aggregation and authenticated rank observations are the two
        # independent WP4.4 failure signals. A finite zero launcher exit is
        # valid only when the control plane has no rank failure; absence of the
        # mandatory head channel must fail closed rather than bless zero.
        component.result_check = rank_result_check(
            (
                self.head_channel.rank_failure
                if self.head_channel is not None
                else lambda: "mandatory rank control channel is absent"
            )
        )
        if self.head_channel is not None:
            # MPI/PALS exit is only coarse aggregation.  Before assigning the
            # global first cause, preserve the rank-local fatal observation (or
            # an authenticated unexpected disconnect) delivered by WP4.4's
            # independent control signal.
            component.on_unexpected_exit = self.head_channel.launcher_exit_evidence
        if self.sessions is None:
            raise CompositionError("rank launcher requires a session coordinator")
        # Arm at the last parent-only boundary before Popen: a fast MPI child
        # must never reach REGISTER before the registration clock exists.
        # Potentially multi-hour staging before this point consumes no budget;
        # a failed launch attempt terminates the generation immediately.
        component.on_starting = self.sessions.begin_registration
        self.supervisor.install_signal_handlers()
        self.supervisor.start_all(rollback_s=self.plan.control.watchdog_cleanup_deadline_s)
        self._log(f"[Composition] launched {self.plan.num_nodes} rank(s)")
        if self.status is not None:
            from .state.status import DeploymentState

            # A run that skipped staging (null-compute) has never left PLANNED,
            # so the walk starts from wherever it actually is.
            states = [DeploymentState.CLUSTER_STARTING, DeploymentState.DEPLOYING]
            if self.status.state == DeploymentState.PLANNED.value:
                states.insert(0, DeploymentState.STAGING)
            self.status.advance_through(*states, reason_code="RANKS_LAUNCHED")
        return component

    def await_all_registered(self, *, poll_s: float = 1.0) -> None:
        """Hold every Ray child until every planned rank is established."""
        while True:
            if self.head_channel is None:
                raise CompositionError("mandatory control listener is absent")
            self.raise_if_termination("rank registration interrupted")
            reason = self.sessions.check_registration_deadline()
            if reason:
                raise CompositionError(reason)
            if self.sessions.all_registered():
                # Snapshot ACKs establish the in-memory control barrier. Cross
                # the head's group-commit barrier before START so a crash can
                # never leave long-lived children running from receipts that
                # were accepted but not durably recorded.
                self.head_channel.flush_durable_evidence(
                    timeout=float(self.plan.control.registration_deadline_s)
                )
                self.raise_if_termination("durable rank registration interrupted")
                self._log(
                    "[Composition] every planned rank durably established; "
                    "Ray children remain fenced"
                )
                return
            try:
                remaining = self.sessions.registration_remaining_s()
            except RuntimeError as exc:
                raise CompositionError(str(exc)) from exc
            # check_registration_deadline owns the exact >= boundary and the
            # generation-fatal transition. Never bypass it with a local loop
            # condition that leaves the coordinator REGISTERING.
            if remaining <= 0:
                reason = self.sessions.check_registration_deadline()
                if reason:
                    raise CompositionError(reason)
                raise CompositionError("registration deadline expired without a terminal reason")
            time.sleep(min(poll_s, remaining))

    def _await_rank_roles(
        self, expected: dict[int, str], *, timeout_s: float, poll_s: float
    ) -> None:
        """Wait for fresh typed RUNNING/READY observations from exact ranks."""
        if self.head_channel is None:
            raise CompositionError("mandatory control listener is absent")
        deadline = time.monotonic() + timeout_s
        missing = [f"rank {rank}/{role}" for rank, role in expected.items()]
        while time.monotonic() < deadline:
            self.raise_if_termination("Ray startup interrupted")
            missing = []
            for rank, role in expected.items():
                observations = self.head_channel.current_observations(
                    rank=rank,
                    role=role,
                    max_age_s=float(self.plan.readiness.observation_freshness_s),
                )
                acceptable = "READY" if role == "ray_head_endpoint" else "RUNNING"
                if not any(obs.state == acceptable for obs in observations):
                    missing.append(f"rank {rank}/{role}/{acceptable}")
            if not missing:
                return
            time.sleep(poll_s)
        raise CompositionError(f"Ray startup deadline expired awaiting {missing[:8]}")

    def start_ray_cluster(self, plan_path: str, *, poll_s: Optional[float] = None) -> dict:
        """Start head, then workers, then prove exact Ray membership/resources.

        The proof is a finite, killable process because Ray's blocking driver
        connection has no cancellation surface. It communicates only through a
        versioned atomic artifact; stdout remains diagnostic.
        """
        if self.head_channel is None or self.binding is None:
            raise CompositionError("Ray startup requires control and allocation bindings")
        if self.local_runtime_paths is None:
            raise CompositionError("Ray startup requires a validated local runtime capsule")
        poll_s = (
            float(self.plan.readiness.validation_interval_s) if poll_s is None else float(poll_s)
        )
        if poll_s <= 0:
            raise ValueError("Ray startup poll interval must be positive")
        deadline = time.monotonic() + float(self.plan.readiness.initial_deadline_s)

        self.raise_if_termination("Ray head start interrupted")
        remaining = max(0.1, deadline - time.monotonic())
        if not self.head_channel.start_head(
            timeout=remaining,
            cancel_requested=self.termination_requested,
        ):
            raise CompositionError(self.head_channel.rank_failure() or "START_HEAD failed")
        self.raise_if_termination("Ray head start interrupted")
        self._log("[Composition] START_HEAD acknowledged by rank 0")
        remaining = max(0.1, deadline - time.monotonic())
        self._await_rank_roles({0: "ray"}, timeout_s=remaining, poll_s=poll_s)
        remaining = max(0.1, deadline - time.monotonic())
        self._await_rank_roles({0: "ray_head_endpoint"}, timeout_s=remaining, poll_s=poll_s)
        self._log("[Composition] typed Ray head endpoint is available")

        remaining = max(0.1, deadline - time.monotonic())
        workers = self.head_channel.start_workers(
            timeout=remaining,
            cancel_requested=self.termination_requested,
        )
        expected_workers = self.plan.num_nodes - 1
        if workers != expected_workers:
            raise CompositionError(
                self.head_channel.rank_failure()
                or f"START_WORKER reached {workers}/{expected_workers} workers"
            )
        self.raise_if_termination("Ray worker start interrupted")
        self._log(f"[Composition] START_WORKER acknowledged by {workers} worker(s)")
        remaining = max(0.1, deadline - time.monotonic())
        self._await_rank_roles(
            {rank: "ray" for rank in self.binding.ranks()}, timeout_s=remaining, poll_s=poll_s
        )

        from .control.finite_process import (
            FiniteProcessError,
            FiniteProcessTimeout,
            run_finite,
        )
        from .control.ray_cluster_probe import (
            RayClusterProbeError,
            load_probe_snapshot,
        )

        output_path = os.path.join(self.run_dir, "ray_cluster.snapshot.json")
        attempt_path = os.path.join(
            self.local_runtime_paths.logs,
            f".ray_cluster.probe.{os.getpid()}.{time.time_ns()}.json",
        )
        from .plan.runtime_environment import closed_runtime_environment

        env = closed_runtime_environment(
            self.plan,
            paths=self.local_runtime_paths,
            base_environment=os.environ,
            policy=self.site_profile or self.plan,
        )
        for key in (
            "EXASERVE_CONTROL_HOST",
            "EXASERVE_CONTROL_PORT",
            "EXASERVE_CONTROL_SECRET",
        ):
            env.pop(key, None)
        remaining = max(0.1, deadline - time.monotonic())
        argv = [
            env["EXASERVE_QUALIFIED_PYTHON"],
            "-m",
            "exaserve.control.ray_cluster_probe",
            "--plan",
            os.path.abspath(plan_path),
            "--binding",
            str(self.local_runtime_paths.binding_path),
            "--address",
            f"{self.head_address()}:{self.plan.ray_port}",
            "--output",
            os.path.abspath(attempt_path),
            "--timeout",
            str(remaining),
            "--poll",
            str(poll_s),
        ]
        try:
            completed = run_finite(
                argv,
                timeout_s=remaining + 15.0,
                env=env,
                cwd=self.local_runtime_paths.python_root,
                termination_grace_s=min(10.0, max(1.0, remaining / 10)),
                cancel_requested=self.termination_requested,
            )
            self.raise_if_termination("Ray membership probe interrupted")
            if completed.returncode not in {0, 3}:
                detail = (completed.stderr or completed.stdout)[-1000:]
                raise CompositionError(
                    f"typed Ray membership probe exited {completed.returncode}: {detail}"
                )
            snapshot = load_probe_snapshot(attempt_path, plan=self.plan, binding=self.binding)
        except (FiniteProcessError, FiniteProcessTimeout, RayClusterProbeError) as exc:
            raise CompositionError(f"typed Ray membership probe failed: {exc}") from exc
        from .state.atomic import atomic_create_or_verify_json

        atomic_create_or_verify_json(output_path, snapshot)
        try:
            os.unlink(attempt_path)
        except FileNotFoundError:
            pass
        if not snapshot["ready"]:
            raise CompositionError(
                f"Ray membership/resources did not converge: {snapshot['blockers'][:8]}"
            )
        if completed.returncode != 0:
            raise CompositionError("Ray membership probe returned a contradictory exit status")
        self._log(
            f"[Composition] exact Ray membership/resources verified for "
            f"{len(snapshot['nodes'])} node(s)"
        )
        return snapshot

    # -- 6b. globally owned isolated deployment fallback -----------------
    def start_deployment(self, plan_path: str, *, env: Optional[dict] = None) -> ManagedComponent:
        """Start the one permitted deployment child under OUTER ownership.

        The child is a fault-isolation boundary only.  It owns no global
        readiness or status decision, and rank zero never creates or manages
        its PID.  Evidence returns over a versioned local Unix protocol whose
        kernel peer PID must equal this exact component instance.
        """
        if self.binding is None:
            raise CompositionError("deployment start requires AllocationBinding")
        if self.deployment_component is not None:
            raise CompositionError("deployment component is already started")

        from .compat.local_ingress import (
            SOCKET_ENV as RECEIPT_SOCKET_ENV,
            socket_path_for as receipt_socket_path_for,
        )
        from .control.deployment_ipc import (
            SOCKET_ENV as DEPLOYMENT_SOCKET_ENV,
            DeploymentObservationIngress,
            socket_path_for as deployment_socket_path_for,
        )
        from .control.deployment_observer import DeploymentObserver
        from .control.ray_runtime import server_argv
        from .plan.runtime_environment import closed_runtime_environment

        if self.local_runtime_paths is None:
            raise CompositionError("deployment start requires a validated local runtime capsule")

        socket_path = deployment_socket_path_for(self.plan.deployment_id, self.generation)
        ingress = DeploymentObservationIngress(socket_path, log=self._log)
        if not ingress.start():
            raise CompositionError("required deployment observation IPC could not bind")

        child_env = closed_runtime_environment(
            self.plan,
            paths=self.local_runtime_paths,
            base_environment=(os.environ if env is None else env),
            policy=self.site_profile or self.plan,
        )
        for key in (
            "EXASERVE_CONTROL_HOST",
            "EXASERVE_CONTROL_PORT",
            "EXASERVE_CONTROL_SECRET",
        ):
            child_env.pop(key, None)
        child_env.update(
            {
                "RAY_ADDRESS": f"{self.head_address()}:{self.plan.ray_port}",
                "EXASERVE_DEPLOYMENT_ID": self.plan.deployment_id,
                "EXASERVE_GENERATION": str(self.generation),
                "EXASERVE_PLAN_HASH": self.plan.deployment_plan_hash,
                "EXASERVE_SITE_PROFILE_HASH": self.plan.site_profile_hash,
                "EXASERVE_ALLOCATION_BINDING_HASH": (self.binding.allocation_binding_hash),
                "EXASERVE_PLAN_PATH": str(self.local_runtime_paths.plan_path),
                "EXASERVE_ALLOCATION_BINDING_PATH": str(self.local_runtime_paths.binding_path),
                # This child is explicitly allocation-head-owned and is the
                # sole producer of the paper startup trace. Actors do not
                # inherit this key; their runtime_env has only node-local
                # paths. Keeping this one head writer preserves the durable
                # result contract without worker filesystem fan-out.
                "EXASERVE_RUN_LOG_DIR": self.run_dir,
                # Rank zero owns this node-local exact-receipt hop.  The path is
                # deterministic and the receiving rank validates every receipt.
                RECEIPT_SOCKET_ENV: receipt_socket_path_for(
                    self.plan.deployment_id, self.generation, owner_rank=0
                ),
                DEPLOYMENT_SOCKET_ENV: socket_path,
                "EXASERVE_ROOT_OWNS_READINESS": "1",
                "EXASERVE_RECEIPT_RANK": "0",
                "EXASERVE_COMPAT_ROLE": "deployment",
            }
        )
        if not self.plan.runtime.null_compute:
            model_result = os.path.join(self.run_dir, "model_bcast_timing.json")
            if not os.path.isfile(model_result):
                ingress.stop(timeout_s=1.0)
                raise CompositionError(
                    "deployment start requires the validated model staging result"
                )
            # This shared path is consumed once by the allocation-head-owned
            # deployment driver. build_actor_runtime_env deliberately omits it;
            # replicas receive only the validated node-local path mapping.
            child_env["EXASERVE_MODEL_BCAST_RESULT"] = model_result
        deployment_argv = server_argv(str(self.local_runtime_paths.plan_path))
        component = self.supervisor.register(
            ManagedComponent(
                component_id="deployment",
                argv=deployment_argv,
                env=child_env,
                cwd=str(self.local_runtime_paths.python_root),
                long_lived=True,
            )
        )
        observer = None
        rollback_deadline = time.monotonic() + min(
            30.0, self.plan.control.watchdog_cleanup_deadline_s
        )

        def rollback_remaining() -> float:
            return max(0.0, rollback_deadline - time.monotonic())

        try:
            component.start(rollback_deadline=rollback_deadline)
            observer = DeploymentObserver(
                ingress=ingress,
                owned_pid=lambda: (
                    component.process.pid if component.process is not None else None
                ),
                plan=self.plan,
                binding=self.binding,
                poll_s=min(0.5, self.plan.readiness.validation_interval_s),
                log=self._log,
            )
            observer.start()
        except BaseException as exc:
            if observer is not None:
                try:
                    if not observer.stop(timeout_s=rollback_remaining()):
                        add_exception_note(
                            exc, "deployment observer rollback exceeded its deadline"
                        )
                except Exception as cleanup_exc:
                    add_exception_note(exc, f"deployment observer rollback failed: {cleanup_exc}")
            try:
                component.stop("deployment startup rollback", deadline=rollback_deadline)
            except BaseException as cleanup_exc:
                add_exception_note(exc, f"deployment child rollback failed: {cleanup_exc}")
            try:
                if not ingress.stop(timeout_s=rollback_remaining()):
                    add_exception_note(exc, "deployment IPC rollback exceeded its deadline")
            except Exception as cleanup_exc:
                add_exception_note(exc, f"deployment IPC rollback failed: {cleanup_exc}")
            self.supervisor.components.pop(component.component_id, None)
            raise
        self.deployment_component = component
        self._deployment_ingress = ingress
        self._deployment_observer = observer
        self._log(
            f"[Composition] deployment child started under outer ownership "
            f"(pid={component.process.pid if component.process else '?'})"
        )
        return component

    # -- 7. gateway as a GLOBAL component ---------------------------------
    def start_gateway(
        self, argv: list[str], *, env: Optional[dict] = None, log_file=None
    ) -> Optional[ManagedComponent]:
        """The allocation head owns the gateway. Rank zero must not."""
        if self.plan.gateway is None:
            return None
        from .state.ports import PortUnavailable, reserve_port

        pass_fds: tuple[int, ...] = ()
        if self.plan.gateway.kind == "haproxy":
            if self._gateway_listener is None:
                raise CompositionError("HAProxy configuration has no pre-bound listener")
            pass_fds = (self._gateway_listener.fileno(),)
        else:
            # Validation-only gateways lacking socket activation retain a
            # bounded, explicitly non-production lease handoff.  The lease is
            # released immediately before Popen below: holding it through
            # start would guarantee that the child cannot bind.  The resulting
            # close/bind interval is recorded as a validation-only residual;
            # production HAProxy uses exact FD inheritance and has no gap.
            try:
                self._gateway_port_lease = reserve_port(
                    self.plan.gateway.port, max_retries=1, bind_host="0.0.0.0"
                )
            except PortUnavailable as exc:
                raise CompositionError(
                    f"planned gateway port {self.plan.gateway.port} is not ownable: {exc}"
                ) from exc
        component = self.supervisor.register(
            ManagedComponent(
                component_id=f"gateway/{self.plan.gateway.kind}",
                argv=list(argv),
                env=env,
                stdout=log_file,
                long_lived=True,
                pass_fds=pass_fds,
                output_capture=(
                    None if log_file is not None else BoundedOutputCapture(max_bytes=64 << 10)
                ),
                on_unexpected_exit=self._capture_gateway_death,
            )
        )
        rollback_deadline = time.monotonic() + min(
            30.0, self.plan.control.watchdog_cleanup_deadline_s
        )
        try:
            if self._gateway_port_lease is not None:
                self._gateway_port_lease.release()
                self._gateway_port_lease = None
            component.start(rollback_deadline=rollback_deadline)
        except BaseException as exc:
            if self._gateway_port_lease is not None:
                try:
                    self._gateway_port_lease.release()
                except Exception as cleanup_exc:
                    add_exception_note(exc, f"gateway port-lease rollback failed: {cleanup_exc}")
            self._gateway_port_lease = None
            if self._gateway_listener is not None:
                try:
                    self._gateway_listener.close()
                except OSError as cleanup_exc:
                    add_exception_note(exc, f"gateway listener rollback failed: {cleanup_exc}")
                self._gateway_listener = None
            self.supervisor.components.pop(component.component_id, None)
            raise
        if self._gateway_listener is not None:
            # No close/rebind gap: HAProxy inherited this exact listening
            # socket before the parent releases its duplicate descriptor.
            try:
                self._gateway_listener.close()
            except OSError as exc:
                try:
                    component.stop("parent listener close failed", deadline=rollback_deadline)
                except BaseException as cleanup_exc:
                    add_exception_note(
                        exc,
                        f"gateway rollback after listener-close failure: {cleanup_exc}",
                    )
                self.supervisor.components.pop(component.component_id, None)
                raise CompositionError(
                    f"could not release the parent's inherited gateway listener: {exc}"
                ) from exc
            finally:
                self._gateway_listener = None
        self.gateway_component = component
        self._gateway_capture = component.output_capture
        self._log(
            f"[Composition] gateway {self.plan.gateway.kind} started "
            f"(pid={component.process.pid if component.process else '?'})"
        )
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

    def gateway_health_check(
        self, *, timeout_s: Optional[float] = None, poll_s: float = 0.2
    ) -> bool:
        """Prove the owned gateway is alive and accepting TCP locally."""
        if self.plan.gateway is None or self.gateway_component is None:
            return False
        timeout_s = (
            float(self.plan.readiness.gateway_start_deadline_s)
            if timeout_s is None
            else float(timeout_s)
        )
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            self.raise_if_termination("gateway health check interrupted")
            if not self.gateway_alive():
                self._record_gateway_evidence(
                    health_ok=False,
                    detail="gateway process exited during health check",
                    failure=True,
                )
                return False
            try:
                with socket.create_connection(("127.0.0.1", self.plan.gateway.port), timeout=1.0):
                    self._record_gateway_evidence(
                        health_ok=True, detail="owned process accepted TCP on the planned listener"
                    )
                    return True
            except OSError:
                time.sleep(poll_s)
        self._record_gateway_evidence(
            health_ok=False, detail="gateway remained alive but did not accept TCP before deadline"
        )
        return False

    def _gateway_evidence(self, *, health_ok: bool, detail: str):
        from .state.gateway import classify_gateway_evidence

        if self.gateway_component is None or self.plan.gateway is None or self.binding is None:
            raise CompositionError("gateway evidence requested before ownership")
        state, returncode = self.gateway_component.observe()
        capture = self._gateway_capture.snapshot() if self._gateway_capture is not None else {}
        return classify_gateway_evidence(
            deployment_id=self.plan.deployment_id,
            generation=self.generation,
            deployment_plan_hash=self.plan.deployment_plan_hash,
            allocation_binding_hash=self.binding.allocation_binding_hash,
            gateway_kind=self.plan.gateway.kind,
            process_state=state,
            returncode=returncode,
            health_ok=health_ok,
            detail=detail,
            capture=capture,
        )

    def _record_gateway_evidence(self, *, health_ok: bool, detail: str, failure: bool = False):
        from .state.gateway import write_gateway_evidence

        evidence = self._gateway_evidence(health_ok=health_ok, detail=detail)
        self._last_gateway_health = evidence
        write_gateway_evidence(os.path.join(self.run_dir, "gateway_health.json"), evidence)
        if failure or evidence.classification == "process_dead":
            write_gateway_evidence(os.path.join(self.run_dir, "gateway_failure.json"), evidence)
        return evidence

    def _capture_gateway_death(self, returncode: Optional[int]) -> str:
        """Persist death evidence before RuntimeSupervisor assigns first cause."""
        evidence = self._record_gateway_evidence(
            health_ok=False,
            detail=f"owned gateway exited unexpectedly (returncode={returncode})",
            failure=True,
        )
        return (
            f"gateway process_dead; exit_code={evidence.exit_code}; "
            f"signal={evidence.signal}; evidence=gateway_failure.json"
        )

    # -- 8. readiness over the advertised endpoint -------------------------
    def build_readiness(self):
        """Plan-bound readiness bound to THIS generation's evidence."""
        from .control.readiness import ReadinessCoordinator

        self.readiness = ReadinessCoordinator(
            plan=self.plan,
            binding=self.binding,
            receipts=self.receipts,
            sessions=self.sessions,
            log=self._log,
        )
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
        endpoint = self.advertised_endpoint(ip or self.head_address())
        self.readiness.set_advertised_endpoint(endpoint)
        if self.status is not None:
            from .state.status import DeploymentState

            self.status.advance(
                DeploymentState.VALIDATING,
                reason_code="ENDPOINT_ESTABLISHED",
                advertised_endpoint=endpoint,
            )
        if self.plan.gateway is not None:
            self.readiness.set_gateway(alive=self.gateway_alive(), healthy=None)
        self._log(f"[Composition] advertised endpoint {endpoint} ({self.plan.exposure.mode})")
        return endpoint

    def await_deployment_serving(
        self, *, timeout_s: Optional[float] = None, poll_s: Optional[float] = None
    ) -> bool:
        """Wait for fresh authenticated application observations from rank 0."""
        timeout_s = (
            float(self.plan.readiness.initial_deadline_s) if timeout_s is None else float(timeout_s)
        )
        poll_s = (
            float(self.plan.readiness.validation_interval_s) if poll_s is None else float(poll_s)
        )
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            self.raise_if_termination("deployment evidence wait interrupted")
            if self._deployment_observer is None:
                raise CompositionError("deployment evidence observer was not started")
            observer_failure = self._deployment_observer.failure()
            if observer_failure:
                raise CompositionError(
                    f"deployment observation protocol failed: {observer_failure}"
                )
            applications = self.deployment_application_evidence()
            from .control.plan_readiness import planned_application_names

            observed_names = set(applications) - {"__cluster_snapshot__"}
            if observed_names == set(planned_application_names(self.plan)) and all(
                (app := self.application_for_model(model, applications))
                and app["_observation_state"] == "READY"
                and app["target"] == model.num_replicas
                and app["running"] == model.num_replicas
                for model in self.plan.models
            ):
                return True
            if self.sessions is not None and self.sessions.generation_state == "TERMINAL":
                return False
            time.sleep(poll_s)
        return False

    def deployment_application_evidence(self, *, max_age_s: Optional[float] = None) -> dict:
        """Return the current receive-clock-fresh owned-child projection."""
        if self._deployment_observer is None:
            return {}
        freshness = (
            float(max_age_s)
            if max_age_s is not None
            else float(self.plan.readiness.observation_freshness_s)
        )
        snapshot = self._deployment_observer.current(max_age_s=freshness)
        if snapshot is None:
            return {}
        applications = {
            name: {
                **info,
                "_observation_state": (
                    "READY"
                    if info["target"] > 0
                    and info["running"] == info["target"]
                    and info["status"].upper() == "RUNNING"
                    else "STARTING"
                ),
            }
            for name, info in snapshot["applications"].items()
        }
        applications["__cluster_snapshot__"] = {
            "nodes": snapshot["nodes"],
            "proxies": snapshot["proxies"],
        }
        return applications

    def application_for_model(self, model, applications: dict):
        """Aggregate exactly the Serve applications that implement one model.

        Multi-replica canonical placement is represented by one application per
        logical slot (``<route>_rN``), except TP1 null-compute where exact slots
        sharing a rank use one node-grouped application (``<route>_gN``).
        Treating only the first application as the model made a healthy
        deployment look partial and could hide a failed sibling.
        """
        if not applications:
            return None
        app_items = {
            name: info for name, info in applications.items() if name != "__cluster_snapshot__"
        }
        if model.num_replicas > 1 and not self.plan.uses_head_only_serve_proxy():
            groups = self.plan.node_grouped_null_application_groups(model)
            expected_names = (
                {f"{model.route_name}_g{group_index}" for group_index, _group in enumerate(groups)}
                if groups
                else {f"{model.route_name}_r{index}" for index in range(model.num_replicas)}
            )
            members = [info for name, info in app_items.items() if name in expected_names]
            if not members:
                return None
            statuses = [info["status"].upper() for info in members]
            running = sum(info["running"] for info in members)
            target = sum(info["target"] for info in members)
            status = (
                "RUNNING"
                if len(members) == len(expected_names)
                and all(status == "RUNNING" for status in statuses)
                else "STARTING"
            )
            return {
                "running": running,
                "target": target,
                "route_prefix": ("/" if len(self.plan.models) == 1 else f"/{model.route_name}"),
                "status": status,
                "_observation_state": (
                    "READY"
                    if status == "RUNNING"
                    and target == model.num_replicas
                    and running == model.num_replicas
                    else "STARTING"
                ),
                "member_applications": tuple(sorted(expected_names & set(app_items))),
            }
        return app_items.get(model.route_name)

    def apply_deployment_evidence(self) -> dict:
        """Project fresh authenticated app observations into PlanReadiness."""
        if self.readiness is None:
            raise CompositionError("readiness has not been constructed")
        applications = self.deployment_application_evidence()
        cluster = applications.get("__cluster_snapshot__", {})
        nodes = cluster.get("nodes", ())
        proxies = list(cluster.get("proxies", ()))
        if self.head_channel is not None:
            from .plan.contracts import same_node

            freshness = float(self.plan.readiness.observation_freshness_s)
            proxies = []
            proxy_ranks = (
                ((0, dict(self.binding.rank_to_node)[0]),)
                if self.plan.uses_head_only_serve_proxy()
                else self.binding.rank_to_node
            )
            for rank, planned_node in proxy_ranks:
                matching_nodes = [
                    node for node in nodes if same_node(node["node_name"], planned_node)
                ]
                if len(matching_nodes) != 1:
                    continue
                observations = self.head_channel.current_observations(
                    rank=rank, role="serve_proxy", max_age_s=freshness
                )
                healthy = any(obs.state == "READY" for obs in observations)
                proxies.append(
                    {
                        "node_id": matching_nodes[0]["node_id"],
                        "status": "HEALTHY" if healthy else "STARTING",
                    }
                )
        self.readiness.set_cluster(nodes=nodes, proxies=proxies)
        self.readiness.set_applications(
            name for name in applications if name != "__cluster_snapshot__"
        )
        self.readiness.set_owned_component(
            "deployment",
            bool(self.deployment_component) and self.deployment_component.observe()[0] == "RUNNING",
        )
        self.readiness.set_owned_component(
            "rank_launcher",
            any(
                component.component_id == "rank_launcher" and component.observe()[0] == "RUNNING"
                for component in self.supervisor.components.values()
            ),
        )
        if self.head_channel is not None:
            freshness = float(self.plan.readiness.observation_freshness_s)
            for rank in self.binding.ranks():
                observations = self.head_channel.current_observations(
                    rank=rank, role="ray", max_age_s=freshness
                )
                self.readiness.set_rank_component(
                    rank, any(obs.state == "RUNNING" for obs in observations)
                )
        for model in self.plan.models:
            app = self.application_for_model(model, applications)
            running = (app or {}).get("running", 0)
            observed_target = (app or {}).get("target", 0)
            self.readiness.set_replicas(model.model_id, running, observed_target)
            self.readiness.set_route(
                model.route_name,
                bool(app)
                and app.get("_observation_state") == "READY"
                and app.get("status") == "RUNNING",
            )
        return applications

    def await_initial_readiness(
        self, *, timeout_s: Optional[float] = None, poll_s: Optional[float] = None
    ):
        """Wait for the complete plan predicate to converge before READY.

        Application readiness, per-node proxy probes, receipt delivery, and
        gateway health are independent observation streams. A one-shot verdict
        races those streams on multi-node deployments: applications may be at
        target one probe cycle before a worker's proxy observation arrives.
        This loop retains the immutable predicate and fails only at its bounded
        initial deadline or on a typed component/control failure.
        """
        if self.readiness is None or self.readiness.phase != "VALIDATING":
            raise CompositionError("initial readiness requires the VALIDATING phase")
        timeout_s = self.plan.readiness.initial_deadline_s if timeout_s is None else timeout_s
        poll_s = self.plan.readiness.validation_interval_s if poll_s is None else poll_s
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or isinstance(poll_s, bool)
            or not isinstance(poll_s, (int, float))
            or not math.isfinite(float(timeout_s))
            or timeout_s <= 0
            or not math.isfinite(float(poll_s))
            or poll_s <= 0
        ):
            raise ValueError("initial readiness timeout and poll interval must be positive/finite")
        timeout_s = float(timeout_s)
        poll_s = float(poll_s)

        deadline = time.monotonic() + timeout_s
        last_blockers: tuple[str, ...] | None = None
        verdict = None
        while True:
            self.raise_if_termination("initial readiness interrupted")
            if self.sessions is not None and self.sessions.generation_state == "TERMINAL":
                raise CompositionError(
                    f"rank generation became terminal during readiness: "
                    f"{self.sessions.terminal_reason}"
                )
            if self._deployment_observer is not None:
                observer_failure = self._deployment_observer.failure()
                if observer_failure:
                    raise CompositionError(
                        f"deployment observation protocol failed: {observer_failure}"
                    )

            if self.plan.gateway is not None:
                alive = self.gateway_alive()
                if alive is False:
                    reason = "gateway process exited during initial readiness"
                    self._record_gateway_evidence(health_ok=False, detail=reason, failure=True)
                    raise CompositionError(reason)
                healthy = self.gateway_health_check(timeout_s=max(0.1, min(5.0, poll_s)))
                self.readiness.set_gateway(alive=alive, healthy=healthy)

            self.apply_deployment_evidence()
            for model in self.plan.models:
                # The polling cadence controls how often validation starts; it
                # is not an inference deadline.  In particular, substituting a
                # five-second poll interval for the resolved canary timeout
                # makes a healthy but queued gateway fail under planned load.
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                ok, detail = self.canary_advertised_endpoint(
                    model,
                    timeout_s=min(
                        float(self.plan.readiness.canary_timeout_s),
                        remaining,
                    ),
                )
                self.readiness.set_canary(model.model_id, ok)
                if not ok:
                    self._log(f"[Readiness] initial canary {model.model_id} failed: {detail}")
            verdict = self.readiness.evaluate()
            if verdict.ready:
                return verdict
            if verdict.blockers != last_blockers:
                self._log(f"[Readiness] awaiting convergence: {list(verdict.blockers)[:6]}")
                last_blockers = verdict.blockers
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(poll_s, remaining))

        assert verdict is not None
        raise CompositionError(
            f"readiness not satisfied via the advertised endpoint within {timeout_s:g}s: "
            f"{list(verdict.blockers)}"
        )

    def _gateway_backend_endpoints(self):
        """Return the canonical Serve applications visible to the gateway."""
        if self.binding is None:
            raise CompositionError("gateway preparation requires AllocationBinding")
        if self.plan.gateway is None:
            return []

        from .proxy.base import BackendEndpoint

        gateway = self.plan.gateway
        single_model = len(self.plan.models) == 1
        endpoints = []
        rank_to_node = dict(self.binding.rank_to_node)
        if gateway.kind == "litellm":
            # LiteLLM performs application selection itself.  Give it exactly
            # one directly addressable entry per canonical replica, on that
            # replica's primary planned node.  Feeding every replica route to
            # every node creates a node x replica Cartesian product (49,152
            # entries at the 64-node paper point) with no additional reachability.
            for model in self.plan.models:
                if model.num_replicas > 1:
                    for replica in model.replicas:
                        endpoints.append(
                            BackendEndpoint(
                                host=rank_to_node[replica.planned_ranks[0]],
                                port=gateway.backend_port,
                                model_id=model.model_id,
                                path_prefix=(f"/{model.route_name}_r{replica.replica_index}"),
                            )
                        )
                else:
                    replica = model.replicas[0]
                    endpoints.append(
                        BackendEndpoint(
                            host=rank_to_node[replica.planned_ranks[0]],
                            port=gateway.backend_port,
                            model_id=model.model_id,
                            path_prefix=(f"/{model.route_name}" if not single_model else ""),
                        )
                    )
            return endpoints

        grouped_routes = {
            model.model_id: self.plan.node_grouped_null_application_groups(model)
            for model in self.plan.models
        }
        for _rank, node in self.binding.rank_to_node:
            for model in self.plan.models:
                groups = grouped_routes[model.model_id]
                replica_routes = (
                    len(groups) if groups else (model.num_replicas if model.num_replicas > 1 else 0)
                )
                endpoints.append(
                    BackendEndpoint(
                        host=node,
                        port=gateway.backend_port,
                        model_id=model.model_id,
                        path_prefix=(
                            f"/{model.route_name}" if replica_routes or not single_model else ""
                        ),
                        replica_routes=replica_routes,
                        route_suffix=("_g" if groups else "_r"),
                    )
                )
        return endpoints

    def gateway_argv(self, config_dir: str) -> Optional[list[str]]:
        """Render, atomically publish, and preflight the exact gateway config."""
        if self.plan.gateway is None:
            return None
        if self.binding is None:
            raise CompositionError("gateway preparation requires AllocationBinding")
        import hashlib
        import re
        import shutil
        from pathlib import Path

        from .control.finite_process import run_finite
        from .state.atomic import atomic_create_or_verify_json, regular_file_reader

        gateway = self.plan.gateway
        ref = gateway.executable_ref
        if ref.startswith("PATH:"):
            name = ref.removeprefix("PATH:")
            if not re.fullmatch(r"[A-Za-z0-9_.+-]+", name):
                raise CompositionError(f"unsafe gateway PATH reference {ref!r}")
            executable = shutil.which(name)
        else:
            executable = ref if os.path.isabs(ref) and os.access(ref, os.X_OK) else None
        if not executable:
            raise CompositionError(f"gateway executable could not be resolved from {ref!r}")

        endpoints = self._gateway_backend_endpoints()

        def thaw(value):
            if isinstance(value, tuple):
                if all(
                    isinstance(item, tuple) and len(item) == 2 and isinstance(item[0], str)
                    for item in value
                ):
                    return {key: thaw(item) for key, item in value}
                return [thaw(item) for item in value]
            return value

        options = thaw(gateway.options)
        port_handoff = "close_then_bind_validation_only"
        gateway_resource_evidence = None
        if gateway.kind == "haproxy":
            import resource

            from .proxy.haproxy_proxy import HAProxyProxy
            from .proxy.haproxy_proxy import required_nofile
            from .state.ports import PortUnavailable, bind_listener

            required_fds = required_nofile(int(options.get("maxconn", 8000)), len(endpoints))
            soft_fds, hard_fds = resource.getrlimit(resource.RLIMIT_NOFILE)
            if hard_fds != resource.RLIM_INFINITY and required_fds > hard_fds:
                raise CompositionError(
                    f"HAProxy maxconn={options.get('maxconn', 8000)} with {len(endpoints)} "
                    f"backend servers requires at least {required_fds} file descriptors, "
                    f"but this allocation's hard RLIMIT_NOFILE is {hard_fds}; lower maxconn "
                    "or qualify a site environment with a higher hard limit"
                )
            if soft_fds != resource.RLIM_INFINITY and soft_fds < required_fds:
                try:
                    resource.setrlimit(resource.RLIMIT_NOFILE, (required_fds, hard_fds))
                except (OSError, ValueError) as exc:
                    raise CompositionError(
                        f"could not raise soft RLIMIT_NOFILE from {soft_fds} to "
                        f"the HAProxy requirement {required_fds}: {exc}"
                    ) from exc
                soft_fds = required_fds
            gateway_resource_evidence = {
                "required_nofile": required_fds,
                "soft_nofile": soft_fds,
                "hard_nofile": hard_fds,
            }

            try:
                self._gateway_listener = bind_listener(gateway.port, host="0.0.0.0")
            except PortUnavailable as exc:
                raise CompositionError(f"planned HAProxy listener cannot bind: {exc}") from exc
            try:
                request_body_limit_bytes = self.plan.exposure.request_body_limit_bytes
                if request_body_limit_bytes is None:
                    raise CompositionError(
                        "compiled HAProxy exposure has no enforced request-body limit"
                    )
                options["request_body_limit_bytes"] = request_body_limit_bytes
                options["bind_target"] = f"fd@{self._gateway_listener.fileno()}"
                if not options.get("nbthread"):
                    options["nbthread"] = gateway.worker_count
                config_path = HAProxyProxy().generate_config(endpoints, Path(config_dir), **options)
                with regular_file_reader(config_path) as config_handle:
                    config_text = config_handle.read()
                if (
                    "{PORT}" in config_text
                    or config_text.count(f"bind fd@{self._gateway_listener.fileno()}") != 1
                ):
                    raise CompositionError(
                        "generated HAProxy config does not bind exactly the inherited socket"
                    )
                checked = run_finite(
                    [executable, "-c", "-f", str(config_path)],
                    check=False,
                    timeout_s=30,
                    pass_fds=(self._gateway_listener.fileno(),),
                    cancel_requested=self.termination_requested,
                )
                if checked.returncode != 0:
                    detail = checked.stderr.strip() or checked.stdout.strip()
                    raise CompositionError(f"HAProxy configuration preflight failed: {detail}")
            except BaseException:
                # Listener ownership begins before rendering. Every failure
                # between bind and child inheritance must unwind it, including
                # option validation, artifact I/O, and preflight timeout.
                self._gateway_listener.close()
                self._gateway_listener = None
                raise
            argv = [executable, "-f", str(config_path), "-db"]
            self._gateway_environment = None
            port_handoff = "inherited_listening_fd"
        elif gateway.kind == "litellm":
            from .proxy.litellm_proxy import LiteLLMProxy

            child_env = dict(os.environ)
            for option in ("master_key", "db_url"):
                reference = options.pop(option, None)
                if reference is None:
                    continue
                if (
                    not isinstance(reference, dict)
                    or set(reference) != {"secret_ref"}
                    or not str(reference["secret_ref"]).startswith("ENV:")
                ):
                    raise CompositionError(f"LiteLLM {option} requires an ENV:<name> secret_ref")
                env_name = str(reference["secret_ref"])[4:]
                secret = os.environ.get(env_name)
                if secret is None:
                    raise CompositionError(
                        f"LiteLLM secret reference ENV:{env_name} is unavailable"
                    )
                child_env[("LITELLM_MASTER_KEY" if option == "master_key" else "DATABASE_URL")] = (
                    secret
                )
            for key in list(child_env):
                if (
                    key.lower() in {"http_proxy", "https_proxy"}
                    or key in {"DEBUG", "DETAILED_DEBUG"}
                    or key.startswith(
                        (
                            "ZE_",
                            "ONEAPI_",
                            "SYCL_",
                            "CCL_",
                            "I_MPI_",
                            "FI_",
                            "INTEL_",
                            "LIBOMPTARGET_",
                        )
                    )
                ):
                    child_env.pop(key, None)
            child_env["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
            config_path = LiteLLMProxy().generate_config(endpoints, Path(config_dir), **options)
            with regular_file_reader(config_path) as config_handle:
                config_text = config_handle.read()
            argv = [
                executable,
                "--config",
                str(config_path),
                "--port",
                str(gateway.port),
                "--host",
                "0.0.0.0",
                "--num_workers",
                str(gateway.worker_count),
                "--keepalive_timeout",
                str(options["keepalive_timeout"]),
            ]
            self._gateway_environment = child_env
        elif gateway.kind == "nginx":
            from .proxy.nginx_proxy import NGINXProxy

            options["listen_port"] = gateway.port
            options.setdefault("worker_processes", gateway.worker_count)
            config_path = NGINXProxy().generate_config(endpoints, Path(config_dir), **options)
            with regular_file_reader(config_path) as config_handle:
                config_text = config_handle.read()
            preflight = run_finite(
                [executable, "-t", "-c", str(config_path), "-p", str(Path(config_dir).resolve())],
                timeout_s=30.0,
                cancel_requested=self.termination_requested,
            )
            if preflight.returncode != 0:
                detail = preflight.stderr.strip() or preflight.stdout.strip()
                raise CompositionError(f"NGINX configuration preflight failed: {detail[:800]}")
            argv = [executable, "-c", str(config_path), "-p", str(Path(config_dir).resolve())]
            self._gateway_environment = None
        elif gateway.kind == "envoy":
            from .proxy.envoy_proxy import EnvoyProxy

            options["listen_port"] = gateway.port
            options.setdefault("concurrency", gateway.worker_count)
            config_path = EnvoyProxy().generate_config(endpoints, Path(config_dir), **options)
            with regular_file_reader(config_path) as config_handle:
                config_text = config_handle.read()
            preflight = run_finite(
                [executable, "--mode", "validate", "-c", str(config_path)],
                timeout_s=30.0,
                cancel_requested=self.termination_requested,
            )
            if preflight.returncode != 0:
                detail = preflight.stderr.strip() or preflight.stdout.strip()
                raise CompositionError(f"Envoy configuration preflight failed: {detail[:800]}")
            argv = [
                executable,
                "-c",
                str(config_path),
                "--concurrency",
                str(options["concurrency"]),
            ]
            self._gateway_environment = None
        elif gateway.kind == "pingora":
            from .proxy.pingora_proxy import PingoraProxy

            options["listen_port"] = gateway.port
            options.setdefault("threads", gateway.worker_count)
            config_path = PingoraProxy().generate_config(endpoints, Path(config_dir), **options)
            with regular_file_reader(config_path) as config_handle:
                config_text = config_handle.read()
            preflight = run_finite(
                [executable, "--config", str(config_path), "--check-config"],
                timeout_s=30.0,
                cancel_requested=self.termination_requested,
            )
            if preflight.returncode != 0:
                detail = preflight.stderr.strip() or preflight.stdout.strip()
                raise CompositionError(f"Pingora configuration preflight failed: {detail[:800]}")
            argv = [executable, "--config", str(config_path)]
            self._gateway_environment = None
        else:
            raise CompositionError(f"gateway {gateway.kind} has no supervised launcher yet")
        config_hash = hashlib.sha256(config_text.encode()).hexdigest()
        try:
            atomic_create_or_verify_json(
                os.path.join(config_dir, "gateway_config_manifest.json"),
                {
                    "schema_version": 1,
                    "deployment_id": self.plan.deployment_id,
                    "generation": self.generation,
                    "deployment_plan_hash": self.plan.deployment_plan_hash,
                    "allocation_binding_hash": self.binding.allocation_binding_hash,
                    "gateway_kind": gateway.kind,
                    "config_sha256": config_hash,
                    "executable": executable,
                    "argv": argv,
                    "port_handoff": port_handoff,
                    "production_qualified": self.production_qualified,
                    "resource_limits": gateway_resource_evidence,
                },
            )
        except BaseException:
            if self._gateway_listener is not None:
                self._gateway_listener.close()
                self._gateway_listener = None
            raise
        self._log(f"[Composition] {gateway.kind} config {config_hash[:12]} preflight passed")
        return argv

    def gateway_environment(self) -> Optional[dict]:
        """Sanitized child environment prepared with the gateway config."""
        return self._gateway_environment

    def canary_advertised_endpoint(self, model, *, timeout_s: Optional[float] = None) -> tuple:
        """One real completion through the COMPILED advertised endpoint."""
        import json
        import urllib.error
        import urllib.request

        if (
            self.plan.gateway is None
            and model.num_replicas > 1
            and not self.plan.uses_head_only_serve_proxy()
        ):
            # DIRECT_VALIDATION has no gateway to select a replica route. Probe
            # a concrete bound application; exact receipt/Serve evidence still
            # covers every sibling. Production clients never see this mode.
            route = f"/{model.route_name}_r0"
        elif len(self.plan.models) == 1 or (
            self.plan.gateway is not None and self.plan.gateway.kind == "litellm"
        ):
            route = ""
        else:
            route = f"/{model.route_name}"
        url = f"{self.readiness.advertised_endpoint}{route}/v1/completions"
        body = json.dumps(
            {"model": model.model_id, "prompt": "The capital of France is", "max_tokens": 4}
        ).encode()
        request = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        timeout_s = (
            float(self.plan.readiness.canary_timeout_s) if timeout_s is None else float(timeout_s)
        )
        try:
            with opener.open(request, timeout=timeout_s) as response:
                from .state.atomic import strict_json_loads

                payload = strict_json_loads(response.read().decode())
        except (urllib.error.URLError, OSError, ValueError) as exc:
            return False, f"{type(exc).__name__}: {exc}"
        if not isinstance(payload, dict):
            return False, f"completion payload is not an object: {str(payload)[:120]}"
        choices = payload.get("choices")
        if (
            not isinstance(choices, list)
            or not choices
            or not isinstance(choices[0], dict)
            or not isinstance(choices[0].get("text"), str)
        ):
            return False, f"no completion: {str(payload)[:120]}"
        return True, choices[0]["text"][:60]

    def monitor_readiness(self) -> Optional[FirstCause]:
        """Re-evaluate the live predicate and enforce post-READY recovery.

        Evidence expiry, replica/route loss, gateway health, and advertised-
        endpoint canaries are checked on a bounded cadence. A gateway process
        exit is immediately terminal; recoverable validation loss gets one
        resolved grace interval with the durable READY record revoked.
        """
        if self.readiness is None or self.readiness.phase not in {"READY", "VALIDATING"}:
            return None
        if self._deployment_observer is not None:
            observer_failure = self._deployment_observer.failure()
            if observer_failure:
                reason = f"deployment observation protocol failed: {observer_failure}"
                self.readiness.fail(reason)
                return FirstCause("readiness", "DEPLOYMENT_OBSERVATION_FAILED", reason)
        now = time.monotonic()
        interval = float(self.plan.readiness.validation_interval_s)
        if now - self._last_live_validation < interval:
            return None
        self._last_live_validation = now

        if self.plan.gateway is not None:
            alive = self.gateway_alive()
            if alive is False:
                reason = "gateway process exited after READY"
                self._record_gateway_evidence(health_ok=False, detail=reason, failure=True)
                self.readiness.revoke(reason, gateway_dead=True)
                return FirstCause("gateway", "GATEWAY_FAILURE", reason)
            healthy = self.gateway_health_check(timeout_s=min(5.0, interval))
            self.readiness.set_gateway(alive=alive, healthy=healthy)

        self.apply_deployment_evidence()
        for model in self.plan.models:
            ok, detail = self.canary_advertised_endpoint(
                model,
                timeout_s=float(self.plan.readiness.canary_timeout_s),
            )
            self.readiness.set_canary(model.model_id, ok)
            if not ok:
                self._log(f"[Readiness] live canary {model.model_id} failed: {detail}")

        # A timeout becomes evidence only when its probe finishes.  Anchoring
        # recovery to the pre-probe timestamp silently spends the entire
        # recovery window while a bounded canary is still in flight.
        observed_at = time.monotonic()
        verdict = self.readiness.evaluate()
        if verdict.ready:
            if self.readiness.phase == "VALIDATING" and self._validation_loss_started is not None:
                candidate = self.readiness.evaluate()
                self.publish_ready(candidate)
                committed = self.readiness.commit_ready()
                if not committed.ready:
                    raise CompositionError("revalidation predicate changed before READY commit")
                self._log("[Readiness] validation recovered; READY re-persisted")
            elif self.status is not None:
                from .status_api import ReadyEvidenceChanged

                try:
                    self.status.refresh_ready(
                        readiness_snapshot=verdict.to_dict(),
                        model_map=verdict.model_map,
                        capability_map=verdict.capability_map,
                        receipt_hashes=list(verdict.receipt_hashes),
                    )
                except ReadyEvidenceChanged as exc:
                    # A compact heartbeat cannot bind a new receipt/topology
                    # set. Cross a visible validation boundary, persist a new
                    # immutable receipt manifest, and then publish a full READY
                    # record. This is rare (for example a fast actor restart)
                    # and avoids both stale evidence and periodic 4 MiB writes.
                    from .state.status import DeploymentState

                    changed = verdict.to_dict()
                    self.status.advance(
                        DeploymentState.VALIDATING,
                        reason_code="READY_EVIDENCE_CHANGED",
                        detail=str(exc),
                        readiness_snapshot=changed,
                        model_map=verdict.model_map,
                        capability_map=verdict.capability_map,
                        receipt_hashes=list(verdict.receipt_hashes),
                    )
                    self.publish_ready(verdict)
                    self._log("[Readiness] READY evidence changed; full manifest republished")
            self._validation_loss_started = None
            return None

        reason = "; ".join(verdict.blockers[:4]) or "readiness predicate lost"
        if self.readiness.phase == "READY":
            self.readiness.revoke(reason)
            if self.status is not None:
                from .state.status import DeploymentState

                revoked = self.readiness.evaluate()
                self.status.advance(
                    DeploymentState.VALIDATING,
                    reason_code="READINESS_REVOKED",
                    detail=reason,
                    readiness_snapshot=revoked.to_dict(),
                    model_map=revoked.model_map,
                    capability_map=revoked.capability_map,
                    receipt_hashes=list(revoked.receipt_hashes),
                )
            self._validation_loss_started = observed_at
        elif self._validation_loss_started is None:
            self._validation_loss_started = observed_at

        recovery_s = float(self.plan.readiness.recovery_deadline_s)
        if observed_at - self._validation_loss_started >= recovery_s:
            terminal = f"readiness did not recover within {recovery_s:.1f}s: {reason}"
            self.readiness.fail(terminal)
            return FirstCause("readiness", "READINESS_RECOVERY_EXPIRED", terminal)
        return None

    def publish_ready(self, verdict) -> None:
        """Atomically publish READY and the exact evidence that justifies it."""
        if self.status is None:
            raise CompositionError("authoritative deployment status is unavailable; refusing READY")
        from .state.receipts import ReceiptManifest, write_receipt_manifest
        from .state.status import DeploymentState

        # Immutable binding events are authoritative; force their batched live
        # projection to the exact accepted set before publishing READY.
        self.binding_store.flush()
        accepted = self.receipts.accepted_receipts()
        receipt_hashes = tuple(sorted(item.receipt_hash for item in accepted))
        if receipt_hashes != tuple(verdict.receipt_hashes):
            raise CompositionError("accepted receipt set changed before READY evidence persistence")
        receipt_manifest = ReceiptManifest(
            schema_version=1,
            deployment_id=self.plan.deployment_id,
            generation=self.generation,
            deployment_plan_hash=self.plan.deployment_plan_hash,
            allocation_binding_hash=self.binding.allocation_binding_hash,
            receipt_hashes=receipt_hashes,
            receipts=tuple(item.to_dict() for item in accepted),
        ).finalize()
        receipt_path = os.path.join(
            self.run_dir, "compatibility_receipts", f"{receipt_manifest.manifest_hash}.json"
        )
        write_receipt_manifest(receipt_path, receipt_manifest)

        snapshot = verdict.to_dict()
        snapshot["phase"] = DeploymentState.READY.value
        snapshot["ready"] = True
        snapshot["receipt_manifest_path"] = receipt_path
        snapshot["receipt_manifest_hash"] = receipt_manifest.manifest_hash
        self.status.advance(
            DeploymentState.READY,
            reason_code="READY",
            advertised_endpoint=verdict.advertised_endpoint,
            readiness_snapshot=snapshot,
            model_map=verdict.model_map,
            capability_map=verdict.capability_map,
            receipt_hashes=list(verdict.receipt_hashes),
        )

    # -- 9. termination ----------------------------------------------------
    def fail(self, reason: str) -> None:
        if self.first_cause is None:
            self.first_cause = reason
        self._log(f"[Composition] FIRST CAUSE: {reason}")
        # A requested shutdown is not a fault. `exit_code()` already keeps 143
        # distinct from a failure; publishing FAILED on the shared record threw
        # that distinction away again, so a consumer read an orderly teardown as
        # a fault. The drain path publishes DRAINING -> STOPPED instead.
        if self.status is not None and not self._operator_shutdown_requested():
            from .state.status import DeploymentState
            from .status_api import StatusPublicationError

            if self.status.state not in {
                DeploymentState.FAILED.value,
                DeploymentState.CANCELLED.value,
                DeploymentState.STOPPED.value,
            }:
                try:
                    self.status.advance(
                        DeploymentState.FAILED, reason_code="FIRST_CAUSE", detail=str(reason)[:400]
                    )
                except StatusPublicationError as exc:
                    # We are already on the fatal path. Preserve both causes
                    # and continue into bounded cleanup rather than masking the
                    # original failure with a second exception.
                    self._log(f"[Status] terminal publication failed: {exc}")

    def _rank_sessions_may_be_unavailable(self) -> bool:
        """Whether the fatal cause itself can remove rank control sessions.

        A rank-launcher or authenticated-control failure still requires every
        owned process group and local thread to be reaped. It cannot, however,
        require a DRAIN acknowledgement from a rank whose already-recorded
        failure removed that very session. Other failures (for example a dead
        gateway) retain the strict all-rank DRAIN/GOODBYE contract.
        """
        cause = self.supervisor.first_cause
        return cause is not None and (
            cause.component_id in {"rank_launcher", "control"}
            or cause.reason_code == "CONTROL_FAILURE"
        )

    def shutdown(self, *, drain_s: float = 30.0) -> None:
        """Bounded reverse-order cleanup; a cleanup error never hides the cause."""
        from .status_api import StatusPublicationError

        if not isinstance(drain_s, (int, float)) or isinstance(drain_s, bool):
            raise ValueError("shutdown deadline must be numeric")
        if not math.isfinite(float(drain_s)) or drain_s < 0:
            raise ValueError("shutdown deadline must be finite and nonnegative")
        cleanup_deadline = time.monotonic() + float(drain_s)
        cleanup_errors: list[str] = []
        cleanup_clean = True
        channel_clean = True

        def cleanup_failed(detail: str) -> None:
            nonlocal cleanup_clean
            cleanup_clean = False
            cleanup_errors.append(detail)
            self.first_cause = self.first_cause or detail
            self._log(f"[Composition] cleanup error: {detail}")

        def remaining() -> float:
            return max(0.0, cleanup_deadline - time.monotonic())

        def stop_global_component(
            component: Optional[ManagedComponent],
            *,
            reason: str,
            deadline: float,
        ) -> None:
            """Stop one allocation-head component without hiding cleanup failure.

            The gateway and isolated deployment child depend on a live Ray
            cluster in opposite ways: ingress must close before the serving
            application drains, while the deployment child must finish its
            ``serve.shutdown()`` call before rank-local supervisors stop Ray.
            Keeping this sequencing here preserves one outer deadline and
            leaves ``RuntimeSupervisor.shutdown`` as the idempotent final reap.
            """
            if component is None:
                return
            try:
                component.stop(reason, deadline=max(time.monotonic(), deadline))
            except (OSError, RuntimeError, ValueError) as exc:
                cleanup_failed(
                    f"{component.component_id} ordered cleanup failed: {type(exc).__name__}: {exc}"
                )

        if self.status is not None:
            from .state.status import DeploymentState

            # Draining is a state consumers must be able to see: a client that
            # keeps dispatching into a draining deployment reads as a serving
            # failure when it is an orderly shutdown.
            try:
                if self.status.state == DeploymentState.READY.value:
                    self.status.advance(DeploymentState.DRAINING, reason_code="SHUTDOWN_REQUESTED")
                    self._draining = True
                elif self.status.state in {
                    DeploymentState.PLANNED.value,
                    DeploymentState.STAGING.value,
                    DeploymentState.CLUSTER_STARTING.value,
                    DeploymentState.DEPLOYING.value,
                    DeploymentState.VALIDATING.value,
                }:
                    # Cancellation is terminal. Publish it only after cleanup,
                    # so an unreaped child can still produce FAILED rather than
                    # a misleading clean cancellation.
                    self._cancelling = True
            except (StatusPublicationError, RuntimeError, ValueError) as exc:
                cleanup_failed(f"shutdown status publication failed: {exc}")
        if self.head_channel is not None and self.head_channel.start_broadcast():
            rank_sessions_unavailable = self._rank_sessions_may_be_unavailable()
            if not rank_sessions_unavailable:
                # Revoke external ingress first, then let the deployment child
                # drain Serve while Ray is still alive.  Broadcasting rank
                # DRAIN before this point used to tear down Ray underneath
                # ``serve.shutdown()``, producing a deterministic 30-second
                # hang and forced SIGKILL on every otherwise-clean stop.
                # Rank-local shutdown includes Ray process-group reaping,
                # optional bounded diagnostics, the terminal observation, and
                # GOODBYE delivery.  A ten-second ceiling made that mandatory
                # tail shorter than the rank's own cleanup operations at
                # scale: every rank could exit zero while the head reached its
                # deadline before consuming all queued GOODBYEs.  Preserve a
                # quarter of the one outer deadline (with a two-second floor)
                # for the all-rank protocol instead of capping it below the
                # work the protocol is required to perform.
                rank_cleanup_reserve = max(2.0, remaining() * 0.25)
                gateway_budget = max(0.0, remaining() - rank_cleanup_reserve)
                # A universal five-second slice is too short for an owned
                # multiprocess gateway. In particular, LiteLLM's Uvicorn
                # parent must terminate and reap every spawned worker before
                # the process group can be proven empty. Give ingress up to
                # one third of the pre-rank budget (bounded at 30 seconds),
                # while preserving both the deployment-drain tail and the
                # explicit all-rank reserve under this same outer deadline.
                gateway_stop_budget = min(
                    gateway_budget,
                    30.0,
                    max(5.0, gateway_budget / 3.0),
                )
                stop_global_component(
                    self.gateway_component,
                    reason="revoke ingress before deployment drain",
                    deadline=time.monotonic() + gateway_stop_budget,
                )
                stop_global_component(
                    self.deployment_component,
                    reason="drain deployment before rank-local Ray shutdown",
                    deadline=max(time.monotonic(), cleanup_deadline - rank_cleanup_reserve),
                )
            if rank_sessions_unavailable:
                # A typed rank/control failure commonly makes the MPI launcher
                # tear down every rank session before the root can deliver
                # DRAIN.  Give any surviving session a short chance to accept
                # the command, but do not spend the global watchdog deadline
                # waiting for acknowledgements that the recorded failure made
                # impossible.  The remaining budget belongs to mandatory
                # process-group and local-thread reaping.
                graceful_budget = min(5.0, remaining() * 0.1)
                graceful_deadline = time.monotonic() + graceful_budget
            else:
                # On graceful and non-rank failure paths every planned rank is
                # still required to acknowledge DRAIN and send GOODBYE.  Leave
                # a bounded tail for forced process-group reaping if one rank
                # does not complete its local cleanup.
                force_reap_budget = min(5.0, max(1.0, remaining() * 0.2))
                graceful_deadline = max(time.monotonic(), cleanup_deadline - force_reap_budget)
            acknowledged = self.head_channel.broadcast_shutdown("DRAIN", deadline=graceful_deadline)
            self._log(
                f"[Composition] DRAIN acknowledged by {acknowledged}/{self.plan.num_nodes} rank(s)"
            )
            goodbyes = self.head_channel.wait_shutdown_goodbyes(deadline=graceful_deadline)
            self._log(
                f"[Composition] GOODBYE received from {goodbyes}/"
                f"{acknowledged} acknowledging rank(s)"
            )
            if acknowledged != self.plan.num_nodes or goodbyes != self.plan.num_nodes:
                detail = (
                    "rank drain protocol incomplete: "
                    f"acknowledged={acknowledged}/{self.plan.num_nodes}, "
                    f"goodbye={goodbyes}/{self.plan.num_nodes}"
                )
                if rank_sessions_unavailable:
                    self._log(
                        f"[Composition] {detail} after fatal rank/control loss; "
                        "owned process reaping remains mandatory"
                    )
                else:
                    cleanup_failed(detail)
        if not self.supervisor.shutdown(deadline=cleanup_deadline):
            cleanup_failed("one or more owned process groups survived cleanup")
        if self._gateway_port_lease is not None:
            try:
                self._gateway_port_lease.release()
            except (OSError, RuntimeError) as exc:
                cleanup_failed(f"gateway port lease cleanup failed: {exc}")
            self._gateway_port_lease = None
        if self._gateway_listener is not None:
            try:
                self._gateway_listener.close()
            except OSError as exc:
                cleanup_failed(f"gateway listener cleanup failed: {exc}")
            self._gateway_listener = None
        if self._deployment_observer is not None:
            try:
                if not self._deployment_observer.stop(timeout_s=min(5.0, remaining())):
                    cleanup_failed("deployment observer thread did not stop")
            except (OSError, RuntimeError) as exc:
                cleanup_failed(f"deployment observer cleanup failed: {exc}")
        if self._deployment_ingress is not None:
            try:
                if not self._deployment_ingress.stop(timeout_s=min(5.0, remaining())):
                    cleanup_failed("deployment ingress thread did not stop")
            except OSError as exc:
                cleanup_failed(f"deployment ingress cleanup failed: {exc}")
        if self.head_channel is not None:
            try:
                channel_clean = self.head_channel.stop(deadline=cleanup_deadline)
            except (OSError, RuntimeError) as exc:
                channel_clean = False
                cleanup_failed(f"control listener cleanup failed: {exc}")
            if not channel_clean:
                cleanup_failed("control listener or event-loop cleanup was incomplete")

        # Publish a conservative audit record before the terminal transition,
        # then atomically replace it with the observed publication outcome. If
        # the second write fails, the durable first copy still says ``pending``
        # and ``clean=false`` rather than overclaiming a fully audited stop.
        from .state.atomic import atomic_write_json
        from .state.status import DeploymentState

        terminal_requested = self.status is not None and (
            getattr(self, "_draining", False) or getattr(self, "_cancelling", False)
        )
        terminal_publication = "pending" if terminal_requested else "not_required"

        def shutdown_payload() -> dict:
            publication_complete = terminal_publication in {"published", "not_required"}
            return {
                "schema_version": 2,
                "deployment_id": self.plan.deployment_id,
                "generation": self.generation,
                "deployment_plan_hash": self.plan.deployment_plan_hash,
                "clean": cleanup_clean and channel_clean and publication_complete,
                "errors": list(cleanup_errors),
                "deadline_exhausted": remaining() <= 0,
                "terminal_publication": terminal_publication,
                "observed_terminal_state": (self.status.state if self.status is not None else None),
                "components": {
                    component_id: {
                        "state": component.state,
                        "returncode": (
                            component.process.poll() if component.process is not None else None
                        ),
                    }
                    for component_id, component in sorted(self.supervisor.components.items())
                },
            }

        report_path = os.path.join(self.run_dir, "shutdown_report.json")
        try:
            atomic_write_json(report_path, shutdown_payload())
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            cleanup_failed(f"shutdown report publication failed: {exc}")

        if terminal_requested:
            try:
                if cleanup_clean and channel_clean:
                    if getattr(self, "_draining", False):
                        self.status.advance(
                            DeploymentState.STOPPED, reason_code="DRAINED_AND_REAPED"
                        )
                    else:
                        self.status.advance(
                            DeploymentState.CANCELLED, reason_code="SHUTDOWN_BEFORE_READY"
                        )
                else:
                    self.status.advance(
                        DeploymentState.FAILED,
                        reason_code="CLEANUP_INCOMPLETE",
                        detail=str(self.first_cause or "cleanup incomplete")[:400],
                    )
                terminal_publication = "published"
            except (StatusPublicationError, RuntimeError, ValueError) as exc:
                terminal_publication = f"failed: {type(exc).__name__}: {exc}"
                cleanup_failed(f"terminal status publication failed: {exc}")

        try:
            atomic_write_json(report_path, shutdown_payload())
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            # The conservative pre-transition report remains on disk. Preserve
            # the failure causally even though a terminal state may already be
            # durable in the independent status record.
            cleanup_failed(f"final shutdown report publication failed: {exc}")

    def exit_code(self) -> int:
        """Typed exit. A requested shutdown is 143, not a generic failure."""
        supervised = self.supervisor.exit_code()
        if self.first_cause is not None:
            # Exit status 143 is ambiguous by itself: it can mean either an
            # operator-requested drain or an owned child that unexpectedly
            # died from SIGTERM.  Reserve 143 for the typed shutdown cause and
            # keep an unexpected child signal in the failure class.  The raw
            # child signal remains available in FirstCause and component
            # evidence, so this remapping loses no diagnostic information.
            if self._operator_shutdown_requested():
                return 143
            return 1 if supervised in (0, 143) else supervised
        return supervised

    def _operator_shutdown_requested(self) -> bool:
        """Use the typed supervisor cause, never an exit-code/string heuristic."""
        cause = self.supervisor.first_cause
        return cause is not None and cause.reason_code == "SHUTDOWN_REQUESTED"


def _is_requested_shutdown(reason: str) -> bool:
    """Parse a serialized typed cause at compatibility/test boundaries only."""
    text = str(reason).upper()
    return "SHUTDOWN_REQUESTED" in text


def read_nodefile(
    path: Optional[str] = None, *, cancel_requested: Optional[Callable[[], bool]] = None
) -> list:
    """Discover the allocation.

    The Python launcher normally exports EXASERVE_NODEFILE after validating the
    scheduler environment. Direct library callers may still supply a path, so
    scheduler-native variables remain supported at this narrow boundary.
    """
    explicit = path or os.environ.get("EXASERVE_NODEFILE")
    if explicit:
        if not os.path.isfile(explicit) or not os.access(explicit, os.R_OK):
            raise CompositionError(f"no nodefile found at explicit path {explicit!r}")
        path = explicit
    else:
        native = os.environ.get("PBS_NODEFILE")
        if native:
            if not os.path.isfile(native) or not os.access(native, os.R_OK):
                raise CompositionError(f"no nodefile found at PBS_NODEFILE {native!r}")
            path = native
    if not path:
        nodelist = os.environ.get("SLURM_JOB_NODELIST")
        if nodelist:
            from .control.finite_process import FiniteProcessError, run_finite

            try:
                out = run_finite(
                    ["scontrol", "show", "hostnames", nodelist],
                    timeout_s=30.0,
                    cancel_requested=cancel_requested,
                )
            except (OSError, FiniteProcessError) as exc:
                raise CompositionError(f"could not expand SLURM_JOB_NODELIST: {exc}") from exc
            if out.returncode != 0:
                detail = out.stderr.strip() or out.stdout.strip()
                raise CompositionError(
                    f"scontrol host expansion failed ({out.returncode}): {detail[:400]}"
                )
            nodes = [n.strip() for n in out.stdout.splitlines() if n.strip()]
            if nodes:
                return list(dict.fromkeys(nodes))
        raise CompositionError(
            "no nodefile found (checked EXASERVE_NODEFILE, PBS_NODEFILE, "
            "SLURM_JOB_NODELIST); cannot bind an allocation"
        )
    from .state.atomic import regular_file_reader

    with regular_file_reader(path) as handle:
        nodes = [line.strip() for line in handle if line.strip()]
    unique: list = []
    for node in nodes:
        if node not in unique:
            unique.append(node)
    if not unique:
        raise CompositionError(f"nodefile {path} is empty")
    return unique
