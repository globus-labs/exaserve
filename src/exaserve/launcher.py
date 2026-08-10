"""Import-light Python composition root (plan §3.2.1, packet P04, IMP-H03).

The site adapter `exec`s into this and nothing else. Previously this module
supervised `bash launch_cluster.sh`, which meant the shell still owned staging,
distribution, Copper, logs, collection, cleanup and the launch — a Python
supervisor whose only child was a lifecycle-owning shell.

It stays *import-light* on purpose: it boots from the clean packaged artifact
on the shared filesystem, activates the compatibility profile **before** any
Ray or engine import, and only then constructs the supervisor. Ray, vLLM and
the deployment machinery are imported lazily inside `run()` for that reason.
"""

from __future__ import annotations

import os
import sys
import time
from typing import Optional, Sequence


def _log(message: str) -> None:
    print(message, flush=True)


def load_or_compile_plan(config_path: str, *, deployment_id: str):
    """One canonical DeploymentPlan. Downstream never recompiles it.

    The normal scheduler/eval path persists the compiled artifact before
    submission and this verifies its hash; a direct manual launch invokes the
    one-way compiler exactly once, here.
    """
    from .plan.compiler import compile_deployment_plan
    from .plan.io import load_deployment_plan

    if config_path.endswith(".plan.json") and os.path.exists(config_path):
        return load_deployment_plan(config_path)

    from .site import default_site_profile
    from .yaml_support import load_yaml_mapping

    raw = load_yaml_mapping(config_path)
    return compile_deployment_plan(raw, site=default_site_profile(), deployment_id=deployment_id)


def plan_from_dict(payload: dict):
    """Compatibility wrapper around the sole strict artifact loader."""
    from .plan.io import deployment_plan_from_dict

    return deployment_plan_from_dict(payload)


def resolve_site_profile(plan, config_path: str):
    """Resolve and verify the separate SiteProfile artifact for ``plan``.

    Scheduler/eval runs persist ``site.profile.json`` next to their compiled
    plan.  The one-way direct-launch adapter may use the installed site's
    registered default, but only when its recomputed hash is exactly the hash
    embedded in the DeploymentPlan.  There is no ambient-value fallback.
    """
    from .plan.io import load_site_profile
    from .site import default_site_profile

    explicit = os.environ.get("EXASERVE_SITE_PROFILE_PATH", "").strip()
    adjacent = os.path.join(os.path.dirname(os.path.abspath(config_path)), "site.profile.json")
    if explicit:
        profile = load_site_profile(explicit)
    elif config_path.endswith(".plan.json") and os.path.isfile(adjacent):
        profile = load_site_profile(adjacent)
    else:
        profile = default_site_profile(plan.site_profile_id)
    if profile.site_id != plan.site_profile_id:
        raise ValueError(
            f"SiteProfile id {profile.site_id!r} does not match plan {plan.site_profile_id!r}"
        )
    if profile.site_profile_hash != plan.site_profile_hash:
        raise ValueError(
            "SiteProfile hash does not match DeploymentPlan: "
            f"profile={profile.site_profile_hash}, plan={plan.site_profile_hash}"
        )
    if profile.environment_profile_ref != plan.compatibility_profile_hash:
        raise ValueError(
            "SiteProfile prepared environment references a different "
            "compatibility profile than the DeploymentPlan"
        )
    return profile


def _resolve_run_dir(generation: int, config_path: str) -> str:
    """The root owns its run directory rather than inheriting one or using cwd.

    The shell used to compute `<root>/<stamp>_<config>`; when the root took
    over it fell back to the working directory, so durable artifacts landed
    wherever the process happened to start.
    """
    explicit = os.environ.get("EXASERVE_RUN_LOG_DIR")
    if explicit:
        run_dir = explicit
    else:
        root = os.environ.get("EXASERVE_RUN_LOG_ROOT") or os.path.join(os.getcwd(), "run_logs")
        stem = os.path.splitext(os.path.basename(config_path))[0]
        run_dir = os.path.join(root, f"gen{generation}_{stem}")
    from .state.atomic import ensure_owned_directory

    run_dir = ensure_owned_directory(run_dir)
    os.environ["EXASERVE_RUN_LOG_DIR"] = run_dir
    return run_dir


def _scheduler_allocation_id(scheduler: str | None = None) -> str:
    """Resolve one native identity, permitting an exact validation alias.

    ``EXASERVE_JOBID`` is propagated to children after validation; it is not
    allowed to replace a different native scheduler identity.  Doing so would
    bind durable state to an invented allocation and could let a future
    production-qualified plan cross the wrong scheduler boundary.
    """
    selected = (scheduler or os.environ.get("EXASERVE_SCHEDULER", "")).strip().lower()
    if selected and selected not in {"pbs", "slurm"}:
        raise ValueError(f"unsupported scheduler identity {selected!r}")
    pbs_id = os.environ.get("PBS_JOBID", "").strip()
    slurm_id = os.environ.get("SLURM_JOB_ID", "").strip()
    explicit = os.environ.get("EXASERVE_JOBID", "").strip()
    if pbs_id and slurm_id:
        raise ValueError(
            "both PBS_JOBID and SLURM_JOB_ID are set; allocation identity is ambiguous"
        )
    if not selected:
        selected = "pbs" if pbs_id else ("slurm" if slurm_id else "")
    if selected == "pbs" and slurm_id:
        raise ValueError("EXASERVE_SCHEDULER=pbs disagrees with the native Slurm allocation")
    if selected == "slurm" and pbs_id:
        raise ValueError("EXASERVE_SCHEDULER=slurm disagrees with the native PBS allocation")
    native = pbs_id if selected == "pbs" else (slurm_id if selected == "slurm" else "")
    if native:
        if explicit and explicit != native:
            raise ValueError(
                f"EXASERVE_JOBID {explicit!r} disagrees with native {selected} identity {native!r}"
            )
        return native
    if explicit:
        return explicit
    raise ValueError(
        "no scheduler allocation identity is available; set EXASERVE_JOBID "
        "only for an explicit local validation allocation"
    )


def _validate_runtime_scheduler(plan, site_profile) -> None:
    """Bind the live scheduler to the immutable site/envelope contract."""
    scheduler = os.environ.get("EXASERVE_SCHEDULER", "").strip().lower()
    if scheduler not in site_profile.scheduler_types:
        raise ValueError(
            f"runtime scheduler {scheduler!r} is not supported by SiteProfile "
            f"{site_profile.site_id!r}"
        )
    if scheduler != plan.scale_envelope.scheduler_type:
        raise ValueError(
            f"runtime scheduler {scheduler!r} disagrees with scale envelope "
            f"{plan.scale_envelope.envelope_id!r} scheduler "
            f"{plan.scale_envelope.scheduler_type!r}"
        )
    native_id = (
        os.environ.get("PBS_JOBID", "").strip()
        if scheduler == "pbs"
        else os.environ.get("SLURM_JOB_ID", "").strip()
    )
    if not native_id and not plan.validation_mode:
        raise ValueError(
            "a production deployment requires the native scheduler job identity; "
            "EXASERVE_JOBID alone is permitted only for validation_mode"
        )


def _deployment_done_or_failed(root) -> bool:
    """Classify typed non-process failures for the supervisor loop."""

    channel_failure = root.head_channel.poll()
    if channel_failure:
        root.supervisor.record_cause("control", "CONTROL_FAILURE", channel_failure)
        return True
    readiness_failure = root.monitor_readiness()
    if readiness_failure is not None:
        root.supervisor.record_cause(
            readiness_failure.component_id,
            readiness_failure.reason_code,
            readiness_failure.detail,
            readiness_failure.exit_code,
        )
        return True
    # Process termination is owned and classified by RuntimeSupervisor.poll_once().
    return False


def _prepare_scheduler_environment() -> str:
    """Perform the former shell adapter's allocation preflight in Python."""
    explicit = os.environ.get("EXASERVE_SCHEDULER", "").strip().lower()
    if explicit and explicit not in {"pbs", "slurm"}:
        raise ValueError(f"unsupported EXASERVE_SCHEDULER {explicit!r}")
    has_pbs = bool(os.environ.get("PBS_JOBID", "").strip())
    has_slurm = bool(os.environ.get("SLURM_JOB_ID", "").strip())
    if not explicit:
        if has_pbs == has_slurm:
            raise ValueError(
                "cannot infer exactly one scheduler from PBS_JOBID/SLURM_JOB_ID; "
                "set EXASERVE_SCHEDULER explicitly"
            )
        explicit = "pbs" if has_pbs else "slurm"
    os.environ["EXASERVE_SCHEDULER"] = explicit

    nodefile = os.environ.get("EXASERVE_NODEFILE", "").strip()
    if not nodefile and explicit == "pbs":
        nodefile = os.environ.get("PBS_NODEFILE", "").strip()
        if nodefile:
            os.environ["EXASERVE_NODEFILE"] = nodefile
    if nodefile and not os.path.isfile(nodefile):
        raise ValueError(f"allocation nodefile is not readable: {nodefile}")
    if explicit == "pbs" and not nodefile:
        raise ValueError("PBS allocation requires EXASERVE_NODEFILE or PBS_NODEFILE")
    if (
        explicit == "slurm"
        and not nodefile
        and not os.environ.get("SLURM_JOB_NODELIST", "").strip()
    ):
        raise ValueError("Slurm allocation requires EXASERVE_NODEFILE or SLURM_JOB_NODELIST")

    allocation_id = _scheduler_allocation_id(explicit)
    os.environ["EXASERVE_JOBID"] = allocation_id
    return allocation_id


def _sanitize_child_pythonpath() -> None:
    """Remove retired generation overlays from every process we launch."""
    retained = []
    for entry in os.environ.get("PYTHONPATH", "").split(os.pathsep):
        if not entry:
            continue
        normalized = os.path.normpath(entry)
        if normalized == "/tmp/exaserve_src" or normalized.startswith("/tmp/exaserve_src."):
            continue
        if normalized == "/tmp/exaserve_overlay" or normalized.startswith("/tmp/exaserve_overlay."):
            continue
        retained.append(entry)
    if retained:
        os.environ["PYTHONPATH"] = os.pathsep.join(retained)
    else:
        os.environ.pop("PYTHONPATH", None)


def _generation_from_environment() -> int:
    raw = os.environ.get("EXASERVE_GENERATION", "").strip()
    if not raw:
        return time.time_ns()
    try:
        generation = int(raw)
    except ValueError as exc:
        raise ValueError("EXASERVE_GENERATION must be a nonnegative integer") from exc
    if generation < 0:
        raise ValueError("EXASERVE_GENERATION must be a nonnegative integer")
    return generation


def run(config_path: str) -> int:
    """Own one deployment generation end to end."""
    from .composition import CompositionError, CompositionRoot, read_nodefile

    try:
        _sanitize_child_pythonpath()
        scheduler_allocation_id = _prepare_scheduler_environment()
        generation = _generation_from_environment()
    except ValueError as exc:
        _log(f"[Composition] FIRST CAUSE: allocation identity failed: {exc}")
        return 2
    deployment_id = (os.environ.get("EXASERVE_DEPLOYMENT_ID") or scheduler_allocation_id).split(
        "."
    )[0][:40]
    run_dir = _resolve_run_dir(generation, config_path)

    try:
        plan = load_or_compile_plan(config_path, deployment_id=deployment_id)
        site_profile = resolve_site_profile(plan, config_path)
        _validate_runtime_scheduler(plan, site_profile)
        from .site import require_execution_qualification

        production_qualified = require_execution_qualification(plan, site_profile)
    except Exception as exc:  # noqa: BLE001 - typed first cause
        _log(f"[Composition] FIRST CAUSE: plan compilation failed: {exc}")
        return 2
    _log(
        f"[Composition] plan {plan.deployment_plan_hash[:12]} "
        f"({plan.num_nodes} node(s), exposure {plan.exposure.mode})"
    )
    os.environ["EXASERVE_DEPLOYMENT_ID"] = plan.deployment_id
    os.environ["EXASERVE_GENERATION"] = str(generation)

    # Even a direct legacy launch crosses the runtime boundary through an
    # immutable canonical artifact. Ranks and deployment code never reopen the
    # source YAML.
    from .plan.io import write_deployment_plan, write_site_profile

    plan_path = os.path.join(run_dir, "deployment.plan.json")
    site_profile_path = os.path.join(run_dir, "site.profile.json")
    try:
        from .state.atomic import ensure_owned_directory

        ensure_owned_directory(run_dir)
        write_deployment_plan(plan_path, plan)
        write_site_profile(site_profile_path, site_profile)
    except Exception as exc:  # noqa: BLE001 - artifact boundary
        _log(f"[Composition] FIRST CAUSE: plan persistence failed: {exc}")
        return 2

    # Site preparation is applied from content-addressed data, before
    # compatibility activation and before any Ray/engine import.  Every child
    # receives the same verified profile artifact path and prepared values.
    try:
        from .site import apply_site_profile_environment

        apply_site_profile_environment(site_profile)
        os.environ["EXASERVE_SITE_PROFILE_PATH"] = site_profile_path
    except Exception as exc:  # noqa: BLE001
        _log(f"[Composition] FIRST CAUSE: site environment preparation failed: {exc}")
        return 2

    # Compatibility activation happens BEFORE any Ray/engine import.
    try:
        from .compat.activator import CompatibilityActivator

        activator = CompatibilityActivator(
            deployment_id=plan.deployment_id,
            generation=generation,
        )
        activator.activate("supervisor", apply_fn=lambda: None)
        os.environ["EXASERVE_COMPAT_PROFILE_ID"] = activator.profile.profile_id
        _log(
            f"[Composition] compatibility profile {activator.profile.name} "
            f"({activator.profile.profile_id[:12]})"
        )
    except Exception as exc:  # noqa: BLE001
        _log(f"[Composition] FIRST CAUSE: compatibility activation failed: {exc}")
        return 2

    root = CompositionRoot(
        plan=plan,
        generation=generation,
        run_dir=run_dir,
        log=_log,
        production_qualified=production_qualified,
    )
    try:
        # Signal ownership precedes every process boundary, including gateway
        # config preflight and source/model staging. Synchronous phase waits
        # consume the same typed supervisor cause and unwind their owned group.
        root.supervisor.install_signal_handlers()
        root.bind_allocation(
            read_nodefile(cancel_requested=root.termination_requested),
            scheduler_allocation_id,
        )
        root.raise_if_termination("allocation binding interrupted")
        # The production HAProxy path uses exact listening-FD inheritance.
        # Bind and retain that FD as soon as the allocation topology is known:
        # a conflicting listener must fail before source staging, Ray startup,
        # or model load, and ownership must remain continuous until HAProxy
        # inherits the socket.  shutdown() releases a prepared listener on
        # every failure path, even when no gateway child was started.
        prepared_gateway_argv = root.gateway_argv(root.run_dir)
        root.raise_if_termination("gateway preparation interrupted")
        root.bind_control_listener()
        root.run_staging(root.default_staging_steps(plan_path))
        root.raise_if_termination("staging interrupted")
        rank_argv = [sys.executable, "-m", "exaserve.rank_main", "--plan", plan_path]
        root.launch_ranks(rank_argv, scheduler=os.environ.get("EXASERVE_SCHEDULER", "pbs"))
        root.await_all_registered()
        root.start_ray_cluster(plan_path)
        root.start_deployment(plan_path)

        # §3.2.1 Q3: the ROOT owns the advertised endpoint and commits READY.
        # Previously the deployment child's own gate decided readiness against
        # the internal Serve endpoint, so the gateway contract was never
        # exercised even on a PROXIED_INTERNAL plan.
        _drive_readiness(root, prepared_gateway_argv=prepared_gateway_argv)

        cause = root.supervisor.supervise(until=lambda: _deployment_done_or_failed(root))
        # ``until`` is allowed to stop supervision normally, so the generic
        # supervisor returns ``None`` when its predicate becomes true.  This
        # launcher's predicate, however, becomes true only after recording a
        # typed control/readiness/gateway cause.  Recover that cause from the
        # supervisor before entering cleanup; otherwise a rank-control failure
        # can be durably relabelled READY -> DRAINING -> STOPPED even though the
        # launcher exits nonzero.
        cause = cause or root.supervisor.first_cause
        if cause is not None:
            root.fail(str(cause))
    except CompositionError as exc:
        root.fail(str(exc))
    except Exception as exc:  # noqa: BLE001
        root.fail(f"{type(exc).__name__}: {exc}")
    finally:
        root.shutdown(drain_s=plan.control.watchdog_cleanup_deadline_s)

    code = root.exit_code()
    if root.first_cause:
        _log(f"[Composition] exit {code}: {root.first_cause}")
    return code


def _drive_readiness(root, *, prepared_gateway_argv: Optional[list[str]]) -> None:
    """Deploy -> VALIDATING -> gateway -> verify -> persist one READY."""
    from .composition import CompositionError

    if not root.await_deployment_serving():
        raise CompositionError("the deployment never reported its applications running")

    readiness = root.build_readiness()
    endpoint = root.establish_advertised_endpoint()

    if root.plan.gateway is not None:
        if prepared_gateway_argv is None:
            raise CompositionError(
                "gateway was not prepared before source staging and deployment startup"
            )
        try:
            root.start_gateway(prepared_gateway_argv, env=root.gateway_environment())
        except (OSError, FileNotFoundError) as exc:
            raise CompositionError(
                f"gateway {root.plan.gateway.kind} could not start: {exc}"
            ) from exc
        healthy = root.gateway_health_check()
        readiness.set_gateway(alive=root.gateway_alive(), healthy=healthy)
        if not healthy:
            raise CompositionError("gateway process did not become healthy on its planned port")

    # The GLOBAL slots enter from this process or not at all; the gateway slot
    # needs a live pid, so it is issued after the gateway starts.
    issued = root.attest_global()
    _log(f"[Composition] GLOBAL receipts issued: {issued}")
    _log_receipt_evidence(root)

    # Replica/route evidence is fresh on the head's monotonic receive clock
    # and arrived through authenticated rank sessions. Applications, receipts,
    # per-node proxies, and canaries are independent streams, so retain the
    # exact predicate while they converge within the resolved deadline.
    verdict = root.await_initial_readiness()
    # READY and the evidence that proves it are one CAS-guarded status write.
    # Only after that durable publication do we commit the in-memory phase and
    # render a human-readable line. No control path parses this text.
    root.publish_ready(verdict)
    committed = readiness.commit_ready()
    if not committed.ready:
        raise CompositionError("readiness predicate changed during READY publication")
    _log(f"[Composition] READY via {endpoint} — {list(committed.satisfied)}")


def _log_receipt_evidence(root) -> None:
    """Report what arrived that is NOT an exactly-planned slot.

    The final plan names replica, engine-core, worker, daemon, supervisor, and
    gateway slots exactly. This diagnostic reports only authenticated payloads
    that were intentionally classified as non-authoritative evidence.
    """
    head = getattr(root, "head_channel", None)
    if head is None:
        return
    roles: dict = {}
    for _, payload in getattr(head, "evidence_receipts", []):
        role = str(payload.get("role", "?"))
        roles[role] = roles.get(role, 0) + 1
    _log(f"[Composition] evidence receipts: {sum(roles.values())} {dict(sorted(roles.items()))}")
    rejected = getattr(head, "receipt_rejections", [])
    if rejected:
        _log(f"[Composition] receipts rejected: {len(rejected)} — {rejected[:4]}")


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) != 1:
        raise SystemExit("usage: exaserve-launch-cluster <deployment.plan.json|config.yaml>")
    config_path = args[0]

    raise SystemExit(run(config_path))


if __name__ == "__main__":
    main()
