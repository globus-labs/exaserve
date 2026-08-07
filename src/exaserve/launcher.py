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

LEGACY_ENTRY_ENV = "EXASERVE_LEGACY_SHELL_LIFECYCLE"


def _log(message: str) -> None:
    print(message, flush=True)


def load_or_compile_plan(config_path: str, *, deployment_id: str):
    """One canonical DeploymentPlan. Downstream never recompiles it.

    The normal scheduler/eval path persists the compiled artifact before
    submission and this verifies its hash; a direct manual launch invokes the
    one-way compiler exactly once, here.
    """
    import json

    from .plan.compiler import compile_deployment_plan
    from .plan.contracts import PlanError

    if config_path.endswith(".plan.json") and os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as handle:
            payload = json.load(handle)
        declared = payload.get("deployment_plan_hash", "")
        plan = plan_from_dict(payload)
        if declared and plan.deployment_plan_hash != declared:
            raise PlanError(
                f"compiled plan hash mismatch: artifact declares "
                f"{declared[:12]}, recomputed {plan.deployment_plan_hash[:12]}")
        return plan

    from .schemas import require_yaml
    from .site import default_site_profile

    yaml = require_yaml()
    with open(config_path, encoding="utf-8") as handle:
        raw = yaml.safe_load(handle) or {}
    return compile_deployment_plan(raw, site=default_site_profile(),
                                   deployment_id=deployment_id)


def plan_from_dict(payload: dict):
    """Rehydrate a persisted plan without reinterpreting configuration."""
    from .plan.contracts import (
        ControlLimits,
        DeploymentPlan,
        ExposurePlan,
        GatewayPlan,
        ModelPlan,
        ReceiptRequirement,
    )

    gateway = payload.get("gateway")
    return DeploymentPlan(
        schema_version=int(payload["schema_version"]),
        deployment_id=str(payload["deployment_id"]),
        site_profile_id=str(payload["site_profile_id"]),
        site_profile_hash=str(payload["site_profile_hash"]),
        compatibility_profile_hash=str(payload.get("compatibility_profile_hash", "")),
        manifest_hash=str(payload.get("manifest_hash", "")),
        num_nodes=int(payload["num_nodes"]),
        num_gpus_per_node=int(payload["num_gpus_per_node"]),
        vendor=str(payload["vendor"]), engine=str(payload["engine"]),
        model_storage_path=str(payload["model_storage_path"]),
        local_stage_path=str(payload["local_stage_path"]),
        models=tuple(ModelPlan(**m) for m in payload["models"]),
        exposure=ExposurePlan(**payload["exposure"]),
        gateway=(GatewayPlan(kind=gateway["kind"], port=gateway["port"],
                             options=tuple(tuple(o) for o in gateway.get("options", ())))
                 if gateway else None),
        receipt_requirements=tuple(ReceiptRequirement(**r)
                                   for r in payload["receipt_requirements"]),
        control=ControlLimits(**payload["control"]),
        validation_mode=bool(payload.get("validation_mode", False)),
    ).finalize()


def _resolve_run_dir(generation: int, config_path: str) -> str:
    """The root owns its run directory rather than inheriting one or using cwd.

    The shell used to compute `<root>/<stamp>_<config>`; when the root took
    over it fell back to the working directory, so durable artifacts landed
    wherever the process happened to start.
    """
    explicit = os.environ.get("EXASERVE_RUN_LOG_DIR")
    if explicit:
        return explicit
    root = os.environ.get("EXASERVE_RUN_LOG_ROOT")
    if not root:
        return os.getcwd()
    stem = os.path.splitext(os.path.basename(config_path))[0]
    run_dir = os.path.join(root, f"gen{generation}_{stem}")
    os.makedirs(run_dir, exist_ok=True)
    os.environ["EXASERVE_RUN_LOG_DIR"] = run_dir
    return run_dir


def run(config_path: str) -> int:
    """Own one deployment generation end to end."""
    from .composition import CompositionError, CompositionRoot, read_nodefile

    deployment_id = (os.environ.get("EXASERVE_DEPLOYMENT_ID")
                     or os.environ.get("EXASERVE_JOBID", "local")).split(".")[0][:40]
    generation = int(os.environ.get("EXASERVE_GENERATION", "0") or int(time.time()))
    run_dir = _resolve_run_dir(generation, config_path)

    try:
        plan = load_or_compile_plan(config_path, deployment_id=deployment_id)
    except Exception as exc:              # noqa: BLE001 - typed first cause
        _log(f"[Composition] FIRST CAUSE: plan compilation failed: {exc}")
        return 2
    _log(f"[Composition] plan {plan.deployment_plan_hash[:12]} "
         f"({plan.num_nodes} node(s), exposure {plan.exposure.mode})")

    # Compatibility activation happens BEFORE any Ray/engine import.
    try:
        from .compat.activator import CompatibilityActivator

        activator = CompatibilityActivator(deployment_id=deployment_id,
                                           generation=generation)
        os.environ["EXASERVE_COMPAT_PROFILE_ID"] = activator.profile.profile_id
        _log(f"[Composition] compatibility profile {activator.profile.name} "
             f"({activator.profile.profile_id[:12]})")
    except Exception as exc:              # noqa: BLE001
        _log(f"[Composition] FIRST CAUSE: compatibility activation failed: {exc}")
        return 2

    root = CompositionRoot(plan=plan, generation=generation, run_dir=run_dir,
                           log=_log)
    try:
        root.bind_allocation(read_nodefile(),
                             os.environ.get("EXASERVE_JOBID", "local"))
        root.bind_control_listener()
        rank_argv = [sys.executable, "-m", "exaserve.rank_main",
                     "--config", config_path]
        component = root.launch_ranks(
            rank_argv, scheduler=os.environ.get("EXASERVE_SCHEDULER", "pbs"))
        root.await_all_registered()

        # §3.2.1 Q3: the ROOT owns the advertised endpoint and commits READY.
        # Previously the deployment child's own gate decided readiness against
        # the internal Serve endpoint, so the gateway contract was never
        # exercised even on a PROXIED_INTERNAL plan.
        _drive_readiness(root, config_path)

        cause = root.supervisor.supervise(
            until=lambda: component.process is not None
            and component.process.poll() is not None)
        if cause is not None:
            root.fail(str(cause))
    except CompositionError as exc:
        root.fail(str(exc))
    except Exception as exc:              # noqa: BLE001
        root.fail(f"{type(exc).__name__}: {exc}")
    finally:
        root.shutdown(drain_s=30.0)

    code = root.exit_code()
    if root.first_cause:
        _log(f"[Composition] exit {code}: {root.first_cause}")
    return code


def _drive_readiness(root, config_path: str) -> None:
    """Deploy -> VALIDATING -> gateway -> verify -> persist one READY."""
    from .composition import CompositionError

    if not root.await_deployment_serving():
        raise CompositionError("the deployment never reported its applications running")

    readiness = root.build_readiness()
    endpoint = root.establish_advertised_endpoint()

    if root.plan.gateway is not None:
        argv = root.gateway_argv(root.run_dir)
        try:
            root.start_gateway(argv)
        except (OSError, FileNotFoundError) as exc:
            raise CompositionError(
                f"gateway {root.plan.gateway.kind} could not start: {exc}") from exc
        readiness.set_gateway(alive=root.gateway_alive(), healthy=True)

    # The GLOBAL slots enter from this process or not at all; the gateway slot
    # needs a live pid, so it is issued after the gateway starts.
    issued = root.attest_global()
    _log(f"[Composition] GLOBAL receipts issued: {issued}")
    _log_receipt_evidence(root)

    # Replica evidence comes from the deployment child's snapshot; the root
    # verifies the ENDPOINT itself rather than trusting that report. The child
    # writes a DIFFERENT filename on purpose -- readiness.json is the root's
    # single READY record, and one file cannot be both the evidence and the
    # decision without one silently overwriting the other.
    import json

    from .control.serve_readiness import EVIDENCE_FILENAME

    try:
        with open(os.path.join(root.run_dir, EVIDENCE_FILENAME),
                  encoding="utf-8") as fh:
            evidence = json.load(fh)
    except (OSError, ValueError):
        evidence = {}
    applications = evidence.get("applications") or {}
    for model in root.plan.models:
        app = _application_for(model, applications, root.plan.models)
        running = int((app or {}).get("running", 0) or 0)
        target = int((app or {}).get("target", 0) or 0) or (model.num_replicas or 0)
        readiness.set_replicas(model.model_id, running, target)
        # The route is healthy when the child observed its application RUNNING;
        # asserting it unconditionally would make the route check decorative.
        readiness.set_route(model.route_name,
                            str((app or {}).get("status", "")).upper() == "RUNNING")
        ok, detail = root.canary_advertised_endpoint(model)
        readiness.set_canary(model.model_id, ok)
        if not ok:
            _log(f"[Composition] canary via {endpoint} failed: {detail}")

    verdict = readiness.commit_ready(root.run_dir)
    if verdict.ready:
        # Only now does the SHARED record say READY: the durable verdict is
        # already on disk, so no consumer can see READY ahead of its evidence.
        root.publish_ready(endpoint)
        _log(f"[Composition] READY via {endpoint} — {list(verdict.satisfied)}")
    else:
        raise CompositionError(
            f"readiness not satisfied via the advertised endpoint: "
            f"{list(verdict.blockers)}")


def _application_for(model, applications: dict, models) -> Optional[dict]:
    """Match a planned model to the Serve application the child observed.

    Serve does not name applications after model ids — a single-model
    deployment is just `default` — so matching on the model id found nothing
    and every model resolved to a zero replica target, which reads as "the
    deployment is empty" for a deployment that is fully up.
    """
    if not applications:
        return None
    if len(models) == 1 and len(applications) == 1:
        return next(iter(applications.values()))
    for name, info in applications.items():
        prefix = str(info.get("route_prefix", "")).strip("/")
        if model.route_name in (name, prefix):
            return info
    for name, info in applications.items():
        if model.model_id in str(name):
            return info
    return None


def _log_receipt_evidence(root) -> None:
    """Report what arrived that is NOT an exactly-planned slot.

    Replicas and engine cores are placed at runtime, so the plan cannot name
    them and they are not ledger slots. They still travel the authenticated
    path, and naming the roles here is what makes "the engine attested itself"
    an observable fact rather than an assumption.
    """
    head = getattr(root, "head_channel", None)
    if head is None:
        return
    roles: dict = {}
    for _, payload in getattr(head, "evidence_receipts", []):
        role = str(payload.get("role", "?"))
        roles[role] = roles.get(role, 0) + 1
    _log(f"[Composition] evidence receipts: {sum(roles.values())} "
         f"{dict(sorted(roles.items()))}")
    rejected = getattr(head, "receipt_rejections", [])
    if rejected:
        _log(f"[Composition] receipts rejected: {len(rejected)} — {rejected[:4]}")


def use_supervisor() -> bool:
    """There is exactly one lifecycle owner now (WP13).

    Kept as a predicate because callers and tests ask the question; it no
    longer has a false branch to return.
    """
    return True


def main(argv: Optional[Sequence[str]] = None) -> None:
    args = list(argv if argv is not None else sys.argv[1:])
    if not args:
        raise SystemExit("usage: exaserve-launch-cluster <config>")
    config_path = args[0]

    # WP13: the legacy shell-lifecycle branch is gone. It could not have worked
    # anyway -- it exec'd `bash launch_cluster.sh`, which since the P04 cutover
    # execs straight back into this module with the same environment, so the
    # flag it advertised was an infinite exec loop rather than the run-to-run
    # comparison its comment promised.
    if os.environ.get(LEGACY_ENTRY_ENV) == "1":
        _log(f"[Composition] {LEGACY_ENTRY_ENV} is no longer supported; "
             "the shell is a site adapter and Python owns the lifecycle")
    raise SystemExit(run(config_path))


if __name__ == "__main__":
    main()
