"""Strict persistence for canonical plan artifacts.

These files are immutable semantic inputs, not editable runtime config.  Loading
rehydrates the typed object and recomputes its hash before returning it.
"""

from __future__ import annotations

from dataclasses import asdict, fields
import json
import os
import stat
from typing import Any, Mapping

from ..state.atomic import strict_json_load, strict_json_loads

from .contracts import (
    SCHEMA_VERSION,
    AllocationBinding,
    ArtifactPolicy,
    BackendPolicy,
    ClientPolicy,
    ComponentInstanceBinding,
    ControlLimits,
    DeploymentPlan,
    ExposurePlan,
    GatewayPlan,
    ModelPlan,
    PlanError,
    ReadinessLimits,
    ReceiptRequirement,
    ReplicaPlan,
    RuntimePolicy,
    RunPlan,
    RunProvenance,
    SaturationPolicy,
    ScaleEnvelope,
    SchedulerPlan,
    SiteProfile,
    TracePolicy,
    WorkloadPolicy,
    same_node,
)


def _write_immutable_artifact(path: str, payload: Mapping[str, Any], kind: str) -> None:
    """Create one semantic artifact, permitting only an identical retry."""
    from ..state.atomic import atomic_create_json

    normalized = strict_json_loads(json.dumps(payload, allow_nan=False))
    canonical = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    try:
        atomic_create_json(path, normalized)
        return
    except FileExistsError:
        pass

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise PlanError(f"existing {kind} artifact is not safely readable: {exc}") from exc
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise PlanError(f"existing {kind} artifact is not a regular file")
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            fd = -1
            existing = strict_json_load(handle)
    except (OSError, ValueError) as exc:
        raise PlanError(f"existing {kind} artifact is invalid: {exc}") from exc
    finally:
        if fd >= 0:
            os.close(fd)
    # Python equality aliases JSON booleans and numbers (``True == 1``) and
    # integral floats and integers (``1.0 == 1``).  Those are different
    # immutable artifacts and, for identity fields, can change validation
    # semantics.  Compare canonical JSON encodings so an idempotent retry is
    # type-exact as well as value-exact.
    existing_canonical = json.dumps(
        existing,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if existing_canonical != canonical:
        raise PlanError(f"refusing to replace immutable {kind} artifact")


def _load_artifact(path: str, kind: str) -> Any:
    """Read one regular immutable input without following a mutable symlink."""
    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags)
    except OSError as exc:
        raise PlanError(f"could not load {kind} {path!r}: {exc}") from exc
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise PlanError(f"could not load {kind} {path!r}: artifact is not a regular file")
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            fd = -1
            return strict_json_load(handle)
    except (OSError, ValueError) as exc:
        raise PlanError(f"could not load {kind} {path!r}: {exc}") from exc
    finally:
        if fd >= 0:
            os.close(fd)


def _object(value: Any, cls, path: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PlanError(f"{path} must be a JSON object")
    result = dict(value)
    expected = {item.name for item in fields(cls)}
    unknown = sorted(set(result) - expected)
    missing = sorted(expected - set(result))
    if unknown or missing:
        raise PlanError(f"{path} shape mismatch: unknown={unknown}, missing={missing}")
    return result


def _array(value: Any, path: str) -> list[Any]:
    if not isinstance(value, list):
        raise PlanError(f"{path} must be a JSON array")
    return value


def _map(value: Any, path: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PlanError(f"{path} must be a JSON object")
    result = dict(value)
    if any(not isinstance(key, str) for key in result):
        raise PlanError(f"{path} must have string keys")
    return result


def _pairs(value: Any, path: str) -> tuple[tuple[str, Any], ...]:
    """Strict JSON representation of a deeply frozen semantic mapping."""
    items = _array(value, path)
    result: list[tuple[str, Any]] = []
    for index, item in enumerate(items):
        if not isinstance(item, list) or len(item) != 2 or not isinstance(item[0], str):
            raise PlanError(f"{path}[{index}] must be a [string, value] pair")
        result.append((item[0], item[1]))
    names = [name for name, _ in result]
    if len(names) != len(set(names)):
        raise PlanError(f"{path} contains duplicate keys")
    return tuple(result)


def deployment_plan_to_dict(plan: DeploymentPlan) -> dict[str, Any]:
    return asdict(plan)


def deployment_plan_from_dict(payload: Mapping[str, Any]) -> DeploymentPlan:
    if not isinstance(payload, Mapping):
        raise PlanError("compiled plan artifact must be a JSON object")
    payload = _object(payload, DeploymentPlan, "compiled plan artifact")
    if (
        type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != SCHEMA_VERSION
    ):
        raise PlanError(
            f"compiled plan schema {payload.get('schema_version')!r} is not "
            f"supported schema {SCHEMA_VERSION}"
        )
    declared_hash = payload.get("deployment_plan_hash")
    if not isinstance(declared_hash, str) or len(declared_hash) != 64:
        raise PlanError("compiled plan artifact lacks a valid deployment_plan_hash")

    models: list[ModelPlan] = []
    raw_models = _array(payload.get("models"), "compiled plan models")
    for index, raw_model in enumerate(raw_models):
        try:
            model = _object(raw_model, ModelPlan, f"compiled plan models[{index}]")
            replicas = _array(model.get("replicas"), f"compiled plan models[{index}].replicas")
            model["replicas"] = tuple(
                ReplicaPlan(
                    **_object(item, ReplicaPlan, f"compiled plan models[{index}].replicas[{j}]")
                )
                for j, item in enumerate(replicas)
            )
            models.append(ModelPlan(**model))
        except (TypeError, ValueError, KeyError, PlanError) as exc:
            raise PlanError(f"compiled plan models[{index}] invalid: {exc}") from None

    gateway_raw = payload.get("gateway")
    try:
        gateway = (
            None
            if gateway_raw is None
            else GatewayPlan(**_object(gateway_raw, GatewayPlan, "compiled plan gateway"))
        )
        plan = DeploymentPlan(
            schema_version=payload["schema_version"],
            deployment_id=payload["deployment_id"],
            site_profile_id=payload["site_profile_id"],
            site_profile_hash=payload["site_profile_hash"],
            compatibility_profile_hash=payload["compatibility_profile_hash"],
            manifest_hash=payload["manifest_hash"],
            scale_envelope=ScaleEnvelope(
                **_object(payload["scale_envelope"], ScaleEnvelope, "compiled plan scale_envelope")
            ),
            num_nodes=payload["num_nodes"],
            num_gpus_per_node=payload["num_gpus_per_node"],
            node_cpus=payload["node_cpus"],
            ray_port=payload["ray_port"],
            vendor=payload["vendor"],
            engine=payload["engine"],
            model_storage_path=payload["model_storage_path"],
            local_stage_path=payload["local_stage_path"],
            deployment_name=payload["deployment_name"],
            replica_max_ongoing_requests=payload["replica_max_ongoing_requests"],
            collect_stats=payload["collect_stats"],
            models=tuple(models),
            exposure=ExposurePlan(
                **_object(payload["exposure"], ExposurePlan, "compiled plan exposure")
            ),
            gateway=gateway,
            receipt_requirements=tuple(
                ReceiptRequirement(
                    **_object(
                        item, ReceiptRequirement, f"compiled plan receipt_requirements[{index}]"
                    )
                )
                for index, item in enumerate(
                    _array(payload["receipt_requirements"], "compiled plan receipt_requirements")
                )
            ),
            control=ControlLimits(
                **_object(payload["control"], ControlLimits, "compiled plan control")
            ),
            readiness=ReadinessLimits(
                **_object(payload["readiness"], ReadinessLimits, "compiled plan readiness")
            ),
            runtime=RuntimePolicy(
                **_object(payload["runtime"], RuntimePolicy, "compiled plan runtime")
            ),
            validation_mode=payload["validation_mode"],
        ).finalize()
    except (TypeError, ValueError, KeyError, PlanError) as exc:
        raise PlanError(f"compiled plan artifact invalid: {exc}") from None
    if plan.deployment_plan_hash != declared_hash:
        raise PlanError(
            f"compiled plan hash mismatch: artifact declares {declared_hash[:12]}, "
            f"recomputed {plan.deployment_plan_hash[:12]}"
        )
    return plan


def load_deployment_plan(path: str) -> DeploymentPlan:
    return deployment_plan_from_dict(_load_artifact(path, "compiled plan"))


def write_deployment_plan(path: str, plan: DeploymentPlan) -> None:
    if plan.deployment_plan_hash != plan.compute_hash():
        raise PlanError("refusing to write an unfinalized/tampered DeploymentPlan")
    _write_immutable_artifact(path, deployment_plan_to_dict(plan), "DeploymentPlan")


def site_profile_to_dict(profile: SiteProfile) -> dict[str, Any]:
    return asdict(profile)


def site_profile_from_dict(payload: Mapping[str, Any]) -> SiteProfile:
    """Strictly rehydrate and verify a separate content-addressed site artifact."""
    payload = _object(payload, SiteProfile, "site profile artifact")
    declared = payload.get("site_profile_hash")
    try:
        profile = SiteProfile(
            schema_version=payload["schema_version"],
            site_id=payload["site_id"],
            max_nodes=payload["max_nodes"],
            gpus_per_node=payload["gpus_per_node"],
            cpus_per_node=payload["cpus_per_node"],
            scheduler_types=tuple(_array(payload["scheduler_types"], "site.scheduler_types")),
            gateway_kinds=tuple(_array(payload["gateway_kinds"], "site.gateway_kinds")),
            vendors=tuple(_array(payload["vendors"], "site.vendors")),
            engines=tuple(_array(payload["engines"], "site.engines")),
            model_storage_path=payload["model_storage_path"],
            local_stage_path=payload["local_stage_path"],
            control=ControlLimits(**_object(payload["control"], ControlLimits, "site.control")),
            readiness=ReadinessLimits(
                **_object(payload["readiness"], ReadinessLimits, "site.readiness")
            ),
            launcher_capabilities=tuple(
                _array(payload["launcher_capabilities"], "site.launcher_capabilities")
            ),
            filesystem_semantics=tuple(
                (item[0], item[1])
                for item in _array(payload["filesystem_semantics"], "site.filesystem_semantics")
            ),
            accelerator_inventory=tuple(
                _array(payload["accelerator_inventory"], "site.accelerator_inventory")
            ),
            network_boundary=payload["network_boundary"],
            environment_profile_ref=payload["environment_profile_ref"],
            prepared_environment=tuple(
                (item[0], item[1])
                for item in _array(payload["prepared_environment"], "site.prepared_environment")
            ),
            environment_unset=tuple(_array(payload["environment_unset"], "site.environment_unset")),
            stack_size_kb=payload["stack_size_kb"],
            scale_envelopes=tuple(
                ScaleEnvelope(**_object(item, ScaleEnvelope, f"site.scale_envelopes[{index}]"))
                for index, item in enumerate(
                    _array(payload["scale_envelopes"], "site.scale_envelopes")
                )
            ),
        ).finalize()
    except (TypeError, ValueError, KeyError, IndexError, PlanError) as exc:
        raise PlanError(f"site profile artifact invalid: {exc}") from None
    if profile.site_profile_hash != declared:
        raise PlanError(
            f"site profile hash mismatch: artifact declares {str(declared)[:12]}, "
            f"recomputed {profile.site_profile_hash[:12]}"
        )
    return profile


def load_site_profile(path: str) -> SiteProfile:
    return site_profile_from_dict(_load_artifact(path, "site profile"))


def write_site_profile(path: str, profile: SiteProfile) -> None:
    if profile.site_profile_hash != profile.compute_hash():
        raise PlanError("refusing to write an unfinalized/tampered SiteProfile")
    _write_immutable_artifact(path, site_profile_to_dict(profile), "SiteProfile")


def run_plan_to_dict(plan: RunPlan) -> dict[str, Any]:
    return asdict(plan)


def run_plan_from_dict(payload: Mapping[str, Any]) -> RunPlan:
    """Strictly rehydrate the eval/ClientLab wrapper and both nested hashes."""
    payload = _object(payload, RunPlan, "run plan artifact")
    declared = payload.get("run_semantic_hash")
    try:
        scheduler = _object(payload["scheduler"], SchedulerPlan, "run.scheduler")
        workload = _object(payload["workload"], WorkloadPolicy, "run.workload")
        trace = _object(payload["trace"], TracePolicy, "run.trace")
        client = _object(payload["client"], ClientPolicy, "run.client")
        saturation = _object(client["saturation"], SaturationPolicy, "run.client.saturation")
        backend = _object(payload["backend"], BackendPolicy, "run.backend")
        artifacts = _object(payload["artifacts"], ArtifactPolicy, "run.artifacts")
        plan = RunPlan(
            schema_version=payload["schema_version"],
            run_id=payload["run_id"],
            deployment=deployment_plan_from_dict(
                _object(payload["deployment"], DeploymentPlan, "run.deployment")
            ),
            scheduler=SchedulerPlan(
                **{
                    **scheduler,
                    "resources": _pairs(scheduler["resources"], "run.scheduler.resources"),
                    "policy": _pairs(scheduler["policy"], "run.scheduler.policy"),
                    "filesystem_refs": tuple(
                        _array(scheduler["filesystem_refs"], "run.scheduler.filesystem_refs")
                    ),
                    "secret_refs": tuple(
                        _array(scheduler["secret_refs"], "run.scheduler.secret_refs")
                    ),
                }
            ),
            workload=WorkloadPolicy(
                **{
                    **workload,
                    "modes": _pairs(workload["modes"], "run.workload.modes"),
                }
            ),
            trace=TracePolicy(
                **{
                    **trace,
                    "generation_policy": _pairs(
                        trace["generation_policy"], "run.trace.generation_policy"
                    ),
                }
            ),
            client=ClientPolicy(
                **{
                    **client,
                    "dispatch_topologies": tuple(
                        _array(client["dispatch_topologies"], "run.client.dispatch_topologies")
                    ),
                    "saturation": SaturationPolicy(**saturation),
                    "options": _pairs(client["options"], "run.client.options"),
                }
            ),
            backend=BackendPolicy(
                **{
                    **backend,
                    "options": _pairs(backend["options"], "run.backend.options"),
                }
            ),
            artifacts=ArtifactPolicy(
                **{
                    **artifacts,
                    "expected_artifacts": tuple(
                        _array(artifacts["expected_artifacts"], "run.artifacts.expected_artifacts")
                    ),
                }
            ),
        ).finalize()
    except (TypeError, ValueError, KeyError, PlanError) as exc:
        raise PlanError(f"run plan artifact invalid: {exc}") from None
    if plan.run_semantic_hash != declared:
        raise PlanError(
            f"run plan hash mismatch: artifact declares {str(declared)[:12]}, "
            f"recomputed {plan.run_semantic_hash[:12]}"
        )
    return plan


def load_run_plan(path: str) -> RunPlan:
    return run_plan_from_dict(_load_artifact(path, "run plan"))


def write_run_plan(path: str, plan: RunPlan) -> None:
    if plan.deployment.deployment_plan_hash != plan.deployment.compute_hash():
        raise PlanError("refusing to write a run with a tampered DeploymentPlan")
    if plan.run_semantic_hash != plan.compute_hash():
        raise PlanError("refusing to write an unfinalized/tampered RunPlan")
    _write_immutable_artifact(path, run_plan_to_dict(plan), "RunPlan")


def allocation_binding_from_dict(payload: Mapping[str, Any]) -> AllocationBinding:
    if not isinstance(payload, Mapping):
        raise PlanError("allocation binding artifact must be a JSON object")
    payload = _object(payload, AllocationBinding, "allocation binding")
    declared = payload.get("allocation_binding_hash")
    try:
        binding = AllocationBinding(
            schema_version=payload["schema_version"],
            deployment_id=payload["deployment_id"],
            generation=payload["generation"],
            deployment_plan_hash=payload["deployment_plan_hash"],
            site_profile_hash=payload["site_profile_hash"],
            scheduler_allocation_id=payload["scheduler_allocation_id"],
            rank_to_node=tuple(
                (item[0], item[1])
                for item in _array(payload["rank_to_node"], "allocation binding rank_to_node")
            ),
        ).finalize()
    except (TypeError, ValueError, KeyError, IndexError) as exc:
        raise PlanError(f"allocation binding artifact invalid: {exc}") from None
    if binding.allocation_binding_hash != declared:
        raise PlanError("allocation binding hash mismatch")
    return binding


def load_allocation_binding(path: str) -> AllocationBinding:
    return allocation_binding_from_dict(_load_artifact(path, "allocation binding"))


def write_allocation_binding(path: str, binding: AllocationBinding) -> None:
    if binding.allocation_binding_hash != binding.compute_hash():
        raise PlanError("refusing to write an unfinalized/tampered AllocationBinding")
    _write_immutable_artifact(path, asdict(binding), "AllocationBinding")


def component_instance_binding_from_dict(
    payload: Mapping[str, Any],
) -> ComponentInstanceBinding:
    payload = _object(payload, ComponentInstanceBinding, "component instance binding")
    declared = payload.get("component_instance_binding_hash")
    try:
        binding = ComponentInstanceBinding(**payload).finalize()
    except (TypeError, ValueError, KeyError, PlanError) as exc:
        raise PlanError(f"component instance binding invalid: {exc}") from None
    if binding.component_instance_binding_hash != declared:
        raise PlanError("component instance binding hash mismatch")
    return binding


def load_component_instance_binding(path: str) -> ComponentInstanceBinding:
    return component_instance_binding_from_dict(_load_artifact(path, "component instance binding"))


def write_component_instance_binding(
    path: str,
    binding: ComponentInstanceBinding,
) -> None:
    if binding.component_instance_binding_hash != binding.compute_hash():
        raise PlanError("refusing to write an unfinalized/tampered ComponentInstanceBinding")
    _write_immutable_artifact(path, asdict(binding), "ComponentInstanceBinding")


def run_provenance_from_dict(payload: Mapping[str, Any]) -> RunProvenance:
    payload = _object(payload, RunProvenance, "run provenance artifact")
    declared = payload.get("run_provenance_hash")
    try:
        provenance = RunProvenance(
            **{
                **payload,
                "resolved_input_paths": tuple(
                    _array(payload["resolved_input_paths"], "provenance.resolved_input_paths")
                ),
                "argv": tuple(_array(payload["argv"], "provenance.argv")),
                "output_locations": tuple(
                    _array(payload["output_locations"], "provenance.output_locations")
                ),
            },
        ).finalize()
    except (TypeError, ValueError, KeyError, PlanError) as exc:
        raise PlanError(f"run provenance artifact invalid: {exc}") from None
    if provenance.run_provenance_hash != declared:
        raise PlanError("run provenance hash mismatch")
    return provenance


def load_run_provenance(path: str) -> RunProvenance:
    return run_provenance_from_dict(_load_artifact(path, "run provenance"))


def write_run_provenance(path: str, provenance: RunProvenance) -> None:
    if provenance.run_provenance_hash != provenance.compute_hash():
        raise PlanError("refusing to write unfinalized/tampered RunProvenance")
    _write_immutable_artifact(path, asdict(provenance), "RunProvenance")


def rank_for_node(binding: AllocationBinding, node_id: str) -> int:
    matches = [
        rank for rank, planned_node in binding.rank_to_node if same_node(planned_node, node_id)
    ]
    if len(matches) != 1:
        raise PlanError(f"node {node_id!r} maps to {len(matches)} allocation ranks, expected one")
    return matches[0]


def _logical_replica(plan: DeploymentPlan, model_id: str, replica_index: int):
    model = next((item for item in plan.models if item.model_id == model_id), None)
    if model is None:
        raise PlanError(f"model {model_id!r} has no planned receipt topology")
    if isinstance(replica_index, bool) or not isinstance(replica_index, int) or replica_index < 0:
        raise PlanError("replica_index must be a non-negative integer")
    candidates = [item for item in model.replicas if item.replica_index == replica_index]
    if len(candidates) != 1:
        raise PlanError(
            f"model {model_id!r} replica index {replica_index} maps to "
            f"{len(candidates)} planned replicas"
        )
    return model, candidates[0]


def resolve_replica_receipt_requirement(
    *, plan: DeploymentPlan, model_id: str, replica_index: int, role: str
) -> str:
    """Resolve an actor/core slot from its immutable logical replica identity.

    Ray chooses physical GPU resource ids when an actor is scheduled and may
    choose a different id after a constructor retry.  Physical ids therefore
    cannot be the identity of a canonical replica.  The deployment graph
    already carries the exact logical replica index, which survives retries.
    """
    if role not in {"replica", "engine_core"}:
        raise PlanError(f"unsupported replica receipt role {role!r}")
    model, replica = _logical_replica(plan, model_id, replica_index)
    base = f"model/{model.route_name}/replica/{replica.replica_index}"
    requirement_id = f"{base}/engine/core" if role == "engine_core" else base
    matches = [
        item for item in plan.receipt_requirements if item.receipt_requirement_id == requirement_id
    ]
    if len(matches) != 1 or matches[0].role != role:
        raise PlanError(
            f"logical {role} slot {requirement_id!r} maps to "
            f"{len(matches)} matching receipt requirements"
        )
    if matches[0].planned_rank != replica.planned_ranks[0]:
        raise PlanError(f"logical {role} slot {requirement_id!r} has inconsistent rank ownership")
    return requirement_id


def resolve_replica_index_for_live_placement(
    *, plan: DeploymentPlan, model_id: str, owner_rank: int, device_ids: tuple[int, ...]
) -> int:
    """Map a Serve-scheduled TP actor to one exact canonical replica slot.

    Native HeadOnly uses one public Ray Serve deployment so Serve itself owns
    request-to-replica balancing. The actor's allocation rank and accelerator
    ids are independently observable and together identify its precompiled TP
    slot. Pipeline-parallel replicas are intentionally excluded because their
    stage placement cannot be inferred from the ingress actor alone.
    """
    if isinstance(owner_rank, bool) or not isinstance(owner_rank, int) or owner_rank < 0:
        raise PlanError("owner_rank must be a non-negative integer")
    if (
        not isinstance(device_ids, tuple)
        or not device_ids
        or any(
            isinstance(item, bool) or not isinstance(item, int) or item < 0 for item in device_ids
        )
        or tuple(sorted(set(device_ids))) != device_ids
    ):
        raise PlanError("device_ids must be a non-empty sorted tuple of unique integers")
    model = next((item for item in plan.models if item.model_id == model_id), None)
    if model is None:
        raise PlanError(f"model {model_id!r} has no planned receipt topology")
    if model.pipeline_parallel_size != 1:
        raise PlanError("live placement resolution supports tensor-parallel replicas only")
    candidates = [
        replica
        for replica in model.replicas
        if replica.planned_ranks == (owner_rank,) and replica.planned_device_ids == (device_ids,)
    ]
    if len(candidates) != 1:
        raise PlanError(
            f"model {model_id!r} live placement rank={owner_rank} devices={device_ids} "
            f"maps to {len(candidates)} canonical replicas"
        )
    return candidates[0].replica_index


def resolve_engine_worker_receipt_requirement(
    *,
    plan: DeploymentPlan,
    model_id: str,
    replica_index: int,
    worker_rank: int,
    owner_rank: int,
) -> str:
    """Resolve one engine worker from vLLM's logical world rank.

    ``worker_rank`` is assigned by the pinned vLLM executor before worker
    initialization.  Unlike the Ray-selected accelerator id, it is stable for
    the topology across actor restarts and directly identifies stage/TP
    ordinal.  The actual hostname is independently bound to ``owner_rank`` by
    the allocation receipt before this helper is called.
    """
    model, replica = _logical_replica(plan, model_id, replica_index)
    if isinstance(worker_rank, bool) or not isinstance(worker_rank, int) or worker_rank < 0:
        raise PlanError("engine worker rank must be a non-negative integer")
    world_size = replica.tensor_parallel_size * replica.pipeline_parallel_size
    if worker_rank >= world_size:
        raise PlanError(
            f"engine worker rank {worker_rank} is outside replica world size {world_size}"
        )
    stage, tensor_rank = divmod(worker_rank, replica.tensor_parallel_size)
    planned_rank = replica.planned_ranks[stage]
    if owner_rank != planned_rank:
        raise PlanError(
            f"engine worker rank {worker_rank} is on allocation rank {owner_rank}, "
            f"planned rank is {planned_rank}"
        )
    planned_device = replica.planned_device_ids[stage][tensor_rank]
    base = f"model/{model.route_name}/replica/{replica.replica_index}"
    requirement_id = f"{base}/engine/worker/stage{stage}/device{planned_device}"
    matches = [
        item for item in plan.receipt_requirements if item.receipt_requirement_id == requirement_id
    ]
    if len(matches) != 1 or matches[0].role != "engine_worker":
        raise PlanError(
            f"logical engine worker slot {requirement_id!r} maps to "
            f"{len(matches)} matching receipt requirements"
        )
    if matches[0].planned_rank != planned_rank:
        raise PlanError(
            f"logical engine worker slot {requirement_id!r} has inconsistent rank ownership"
        )
    return requirement_id
