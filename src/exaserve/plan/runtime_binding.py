"""Bind immutable replica slots to one verified live Ray allocation.

The compiler owns counts, ranks, device slots, and replica identities. Runtime
discovery contributes only the allocation-dependent Ray node addresses and
resource keys; it never invokes the legacy replica planner or changes a count.
"""

from __future__ import annotations

from dataclasses import dataclass

from .contracts import (
    AllocationBinding,
    DeploymentPlan,
    ModelPlan,
    PlanError,
    ReplicaPlan,
    same_node,
)


@dataclass(frozen=True)
class LiveNodeInventory:
    ip: str
    resource_key: str
    total_gpus: int
    total_cpus: int
    hostname: str = ""

    def __post_init__(self) -> None:
        for name in ("ip", "resource_key"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"live node {name} must be non-empty text")
        if not isinstance(self.hostname, str):
            raise PlanError("live node hostname must be text")
        for name in ("total_gpus", "total_cpus"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise PlanError(f"live node {name} must be a non-negative integer")


@dataclass(frozen=True)
class BoundReplica:
    replica: ReplicaPlan
    node_ips: tuple[str, ...]
    node_resource_keys: tuple[str, ...]

    @property
    def replica_index(self) -> int:
        return self.replica.replica_index

    @property
    def owner_rank(self) -> int:
        """Return the allocation rank that owns the replica actor."""
        return self.replica.planned_ranks[0]


@dataclass(frozen=True)
class BoundModel:
    model: ModelPlan
    replicas: tuple[BoundReplica, ...]

    @property
    def assigned_replicas(self) -> int:
        return len(self.replicas)


@dataclass(frozen=True)
class BoundNode:
    """One planned allocation rank resolved to its exact live Ray node."""

    rank: int
    inventory: LiveNodeInventory


@dataclass(frozen=True)
class BoundDeployment:
    plan: DeploymentPlan
    nodes: tuple[BoundNode, ...]
    models: tuple[BoundModel, ...]


def bind_runtime_deployment(
    plan: DeploymentPlan,
    binding: AllocationBinding,
    live_nodes,
) -> BoundDeployment:
    """Resolve planned ranks to live nodes and fail on any inventory drift."""
    if not isinstance(plan, DeploymentPlan) or not isinstance(binding, AllocationBinding):
        raise PlanError("runtime binding requires canonical plan and allocation contracts")
    if binding.deployment_plan_hash != plan.deployment_plan_hash:
        raise PlanError("allocation binding does not belong to canonical plan")
    if not isinstance(live_nodes, (tuple, list)):
        raise PlanError("live node inventory must be a sequence")
    live_nodes = tuple(live_nodes)
    if any(not isinstance(node, LiveNodeInventory) for node in live_nodes):
        raise PlanError("live node inventory must contain LiveNodeInventory values")
    by_rank = {}
    used_live_ids: set[int] = set()
    for rank, bound_name in binding.rank_to_node:
        matches = [node for node in live_nodes if same_node(node.hostname or node.ip, bound_name)]
        if len(matches) != 1:
            raise PlanError(
                f"planned rank {rank} node {bound_name!r} maps to {len(matches)} live Ray GPU nodes"
            )
        node = matches[0]
        if id(node) in used_live_ids:
            raise PlanError("two allocation ranks mapped to the same live Ray node")
        used_live_ids.add(id(node))
        if node.total_gpus < plan.num_gpus_per_node:
            raise PlanError(
                f"live rank {rank} has {node.total_gpus} GPUs, plan requires "
                f"{plan.num_gpus_per_node}"
            )
        if node.total_cpus < plan.node_cpus:
            raise PlanError(
                f"live rank {rank} has {node.total_cpus} CPUs, plan requires {plan.node_cpus}"
            )
        by_rank[rank] = node
    if set(by_rank) != set(binding.ranks()) or len(by_rank) != plan.num_nodes:
        raise PlanError("live Ray inventory does not cover every planned allocation rank")

    bound_models = []
    for model in plan.models:
        bound_replicas = []
        for replica in model.replicas:
            nodes = tuple(by_rank[rank] for rank in replica.planned_ranks)
            for stage, node in enumerate(nodes):
                missing_devices = [
                    device
                    for device in replica.planned_device_ids[stage]
                    if device >= node.total_gpus
                ]
                if missing_devices:
                    raise PlanError(
                        f"replica {replica.replica_id} names unavailable devices "
                        f"{missing_devices} on rank {replica.planned_ranks[stage]}"
                    )
            bound_replicas.append(
                BoundReplica(
                    replica=replica,
                    node_ips=tuple(node.ip for node in nodes),
                    node_resource_keys=tuple(node.resource_key for node in nodes),
                )
            )
        bound_models.append(BoundModel(model=model, replicas=tuple(bound_replicas)))
    return BoundDeployment(
        plan=plan,
        nodes=tuple(BoundNode(rank=rank, inventory=by_rank[rank]) for rank in binding.ranks()),
        models=tuple(bound_models),
    )


def format_runtime_binding(bound: BoundDeployment) -> str:
    lines = [
        f"Canonical runtime binding: {len(bound.models)} model(s), "
        f"{sum(len(model.replicas) for model in bound.models)} replica(s)"
    ]
    for model in bound.models:
        placements = ", ".join(
            f"r{item.replica_index}={list(item.node_ips)}" for item in model.replicas
        )
        lines.append(f"  {model.model.model_id}: {placements}")
    return "\n".join(lines)
