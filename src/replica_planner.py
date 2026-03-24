from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Tuple

from schemas import ModelConfig


@dataclass
class NodeInventory:
    ip: str
    resource_key: str
    total_gpus: int
    remaining_gpus: int
    total_cpus: int
    remaining_cpus: int


@dataclass(frozen=True)
class ReplicaPlacement:
    model_id: str
    replica_index: int
    node_ips: Tuple[str, ...]
    tensor_parallel_size: int
    pipeline_parallel_size: int


@dataclass
class ModelReplicaPlan:
    model_config: ModelConfig
    requested_replicas: Optional[int]
    assigned_replicas: int = 0
    placements: List[ReplicaPlacement] = field(default_factory=list)
    skipped_reason: Optional[str] = None

    @property
    def explicit(self) -> bool:
        return self.requested_replicas is not None

    @property
    def active(self) -> bool:
        return self.assigned_replicas > 0


@dataclass
class DeploymentReplicaPlan:
    model_plans: List[ModelReplicaPlan]
    nodes: List[NodeInventory]

    @property
    def active_model_plans(self) -> List[ModelReplicaPlan]:
        return [plan for plan in self.model_plans if plan.active]

    @property
    def skipped_model_plans(self) -> List[ModelReplicaPlan]:
        return [plan for plan in self.model_plans if not plan.active]


def tp_replica_capacity_for_nodes(
    nodes: Sequence[NodeInventory],
    tensor_parallel_size: int,
    num_cpus_per_replica: int,
) -> Tuple[int, int]:
    """
    Compute TP-only replica capacity from per-node GPU and CPU budgets.

    Returns:
        (total_replicas_that_fit, max_replicas_that_fit_on_any_single_node)
    """
    if tensor_parallel_size < 1:
        raise ValueError("tensor_parallel_size must be >= 1")

    per_node_caps: List[int] = []
    for node in nodes:
        gpu_cap = node.remaining_gpus // tensor_parallel_size
        if num_cpus_per_replica > 0:
            cpu_cap = node.remaining_cpus // num_cpus_per_replica
            per_node_caps.append(min(gpu_cap, cpu_cap))
        else:
            per_node_caps.append(gpu_cap)

    return sum(per_node_caps), max(per_node_caps, default=0)


def clone_nodes(nodes: Sequence[NodeInventory]) -> List[NodeInventory]:
    return [
        NodeInventory(
            ip=node.ip,
            resource_key=node.resource_key,
            total_gpus=node.total_gpus,
            remaining_gpus=node.remaining_gpus,
            total_cpus=node.total_cpus,
            remaining_cpus=node.remaining_cpus,
        )
        for node in nodes
    ]


def _sorted_eligible_nodes(
    nodes: Sequence[NodeInventory],
    required_gpus: int,
    required_cpus: int = 0,
) -> List[Tuple[int, NodeInventory]]:
    eligible = [
        (index, node)
        for index, node in enumerate(nodes)
        if node.remaining_gpus >= required_gpus and node.remaining_cpus >= required_cpus
    ]
    eligible.sort(
        key=lambda item: (
            item[1].remaining_gpus,
            item[1].remaining_cpus,
            item[1].ip,
        )
    )
    return eligible


def _select_tp_nodes(
    nodes: Sequence[NodeInventory],
    tensor_parallel_size: int,
    num_cpus_per_replica: int,
) -> Optional[List[int]]:
    eligible = _sorted_eligible_nodes(
        nodes,
        tensor_parallel_size,
        required_cpus=num_cpus_per_replica,
    )
    if not eligible:
        return None
    return [eligible[0][0]]


def _select_pp_nodes(
    nodes: Sequence[NodeInventory],
    tensor_parallel_size: int,
    pipeline_parallel_size: int,
    num_cpus_per_replica: int,
) -> Optional[List[int]]:
    stage0_eligible = _sorted_eligible_nodes(
        nodes,
        tensor_parallel_size,
        required_cpus=num_cpus_per_replica,
    )
    for stage0_index, _ in stage0_eligible:
        remaining_candidates = _sorted_eligible_nodes(nodes, tensor_parallel_size)
        other_indices = [
            index for index, _ in remaining_candidates if index != stage0_index
        ]
        if len(other_indices) < pipeline_parallel_size - 1:
            continue
        return [stage0_index, *other_indices[: pipeline_parallel_size - 1]]
    return None


def _reserve_nodes(
    nodes: Sequence[NodeInventory],
    selected_indices: Sequence[int],
    tensor_parallel_size: int,
    num_cpus_per_replica: int,
) -> List[NodeInventory]:
    updated_nodes = clone_nodes(nodes)
    for index in selected_indices:
        updated_nodes[index].remaining_gpus -= tensor_parallel_size
    if selected_indices:
        updated_nodes[selected_indices[0]].remaining_cpus -= num_cpus_per_replica
    return updated_nodes


def try_reserve_replica(
    model_config: ModelConfig,
    nodes: Sequence[NodeInventory],
    replica_index: int,
) -> Tuple[Optional[ReplicaPlacement], List[NodeInventory]]:
    if model_config.pipeline_parallel_size > 1:
        selected_indices = _select_pp_nodes(
            nodes,
            tensor_parallel_size=model_config.tensor_parallel_size,
            pipeline_parallel_size=model_config.pipeline_parallel_size,
            num_cpus_per_replica=model_config.num_cpus_per_replica,
        )
    else:
        selected_indices = _select_tp_nodes(
            nodes,
            tensor_parallel_size=model_config.tensor_parallel_size,
            num_cpus_per_replica=model_config.num_cpus_per_replica,
        )

    if selected_indices is None:
        return None, clone_nodes(nodes)

    updated_nodes = _reserve_nodes(
        nodes,
        selected_indices=selected_indices,
        tensor_parallel_size=model_config.tensor_parallel_size,
        num_cpus_per_replica=model_config.num_cpus_per_replica,
    )
    placement = ReplicaPlacement(
        model_id=model_config.model_id,
        replica_index=replica_index,
        node_ips=tuple(nodes[index].ip for index in selected_indices),
        tensor_parallel_size=model_config.tensor_parallel_size,
        pipeline_parallel_size=model_config.pipeline_parallel_size,
    )
    return placement, updated_nodes


def compute_replica_plan(
    model_configs: Iterable[ModelConfig],
    nodes: Sequence[NodeInventory],
) -> DeploymentReplicaPlan:
    plans = [
        ModelReplicaPlan(
            model_config=model_config,
            requested_replicas=model_config.num_replicas,
        )
        for model_config in model_configs
    ]

    planned_nodes = clone_nodes(nodes)

    for plan in (item for item in plans if item.explicit):
        assert plan.requested_replicas is not None
        for replica_index in range(plan.requested_replicas):
            placement, updated_nodes = try_reserve_replica(
                plan.model_config,
                planned_nodes,
                replica_index=replica_index,
            )
            if placement is None:
                raise ValueError(
                    "Unable to satisfy explicit replica request for "
                    f"{plan.model_config.model_id}: requested "
                    f"{plan.requested_replicas}, assigned {plan.assigned_replicas}"
                )
            planned_nodes = updated_nodes
            plan.placements.append(placement)
            plan.assigned_replicas += 1

    auto_plans = [plan for plan in plans if not plan.explicit]
    while auto_plans:
        round_progress = False
        for plan in auto_plans:
            placement, updated_nodes = try_reserve_replica(
                plan.model_config,
                planned_nodes,
                replica_index=plan.assigned_replicas,
            )
            if placement is None:
                continue
            planned_nodes = updated_nodes
            plan.placements.append(placement)
            plan.assigned_replicas += 1
            round_progress = True
        if not round_progress:
            break

    for plan in auto_plans:
        if plan.assigned_replicas == 0:
            plan.skipped_reason = (
                "No feasible placement remained after explicit replica reservations"
            )

    return DeploymentReplicaPlan(model_plans=plans, nodes=planned_nodes)


def format_replica_plan(plan: DeploymentReplicaPlan) -> str:
    lines = ["[AuroraServe] Planner summary:"]
    for model_plan in plan.model_plans:
        requested = (
            str(model_plan.requested_replicas)
            if model_plan.requested_replicas is not None
            else "auto"
        )
        status = (
            f"assigned={model_plan.assigned_replicas}"
            if model_plan.active
            else f"skipped ({model_plan.skipped_reason})"
        )
        lines.append(
            f"  - {model_plan.model_config.model_id}: requested={requested}, {status}"
        )
        for placement in model_plan.placements:
            lines.append(
                "    "
                f"replica {placement.replica_index}: nodes={list(placement.node_ips)} "
                f"(TP={placement.tensor_parallel_size}, PP={placement.pipeline_parallel_size})"
            )
    residual = ", ".join(
        f"{node.ip}: {node.remaining_gpus}/{node.total_gpus} GPUs free, "
        f"{node.remaining_cpus}/{node.total_cpus} CPUs free"
        for node in sorted(plan.nodes, key=lambda item: item.ip)
    )
    lines.append(f"  Residual resources: {residual}")
    return "\n".join(lines)
