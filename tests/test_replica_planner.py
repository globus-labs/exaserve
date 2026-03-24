import os
import sys
import unittest
from typing import List, Optional


REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from replica_planner import NodeInventory, compute_replica_plan
from schemas import ModelConfig


def make_nodes(
    num_nodes: int,
    gpus_per_node: int = 12,
    cpus_per_node: int = 8,
) -> List[NodeInventory]:
    return [
        NodeInventory(
            ip=f"10.0.0.{index + 1}",
            resource_key=f"node:10.0.0.{index + 1}",
            total_gpus=gpus_per_node,
            remaining_gpus=gpus_per_node,
            total_cpus=cpus_per_node,
            remaining_cpus=cpus_per_node,
        )
        for index in range(num_nodes)
    ]


def make_model(
    model_id: str,
    *,
    tp: int,
    pp: int = 1,
    num_replicas: Optional[int] = None,
    size: int = 8,
) -> ModelConfig:
    return ModelConfig(
        model_id=model_id,
        tensor_parallel_size=tp,
        pipeline_parallel_size=pp,
        max_model_len=4096,
        size=size,
        num_replicas=num_replicas,
    )


class ReplicaPlannerTests(unittest.TestCase):
    def test_three_nodes_fit_only_one_pp_replica(self) -> None:
        plan = compute_replica_plan(
            [make_model("llama", tp=8, pp=2)],
            make_nodes(3),
        )

        model_plan = plan.model_plans[0]
        self.assertEqual(model_plan.assigned_replicas, 1)
        self.assertEqual(len(model_plan.placements), 1)

    def test_four_nodes_fit_two_pp_replicas(self) -> None:
        plan = compute_replica_plan(
            [make_model("llama", tp=8, pp=2)],
            make_nodes(4),
        )

        model_plan = plan.model_plans[0]
        self.assertEqual(model_plan.assigned_replicas, 2)
        self.assertEqual(len(model_plan.placements), 2)

    def test_explicit_models_are_placed_before_auto_models(self) -> None:
        tp_auto = make_model("tp-auto", tp=8)
        pp_explicit = make_model("pp-explicit", tp=8, pp=2, num_replicas=2)

        plan = compute_replica_plan([tp_auto, pp_explicit], make_nodes(4))

        auto_plan, explicit_plan = plan.model_plans
        self.assertEqual(explicit_plan.assigned_replicas, 2)
        self.assertEqual(auto_plan.assigned_replicas, 0)
        self.assertIsNotNone(auto_plan.skipped_reason)

    def test_round_robin_auto_growth_is_fair(self) -> None:
        plan = compute_replica_plan(
            [
                make_model("model-a", tp=4),
                make_model("model-b", tp=4),
            ],
            make_nodes(2, cpus_per_node=24),
        )

        assigned = [model_plan.assigned_replicas for model_plan in plan.model_plans]
        self.assertEqual(assigned, [3, 3])

    def test_auto_model_can_be_skipped_at_zero_replicas(self) -> None:
        plan = compute_replica_plan(
            [
                make_model("pp-explicit", tp=8, pp=2, num_replicas=2),
                make_model("tp-auto", tp=8),
            ],
            make_nodes(4),
        )

        explicit_plan, auto_plan = plan.model_plans
        self.assertEqual(explicit_plan.assigned_replicas, 2)
        self.assertEqual(auto_plan.assigned_replicas, 0)
        self.assertIn("No feasible placement remained", auto_plan.skipped_reason)

    def test_mixed_pp_and_tp_models_share_cluster_capacity(self) -> None:
        plan = compute_replica_plan(
            [
                make_model("pp-model", tp=8, pp=2),
                make_model("tp-model", tp=4),
            ],
            make_nodes(4),
        )

        pp_plan, tp_plan = plan.model_plans
        self.assertEqual(pp_plan.assigned_replicas, 2)
        self.assertEqual(tp_plan.assigned_replicas, 4)

    def test_cpu_budget_limits_tp_replicas_in_mixed_deployment(self) -> None:
        plan = compute_replica_plan(
            [
                make_model("pp-model", tp=8, pp=2, num_replicas=1),
                make_model("tp-model", tp=4),
            ],
            make_nodes(4, cpus_per_node=8),
        )

        pp_plan, tp_plan = plan.model_plans
        self.assertEqual(pp_plan.assigned_replicas, 1)
        self.assertEqual(tp_plan.assigned_replicas, 6)


if __name__ == "__main__":
    unittest.main()
