import os
import sys
import unittest
from typing import List, Optional


REPO_ROOT = os.path.dirname(os.path.dirname(__file__))
SRC_DIR = os.path.join(REPO_ROOT, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from exaserve.replica_planner import (
    NodeInventory,
    RequiredModelsError,
    compute_replica_plan,
    enforce_required_models_policy,
    tp_replica_capacity_for_nodes,
)
from exaserve.schemas import ModelConfig


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
    def test_tp_capacity_counts_per_node_gpu_slots(self) -> None:
        total, per_node = tp_replica_capacity_for_nodes(
            make_nodes(2, gpus_per_node=12, cpus_per_node=8),
            tensor_parallel_size=8,
            num_cpus_per_replica=4,
        )

        self.assertEqual(total, 2)
        self.assertEqual(per_node, 1)

    def test_tp_capacity_respects_cpu_limit(self) -> None:
        total, per_node = tp_replica_capacity_for_nodes(
            make_nodes(2, gpus_per_node=12, cpus_per_node=4),
            tensor_parallel_size=4,
            num_cpus_per_replica=4,
        )

        self.assertEqual(total, 2)
        self.assertEqual(per_node, 1)

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


class RequiredModelsPolicyTests(unittest.TestCase):
    def _plan_with_skip(self):
        # tp-auto cannot be placed alongside the explicit PP model on 4 nodes.
        return compute_replica_plan(
            [
                make_model("pp-explicit", tp=8, pp=2, num_replicas=2),
                make_model("tp-auto", tp=8),
            ],
            make_nodes(4),
        )

    def test_skipped_model_fails_by_default(self):  # PR-023
        plan = self._plan_with_skip()
        with self.assertRaises(RequiredModelsError) as ctx:
            enforce_required_models_policy(plan, allow_partial=False)
        self.assertIn("tp-auto", str(ctx.exception))

    def test_allow_partial_returns_reasons_without_raising(self):
        plan = self._plan_with_skip()
        reasons = enforce_required_models_policy(plan, allow_partial=True)
        self.assertTrue(any("tp-auto" in r for r in reasons))

    def test_all_placed_is_noop(self):
        plan = compute_replica_plan([make_model("solo", tp=8)], make_nodes(4))
        self.assertEqual(enforce_required_models_policy(plan, allow_partial=False), [])


class LegacySchemaHardeningTests(unittest.TestCase):
    """PR-006/PR-007 on the legacy schemas.py path that runs today."""

    def test_string_false_parses_false(self):
        from exaserve.schemas import _model_config_from_dict
        m = _model_config_from_dict({"model_id": "a/b", "enforce_eager": "false"})
        self.assertFalse(m.enforce_eager)

    def test_invalid_num_replicas_raises(self):
        from exaserve.schemas import _model_config_from_dict
        with self.assertRaises(ValueError):
            _model_config_from_dict({"model_id": "a/b", "num_replicas": "many"})

    def test_derived_identity_collision_rejected(self):
        from exaserve.schemas import (
            DeploymentConfig, _model_config_from_dict, validate_deployment_config,
        )
        cfg = DeploymentConfig(
            num_nodes=1, num_gpus_per_node=8, local_stage_path="/tmp/x",
            model_configs=[
                _model_config_from_dict({"model_id": "a/b--c", "max_model_len": 64}),
                _model_config_from_dict({"model_id": "a--b/c", "max_model_len": 64}),
            ],
        )
        with self.assertRaises(ValueError) as ctx:
            validate_deployment_config(cfg)
        self.assertIn("collision", str(ctx.exception))

if __name__ == "__main__":
    unittest.main()
