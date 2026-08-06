"""Immutable, validated plan contracts (plan WP1).

The compiler consumes user configuration (including legacy YAML shapes),
applies strict coercion, and produces frozen plan objects with a canonical
``plan_hash``. It never mutates or rewrites the source document (PR-003).
Consumers (serving, eval, ClientLab) migrate onto these contracts in later
packets; until cutover the legacy loaders remain the default path
(migration-switch rules, plan §4.1).
"""

from .schemas import (  # noqa: F401
    DeploymentPlan,
    GatewayPlan,
    ModelPlan,
    PlanError,
    ScaleEnvelope,
    SchedulerPlan,
    compile_deployment_plan,
    from_legacy_yaml,
)
