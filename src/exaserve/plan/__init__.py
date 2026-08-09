"""The sole public compiled-plan namespace.

All objects come from :mod:`exaserve.plan.contracts`; all interpretation comes
from :mod:`exaserve.plan.compiler`. There is no secondary schema namespace.
"""

from .compiler import build_receipt_requirements, compile_deployment_plan, compile_run_plan  # noqa: F401
from .contracts import (  # noqa: F401
    SCHEMA_VERSION,
    AllocationBinding,
    ArtifactPolicy,
    BackendPolicy,
    ClientPolicy,
    ComponentInstanceBinding,
    ControlLimits,
    DeploymentPlan,
    ExposureMode,
    ExposurePlan,
    GatewayKind,
    GatewayPlan,
    ModelPlan,
    PlanError,
    ReadinessLimits,
    ReceiptRequirement,
    ReplicaPlan,
    RuntimePolicy,
    RunPlan,
    SaturationPolicy,
    RunProvenance,
    ScaleEnvelope,
    SchedulerPlan,
    SiteProfile,
    TracePolicy,
    WorkloadPolicy,
    build_allocation_binding,
    canonical_hash,
)

__all__ = [name for name in globals() if not name.startswith("_")]
