"""ExaServe canonical planning and supervised serving package.

The public configuration boundary is the immutable plan family. Legacy YAML
is accepted only through the one-way compiler adapter; runtime code consumes
verified artifacts and cannot select the former schema or lifecycle paths.
"""

from .plan import (  # noqa: F401
    AllocationBinding,
    DeploymentPlan,
    RunPlan,
    SiteProfile,
    compile_deployment_plan,
    compile_run_plan,
)
from ._version import __version__

__all__ = [
    "__version__",
    "AllocationBinding",
    "DeploymentPlan",
    "RunPlan",
    "SiteProfile",
    "compile_deployment_plan",
    "compile_run_plan",
]
