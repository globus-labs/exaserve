"""Canonical deployment readiness authority.

There is exactly one readiness implementation.  It evaluates the immutable
``DeploymentPlan`` and ``AllocationBinding`` against current-generation
evidence; it owns no processes, reads no stdout, and writes no private state
file.  The allocation-head composition root publishes its verdict in the same
CAS transition that changes the authoritative ``DeploymentStatus`` to READY.

The implementation currently lives in :mod:`plan_readiness` to retain a
source-compatible import for one release.  That module exports an alias, not a
second coordinator or alternate predicate.
"""

from .plan_readiness import (  # noqa: F401
    DeploymentPhase,
    ReadinessCoordinator,
    ReadinessVerdict,
)

# The old public snapshot name is harmless as a type alias.  ``ReadinessPlan``
# is intentionally gone: the only accepted plan is the canonical immutable
# DeploymentPlan compiled by exaserve.plan.compiler.
ReadinessSnapshot = ReadinessVerdict

__all__ = [
    "DeploymentPhase",
    "ReadinessCoordinator",
    "ReadinessSnapshot",
    "ReadinessVerdict",
]
