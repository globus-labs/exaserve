"""ExaServe distributed control plane (plan §3.1–§3.3).

Modules:
- ``contracts``: lifecycle enums, observations, protocol envelopes, errors,
  serialization, schema validation. No Ray imports, no I/O.
- ``transport``: authenticated length-prefixed framing and session handling.
  No readiness policy.

- ``readiness``: pure indexed readiness projection (no process launch, no log
  reads) — a stdout marker can never make a deployment ready.
- ``supervisor``: ``RuntimeSupervisor`` + ``ManagedComponent`` — typed child
  lifecycle, process-group ownership, first-cause capture, bounded cleanup.

``rank_launcher``/``node_supervisor`` implement the active MPI rank-set split.
The retired shell lifecycle and supervisor feature switch are not reachable.
"""

from .readiness import (  # noqa: F401
    DeploymentPhase,
    ReadinessCoordinator,
    ReadinessSnapshot,
    ReadinessVerdict,
)
from .supervisor import (  # noqa: F401
    FirstCause,
    ManagedComponent,
    RuntimeSupervisor,
    SupervisorError,
)
