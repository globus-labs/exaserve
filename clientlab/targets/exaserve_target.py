"""ClientLab's only legal view of an ExaServe deployment (plan §3.4, WP9).

ClientLab's existing targets are its own synthetic stubs, which it starts and
supervises itself — that is correct and stays. What was missing is the path for
a study that points at a *real* deployment. Without one, the way to do it was
to read a log for a marker or poke a port until something answered, which is
exactly what the boundary table forbids:

    Eval/ClientLab -> deployment | Shared `DeploymentStatus`/event API |
    Generation-specific terminal/readiness state; no private process monitor
    or log grep

So this module has no discovery logic of its own. It asks the shared status API
and takes the *compiled advertised endpoint* the deployment published — the
same endpoint the composition root canaried before committing READY. A client
that measures a different endpoint than readiness verified is measuring
something nobody validated.

Nothing here starts, stops, or inspects a deployment process. ClientLab is a
client; the deployment's lifecycle is not its business.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Optional


class DeploymentTargetError(RuntimeError):
    """The deployment is unusable as a target, with the reason attached."""


@dataclass(frozen=True)
class DeploymentTarget:
    """A resolved, READY deployment endpoint and the identity behind it."""

    base_url: str
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    exposure_mode: str

    @property
    def is_validation_only(self) -> bool:
        """True when the plan says this endpoint is not a production exposure.

        Recorded rather than hidden: a number measured against
        DIRECT_VALIDATION did not traverse the gateway, and a report that does
        not say so invites comparing it with one that did.
        """
        return self.exposure_mode == "DIRECT_VALIDATION"


def resolve(run_dir: str, *, expected_generation: Optional[int] = None,
            expected_plan_hash: str = "") -> DeploymentTarget:
    """Resolve a READY deployment through the shared status API."""
    status, endpoint = _read(run_dir, expected_generation, expected_plan_hash)
    return DeploymentTarget(
        base_url=endpoint, deployment_id=status.deployment_id,
        generation=status.generation,
        deployment_plan_hash=status.deployment_plan_hash,
        exposure_mode=status.exposure_mode)


def wait_until_ready(run_dir: str, *, timeout_s: float = 1800.0,
                     poll_s: float = 5.0,
                     expected_generation: Optional[int] = None,
                     expected_plan_hash: str = "") -> DeploymentTarget:
    """Block until the deployment is READY, or fail with the current state.

    A terminal state ends the wait immediately: continuing to poll a FAILED
    deployment until a timeout turns a precise cause into "it never came up".
    """
    from exaserve.status_api import read_deployment_status

    deadline = time.monotonic() + timeout_s
    last = "no status record published yet"
    while time.monotonic() < deadline:
        status = read_deployment_status(run_dir)
        if status is not None:
            if status.ready:
                return resolve(run_dir, expected_generation=expected_generation,
                               expected_plan_hash=expected_plan_hash)
            if status.terminal:
                raise DeploymentTargetError(
                    f"deployment reached {status.state} without becoming READY"
                    + (f": {status.reason_code} {status.detail}"
                       if status.reason_code else ""))
            last = f"state={status.state}"
        time.sleep(poll_s)
    raise DeploymentTargetError(
        f"deployment under {run_dir} was not READY within {timeout_s:.0f}s ({last})")


def _read(run_dir: str, expected_generation, expected_plan_hash):
    from exaserve.status_api import (
        DeploymentNotReady,
        read_deployment_status,
        require_ready_endpoint,
    )

    try:
        endpoint = require_ready_endpoint(
            run_dir, expected_generation=expected_generation,
            expected_plan_hash=expected_plan_hash)
    except DeploymentNotReady as exc:
        raise DeploymentTargetError(str(exc)) from exc
    status = read_deployment_status(run_dir)
    return status, endpoint
