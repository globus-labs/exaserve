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

import math
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
    run_semantic_hash: str
    exposure_mode: str

    @property
    def is_validation_only(self) -> bool:
        """True when the plan says this endpoint is not a production exposure.

        Recorded rather than hidden: a number measured against
        DIRECT_VALIDATION did not traverse the gateway, and a report that does
        not say so invites comparing it with one that did.
        """
        return self.exposure_mode == "DIRECT_VALIDATION"


def resolve(
    run_dir: str,
    *,
    expected_generation: Optional[int] = None,
    expected_plan_hash: str = "",
    expected_run_semantic_hash: str = "",
) -> DeploymentTarget:
    """Resolve a READY deployment through the shared status API."""
    status, endpoint, run_semantic_hash = _read(
        run_dir, expected_generation, expected_plan_hash, expected_run_semantic_hash
    )
    return DeploymentTarget(
        base_url=endpoint,
        deployment_id=status.deployment_id,
        generation=status.generation,
        deployment_plan_hash=status.deployment_plan_hash,
        run_semantic_hash=run_semantic_hash,
        exposure_mode=status.exposure_mode,
    )


def wait_until_ready(
    run_dir: str,
    *,
    timeout_s: float = 1800.0,
    poll_s: float = 5.0,
    expected_generation: Optional[int] = None,
    expected_plan_hash: str = "",
    expected_run_semantic_hash: str = "",
) -> DeploymentTarget:
    """Block until the deployment is READY, or fail with the current state.

    A terminal state ends the wait immediately: continuing to poll a FAILED
    deployment until a timeout turns a precise cause into "it never came up".
    """
    from exaserve.status_api import read_deployment_status

    for name, value in (("timeout_s", timeout_s), ("poll_s", poll_s)):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
            or value <= 0
        ):
            raise ValueError(f"{name} must be finite and positive")
    deadline = time.monotonic() + timeout_s
    last = "no status record published yet"
    while time.monotonic() < deadline:
        status = read_deployment_status(run_dir)
        if status is not None:
            identity_matches = (
                expected_generation is None or status.generation == expected_generation
            ) and (not expected_plan_hash or status.deployment_plan_hash == expected_plan_hash)
            if not identity_matches:
                last = (
                    f"stale identity generation={status.generation} "
                    f"plan={status.deployment_plan_hash[:12]}"
                )
                time.sleep(min(poll_s, max(0.0, deadline - time.monotonic())))
                continue
            if status.ready:
                return resolve(
                    run_dir,
                    expected_generation=expected_generation,
                    expected_plan_hash=expected_plan_hash,
                    expected_run_semantic_hash=(expected_run_semantic_hash),
                )
            if status.terminal:
                raise DeploymentTargetError(
                    f"deployment reached {status.state} without becoming READY"
                    + (f": {status.reason_code} {status.detail}" if status.reason_code else "")
                )
            last = f"state={status.state}"
        time.sleep(min(poll_s, max(0.0, deadline - time.monotonic())))
    raise DeploymentTargetError(
        f"deployment under {run_dir} was not READY within {timeout_s:.0f}s ({last})"
    )


def _read(run_dir: str, expected_generation, expected_plan_hash, expected_run_semantic_hash):
    import os

    from exaserve.plan.io import load_run_provenance
    from exaserve.status_api import DeploymentNotReady, require_ready_status

    try:
        status = require_ready_status(
            run_dir, expected_generation=expected_generation, expected_plan_hash=expected_plan_hash
        )
    except DeploymentNotReady as exc:
        raise DeploymentTargetError(str(exc)) from exc
    endpoint = status.advertised_endpoint
    if expected_run_semantic_hash or status.run_provenance_hash:
        try:
            provenance = load_run_provenance(os.path.join(run_dir, "run_provenance.json"))
        except Exception as exc:
            raise DeploymentTargetError(
                f"READY deployment has no valid run provenance: {exc}"
            ) from exc
        if provenance.run_provenance_hash != status.run_provenance_hash:
            raise DeploymentTargetError("READY status and run provenance identities disagree")
        if (
            expected_run_semantic_hash
            and provenance.run_semantic_hash != expected_run_semantic_hash
        ):
            raise DeploymentTargetError("READY deployment belongs to a different RunPlan")
        run_semantic_hash = provenance.run_semantic_hash
    else:
        # Core serving launches intentionally have no RunPlan.  Status-only
        # callers may still use this adapter, but eval/ClientLab always passes
        # an expected run hash and therefore takes the verified branch above.
        run_semantic_hash = ""
    return status, endpoint, run_semantic_hash
