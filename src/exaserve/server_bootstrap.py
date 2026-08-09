"""Import-clean deployment bootstrap.

This module is the executable boundary for the Serve deployment.  It verifies
the canonical plan and compatibility identity before importing ``ray``,
``ray.serve``, ``vllm``, or the implementation module that imports them.
"""

from __future__ import annotations

import argparse
import os
import sys


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Verified ExaServe deployment bootstrap")
    parser.add_argument("--plan", required=True)
    args = parser.parse_args(argv)

    from .compat.producers import manifest_hash
    from .compat.profile import default_profile
    from .plan.io import load_deployment_plan
    from .plan.runtime_environment import runtime_environment

    plan = load_deployment_plan(os.path.abspath(args.plan))
    expected = os.environ.get("EXASERVE_PLAN_HASH")
    if expected != plan.deployment_plan_hash:
        raise SystemExit(
            f"deployment bootstrap plan hash mismatch: environment={expected!r}, "
            f"artifact={plan.deployment_plan_hash!r}"
        )
    profile = default_profile(plan.vendor)
    resolved_manifest = manifest_hash(profile)
    if profile.profile_id != plan.compatibility_profile_hash:
        raise SystemExit(
            "deployment bootstrap compatibility profile does not match plan: "
            f"runtime={profile.profile_id}, plan={plan.compatibility_profile_hash}"
        )
    if resolved_manifest != plan.manifest_hash:
        raise SystemExit(
            "deployment bootstrap patch manifest does not match plan: "
            f"runtime={resolved_manifest}, plan={plan.manifest_hash}"
        )
    from .site import prepare_runtime_site

    prepare_runtime_site(plan)
    os.environ.update(runtime_environment(plan))

    # Verification uses package-distribution metadata and imports neither Ray
    # nor vLLM. Only the deployment role's declared adapters run afterward.
    from .compat.activator import CompatibilityActivator

    CompatibilityActivator(profile=profile).activate("deployment")

    from . import server

    sys.argv = ["exaserve.server", "--plan", os.path.abspath(args.plan)]
    server.main()


if __name__ == "__main__":
    main()
