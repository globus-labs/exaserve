"""Site capabilities as one artifact (plan §3.2.1 Q5, packet P01, TD-CONSTS).

Site constants were scattered across a shell script, a Python config module,
and several literals. `TD-CONSTS` was previously dismissed as cosmetic; the
plan is explicit that it is not — the SiteProfile is a hash-bearing input to
the DeploymentPlan, so a site value that lives somewhere else is a value that
can drift without changing any plan identity.

The control-limit values here are **not production-qualified**. They are
structurally valid defaults so the contract can be exercised; `evidence_backed`
stays False until measured values are recorded at the required tiers, and a
plan compiled against an unqualified profile is a validation plan, not a
production one.
"""

from __future__ import annotations

import os
from functools import lru_cache

from .plan.contracts import ControlLimits, SiteProfile

AURORA_SITE_ID = "alcf-aurora"


@lru_cache(maxsize=4)
def default_site_profile(site_id: str = "") -> SiteProfile:
    """The site profile for this deployment host.

    Values come from the environment where the site genuinely varies, and are
    fixed here where they are properties of the machine.
    """
    site_id = site_id or os.environ.get("EXASERVE_SITE_ID", AURORA_SITE_ID)
    return SiteProfile(
        schema_version=2,
        site_id=site_id,
        max_nodes=int(os.environ.get("EXASERVE_SITE_MAX_NODES", "64")),
        gpus_per_node=int(os.environ.get("EXASERVE_SITE_GPUS_PER_NODE", "12")),
        cpus_per_node=int(os.environ.get("EXASERVE_SITE_CPUS_PER_NODE", "64")),
        scheduler_types=("pbs", "slurm"),
        gateway_kinds=("haproxy", "nginx", "envoy", "pingora"),
        vendors=("xpu", "cuda", "rocm"),
        engines=("vllm", "sglang", "null"),
        model_storage_path=os.environ.get(
            "EXASERVE_MODEL_STORAGE_PATH",
            "/lus/flare/projects/AuroraGPT/wenyiw/models"),
        local_stage_path=os.environ.get("EXASERVE_LOCAL_STAGE_PATH", "/tmp/hf_home"),
        control=ControlLimits(),          # evidence_backed=False by construction
    ).finalize()


def is_production_qualified(profile: SiteProfile) -> tuple[bool, str]:
    """A profile without measured control values cannot qualify a production run."""
    if not profile.control.evidence_backed:
        return False, (
            f"SiteProfile {profile.site_id} carries unmeasured control limits; "
            "measure them at the required tiers (S01/WP12) before treating a "
            "plan compiled against it as production-qualified")
    return True, "site profile is evidence-backed"
