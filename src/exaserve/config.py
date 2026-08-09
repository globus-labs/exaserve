"""Site-default lookups for exaserve.

Production deployments need a handful of site-specific values (PBS account,
queue overrides, walltime overrides) that vary per user / per cluster. The
package reads them from environment variables so users can either ``export
EXASERVE_PROJECT_ACCOUNT=...`` once or pass values per invocation. Queue and
walltime deliberately have no fixed built-in fallback: when neither an
explicit argument nor an environment override exists, the canonical scheduler
policy derives both from the compiled deployment's node count.

Layering is: explicit kwarg > env var > built-in default.

Names:
    EXASERVE_PROJECT_ACCOUNT       PBS allocation/account (default: "AuroraGPT")
    EXASERVE_DEFAULT_QUEUE         optional PBS queue override
    EXASERVE_DEFAULT_WALLTIME      optional PBS walltime override (hh:mm:ss)
    EXASERVE_DEFAULT_FILESYSTEMS   PBS filesystem list   (default: "home:flare")
    EXASERVE_PBS_KEEP_FLAG         PBS -k value          (default: "doe")
    EXASERVE_PROJECT_ROOT          Repo root for source-tree usage (default: cwd)
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class SiteDefaults:
    project_account: str
    queue: Optional[str]
    walltime: Optional[str]
    filesystems: str
    keep_flag: str
    project_root: str


_DEFAULTS = SiteDefaults(
    project_account="AuroraGPT",
    queue=None,
    walltime=None,
    filesystems="home:flare",
    keep_flag="doe",
    project_root=".",
)


def get_site_defaults() -> SiteDefaults:
    """Return SiteDefaults populated from env vars, falling back to built-ins."""
    queue = os.environ.get("EXASERVE_DEFAULT_QUEUE", "").strip() or None
    walltime = os.environ.get("EXASERVE_DEFAULT_WALLTIME", "").strip() or None
    return SiteDefaults(
        project_account=os.environ.get("EXASERVE_PROJECT_ACCOUNT", _DEFAULTS.project_account),
        queue=queue,
        walltime=walltime,
        filesystems=os.environ.get("EXASERVE_DEFAULT_FILESYSTEMS", _DEFAULTS.filesystems),
        keep_flag=os.environ.get("EXASERVE_PBS_KEEP_FLAG", _DEFAULTS.keep_flag),
        project_root=os.environ.get("EXASERVE_PROJECT_ROOT", _DEFAULTS.project_root),
    )
