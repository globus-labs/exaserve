"""Site-default lookups for aurora-rayserver.

Production deployments need a handful of site-specific values (PBS account,
queue names, default walltime) that vary per user / per cluster. The package
reads them from environment variables with documented fallbacks so users
can either ``export AURORA_PROJECT_ACCOUNT=...`` once or pass values per
invocation.

Layering is: explicit kwarg > env var > built-in default.

Names:
    AURORA_PROJECT_ACCOUNT       PBS allocation/account (default: "AuroraGPT")
    AURORA_DEFAULT_QUEUE         PBS queue name        (default: "debug-scaling")
    AURORA_DEFAULT_WALLTIME      PBS walltime hh:mm:ss (default: "01:00:00")
    AURORA_DEFAULT_FILESYSTEMS   PBS filesystem list   (default: "home:flare")
    AURORA_PBS_KEEP_FLAG         PBS -k value          (default: "doe")
    AURORA_PROJECT_ROOT          Repo root for source-tree usage (default: cwd)
"""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class SiteDefaults:
    project_account: str
    queue: str
    walltime: str
    filesystems: str
    keep_flag: str
    project_root: str


_DEFAULTS = SiteDefaults(
    project_account="AuroraGPT",
    queue="debug-scaling",
    walltime="01:00:00",
    filesystems="home:flare",
    keep_flag="doe",
    project_root=".",
)


def get_site_defaults() -> SiteDefaults:
    """Return SiteDefaults populated from env vars, falling back to built-ins."""
    return SiteDefaults(
        project_account=os.environ.get("AURORA_PROJECT_ACCOUNT", _DEFAULTS.project_account),
        queue=os.environ.get("AURORA_DEFAULT_QUEUE", _DEFAULTS.queue),
        walltime=os.environ.get("AURORA_DEFAULT_WALLTIME", _DEFAULTS.walltime),
        filesystems=os.environ.get("AURORA_DEFAULT_FILESYSTEMS", _DEFAULTS.filesystems),
        keep_flag=os.environ.get("AURORA_PBS_KEEP_FLAG", _DEFAULTS.keep_flag),
        project_root=os.environ.get("AURORA_PROJECT_ROOT", _DEFAULTS.project_root),
    )
