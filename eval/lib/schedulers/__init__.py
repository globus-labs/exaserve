"""Scheduler implementations for eval run bundles.

Selected by ``scheduler.type`` in a spec (default ``pbs``). Mirrors the
package-side ``exaserve.schedulers`` registry shape.
"""

from __future__ import annotations

from typing import Dict, Type

from .base import EvalScheduler  # noqa: F401
from .pbs import default_queue_and_walltime, render_pbs_job  # noqa: F401  back-compat

_REGISTRY: Dict[str, Type[EvalScheduler]] = {}


def _register() -> None:
    if _REGISTRY:
        return
    from .pbs import PBSScheduler
    from .psij_backend import PSIJEvalScheduler
    from .slurm import SlurmScheduler

    _REGISTRY["psij"] = PSIJEvalScheduler
    _REGISTRY["exawork"] = PSIJEvalScheduler  # alias
    _REGISTRY["pbs"] = PBSScheduler
    _REGISTRY["slurm"] = SlurmScheduler


def get_scheduler(name: str | None = None) -> EvalScheduler:
    """Default is the ExaWorks PSI/J backend when a spec omits scheduler.type;
    existing specs that pin ``type: pbs`` keep the native path."""
    _register()
    key = (name or "psij").lower()
    if key not in _REGISTRY:
        raise KeyError(f"unknown scheduler {key!r}; registered: {sorted(_REGISTRY)}")
    return _REGISTRY[key]()


def available_schedulers() -> list[str]:
    _register()
    return sorted(_REGISTRY)


__all__ = [
    "get_scheduler", "available_schedulers", "EvalScheduler",
    "default_queue_and_walltime", "render_pbs_job",
]
