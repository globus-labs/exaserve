"""
Pluggable batch schedulers (package submission side).

    from exaserve.schedulers import get_scheduler
    sched = get_scheduler()            # EXASERVE_SCHEDULER, default "pbs"
    sched.submit(job_path)

Selected by ``EXASERVE_SCHEDULER`` (default ``pbs`` for the first-release
Aurora SiteProfile). PSI/J is an explicitly selected backend capability, not
an assumed universal default. Native ``pbs``/``slurm`` backends remain
available. Install PSI/J with the ``scheduler`` extra
(``pip install exaserve[scheduler]``). To add a scheduler, implement
``SchedulerBackend`` (base.py) and register it below.

Design doc: doc/design/scheduler_abstraction.md
"""

from __future__ import annotations

import os
from typing import Dict, Type

from .base import (  # noqa: F401
    AllocationMetadata,
    JobObservation,
    JobSpec,
    SchedulerBackend,
    SchedulerState,
    Submission,
    SubmissionAmbiguous,
    SubmissionError,
    SubmissionRejected,
    default_queue_and_walltime,
)

_REGISTRY: Dict[str, Type[SchedulerBackend]] = {}


def _register() -> None:
    if _REGISTRY:
        return
    from .pbs import PBSScheduler
    from .psij_backend import PSIJScheduler
    from .slurm import SlurmScheduler

    _REGISTRY["psij"] = PSIJScheduler
    _REGISTRY["exawork"] = PSIJScheduler  # alias
    _REGISTRY["pbs"] = PBSScheduler
    _REGISTRY["slurm"] = SlurmScheduler


def get_scheduler(name: str | None = None) -> SchedulerBackend:
    """Return an instantiated SchedulerBackend.

    The release default is native PBS because the authoritative Aurora
    SiteProfile names PBS. PSI/J remains available explicitly where a site has
    qualified an executor and its observation/reconciliation capabilities.
    """
    _register()
    key = (name or os.environ.get("EXASERVE_SCHEDULER", "pbs")).lower()
    if key not in _REGISTRY:
        raise KeyError(f"unknown scheduler {key!r}; registered: {sorted(_REGISTRY)}")
    return _REGISTRY[key]()


def available_schedulers() -> list[str]:
    _register()
    return sorted(_REGISTRY)


__all__ = [
    "get_scheduler",
    "available_schedulers",
    "SchedulerBackend",
    "JobSpec",
    "Submission",
    "SubmissionError",
    "SubmissionRejected",
    "SubmissionAmbiguous",
    "JobObservation",
    "SchedulerState",
    "AllocationMetadata",
    "default_queue_and_walltime",
]
