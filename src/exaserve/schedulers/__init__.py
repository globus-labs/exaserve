"""
Pluggable batch schedulers (package submission side).

    from exaserve.schedulers import get_scheduler
    sched = get_scheduler()            # EXASERVE_SCHEDULER, default "pbs"
    sched.submit(job_path)

Selected by ``EXASERVE_SCHEDULER`` (default ``pbs``). To add a scheduler,
implement ``SchedulerBackend`` (base.py) and register it below.

Design doc: doc/design/scheduler_abstraction.md
"""

from __future__ import annotations

import os
from typing import Dict, Type

from .base import JobSpec, SchedulerBackend  # noqa: F401

_REGISTRY: Dict[str, Type[SchedulerBackend]] = {}


def _register() -> None:
    if _REGISTRY:
        return
    from .pbs import PBSScheduler
    from .slurm import SlurmScheduler

    _REGISTRY["pbs"] = PBSScheduler
    _REGISTRY["slurm"] = SlurmScheduler


def get_scheduler(name: str | None = None) -> SchedulerBackend:
    """Return an instantiated SchedulerBackend (default EXASERVE_SCHEDULER / pbs)."""
    _register()
    key = (name or os.environ.get("EXASERVE_SCHEDULER", "pbs")).lower()
    if key not in _REGISTRY:
        raise KeyError(f"unknown scheduler {key!r}; registered: {sorted(_REGISTRY)}")
    return _REGISTRY[key]()


def available_schedulers() -> list[str]:
    _register()
    return sorted(_REGISTRY)


__all__ = ["get_scheduler", "available_schedulers", "SchedulerBackend", "JobSpec"]
