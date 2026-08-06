"""
Pluggable batch schedulers (package submission side).

    from exaserve.schedulers import get_scheduler
    sched = get_scheduler()            # EXASERVE_SCHEDULER, default "psij"
    sched.submit(job_path)

Selected by ``EXASERVE_SCHEDULER`` (default ``psij`` — the ExaWorks PSI/J
backend, portable across PBS/Slurm/LSF/Flux). Native ``pbs``/``slurm``
backends remain available. Install PSI/J with the ``scheduler`` extra
(``pip install exaserve[scheduler]``). To add a scheduler, implement
``SchedulerBackend`` (base.py) and register it below.

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
    from .psij_backend import PSIJScheduler
    from .slurm import SlurmScheduler

    _REGISTRY["psij"] = PSIJScheduler
    _REGISTRY["exawork"] = PSIJScheduler  # alias
    _REGISTRY["pbs"] = PBSScheduler
    _REGISTRY["slurm"] = SlurmScheduler


def get_scheduler(name: str | None = None) -> SchedulerBackend:
    """Return an instantiated SchedulerBackend.

    Default is the ExaWorks PSI/J backend (portable across PBS/Slurm/LSF/Flux;
    executor auto-detected or EXASERVE_PSIJ_EXECUTOR). The hand-rolled native
    backends remain available via EXASERVE_SCHEDULER=pbs|slurm.
    """
    _register()
    key = (name or os.environ.get("EXASERVE_SCHEDULER", "psij")).lower()
    if key not in _REGISTRY:
        raise KeyError(f"unknown scheduler {key!r}; registered: {sorted(_REGISTRY)}")
    return _REGISTRY[key]()


def available_schedulers() -> list[str]:
    _register()
    return sorted(_REGISTRY)


__all__ = ["get_scheduler", "available_schedulers", "SchedulerBackend", "JobSpec"]
