"""Import-light Ray child adapter derived from verified runtime artifacts.

This module builds argument vectors and child environments only. It owns no
process, parses no config, and imports neither Ray nor an engine. Site values
must already have been applied from the hash-bearing SiteProfile.
"""

from __future__ import annotations

import os
import ipaddress
import socket
import sys
from dataclasses import dataclass


@dataclass(frozen=True)
class RayClusterConfig:
    head_ip: str
    port: int
    node_cpus: int

    def __post_init__(self) -> None:
        if not isinstance(self.head_ip, str) or not self.head_ip:
            raise ValueError("Ray head_ip must be non-empty")
        for name, maximum in (("port", 65535), ("node_cpus", 1 << 20)):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < 1
                or value > maximum
            ):
                raise ValueError(f"Ray {name} is invalid: {value!r}")


def get_rank() -> int:
    """Return the rank supplied by the selected MPI/srun boundary."""
    for name in (
        "PALS_RANKID",
        "PMI_RANK",
        "PMI_ID",
        "ALPS_APP_PE",
        "SLURM_PROCID",
        "OMPI_COMM_WORLD_RANK",
    ):
        value = os.environ.get(name)
        if value is None:
            continue
        try:
            rank = int(value)
        except ValueError as exc:
            raise RuntimeError(f"{name} must be an integer, got {value!r}") from exc
        if rank < 0:
            raise RuntimeError(f"{name} must be non-negative, got {rank}")
        return rank
    raise RuntimeError(
        "rank entry has no PALS/PMI/Slurm/OpenMPI rank identity; refusing to guess rank zero"
    )


def ray_child_environment(plan, *, node_ip: str, site_profile=None) -> dict[str, str]:
    """Build the node-local Ray environment with one network identity.

    vLLM's Ray executor compares the driver's ``VLLM_HOST_IP`` with the
    address reported by every worker actor.  On multi-fabric systems, leaving
    the worker value implicit lets vLLM discover a management address while
    Ray itself is using the high-speed-fabric address.  The resulting extra IP
    correctly fails vLLM's one-IP-per-node check.  Every Ray daemon and its
    descendants therefore inherit the exact address also supplied to Ray's
    ``--node-ip-address`` argument.
    """
    if not isinstance(node_ip, str) or not node_ip:
        raise ValueError("Ray child node_ip must be a non-empty IP address")
    try:
        ipaddress.ip_address(node_ip)
    except ValueError as exc:
        raise ValueError(f"Ray child node_ip is invalid: {node_ip!r}") from exc
    expected = {
        "EXASERVE_PLAN_HASH": plan.deployment_plan_hash,
        "EXASERVE_SITE_PROFILE_HASH": plan.site_profile_hash,
        "EXASERVE_VENDOR": plan.vendor,
        "EXASERVE_ENGINE": plan.engine,
    }
    for key, value in expected.items():
        if os.environ.get(key) != value:
            raise RuntimeError(f"prepared runtime {key} does not match DeploymentPlan")
    from ..plan.runtime_environment import (
        LOCAL_RUNTIME_ROOT_ENV,
        closed_runtime_environment,
        runtime_paths,
    )

    if os.environ.get(LOCAL_RUNTIME_ROOT_ENV):
        try:
            generation = int(os.environ.get("EXASERVE_GENERATION", ""))
        except ValueError as exc:
            raise RuntimeError("managed Ray child requires an integer generation") from exc
        paths = runtime_paths(plan, generation=generation, require_runtime=True)
        environment = closed_runtime_environment(
            plan,
            paths=paths,
            base_environment=os.environ,
            policy=site_profile or plan,
        )
    else:
        # Portable argument-construction tests exercise this helper without a
        # staged capsule. Production rank_main rejects that state before it can
        # create a child.
        environment = dict(os.environ)
    environment["VLLM_HOST_IP"] = node_ip
    if plan.vendor == "xpu":
        environment.pop("ONEAPI_DEVICE_SELECTOR", None)
    return environment


def _startup_limit(cluster: RayClusterConfig) -> int:
    raw = os.environ.get("EXASERVE_RAY_INTERNAL_STARTUP_LIMIT")
    if raw is None:
        raise RuntimeError("SiteProfile omitted EXASERVE_RAY_INTERNAL_STARTUP_LIMIT")
    try:
        limit = int(raw)
    except ValueError as exc:
        raise RuntimeError("EXASERVE_RAY_INTERNAL_STARTUP_LIMIT must be an integer") from exc
    if limit < 1:
        raise RuntimeError("EXASERVE_RAY_INTERNAL_STARTUP_LIMIT must be positive")
    return min(cluster.node_cpus, limit)


def _local_hsn_ip() -> str:
    hostname = socket.gethostname()
    for candidate in (f"{hostname}.hsn.cm.aurora.alcf.anl.gov", hostname):
        try:
            return socket.gethostbyname(candidate)
        except OSError:
            continue
    raise RuntimeError(f"could not resolve allocated node {hostname!r}")


def ray_node_ip(cluster: RayClusterConfig, rank: int) -> str:
    """Resolve the one address shared by a rank's Ray and vLLM children."""
    if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
        raise ValueError(f"Ray rank must be a non-negative integer, got {rank!r}")
    return cluster.head_ip if rank == 0 else _local_hsn_ip()


def ray_head_argv(cluster: RayClusterConfig, num_gpus: int) -> list[str]:
    limit = _startup_limit(cluster)
    return [
        _python_executable(),
        "-m",
        "exaserve.ray_start",
        "--head",
        f"--node-ip-address={cluster.head_ip}",
        f"--num-cpus={cluster.node_cpus}",
        f"--num-gpus={_positive_gpus(num_gpus)}",
        f"--port={cluster.port}",
        "--disable-usage-stats",
        "--include-dashboard=false",
        "--block",
        f"--max-startup-concurrency={limit}",
        f"--prestart-python-workers={limit}",
    ]


def ray_worker_argv(
    cluster: RayClusterConfig, num_gpus: int, worker_ip: str | None = None
) -> list[str]:
    limit = _startup_limit(cluster)
    worker_ip = worker_ip or _local_hsn_ip()
    return [
        _python_executable(),
        "-m",
        "exaserve.ray_start",
        f"--address={cluster.head_ip}:{cluster.port}",
        f"--node-ip-address={worker_ip}",
        f"--num-cpus={cluster.node_cpus}",
        f"--num-gpus={_positive_gpus(num_gpus)}",
        "--block",
        f"--max-startup-concurrency={limit}",
        f"--prestart-python-workers={limit}",
    ]


def server_argv(plan_path: str) -> list[str]:
    if not isinstance(plan_path, str) or not plan_path:
        raise ValueError("plan_path must be non-empty")
    return [_python_executable(), "-m", "exaserve.server_bootstrap", "--plan", plan_path]


def _python_executable() -> str:
    """Use the SiteProfile-qualified interpreter for managed descendants."""

    from ..plan.runtime_environment import QUALIFIED_PYTHON_ENV, require_non_shared_path

    executable = os.environ.get(QUALIFIED_PYTHON_ENV, "") or sys.executable
    if not os.path.isabs(executable):
        raise RuntimeError("managed Python executable must be absolute")
    if os.environ.get(QUALIFIED_PYTHON_ENV):
        require_non_shared_path(executable, name="qualified Python executable")
    return executable


def _positive_gpus(value: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"num_gpus must be a positive integer, got {value!r}")
    return value


__all__ = [
    "RayClusterConfig",
    "get_rank",
    "ray_child_environment",
    "ray_head_argv",
    "ray_node_ip",
    "ray_worker_argv",
    "server_argv",
]
