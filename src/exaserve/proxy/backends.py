"""
Backend endpoint discovery for the proxy layer.

Reads PBS_NODEFILE and DeploymentConfig to produce a flat list of
BackendEndpoint objects. All proxy implementations share this module --
discovery logic is never duplicated.
"""

import os
from typing import Optional

from ..model_paths import get_model_route_name
from .base import BackendEndpoint


def _read_nodefile(pbs_nodefile: str) -> list[str]:
    """Return a sorted, deduplicated list of hostnames from a PBS nodefile."""
    with open(pbs_nodefile) as f:
        nodes = sorted(set(line.strip() for line in f if line.strip()))
    if not nodes:
        raise RuntimeError(f"No nodes found in PBS_NODEFILE ('{pbs_nodefile}').")
    return nodes


def discover_backends(
    deploy_config,          # schemas.DeploymentConfig -- not imported to keep proxy/ Ray-free
    backend_port: int = 8000,
    pbs_nodefile: Optional[str] = None,
) -> list[BackendEndpoint]:
    """
    Build the list of BackendEndpoint objects for the proxy.

    Each node in PBS_NODEFILE becomes one BackendEndpoint per model_id,
    because Ray Serve runs a unified HTTP proxy on port 8000 on every node
    that can route to any model replica.

    Args:
        deploy_config:  DeploymentConfig instance (from schemas.py).
        backend_port:   Port where Ray Serve HTTP proxy listens (default 8000).
        pbs_nodefile:   Path to the PBS nodefile. Defaults to $PBS_NODEFILE env var.

    Returns:
        List of BackendEndpoint, one per (node, model_id) pair.

    Raises:
        RuntimeError if PBS_NODEFILE is not set/found or is empty.
    """
    nodefile = pbs_nodefile or os.environ.get("PBS_NODEFILE")
    if not nodefile:
        raise RuntimeError(
            "PBS_NODEFILE is not set. Pass pbs_nodefile= explicitly or run inside a PBS job."
        )
    if not os.path.exists(nodefile):
        raise RuntimeError(f"PBS_NODEFILE '{nodefile}' does not exist.")

    nodes = _read_nodefile(nodefile)
    use_root_route = len(deploy_config.model_configs) == 1
    shard_aware = os.environ.get("EXASERVE_PP_SHARD_AWARE", "0") == "1"

    endpoints: list[BackendEndpoint] = []
    for node in nodes:
        for mc in deploy_config.model_configs:
            model_id = mc.model_id
            n_rep = getattr(mc, "num_replicas", 0) or 0
            is_shard = (shard_aware and getattr(mc, "pipeline_parallel_size", 1) > 1
                        and n_rep > 1)
            if is_shard:
                # Served as N node-pinned single-replica deployments at routes
                # /<route>_r{0..N-1}; the proxy round-robins across them.
                path_prefix = f"/{get_model_route_name(model_id)}"
                shard_replicas = n_rep
            else:
                path_prefix = "" if use_root_route else f"/{get_model_route_name(model_id)}"
                shard_replicas = 0
            endpoints.append(
                BackendEndpoint(
                    host=node,
                    port=backend_port,
                    model_id=model_id,
                    path_prefix=path_prefix,
                    shard_replicas=shard_replicas,
                )
            )

    n_shard = sum(1 for ep in endpoints if ep.shard_replicas > 0)
    print(
        f"[ProxyBackends] Discovered {len(nodes)} node(s) × {len(deploy_config.model_configs)} "
        f"model(s) = {len(endpoints)} backend endpoint(s) on port {backend_port}"
        + (f" (shard-aware PP: {endpoints[0].shard_replicas} replica routes)" if n_shard else "")
        + "."
    )
    return endpoints
