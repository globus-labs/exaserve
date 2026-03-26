"""
Backend endpoint discovery for the proxy layer.

Reads PBS_NODEFILE and DeploymentConfig to produce a flat list of
BackendEndpoint objects. All proxy implementations share this module --
discovery logic is never duplicated.
"""

import os
from typing import Optional

from model_paths import get_model_route_name
from proxy.base import BackendEndpoint


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
    model_ids = [mc.model_id for mc in deploy_config.model_configs]
    use_root_route = len(model_ids) == 1

    endpoints: list[BackendEndpoint] = []
    for node in nodes:
        for model_id in model_ids:
            path_prefix = "" if use_root_route else f"/{get_model_route_name(model_id)}"
            endpoints.append(
                BackendEndpoint(
                    host=node,
                    port=backend_port,
                    model_id=model_id,
                    path_prefix=path_prefix,
                )
            )

    print(
        f"[ProxyBackends] Discovered {len(nodes)} node(s) × {len(model_ids)} model(s) "
        f"= {len(endpoints)} backend endpoint(s) on port {backend_port}."
    )
    return endpoints
