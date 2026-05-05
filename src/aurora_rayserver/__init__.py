"""aurora-rayserver: Ray Serve helpers and scale patches for Aurora.

Public API (all importable as ``from aurora_rayserver import …``):

    Configuration / schema:
        DeploymentConfig, ModelConfig, ProxyConfig
        load_deployment_config, load_proxy_config

    Server entry points (programmatic use):
        serve  (alias for the script main; reads a config and starts the cluster)

    Patch installer:
        apply_all  (must be called BEFORE any ``import ray.serve`` / ``import vllm``;
                    see ``aurora_rayserver.patches.apply_all`` for the why-explicit
                    rationale: Ray instantiates ``ray.serve._private`` classes during
                    package import, so monkey-patching after the fact is too late.)

The launcher (``aurora-launch-cluster``) and ``aurora_rayserver.driver`` both
call ``apply_all()`` themselves at the right point in the lifecycle. If you
embed the package in your own code, call it explicitly before importing
ray.serve or vllm.
"""

from .schemas import (  # noqa: F401
    DeploymentConfig,
    ModelConfig,
    ProxyConfig,
    load_deployment_config,
    load_proxy_config,
)
from .patches import apply_all  # noqa: F401

__all__ = [
    "DeploymentConfig",
    "ModelConfig",
    "ProxyConfig",
    "load_deployment_config",
    "load_proxy_config",
    "apply_all",
]

__version__ = "0.1.0"
