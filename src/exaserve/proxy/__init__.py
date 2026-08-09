"""
Proxy package for ExaServe.

Public rendering API used by the composition root:
    from exaserve.proxy import get_proxy
    from exaserve.proxy.base import BackendEndpoint

    proxy = get_proxy("litellm")        # or "haproxy"
    backends = [BackendEndpoint(...)]
    cfg = proxy.generate_config(backends, output_dir, **options)

Allocation discovery and canonical model routing belong exclusively to the
composition root.  Renderers accept already-resolved endpoints and never read
PBS state or a second deployment schema.

Process lifecycle is intentionally absent here.  The composition root owns
preflight, inherited ports, the exact process group, health evidence, and
bounded cleanup through ``RuntimeSupervisor``.

To add a new proxy backend:
    1. Create src/exaserve/proxy/my_proxy.py implementing ProxyBackend (base.py).
    2. Register it in _REGISTRY below.
    3. Add the kind to the relevant SiteProfile capability envelope.
"""

from .base import BackendEndpoint, ProxyBackend

_REGISTRY: dict[str, type[ProxyBackend]] = {}


def _register():
    from .litellm_proxy import LiteLLMProxy
    from .haproxy_proxy import HAProxyProxy
    from .nginx_proxy import NGINXProxy
    from .envoy_proxy import EnvoyProxy
    from .pingora_proxy import PingoraProxy

    _REGISTRY["litellm"] = LiteLLMProxy
    _REGISTRY["haproxy"] = HAProxyProxy
    _REGISTRY["nginx"] = NGINXProxy
    _REGISTRY["envoy"] = EnvoyProxy
    _REGISTRY["pingora"] = PingoraProxy


def get_proxy(proxy_type: str) -> ProxyBackend:
    """
    Return an instantiated ProxyBackend for the given type string.

    Args:
        proxy_type: One of "litellm", "haproxy", "nginx", "envoy", or
                    "pingora".

    Raises:
        ValueError if proxy_type is unknown.
    """
    if not _REGISTRY:
        _register()

    cls = _REGISTRY.get(proxy_type)
    if cls is None:
        available = ", ".join(sorted(_REGISTRY))
        raise ValueError(f"Unknown proxy type '{proxy_type}'. Available: {available}")
    return cls()


__all__ = ["get_proxy", "ProxyBackend", "BackendEndpoint"]
