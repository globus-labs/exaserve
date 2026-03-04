"""
Proxy package for Aurora Ray Server.

Public API used by driver.py:
    from proxy import get_proxy
    from proxy.backends import discover_backends

    proxy = get_proxy("litellm")        # or "haproxy"
    backends = discover_backends(deploy_config)
    cfg = proxy.generate_config(backends, output_dir, **options)
    proc = proxy.start(cfg, "0.0.0.0", 4000)
    proxy.health_check("127.0.0.1", 4000)
    ...
    proxy.stop(proc)

To add a new proxy backend:
    1. Create src/proxy/my_proxy.py implementing ProxyBackend (base.py).
    2. Register it in _REGISTRY below.
    3. Set type: my_proxy in the experiment config's proxy_config section.
"""

from proxy.base import BackendEndpoint, ProxyBackend

_REGISTRY: dict[str, type[ProxyBackend]] = {}


def _register():
    from proxy.litellm_proxy import LiteLLMProxy
    from proxy.haproxy_proxy import HAProxyProxy
    _REGISTRY["litellm"] = LiteLLMProxy
    _REGISTRY["haproxy"] = HAProxyProxy


def get_proxy(proxy_type: str) -> ProxyBackend:
    """
    Return an instantiated ProxyBackend for the given type string.

    Args:
        proxy_type: One of "litellm", "haproxy", or any registered custom type.

    Raises:
        ValueError if proxy_type is unknown.
    """
    if not _REGISTRY:
        _register()

    cls = _REGISTRY.get(proxy_type)
    if cls is None:
        available = ", ".join(sorted(_REGISTRY))
        raise ValueError(
            f"Unknown proxy type '{proxy_type}'. Available: {available}"
        )
    return cls()


__all__ = ["get_proxy", "ProxyBackend", "BackendEndpoint"]
