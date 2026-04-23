# Installed into the frameworks-Python user site-packages by
# scripts/launch_cluster.sh as `sitecustomize.py`. Python's `site` module
# runs this file at every interpreter startup — before any user code.
#
# Purpose: raise Ray Serve's hardcoded health-check timeouts so the
# ServeController doesn't kill healthy proxy actors and replicas during
# the startup storm at 128+ nodes. Several Ray Serve submodules import
# the constants by name (`from .constants import HTTP_PROXY_TIMEOUT`),
# which creates local copies. Patching `constants` directly leaves those
# copies stale, so we hook `builtins.__import__` and patch each module
# as it is imported.
#
# See README.md § "What the launcher modifies outside the repo".

import builtins as _b

_orig = _b.__import__
_in_hook = False


def _aurora_import(name, *args, **kwargs):
    global _in_hook
    if _in_hook:
        return _orig(name, *args, **kwargs)
    _in_hook = True
    try:
        mod = _orig(name, *args, **kwargs)
        # Proxy timeouts — effectively disable health-check killing
        if hasattr(mod, 'HTTP_PROXY_TIMEOUT') and getattr(mod, 'HTTP_PROXY_TIMEOUT') == 60:
            mod.HTTP_PROXY_TIMEOUT = 3600
        if hasattr(mod, 'PROXY_HEALTH_CHECK_TIMEOUT_S') and getattr(mod, 'PROXY_HEALTH_CHECK_TIMEOUT_S') == 10.0:
            mod.PROXY_HEALTH_CHECK_TIMEOUT_S = 300.0
        if hasattr(mod, 'PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD') and getattr(mod, 'PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD') == 3:
            mod.PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 100
        # Replica timeouts — effectively disable health-check killing
        if hasattr(mod, 'DEFAULT_HEALTH_CHECK_TIMEOUT_S') and getattr(mod, 'DEFAULT_HEALTH_CHECK_TIMEOUT_S') == 30:
            mod.DEFAULT_HEALTH_CHECK_TIMEOUT_S = 600
        if hasattr(mod, 'DEFAULT_HEALTH_CHECK_PERIOD_S') and getattr(mod, 'DEFAULT_HEALTH_CHECK_PERIOD_S') == 10:
            mod.DEFAULT_HEALTH_CHECK_PERIOD_S = 120
        if hasattr(mod, 'REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD') and getattr(mod, 'REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD') == 3:
            mod.REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 100
        return mod
    finally:
        _in_hook = False


_b.__import__ = _aurora_import
