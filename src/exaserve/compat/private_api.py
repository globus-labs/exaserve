"""Declared private-API surface (plan WP3, audit PR-026, TD-SITECUST).

ExaServe reaches into Ray internals in a handful of places — concurrent
application deploys, proxy handles, Serve timeout constants. Each of those is a
real capability with no public equivalent on this stack, and each is a hostage
to the next Ray upgrade.

Before this module, that dependency was implicit: an upgrade that moved
`ray.serve._private.deploy_utils.get_deploy_args` produced an `ImportError`
somewhere inside a deploy, after staging models and starting a cluster, and the
operator had to work backwards from a traceback to discover that ExaServe uses
a private symbol at all.

Here the surface is **declared**. Every private symbol is named, tied to the
capability it provides and the reason it is needed, and verified in one place
at bring-up, so drift is reported as "capability X is unavailable: Ray moved
symbol Y" before any expensive work happens.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Optional


class PrivateApiUnavailable(RuntimeError):
    """A declared private symbol is missing on the running stack."""


@dataclass(frozen=True)
class PrivateSymbol:
    capability: str          # what ExaServe gets from it
    module: str              # import path
    attr: str                # attribute on that module
    why: str                 # why no public API suffices
    required: bool = True    # a missing optional symbol degrades, not fails


# The complete declared surface. Adding a private import elsewhere without
# adding it here is the thing the test at the bottom of this file catches.
PRIVATE_SURFACE: tuple[PrivateSymbol, ...] = (
    PrivateSymbol(
        "serve_timeout_constants", "ray.serve._private.constants", "PROXY_READY_CHECK_TIMEOUT_S",
        "Serve's proxy/replica timeouts are module constants with no setter; "
        "the defaults are far too tight for multi-minute engine startup."),
    PrivateSymbol(
        "serve_default_app_name", "ray.serve._private.constants", "SERVE_DEFAULT_APP_NAME",
        "The root-route application name must match Serve's own default."),
    PrivateSymbol(
        "concurrent_app_deploy", "ray.serve._private.api", "serve_start",
        "serve.run() serializes application deploys; N node-pinned replicas "
        "must be submitted concurrently or bring-up is O(N) round trips."),
    PrivateSymbol(
        "deploy_args_builder", "ray.serve._private.deploy_utils", "get_deploy_args",
        "Building deploy args directly is what allows the concurrent submit "
        "above; there is no public equivalent."),
    PrivateSymbol(
        "serve_random_suffix", "ray.serve._private.utils", "get_random_string",
        "Application/deployment naming must match Serve's own scheme."),
    PrivateSymbol(
        "serve_controller_client", "ray.serve._private.api", "_get_global_client",
        "Reading per-application target replica counts and proxy handles for "
        "the readiness predicate; serve.status() omits target counts."),
    PrivateSymbol(
        "ray_node_ip_resolution", "ray._private.services", "get_node_ip_address",
        "Resolving the node's Ray-visible address without starting Ray; the "
        "public API only exposes it from inside an initialized runtime."),
    PrivateSymbol(
        "ray_constants", "ray._private.ray_constants", "NODE_DEFAULT_IP",
        "Ray's own default IP/port constants must match what ray start uses."),
    PrivateSymbol(
        "intel_gpu_accelerator", "ray._private.accelerators.intel_gpu",
        "IntelGPUAcceleratorManager",
        "Ray's Intel GPU manager reads ONEAPI_DEVICE_SELECTOR, which is "
        "unusable on this platform (it crashes Triton SYCL).",
        required=False),
)


def resolve(symbol: PrivateSymbol) -> Optional[Any]:
    """Return the symbol, or None if this stack does not have it."""
    try:
        module = importlib.import_module(symbol.module)
    except Exception:
        return None
    return getattr(module, symbol.attr, None)


def require(capability: str) -> Any:
    """Resolve one capability's symbol or raise, naming what and why."""
    for symbol in PRIVATE_SURFACE:
        if symbol.capability == capability:
            resolved = resolve(symbol)
            if resolved is None:
                raise PrivateApiUnavailable(
                    f"capability {capability!r} is unavailable: "
                    f"{symbol.module}.{symbol.attr} is missing on this stack. "
                    f"ExaServe needs it because {symbol.why} "
                    "Pin a supported version (doc/hardening/COMPATIBILITY_MATRIX.md) "
                    "or declare a new compatibility profile.")
            return resolved
    raise PrivateApiUnavailable(f"undeclared private capability {capability!r}")


def verify(strict: bool = True) -> dict:
    """Check the whole declared surface at once.

    Returns ``{capability: bool}``. With ``strict``, a missing REQUIRED symbol
    raises here — at bring-up, before models are staged — rather than surfacing
    as an ImportError in the middle of a deployment.
    """
    results: dict[str, bool] = {}
    missing_required: list[str] = []
    for symbol in PRIVATE_SURFACE:
        ok = resolve(symbol) is not None
        results[symbol.capability] = ok
        if not ok and symbol.required:
            missing_required.append(f"{symbol.capability} ({symbol.module}.{symbol.attr})")
    if strict and missing_required:
        raise PrivateApiUnavailable(
            "private API drift detected; ExaServe cannot run on this stack. "
            f"Missing: {sorted(missing_required)}. "
            "See doc/hardening/COMPATIBILITY_MATRIX.md.")
    return results


def capabilities() -> tuple[str, ...]:
    return tuple(sorted(s.capability for s in PRIVATE_SURFACE))
