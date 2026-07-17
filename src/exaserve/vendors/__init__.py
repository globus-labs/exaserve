"""
Pluggable accelerator vendors.

    from exaserve.vendors import get_vendor
    vendor = get_vendor()                 # EXASERVE_VENDOR, default "xpu"
    vendor.isolate_devices([0, 1], "vllm")

Selected by ``EXASERVE_VENDOR`` (default ``xpu`` so Aurora is unchanged). To add
a vendor, implement ``VendorBackend`` (base.py) and register it in ``_register``.

Design doc: doc/design/vendor_site_abstraction.md
"""

from __future__ import annotations

import os
from typing import Dict, Type

from .base import VendorBackend  # noqa: F401

_REGISTRY: Dict[str, Type[VendorBackend]] = {}


def _register() -> None:
    if _REGISTRY:
        return
    from .xpu import XPUVendor
    from .cuda import CUDAVendor
    from .rocm import ROCmVendor

    _REGISTRY["xpu"] = XPUVendor
    _REGISTRY["cuda"] = CUDAVendor
    _REGISTRY["rocm"] = ROCmVendor


def get_vendor(name: str | None = None) -> VendorBackend:
    """Return an instantiated VendorBackend.

    Args:
        name: vendor key; defaults to ``EXASERVE_VENDOR`` env, then ``xpu``.
    """
    _register()
    key = (name or os.environ.get("EXASERVE_VENDOR", "xpu")).lower()
    if key not in _REGISTRY:
        raise KeyError(f"unknown vendor {key!r}; registered: {sorted(_REGISTRY)}")
    return _REGISTRY[key]()


def available_vendors() -> list[str]:
    _register()
    return sorted(_REGISTRY)


__all__ = ["get_vendor", "available_vendors", "VendorBackend"]
