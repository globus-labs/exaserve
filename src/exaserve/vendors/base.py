"""
Abstract interface for pluggable accelerator vendors.

Mirrors ``exaserve.engines`` / ``exaserve.proxy``: a small registry of backends,
selected by ``EXASERVE_VENDOR`` (default ``xpu`` so Aurora is unchanged). The
vendor owns everything accelerator-specific that used to live inline in the
engine ``create()`` methods:

- **Device isolation** — which env var pins a replica to its GPU/tile(s):
  Intel XPU ``ZE_AFFINITY_MASK`` (+ ``ONEAPI_DEVICE_SELECTOR`` quirks),
  NVIDIA ``CUDA_VISIBLE_DEVICES``, AMD ``ROCR_VISIBLE_DEVICES``.
- **PyTorch device string** — ``xpu`` vs ``cuda`` (ROCm also uses ``cuda``).
- **Vendor env defaults** and **distributed/PP workarounds** (the XPU
  compiled-DAG env lives here, gated to XPU so it never leaks to CUDA/ROCm).
- **GPUs-per-node default** (a site may override).

Design doc: doc/design/vendor_site_abstraction.md

IMPORTANT: only Intel XPU is validated (Aurora). CUDA and ROCm backends are
implemented from vendor conventions but UNTESTED — see the per-class notes.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional


class VendorBackend(ABC):
    """Contract for accelerator vendors. Stateless — safe to instantiate per replica."""

    #: registry key, e.g. "xpu"
    name: str = "base"

    # ---- device isolation ----------------------------------------------------

    @abstractmethod
    def isolate_devices(self, device_ids: List[int], engine_name: str = "vllm") -> None:
        """Pin this replica process to ``device_ids`` via the vendor's visibility
        env var. ``engine_name`` lets a vendor apply engine-specific quirks (e.g.
        SGLang needs a valid ``ONEAPI_DEVICE_SELECTOR`` on XPU where vLLM needs it
        unset). Called by the engine before it instantiates the model."""

    # ---- device facts --------------------------------------------------------

    def torch_device(self) -> str:
        """PyTorch/engine device string. ROCm uses ``cuda`` (HIP masquerades)."""
        return "cuda"

    def default_gpus_per_node(self) -> int:
        """Fallback GPUs/tiles per node when the site config doesn't set it."""
        return 8

    # ---- env -----------------------------------------------------------------

    def engine_env(self) -> Dict[str, str]:
        """Vendor env defaults applied (via setdefault) before engine create.
        Kept minimal; the site env script is the primary source of vendor env."""
        return {}

    def distributed_env(self, engine_name: str = "vllm") -> Dict[str, str]:
        """Vendor env for multi-node / pipeline-parallel replicas, applied via
        setdefault. Only XPU currently needs any (compiled-DAG workarounds)."""
        return {}

    def sglang_default_attention(self) -> Optional[str]:
        """Default SGLang ``attention_backend`` for this vendor, or None to let
        SGLang choose. XPU forces ``torch_native`` (fused kernel is wrong on PVC)."""
        return None

    def preflight(self) -> None:
        """Optional vendor sanity check at launch. Default: no-op."""
        return None

    # ---- helpers for subclasses ---------------------------------------------

    @staticmethod
    def _set_or_pop(var: str, value: Optional[str]) -> None:
        if value is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = value
