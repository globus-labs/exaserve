"""Intel XPU vendor for Aurora PVC.

Aurora's site contract uses ``ZE_AFFINITY_MASK`` exclusively and explicitly
forbids introducing ``ONEAPI_DEVICE_SELECTOR``.  SGLang is not in the qualified
Aurora engine envelope; allowing its historical selector workaround here would
let a direct engine call silently violate the site boundary after the launcher
had sanitized the environment.
"""

from __future__ import annotations

import os
from typing import Dict, List, Optional

from .base import VendorBackend


class XPUVendor(VendorBackend):
    name = "xpu"

    def isolate_devices(self, device_ids: List[int], engine_name: str = "vllm") -> None:
        mask = ",".join(str(g) for g in device_ids) if device_ids else None
        # The engine name cannot weaken a site-wide device-isolation rule.
        # Unsupported XPU/SGLang combinations are rejected by SiteProfile
        # compilation before this boundary; this method remains fail-closed if
        # called directly.
        os.environ.pop("ONEAPI_DEVICE_SELECTOR", None)
        if mask is not None:
            os.environ["ZE_AFFINITY_MASK"] = mask
        else:
            os.environ.pop("ZE_AFFINITY_MASK", None)

    def torch_device(self) -> str:
        return "xpu"

    def default_gpus_per_node(self) -> int:
        return 12  # 6 PVC × 2 tiles

    def distributed_env(self, engine_name: str = "vllm") -> Dict[str, str]:
        if engine_name == "sglang":
            return {}
        # vLLM PP on XPU: the compiled DAG crashes XPU workers in Ray's
        # accelerator context; force the uncompiled fallback + auto channel.
        return {
            "EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG": "1",
            "EXASERVE_XPU_VLLM_FORCE_RAY_CHANNEL_TYPE": "auto",
        }

    def sglang_default_attention(self) -> Optional[str]:
        # torch_native is the only numerically-correct backend on Xe-HPC (PVC);
        # the fused intel_xpu kernel is Battlemage-tuned and wrong.
        return "torch_native"
