"""Intel XPU vendor (Aurora PVC). VALIDATED — this replicates the exact device
behavior the engines had inline before the vendor abstraction, so Aurora is
unchanged."""

from __future__ import annotations

import os
from typing import Dict, List, Optional

from .base import VendorBackend


class XPUVendor(VendorBackend):
    name = "xpu"

    def isolate_devices(self, device_ids: List[int], engine_name: str = "vllm") -> None:
        mask = ",".join(str(g) for g in device_ids) if device_ids else None
        if engine_name == "sglang":
            # SGLang's XPU init needs a VALID ONEAPI_DEVICE_SELECTOR (unlike vLLM);
            # tile isolation stays with ZE_AFFINITY_MASK, which composes with it.
            os.environ["ONEAPI_DEVICE_SELECTOR"] = "opencl:gpu;level_zero:gpu"
            if mask is not None:
                os.environ["ZE_AFFINITY_MASK"] = mask
        else:  # vLLM / default: ZE_AFFINITY_MASK set, ONEAPI_DEVICE_SELECTOR unset
            if mask is not None:
                os.environ["ZE_AFFINITY_MASK"] = mask
                os.environ.pop("ONEAPI_DEVICE_SELECTOR", None)
            else:
                os.environ.pop("ZE_AFFINITY_MASK", None)
                os.environ.pop("ONEAPI_DEVICE_SELECTOR", None)

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
