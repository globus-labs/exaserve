"""AMD ROCm vendor (e.g. NCSA Delta MI100 / gfx908, Frontier/LUMI MI250X).
UNTESTED — implemented from ROCm/Ray/vLLM conventions; validate on AMD hardware.

Notes for the first AMD bring-up:
- Device isolation uses ``ROCR_VISIBLE_DEVICES`` (the ROCr-runtime mask, which
  takes precedence over the HIP-level ``HIP_VISIBLE_DEVICES``). A site that
  requires a different contract needs a separately named, hash-bearing vendor
  profile rather than an inherited environment override.
- PyTorch-ROCm reports the device as ``cuda`` (HIP masquerades as CUDA), so
  ``torch_device()`` returns ``cuda`` and vLLM's ROCm build auto-detects.
- MI250X/MI300 expose multiple GCDs; on those, ``device_ids`` are GCD indices.
"""

from __future__ import annotations

import os
from typing import List

from .base import VendorBackend


class ROCmVendor(VendorBackend):
    name = "rocm"

    def isolate_devices(self, device_ids: List[int], engine_name: str = "vllm") -> None:
        var = "ROCR_VISIBLE_DEVICES"
        if device_ids:
            os.environ[var] = ",".join(str(g) for g in device_ids)
        else:
            os.environ.pop(var, None)

    def torch_device(self) -> str:
        return "cuda"  # PyTorch-ROCm / HIP masquerades as cuda

    def default_gpus_per_node(self) -> int:
        return 8  # Delta MI100 node = 8 (site config should override)

    # engine_env / distributed_env: vLLM's ROCm wheel auto-detects the device and
    # its Ray path works without the XPU compiled-DAG workaround, so no vendor env
    # is set here — ROCm PATH / HSA_* belong in the site env script.
