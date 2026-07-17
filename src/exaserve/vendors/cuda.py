"""NVIDIA CUDA vendor. UNTESTED — implemented from standard CUDA/Ray/vLLM
conventions; validate on NVIDIA hardware before trusting."""

from __future__ import annotations

import os
from typing import List

from .base import VendorBackend


class CUDAVendor(VendorBackend):
    name = "cuda"

    def isolate_devices(self, device_ids: List[int], engine_name: str = "vllm") -> None:
        if device_ids:
            os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(str(g) for g in device_ids)
        else:
            os.environ.pop("CUDA_VISIBLE_DEVICES", None)

    def torch_device(self) -> str:
        return "cuda"

    def default_gpus_per_node(self) -> int:
        return 8  # typical DGX/HGX; site config should override

    # engine_env / distributed_env: vLLM's default target is cuda and its Ray
    # compiled DAG works on NVIDIA, so no vendor env is needed here. SGLang picks
    # its own attention backend (flashinfer/triton) — sglang_default_attention
    # returns None (base default).
