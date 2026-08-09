"""Import-light construction of Ray actor runtime environments.

Ray itself is deliberately not imported here.  The deployment composition
root uses this boundary before it asks Ray to create an actor, and package
qualification can verify the identity projection without installing the
entire serving stack.
"""

from __future__ import annotations

import os
from typing import Optional

from .compat.collector import deployment_scope
from .compat.local_ingress import SOCKET_ENV, socket_path_for
from .compat.profile import (
    MULTIPROC_WORKER_PATCH_GATE,
    PP_PATCH_GATE,
    RAY_WORKER_PATCH_GATE,
)


_INHERITED_ACTOR_ENV = (
    "PYTHONPATH",
    PP_PATCH_GATE,
    RAY_WORKER_PATCH_GATE,
    MULTIPROC_WORKER_PATCH_GATE,
    "EXASERVE_VLLM_PATCH_VERBOSE",
    "EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG",
    "EXASERVE_XPU_VLLM_FORCE_RAY_CHANNEL_TYPE",
    "RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR",
    "VLLM_USE_RAY_COMPILED_DAG_CHANNEL_TYPE",
    "VLLM_USE_RAY_WRAPPED_PP_COMM",
    "ZE_FLAT_DEVICE_HIERARCHY",
    "VLLM_TARGET_DEVICE",
    "EXASERVE_SCALING_TRACE",
    "RAYON_NUM_THREADS",
    "TOKENIZERS_PARALLELISM",
    "EXASERVE_DEPLOYMENT_ID",
    "EXASERVE_GENERATION",
    "EXASERVE_VENDOR",
    "EXASERVE_PLAN_HASH",
    "EXASERVE_SITE_PROFILE_HASH",
    "EXASERVE_SITE_PROFILE_PATH",
    "EXASERVE_ALLOCATION_BINDING_HASH",
    "EXASERVE_COMPAT_PROFILE_ID",
    "EXASERVE_COMPAT_MANIFEST_HASH",
    "EXASERVE_PLAN_PATH",
    "EXASERVE_ALLOCATION_BINDING_PATH",
    SOCKET_ENV,
)

_PROTECTED_ACTOR_ENV = frozenset(
    {
        "EXASERVE_DEPLOYMENT_ID",
        "EXASERVE_GENERATION",
        "EXASERVE_VENDOR",
        "EXASERVE_PLAN_HASH",
        "EXASERVE_SITE_PROFILE_HASH",
        "EXASERVE_SITE_PROFILE_PATH",
        "EXASERVE_ALLOCATION_BINDING_HASH",
        "EXASERVE_COMPAT_PROFILE_ID",
        "EXASERVE_COMPAT_MANIFEST_HASH",
        "EXASERVE_PLAN_PATH",
        "EXASERVE_ALLOCATION_BINDING_PATH",
        "EXASERVE_COMPAT_ROLE",
        "EXASERVE_RECEIPT_RANK",
        SOCKET_ENV,
    }
)


def build_actor_runtime_env(
    extra_env_vars: Optional[dict[str, str]] = None,
    *,
    receipt_owner_rank: Optional[int] = None,
) -> dict[str, dict[str, str]]:
    """Project canonical identity and Aurora settings into one Serve actor.

    ``receipt_owner_rank`` binds both receipt fields atomically.  The
    deployment process owns a socket on its own node; forwarding that path to
    an actor placed on another planned rank would make engine receipts
    undeliverable.
    """
    env_vars = {key: value for key in _INHERITED_ACTOR_ENV if (value := os.environ.get(key))}

    if extra_env_vars:
        if not isinstance(extra_env_vars, dict) or any(
            not isinstance(key, str) or not key or not isinstance(value, str)
            for key, value in extra_env_vars.items()
        ):
            raise TypeError("extra actor environment must be a map of non-empty strings")
        protected = sorted(set(extra_env_vars) & _PROTECTED_ACTOR_ENV)
        if protected:
            raise ValueError(
                f"extra actor environment cannot override canonical identity fields: {protected}"
            )
        env_vars.update(extra_env_vars)

    # The scope and role used by the head are authoritative.  Do not preserve
    # a raw, differently-normalized ambient spelling or let an extra option
    # replace the identity after verification.
    env_vars["EXASERVE_DEPLOYMENT_ID"] = deployment_scope()
    env_vars["EXASERVE_COMPAT_ROLE"] = "replica"

    if receipt_owner_rank is not None:
        if (
            isinstance(receipt_owner_rank, bool)
            or not isinstance(receipt_owner_rank, int)
            or receipt_owner_rank < 0
        ):
            raise ValueError("receipt_owner_rank must be a non-negative integer or null")
        deployment_id = env_vars.get("EXASERVE_DEPLOYMENT_ID", "")
        generation_text = env_vars.get("EXASERVE_GENERATION", "")
        try:
            generation = int(generation_text)
        except ValueError as exc:
            raise RuntimeError("actor receipt binding requires an integer generation") from exc
        if not deployment_id or generation < 0:
            raise RuntimeError("actor receipt binding requires deployment identity and generation")
        env_vars["EXASERVE_RECEIPT_RANK"] = str(receipt_owner_rank)
        env_vars[SOCKET_ENV] = socket_path_for(
            deployment_id,
            generation,
            owner_rank=receipt_owner_rank,
        )

    return {"env_vars": env_vars}
