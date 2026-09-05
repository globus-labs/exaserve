"""Import-light construction of Ray actor runtime environments.

Ray itself is deliberately not imported here.  The deployment composition
root uses this boundary before it asks Ray to create an actor, and package
qualification can verify the identity projection without installing the
entire serving stack.
"""

from __future__ import annotations

import os
import re
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
    "PYTHONNOUSERSITE",
    "PYTHONSAFEPATH",
    "PYTHONDONTWRITEBYTECODE",
    "PYTHONPYCACHEPREFIX",
    "HOME",
    "TMPDIR",
    "TMP",
    "TEMP",
    "XDG_CACHE_HOME",
    "XDG_CONFIG_HOME",
    "XDG_DATA_HOME",
    "XDG_RUNTIME_DIR",
    "IPYTHONDIR",
    "JUPYTER_CONFIG_DIR",
    "NUMBA_CACHE_DIR",
    "TORCH_EXTENSIONS_DIR",
    "MPLCONFIGDIR",
    "HF_HOME",
    "HF_HUB_OFFLINE",
    "HF_DATASETS_OFFLINE",
    "TRANSFORMERS_OFFLINE",
    "HF_HUB_CACHE",
    "HUGGINGFACE_HUB_CACHE",
    "TRANSFORMERS_CACHE",
    "TORCH_HOME",
    "TRITON_CACHE_DIR",
    "VLLM_CACHE_ROOT",
    "RAY_TMPDIR",
    "EXASERVE_LOCAL_RUNTIME_ROOT",
    "EXASERVE_LOCAL_STATE_ROOT",
    "EXASERVE_QUALIFIED_PYTHON",
    "EXASERVE_QUALIFIED_PYTHON_SHA256",
    "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH",
    "EXASERVE_COMPAT_OVERLAY_ROOT",
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
    "EXASERVE_COMPAT_SOURCES_NODE_PROFILE",
    "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST",
    "EXASERVE_PLAN_PATH",
    "EXASERVE_ALLOCATION_BINDING_PATH",
    SOCKET_ENV,
)

_PROTECTED_ACTOR_ENV = frozenset(
    {
        "PYTHONPATH",
        "PYTHONNOUSERSITE",
        "PYTHONSAFEPATH",
        "PYTHONPYCACHEPREFIX",
        "HOME",
        "TMPDIR",
        "TMP",
        "TEMP",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_RUNTIME_DIR",
        "IPYTHONDIR",
        "JUPYTER_CONFIG_DIR",
        "NUMBA_CACHE_DIR",
        "TORCH_EXTENSIONS_DIR",
        "MPLCONFIGDIR",
        "HF_HOME",
        "HF_HUB_OFFLINE",
        "HF_DATASETS_OFFLINE",
        "TRANSFORMERS_OFFLINE",
        "HF_HUB_CACHE",
        "HUGGINGFACE_HUB_CACHE",
        "TRANSFORMERS_CACHE",
        "TORCH_HOME",
        "TRITON_CACHE_DIR",
        "VLLM_CACHE_ROOT",
        "RAY_TMPDIR",
        "EXASERVE_LOCAL_RUNTIME_ROOT",
        "EXASERVE_LOCAL_STATE_ROOT",
        "EXASERVE_SHARED_ROOTS",
        "EXASERVE_QUALIFIED_PYTHON",
        "EXASERVE_QUALIFIED_PYTHON_SHA256",
        "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH",
        "EXASERVE_COMPAT_OVERLAY_ROOT",
        "EXASERVE_DEPLOYMENT_ID",
        "EXASERVE_GENERATION",
        "EXASERVE_VENDOR",
        "EXASERVE_PLAN_HASH",
        "EXASERVE_SITE_PROFILE_HASH",
        "EXASERVE_SITE_PROFILE_PATH",
        "EXASERVE_ALLOCATION_BINDING_HASH",
        "EXASERVE_COMPAT_PROFILE_ID",
        "EXASERVE_COMPAT_MANIFEST_HASH",
        "EXASERVE_COMPAT_SOURCES_NODE_PROFILE",
        "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST",
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
    dynamic_receipt_owner: bool = False,
) -> dict[str, dict[str, str]]:
    """Project canonical identity and Aurora settings into one Serve actor.

    ``receipt_owner_rank`` binds both receipt fields atomically.  The
    deployment process owns a socket on its own node; forwarding that path to
    an actor placed on another planned rank would make engine receipts
    undeliverable.
    """
    if type(dynamic_receipt_owner) is not bool:
        raise TypeError("dynamic_receipt_owner must be a boolean")
    if dynamic_receipt_owner and receipt_owner_rank is not None:
        raise ValueError("dynamic receipt ownership cannot also declare receipt_owner_rank")

    env_vars = {key: value for key in _INHERITED_ACTOR_ENV if (value := os.environ.get(key))}
    from .plan.runtime_environment import (
        LOCAL_RUNTIME_ROOT_ENV,
        QUALIFIED_PYTHON_HASH_ENV,
        QUALIFIED_PYTHON_PROFILE_ENV,
        RuntimePathError,
        path_is_shared,
        shared_path_tokens,
    )

    runtime_root = env_vars.get(LOCAL_RUNTIME_ROOT_ENV, "")
    if not runtime_root:
        raise RuntimeError(
            f"actor runtime requires published {LOCAL_RUNTIME_ROOT_ENV}; "
            "shared-repository fallback is forbidden"
        )
    if env_vars.get("PYTHONNOUSERSITE") != "1":
        raise RuntimeError("actor runtime requires PYTHONNOUSERSITE=1")
    if env_vars.get("PYTHONSAFEPATH") != "1":
        raise RuntimeError("actor runtime requires PYTHONSAFEPATH=1")
    if re.fullmatch(r"[0-9a-f]{64}", env_vars.get(QUALIFIED_PYTHON_HASH_ENV, "")) is None:
        raise RuntimeError("actor runtime requires qualified Python hash evidence")
    if env_vars.get(QUALIFIED_PYTHON_PROFILE_ENV) != env_vars.get("EXASERVE_SITE_PROFILE_HASH"):
        raise RuntimeError("actor qualified Python evidence belongs to another SiteProfile")
    if env_vars.get("EXASERVE_COMPAT_SOURCES_NODE_PROFILE") != env_vars.get(
        "EXASERVE_COMPAT_PROFILE_ID"
    ):
        raise RuntimeError("actor compatibility source proof has the wrong profile")
    if env_vars.get("EXASERVE_COMPAT_SOURCES_NODE_MANIFEST") != env_vars.get(
        "EXASERVE_COMPAT_MANIFEST_HASH"
    ):
        raise RuntimeError("actor compatibility source proof has the wrong manifest")
    path_keys = {
        "PYTHONPATH",
        "HOME",
        "TMPDIR",
        "TMP",
        "TEMP",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_RUNTIME_DIR",
        "IPYTHONDIR",
        "JUPYTER_CONFIG_DIR",
        "NUMBA_CACHE_DIR",
        "TORCH_EXTENSIONS_DIR",
        "MPLCONFIGDIR",
        "HF_HOME",
        "HF_HUB_CACHE",
        "HUGGINGFACE_HUB_CACHE",
        "TRANSFORMERS_CACHE",
        "TORCH_HOME",
        "TRITON_CACHE_DIR",
        "VLLM_CACHE_ROOT",
        "RAY_TMPDIR",
        "EXASERVE_LOCAL_RUNTIME_ROOT",
        "EXASERVE_LOCAL_STATE_ROOT",
        "EXASERVE_QUALIFIED_PYTHON",
        "EXASERVE_COMPAT_OVERLAY_ROOT",
        "EXASERVE_SITE_PROFILE_PATH",
        "EXASERVE_PLAN_PATH",
        "EXASERVE_ALLOCATION_BINDING_PATH",
    }
    try:
        for key in path_keys:
            value = env_vars.get(key, "")
            for item in value.split(os.pathsep) if key == "PYTHONPATH" else (value,):
                if item and item.startswith("/") and path_is_shared(item):
                    raise RuntimePathError(f"actor environment {key} names shared storage: {item}")
    except RuntimePathError as exc:
        raise RuntimeError(str(exc)) from exc
    if dynamic_receipt_owner:
        # A native multi-replica Serve deployment lets Serve place each actor.
        # Do not leak the deployment driver's rank-zero socket into those
        # actors; each replica resolves its actual bound rank before engine
        # import and installs that rank's authenticated ingress path.
        env_vars.pop("EXASERVE_RECEIPT_RANK", None)
        env_vars.pop(SOCKET_ENV, None)

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

    # Future engine options must not bypass the explicit protected list by
    # inventing another name for a shared path.
    try:
        for key, value in env_vars.items():
            shared = shared_path_tokens(value)
            if shared:
                raise RuntimePathError(
                    f"actor environment {key} names shared storage: {list(shared)}"
                )
    except RuntimePathError as exc:
        raise RuntimeError(str(exc)) from exc

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
