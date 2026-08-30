import asyncio
import os
import re
import sys
import threading

# Every top-level compatibility mutation in this module must map to one exact
# manifest patch ID.  A static regression compares this registry with the
# executed module-level calls, preventing a helper from becoming an invisible
# third patch policy outside CompatibilityProfile and its receipts.
DECLARED_PATCH_FUNCTIONS = {
    "_patch_vllm_layer_lookup": "SC-01",
    "_patch_vllm_bind_kv_cache": "SC-02",
    "_patch_vllm_forward_context_aliases": "SC-03",
    "_patch_vllm_attention_context": "SC-04",
    "_patch_vllm_gpu_model_runner_attn_backend": "SC-05",
    "_patch_vllm_ray_worker_runtime_env": "EW-01",
    "_patch_vllm_multiproc_worker_identity": "EW-02",
    "_patch_vllm_ray_worker_identity": "EW-03",
    "_patch_vllm_ray_executor_channel_type": "SC-09",
    "_patch_vllm_ray_executor_uncompiled_pp": "SC-10",
    "_patch_ray_oneapi_selector": "SC-11",
    "_patch_ray_accelerator_context": "SC-12",
    "_install_ray_serve_timeout_import_hook": "RS-01",
}


def _patch_log(message: str) -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_VERBOSE") == "1":
        print(f"[AuroraPatch pid={os.getpid()}] {message}", flush=True)


def _local_accelerator_id_list(accelerator_manager) -> list[str]:
    num_devices = accelerator_manager.get_current_node_num_accelerators()
    if num_devices <= 0:
        return []
    return [str(device_id) for device_id in range(num_devices)]


def _parse_numeric_ids(values: list[str]) -> list[int] | None:
    if any(not value.isdigit() for value in values):
        return None
    return [int(value) for value in values]


def _canonicalize_layer_name(layer_name: str) -> str:
    return re.sub(r"(?<=\.)\d+(?=\.|$)", "*", layer_name)


def _extract_layer_ordinal(layer_name: str) -> int | None:
    for pattern in (
        r"\.layers\.(\d+)(?:\.|$)",
        r"\.h\.(\d+)(?:\.|$)",
        r"\.blocks\.(\d+)(?:\.|$)",
    ):
        match = re.search(pattern, layer_name)
        if match is not None:
            return int(match.group(1))
    return None


def _layer_names_compatible(existing_key: str, layer_name: str) -> bool:
    if existing_key == layer_name:
        return True
    if _canonicalize_layer_name(existing_key) != _canonicalize_layer_name(layer_name):
        return False

    existing_ordinal = _extract_layer_ordinal(existing_key)
    target_ordinal = _extract_layer_ordinal(layer_name)
    if existing_ordinal is not None and target_ordinal is not None:
        return existing_ordinal == target_ordinal
    return True


def _find_stage_relative_key(mapping: dict, layer_name: str) -> str | None:
    target_ordinal = _extract_layer_ordinal(layer_name)
    if target_ordinal is None:
        return None

    candidates = []
    canonical_name = _canonicalize_layer_name(layer_name)
    for existing_key in mapping:
        if _canonicalize_layer_name(existing_key) != canonical_name:
            continue
        existing_ordinal = _extract_layer_ordinal(existing_key)
        if existing_ordinal is None:
            continue
        candidates.append((existing_ordinal, existing_key))

    if not candidates:
        return None

    candidates.sort()
    relative_key = candidates[target_ordinal % len(candidates)][1]
    if relative_key != layer_name:
        _patch_log(f"Using stage-relative layer alias: {layer_name} -> {relative_key}")
    return relative_key


def _find_layer_mapping_key(mapping: dict, layer_name: str) -> str | None:
    if layer_name in mapping:
        return layer_name

    for existing_key in mapping:
        if _layer_names_compatible(existing_key, layer_name):
            return existing_key

    stage_relative_key = _find_stage_relative_key(mapping, layer_name)
    if stage_relative_key is not None:
        return stage_relative_key

    return None


def _ensure_layer_aliases(mapping: dict, reference_names: list[str]) -> None:
    for layer_name in reference_names:
        existing_key = _find_layer_mapping_key(mapping, layer_name)
        if existing_key is None:
            continue

        value = mapping[existing_key]
        mapping.setdefault(layer_name, value)


def _iter_canonical_matches(mapping: dict, layer_name: str):
    seen_ids = set()
    matched = False
    for existing_key, value in list(mapping.items()):
        if not _layer_names_compatible(existing_key, layer_name):
            continue
        value_id = id(value)
        if value_id in seen_ids:
            continue
        seen_ids.add(value_id)
        matched = True
        yield existing_key, value

    if matched:
        return

    stage_relative_key = _find_stage_relative_key(mapping, layer_name)
    if stage_relative_key is None or stage_relative_key not in mapping:
        return

    value = mapping[stage_relative_key]
    value_id = id(value)
    if value_id in seen_ids:
        return
    yield stage_relative_key, value


def _get_layer_kv_cache(layer, virtual_engine: int):
    try:
        kv_cache = layer.kv_cache[virtual_engine]
    except Exception:
        return None
    return kv_cache


def _kv_cache_is_bound(kv_cache) -> bool:
    if kv_cache is None:
        return False
    try:
        return kv_cache.numel() > 0
    except Exception:
        return False


def _resolve_layer_and_kv_cache(mapping: dict, layer_name: str, virtual_engine: int):
    fallback = None
    for existing_key, layer in _iter_canonical_matches(mapping, layer_name):
        kv_cache = _get_layer_kv_cache(layer, virtual_engine)
        if fallback is None:
            fallback = (existing_key, layer, kv_cache)
        if _kv_cache_is_bound(kv_cache):
            return existing_key, layer, kv_cache
    return fallback or (None, None, None)


def _find_backend_fallback_layer(mapping: dict, layer_name: str):
    canonical_name = _canonicalize_layer_name(layer_name)
    for existing_key, layer in mapping.items():
        if _canonicalize_layer_name(existing_key) == canonical_name:
            return layer
    return next(iter(mapping.values()), None)


def _find_model_fallback_layer(model_runner, layer_type, layer_name: str):
    static_forward_context = (
        getattr(
            getattr(getattr(model_runner, "vllm_config", None), "compilation_config", None),
            "static_forward_context",
            None,
        )
        or {}
    )

    exact_match = None
    compatible_match = None
    for existing_key, layer in static_forward_context.items():
        if not isinstance(layer, layer_type):
            continue
        if existing_key == layer_name:
            return existing_key, layer
        if compatible_match is None and _layer_names_compatible(existing_key, layer_name):
            compatible_match = (existing_key, layer)
        if exact_match is None:
            exact_match = (existing_key, layer)

    if compatible_match is not None:
        return compatible_match
    if exact_match is not None:
        return exact_match

    model = getattr(model_runner, "model", None)
    if model is None or not hasattr(model, "named_modules"):
        return None, None

    exact_match = None
    compatible_match = None
    for existing_key, layer in model.named_modules():
        if not isinstance(layer, layer_type):
            continue
        if existing_key == layer_name:
            return existing_key, layer
        if compatible_match is None and _layer_names_compatible(existing_key, layer_name):
            compatible_match = (existing_key, layer)
        if exact_match is None:
            exact_match = (existing_key, layer)

    if compatible_match is not None:
        return compatible_match
    return exact_match or (None, None)


def _resolve_mapping_value(mapping: dict, layer_name: str, resolved_key: str | None):
    for candidate_key in (
        resolved_key,
        _find_layer_mapping_key(mapping, layer_name),
        layer_name,
    ):
        if candidate_key is None:
            continue
        if candidate_key in mapping:
            return candidate_key, mapping[candidate_key]

    if resolved_key is not None:
        compatible_key = _find_layer_mapping_key(mapping, resolved_key)
        if compatible_key is not None:
            return compatible_key, mapping[compatible_key]

    fallback_key = next(iter(mapping), None)
    if fallback_key is None:
        return None, None

    _patch_log(f"Falling back to representative mapping entry: {layer_name} <- {fallback_key}")
    return fallback_key, mapping[fallback_key]


def _patch_vllm_layer_lookup() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import vllm.config as config_pkg
        import vllm.config.vllm as config_vllm
    except Exception:
        return

    get_layers = config_vllm.get_layers_from_vllm_config
    if getattr(get_layers, "_exaserve_pp_layer_filter_patch", False):
        return

    def get_layers_from_vllm_config(vllm_config, layer_type, layer_names=None):
        if layer_names is None:
            layer_names = list(vllm_config.compilation_config.static_forward_context.keys())

        forward_context = vllm_config.compilation_config.static_forward_context
        typed_layers = {
            layer_name: layer
            for layer_name, layer in forward_context.items()
            if isinstance(layer, layer_type)
        }
        resolved_layers = {}
        for layer_name in layer_names:
            layer = typed_layers.get(layer_name)
            if layer is None:
                for _, candidate in _iter_canonical_matches(typed_layers, layer_name):
                    layer = candidate
                    break
            if layer is None:
                layer = _find_backend_fallback_layer(typed_layers, layer_name)
                if layer is not None:
                    _patch_log(
                        f"Falling back to representative layer for backend lookup: {layer_name}"
                    )
            if layer is not None:
                resolved_layers[layer_name] = layer
        return resolved_layers

    get_layers_from_vllm_config._exaserve_pp_layer_filter_patch = True
    config_vllm.get_layers_from_vllm_config = get_layers_from_vllm_config
    if getattr(config_pkg, "get_layers_from_vllm_config", None) is get_layers:
        config_pkg.get_layers_from_vllm_config = get_layers_from_vllm_config
    _patch_log("Applied vLLM PP layer lookup patch")


def _patch_vllm_bind_kv_cache() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import vllm.v1.worker.utils as worker_utils
    except Exception:
        return

    bind_kv_cache = worker_utils.bind_kv_cache
    if getattr(bind_kv_cache, "_exaserve_pp_kv_bind_patch", False):
        return

    def bind_kv_cache(kv_caches, forward_context, runner_kv_caches, num_attn_module):
        assert len(runner_kv_caches) == 0

        index2name = worker_utils.defaultdict(list)
        for layer_name in kv_caches:
            index2name[worker_utils.extract_layer_index(layer_name, num_attn_module)].append(
                layer_name
            )

        for layer_index in sorted(index2name.keys()):
            layer_names = index2name[layer_index]
            if len(layer_names) > 1:
                if not (
                    worker_utils.current_platform.is_cuda_alike()
                    or worker_utils.current_platform.is_xpu()
                    or worker_utils.current_platform.is_cpu()
                ):
                    raise NotImplementedError
            for layer_name in layer_names:
                runner_kv_caches.append(kv_caches[layer_name])

        for layer_name, kv_cache in kv_caches.items():
            matches = list(_iter_canonical_matches(forward_context, layer_name))
            if not matches:
                fallback_key = _find_layer_mapping_key(forward_context, layer_name)
                if fallback_key is not None and fallback_key in forward_context:
                    matches = [(fallback_key, forward_context[fallback_key])]
                    _patch_log(
                        f"Using direct KV cache binding fallback: {layer_name} -> {fallback_key}"
                    )
            if not matches:
                _patch_log(f"Skipped KV cache binding for unmatched layer {layer_name}")
                continue

            for resolved_key, layer in matches:
                forward_context.setdefault(layer_name, layer)
                layer.kv_cache = [kv_cache]
                _patch_log(
                    "Bound KV cache for layer "
                    f"{layer_name} via {resolved_key} "
                    f"with shape={tuple(kv_cache.shape)}"
                )

    bind_kv_cache._exaserve_pp_kv_bind_patch = True
    worker_utils.bind_kv_cache = bind_kv_cache
    _patch_log("Applied vLLM KV cache binding patch")


def _patch_vllm_forward_context_aliases() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import vllm.forward_context as forward_context
    except Exception:
        return

    create_forward_context = forward_context.create_forward_context
    if getattr(create_forward_context, "_exaserve_layer_alias_patch", False):
        return

    def create_forward_context_with_aliases(*args, **kwargs):
        context = create_forward_context(*args, **kwargs)
        reference_names = list(context.no_compile_layers.keys())

        _ensure_layer_aliases(context.no_compile_layers, reference_names)

        if isinstance(context.attn_metadata, dict):
            _ensure_layer_aliases(context.attn_metadata, reference_names)
        elif isinstance(context.attn_metadata, list):
            for item in context.attn_metadata:
                if isinstance(item, dict):
                    _ensure_layer_aliases(item, reference_names)

        if isinstance(context.slot_mapping, dict):
            _ensure_layer_aliases(context.slot_mapping, reference_names)
        elif isinstance(context.slot_mapping, list):
            for item in context.slot_mapping:
                if isinstance(item, dict):
                    _ensure_layer_aliases(item, reference_names)

        return context

    create_forward_context_with_aliases._exaserve_layer_alias_patch = True
    forward_context.create_forward_context = create_forward_context_with_aliases
    _patch_log("Applied vLLM forward-context layer alias patch")


def _patch_vllm_attention_context() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import torch
        import vllm.attention.layer as attention_layer
    except Exception:
        return

    get_attention_context = attention_layer.get_attention_context
    unified_kv_cache_update = attention_layer.unified_kv_cache_update
    if getattr(get_attention_context, "_exaserve_layer_context_patch", False):
        return

    def resolve_metadata(attn_metadata, layer_name: str, resolved_key: str | None):
        if not isinstance(attn_metadata, dict):
            return attn_metadata
        _, metadata_value = _resolve_mapping_value(attn_metadata, layer_name, resolved_key)
        if metadata_value is not None:
            return metadata_value
        return attn_metadata

    def get_attention_context_with_aliases(layer_name: str):
        forward_context = attention_layer.get_forward_context()
        resolved_key, attn_layer, kv_cache = _resolve_layer_and_kv_cache(
            forward_context.no_compile_layers,
            layer_name,
            forward_context.virtual_engine,
        )
        if attn_layer is None:
            return get_attention_context(layer_name)

        attn_metadata = resolve_metadata(
            forward_context.attn_metadata,
            layer_name,
            resolved_key,
        )
        return attn_metadata, attn_layer, kv_cache

    def unified_kv_cache_update_with_aliases(
        key: torch.Tensor,
        value: torch.Tensor,
        layer_name: str,
    ) -> torch.Tensor:
        forward_context = attention_layer.get_forward_context()
        resolved_key, attn_layer, kv_cache = _resolve_layer_and_kv_cache(
            forward_context.no_compile_layers,
            layer_name,
            forward_context.virtual_engine,
        )
        if attn_layer is None:
            return unified_kv_cache_update(key, value, layer_name)

        slot_mapping = forward_context.slot_mapping
        if isinstance(slot_mapping, dict):
            slot_key = resolved_key or _find_layer_mapping_key(slot_mapping, layer_name)
            layer_slot_mapping = slot_mapping.get(slot_key) if slot_key is not None else None
        else:
            layer_slot_mapping = None

        if layer_slot_mapping is not None:
            if not _kv_cache_is_bound(kv_cache):
                _patch_log(
                    "Skipping KV cache update for placeholder layer "
                    f"{layer_name} (resolved={resolved_key})"
                )
            else:
                assert hasattr(attn_layer.impl, "do_kv_cache_update"), (
                    f"{attn_layer.impl.__class__.__name__} does not support kv cache update"
                )
                attn_layer.impl.do_kv_cache_update(
                    attn_layer,
                    key,
                    value,
                    kv_cache,
                    layer_slot_mapping,
                )

        if _kv_cache_is_bound(kv_cache):
            return torch.empty(0, device=kv_cache.device, dtype=kv_cache.dtype)
        return torch.empty(0, device=key.device, dtype=key.dtype)

    get_attention_context_with_aliases._exaserve_layer_context_patch = True
    attention_layer.get_attention_context = get_attention_context_with_aliases
    attention_layer.unified_kv_cache_update = unified_kv_cache_update_with_aliases
    _patch_log("Applied vLLM attention-context alias patch")


def _patch_vllm_gpu_model_runner_attn_backend() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        from collections import defaultdict
        from typing import Any, NamedTuple, cast

        import vllm.v1.worker.gpu_model_runner as gpu_model_runner
    except Exception:
        return

    runner_cls = gpu_model_runner.GPUModelRunner
    initialize_attn_backend = runner_cls.initialize_attn_backend
    if getattr(initialize_attn_backend, "_exaserve_pp_backend_lookup_patch", False):
        return

    def initialize_attn_backend(self, kv_cache_config) -> None:
        assert len(self.attn_groups) == 0, "Attention backends are already initialized"

        class AttentionGroupKey(NamedTuple):
            attn_backend: type[Any]
            kv_cache_spec: object

        def resolve_backend_layer(layer_name: str, layers: dict, layer_type):
            layer = layers.get(layer_name)
            if layer is not None:
                return layer

            resolved_key = _find_layer_mapping_key(layers, layer_name)
            if resolved_key is not None:
                layer = layers[resolved_key]
                layers[layer_name] = layer
                return layer

            layer = _find_backend_fallback_layer(layers, layer_name)
            source_key = None
            if layer is None:
                source_key, layer = _find_model_fallback_layer(self, layer_type, layer_name)

            if layer is not None:
                layers[layer_name] = layer
                _patch_log(
                    "Resolved GPUModelRunner backend layer via fallback: "
                    f"{layer_name} <- {source_key or 'representative-layer'}"
                )
            return layer

        def get_attn_backends_for_group(kv_cache_group_spec):
            layer_type = cast(type[Any], gpu_model_runner.AttentionLayerBase)
            layers = gpu_model_runner.get_layers_from_vllm_config(
                self.vllm_config, layer_type, kv_cache_group_spec.layer_names
            )
            attn_backends = {}
            attn_backend_layers = defaultdict(list)

            for layer_name in kv_cache_group_spec.layer_names:
                layer = resolve_backend_layer(layer_name, layers, layer_type)
                if layer is None:
                    raise KeyError(layer_name)

                attn_backend = layer.get_attn_backend()

                if layer_name in self.kv_sharing_fast_prefill_eligible_layers:
                    attn_backend = gpu_model_runner.create_fast_prefill_custom_backend(
                        "FastPrefill",
                        attn_backend,
                    )

                full_cls_name = attn_backend.full_cls_name()
                layer_kv_cache_spec = kv_cache_group_spec.kv_cache_spec
                if isinstance(layer_kv_cache_spec, gpu_model_runner.UniformTypeKVCacheSpecs):
                    layer_kv_cache_spec = layer_kv_cache_spec.kv_cache_specs[layer_name]
                key = (full_cls_name, layer_kv_cache_spec)
                attn_backends[key] = AttentionGroupKey(attn_backend, layer_kv_cache_spec)
                attn_backend_layers[key].append(layer_name)
            return (
                {attn_backends[k]: v for k, v in attn_backend_layers.items()},
                set(group_key.attn_backend for group_key in attn_backends.values()),
            )

        def create_attn_groups(attn_backends_map, kv_cache_group_id: int):
            attn_groups = []
            for (attn_backend, kv_cache_spec), layer_names in attn_backends_map.items():
                attn_group = gpu_model_runner.AttentionGroup(
                    attn_backend,
                    layer_names,
                    kv_cache_spec,
                    kv_cache_group_id,
                )
                attn_groups.append(attn_group)
            return attn_groups

        attention_backend_maps = []
        attention_backend_list = []
        for kv_cache_group_spec in kv_cache_config.kv_cache_groups:
            attn_backends = get_attn_backends_for_group(kv_cache_group_spec)
            attention_backend_maps.append(attn_backends[0])
            attention_backend_list.append(attn_backends[1])

        self._check_and_update_cudagraph_mode(
            attention_backend_list, kv_cache_config.kv_cache_groups
        )

        gpu_model_runner.check_attention_cp_compatibility(self.vllm_config)

        for i, attn_backend_map in enumerate(attention_backend_maps):
            self.attn_groups.append(create_attn_groups(attn_backend_map, i))

    initialize_attn_backend._exaserve_pp_backend_lookup_patch = True
    runner_cls.initialize_attn_backend = initialize_attn_backend
    _patch_log("Applied vLLM GPUModelRunner backend lookup patch")


def _patch_vllm_ray_worker_runtime_env() -> None:
    """Deliver the generated shim before each vLLM Ray actor starts.

    Mutating ``os.environ`` in EngineCore is insufficient: Ray actors are
    created by raylets and do not inherit the driver's late environment.
    ``runtime_env.env_vars`` is the supported pre-interpreter boundary, so it
    is the only point where worker ``sitecustomize`` can run before vLLM
    imports.  The allowlist deliberately excludes head-control credentials and
    arbitrary ambient ``EXASERVE_*`` values.
    """
    try:
        import vllm.v1.executor.ray_executor as ray_executor
    except Exception:
        return

    executor_cls = ray_executor.RayDistributedExecutor
    init_workers = executor_cls._init_workers_ray
    if getattr(init_workers, "_exaserve_worker_runtime_env_patch", False):
        return

    exact_keys = {
        "PYTHONPATH",
        "EXASERVE_ENGINE_RECEIPT_DIR",
        "EXASERVE_ENGINE_SHIM_PATCHES",
        "EXASERVE_ENGINE_SHIM_KIND",
        "EXASERVE_RECEIPT_MODEL_ID_ENGINE",
        "EXASERVE_RECEIPT_DEVICE_IDS_ENGINE",
        "EXASERVE_RECEIPT_REPLICA_INDEX",
        "EXASERVE_RECEIPT_REQUIREMENT_ID_ENGINE",
        "EXASERVE_RECEIPT_COMPONENT_ID_ENGINE",
        "EXASERVE_RECEIPT_RANK",
        "EXASERVE_RECEIPT_SOCKET",
        "EXASERVE_DEPLOYMENT_ID",
        "EXASERVE_GENERATION",
        "EXASERVE_VENDOR",
        "EXASERVE_PLAN_PATH",
        "EXASERVE_ALLOCATION_BINDING_PATH",
        "EXASERVE_PLAN_HASH",
        "EXASERVE_SITE_PROFILE_HASH",
        "EXASERVE_ALLOCATION_BINDING_HASH",
        "EXASERVE_COMPAT_PROFILE_ID",
        "EXASERVE_COMPAT_MANIFEST_HASH",
    }
    prefix_keys = ("EXASERVE_VERSION_", "EXASERVE_VLLM_", "EXASERVE_XPU_")

    def _init_workers_ray(self, placement_group, **ray_remote_kwargs):
        runtime_env = ray_remote_kwargs.get("runtime_env")
        if runtime_env is None:
            runtime_env = {}
        elif not isinstance(runtime_env, dict):
            raise RuntimeError("vLLM worker runtime_env must be a mapping")
        else:
            runtime_env = dict(runtime_env)
        env_vars = runtime_env.get("env_vars")
        if env_vars is None:
            env_vars = {}
        elif not isinstance(env_vars, dict):
            raise RuntimeError("vLLM worker runtime_env.env_vars must be a mapping")
        else:
            env_vars = dict(env_vars)
        for key, value in os.environ.items():
            if key in exact_keys or key.startswith(prefix_keys):
                env_vars[key] = value
        env_vars["EXASERVE_ENGINE_WORKER_KIND"] = "ray"
        env_vars["EXASERVE_COMPAT_ROLE"] = "engine_worker"
        runtime_env["env_vars"] = env_vars
        ray_remote_kwargs["runtime_env"] = runtime_env
        return init_workers(self, placement_group, **ray_remote_kwargs)

    _init_workers_ray._exaserve_worker_runtime_env_patch = True
    executor_cls._init_workers_ray = _init_workers_ray
    _patch_log("Applied vLLM Ray-worker pre-interpreter runtime_env patch")


def _patch_vllm_ray_worker_identity() -> None:
    """Expose vLLM's stable worker world rank before model-runner import.

    The Ray accelerator id is selected by the scheduler and can change after a
    worker restart.  ``rpc_rank`` is vLLM's logical topology identity.  vLLM
    may rerank workers after placement, so both construction and ``adjust_rank``
    update the process-local value consumed by the attestation watcher.
    """
    try:
        from vllm.v1.executor.ray_utils import RayWorkerWrapper
    except Exception:
        return

    original_init = RayWorkerWrapper.__init__
    if getattr(original_init, "_exaserve_worker_identity_patch", False):
        return
    original_adjust_rank = RayWorkerWrapper.adjust_rank

    def _init(self, *args, **kwargs):
        rank = kwargs.get("rpc_rank")
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
            raise RuntimeError(f"vLLM Ray worker supplied invalid rpc_rank {rank!r}")
        os.environ["EXASERVE_ENGINE_WORKER_GLOBAL_RANK"] = str(rank)
        return original_init(self, *args, **kwargs)

    def _adjust_rank(self, rank_mapping):
        result = original_adjust_rank(self, rank_mapping)
        rank = getattr(self, "rpc_rank", None)
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
            raise RuntimeError(f"vLLM Ray worker resolved invalid rpc_rank {rank!r}")
        os.environ["EXASERVE_ENGINE_WORKER_GLOBAL_RANK"] = str(rank)
        return result

    _init._exaserve_worker_identity_patch = True
    _adjust_rank._exaserve_worker_identity_patch = True
    RayWorkerWrapper.__init__ = _init
    RayWorkerWrapper.adjust_rank = _adjust_rank
    _patch_log("Applied vLLM Ray-worker logical identity adapter")


_EXASERVE_MULTIPROC_SPAWN_LOCK = threading.Lock()


def _patch_vllm_multiproc_worker_identity() -> None:
    """Pass vLLM's pinned worker rank into the spawned interpreter.

    A multiprocessing child does not have a Ray actor/resource identity. The
    executor's explicit ``rank`` argument is therefore the authoritative
    mapping to the ordered device set inherited from its EngineCore.  Keep
    vLLM's ``worker_main`` as the multiprocessing target: Python ``spawn``
    resolves targets in a fresh interpreter, so a replacement target cannot
    safely depend on a process-local captured upstream function.
    """
    try:
        import vllm.v1.executor.multiproc_executor as multiproc_executor
    except Exception:
        return

    worker_cls = multiproc_executor.WorkerProc
    make_worker_process = worker_cls.make_worker_process
    if getattr(make_worker_process, "_exaserve_worker_identity_patch", False):
        return

    def make_worker_process_with_identity(*args, **kwargs):
        rank = kwargs.get("rank", args[2] if len(args) > 2 else None)
        if isinstance(rank, bool) or not isinstance(rank, int) or rank < 0:
            raise RuntimeError(f"vLLM worker supplied invalid global rank {rank!r}")
        managed = {
            "EXASERVE_ENGINE_WORKER_KIND": "multiproc",
            "EXASERVE_ENGINE_WORKER_GLOBAL_RANK": str(rank),
        }
        # os.environ is process-global.  vLLM creates these children
        # sequentially, and the lock also prevents an incidental concurrent
        # spawner from observing another worker's logical identity.
        with _EXASERVE_MULTIPROC_SPAWN_LOCK:
            previous = {key: os.environ.get(key) for key in managed}
            os.environ.update(managed)
            try:
                return make_worker_process(*args, **kwargs)
            finally:
                for key, value in previous.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

    make_worker_process_with_identity._exaserve_worker_identity_patch = True
    worker_cls.make_worker_process = staticmethod(make_worker_process_with_identity)
    _patch_log("Applied vLLM multiprocessing worker identity adapter")


def _patch_vllm_ray_executor_channel_type() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import ray
        import vllm.v1.executor.ray_executor as ray_executor
    except Exception:
        return

    executor_cls = ray_executor.RayDistributedExecutor
    init_executor = executor_cls._init_executor
    if getattr(init_executor, "_exaserve_xpu_channel_patch", False):
        return

    def _init_executor(self) -> None:
        self.forward_dag: ray.dag.CompiledDAG | None = None

        override_channel_type = os.getenv("EXASERVE_XPU_VLLM_FORCE_RAY_CHANNEL_TYPE")
        if ray_executor.current_platform.is_tpu() or ray_executor.current_platform.is_xpu():
            if override_channel_type:
                os.environ["VLLM_USE_RAY_COMPILED_DAG_CHANNEL_TYPE"] = override_channel_type
                _patch_log(
                    "Overriding vLLM Ray channel type on accelerator platform: "
                    f"{override_channel_type}"
                )
            else:
                os.environ["VLLM_USE_RAY_COMPILED_DAG_CHANNEL_TYPE"] = "shm"

        assert self.uses_ray
        ray_executor.initialize_ray_cluster(self.parallel_config)
        placement_group = self.parallel_config.placement_group

        ray_usage = os.environ.get("RAY_USAGE_STATS_ENABLED", "0")
        if ray_usage != "1":
            os.environ["RAY_USAGE_STATS_ENABLED"] = "0"

        self._init_workers_ray(placement_group)

        self.has_connector = self.vllm_config.kv_transfer_config is not None
        self.uses_sampler = self.vllm_config.model_config.runner_type != "pooling" and (
            self.vllm_config.ec_transfer_config is None
            or not self.vllm_config.ec_transfer_config.is_ec_producer
        )
        self.scheduler_output = None

    _init_executor._exaserve_xpu_channel_patch = True
    executor_cls._init_executor = _init_executor
    _patch_log("Applied vLLM Ray executor channel override patch")


def _patch_vllm_ray_executor_uncompiled_pp() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import ray
        import vllm.v1.executor.ray_executor as ray_executor
        from vllm.v1.executor.ray_utils import FutureWrapper
    except Exception:
        return

    executor_cls = ray_executor.RayDistributedExecutor
    execute_dag = executor_cls._execute_dag
    if getattr(execute_dag, "_exaserve_uncompiled_pp_patch", False):
        return

    def _execute_dag(
        self,
        scheduler_output,
        grammar_output,
        non_block: bool = False,
    ):
        if (
            os.getenv("EXASERVE_XPU_VLLM_DISABLE_RAY_COMPILED_DAG") != "1"
            or self.parallel_config.pipeline_parallel_size <= 1
        ):
            return execute_dag(self, scheduler_output, grammar_output, non_block)

        _patch_log(
            "Using uncompiled Ray PP executor fallback "
            f"(pp={self.parallel_config.pipeline_parallel_size}, "
            f"tp={self.parallel_config.tensor_parallel_size})"
        )

        outputs = [
            (scheduler_output, grammar_output)
            for _ in range(self.parallel_config.tensor_parallel_size)
        ]
        for tp_group in self.pp_tp_workers:
            outputs = [
                worker.execute_model_ray.remote(outputs[i])  # type: ignore[attr-defined]
                for i, worker in enumerate(tp_group)
            ]

        if not self.has_connector:
            if not non_block:
                return ray.get(outputs[0])
            return FutureWrapper(outputs[0])

        assert self.kv_output_aggregator is not None
        if not non_block:
            return self.kv_output_aggregator.aggregate(ray.get(outputs))
        return FutureWrapper(outputs, self.kv_output_aggregator)

    _execute_dag._exaserve_uncompiled_pp_patch = True
    executor_cls._execute_dag = _execute_dag
    _patch_log("Applied vLLM uncompiled Ray PP fallback patch")


def _patch_ray_oneapi_selector() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import ray._private.accelerators.intel_gpu as intel_gpu
    except Exception:
        return

    manager = intel_gpu.IntelGPUAcceleratorManager
    get_visible_ids = manager.get_current_process_visible_accelerator_ids
    if getattr(get_visible_ids, "_exaserve_generic_selector_patch", False):
        return

    def get_current_process_visible_accelerator_ids():
        oneapi_visible_devices = os.environ.get(manager.get_visible_accelerator_ids_env_var(), None)
        if oneapi_visible_devices in (None, "", "NoDevFiles"):
            if os.getenv("RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR") == "1":
                local_ids = _local_accelerator_id_list(manager)
                if local_ids:
                    _patch_log(
                        "Ray left ONEAPI_DEVICE_SELECTOR unset; "
                        f"using local accelerator IDs {local_ids}"
                    )
                    return local_ids
            return get_visible_ids()

        prefix = intel_gpu.ONEAPI_DEVICE_BACKEND_TYPE + ":"
        if prefix not in oneapi_visible_devices:
            return get_visible_ids()

        visible_ids = oneapi_visible_devices.split(prefix, 1)[1].split(",")
        if all(device_id.isdigit() for device_id in visible_ids):
            return visible_ids

        # Aurora's module environment uses a generic selector like
        # "opencl:gpu;level_zero:gpu". Ray's compiled DAG path expects a numeric
        # visibility list so it can map assigned accelerator IDs back to local
        # torch device ordinals. When the selector is generic, expose the full
        # node-local GPU index list instead.
        local_ids = _local_accelerator_id_list(manager)
        if local_ids:
            _patch_log(
                f"ONEAPI_DEVICE_SELECTOR is generic; using local accelerator IDs {local_ids}"
            )
            return local_ids

        return get_visible_ids()

    get_current_process_visible_accelerator_ids._exaserve_generic_selector_patch = True
    manager.get_current_process_visible_accelerator_ids = staticmethod(
        get_current_process_visible_accelerator_ids
    )
    _patch_log("Applied Ray ONEAPI selector patch")


def _patch_ray_accelerator_context() -> None:
    if os.getenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import ray
        import torch
        from ray._private.accelerators import get_accelerator_manager_for_resource
        from ray.experimental.channel.accelerator_context import AcceleratorContext
    except Exception:
        return

    get_accelerator_devices = AcceleratorContext.get_accelerator_devices
    if getattr(get_accelerator_devices, "_exaserve_generic_selector_patch", False):
        return

    def get_accelerator_devices(self):
        if self._torch_module_name == "cpu":
            return [torch.device("cpu")]

        if self._torch_module_name == "cuda":
            accelerator_ids = [str(device_id) for device_id in ray.get_gpu_ids()]
            accelerator_manager = get_accelerator_manager_for_resource("GPU")
        else:
            accelerator_ids = [
                str(device_id)
                for device_id in ray.get_runtime_context().get_accelerator_ids()[
                    self._torch_module_name.upper()
                ]
            ]
            accelerator_manager = get_accelerator_manager_for_resource(
                self._torch_module_name.upper()
            )

        device_ids = []
        if accelerator_ids:
            accelerator_visible_list = (
                accelerator_manager.get_current_process_visible_accelerator_ids() or []
            )
            visible_numeric_ids = _parse_numeric_ids(accelerator_visible_list)
            local_numeric_ids = _parse_numeric_ids(_local_accelerator_id_list(accelerator_manager))

            for accelerator_id in accelerator_ids:
                resolved_device_id = None
                try:
                    resolved_device_id = accelerator_visible_list.index(accelerator_id)
                except ValueError:
                    if accelerator_id.isdigit():
                        accelerator_id_int = int(accelerator_id)

                        if not accelerator_visible_list and local_numeric_ids:
                            if accelerator_id_int in local_numeric_ids:
                                resolved_device_id = accelerator_id_int
                                _patch_log(
                                    "Resolved accelerator ID via local ordinal fallback: "
                                    f"{accelerator_id} -> {resolved_device_id}"
                                )

                        if (
                            resolved_device_id is None
                            and visible_numeric_ids
                            and visible_numeric_ids == list(range(len(visible_numeric_ids)))
                            and accelerator_id_int < len(visible_numeric_ids)
                        ):
                            resolved_device_id = accelerator_id_int
                            _patch_log(
                                "Resolved accelerator ID via numeric visible-list fallback: "
                                f"{accelerator_id} -> {resolved_device_id}"
                            )

                        if (
                            resolved_device_id is None
                            and visible_numeric_ids
                            and len(visible_numeric_ids) == 1
                        ):
                            resolved_device_id = 0
                            _patch_log(
                                "Resolved accelerator ID via single-device fallback: "
                                f"{accelerator_id} -> {resolved_device_id}"
                            )

                if resolved_device_id is not None:
                    device_ids.append(resolved_device_id)
                    continue

                if (
                    accelerator_visible_list
                    or accelerator_manager.get_visible_accelerator_ids_env_var()
                    == "ONEAPI_DEVICE_SELECTOR"
                ):
                    _patch_log(
                        "Failed to resolve accelerator device mapping: "
                        f"accelerator_ids={accelerator_ids}, "
                        f"visible_list={accelerator_visible_list}, "
                        f"torch_module={self._torch_module_name}"
                    )
                    raise RuntimeError(
                        f"{accelerator_manager.get_visible_accelerator_ids_env_var()} set incorrectly. "
                        f"expected to include {accelerator_id}. "
                        "Did you override this environment"
                        " variable? If not, please help file an issue on Github."
                    )
                raise RuntimeError(
                    "Unable to resolve accelerator device mapping without a "
                    f"{accelerator_manager.get_visible_accelerator_ids_env_var()} "
                    f"assignment for accelerator ID {accelerator_id}."
                )
        else:
            device_ids.append(0)

        return [torch.device(f"{self._torch_module_name}:{device_id}") for device_id in device_ids]

    get_accelerator_devices._exaserve_generic_selector_patch = True
    AcceleratorContext.get_accelerator_devices = get_accelerator_devices
    _patch_log("Applied Ray accelerator context patch")


def _patch_ray_serve_start_timeout(module=None) -> None:
    """Apply the one Ray Serve constant with no supported configuration API.

    Proxy health/ready checks use Ray's environment controls, and replica
    checks use public deployment options.  Only ``HTTP_PROXY_TIMEOUT`` still
    needs a pinned compatibility hook on Ray 2.53.
    """
    raw = os.getenv("EXASERVE_RAY_SERVE_START_PROXY_TIMEOUT_S")
    if raw is None:
        return
    try:
        value = float(raw)
    except ValueError as exc:
        raise RuntimeError("EXASERVE_RAY_SERVE_START_PROXY_TIMEOUT_S must be numeric") from exc
    if value <= 0:
        raise RuntimeError("EXASERVE_RAY_SERVE_START_PROXY_TIMEOUT_S must be positive")
    if module is None:
        module = sys.modules.get("ray.serve._private.constants")
    if module is None:
        return
    module.HTTP_PROXY_TIMEOUT = value
    module._exaserve_serve_start_timeout_patch = True
    _patch_log(f"Applied Ray Serve startup timeout: {value}s")


def _install_ray_serve_timeout_import_hook() -> None:
    module = sys.modules.get("ray.serve._private.constants")
    if module is not None:
        _patch_ray_serve_start_timeout(module)
        return

    import importlib.abc
    import importlib.machinery

    class _RayServeTimeoutLoader(importlib.abc.Loader):
        def __init__(self, wrapped):
            self.wrapped = wrapped

        def create_module(self, spec):
            creator = getattr(self.wrapped, "create_module", None)
            return creator(spec) if creator is not None else None

        def exec_module(self, module):
            self.wrapped.exec_module(module)
            _patch_ray_serve_start_timeout(module)

    class _RayServeTimeoutFinder(importlib.abc.MetaPathFinder):
        _exaserve_ray_serve_timeout_finder = True

        def find_spec(self, fullname, path=None, target=None):
            if fullname != "ray.serve._private.constants":
                return None
            spec = importlib.machinery.PathFinder.find_spec(fullname, path, target)
            if spec is None or spec.loader is None:
                return spec
            spec.loader = _RayServeTimeoutLoader(spec.loader)
            return spec

    if not any(
        getattr(finder, "_exaserve_ray_serve_timeout_finder", False) for finder in sys.meta_path
    ):
        sys.meta_path.insert(0, _RayServeTimeoutFinder())


# WP3 delete-first (ADR-003 decision 1, 2026-08-05): three dead patches
# removed — SC-D1 wrap_as_future timeout, SC-D2 stale Serve constants
# (values disagreed with the live RS-01 profile), and SC-D3 ProxyActor
# profiling. The file-replacement overlay and its instrumentation were also
# retired; current diagnostics use ExaServe-owned collectors and traces.
