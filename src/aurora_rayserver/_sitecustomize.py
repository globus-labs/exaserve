import asyncio
import os
import re
import sys


def _patch_log(message: str) -> None:
    if os.getenv("AURORA_VLLM_PATCH_VERBOSE") == "1":
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
        _patch_log(
            "Using stage-relative layer alias: "
            f"{layer_name} -> {relative_key}"
        )
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

    _patch_log(
        "Falling back to representative mapping entry: "
        f"{layer_name} <- {fallback_key}"
    )
    return fallback_key, mapping[fallback_key]


def _patch_vllm_layer_lookup() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import vllm.config as config_pkg
        import vllm.config.vllm as config_vllm
    except Exception:
        return

    get_layers = config_vllm.get_layers_from_vllm_config
    if getattr(get_layers, "_aurora_pp_layer_filter_patch", False):
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
                        "Falling back to representative layer for backend lookup: "
                        f"{layer_name}"
                    )
            if layer is not None:
                resolved_layers[layer_name] = layer
        return resolved_layers

    get_layers_from_vllm_config._aurora_pp_layer_filter_patch = True
    config_vllm.get_layers_from_vllm_config = get_layers_from_vllm_config
    if getattr(config_pkg, "get_layers_from_vllm_config", None) is get_layers:
        config_pkg.get_layers_from_vllm_config = get_layers_from_vllm_config
    _patch_log("Applied vLLM PP layer lookup patch")


_patch_vllm_layer_lookup()


def _patch_vllm_bind_kv_cache() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import vllm.v1.worker.utils as worker_utils
    except Exception:
        return

    bind_kv_cache = worker_utils.bind_kv_cache
    if getattr(bind_kv_cache, "_aurora_pp_kv_bind_patch", False):
        return

    def bind_kv_cache(kv_caches, forward_context, runner_kv_caches, num_attn_module):
        assert len(runner_kv_caches) == 0

        index2name = worker_utils.defaultdict(list)
        for layer_name in kv_caches:
            index2name[
                worker_utils.extract_layer_index(layer_name, num_attn_module)
            ].append(layer_name)

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
                        "Using direct KV cache binding fallback: "
                        f"{layer_name} -> {fallback_key}"
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
    bind_kv_cache._aurora_pp_kv_bind_patch = True
    worker_utils.bind_kv_cache = bind_kv_cache
    _patch_log("Applied vLLM KV cache binding patch")


_patch_vllm_bind_kv_cache()


def _patch_vllm_forward_context_aliases() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import vllm.forward_context as forward_context
    except Exception:
        return

    create_forward_context = forward_context.create_forward_context
    if getattr(create_forward_context, "_aurora_layer_alias_patch", False):
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

    create_forward_context_with_aliases._aurora_layer_alias_patch = True
    forward_context.create_forward_context = create_forward_context_with_aliases
    _patch_log("Applied vLLM forward-context layer alias patch")


_patch_vllm_forward_context_aliases()


def _patch_vllm_attention_context() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import torch
        import vllm.attention.layer as attention_layer
    except Exception:
        return

    get_attention_context = attention_layer.get_attention_context
    unified_kv_cache_update = attention_layer.unified_kv_cache_update
    if getattr(get_attention_context, "_aurora_layer_context_patch", False):
        return

    def resolve_metadata(attn_metadata, layer_name: str, resolved_key: str | None):
        if not isinstance(attn_metadata, dict):
            return attn_metadata
        _, metadata_value = _resolve_mapping_value(
            attn_metadata, layer_name, resolved_key
        )
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

    get_attention_context_with_aliases._aurora_layer_context_patch = True
    attention_layer.get_attention_context = get_attention_context_with_aliases
    attention_layer.unified_kv_cache_update = unified_kv_cache_update_with_aliases
    _patch_log("Applied vLLM attention-context alias patch")


_patch_vllm_attention_context()


def _patch_vllm_gpu_model_runner_attn_backend() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        from collections import defaultdict
        from typing import Any, NamedTuple, cast

        import vllm.v1.worker.gpu_model_runner as gpu_model_runner
    except Exception:
        return

    runner_cls = gpu_model_runner.GPUModelRunner
    initialize_attn_backend = runner_cls.initialize_attn_backend
    if getattr(initialize_attn_backend, "_aurora_pp_backend_lookup_patch", False):
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
                if isinstance(
                    layer_kv_cache_spec, gpu_model_runner.UniformTypeKVCacheSpecs
                ):
                    layer_kv_cache_spec = layer_kv_cache_spec.kv_cache_specs[layer_name]
                key = (full_cls_name, layer_kv_cache_spec)
                attn_backends[key] = AttentionGroupKey(
                    attn_backend, layer_kv_cache_spec
                )
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

    initialize_attn_backend._aurora_pp_backend_lookup_patch = True
    runner_cls.initialize_attn_backend = initialize_attn_backend
    _patch_log("Applied vLLM GPUModelRunner backend lookup patch")


_patch_vllm_gpu_model_runner_attn_backend()


def _install_vllm_gpu_model_runner_import_hook() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import builtins
    except Exception:
        return

    original_import = builtins.__import__
    if getattr(original_import, "_aurora_gpu_model_runner_import_hook", False):
        return

    hook_state = {"active": False}

    def aurora_import(name, globals=None, locals=None, fromlist=(), level=0):
        module = original_import(name, globals, locals, fromlist, level)
        if hook_state["active"]:
            return module

        if not name.startswith("vllm"):
            return module

        try:
            hook_state["active"] = True
            if "vllm.v1.worker.gpu_model_runner" in sys.modules:
                _patch_vllm_gpu_model_runner_attn_backend()
        finally:
            hook_state["active"] = False
        return module

    aurora_import._aurora_gpu_model_runner_import_hook = True
    builtins.__import__ = aurora_import
    _patch_log("Installed vLLM GPUModelRunner import hook")


_install_vllm_gpu_model_runner_import_hook()


def _patch_vllm_ray_multigpu_bundles() -> None:
    # STATUS: written to let multi-GPU per-stage bundles pass vLLM 0.15's
    # one-GPU-per-bundle check, but server.py now emits per-GPU bundles
    # (commit 17b88be) which pass the upstream check unpatched — so this is
    # likely redundant. It only loads in the EngineCore at all since the
    # PYTHONPATH sitecustomize shim (commit 4d9d846). Kept because the
    # verified 405B PP run had it loaded; removal needs a PP re-test.
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        from collections import defaultdict

        import ray
        import vllm.v1.executor.ray_executor as ray_executor
        import vllm.v1.executor.ray_utils as ray_utils
    except Exception:
        return

    initialize_ray_cluster = ray_utils.initialize_ray_cluster
    if getattr(initialize_ray_cluster, "_aurora_multigpu_bundle_patch", False):
        return

    def _format_device_count(count: float) -> str:
        if float(count).is_integer():
            return str(int(count))
        return f"{count:g}"

    def _verify_bundles(
        placement_group,
        parallel_config,
        device_str: str,
    ) -> None:
        assert ray.is_initialized(), (
            "Ray is not initialized although distributed-executor-backend is ray."
        )
        pg_data = ray_utils.placement_group_table(placement_group)
        bundle_to_node_ids = pg_data["bundles_to_node_id"]
        bundles = pg_data["bundles"]
        node_id_to_bundle = defaultdict(list)

        for bundle_idx, node_id in bundle_to_node_ids.items():
            node_id_to_bundle[node_id].append(bundles[bundle_idx])
        driver_node_id = ray.get_runtime_context().get_node_id()

        if driver_node_id not in node_id_to_bundle:
            raise RuntimeError(
                f"driver node id {driver_node_id} is not included in a placement "
                f"group {placement_group.id}. Node id -> bundles "
                f"{node_id_to_bundle}. "
                "You don't have enough GPUs available in a current node. Check "
                "`ray status` and `ray list nodes` to see if you have available "
                "GPUs in a node `{driver_node_id}` before starting an vLLM engine."
            )

        for node_id, node_bundles in node_id_to_bundle.items():
            reserved_devices = sum(
                float(bundle.get(device_str, 0) or 0) for bundle in node_bundles
            )
            if reserved_devices + 1e-9 < parallel_config.tensor_parallel_size:
                ray_utils.logger.warning(
                    "tensor_parallel_size=%d "
                    "is bigger than a reserved number of %ss (%s "
                    "%ss) in a node %s. Tensor parallel workers can be "
                    "spread out to 2+ nodes which can degrade the performance "
                    "unless you have fast interconnect across nodes, like "
                    "Infiniband. To resolve this issue, make sure you have more "
                    "than %d GPUs available at each node.",
                    parallel_config.tensor_parallel_size,
                    device_str,
                    _format_device_count(reserved_devices),
                    device_str,
                    node_id,
                    parallel_config.tensor_parallel_size,
                )

    def initialize_ray_cluster(parallel_config, ray_address=None):
        ray_utils.assert_ray_available()
        from vllm.platforms import current_platform

        if current_platform.is_cuda() and parallel_config.world_size > 1:
            from vllm.utils.torch_utils import cuda_device_count_stateless

            available_gpus = cuda_device_count_stateless()
            if parallel_config.world_size > available_gpus:
                ray_utils.logger.warning(
                    "Tensor parallel size (%d) exceeds available GPUs (%d). "
                    "This may result in Ray placement group allocation failures. "
                    "Consider reducing tensor_parallel_size to %d or less, "
                    "or ensure your Ray cluster has %d GPUs available.",
                    parallel_config.world_size,
                    available_gpus,
                    available_gpus,
                    parallel_config.world_size,
                )

        if ray.is_initialized():
            ray_utils.logger.info(
                "Ray is already initialized. Skipping Ray initialization."
            )
        elif current_platform.is_rocm() or current_platform.is_xpu():
            try:
                ray.init("auto")
            except ConnectionError:
                ray_utils.logger.warning(
                    "No existing RAY instance detected. "
                    "A new instance will be launched with current node resources."
                )
                ray.init(
                    address=ray_address,
                    num_gpus=parallel_config.world_size,
                    runtime_env=parallel_config.ray_runtime_env,
                )
        else:
            ray.init(
                address=ray_address,
                runtime_env=parallel_config.ray_runtime_env,
            )

        device_str = current_platform.ray_device_key
        if not device_str:
            raise ValueError(
                f"current platform {current_platform.device_name} does not support ray."
            )

        if parallel_config.placement_group:
            current_placement_group = parallel_config.placement_group
        else:
            current_placement_group = ray.util.get_current_placement_group()

        if current_placement_group:
            ray_utils.logger.info("Using the existing placement group")

            total_devices = sum(
                float(bundle.get(device_str, 0) or 0)
                for bundle in current_placement_group.bundle_specs
            )
            if parallel_config.world_size > total_devices + 1e-9:
                raise ValueError(
                    f"The number of required {device_str}s exceeds the total "
                    f"number of available {device_str}s in the placement group. "
                    f"Required number of devices: {parallel_config.world_size}. "
                    f"Total number of devices: {_format_device_count(total_devices)}."
                )
        else:
            ray_utils.logger.info(
                "No current placement group found. Creating a new placement group."
            )
            num_devices_in_cluster = ray.cluster_resources().get(device_str, 0)
            if parallel_config.world_size > num_devices_in_cluster:
                ray_utils.logger.warning(
                    "The number of required %ss exceeds the total "
                    "number of available %ss in the placement group.",
                    device_str,
                    device_str,
                )
            placement_group_specs = [
                {device_str: 1.0} for _ in range(parallel_config.world_size)
            ]

            current_ip = ray_utils.get_ip()
            current_node_id = ray.get_runtime_context().get_node_id()
            current_node_resource = ray_utils.available_resources_per_node()[
                current_node_id
            ]
            if current_node_resource.get(device_str, 0) < 1:
                raise ValueError(
                    f"Current node has no {device_str} available. "
                    f"current_node_resource={current_node_resource}. "
                    f"vLLM engine cannot start without "
                    f"{device_str}. Make sure you have at least 1 {device_str} "
                    f"available in a node current_node_id={current_node_id} "
                    f"current_ip={current_ip}."
                )
            placement_group_specs[0][f"node:{current_ip}"] = 0.001

            current_placement_group = ray.util.placement_group(
                placement_group_specs,
                strategy="PACK",
            )
            ray_utils._wait_until_pg_ready(current_placement_group)

        assert current_placement_group is not None
        _verify_bundles(current_placement_group, parallel_config, device_str)
        parallel_config.placement_group = current_placement_group

    _verify_bundles._aurora_multigpu_bundle_patch = True
    initialize_ray_cluster._aurora_multigpu_bundle_patch = True
    ray_utils._verify_bundles = _verify_bundles
    ray_utils.initialize_ray_cluster = initialize_ray_cluster
    ray_executor.initialize_ray_cluster = initialize_ray_cluster
    _patch_log("Applied vLLM Ray multi-GPU placement-group patch")


_patch_vllm_ray_multigpu_bundles()


def _patch_vllm_ray_executor_bundle_indices() -> None:
    # STATUS: written to allow duplicate VLLM_RAY_BUNDLE_INDICES (multiple
    # workers sharing a multi-GPU stage bundle). server.py no longer sets
    # that env var and emits per-GPU bundles (commit 17b88be), so the
    # default no-env path here matches upstream behavior — likely redundant.
    # Same caveat as _patch_vllm_ray_multigpu_bundles: loaded during the
    # verified 405B PP run via the 4d9d846 shim; removal needs a PP re-test.
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        from collections import defaultdict

        import ray
        import vllm.v1.executor.ray_executor as ray_executor
    except Exception:
        return

    executor_cls = ray_executor.RayDistributedExecutor
    init_workers = executor_cls._init_workers_ray
    if getattr(init_workers, "_aurora_bundle_index_patch", False):
        return

    def _init_workers_ray(self, placement_group, **ray_remote_kwargs):
        num_gpus = ray_executor.envs.VLLM_RAY_PER_WORKER_GPUS

        self.driver_dummy_worker = None
        self.workers = []
        self.pp_tp_workers = []

        if self.parallel_config.ray_workers_use_nsight:
            ray_remote_kwargs = self._configure_ray_workers_use_nsight(
                ray_remote_kwargs
            )

        preserve_bundle_order = False
        if ray_executor.envs.VLLM_RAY_BUNDLE_INDICES:
            bundle_indices = list(
                map(int, ray_executor.envs.VLLM_RAY_BUNDLE_INDICES.split(","))
            )
            assert len(bundle_indices) == self.parallel_config.world_size, (
                "VLLM_RAY_BUNDLE_INDICES must have the same size"
                f" as the world size, but got bundle_indices={bundle_indices} "
                "and "
                f"world_size={self.parallel_config.world_size}"
            )
            preserve_bundle_order = True
        else:
            bundle_indices = []
            for bundle_id, bundle in enumerate(placement_group.bundle_specs):
                bundle_devices = float(
                    bundle.get(ray_executor.current_platform.ray_device_key, 0) or 0
                )
                if bundle_devices <= 0:
                    continue
                workers_in_bundle = max(
                    1,
                    int(round(bundle_devices / float(num_gpus))),
                )
                bundle_indices.extend([bundle_id] * workers_in_bundle)
            bundle_indices = bundle_indices[: self.parallel_config.world_size]

        worker_metadata = []
        driver_ip = (
            os.environ.get("VLLM_HOST_IP")
            or os.environ.get("MASTER_ADDR")
            or ray_executor.get_ip()
        )
        for rank, bundle_id in enumerate(bundle_indices):
            scheduling_strategy = ray_executor.PlacementGroupSchedulingStrategy(
                placement_group=placement_group,
                placement_group_capture_child_tasks=True,
                placement_group_bundle_index=bundle_id,
            )

            if ray_executor.current_platform.ray_device_key == "GPU":
                worker = ray.remote(
                    num_cpus=0,
                    num_gpus=num_gpus,
                    scheduling_strategy=scheduling_strategy,
                    **ray_remote_kwargs,
                )(ray_executor.RayWorkerWrapper).remote(rpc_rank=rank)
            else:
                worker = ray.remote(
                    num_cpus=0,
                    num_gpus=0,
                    resources={
                        ray_executor.current_platform.ray_device_key: num_gpus
                    },
                    scheduling_strategy=scheduling_strategy,
                    **ray_remote_kwargs,
                )(ray_executor.RayWorkerWrapper).remote(rpc_rank=rank)

            worker_metadata.append(
                ray_executor.RayWorkerMetaData(worker=worker, created_rank=rank)
            )

        worker_ips = ray.get(
            [
                each.worker.get_node_ip.remote()  # type: ignore[attr-defined]
                for each in worker_metadata
            ]
        )

        for each, ip in zip(worker_metadata, worker_ips):
            each.ip = ip

        ray_executor.logger.debug("workers: %s", worker_metadata)
        ray_executor.logger.debug("driver_dummy_worker: %s", self.driver_dummy_worker)

        if preserve_bundle_order:
            sorted_worker_metadata = worker_metadata
        else:
            ip_counts = {}
            for ip in worker_ips:
                ip_counts[ip] = ip_counts.get(ip, 0) + 1

            def sort_by_driver_then_worker_ip(item):
                ip = item.ip
                return 0 if ip == driver_ip else 1, ip_counts[ip], ip

            sorted_worker_metadata = sorted(
                worker_metadata,
                key=sort_by_driver_then_worker_ip,
            )

        for i, item in enumerate(sorted_worker_metadata):
            item.adjusted_rank = i
        self.workers = [item.worker for item in sorted_worker_metadata]
        rerank_mapping = {
            item.created_rank: item.adjusted_rank
            for item in sorted_worker_metadata
        }
        self.collective_rpc("adjust_rank", args=(rerank_mapping,))

        worker_node_and_gpu_ids = []
        for worker in [self.driver_dummy_worker] + self.workers:
            if worker is None:
                continue
            worker_node_and_gpu_ids.append(
                ray.get(worker.get_node_and_gpu_ids.remote())
            )  # type: ignore[attr-defined]

        node_workers = defaultdict(list)
        node_gpus = defaultdict(list)

        for i, (node_id, gpu_ids) in enumerate(worker_node_and_gpu_ids):
            node_workers[node_id].append(i)
            gpu_ids = [int(x) for x in gpu_ids]
            node_gpus[node_id].extend(gpu_ids)
        for node_id, gpu_ids in node_gpus.items():
            node_gpus[node_id] = sorted(gpu_ids)

        single_node_worker_topology = len(node_workers) == 1
        if single_node_worker_topology:
            driver_ip = "127.0.0.1"

        all_ips = set(worker_ips)
        if not single_node_worker_topology and (
            not preserve_bundle_order or driver_ip in all_ips
        ):
            all_ips.add(driver_ip)
        n_ips = len(all_ips)
        n_nodes = len(node_workers)

        if (
            preserve_bundle_order
            and not single_node_worker_topology
            and driver_ip not in set(worker_ips)
        ):
            _patch_log(
                "Driver IP is not among worker IPs for explicit bundle order; "
                f"using driver_ip={driver_ip}, worker_ips={worker_ips}"
            )
        elif not single_node_worker_topology and n_nodes != n_ips:
            raise RuntimeError(
                f"Every node should have a unique IP address. Got {n_nodes}"
                f" nodes with node ids {list(node_workers.keys())} and "
                f"{n_ips} unique IP addresses {all_ips}. Please check your"
                " network configuration. If you set `VLLM_HOST_IP`"
                " environment variable, make sure it is unique for"
                " each node."
            )

        all_args_to_update_environment_variables = [
            {
                ray_executor.current_platform.device_control_env_var: ",".join(
                    map(str, node_gpus[node_id])
                ),
            }
            for (node_id, _) in worker_node_and_gpu_ids
        ]

        env_vars_to_copy = ray_executor.get_env_vars_to_copy(
            exclude_vars=self.WORKER_SPECIFIC_ENV_VARS,
            additional_vars=set(
                ray_executor.current_platform.additional_env_vars
            ).union(self.ADDITIONAL_ENV_VARS),
            destination="workers",
        )

        for args in all_args_to_update_environment_variables:
            for name in env_vars_to_copy:
                if name in os.environ:
                    args[name] = os.environ[name]

        self._env_vars_for_all_workers = all_args_to_update_environment_variables

        self.collective_rpc(
            "update_environment_variables",
            args=(self._get_env_vars_to_be_updated(),),
        )

        distributed_init_method = ray_executor.get_distributed_init_method(
            driver_ip,
            ray_executor.get_open_port(),
        )

        all_kwargs = []
        for rank, (node_id, _) in enumerate(worker_node_and_gpu_ids):
            local_rank = node_workers[node_id].index(rank)
            kwargs = dict(
                vllm_config=self.vllm_config,
                local_rank=local_rank,
                rank=rank,
                distributed_init_method=distributed_init_method,
                is_driver_worker=(not self.parallel_config)
                or (rank % self.parallel_config.tensor_parallel_size == 0),
            )
            all_kwargs.append(kwargs)
        self.collective_rpc("init_worker", args=(all_kwargs,))

        self.collective_rpc("init_device")
        self.collective_rpc("load_model")

        for pp_rank in range(self.parallel_config.pipeline_parallel_size):
            self.pp_tp_workers.append([])
            for tp_rank in range(self.parallel_config.tensor_parallel_size):
                rank = (
                    pp_rank * self.parallel_config.tensor_parallel_size
                ) + tp_rank
                assert len(self.pp_tp_workers[pp_rank]) == tp_rank
                assert pp_rank < len(self.pp_tp_workers)
                self.pp_tp_workers[pp_rank].append(self.workers[rank])

    _init_workers_ray._aurora_bundle_index_patch = True
    executor_cls._init_workers_ray = _init_workers_ray
    _patch_log("Applied vLLM Ray bundle index ordering patch")


_patch_vllm_ray_executor_bundle_indices()


def _patch_vllm_ray_executor_channel_type() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import ray
        import vllm.v1.executor.ray_executor as ray_executor
    except Exception:
        return

    executor_cls = ray_executor.RayDistributedExecutor
    init_executor = executor_cls._init_executor
    if getattr(init_executor, "_aurora_xpu_channel_patch", False):
        return

    def _init_executor(self) -> None:
        self.forward_dag: ray.dag.CompiledDAG | None = None

        override_channel_type = os.getenv("AURORA_VLLM_FORCE_RAY_CHANNEL_TYPE")
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
        self.uses_sampler = (
            self.vllm_config.model_config.runner_type != "pooling"
            and (
                self.vllm_config.ec_transfer_config is None
                or not self.vllm_config.ec_transfer_config.is_ec_producer
            )
        )
        self.scheduler_output = None

    _init_executor._aurora_xpu_channel_patch = True
    executor_cls._init_executor = _init_executor
    _patch_log("Applied vLLM Ray executor channel override patch")


_patch_vllm_ray_executor_channel_type()


def _patch_vllm_ray_executor_uncompiled_pp() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import ray
        import vllm.v1.executor.ray_executor as ray_executor
        from vllm.v1.executor.ray_utils import FutureWrapper
    except Exception:
        return

    executor_cls = ray_executor.RayDistributedExecutor
    execute_dag = executor_cls._execute_dag
    if getattr(execute_dag, "_aurora_uncompiled_pp_patch", False):
        return

    def _execute_dag(
        self,
        scheduler_output,
        grammar_output,
        non_block: bool = False,
    ):
        if (
            os.getenv("AURORA_VLLM_DISABLE_RAY_COMPILED_DAG") != "1"
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

    _execute_dag._aurora_uncompiled_pp_patch = True
    executor_cls._execute_dag = _execute_dag
    _patch_log("Applied vLLM uncompiled Ray PP fallback patch")


_patch_vllm_ray_executor_uncompiled_pp()


def _patch_ray_oneapi_selector() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import ray._private.accelerators.intel_gpu as intel_gpu
    except Exception:
        return

    manager = intel_gpu.IntelGPUAcceleratorManager
    get_visible_ids = manager.get_current_process_visible_accelerator_ids
    if getattr(get_visible_ids, "_aurora_generic_selector_patch", False):
        return

    def get_current_process_visible_accelerator_ids():
        oneapi_visible_devices = os.environ.get(
            manager.get_visible_accelerator_ids_env_var(), None
        )
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
                "ONEAPI_DEVICE_SELECTOR is generic; "
                f"using local accelerator IDs {local_ids}"
            )
            return local_ids

        return get_visible_ids()

    get_current_process_visible_accelerator_ids._aurora_generic_selector_patch = True
    manager.get_current_process_visible_accelerator_ids = staticmethod(
        get_current_process_visible_accelerator_ids
    )
    _patch_log("Applied Ray ONEAPI selector patch")


_patch_ray_oneapi_selector()


def _patch_ray_accelerator_context() -> None:
    if os.getenv("AURORA_VLLM_PATCH_PP_LAYER_FILTER") != "1":
        return

    try:
        import ray
        import torch
        from ray._private.accelerators import get_accelerator_manager_for_resource
        from ray.experimental.channel.accelerator_context import AcceleratorContext
    except Exception:
        return

    get_accelerator_devices = AcceleratorContext.get_accelerator_devices
    if getattr(get_accelerator_devices, "_aurora_generic_selector_patch", False):
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
            local_numeric_ids = _parse_numeric_ids(
                _local_accelerator_id_list(accelerator_manager)
            )

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

        return [
            torch.device(f"{self._torch_module_name}:{device_id}")
            for device_id in device_ids
        ]

    get_accelerator_devices._aurora_generic_selector_patch = True
    AcceleratorContext.get_accelerator_devices = get_accelerator_devices
    _patch_log("Applied Ray accelerator context patch")


_patch_ray_accelerator_context()


def _patch_ray_serve_proxy_future_timeout() -> None:
    try:
        import ray.serve._private.proxy_state as proxy_state
    except Exception:
        return

    wrap_as_future = getattr(proxy_state, "wrap_as_future", None)
    if wrap_as_future is None:
        return
    if getattr(wrap_as_future, "_aurora_proxy_timeout_patch", False):
        return

    def _set_future_from_source(result_fut, source_fut) -> None:
        if result_fut.done():
            return
        if source_fut.cancelled():
            result_fut.cancel()
            return
        exc = source_fut.exception()
        if exc is not None:
            result_fut.set_exception(exc)
            return
        result_fut.set_result(source_fut.result())

    def _set_timeout_if_pending(result_fut, timeout_s: float) -> None:
        if result_fut.done():
            return
        result_fut.set_exception(
            TimeoutError(f"Future cancelled after timeout {timeout_s}s")
        )

    def wrap_as_future_safe(ref, timeout_s=None):
        loop = asyncio.get_running_loop()
        source_fut = asyncio.wrap_future(ref.future())

        if timeout_s is None:
            return source_fut

        assert timeout_s >= 0, "Timeout value should be non-negative"
        result_fut = loop.create_future()
        source_fut.add_done_callback(
            lambda completed: _set_future_from_source(result_fut, completed)
        )
        timeout_handler = loop.call_later(
            max(timeout_s, 0),
            _set_timeout_if_pending,
            result_fut,
            timeout_s,
        )
        result_fut.add_done_callback(lambda _: timeout_handler.cancel())
        return result_fut

    wrap_as_future_safe._aurora_proxy_timeout_patch = True
    proxy_state.wrap_as_future = wrap_as_future_safe
    _patch_log("Applied Ray Serve proxy timeout future patch")


# perf-inst-dev: Ray Serve patches disabled — constants.py in the overlay
# (~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/ray)
# applies timeouts directly and proxy.py in the overlay carries the
# ProxyActor instrumentation. Restore from main-repo commit 2f32633 if needed.
# _patch_ray_serve_proxy_future_timeout()


def _patch_ray_serve_proxy_startup_timeout() -> None:
    """Increase HTTP_PROXY_TIMEOUT from 60s to 600s for large-scale deployments.

    At 128 nodes Ray Serve needs to start ~128 EveryNode proxy actors and
    ~1536 vLLM replicas.  The default 60 s timeout is too short — the
    ServeController kills proxy actors that haven't become healthy yet,
    causing a cascade of ActorDiedError.
    """
    try:
        import ray.serve._private.constants as constants
    except Exception:
        return
    old = getattr(constants, "HTTP_PROXY_TIMEOUT", None)
    if old is None:
        return
    new_timeout = int(os.environ.get("RAY_SERVE_HTTP_PROXY_TIMEOUT", "600"))
    constants.HTTP_PROXY_TIMEOUT = new_timeout
    constants.PROXY_HEALTH_CHECK_TIMEOUT_S = 60.0
    constants.PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD = 10
    # Also patch modules that imported the constant by name
    import sys as _sys
    for _mod_name in list(_sys.modules):
        if "ray.serve" in _mod_name:
            _mod = _sys.modules[_mod_name]
            if hasattr(_mod, "HTTP_PROXY_TIMEOUT"):
                _mod.HTTP_PROXY_TIMEOUT = new_timeout
    _patch_log(
        f"Patched HTTP_PROXY_TIMEOUT={new_timeout}s, "
        f"HEALTH_CHECK_TIMEOUT=60s, UNHEALTHY_THRESHOLD=10"
    )


# perf-inst-dev: disabled — see note above. Overlay constants.py carries these values.
# _patch_ray_serve_proxy_startup_timeout()


def _install_proxy_actor_profiling_hook() -> None:
    """Wrap ProxyActor.__init__ and ready() to collect per-process lifecycle timestamps.

    Each proxy writes a JSON file to /tmp/aurora_proxy_profile/ with:
      - process_python_start_s: wall time when the proxy module was first loaded
      - init_start_s / init_end_s: wall time around __init__
      - ready_start_s / ready_end_s: wall time around ready()
      - hostname, pid, node_id
    Enabled by AURORA_PROXY_PROFILE=1.
    """
    if os.environ.get("AURORA_PROXY_PROFILE") != "1":
        return

    try:
        import builtins
    except Exception:
        return

    original_import = builtins.__import__
    if getattr(original_import, "_aurora_proxy_profile_hook", False):
        return

    _hook_state = {"active": False}

    def _profiling_import(name, globals=None, locals=None, fromlist=(), level=0):
        module = original_import(name, globals, locals, fromlist, level)
        if _hook_state["active"]:
            return module
        if name != "ray.serve._private.proxy":
            return module

        try:
            _hook_state["active"] = True
            _wrap_proxy_actor_class(module)
        finally:
            _hook_state["active"] = False
        return module

    _profiling_import._aurora_proxy_profile_hook = True
    builtins.__import__ = _profiling_import
    _patch_log("Installed ProxyActor profiling import hook")


def _wrap_proxy_actor_class(proxy_module) -> None:
    """Monkey-patch ProxyActor.__init__ and ready() for profiling."""
    import time as _time
    import json as _json
    import socket as _socket

    cls = getattr(proxy_module, "ProxyActor", None)
    if cls is None or getattr(cls, "_aurora_profiled", False):
        return

    _orig_init = cls.__init__
    _orig_ready = cls.ready

    # When this code runs, the proxy module is being imported inside the
    # ProxyActor worker process. Record the wall time as an approximation
    # of when Python became available (after C++ metrics timeout).
    _process_python_start = _time.time()

    def _profiled_init(self, *args, **kwargs):
        self._aurora_profile = {
            "process_python_start_s": _process_python_start,
            "init_start_s": _time.time(),
            "hostname": _socket.gethostname(),
            "pid": os.getpid(),
        }
        try:
            _orig_init(self, *args, **kwargs)
        finally:
            self._aurora_profile["init_end_s"] = _time.time()
            self._aurora_profile["init_duration_s"] = round(
                self._aurora_profile["init_end_s"] - self._aurora_profile["init_start_s"], 4
            )
            self._aurora_profile["node_id"] = getattr(self, "_node_id", "unknown")

    async def _profiled_ready(self):
        if hasattr(self, "_aurora_profile"):
            self._aurora_profile["ready_start_s"] = _time.time()
        try:
            result = await _orig_ready(self)
        finally:
            if hasattr(self, "_aurora_profile"):
                self._aurora_profile["ready_end_s"] = _time.time()
                self._aurora_profile["ready_duration_s"] = round(
                    self._aurora_profile["ready_end_s"] - self._aurora_profile["ready_start_s"], 4
                )
                self._aurora_profile["total_python_s"] = round(
                    self._aurora_profile["ready_end_s"] - _process_python_start, 4
                )
                _save_proxy_profile(self._aurora_profile)
        return result

    cls.__init__ = _profiled_init
    cls.ready = _profiled_ready
    cls._aurora_profiled = True
    _patch_log("Wrapped ProxyActor.__init__ and ready() for profiling")


def _save_proxy_profile(profile: dict) -> None:
    """Write proxy profile to /tmp/aurora_proxy_profile/<hostname>_<pid>.json"""
    import json as _json
    profile_dir = "/tmp/aurora_proxy_profile"
    try:
        os.makedirs(profile_dir, exist_ok=True)
        path = os.path.join(profile_dir, f"{profile.get('hostname', 'unknown')}_{profile.get('pid', 0)}.json")
        with open(path, "w") as f:
            _json.dump(profile, f, indent=2)
    except Exception as e:
        print(f"[AuroraProxyProfile] Failed to save profile: {e}", flush=True)


# perf-inst-dev: disabled — overlay proxy.py carries the ProxyActor profiling
# directly in ProxyActor.__init__/ready(). Restore from main-repo commit 2f32633 if needed.
# _install_proxy_actor_profiling_hook()
