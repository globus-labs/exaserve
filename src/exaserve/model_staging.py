"""
Model staging and hydration utilities for ExaServe.

This module handles downloading and caching models to a specified lustre path
before launching services, ensuring all vLLM instances can use local copies
instead of downloading from HuggingFace directly.
"""

import json
import os
import shutil
import time
from pathlib import Path
from typing import Dict, List
from .schemas import ModelConfig
from .model_paths import (
    get_model_storage_path,
    get_model_storage_name,
    iter_unique_model_ids,
)


def print_red(message: str):
    """Print message in red color."""
    RED = '\033[91m'
    RESET = '\033[0m'
    print(f"{RED}{message}{RESET}", flush=True)


COMPLETION_MARKER = ".exaserve_complete.json"


def _validate_model_dir(model_path: Path) -> tuple[bool, str]:
    """PR-005: structural completeness check.

    Requires config.json and at least one weight file, and — when a
    safetensors/pytorch shard index is present — every shard the index
    references. This catches the "interrupted after shard k of N" case that
    the old any-one-weight-file check classified as complete.
    """
    if not (model_path / "config.json").is_file():
        return False, "missing config.json"

    names = {p.name for p in model_path.iterdir() if p.is_file()}
    weights = [n for n in names if n.endswith((".safetensors", ".bin", ".pt"))]
    if not weights:
        return False, "no weight files"

    for index_name in ("model.safetensors.index.json", "pytorch_model.bin.index.json"):
        index_path = model_path / index_name
        if index_path.is_file():
            try:
                weight_map = json.loads(index_path.read_text()).get("weight_map", {})
            except (json.JSONDecodeError, OSError) as exc:
                return False, f"unreadable {index_name}: {exc}"
            shards = set(weight_map.values())
            missing = sorted(s for s in shards if s not in names)
            if missing:
                return False, f"{index_name} references {len(missing)} missing shard(s): {missing[:3]}"
    return True, "ok"


def check_model_exists(model_path: Path) -> bool:
    """
    Check if a model already exists and appears complete.

    Prefers an explicit completion marker (written last after a validated
    download). For pre-existing directories staged before markers existed,
    falls back to structural validation and upgrades the marker in place so
    subsequent checks are cheap and unambiguous.
    """
    if not model_path.exists():
        return False

    marker = model_path / COMPLETION_MARKER
    if marker.is_file():
        return True

    complete, _reason = _validate_model_dir(model_path)
    if complete:
        # Upgrade a legacy-complete directory to a marker-bearing one.
        try:
            _write_completion_marker(model_path)
        except OSError:
            pass  # read-only store: still complete, just not upgradeable
    return complete


def _write_completion_marker(model_path: Path) -> None:
    from exaserve.state.atomic import atomic_write_json

    inventory = {
        p.name: p.stat().st_size
        for p in sorted(model_path.iterdir())
        if p.is_file() and p.name != COMPLETION_MARKER
    }
    atomic_write_json(model_path / COMPLETION_MARKER, {
        "version": 1,
        "file_count": len(inventory),
        "total_bytes": sum(inventory.values()),
        "files": inventory,
    })


def get_model_dir_state(model_path: Path) -> str:
    """
    Return 'missing', 'partial', or 'complete' for a model directory.
    """
    if not model_path.exists():
        return "missing"
    return "complete" if check_model_exists(model_path) else "partial"


def _resolve_hf_cache_snapshot(cache_dir: Path) -> Path | None:
    """
    Resolve a usable snapshot directory from a Hugging Face cache directory.
    """
    refs_main = cache_dir / "refs" / "main"
    snapshot_id = None
    if refs_main.is_file():
        snapshot_id = refs_main.read_text().strip()

    snapshots_dir = cache_dir / "snapshots"
    if snapshot_id:
        snapshot_dir = snapshots_dir / snapshot_id
        if snapshot_dir.is_dir():
            return snapshot_dir

    if snapshots_dir.is_dir():
        # PR-005: pick the NEWEST snapshot, not the lexicographically first
        # (commit-SHA-named dirs sort arbitrarily). refs/main above is still
        # preferred; this is the ambiguous fallback.
        candidates = [p for p in snapshots_dir.iterdir() if p.is_dir()]
        if candidates:
            return max(candidates, key=lambda p: p.stat().st_mtime)
    return None


def resolve_existing_model_path(model_id: str, storage_path: str) -> Path | None:
    """
    Resolve an already-downloaded model directory under `storage_path`.

    Supports both the repo's flat cache layout (`org--name`) and the Hugging
    Face shared cache layout (`models--org--name/snapshots/<rev>`), including a
    nested `hub/` directory when present.
    """
    base_path = Path(storage_path)
    flat_dir = get_model_storage_path(model_id, base_path)
    if get_model_dir_state(flat_dir) == "complete":
        return flat_dir

    hf_cache_name = f"models--{get_model_storage_name(model_id)}"
    cache_roots = [base_path, base_path / "hub"]
    for root in cache_roots:
        cache_dir = root / hf_cache_name
        if not cache_dir.is_dir():
            continue
        snapshot_dir = _resolve_hf_cache_snapshot(cache_dir)
        if snapshot_dir and get_model_dir_state(snapshot_dir) == "complete":
            return snapshot_dir
    return None


def load_model_config(model_path: Path) -> dict:
    """
    Load the Hugging Face `config.json` for a resolved model directory.
    """
    config_path = model_path / "config.json"
    if not config_path.is_file():
        raise FileNotFoundError(f"Missing config.json for model at {model_path}")
    return json.loads(config_path.read_text())


def validate_tensor_parallel_compatibility(
    model_id: str,
    model_path: Path,
    tensor_parallel_size: int,
) -> None:
    """
    Fail fast when the model architecture cannot support the requested TP size.

    This catches obvious incompatibilities before we spend time staging the
    model to node-local storage and before Ray/vLLM startup.
    """
    config_data = load_model_config(model_path)
    num_attention_heads = config_data.get("num_attention_heads")
    if not isinstance(num_attention_heads, int) or num_attention_heads < 1:
        return
    if num_attention_heads % tensor_parallel_size != 0:
        raise RuntimeError(
            f"[ModelStaging] Model {model_id} at {model_path} is incompatible with "
            f"tensor_parallel_size={tensor_parallel_size}: num_attention_heads="
            f"{num_attention_heads} is not divisible by {tensor_parallel_size}."
        )


def download_model(model_id: str, local_path: Path, tokenizer_only: bool = False) -> str:
    """
    Download a model from HuggingFace to the specified local path.
    
    Args:
        model_id: HuggingFace model ID (e.g., "meta-llama/Meta-Llama-3-8B-Instruct")
        local_path: Local path where the model should be stored
        tokenizer_only: If True, only download tokenizer files
        
    Returns:
        str: Path to the downloaded model
    """
    print(f"[ModelStaging] Downloading {model_id} to {local_path}...", flush=True)
    start_time = time.time()

    from exaserve.state.atomic import ExclusiveLease, LeaseHeldError

    try:
        from huggingface_hub import snapshot_download

        local_path.parent.mkdir(parents=True, exist_ok=True)
        allow_patterns = (
            ["*.json", "*.txt", "*.model", "tokenizer*"] if tokenizer_only else None
        )

        # PR-005: transactional staging. One lease per model prevents
        # concurrent stagers from racing into the same tree; the download
        # lands in a sibling staging dir, is validated, marked complete, and
        # only then atomically published to the final path.
        lease_path = local_path.parent / f".{local_path.name}.download.lease"
        try:
            lease = ExclusiveLease(lease_path, ttl_s=7200,
                                   owner_note=f"download {model_id}").acquire()
        except LeaseHeldError:
            print(f"[ModelStaging] Another stager holds {model_id}; waiting...",
                  flush=True)
            # Wait for the winner; then use its result if complete.
            deadline = time.time() + 7200
            while time.time() < deadline:
                time.sleep(5)
                if check_model_exists(local_path):
                    print(f"[ModelStaging] ✓ {model_id} completed by another stager",
                          flush=True)
                    return str(local_path)
            raise RuntimeError(f"timed out waiting for concurrent download of {model_id}")

        try:
            if check_model_exists(local_path):  # re-check under lease
                return str(local_path)
            staging = local_path.parent / f".{local_path.name}.staging"
            if staging.exists():
                shutil.rmtree(staging)
            snapshot_download(
                repo_id=model_id,
                local_dir=str(staging),
                local_dir_use_symlinks=False,
                allow_patterns=allow_patterns,
            )
            if not tokenizer_only:
                complete, reason = _validate_model_dir(staging)
                if not complete:
                    raise RuntimeError(
                        f"downloaded {model_id} failed validation: {reason}")
            _write_completion_marker(staging)
            # Atomic publish. If final exists (partial), clear it first — we
            # hold the lease so no other writer is active.
            if local_path.exists():
                shutil.rmtree(local_path)
            os.replace(staging, local_path)
        finally:
            lease.release()

        elapsed = time.time() - start_time
        print_red(f"[ModelStaging] ✓ Downloaded {model_id} in {elapsed:.2f}s")
        return str(local_path)
    except Exception as e:
        elapsed = time.time() - start_time
        print_red(f"[ModelStaging] ✗ Failed to download {model_id} after {elapsed:.2f}s: {e}")
        raise


def stage_models(model_configs: List[ModelConfig], storage_path: str) -> dict:
    """
    Stage all models specified in model_configs to the storage path.
    Downloads models if they don't already exist locally.
    
    Args:
        model_configs: List of ModelConfig objects
        storage_path: Base path for storing models (e.g., lustre path)
        
    Returns:
        dict: Mapping of model_id to local path
    """
    storage_path = Path(storage_path)
    storage_path.mkdir(parents=True, exist_ok=True)
    
    model_paths: Dict[str, str] = {}
    unique_models = list(iter_unique_model_ids(model_configs))
    
    print(f"[ModelStaging] Staging {len(unique_models)} unique model(s) to {storage_path}", flush=True)
    total_start = time.time()
    
    for model_id in unique_models:
        existing_path = resolve_existing_model_path(model_id, str(storage_path))
        local_path = existing_path or get_model_storage_path(model_id, storage_path)
        state = get_model_dir_state(local_path)

        if state == "complete":
            print(f"[ModelStaging] ✓ Model {model_id} already exists at {local_path}", flush=True)
            model_paths[model_id] = str(local_path)
        elif state == "partial":
            raise RuntimeError(
                f"[ModelStaging] Found partial/corrupt model directory for {model_id} at {local_path}. "
                "Refusing to overwrite it automatically."
            )
        else:
            print(f"[ModelStaging] Model {model_id} not found, downloading...", flush=True)
            try:
                downloaded_path = download_model(model_id, local_path)
                model_paths[model_id] = downloaded_path
            except Exception as e:
                print(f"[ModelStaging] Failed to stage {model_id}: {e}", flush=True)
                raise
    
    total_elapsed = time.time() - total_start
    print_red(f"[ModelStaging] ✓ All models staged in {total_elapsed:.2f}s")
    
    return model_paths


def resolve_model_paths(
    model_configs: List[ModelConfig],
    storage_path: str,
    require_complete: bool = False,
) -> Dict[str, str]:
    """
    Resolve model IDs to cache paths under `storage_path`.

    When `require_complete` is True, every resolved directory must already exist
    and pass the completeness check.
    """
    model_paths: Dict[str, str] = {}
    for model_id in iter_unique_model_ids(model_configs):
        local_path = get_model_storage_path(model_id, storage_path)
        state = get_model_dir_state(local_path)
        if require_complete and state != "complete":
            raise RuntimeError(
                f"[ModelStaging] Expected a complete staged model for {model_id} at {local_path}, "
                f"but found state={state}."
            )
        model_paths[model_id] = str(local_path)
    return model_paths


def get_local_model_path(model_id: str, storage_path: str) -> str:
    """
    Get the local path for a model.
    
    Args:
        model_id: HuggingFace model ID
        storage_path: Base storage path
        
    Returns:
        str: Local path to the model
    """
    return str(get_model_storage_path(model_id, storage_path))
