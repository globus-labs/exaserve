"""
Model staging and hydration utilities for Aurora Ray Server.

This module handles downloading and caching models to a specified lustre path
before launching services, ensuring all vLLM instances can use local copies
instead of downloading from HuggingFace directly.
"""

import json
import os
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


def check_model_exists(model_path: Path) -> bool:
    """
    Check if a model already exists in the specified path.
    
    Args:
        model_path: Path to the model directory
        
    Returns:
        bool: True if model exists and appears complete, False otherwise
    """
    if not model_path.exists():
        return False
    
    # Check for common model files to verify completeness
    required_files = ["config.json"]
    has_weights = False
    
    for file in model_path.iterdir():
        if file.name in required_files:
            required_files.remove(file.name)
        if file.suffix in [".bin", ".safetensors", ".pt"]:
            has_weights = True
    
    return len(required_files) == 0 and has_weights


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
        candidates = sorted(p for p in snapshots_dir.iterdir() if p.is_dir())
        if candidates:
            return candidates[0]
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
    
    try:
        from huggingface_hub import snapshot_download

        # Create parent directory if it doesn't exist
        local_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download model using huggingface_hub
        if tokenizer_only:
            # Only download tokenizer files
            allow_patterns = ["*.json", "*.txt", "*.model", "tokenizer*"]
        else:
            # Download all model files
            allow_patterns = None
        
        downloaded_path = snapshot_download(
            repo_id=model_id,
            local_dir=str(local_path),
            local_dir_use_symlinks=False,
            allow_patterns=allow_patterns,
        )
        
        elapsed = time.time() - start_time
        print_red(f"[ModelStaging] ✓ Downloaded {model_id} in {elapsed:.2f}s")
        
        return downloaded_path
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
