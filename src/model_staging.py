"""
Model staging and hydration utilities for Aurora Ray Server.

This module handles downloading and caching models to a specified lustre path
before launching services, ensuring all vLLM instances can use local copies
instead of downloading from HuggingFace directly.
"""

import os
import time
from pathlib import Path
from typing import Dict, List
from schemas import ModelConfig
from model_paths import (
    get_model_storage_path,
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
        local_path = get_model_storage_path(model_id, storage_path)
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
