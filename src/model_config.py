"""
Model configuration dataclass for Aurora Ray Server.

This module defines the ModelConfig and DeploymentConfig dataclasses used to configure model deployments.
"""

from dataclasses import dataclass, field
from typing import Optional, List


@dataclass
class ModelConfig:
    """Configuration for a single model deployment."""
    model_id: str
    mode: str
    tensor_parallel_size: int
    size: int
    num_replicas: Optional[int] = None
    node_index: Optional[int] = None
    tokenizer_path: Optional[str] = None


@dataclass
class DeploymentConfig:
    """Complete deployment configuration including models and system settings."""
    # Model configurations
    model_configs: List[ModelConfig]
    
    # System configuration
    num_gpu_tiles: int = 12
    num_routers: int = 4
    worker_max_ongoing: int = 16
    model_storage_path: str = "/lus/flare/projects/AuroraGPT/wenyiw/models"
    
    # PVC-specific settings
    tiles_per_card: int = 2
    init_stagger_seconds: int = 5
    engine_init_retries: int = 3
    
    # Deployment name for identification
    deployment_name: str = "default"
