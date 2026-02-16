"""
Aurora model deployment configuration.

This module provides functions to generate complete DeploymentConfig for deploying
diverse models on the Aurora Ray Server. All configuration is centralized here.
"""

import os
from typing import List
from schemas import ModelConfig, DeploymentConfig


def get_default_config() -> DeploymentConfig:
    """
    Returns default deployment configuration with single Llama-3-8B-Instruct model.
    
    Returns:
        DeploymentConfig: Complete deployment configuration
    """
    return DeploymentConfig(
        deployment_name="default",
        model_configs=[
            ModelConfig(
                model_id="meta-llama/Meta-Llama-3-8B-Instruct",
                tensor_parallel_size=1,
                max_model_len=4096,
                size=8,
                num_replicas=None,
                num_routers_per_replica=0.5,
            )
        ],
        model_storage_path="/lus/flare/projects/AuroraGPT/wenyiw/models",
        worker_max_ongoing=32,
        router_max_ongoing=200,
        num_nodes=1,
    )

def get_tp2_config() -> DeploymentConfig:
    """
    Returns deployment configuration for tp2 models.
    
    Returns:
        DeploymentConfig: Configuration optimized for tp2 models
    """
    return DeploymentConfig(
        deployment_name="tp2",
        model_configs=[
            ModelConfig(
                model_id="meta-llama/Meta-Llama-3-8B-Instruct",
                tensor_parallel_size=2,
                max_model_len=4096,
                size=8,
                num_replicas=None,
            )
        ],
        model_storage_path="/lus/flare/projects/AuroraGPT/wenyiw/models",
        worker_max_ongoing=32,
        router_max_ongoing=200,
        num_nodes=1,
    )

def get_diverse_config() -> DeploymentConfig:
    """
    Returns deployment configuration with diverse set of models.
    
    Returns:
        DeploymentConfig: Complete deployment configuration with multiple models
    """
    return DeploymentConfig(
        deployment_name="diverse",
        model_configs=[
            ModelConfig(
                model_id="meta-llama/Meta-Llama-3-8B-Instruct",
                tensor_parallel_size=1,
                max_model_len=4096,
                size=8,
                num_replicas=6,
            ),
            ModelConfig(
                model_id="meta-llama/Meta-Llama-3-70B-Instruct",
                tensor_parallel_size=2,
                max_model_len=4096,
                size=70,
                num_replicas=3,
            ),
            ModelConfig(
                model_id="mistralai/Mistral-7B-Instruct-v0.3",
                tensor_parallel_size=1,
                max_model_len=4096,
                size=7,
                num_replicas=3,
            ),
        ],
        worker_max_ongoing=16,
        num_nodes=1,
    )

def get_weak_scaling_config() -> DeploymentConfig:
    """
    Returns deployment configuration for weak scaling experiments.
    
    Returns:
        DeploymentConfig: Configuration optimized for weak scaling
    """
    return DeploymentConfig(
        deployment_name="weak_scaling",
        model_configs=[
            ModelConfig(
                model_id="meta-llama/Meta-Llama-3-8B",
                tensor_parallel_size=1,
                max_model_len=4096,
                size=8,
            )
        ],
        worker_max_ongoing=16,
        num_nodes=1,
    )

def get_deployment_config(config_name: str = None) -> DeploymentConfig:
    """
    Returns the appropriate deployment configuration based on config name.
    
    Args:
        config_name: Configuration name ("default", "diverse", "weak_scaling", "tp2")
    Returns:
        DeploymentConfig: Complete deployment configuration
    """
    if config_name is None:
        config_name = os.getenv("AURORA_CONFIG", "default")
    
    # Map config names to functions
    configs = {
        "default": get_default_config,
        "diverse": get_diverse_config,
        "weak_scaling": get_weak_scaling_config,
        "tp2": get_tp2_config,
    }
    
    config_func = configs.get(config_name, get_default_config)
    return config_func()
