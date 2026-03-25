from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional

from src.schemas import ExpConfig, TraceGeneratorConfig, WeakScalingConfig

try:
    from site_config import get_site_config
except ImportError:  # pragma: no cover - package-mode fallback
    from src.site_config import get_site_config

from eval.lib.catalog import find_spec_path, list_spec_names


SITE_CONFIG = get_site_config()

DEFAULT_DATA_ROOT = SITE_CONFIG.user_data_root
DEFAULT_MODEL_PATH = SITE_CONFIG.model_storage_path
DEFAULT_INPUT_TRACE_PATH = SITE_CONFIG.input_trace_path
DEFAULT_INPUT_PROMPT_PATH = SITE_CONFIG.input_prompt_path
DEFAULT_OUTPUT_TRACE_DIR = SITE_CONFIG.output_trace_dir
DEFAULT_EXPERIMENTS_ROOT = SITE_CONFIG.experiments_root

EXPERIMENT_REGISTRY = {name: find_spec_path(name) for name in list_spec_names()}


@dataclass
class WeakScalingExpParams:
    """
    Deprecated compatibility placeholder.

    The experiment source of truth now lives in eval/specs/*.yaml.
    """

    batch_name: str = ""
    num_nodes_list: List[int] = field(default_factory=list)
    null_compute: bool = False
    rate_per_node: float = 80.0
    duration: float = 5.0
    input_len: int = 2048
    output_len: int = 512
    model_id: str = "meta-llama/Meta-Llama-3-8B-Instruct"
    model_tensor_parallel_size: int = 1
    model_pipeline_parallel_size: int = 1
    model_num_replicas: Optional[int] = None
    model_max_model_len: int = 4096
    model_size: int = 8
    model_storage_path: str = DEFAULT_MODEL_PATH
    local_stage_path: str = SITE_CONFIG.local_stage_path
    deployment_worker_max_ongoing: int = 64
    client_num_runs: int = 1
    client_num_go_procs: int = 16
    client_num_go_workers: int = 2
    client_go_concurrency: int = 40
    client_warmup_rps: int = 0
    client_warmup_duration_s: float = 0.0
    client_dest: str = "proxy"
    proxy_type: str = "litellm"
    proxy_python_path: str = SITE_CONFIG.litellm_python_path
    proxy_num_workers: int = 1


def build_weak_scaling_configs(*_args, **_kwargs):
    raise RuntimeError(
        "build_weak_scaling_configs() is deprecated. "
        "Use `python -m eval.cli run materialize <spec.yaml>` instead."
    )
