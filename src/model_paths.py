from pathlib import Path
from typing import Iterable, Iterator, Union


def get_model_storage_name(model_id: str) -> str:
    """Return the filesystem-safe cache directory name for a model."""
    return model_id.replace("/", "--")


def get_model_route_name(model_id: str) -> str:
    """Return the HTTP route-safe name for a model."""
    return get_model_storage_name(model_id).replace(".", "-")


def get_model_storage_path(model_id: str, base_path: Union[str, Path]) -> Path:
    """Return the full cache directory path for a model under `base_path`."""
    return Path(base_path) / get_model_storage_name(model_id)


def iter_unique_model_ids(model_configs: Iterable[object]) -> Iterator[str]:
    """Yield model IDs once each while preserving their first-seen order."""
    seen = set()
    for config in model_configs:
        model_id = getattr(config, "model_id")
        if model_id not in seen:
            seen.add(model_id)
            yield model_id
