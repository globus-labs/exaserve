"""Shared utilities for the eval control plane.

Low-level helpers used across multiple modules: YAML/JSON I/O, stable
content hashing, dataclass serialization, dotted-path field access for
matrix expansion, path resolution, and slug generation.
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import re
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

import yaml


def utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(path: str | Path) -> str:
    path_str = os.path.abspath(os.fspath(path))
    os.makedirs(path_str, exist_ok=True)
    return path_str


def slugify(value: Any) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text or "value"


def deep_copy(value: Any) -> Any:
    return copy.deepcopy(value)


def dataclass_to_dict(value: Any) -> Any:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        return {str(k): dataclass_to_dict(v) for k, v in value.items()}
    if isinstance(value, list):
        return [dataclass_to_dict(v) for v in value]
    if isinstance(value, tuple):
        return [dataclass_to_dict(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value


def canonical_data(value: Any) -> Any:
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        return {str(k): canonical_data(value[k]) for k in sorted(value)}
    if isinstance(value, list):
        return [canonical_data(v) for v in value]
    if isinstance(value, tuple):
        return [canonical_data(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value


def stable_hash(value: Any, length: int = 12) -> str:
    payload = json.dumps(canonical_data(value), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def load_yaml_file(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise TypeError(f"Expected YAML mapping at {path}, got {type(data).__name__}")
    return data


def dump_yaml_file(path: str | Path, data: Any) -> None:
    parent = os.path.dirname(os.path.abspath(os.fspath(path)))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(dataclass_to_dict(data), handle, sort_keys=False)


def dump_json_file(path: str | Path, data: Any) -> None:
    parent = os.path.dirname(os.path.abspath(os.fspath(path)))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(dataclass_to_dict(data), handle, indent=2, sort_keys=True)


def resolve_path(raw_path: str | None, *, base_dir: str | None = None) -> str | None:
    if not raw_path:
        return None
    expanded = os.path.expanduser(raw_path)
    if os.path.isabs(expanded):
        return expanded
    if base_dir:
        return os.path.abspath(os.path.join(base_dir, expanded))
    return os.path.abspath(expanded)


def dotted_get(data: Any, dotted_path: str) -> Any:
    current = data
    for part in dotted_path.split("."):
        if not hasattr(current, part):
            raise KeyError(f"Unknown field path: {dotted_path}")
        current = getattr(current, part)
    return current


def dotted_set(data: Any, dotted_path: str, value: Any) -> None:
    parts = dotted_path.split(".")
    current = data
    for part in parts[:-1]:
        if not hasattr(current, part):
            raise KeyError(f"Unknown field path: {dotted_path}")
        current = getattr(current, part)
    final_part = parts[-1]
    if not hasattr(current, final_part):
        raise KeyError(f"Unknown field path: {dotted_path}")
    setattr(current, final_part, value)


def format_template(template: str, values: dict[str, Any]) -> str:
    normalized = {key: values[key] for key in values}
    return template.format(**normalized)
