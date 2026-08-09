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


def utc_timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(path: str | Path) -> str:
    from exaserve.state.atomic import ensure_owned_directory

    return ensure_owned_directory(path)


def slugify(value: Any) -> str:
    text = str(value).strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text)
    text = text.strip("-")
    return text or "value"


def deep_copy(value: Any) -> Any:
    return copy.deepcopy(value)


def dataclass_to_dict(value: Any) -> Any:
    materializer = getattr(value, "to_materialization_dict", None)
    if callable(materializer):
        value = materializer()
    if is_dataclass(value):
        value = asdict(value)
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("serialized mappings must have string keys")
        return {key: dataclass_to_dict(nested) for key, nested in value.items()}
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
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical mappings must have string keys")
        return {key: canonical_data(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [canonical_data(v) for v in value]
    if isinstance(value, tuple):
        return [canonical_data(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    return value


def stable_hash(value: Any, length: int = 12) -> str:
    payload = json.dumps(
        canonical_data(value), sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def load_yaml_file(path: str | Path) -> dict[str, Any]:
    from exaserve.yaml_support import load_yaml_mapping

    return load_yaml_mapping(path)


def dump_yaml_file(path: str | Path, data: Any) -> None:
    # WP2.1 (PR-035): atomic temp+rename publish; a crash or concurrent
    # reader sees either the previous file or the complete new one.
    from exaserve.state.atomic import atomic_write_yaml

    parent = os.path.dirname(os.path.abspath(os.fspath(path)))
    if parent:
        ensure_dir(parent)
    atomic_write_yaml(path, dataclass_to_dict(data))


def dump_json_file(path: str | Path, data: Any) -> None:
    from exaserve.state.atomic import atomic_write_json

    parent = os.path.dirname(os.path.abspath(os.fspath(path)))
    if parent:
        ensure_dir(parent)
    atomic_write_json(path, dataclass_to_dict(data))


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
        if part.isdigit() and isinstance(current, (list, tuple)):
            index = int(part)
            if index >= len(current):
                raise KeyError(f"Unknown field path: {dotted_path}")
            current = current[index]
        elif hasattr(current, part):
            current = getattr(current, part)
        else:
            raise KeyError(f"Unknown field path: {dotted_path}")
    return current


def dotted_set(data: Any, dotted_path: str, value: Any) -> None:
    parts = dotted_path.split(".")
    current = data
    for part in parts[:-1]:
        if part.isdigit():
            current = current[int(part)]
        elif hasattr(current, part):
            current = getattr(current, part)
        else:
            raise KeyError(f"Unknown field path: {dotted_path}")
    final_part = parts[-1]
    if final_part.isdigit():
        current[int(final_part)] = value
    elif hasattr(current, final_part):
        setattr(current, final_part, value)
    else:
        raise KeyError(f"Unknown field path: {dotted_path}")


def format_template(template: str, values: dict[str, Any]) -> str:
    normalized = {key: values[key] for key in values}
    return template.format(**normalized)


def result_is_complete(result_data: dict) -> tuple[bool, str]:
    """Validate exact per-repeat distributed result completeness.

    Historical/count-only output is intentionally not upgraded to success.
    Analysis of a production result must be bound to the same shard evidence
    that made the run's ``ResultManifest`` complete.
    """
    if not isinstance(result_data, dict):
        return False, "result is not an object"
    meta = result_data.get("meta")
    if not isinstance(meta, dict):
        return False, "result lacks structured meta"
    completed_runs = meta.get("completed_runs")
    gathers = meta.get("gather_by_run")
    per_run = result_data.get("per_run")
    if (
        isinstance(completed_runs, bool)
        or not isinstance(completed_runs, int)
        or completed_runs < 1
    ):
        return False, "meta.completed_runs must be a positive integer"
    if not isinstance(gathers, list) or len(gathers) != completed_runs:
        return False, "gather_by_run does not cover every completed run"
    if not isinstance(per_run, list) or len(per_run) != completed_runs:
        return False, "per_run does not cover every completed run"
    for run_index, gather in enumerate(gathers):
        if not isinstance(gather, dict) or gather.get("complete") is not True:
            return False, f"run {run_index} gather is absent or incomplete"
        expected = gather.get("expected_ranks")
        collected = gather.get("collected_ranks")
        missing = gather.get("missing_ranks")
        shards = gather.get("shards")
        if isinstance(expected, bool) or not isinstance(expected, int) or expected < 1:
            return False, f"run {run_index} expected_ranks is invalid"
        exact_ranks = list(range(expected))
        if collected != exact_ranks or missing != []:
            return False, (
                f"run {run_index} rank set is incomplete: "
                f"collected={collected}, expected={exact_ranks}, missing={missing}"
            )
        if (
            not isinstance(shards, list)
            or len(shards) != expected
            or sorted(item.get("rank") for item in shards if isinstance(item, dict)) != exact_ranks
        ):
            return False, f"run {run_index} shard evidence is incomplete"
    return True, "complete"
