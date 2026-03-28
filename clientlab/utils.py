import copy
import datetime
import hashlib
import json
import os
from pathlib import Path

try:
    import yaml
except ImportError:  # pragma: no cover - depends on runtime env
    yaml = None


def utc_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(path):
    path_str = os.path.abspath(os.fspath(path))
    if not os.path.isdir(path_str):
        os.makedirs(path_str)
    return path_str


def canonical_data(value):
    if isinstance(value, dict):
        return {str(key): canonical_data(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [canonical_data(item) for item in value]
    if isinstance(value, tuple):
        return [canonical_data(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def stable_hash(value, length=12):
    payload = json.dumps(canonical_data(value), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def load_yaml_file(path):
    with open(path, "r", encoding="utf-8") as handle:
        if yaml is not None:
            payload = yaml.safe_load(handle) or {}
        else:
            suffix = str(Path(path).suffix).lower()
            if suffix in {".yaml", ".yml"}:
                raise RuntimeError(
                    "PyYAML is not available in this environment. Use a JSON ClientLab spec or install PyYAML."
                )
            payload = json.load(handle)
    if not isinstance(payload, dict):
        raise TypeError("Expected YAML mapping at %s" % path)
    return payload


def dump_yaml_file(path, data):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        if yaml is not None:
            yaml.safe_dump(copy.deepcopy(data), handle, sort_keys=False)
        else:
            json.dump(copy.deepcopy(data), handle, indent=2, sort_keys=False)
            handle.write("\n")


def dump_json_file(path, data):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(copy.deepcopy(data), handle, indent=2, sort_keys=True)
        handle.write("\n")
