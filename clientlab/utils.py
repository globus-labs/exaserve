import copy
import datetime
import hashlib
import json
from pathlib import Path

from exaserve.state.atomic import (
    atomic_write_text as _atomic_write_text,
    ensure_owned_directory,
    regular_file_reader,
    strict_json_load,
)
from exaserve.yaml_support import load_yaml_mapping

try:
    import yaml
except ImportError:  # pragma: no cover - depends on runtime env
    yaml = None


def utc_timestamp():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_dir(path):
    return ensure_owned_directory(path)


def canonical_data(value):
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("canonical mappings must have string keys")
        return {key: canonical_data(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [canonical_data(item) for item in value]
    if isinstance(value, tuple):
        return [canonical_data(item) for item in value]
    if isinstance(value, Path):
        return str(value)
    return value


def stable_hash(value, length=12):
    payload = json.dumps(
        canonical_data(value), sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def load_yaml_file(path):
    if yaml is not None:
        payload = load_yaml_mapping(path)
    else:
        suffix = str(Path(path).suffix).lower()
        if suffix in {".yaml", ".yml"}:
            raise RuntimeError(
                "PyYAML is not available in this environment. Use a JSON ClientLab spec or install PyYAML."
            )
        with regular_file_reader(path) as handle:
            payload = strict_json_load(handle)
    if not isinstance(payload, dict):
        raise TypeError("Expected YAML mapping at %s" % path)
    return payload


def atomic_write_text(target: Path, text: str) -> None:
    """PR-035: same-directory temp + fsync + rename.

    A reader that polls these files while a run is still writing them would
    otherwise see a truncated document and mis-parse it as a completed result.
    ClientLab is shipped with ExaServe and delegates to the same atomic state
    primitive, avoiding a subtly different second publication protocol.
    """
    _atomic_write_text(target, text)


def dump_yaml_file(path, data):
    payload = copy.deepcopy(data)
    if yaml is not None:
        text = yaml.safe_dump(payload, sort_keys=False)
    else:
        text = json.dumps(payload, indent=2, sort_keys=False, allow_nan=False) + "\n"
    atomic_write_text(Path(path), text)


def dump_json_file(path, data):
    atomic_write_text(
        Path(path),
        json.dumps(copy.deepcopy(data), indent=2, sort_keys=True, allow_nan=False) + "\n",
    )
