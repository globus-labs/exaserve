"""Small dependency boundary for human-authored YAML input."""

from __future__ import annotations

import os
from typing import Any

from .state.atomic import regular_file_reader


def require_yaml():
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - package gate verifies dependency
        raise RuntimeError("PyYAML is required to read ExaServe YAML input") from exc
    return yaml


def _unique_key_loader(yaml):
    class UniqueKeyLoader(yaml.SafeLoader):
        pass

    def construct_mapping(loader, node, deep=False):
        loader.flatten_mapping(node)
        mapping = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=deep)
            try:
                duplicate = key in mapping
            except TypeError as exc:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    "found an unhashable mapping key",
                    key_node.start_mark,
                ) from exc
            if duplicate:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            mapping[key] = loader.construct_object(value_node, deep=deep)
        return mapping

    UniqueKeyLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping,
    )
    return UniqueKeyLoader


def _require_mapping(payload: Any, source: object) -> dict[str, Any]:
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise TypeError(f"Expected YAML mapping at {source}, got {type(payload).__name__}")
    return payload


def load_yaml_mapping(path: str | os.PathLike) -> dict[str, Any]:
    """Load one mapping while rejecting duplicate keys at every depth."""
    yaml = require_yaml()
    with regular_file_reader(path) as handle:
        payload = yaml.load(handle, Loader=_unique_key_loader(yaml))
    return _require_mapping(payload, path)


def load_yaml_mapping_text(text: str | bytes, *, source: object = "<memory>") -> dict[str, Any]:
    """In-memory variant for content already read for hashing or provenance."""
    if not isinstance(text, (str, bytes)):
        raise TypeError("YAML input must be text or bytes")
    yaml = require_yaml()
    payload = yaml.load(text, Loader=_unique_key_loader(yaml))
    return _require_mapping(payload, source)
