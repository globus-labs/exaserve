"""Spec catalog: discovery and resolution of eval/specs/*.yaml files.

Spec files are the single source of truth for experiment definitions.
This module provides lookup by name (stem of the filename) or by absolute
path, so callers don't need to know the filesystem layout.
"""

from __future__ import annotations

import os
from pathlib import Path


def spec_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "specs"))


def list_spec_paths() -> list[str]:
    root = Path(spec_root())
    if not root.is_dir():
        return []
    return sorted(str(path) for path in root.rglob("*.yaml"))


def list_spec_names() -> list[str]:
    names = []
    for path in list_spec_paths():
        names.append(os.path.splitext(os.path.basename(path))[0])
    return sorted(set(names))


def spec_group_relpath(name_or_path: str) -> str:
    """Directory of a spec relative to eval/specs ('' for flat or unknown specs).

    Lets the results tree mirror the spec tree: a spec living at
    eval/specs/sc26workshop/full/foo.yaml gets its run groups under
    runs/sc26workshop/full/foo/ instead of the flat runs/foo/.
    """
    try:
        spec_path = find_spec_path(name_or_path)
    except (FileNotFoundError, RuntimeError):
        return ""
    rel = os.path.relpath(os.path.dirname(spec_path), spec_root())
    return "" if rel == "." else rel


def find_spec_path(name_or_path: str) -> str:
    if os.path.isfile(name_or_path):
        return os.path.abspath(name_or_path)

    candidates = [
        path
        for path in list_spec_paths()
        if os.path.splitext(os.path.basename(path))[0] == name_or_path
    ]
    if not candidates:
        raise FileNotFoundError(f"Unknown spec: {name_or_path}")
    if len(candidates) > 1:
        raise RuntimeError(f"Spec name {name_or_path} is ambiguous: {candidates}")
    return candidates[0]
