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
