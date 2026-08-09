"""Fail-closed readers for qualification-analysis evidence.

Analysis output is used to make support-envelope decisions.  Returning partial
statistics after a corrupt or unreadable probe file is therefore less safe
than refusing the analysis with the exact evidence location.
"""

from __future__ import annotations

import csv
import json
import math
from pathlib import Path
from typing import Any


class EvidenceReadError(RuntimeError):
    """One input artifact could not be read without losing evidence."""


def read_json_object(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise EvidenceReadError(f"cannot read JSON evidence {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EvidenceReadError(f"JSON evidence {path} must contain an object")
    return value


def read_jsonl_objects(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                try:
                    value = json.loads(line)
                except (UnicodeError, json.JSONDecodeError) as exc:
                    raise EvidenceReadError(
                        f"cannot read JSONL evidence {path}:{line_number}: {exc}"
                    ) from exc
                if not isinstance(value, dict):
                    raise EvidenceReadError(
                        f"JSONL evidence {path}:{line_number} must contain an object"
                    )
                rows.append(value)
    except OSError as exc:
        raise EvidenceReadError(f"cannot read JSONL evidence {path}: {exc}") from exc
    return rows


def read_duration_csv(path: Path) -> list[float]:
    """Read the probe's exact two-column ``timestamp,duration_ms`` schema."""

    durations: list[float] = []
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            for line_number, row in enumerate(csv.reader(handle), start=1):
                if len(row) != 2:
                    raise EvidenceReadError(
                        f"CSV evidence {path}:{line_number} must contain exactly two columns"
                    )
                try:
                    duration_ms = float(row[1])
                except ValueError as exc:
                    raise EvidenceReadError(
                        f"CSV evidence {path}:{line_number} has a non-numeric duration"
                    ) from exc
                if not math.isfinite(duration_ms) or duration_ms < 0:
                    raise EvidenceReadError(
                        f"CSV evidence {path}:{line_number} has an invalid duration"
                    )
                durations.append(duration_ms)
    except OSError as exc:
        raise EvidenceReadError(f"cannot read CSV evidence {path}: {exc}") from exc
    return durations
