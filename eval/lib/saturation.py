"""One strict saturation-search schema shared by specs and runtime manifests."""

from __future__ import annotations

import math
from dataclasses import fields
from typing import Any

from .models import SaturationSpec


_INTEGER_FIELDS = {
    "initial_rate",
    "max_rate",
    "step_up_start",
    "step_up_end",
    "step_up_increment",
}
_NUMBER_FIELDS = {
    "step_duration_s",
    "warmup_duration_s",
    "cooldown_pause_s",
    "tolerance",
    "max_error_rate",
    "plateau_ratio",
    "max_p99_ttft",
}
_BOOLEAN_FIELDS = {"enabled", "verify", "stream"}
_STEP_REQUIRED_FIELDS = {
    "target_rate",
    "achieved_rate",
    "duration_s",
    "completed",
    "failed",
    "error_rate",
    "p50_latency_s",
    "p99_latency_s",
    "mean_latency_s",
    "new_connections",
    "reused_connections",
    "max_observed_active",
    "healthy",
}
_STEP_OPTIONAL_FIELDS = {
    "p50_ttft_s",
    "p99_ttft_s",
    "mean_ttft_s",
    "fail_reasons",
}


def _integer(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{path} must be an integer")
    return value


def _number(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{path} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{path} must be finite")
    return result


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{path} must be a boolean")
    return value


def parse_saturation_spec(value: Any, *, path: str) -> SaturationSpec:
    """Parse and semantically validate a saturation-search mapping."""
    if not isinstance(value, dict) or any(not isinstance(key, str) for key in value):
        raise ValueError(f"{path} must be a mapping with string keys")
    allowed = {item.name for item in fields(SaturationSpec)}
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{path} contains unknown fields: {unknown}")
    defaults = SaturationSpec()
    parsed: dict[str, Any] = {}
    for name in allowed:
        item = value.get(name, getattr(defaults, name))
        item_path = f"{path}.{name}"
        if name in _INTEGER_FIELDS:
            parsed[name] = _integer(item, item_path)
        elif name in _NUMBER_FIELDS:
            parsed[name] = _number(item, item_path)
        elif name in _BOOLEAN_FIELDS:
            parsed[name] = _boolean(item, item_path)
        elif not isinstance(item, str):
            raise ValueError(f"{item_path} must be a string")
        else:
            parsed[name] = item
    result = SaturationSpec(**parsed)
    validate_saturation_spec(result, path=path)
    return result


def validate_saturation_spec(value: SaturationSpec, *, path: str) -> None:
    """Validate intrinsic search semantics independent of deployment shape."""
    if value.search_mode not in {"binary", "step-up"}:
        raise ValueError(f"{path}.search_mode must be 'binary' or 'step-up'")
    if value.initial_rate < 1:
        raise ValueError(f"{path}.initial_rate must be > 0")
    if value.max_rate < 0:
        raise ValueError(f"{path}.max_rate must be >= 0")
    if value.step_duration_s <= 0:
        raise ValueError(f"{path}.step_duration_s must be > 0")
    if value.warmup_duration_s < 0 or value.cooldown_pause_s < 0:
        raise ValueError(f"{path} warmup/cooldown durations must be non-negative")
    if not 0 < value.tolerance < 1:
        raise ValueError(f"{path}.tolerance must be in (0, 1)")
    if not 0 <= value.max_error_rate <= 1:
        raise ValueError(f"{path}.max_error_rate must be in [0, 1]")
    if not 0 < value.plateau_ratio <= 1:
        raise ValueError(f"{path}.plateau_ratio must be in (0, 1]")
    if value.max_p99_ttft < 0:
        raise ValueError(f"{path}.max_p99_ttft must be >= 0")
    if value.search_mode == "step-up" and value.enabled:
        if min(value.step_up_start, value.step_up_end, value.step_up_increment) < 1:
            raise ValueError(f"{path} step-up bounds and increment must all be positive")
        if value.step_up_end < value.step_up_start:
            raise ValueError(f"{path}.step_up_end must be >= {path}.step_up_start")


def validate_step_result(value: Any, *, path: str = "saturation step") -> dict[str, Any]:
    """Validate the exact Go/Python saturation step transport contract."""
    if (
        not isinstance(value, dict)
        or not _STEP_REQUIRED_FIELDS <= set(value)
        or not set(value) <= (_STEP_REQUIRED_FIELDS | _STEP_OPTIONAL_FIELDS)
    ):
        raise ValueError(f"{path} fields are invalid")
    for field in (
        "target_rate",
        "completed",
        "failed",
        "new_connections",
        "reused_connections",
        "max_observed_active",
    ):
        parsed = _integer(value[field], f"{path}.{field}")
        if parsed < 0:
            raise ValueError(f"{path}.{field} must be nonnegative")
    for field in (
        "achieved_rate",
        "duration_s",
        "error_rate",
        "p50_latency_s",
        "p99_latency_s",
        "mean_latency_s",
        "p50_ttft_s",
        "p99_ttft_s",
        "mean_ttft_s",
    ):
        if field in value:
            parsed = _number(value[field], f"{path}.{field}")
            if parsed < 0:
                raise ValueError(f"{path}.{field} must be nonnegative")
    if value["error_rate"] > 1:
        raise ValueError(f"{path}.error_rate must be in [0, 1]")
    _boolean(value["healthy"], f"{path}.healthy")
    if "fail_reasons" in value and (
        not isinstance(value["fail_reasons"], list)
        or any(not isinstance(reason, str) or not reason for reason in value["fail_reasons"])
    ):
        raise ValueError(f"{path}.fail_reasons must be a list of nonempty strings")
    total = value["completed"] + value["failed"]
    expected_error_rate = value["failed"] / total if total else 0.0
    if not math.isclose(value["error_rate"], expected_error_rate, rel_tol=1e-9, abs_tol=1e-12):
        raise ValueError(f"{path}.error_rate disagrees with completed/failed")
    return value


def validate_saturation_output(value: Any, *, path: str = "saturation output") -> dict[str, Any]:
    required = {"mode", "saturation_rate", "tolerance", "slo", "steps"}
    optional = {"verification_steps"}
    if (
        not isinstance(value, dict)
        or not required <= set(value)
        or not set(value) <= (required | optional)
    ):
        raise ValueError(f"{path} fields are invalid")
    if value["mode"] not in {"binary", "step-up"}:
        raise ValueError(f"{path}.mode is invalid")
    if _integer(value["saturation_rate"], f"{path}.saturation_rate") < 0:
        raise ValueError(f"{path}.saturation_rate must be nonnegative")
    tolerance = _number(value["tolerance"], f"{path}.tolerance")
    if not 0 < tolerance < 1:
        raise ValueError(f"{path}.tolerance must be in (0, 1)")
    slo = value["slo"]
    if not isinstance(slo, dict) or set(slo) != {"max_error_rate", "plateau_ratio"}:
        raise ValueError(f"{path}.slo fields are invalid")
    error_limit = _number(slo["max_error_rate"], f"{path}.slo.max_error_rate")
    plateau = _number(slo["plateau_ratio"], f"{path}.slo.plateau_ratio")
    if not 0 <= error_limit <= 1 or not 0 < plateau <= 1:
        raise ValueError(f"{path}.slo values are invalid")
    steps = value["steps"]
    if not isinstance(steps, list) or not steps:
        raise ValueError(f"{path}.steps must be a nonempty list")
    for index, step in enumerate(steps):
        validate_step_result(step, path=f"{path}.steps[{index}]")
    verification = value.get("verification_steps", [])
    if not isinstance(verification, list):
        raise ValueError(f"{path}.verification_steps must be a list")
    for index, step in enumerate(verification):
        validate_step_result(step, path=f"{path}.verification_steps[{index}]")
    return value
