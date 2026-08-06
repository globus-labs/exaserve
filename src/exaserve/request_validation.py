"""OpenAI request validation (PR-011), ray-free so it is unit-testable.

The serving handlers previously called ``float()``/``int()`` on raw body
values with no bounds and ignored the ``model`` field, so malformed input
became opaque 500s. These helpers validate types and ranges and raise a
typed error the handler maps to a 400.
"""

from __future__ import annotations

from typing import Any, Iterable

# vLLM/most engines cap sampled tokens well under this; a request asking for
# more is a client error, not a 500 waiting to happen.
_MAX_TOKENS_CEILING = 128_000


class RequestValidationError(ValueError):
    """A request body failed validation; the handler returns HTTP 400."""


def _req_float(body: dict, key: str, *, lo: float, hi: float) -> float:
    value = body[key]
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RequestValidationError(f"{key} must be a number, got {value!r}")
    value = float(value)
    if not (lo <= value <= hi):
        raise RequestValidationError(f"{key} must be in [{lo}, {hi}], got {value}")
    return value


def _req_int(body: dict, key: str, *, lo: int, hi: int) -> int:
    value = body[key]
    if isinstance(value, bool) or not isinstance(value, int):
        raise RequestValidationError(f"{key} must be an integer, got {value!r}")
    if not (lo <= value <= hi):
        raise RequestValidationError(f"{key} must be in [{lo}, {hi}], got {value}")
    return value


def parse_sampling(body: dict) -> dict:
    """Validate + normalize OpenAI sampling params. Raises on bad input."""
    if not isinstance(body, dict):
        raise RequestValidationError("request body must be a JSON object")
    sampling: dict = {}
    if "temperature" in body:
        sampling["temperature"] = _req_float(body, "temperature", lo=0.0, hi=2.0)
    if "top_p" in body:
        sampling["top_p"] = _req_float(body, "top_p", lo=0.0, hi=1.0)
    if "max_tokens" in body:
        sampling["max_tokens"] = _req_int(body, "max_tokens", lo=1,
                                          hi=_MAX_TOKENS_CEILING)
    if "min_tokens" in body:
        sampling["min_tokens"] = _req_int(body, "min_tokens", lo=0,
                                          hi=_MAX_TOKENS_CEILING)
    if "min_tokens" in sampling and "max_tokens" in sampling:
        if sampling["min_tokens"] > sampling["max_tokens"]:
            raise RequestValidationError("min_tokens must not exceed max_tokens")
    if "stop" in body:
        stop = body["stop"]
        if not (stop is None or isinstance(stop, str)
                or (isinstance(stop, list) and all(isinstance(s, str) for s in stop))):
            raise RequestValidationError("stop must be a string or list of strings")
        sampling["stop"] = stop
    if body.get("ignore_eos"):
        sampling["ignore_eos"] = True
    sampling.setdefault("temperature", 0.7)
    sampling.setdefault("max_tokens", 1024)
    return sampling


def validate_model_field(body: dict, allowed: Iterable[str]) -> None:
    """PR-011: a model-specific deployment must not silently ignore a
    mismatched ``model`` field. Absent model is fine (this deployment's model
    is implied); a present-but-unknown model is a 400."""
    requested = body.get("model")
    if requested is None:
        return
    allowed_set = set(allowed)
    if requested not in allowed_set:
        raise RequestValidationError(
            f"unknown model {requested!r}; this endpoint serves {sorted(allowed_set)}")


def validate_messages(messages: Any) -> None:
    if not isinstance(messages, list) or not messages:
        raise RequestValidationError("messages must be a non-empty list")
    for i, m in enumerate(messages):
        if not (isinstance(m, dict) and "role" in m):
            raise RequestValidationError(f"messages[{i}] must be an object with a role")
