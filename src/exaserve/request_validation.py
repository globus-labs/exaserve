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


class ContextLengthExceeded(RequestValidationError):
    """The rendered prompt and requested output cannot fit the model context."""

    error_type = "invalid_request_error"
    code = "context_length_exceeded"

    def __init__(
        self,
        *,
        prompt_tokens: int,
        requested_completion_tokens: int,
        max_model_len: int,
        prompt_param: str,
    ) -> None:
        self.prompt_tokens = prompt_tokens
        self.requested_completion_tokens = requested_completion_tokens
        self.max_model_len = max_model_len
        self.total_tokens = prompt_tokens + requested_completion_tokens
        self.param = prompt_param if prompt_tokens > max_model_len else "max_tokens"
        super().__init__(
            f"maximum context length is {max_model_len} tokens, but the rendered prompt uses "
            f"{prompt_tokens} tokens and requests {requested_completion_tokens} completion "
            f"tokens ({self.total_tokens} total)"
        )


def validate_context_window(
    *,
    prompt_tokens: int,
    requested_completion_tokens: int,
    max_model_len: int,
    prompt_param: str,
) -> None:
    """Reject an over-context request using trusted, exact tokenizer counts."""
    values = {
        "prompt_tokens": prompt_tokens,
        "requested_completion_tokens": requested_completion_tokens,
        "max_model_len": max_model_len,
    }
    for name, value in values.items():
        minimum = 0 if name == "prompt_tokens" else 1
        if type(value) is not int or value < minimum:
            raise RuntimeError(f"{name} must be an integer >= {minimum}, got {value!r}")
    if not isinstance(prompt_param, str) or not prompt_param:
        raise RuntimeError("prompt_param must be non-empty text")
    if prompt_tokens + requested_completion_tokens > max_model_len:
        raise ContextLengthExceeded(
            prompt_tokens=prompt_tokens,
            requested_completion_tokens=requested_completion_tokens,
            max_model_len=max_model_len,
            prompt_param=prompt_param,
        )


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


def strict_flag(body: dict, key: str, default: bool) -> bool:
    """IMP-H04: boolean protocol fields must not use truthiness — the string
    ``"false"`` previously became True for stream/ignore_eos/etc."""
    if key not in body or body[key] is None:
        return default
    value = body[key]
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        low = value.strip().lower()
        if low in ("true", "1", "yes"):
            return True
        if low in ("false", "0", "no"):
            return False
    raise RequestValidationError(f"{key} must be a boolean, got {value!r}")


def parse_sampling(body: dict) -> dict:
    """Validate + normalize OpenAI sampling params. Raises on bad input."""
    require_object_body(body)
    sampling: dict = {}
    if "temperature" in body:
        sampling["temperature"] = _req_float(body, "temperature", lo=0.0, hi=2.0)
    if "top_p" in body:
        sampling["top_p"] = _req_float(body, "top_p", lo=0.0, hi=1.0)
    if "max_tokens" in body:
        sampling["max_tokens"] = _req_int(body, "max_tokens", lo=1, hi=_MAX_TOKENS_CEILING)
    if "min_tokens" in body:
        sampling["min_tokens"] = _req_int(body, "min_tokens", lo=0, hi=_MAX_TOKENS_CEILING)
    if "min_tokens" in sampling and "max_tokens" in sampling:
        if sampling["min_tokens"] > sampling["max_tokens"]:
            raise RequestValidationError("min_tokens must not exceed max_tokens")
    if "stop" in body:
        stop = body["stop"]
        if not (
            stop is None
            or isinstance(stop, str)
            or (isinstance(stop, list) and all(isinstance(s, str) for s in stop))
        ):
            raise RequestValidationError("stop must be a string or list of strings")
        sampling["stop"] = stop
    if strict_flag(body, "ignore_eos", False):
        sampling["ignore_eos"] = True
    sampling.setdefault("temperature", 0.7)
    sampling.setdefault("max_tokens", 1024)
    return sampling


def require_object_body(body: Any) -> dict:
    """IMP-H04: the body must be a JSON object before any field access.

    A list body previously reached ``body.get`` and escaped as AttributeError
    (HTTP 500); callers must funnel every request through this first.
    """
    if not isinstance(body, dict):
        raise RequestValidationError(
            f"request body must be a JSON object, got {type(body).__name__}"
        )
    return body


def validate_model_field(body: dict, allowed: Iterable[str]) -> None:
    """PR-011: a model-specific deployment must not silently ignore a
    mismatched ``model`` field. Absent model is fine (this deployment's model
    is implied); a present-but-unknown model is a 400."""
    require_object_body(body)
    requested = body.get("model")
    if requested is None:
        return
    # IMP-H04: a non-string model (e.g. a list) previously raised TypeError
    # from the set membership test.
    if not isinstance(requested, str):
        raise RequestValidationError(f"model must be a string, got {type(requested).__name__}")
    allowed_set = set(allowed)
    if requested not in allowed_set:
        raise RequestValidationError(
            f"unknown model {requested!r}; this endpoint serves {sorted(allowed_set)}"
        )


def validate_messages(messages: Any) -> None:
    if not isinstance(messages, list) or not messages:
        raise RequestValidationError("messages must be a non-empty list")
    for i, m in enumerate(messages):
        if not isinstance(m, dict):
            raise RequestValidationError(f"messages[{i}] must be an object")
        role = m.get("role")
        if not isinstance(role, str) or not role.strip():
            raise RequestValidationError(f"messages[{i}].role must be a non-empty string")
        if "content" in m:
            content = m["content"]
            valid_content = content is None or isinstance(content, str)
            if isinstance(content, list):
                valid_content = all(
                    isinstance(part, dict)
                    and set(part) == {"type", "text"}
                    and part.get("type") == "text"
                    and isinstance(part.get("text"), str)
                    for part in content
                )
            if not valid_content:
                raise RequestValidationError(
                    f"messages[{i}].content must be text, text-part list, or null"
                )


def validate_chat_options(body: dict) -> None:
    """Validate optional tokenizer inputs before ``**kwargs`` expansion."""
    require_object_body(body)
    template = body.get("chat_template")
    if template is not None and not isinstance(template, str):
        raise RequestValidationError("chat_template must be a string or null")
    kwargs = body.get("chat_template_kwargs")
    if kwargs is not None and not isinstance(kwargs, dict):
        raise RequestValidationError("chat_template_kwargs must be an object or null")
    if isinstance(kwargs, dict) and not all(isinstance(key, str) for key in kwargs):
        raise RequestValidationError("chat_template_kwargs keys must be strings")


def validate_prompt(prompt: Any) -> str:
    """This single-choice endpoint supports exactly one text prompt.

    OpenAI's wider completions schema also permits batch/token-id forms, but
    ExaServe does not emit batch choices. Accepting a list here previously
    deferred the mismatch until an engine-specific AttributeError or malformed
    result, so unsupported forms now fail as a typed client error.
    """
    if not isinstance(prompt, str):
        raise RequestValidationError("prompt must be a string")
    return prompt
