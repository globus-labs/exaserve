"""Bounded, terminal-safe diagnostic text shared across runtime layers."""

from __future__ import annotations

import re

DEFAULT_DIAGNOSTIC_TEXT_LIMIT = 320

_SENSITIVE_DIAGNOSTIC_PATTERN = re.compile(
    r"(?i)(?:password|passwd|secret|credential|authorization|bearer|"
    r"api[\s_-]?key|master[\s_-]?key|private[\s_-]?key|access[\s_-]?key|"
    r"(?:^|[^A-Z0-9])(?:access[\s_-]?token|refresh[\s_-]?token|token)"
    r"(?=$|[^A-Z0-9]))"
)
_ANSI_ESCAPE_PATTERN = re.compile(
    r"(?:\x1B\[[0-?]*[ -/]*[@-~]|\x9B[0-?]*[ -/]*[@-~]|"
    r"\x1B\][^\x07]*(?:\x07|\x1B\\))"
)


def bounded_diagnostic_text(
    value: object,
    *,
    limit: int = DEFAULT_DIAGNOSTIC_TEXT_LIMIT,
    preserve_tail: bool = False,
) -> str:
    """Return normalized, secret-safe text within one exact character bound.

    Sensitive-label detection runs over the complete normalized input before
    truncation, so a credential placed in the discarded middle cannot leak
    through a retained head or causal tail.
    """

    if type(limit) is not int or limit < 32:
        raise ValueError("diagnostic text limit must be an integer of at least 32")
    if type(preserve_tail) is not bool:
        raise TypeError("preserve_tail must be a boolean")
    if not isinstance(value, str):
        return f"<{type(value).__name__}>"[:limit]
    without_ansi = _ANSI_ESCAPE_PATTERN.sub("", value)
    text = " ".join("".join(char if char.isprintable() else " " for char in without_ansi).split())
    if _SENSITIVE_DIAGNOSTIC_PATTERN.search(text):
        text = "<redacted sensitive detail>"
    if not text:
        text = "<no detail>"
    if len(text) <= limit:
        return text
    if not preserve_tail:
        marker = "...[truncated]"
        return text[: limit - len(marker)] + marker
    marker = "...[middle truncated]..."
    head_size = min(160, (limit - len(marker)) // 3)
    tail_size = limit - len(marker) - head_size
    return text[:head_size] + marker + text[-tail_size:]


__all__ = ["DEFAULT_DIAGNOSTIC_TEXT_LIMIT", "bounded_diagnostic_text"]
