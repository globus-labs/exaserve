"""Cause-preserving exception notes across the supported Python versions."""

from __future__ import annotations


def add_exception_note(error: BaseException, note: str) -> None:
    """Attach a secondary failure without masking the primary exception.

    ``BaseException.add_note`` was introduced in Python 3.11, while ExaServe's
    portable release contract still includes Python 3.10.  Storing the same
    ``__notes__`` attribute on 3.10 preserves the evidence for callers and
    tests even though that interpreter's default traceback renderer does not
    display it automatically.
    """
    text = str(note)
    native = getattr(error, "add_note", None)
    if callable(native):
        native(text)
        return
    existing = getattr(error, "__notes__", None)
    if existing is None:
        error.__notes__ = [text]
    elif isinstance(existing, list):
        existing.append(text)
    else:
        error.__notes__ = [str(existing), text]
