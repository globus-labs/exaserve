from __future__ import annotations

from exaserve.exception_notes import add_exception_note


class _Python310StyleError(RuntimeError):
    add_note = None


def test_exception_notes_fall_back_for_python_310_style_exceptions():
    error = _Python310StyleError("primary")

    add_exception_note(error, "secondary one")
    add_exception_note(error, "secondary two")

    assert error.__notes__ == ["secondary one", "secondary two"]
