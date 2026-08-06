"""PR-026 / TD-SITECUST: private-API use is declared and capability-checked."""

from __future__ import annotations

import pytest

from exaserve.compat import private_api


def test_the_running_stack_provides_every_required_capability():
    """Drift must fail at bring-up, so this is the canary for it.

    Needs a real Ray: the hermetic CI lane has none, and "Ray is absent" is
    not the drift this is watching for.
    """
    pytest.importorskip("ray", exc_type=ImportError)
    results = private_api.verify(strict=True)
    assert results, "no private surface declared"
    for symbol in private_api.PRIVATE_SURFACE:
        if symbol.required:
            assert results[symbol.capability], (
                f"{symbol.capability} missing: {symbol.module}.{symbol.attr}")


def test_a_missing_required_symbol_fails_by_capability_name(monkeypatch):
    broken = private_api.PrivateSymbol(
        "made_up_capability", "ray.serve._private.constants", "NO_SUCH_ATTR",
        "test", required=True)
    monkeypatch.setattr(private_api, "PRIVATE_SURFACE",
                        private_api.PRIVATE_SURFACE + (broken,))
    with pytest.raises(private_api.PrivateApiUnavailable) as excinfo:
        private_api.verify(strict=True)
    assert "made_up_capability" in str(excinfo.value)
    assert "NO_SUCH_ATTR" in str(excinfo.value)


def test_an_optional_symbol_degrades_instead_of_failing(monkeypatch):
    optional = private_api.PrivateSymbol(
        "optional_thing", "ray.serve._private.constants", "NO_SUCH_ATTR",
        "test", required=False)
    pytest.importorskip("ray", exc_type=ImportError)
    monkeypatch.setattr(private_api, "PRIVATE_SURFACE",
                        private_api.PRIVATE_SURFACE + (optional,))
    results = private_api.verify(strict=True)
    assert results["optional_thing"] is False


def test_require_explains_why_the_private_symbol_is_needed():
    with pytest.raises(private_api.PrivateApiUnavailable, match="undeclared"):
        private_api.require("not_a_capability")
    # A declared one resolves on this stack.
    pytest.importorskip("ray", exc_type=ImportError)
    assert private_api.require("serve_default_app_name") is not None


def test_every_declared_symbol_states_a_reason():
    """A private dependency with no justification is one nobody reviewed."""
    for symbol in private_api.PRIVATE_SURFACE:
        assert symbol.why and len(symbol.why) > 20, f"{symbol.capability} has no rationale"


def test_production_private_imports_are_all_declared():
    """Adding a private import without declaring it is the drift this catches."""
    import re
    from importlib import resources

    declared_modules = {s.module for s in private_api.PRIVATE_SURFACE}
    # `from ray.serve._private import constants` imports the SUBMODULE, so the
    # declared name is parent + "." + imported name; check both spellings.
    pattern = re.compile(
        r"from (ray\.[A-Za-z0-9_.]*_private[A-Za-z0-9_.]*) import ([A-Za-z0-9_, ]+)")
    root = resources.files("exaserve")
    undeclared = {}
    for name in ("server.py", "control/serve_readiness.py", "ray_start.py",
                 "driver.py", "supervisor_main.py"):
        try:
            text = (root / name).read_text()
        except (FileNotFoundError, OSError):
            continue
        for module, imported in pattern.findall(text):
            names = [n.strip().split(" as ")[0].strip() for n in imported.split(",")]
            for imported_name in names:
                if module in declared_modules:
                    continue
                if f"{module}.{imported_name}" in declared_modules:
                    continue
                undeclared.setdefault(name, set()).add(f"{module}.{imported_name}")
    assert not undeclared, (
        f"private imports not declared in private_api.PRIVATE_SURFACE: {undeclared}")
