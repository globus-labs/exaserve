"""PR-010: admin and status surfaces are not open by default."""

from __future__ import annotations

import inspect

import pytest

from exaserve.proxy.base import BackendEndpoint


def _backends():
    return [BackendEndpoint(model_id="m", host="10.0.0.1", port=8000)]


def _nginx_conf(tmp_path, **options):
    from exaserve.proxy.nginx_proxy import NGINXProxy

    path = NGINXProxy().generate_config(_backends(), tmp_path, **options)
    return open(path).read()


def test_nginx_status_is_loopback_only_by_default(tmp_path):
    """`allow all` published request counters to anything reachable."""
    conf = _nginx_conf(tmp_path)
    assert "stub_status" in conf
    assert "allow 127.0.0.1;" in conf and "deny all;" in conf
    assert "allow all;" not in conf


def test_nginx_status_can_be_widened_explicitly(tmp_path):
    conf = _nginx_conf(tmp_path, status_allow_from=["10.0.0.0/8"])
    assert "allow 10.0.0.0/8;" in conf and "deny all;" in conf


def test_publishing_the_status_page_requires_saying_all(tmp_path):
    conf = _nginx_conf(tmp_path, status_allow_from="all")
    assert "allow all;" in conf


def test_an_empty_allow_list_is_refused(tmp_path):
    with pytest.raises(ValueError, match="non-empty"):
        _nginx_conf(tmp_path, status_allow_from=[])


def test_envoy_admin_is_loopback_only(tmp_path):
    """Envoy's admin interface is a full control surface."""
    import json

    from exaserve.proxy.envoy_proxy import EnvoyProxy

    path = EnvoyProxy().generate_config(_backends(), tmp_path)
    text = open(path).read()
    try:
        config = json.loads(text)
    except ValueError:
        import yaml

        config = yaml.safe_load(text)
    admin = config.get("admin")
    if admin:
        address = admin["address"]["socket_address"]["address"]
        assert address == "127.0.0.1", f"envoy admin bound to {address}"


def test_haproxy_admin_requires_auth():
    """`stats admin if TRUE` on a wildcard bind let anyone drain a backend."""
    from exaserve.proxy.haproxy_proxy import HAProxyProxy

    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(ValueError, match="stats_auth"):
            HAProxyProxy().generate_config(_backends(), Path(tmp), stats_admin=True)


@pytest.mark.parametrize(
    "module_name,class_name",
    [
        ("haproxy_proxy", "HAProxyProxy"),
        ("litellm_proxy", "LiteLLMProxy"),
        ("nginx_proxy", "NGINXProxy"),
        ("envoy_proxy", "EnvoyProxy"),
        ("pingora_proxy", "PingoraProxy"),
    ],
)
def test_proxy_backends_are_pure_strict_renderers(tmp_path, module_name, class_name):
    module = __import__(f"exaserve.proxy.{module_name}", fromlist=[class_name])
    backend_type = getattr(module, class_name)
    source = inspect.getsource(module)
    assert "subprocess" not in source and "Popen" not in source
    assert not any(hasattr(backend_type, name) for name in ("start", "stop", "health_check"))
    with pytest.raises(ValueError, match="unknown fields"):
        backend_type().generate_config(_backends(), tmp_path / class_name, invented_option=True)
    with pytest.raises(ValueError, match="at least one backend"):
        backend_type().generate_config([], tmp_path / f"empty-{class_name}")
