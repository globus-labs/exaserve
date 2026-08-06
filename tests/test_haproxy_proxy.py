import os
import sys
from pathlib import Path

SRC_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

from exaserve.proxy.base import BackendEndpoint
from exaserve.proxy.haproxy_proxy import HAProxyProxy


def _read_config(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_haproxy_single_model_uses_root_health_check(tmp_path):
    proxy = HAProxyProxy()
    config_path = proxy.generate_config(
        [
            BackendEndpoint(
                host="node-a",
                port=8000,
                model_id="meta-llama/Meta-Llama-3-8B-Instruct",
                path_prefix="",
            )
        ],
        output_dir=tmp_path,
        balance="leastconn",
    )

    config = _read_config(config_path)
    assert "default_backend meta_llama_Meta_Llama_3_8B_Instruct" in config
    # Intentional design since commit 3d130c8 (KNOWN_ISSUES shard-health
    # funnel fix): ONE proxy-liveness check against Serve's /-/healthz, never
    # per-model route checks that funnel through a single replica.
    assert "option httpchk GET /-/healthz" in config
    assert "GET /health " not in config


def test_haproxy_multi_model_routes_by_path_prefix(tmp_path):
    proxy = HAProxyProxy()
    config_path = proxy.generate_config(
        [
            BackendEndpoint(
                host="node-a",
                port=8000,
                model_id="meta-llama/Meta-Llama-3-8B-Instruct",
                path_prefix="/meta-llama--Meta-Llama-3-8B-Instruct",
            ),
            BackendEndpoint(
                host="node-b",
                port=8000,
                model_id="mistralai/Mistral-7B-Instruct-v0.3",
                path_prefix="/mistralai--Mistral-7B-Instruct-v0-3",
            ),
        ],
        output_dir=tmp_path,
        balance="leastconn",
    )

    config = _read_config(config_path)
    assert (
        "acl is_meta_llama_Meta_Llama_3_8B_Instruct path_beg "
        "/meta-llama--Meta-Llama-3-8B-Instruct "
        "/meta-llama--Meta-Llama-3-8B-Instruct/"
    ) in config
    assert (
        "use_backend meta_llama_Meta_Llama_3_8B_Instruct "
        "if is_meta_llama_Meta_Llama_3_8B_Instruct"
    ) in config
    # Same intentional design as the single-model case: proxy-liveness
    # /-/healthz for every backend; per-model route health checks are the
    # 508-cascade defect the 3d130c8 fix removed.
    assert config.count("option httpchk GET /-/healthz") >= 2
    assert "/meta-llama--Meta-Llama-3-8B-Instruct/health" not in config
    assert "backend all_nodes" not in config


def test_haproxy_stats_admin_defaults_off_and_loopback(tmp_path):
    # PR-010: default stats page is read-only and loopback-bound. Options are
    # passed as **kwargs (see generate_config signature).
    proxy = HAProxyProxy()
    cfg = _read_config(proxy.generate_config(
        [BackendEndpoint(host="node-a", port=8000,
                         model_id="test/model", path_prefix="")],
        output_dir=tmp_path, balance="leastconn", stats_port=9999))
    assert "bind 127.0.0.1:9999" in cfg
    assert "stats admin if TRUE" not in cfg


def test_haproxy_stats_admin_requires_auth(tmp_path):
    import pytest
    proxy = HAProxyProxy()
    with pytest.raises(ValueError, match="stats_auth"):
        proxy.generate_config(
            [BackendEndpoint(host="node-a", port=8000,
                             model_id="test/model", path_prefix="")],
            output_dir=tmp_path, balance="leastconn",
            stats_port=9999, stats_admin=True)
    cfg = _read_config(proxy.generate_config(
        [BackendEndpoint(host="node-a", port=8000,
                         model_id="test/model", path_prefix="")],
        output_dir=tmp_path, balance="leastconn",
        stats_port=9999, stats_admin=True,
        stats_auth="admin:s3cret", stats_bind="0.0.0.0"))
    assert "stats auth admin:s3cret" in cfg
    assert "stats admin if TRUE" in cfg
