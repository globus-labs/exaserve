import os
import sys
from pathlib import Path

SRC_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "src"))
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

from aurora_rayserver.proxy.base import BackendEndpoint
from aurora_rayserver.proxy.haproxy_proxy import HAProxyProxy


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
    assert "option httpchk GET /health" in config


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
    assert "option httpchk GET /meta-llama--Meta-Llama-3-8B-Instruct/health" in config
    assert "option httpchk GET /mistralai--Mistral-7B-Instruct-v0-3/health" in config
    assert "backend all_nodes" not in config
