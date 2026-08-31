from pathlib import Path
import http.server
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request

import pytest

from exaserve.proxy.base import BackendEndpoint
from exaserve.proxy.haproxy_proxy import (
    HAProxyProxy,
    _safe_backend_name,
    required_nofile,
)


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
    assert "default_backend " + _safe_backend_name("meta-llama/Meta-Llama-3-8B-Instruct") in config
    # Intentional design since commit 3d130c8 (KNOWN_ISSUES shard-health
    # funnel fix): ONE proxy-liveness check against Serve's /-/healthz, never
    # per-model route checks that funnel through a single replica.
    assert "option httpchk GET /-/healthz" in config
    assert "GET /health " not in config


def test_gateway_config_is_create_once_with_exact_retry_only(tmp_path):
    proxy = HAProxyProxy()
    backends = [BackendEndpoint(host="node-a", port=8000, model_id="model")]
    path = proxy.generate_config(backends, output_dir=tmp_path, balance="leastconn")
    original = path.read_bytes()

    assert proxy.generate_config(backends, output_dir=tmp_path, balance="leastconn") == path
    with pytest.raises(FileExistsError, match="different content"):
        proxy.generate_config(backends, output_dir=tmp_path, balance="roundrobin")
    assert path.read_bytes() == original


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
    llama_backend = _safe_backend_name("meta-llama/Meta-Llama-3-8B-Instruct")
    assert f"acl is_{llama_backend} path /meta-llama--Meta-Llama-3-8B-Instruct" in config
    assert f"use_backend {llama_backend} if is_{llama_backend}" in config
    # Same intentional design as the single-model case: proxy-liveness
    # /-/healthz for every backend; per-model route health checks are the
    # 508-cascade defect the 3d130c8 fix removed.
    assert config.count("option httpchk GET /-/healthz") >= 2
    assert "/meta-llama--Meta-Llama-3-8B-Instruct/health" not in config
    assert "backend all_nodes" not in config
    # An unconditional http-request fallback is evaluated before use_backend
    # even if it appears later in the file. The fallback must be a backend.
    assert "default_backend exaserve_unknown_route" in config
    frontend = config.split("backend exaserve_unknown_route", 1)[0]
    assert "missing or unknown model route prefix" not in frontend
    assert frontend.count("bind *:4001") == 1


def test_haproxy_multimodel_inherited_socket_is_bound_exactly_once(tmp_path):
    config = _read_config(
        HAProxyProxy().generate_config(
            [
                BackendEndpoint("node-a", 8000, "org/model", "/org--model"),
                BackendEndpoint("node-b", 8000, "other/model", "/other--model"),
            ],
            output_dir=tmp_path,
            bind_target="fd@17",
            stats_port=0,
        )
    )
    assert config.count("bind fd@17") == 1


def test_haproxy_rewrites_canonical_multi_model_path_to_bound_replica_route(tmp_path):
    cfg = _read_config(
        HAProxyProxy().generate_config(
            [
                BackendEndpoint(
                    host="127.0.0.1",
                    port=8000,
                    model_id="org/model",
                    path_prefix="/org--model",
                    replica_routes=3,
                ),
                BackendEndpoint(
                    host="127.0.0.2",
                    port=8000,
                    model_id="other/model",
                    path_prefix="/other--model",
                ),
            ],
            output_dir=tmp_path,
            stats_port=0,
        )
    )
    assert "set-var(txn.ridx) rand(3)" in cfg
    assert (
        r"replace-path ^/org\-\-model(/.*)?$ "
        r"/org--model_r%[var(txn.ridx)]\1"
    ) in cfg


def test_haproxy_rewrites_to_node_grouped_null_routes(tmp_path):
    cfg = _read_config(
        HAProxyProxy().generate_config(
            [
                BackendEndpoint("node-a", 8000, "org/model", "/org--model", 2, "_g"),
                BackendEndpoint("node-b", 8000, "org/model", "/org--model", 2, "_g"),
            ],
            output_dir=tmp_path,
            stats_port=0,
        )
    )
    assert "set-var(txn.ridx) rand(2)" in cfg
    assert (
        r"replace-path ^/org\-\-model(/.*)?$ "
        r"/org--model_g%[var(txn.ridx)]\1"
    ) in cfg


def test_haproxy_rejects_mixed_route_suffixes(tmp_path):
    with pytest.raises(ValueError, match="suffixes"):
        HAProxyProxy().generate_config(
            [
                BackendEndpoint("node-a", 8000, "org/model", "/org--model", 2, "_r"),
                BackendEndpoint("node-b", 8000, "org/model", "/org--model", 2, "_g"),
            ],
            output_dir=tmp_path,
            stats_port=0,
        )


def test_haproxy_stats_admin_defaults_off_and_loopback(tmp_path):
    # PR-010: default stats page is read-only and loopback-bound. Options are
    # passed as **kwargs (see generate_config signature).
    proxy = HAProxyProxy()
    cfg = _read_config(
        proxy.generate_config(
            [BackendEndpoint(host="node-a", port=8000, model_id="test/model", path_prefix="")],
            output_dir=tmp_path,
            balance="leastconn",
            stats_port=9999,
        )
    )
    assert "bind 127.0.0.1:9999" in cfg
    assert "stats admin if TRUE" not in cfg


def test_haproxy_stats_admin_requires_auth(tmp_path):
    import pytest

    proxy = HAProxyProxy()
    with pytest.raises(ValueError, match="stats_auth"):
        proxy.generate_config(
            [BackendEndpoint(host="node-a", port=8000, model_id="test/model", path_prefix="")],
            output_dir=tmp_path,
            balance="leastconn",
            stats_port=9999,
            stats_admin=True,
        )
    cfg = _read_config(
        proxy.generate_config(
            [BackendEndpoint(host="node-a", port=8000, model_id="test/model", path_prefix="")],
            output_dir=tmp_path,
            balance="leastconn",
            stats_port=9999,
            stats_admin=True,
            stats_auth="admin:s3cret",
            stats_bind="0.0.0.0",
        )
    )
    assert "stats auth admin:s3cret" in cfg
    assert "stats admin if TRUE" in cfg


def test_haproxy_can_bind_an_inherited_socket_descriptor(tmp_path):
    cfg = _read_config(
        HAProxyProxy().generate_config(
            [BackendEndpoint(host="node-a", port=8000, model_id="test/model", path_prefix="")],
            output_dir=tmp_path,
            bind_target="fd@17",
            stats_port=0,
        )
    )
    assert cfg.count("bind fd@17") == 1
    assert "bind *:{PORT}" not in cfg


def test_haproxy_rejects_unbounded_request_framing_before_routing(tmp_path):
    cfg = _read_config(
        HAProxyProxy().generate_config(
            [BackendEndpoint(host="node-a", port=8000, model_id="test/model", path_prefix="")],
            output_dir=tmp_path,
            request_body_limit_bytes=4096,
            stats_port=0,
        )
    )
    chunked = cfg.index('string "Chunked request bodies are unsupported"')
    missing_length = cfg.index('string "Length Required"')
    oversized = cfg.index('string "Request body too large"')
    route = cfg.index("default_backend")
    assert chunked < missing_length < oversized < route
    assert "req.hdr_val(content-length) gt 4096" in cfg


def test_haproxy_backend_names_do_not_alias_distinct_model_ids():
    assert _safe_backend_name("a/b") != _safe_backend_name("a-b")
    assert len(_safe_backend_name("x" * 5000)) <= 64


def test_haproxy_default_descriptor_budget_fits_aurora_limit(tmp_path):
    cfg = _read_config(
        HAProxyProxy().generate_config(
            [BackendEndpoint("node-a", 8000, "org/model")],
            output_dir=tmp_path,
            stats_port=0,
        )
    )
    assert "maxconn 8000" in cfg
    assert required_nofile(8000, 64) <= 16384


def test_haproxy_rejects_inconsistent_replica_route_counts(tmp_path):
    with pytest.raises(ValueError, match="inconsistent replica-route"):
        HAProxyProxy().generate_config(
            [
                BackendEndpoint("node-a", 8000, "org/model", "/org--model", 2),
                BackendEndpoint("node-b", 8000, "org/model", "/org--model", 3),
            ],
            output_dir=tmp_path,
            stats_port=0,
        )


@pytest.mark.parametrize(
    ("route_suffix", "expected_path"),
    [
        ("_r", "/org--model_r0/v1/models"),
        ("_g", "/org--model_g0/v1/models"),
    ],
)
def test_haproxy_live_multimodel_routing_and_replica_rewrite(tmp_path, route_suffix, expected_path):
    executable = shutil.which("haproxy")
    if executable is None:
        pytest.skip("HAProxy executable is not installed")

    class EchoPath(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            # HAProxy intentionally closes a successful health-check response
            # after inspecting its status. An empty health body avoids noisy
            # ConnectionResetError traces from the test backend.
            body = b"" if self.path == "/-/healthz" else self.path.encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
            return

    servers = [http.server.ThreadingHTTPServer(("127.0.0.1", 0), EchoPath) for _ in range(2)]
    threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
    for thread in threads:
        thread.start()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        frontend_port = probe.getsockname()[1]
    config = HAProxyProxy().generate_config(
        [
            BackendEndpoint(
                "127.0.0.1",
                servers[0].server_port,
                "org/model",
                "/org--model",
                1,
                route_suffix,
            ),
            BackendEndpoint("127.0.0.1", servers[1].server_port, "other/model", "/other--model", 0),
        ],
        output_dir=tmp_path,
        listen_port=frontend_port,
        stats_port=0,
        check_interval=100,
        check_fall=1,
        check_rise=1,
    )
    process = subprocess.Popen(
        [executable, "-f", str(config), "-db"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=True,
    )
    try:
        base = f"http://127.0.0.1:{frontend_port}"
        # Compute-node environments define an outbound HTTP proxy but do not
        # necessarily list numeric loopback in no_proxy. This is an explicitly
        # local integration test, so never let ambient site policy reroute it.
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        deadline = time.monotonic() + 5.0
        model_path = None
        while time.monotonic() < deadline:
            if process.poll() is not None:
                output = process.stdout.read() if process.stdout is not None else ""
                pytest.fail(f"HAProxy exited during routing test: {output}")
            try:
                with opener.open(base + "/org--model/v1/models", timeout=0.5) as response:
                    model_path = response.read().decode()
                break
            except (OSError, urllib.error.URLError):
                time.sleep(0.05)
        assert model_path == expected_path
        with opener.open(base + "/other--model/v1/models", timeout=1.0) as response:
            assert response.read().decode() == "/other--model/v1/models"
        with pytest.raises(urllib.error.HTTPError) as excinfo:
            opener.open(base + "/unknown/v1/models", timeout=1.0)
        assert excinfo.value.code == 404
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
        if process.stdout is not None:
            process.stdout.close()
        for server in servers:
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join(timeout=2)
