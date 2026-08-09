"""Static contract checks for the repository-owned Pingora composition root.

The login environment need not contain a Rust toolchain, but the production
thread-count setting must still be guarded against the previously observed
failure mode where it was parsed, advertised, and ignored.
"""

from pathlib import Path


def _source() -> str:
    return (Path(__file__).parents[1] / "scripts/pingora_lb/src/main.rs").read_text()


def test_configured_worker_threads_reach_pingora_server_configuration():
    source = _source()
    assert "ServerConf {" in source
    assert "threads: worker_threads" in source
    assert "Server::new_with_opt_and_conf" in source


def test_zero_threads_resolves_to_available_parallelism():
    source = _source()
    assert "cfg.threads == 0" in source
    assert "std::thread::available_parallelism()" in source


def test_effective_thread_count_is_logged():
    source = _source()
    assert "worker_threads," in source
