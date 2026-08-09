"""Regression tests for bounded, identity-fenced proxy qualification evidence."""

from pathlib import Path
import importlib.util
import os

import pytest


ROOT = Path(__file__).resolve().parents[1]
HARNESS_PATH = ROOT / "scripts" / "hardening" / "run_proxy_qualification.py"
EXPERIMENT_PATH = ROOT / "artifacts" / "hardening" / "final35-proxy-experiment-plan.json"
SPEC = importlib.util.spec_from_file_location("proxy_qualification", HARNESS_PATH)
assert SPEC is not None and SPEC.loader is not None
harness = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(harness)


def _requires_evidence(*paths: Path):
    return pytest.mark.skipif(
        any(not path.is_file() for path in paths),
        reason="hardware campaign evidence is retained outside clean source checkouts",
    )


@pytest.mark.parametrize(
    ("gate_id", "expected_no_delay", "arm"),
    (
        ("FQ-FINAL35-PROXY-NODELAY-ON-1N-20260809", True, "on"),
        ("FQ-FINAL35-PROXY-NODELAY-OFF-1N-20260809", False, "off"),
    ),
)
@_requires_evidence(EXPERIMENT_PATH)
def test_immutable_proxy_gate_declarations_are_exact(gate_id, expected_no_delay, arm):
    document, gate, paths = harness._load_gate(EXPERIMENT_PATH, gate_id)
    assert document["schema_version"] == 1
    assert gate["attempt"] == gate["attempt_limit"] == 1
    assert gate["logical_nodes"] == gate["physical_allocation_nodes"] == 1
    assert gate["http_no_delay"] is expected_no_delay
    assert paths["deployment_plan"].parent.name == arm
    result = harness._load_json(paths["output"] / "result.json")
    assert result["passed"] is True
    assert result["declared_gate"] == gate


def _proc_fixture(root: Path, *, pid: int = 42, start_ticks: int = 1234) -> Path:
    process = root / str(pid)
    (process / "fd").mkdir(parents=True)
    (process / "fd/1").touch()
    tail = ["0"] * 22
    tail[0] = "S"
    tail[11] = "20"
    tail[12] = "10"
    tail[17] = "4"
    tail[19] = str(start_ticks)
    tail[21] = "8"
    (process / "stat").write_text(f"{pid} (haproxy) {' '.join(tail)}\n", encoding="utf-8")
    (process / "status").write_text(f"Name:\thaproxy\nUid:\t{os.getuid()}\t0\t0\t0\n")
    net = root / "net"
    net.mkdir()
    (net / "snmp").write_text(
        "Tcp: ActiveOpens PassiveOpens CurrEstab RetransSegs\nTcp: 10 11 2 7\n",
        encoding="utf-8",
    )
    header = "  sl  local_address rem_address   st\n"
    (net / "tcp").write_text(
        header + "   0: 0100007F:0FA1 0100007F:D431 01\n",
        encoding="utf-8",
    )
    (net / "tcp6").write_text(header, encoding="utf-8")
    return root


def test_process_sample_binds_pid_start_cpu_tcp_and_connections(tmp_path):
    proc = _proc_fixture(tmp_path)
    sample = harness._process_sample(42, 1234, 4001, proc_root=proc)
    assert sample["pid"] == 42
    assert sample["process_start_ticks"] == 1234
    assert sample["process_state"] == "S"
    assert sample["cpu_ticks"] == 30
    assert sample["threads"] == 4
    assert sample["open_fds"] == 1
    assert sample["tcp"]["RetransSegs"] == 7
    assert sample["connections"]["ESTABLISHED"] == 1
    assert sample["connections"]["TOTAL"] == 1


def test_process_sample_rejects_pid_reuse(tmp_path):
    proc = _proc_fixture(tmp_path)
    with pytest.raises(RuntimeError, match="PID 42 was reused"):
        harness._process_sample(42, 9999, 4001, proc_root=proc)


def test_bounded_client_has_finite_limits_and_unique_request_ids(monkeypatch):
    def request(_endpoint, _model, request_id, _tokens, _timeout):
        return {
            "request_id": request_id,
            "status_code": 200,
            "response_bytes": 10,
            "duration_s": 0.001,
        }

    monkeypatch.setattr(harness, "_stream_request", request)
    result = harness._bounded_clients(
        "http://127.0.0.1:4001",
        "model",
        {
            "gate_id": "FQ-PROXY",
            "client_duration_s": 0.05,
            "client_workers": 2,
            "client_max_requests": 5,
            "max_tokens": 4,
            "request_timeout_s": 1.0,
        },
    )
    assert result["issued"] == result["completed"] == result["unique_request_ids"] == 5
    assert result["failed"] == 0
    assert result["worker_limit"] == 2
    assert result["request_limit"] == 5


def test_bounded_client_fails_closed_on_any_request_error(monkeypatch):
    monkeypatch.setattr(
        harness,
        "_stream_request",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("transport failed")),
    )
    with pytest.raises(RuntimeError, match="workload is incomplete"):
        harness._bounded_clients(
            "http://127.0.0.1:4001",
            "model",
            {
                "gate_id": "FQ-PROXY",
                "client_duration_s": 0.01,
                "client_workers": 1,
                "client_max_requests": 1,
                "max_tokens": 4,
                "request_timeout_s": 1.0,
            },
        )
