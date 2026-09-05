import io
import json
from pathlib import Path
import shutil
import socket
import subprocess

import pytest

from clientlab.runner.runtime import run_study
from exaserve.state.results import load_result_manifest


def _pbs_synthetic_config(*, client_nodes=1, synthetic_nodes=2):
    return {
        "execution": {"mode": "pbs_interactive", "client_nodes": client_nodes},
        "target": {
            "type": "synthetic",
            "host": "127.0.0.1",
            "port": 18100,
            "synthetic_nodes": synthetic_nodes,
            "response_tokens": 8,
        },
        "client": {"model": "stub", "prompt_words": 4, "max_active_requests": 8},
        "faults": {"service_time": {"distribution": "fixed", "value_ms": 1.0}},
    }


def test_pbs_synthetic_targets_use_pals_transfer_and_inline_config(tmp_path, monkeypatch):
    from clientlab.runner import runtime

    class Supervisor:
        def __init__(self, *args, **kwargs):
            self.component = None

        def register(self, component):
            self.component = component
            return component

        def start_all(self, **_kwargs):
            return None

        def shutdown(self, **_kwargs):
            return True

    supervisors = []

    def supervisor_factory(*args, **kwargs):
        result = Supervisor(*args, **kwargs)
        supervisors.append(result)
        return result

    monkeypatch.setattr(runtime, "RuntimeSupervisor", supervisor_factory)
    monkeypatch.setattr(runtime, "validate_pbs_session", lambda: ["client", "target0", "target1"])
    monkeypatch.setattr(runtime, "resolve_hsn_host", lambda node: node)
    monkeypatch.setattr(runtime, "wait_for_health", lambda *_a, **_k: None)
    monkeypatch.setattr(
        runtime, "ensure_cpp_server", lambda *, head_local=False: "/tmp/local/synthetic_server"
    )

    config = _pbs_synthetic_config()
    config["target"]["trace_path"] = "/home/user/must-not-reach-worker"
    config["target"]["deployment_status_dir"] = "/lus/flare/must-not-reach-worker"
    handles = runtime.launch_pbs_synthetic_targets(config, tmp_path)
    component = supervisors[0].component
    argv = list(component.argv)
    assert argv[:4] == ["mpiexec", "--transfer", "--genvnone", "--envnone"]
    assert "--wdir" in argv and argv[argv.index("--wdir") + 1] == "/tmp"
    assert "/tmp/local/synthetic_server" in argv
    assert "clientlab.targets.synthetic_target" not in argv
    inline = argv[argv.index("--config-json") + 1]
    assert json.loads(inline)["target"]["port"] == 18100
    assert "/home/" not in inline and "/lus/flare/" not in inline
    assert set(json.loads(inline)["target"]) == {"host", "port", "response_tokens"}
    assert "--rank-port-offset" in argv
    assert "--require-aurora-local-runtime" in argv
    assert str(tmp_path) not in "\0".join(argv)
    hostfile = Path(argv[argv.index("--hostfile") + 1])
    assert hostfile.parent == Path("/tmp") and hostfile.is_file()
    assert component.cwd == "/tmp"
    assert component.env["PYTHONNOUSERSITE"] == "1"
    exported = [argv[index + 1] for index, item in enumerate(argv[:-1]) if item == "--genv"]
    assert "PMIX_MCA_mca_base_param_files=/etc/pmix-mca-params.conf" in exported
    assert "PMIX_MCA_mca_base_component_path=/usr/lib64/pmix" in exported
    assert not any(item.startswith(("PMIX_RANK=", "PALS_RANKID=")) for item in exported)

    source = (
        Path(__file__).resolve().parents[1] / "targets" / "cpp_server" / "main.cpp"
    ).read_text()
    assert 'statfs("/tmp", &filesystem)' in source
    assert 'root_owned_site_path("/etc/pmix-mca-params.conf", false)' in source
    assert 'root_owned_site_path("/usr/lib64/pmix", true)' in source

    runtime.stop_targets(handles)
    assert not hostfile.exists()


def test_pbs_clientlab_rejects_unsupported_distributed_clients(tmp_path, monkeypatch):
    from clientlab.runner import runtime

    monkeypatch.setattr(runtime, "validate_pbs_session", lambda: ["client0", "client1", "target0"])
    monkeypatch.setattr(
        runtime,
        "ensure_cpp_server",
        lambda **_kwargs: (_ for _ in ()).throw(AssertionError("must fail before staging")),
    )
    with pytest.raises(RuntimeError, match="unsupported feature.*exactly one client node"):
        runtime.launch_pbs_synthetic_targets(
            _pbs_synthetic_config(client_nodes=2, synthetic_nodes=1), tmp_path
        )


def test_cpp_synthetic_server_reports_bind_failure_with_nonzero_exit():
    from clientlab.runner import runtime

    if shutil.which("g++") is None:
        pytest.skip("C++ compiler is unavailable")
    binary = runtime.ensure_cpp_server(head_local=True)
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = listener.getsockname()[1]
        payload = json.dumps(
            {
                "target": {"host": "127.0.0.1", "port": port, "response_tokens": 1},
                "client": {"model": "stub", "prompt_words": 1},
                "faults": {},
            },
            separators=(",", ":"),
        )
        completed = subprocess.run(
            [binary, "--config-json", payload],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=5.0,
            check=False,
        )
    assert completed.returncode != 0
    assert "bind" in completed.stderr.lower()


def test_synthetic_target_inline_mode_execs_without_config_file(tmp_path, monkeypatch):
    from clientlab.targets import synthetic_target

    binary = tmp_path / "server"
    binary.write_text("#!/bin/sh\n", encoding="utf-8")
    binary.chmod(0o700)

    class Listener:
        def fileno(self):
            return 17

    monkeypatch.setattr("exaserve.state.ports.bind_listener", lambda *_a, **_k: Listener())
    monkeypatch.setattr(
        "exaserve.state.atomic.strict_json_load_path",
        lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("opened a config file")),
    )
    executed = {}

    def execve(path, argv, env):
        executed.update(path=path, argv=argv, env=env)
        raise RuntimeError("exec intercepted")

    monkeypatch.setattr(synthetic_target.os, "execve", execve)
    payload = json.dumps({"target": {"host": "127.0.0.1", "port": 18100}})
    with pytest.raises(RuntimeError, match="exec intercepted"):
        synthetic_target.main(["--config-json", payload, "--binary", str(binary)])
    assert executed["path"] == str(binary)
    assert executed["argv"][:2] == [str(binary), "--config-json"]
    assert json.loads(executed["argv"][2]) == json.loads(payload)
    assert executed["env"]["CLIENTLAB_LISTEN_FD"] == "17"
    assert executed["env"]["PYTHONNOUSERSITE"] == "1"


def test_client_cleanup_shares_one_deadline_across_every_process(monkeypatch):
    from clientlab.runner import runtime

    deadlines = []

    def terminate(_process, *, grace_s=5.0, deadline=None):
        deadlines.append(deadline)

    class Drain:
        def __init__(self):
            self.timeouts = []

        def join(self, timeout):
            self.timeouts.append(timeout)

        def is_alive(self):
            return False

    drains = [Drain(), Drain()]
    monkeypatch.setattr(runtime, "_terminate_exact_process", terminate)
    logs = [io.StringIO(), io.StringIO()]
    runtime._finish_client_processes(
        [
            {"process": object(), "drains": (drains[0],)},
            {"process": object(), "drains": (drains[1],)},
        ],
        *logs,
        cleanup_s=2.0,
    )
    assert len(deadlines) == 2 and deadlines[0] == deadlines[1]
    assert all(drain.timeouts and 0 <= drain.timeouts[0] <= 2.0 for drain in drains)
    assert all(log.closed for log in logs)


def test_saturation_ceiling_probe_is_bounded(tmp_path, monkeypatch):
    from clientlab.runner import runtime

    monkeypatch.setattr(
        runtime,
        "_run_multi_step",
        lambda *_args, **_kwargs: {"achieved_rate": 1.0, "errors": 0},
    )
    monkeypatch.setattr(runtime, "_evaluate_merged_health", lambda *_args: True)
    config = {"client": {"saturation": {"search_mode": "binary", "initial_rate": 1}}}

    with pytest.raises(RuntimeError, match="bounded attempts"):
        runtime._run_saturation_multi("go", config, tmp_path, ["http://target"], {}, 1)


def test_runtime_aggregations_reject_coercible_counts_and_histogram_drift():
    from clientlab.runner import runtime

    with pytest.raises(ValueError, match="completed"):
        runtime._merge_step_results(
            [
                {
                    "target_rate": 1,
                    "achieved_rate": 1.0,
                    "completed": "1",
                    "failed": 0,
                    "error_rate": 0.0,
                    "duration_s": 1.0,
                    "p50_latency_s": 0.1,
                    "p99_latency_s": 0.2,
                    "mean_latency_s": 0.1,
                    "latency_histogram": {
                        "bucket_upper_bounds_s": [0.1, 1.0, -1.0],
                        "counts": [1, 0, 0],
                        "count": 1,
                        "sum_s": 0.1,
                    },
                    "new_connections": 1,
                    "reused_connections": 0,
                    "max_observed_active": 1,
                    "healthy": True,
                }
            ],
            1.0,
        )

    summary = {
        "requests_completed": True,
        "requests_scheduled": 1,
        "errors": 0,
        "p50_s": 0.1,
        "p99_s": 0.2,
        "total_input_tokens": 1,
        "total_output_tokens": 1,
    }
    with pytest.raises(RuntimeError, match="requests_completed"):
        runtime.merge_summary_results([summary])


def test_clientlab_merges_summary_and_saturation_histograms_not_quantile_maxima():
    from clientlab.runner import runtime

    def histogram(counts, total):
        return {
            "bucket_upper_bounds_s": [0.1, 1.0, -1.0],
            "counts": counts,
            "count": sum(counts),
            "sum_s": total,
        }

    summaries = [
        {
            "requests_completed": 1,
            "requests_scheduled": 1,
            "errors": 0,
            "p50_s": quantile,
            "p99_s": quantile,
            "latency_quantile_method": "mergeable_histogram_estimate_2pct_through_7200s",
            "total_input_tokens": 1,
            "total_output_tokens": 1,
            "latency_histogram": latency,
        }
        for quantile, latency in (
            (0.1, histogram([1, 0, 0], 0.1)),
            (0.9, histogram([0, 1, 0], 0.9)),
        )
    ]
    merged = runtime.merge_summary_results(summaries)
    assert merged["p50_s"] == pytest.approx(0.1)
    assert merged["p99_s"] == pytest.approx(1.0)
    assert merged["latency_quantile_method"] == "mergeable_histogram_estimate_2pct_through_7200s"

    steps = []
    for latency in (histogram([1, 0, 0], 0.1), histogram([0, 1, 0], 0.9)):
        steps.append(
            {
                "target_rate": 1,
                "achieved_rate": 1.0,
                "completed": 1,
                "failed": 0,
                "error_rate": 0.0,
                "duration_s": 1.0,
                "p50_latency_s": 0.9,
                "p99_latency_s": 0.9,
                "mean_latency_s": 0.5,
                "latency_histogram": latency,
                "new_connections": 1,
                "reused_connections": 0,
                "max_observed_active": 1,
                "healthy": True,
            }
        )
    merged_step = runtime._merge_step_results(steps, 2)
    assert merged_step["p50_latency_s"] == pytest.approx(0.1)
    assert merged_step["p99_latency_s"] == pytest.approx(1.0)
    assert merged_step["mean_latency_s"] == pytest.approx(0.5)
    assert runtime._percentile_from_histogram(
        {
            "bucket_upper_bounds_s": [60.0, 7200.0, -1.0],
            "counts": [98, 0, 2],
            "count": 100,
            "sum_s": 6100.0,
        },
        0.99,
    ) == pytest.approx(7200.0)

    left = {
        "latency": {
            "count": 1,
            "sum_s": 0.1,
            "counts": [1, 0],
            "bucket_upper_bounds_s": [0.1, -1.0],
        }
    }
    right = {
        "latency": {
            "count": 1,
            "sum_s": 0.2,
            "counts": [1, 0, 0],
            "bucket_upper_bounds_s": [0.1, 1.0, -1.0],
        }
    }
    with pytest.raises(RuntimeError, match="layouts disagree"):
        runtime.merge_histograms(left, right)


def test_client_result_shard_missing_terminal_is_not_zero_request_success(tmp_path):
    from clientlab.runner import runtime

    path = tmp_path / "result.jsonl"
    path.write_text("", encoding="utf-8")
    with pytest.raises(RuntimeError, match="missing its terminal row"):
        runtime.load_summary_result(path, expected_request_ids=("request-0",), sum_only=True)


def test_netstats_summary_rejects_coercible_counters(tmp_path):
    from clientlab.runner import runtime

    path = tmp_path / "netstats.jsonl"
    path.write_text(
        json.dumps(
            {
                "timestamp": 1.0,
                "hostname": "node",
                "interface": "hsn0",
                "rx_bytes": 1,
                "rx_packets": 1,
                "rx_errors": 0,
                "rx_drops": "0",
                "tx_bytes": 1,
                "tx_packets": 1,
                "tx_errors": 0,
                "tx_drops": 0,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="rx_drops"):
        runtime.summarize_netstats(path)


def test_run_study_local_smoke(tmp_path: Path):
    spec_path = tmp_path / "smoke.json"
    spec_path.write_text(
        json.dumps(
            {
                "study": {"name": "smoke-study", "suite": "client_microbench", "repeats": 1},
                "client": {
                    "duration_s": 1.0,
                    "rate": 5.0,
                    "prompt_words": 8,
                    "output_tokens": 4,
                    "max_active_requests": 2,
                    "queue_capacity": 0,
                    "num_go_workers": 1,
                    "num_go_procs": 1,
                    "phase_trace_sample_rate": 0.5,
                },
                "target": {
                    "type": "synthetic",
                    "host": "127.0.0.1",
                    "port": 19500,
                    "synthetic_nodes": 1,
                },
                "faults": {"service_time": {"distribution": "fixed", "value_ms": 1}},
                "collectors": {"port_monitor": True, "netstats": False},
                "execution": {"mode": "local"},
                "reporting": {"generate_markdown": True},
            }
        ),
        encoding="utf-8",
    )

    study_dir = Path(
        run_study(str(spec_path), output_dir=str(tmp_path / "study"), force_local=True)
    )
    assert (study_dir / "study_manifest.json").is_file()
    assert (study_dir / "results_index.json").is_file()
    assert (study_dir / "report.md").is_file()

    results_index = json.loads((study_dir / "results_index.json").read_text(encoding="utf-8"))
    assert results_index["points"], "expected at least one completed point"
    point_dir = Path(results_index["points"][0]["artifacts"]["point_dir"])
    assert (point_dir / "client_metrics.json").is_file()
    assert (point_dir / "diagnosis.json").is_file()
    manifest = load_result_manifest(point_dir / "result_manifest.json")
    assert manifest.complete
    assert "client_shard/result_p0" in manifest.expected_ids
    assert "client_shard/metrics_p0" in manifest.expected_ids
    run_config = results_index["points"][0]["run_config"]
    assert run_config["canonical_run"]["claim_scope"] == "CLIENT_DIAGNOSTIC_ONLY"


def test_failed_point_is_persisted_but_study_exits_nonzero(tmp_path: Path, monkeypatch):
    from clientlab.runner import runtime

    spec_path = tmp_path / "failure.json"
    spec_path.write_text(
        json.dumps(
            {
                "study": {"name": "failure-study"},
                "target": {"type": "synthetic"},
                "reporting": {"generate_markdown": False, "generate_plots": False},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        runtime, "run_point", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    output = tmp_path / "failed-study"
    with pytest.raises(RuntimeError, match="study failed"):
        run_study(str(spec_path), output_dir=str(output))
    results = json.loads((output / "results_index.json").read_text())
    assert results["points"][0]["summary"]["diagnosis"] == "error"
