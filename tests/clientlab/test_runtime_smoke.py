import json
from pathlib import Path

from clientlab.runner.runtime import run_study


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

    study_dir = Path(run_study(str(spec_path), output_dir=str(tmp_path / "study"), force_local=True))
    assert (study_dir / "study_manifest.json").is_file()
    assert (study_dir / "results_index.json").is_file()
    assert (study_dir / "report.md").is_file()

    results_index = json.loads((study_dir / "results_index.json").read_text(encoding="utf-8"))
    assert results_index["points"], "expected at least one completed point"
    point_dir = Path(results_index["points"][0]["artifacts"]["point_dir"])
    assert (point_dir / "client_metrics.json").is_file()
    assert (point_dir / "diagnosis.json").is_file()
