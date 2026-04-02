import copy
import json
import os
import shlex
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.error import URLError
from urllib.request import build_opener, urlopen, ProxyHandler

from clientlab.analysis.diagnostics import build_operating_envelope, compare_point_summaries, summarize_point, summarize_saturation, write_json
from clientlab.collectors.netstats import NetstatsProcess
from clientlab.collectors.ports import PortCollector, write_port_metrics
from clientlab.reports.html import render_report_html, write_html_report
from clientlab.reports.markdown import render_report, write_report
from clientlab.reports.plots import generate_plots
from clientlab.runner.spec_io import EXPECTED_POINT_ARTIFACTS, expand_matrix, load_study_spec
from clientlab.utils import dump_json_file, dump_yaml_file, ensure_dir, utc_timestamp
from site_config import get_site_config


def plan_study(spec_ref, output_dir=None):
    spec = load_study_spec(spec_ref)
    points = expand_matrix(spec)
    plan = {
        "schema_version": spec["schema_version"],
        "study": spec["study"],
        "execution": spec["execution"],
        "point_count": len(points),
        "output_dir": output_dir,
        "points": [
            {
                "point_id": point["_point_id"],
                "axis_values": point.get("_axis_values", {}),
                "expected_artifacts": EXPECTED_POINT_ARTIFACTS,
            }
            for point in points
        ],
    }
    if output_dir:
        plan_path = Path(output_dir) / "study_plan.json"
        dump_json_file(plan_path, plan)
    return plan


def run_study(spec_ref, output_dir=None, force_local=False, force_pbs=False):
    spec = load_study_spec(spec_ref)
    if force_local:
        spec["execution"]["mode"] = "local"
    if force_pbs:
        spec["execution"]["mode"] = "pbs_interactive"

    study_dir = Path(output_dir or default_study_dir(spec["study"]["name"]))
    ensure_dir(study_dir)
    points = expand_matrix(spec)
    point_results = []

    manifest = {
        "schema_version": spec["schema_version"],
        "study": spec["study"],
        "execution": spec["execution"],
        "reporting": spec["reporting"],
        "spec_path": spec["_spec_path"],
        "created_at": utc_timestamp(),
        "points": [{"point_id": point["_point_id"], "axis_values": point.get("_axis_values", {})} for point in points],
    }
    dump_json_file(study_dir / "study_manifest.json", manifest)

    total = len(points)
    print(f"[clientlab] Study '{spec['study']['name']}' — {total} points", flush=True)

    for idx, point in enumerate(points, 1):
        axis_str = ", ".join(f"{k}={v}" for k, v in point.get("_axis_values", {}).items())
        print(f"[clientlab] [{idx}/{total}] Running point: {axis_str}", flush=True)
        point_dir = study_dir / "points" / point["_point_id"]
        ensure_dir(point_dir)
        try:
            result = run_point(point, point_dir)
            expected = result["summary"].get("expected_rps", 0)
            achieved = result["summary"].get("achieved_rps", 0)
            diag = result["summary"].get("diagnosis", "?")
            print(f"[clientlab] [{idx}/{total}] Done — expected={expected:.1f} achieved={achieved:.1f} rps, diagnosis={diag}", flush=True)
        except Exception as exc:
            print(f"[clientlab] [{idx}/{total}] FAILED: {exc}", flush=True)
            result = {
                "point_id": point["_point_id"],
                "axis_values": point.get("_axis_values", {}),
                "run_config": sanitize_runtime_point(point),
                "artifacts": {"point_dir": str(point_dir)},
                "summary": {
                    "diagnosis": "error",
                    "reasons": [str(exc)],
                    "requested_rps": float(point.get("client", {}).get("rate", 0)),
                    "expected_rps": 0.0,
                    "achieved_rps": 0.0,
                    "success_fraction": 0.0,
                },
            }
        point_results.append(result)

    envelope = build_operating_envelope([result["summary"] for result in point_results])
    results_index = {
        "schema_version": spec["schema_version"],
        "study": spec["study"],
        "execution": spec["execution"],
        "reporting": spec["reporting"],
        "points": point_results,
        "operating_envelope": envelope,
    }
    dump_json_file(study_dir / "results_index.json", results_index)
    write_study_reports(study_dir, manifest, point_results, envelope)
    return str(study_dir)


def report_study(study_dir):
    study_root = Path(study_dir)
    manifest = json.loads((study_root / "study_manifest.json").read_text(encoding="utf-8"))
    results_index = json.loads((study_root / "results_index.json").read_text(encoding="utf-8"))
    write_study_reports(
        study_root,
        manifest,
        results_index.get("points", []),
        results_index.get("operating_envelope", {}),
    )
    return str(study_root / "report.md")


def compare_studies(study_dir_a, study_dir_b):
    points_a = json.loads((Path(study_dir_a) / "results_index.json").read_text(encoding="utf-8")).get("points", [])
    points_b = json.loads((Path(study_dir_b) / "results_index.json").read_text(encoding="utf-8")).get("points", [])
    return compare_point_summaries(points_a, points_b)


def run_smoke(preset, output_dir=None):
    spec = load_study_spec(preset)
    spec["study"]["repeats"] = 1
    spec["client"]["duration_s"] = min(float(spec["client"]["duration_s"]), 1.5)
    spec["client"]["rate"] = min(float(spec["client"]["rate"]), 25.0)
    spec["client"]["phase_trace_sample_rate"] = max(float(spec["client"]["phase_trace_sample_rate"]), 0.2)
    spec["execution"]["mode"] = "local"
    temp_spec = Path(output_dir or default_study_dir(f"{spec['study']['name']}-smoke")) / "_smoke_spec.json"
    ensure_dir(temp_spec.parent)
    temp_spec.write_text(json.dumps(spec, indent=2), encoding="utf-8")
    return run_study(str(temp_spec), output_dir=output_dir, force_local=True)


def default_study_dir(study_name):
    root = Path(get_site_config().bench_results_dir) / "clientlab"
    return str(root / f"{study_name}_{utc_timestamp()}")


def run_point(point, point_dir):
    run_config = sanitize_runtime_point(point)
    dump_yaml_file(point_dir / "run_config.yaml", run_config)
    ensure_dir(point_dir / "profiles")

    target_handles = launch_targets(run_config, point_dir)
    base_urls = [handle["base_url"] for handle in target_handles]
    if not base_urls:
        raise RuntimeError("ClientLab point resolved no target base URLs")

    port_collector = None
    if run_config["collectors"].get("port_monitor"):
        port_collector = PortCollector(interval_s=float(run_config["collectors"].get("port_monitor_interval_s", 1.0)))
        port_collector.start()

    netstats_proc = None
    if run_config["collectors"].get("netstats") and run_config["execution"]["mode"] == "local":
        netstats_proc = NetstatsProcess(
            output_path=str(point_dir / "netstats.jsonl"),
            interval_s=float(run_config["collectors"].get("netstats_interval_s", 1.0)),
            interfaces=str(run_config["collectors"].get("netstats_interfaces", "")),
        )

    sat_enabled = run_config["client"].get("saturation", {}).get("enabled", False)

    try:
        if sat_enabled:
            print("[clientlab]   Saturation mode enabled", flush=True)
            go_outputs = run_saturation_dispatch(run_config, point_dir, base_urls)
            target_metrics = fetch_target_metrics(base_urls) if run_config["collectors"].get("target_metrics") else {}
        else:
            trace_path = point_dir / "trace.jsonl"
            t0 = time.monotonic()
            trace_rows = generate_trace_rows(run_config)
            t1 = time.monotonic()
            write_trace(trace_path, trace_rows)
            t2 = time.monotonic()
            print(f"[clientlab]   Trace: {len(trace_rows)-1} requests (generate={t1-t0:.2f}s, write={t2-t1:.2f}s)", flush=True)
            duration = float(run_config["client"]["duration_s"])
            print(f"[clientlab]   Dispatching (duration={duration}s)...", flush=True)
            go_outputs = run_go_dispatch(trace_rows, run_config, point_dir, base_urls)
            target_metrics = fetch_target_metrics(base_urls) if run_config["collectors"].get("target_metrics") else {}
    finally:
        stop_targets(target_handles)
        time.sleep(1)  # Allow kernel to reclaim thread resources before next point.
        port_payload = port_collector.stop() if port_collector else {"samples": []}
        if netstats_proc is not None:
            _stdout, _stderr = netstats_proc.stop()
        else:
            _stdout = _stderr = ""

    if netstats_proc is None and not (point_dir / "netstats.jsonl").exists():
        (point_dir / "netstats.jsonl").write_text("", encoding="utf-8")

    write_port_metrics(point_dir / "port_metrics.json", port_payload)
    write_json(point_dir / "target_metrics.json", target_metrics)

    if sat_enabled:
        # Write stub artifact files so downstream consumers don't hit missing paths.
        # Saturation mode doesn't produce per-request metrics or phase traces.
        if not (point_dir / "client_metrics.json").exists():
            write_json(point_dir / "client_metrics.json", {})
        if not (point_dir / "phase_trace.jsonl").exists():
            (point_dir / "phase_trace.jsonl").write_text("", encoding="utf-8")
        summary = summarize_saturation(
            run_config=run_config,
            saturation_output=go_outputs.get("saturation_output", {}),
            target_metrics=target_metrics,
            port_metrics=port_payload,
            netstats_summary=summarize_netstats(point_dir / "netstats.jsonl"),
        )
    else:
        summary = summarize_point(
            run_config=run_config,
            client_metrics=go_outputs["client_metrics"],
            target_metrics=target_metrics,
            port_metrics=port_payload,
            netstats_summary=summarize_netstats(point_dir / "netstats.jsonl"),
        )
    write_json(point_dir / "derived_features.json", summary)
    write_json(point_dir / "diagnosis.json", summary)
    return {
        "point_id": point["_point_id"],
        "axis_values": point.get("_axis_values", {}),
        "run_config": run_config,
        "artifacts": {
            "point_dir": str(point_dir),
            "client_metrics": str(point_dir / "client_metrics.json"),
            "phase_trace": str(point_dir / "phase_trace.jsonl"),
            "target_metrics": str(point_dir / "target_metrics.json"),
            "port_metrics": str(point_dir / "port_metrics.json"),
            "netstats": str(point_dir / "netstats.jsonl"),
            "stdout": str(point_dir / "stdout.log"),
            "stderr": str(point_dir / "stderr.log"),
        },
        "summary": summary,
    }


def write_study_reports(study_dir, manifest, point_results, envelope):
    reporting = manifest.get("reporting", {})
    if reporting.get("generate_markdown", True):
        write_report(Path(study_dir) / "report.md", render_report(manifest, point_results, envelope))
    plot_paths = {}
    if reporting.get("generate_plots", True):
        plot_paths = generate_plots(study_dir, point_results)
    write_html_report(
        Path(study_dir) / "report.html",
        render_report_html(manifest, point_results, envelope, plot_paths),
    )


def sanitize_runtime_point(point):
    sanitized = copy.deepcopy(point)
    for key in ("_point_id", "_axis_values", "_repeat", "_spec_path", "_spec_dir"):
        sanitized.pop(key, None)
    execution_python = sanitized["execution"].get("python") or sys.executable
    sanitized["execution"]["python"] = execution_python
    return sanitized


def generate_trace_rows(run_config):
    rate = float(run_config["client"]["rate"])
    duration_s = float(run_config["client"]["duration_s"])
    count = max(1, int(rate * duration_s))
    interval = 1.0 / rate
    prompt = ("benchmark " * int(run_config["client"]["prompt_words"])).strip()
    rows = [{"__type__": "metadata", "generated_at": utc_timestamp(), "clientlab": True}]
    for idx in range(count):
        rows.append(
            {
                "timestamp": round(idx * interval, 6),
                "model": run_config["client"]["model"],
                "mode": run_config["client"]["mode"],
                "prompt": prompt,
                "input_len": int(run_config["client"]["prompt_words"]),
                "output_len": int(run_config["client"]["output_tokens"]),
                "tensor_parallel_size": 1,
                "req_id": uuid.uuid4().hex,
            }
        )
    return rows


def write_trace(path, rows):
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")


def run_go_dispatch(trace_rows, run_config, point_dir, base_urls):
    go_bin = ensure_go_binary()
    num_go_procs = int(run_config["client"].get("num_go_procs", 1))
    trace_partitions = [trace_rows[1 + idx :: max(1, num_go_procs)] for idx in range(max(1, num_go_procs))]

    stdout_log = (point_dir / "stdout.log").open("w", encoding="utf-8")
    stderr_log = (point_dir / "stderr.log").open("w", encoding="utf-8")
    processes = []
    try:
        for proc_idx, partition in enumerate(trace_partitions):
            partition_trace = point_dir / f"trace_p{proc_idx}.jsonl"
            partition_trace_rows = [trace_rows[0], *partition]
            write_trace(partition_trace, partition_trace_rows)
            result_path = point_dir / f"result_p{proc_idx}.jsonl"
            metrics_path = point_dir / f"client_metrics_p{proc_idx}.json"
            phase_path = point_dir / f"phase_trace_p{proc_idx}.jsonl"
            cmd = [
                go_bin,
                "--base-urls",
                ",".join(base_urls),
                "--generation-mode",
                str(run_config["client"].get("generation_mode", "deterministic")),
                "--timeout",
                str(run_config["client"].get("timeout_s", 3600.0)),
                "--max-active-requests",
                str(run_config["client"]["max_active_requests"]),
                "--queue-capacity",
                str(run_config["client"].get("queue_capacity", 0)),
                "--max-conns-per-host",
                str(run_config["client"].get("max_conns_per_host", 0)),
                "--num-go-workers",
                str(run_config["client"].get("num_go_workers", 2)),
                "--worker-id",
                f"{point_dir.name}_p{proc_idx}",
                "--trace-file",
                str(partition_trace),
                "--result-file",
                str(result_path),
                "--metrics-file",
                str(metrics_path),
                "--phase-trace-file",
                str(phase_path),
                "--phase-trace-sample-rate",
                str(run_config["client"].get("phase_trace_sample_rate", 0.0)),
            ]
            if run_config["client"].get("enable_httptrace", True):
                cmd.append("--enable-httptrace")
            if run_config["client"].get("sum_only", True):
                cmd.append("--sum-only")
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
            )
            processes.append(
                {
                    "process": proc,
                    "result_path": result_path,
                    "metrics_path": metrics_path,
                    "phase_path": phase_path,
                    "prefix": f"p{proc_idx}",
                }
            )

        for entry in processes:
            line = entry["process"].stdout.readline().strip()
            stdout_log.write(f"[{entry['prefix']}] {line}\n")
            stdout_log.flush()
            if line != "GO_CLI_READY":
                raise RuntimeError(f"go_dispatch failed readiness handshake for {entry['prefix']}: {line!r}")

        run_t0 = time.time() + 0.25
        for entry in processes:
            process = entry["process"]
            assert process.stdin is not None
            process.stdin.write(f"{run_t0!r}\n")
            process.stdin.flush()
            process.stdin.close()

        timeout_s = max(float(run_config["client"]["duration_s"]) + 120.0, 180.0)
        for entry in processes:
            process = entry["process"]
            try:
                process.wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                print(f"[clientlab]   WARNING: go_dispatch {entry['prefix']} timed out after {timeout_s:.0f}s, killing", flush=True)
                process.kill()
                process.wait(timeout=10)
            remaining_stdout = process.stdout.read() if process.stdout else ""
            remaining_stderr = process.stderr.read() if process.stderr else ""
            if remaining_stdout:
                stdout_log.write(prefix_output(entry["prefix"], remaining_stdout))
            if remaining_stderr:
                stderr_log.write(prefix_output(entry["prefix"], remaining_stderr))
            stdout_log.flush()
            stderr_log.flush()
            if process.returncode != 0:
                raise RuntimeError(f"go_dispatch {entry['prefix']} exited with {process.returncode}")
    finally:
        stdout_log.close()
        stderr_log.close()

    metrics_list = []
    for entry in processes:
        p = Path(entry["metrics_path"])
        if p.exists():
            metrics_list.append(json.loads(p.read_text(encoding="utf-8")))
    merged_metrics = merge_metrics_json(metrics_list) if metrics_list else {}
    result_list = []
    for entry in processes:
        p = Path(entry["result_path"])
        result_list.append(load_summary_result(p))
    merged_results = merge_summary_results(result_list) if result_list else {"requests_completed": 0, "errors": 0}
    merged_metrics["requests_succeeded"] = merged_results["requests_completed"]
    merged_metrics["requests_failed"] = merged_results["errors"]
    dump_json_file(point_dir / "client_metrics.json", merged_metrics)
    merge_phase_traces([Path(entry["phase_path"]) for entry in processes], point_dir / "phase_trace.jsonl")
    return {
        "client_metrics": merged_metrics,
        "result_summary": merged_results,
        "run_t0": run_t0,
    }


def run_saturation_dispatch(run_config, point_dir, base_urls):
    """Run saturation finder. Returns dict with saturation_output and client_metrics."""
    go_bin = ensure_go_binary()
    num_go_procs = int(run_config["client"].get("num_go_procs", 1))
    sat_cfg = run_config["client"]["saturation"]

    if num_go_procs <= 1:
        return _run_saturation_single(go_bin, run_config, point_dir, base_urls, sat_cfg)
    return _run_saturation_multi(go_bin, run_config, point_dir, base_urls, sat_cfg, num_go_procs)


def _build_sat_cmd(go_bin, run_config, base_urls, sat_cfg, mode, output_path, target_rate=None):
    """Build the Go CLI command for saturation or saturation-step mode."""
    cmd = [
        go_bin,
        "--mode", mode,
        "--base-urls", ",".join(base_urls),
        "--max-active-requests", str(run_config["client"]["max_active_requests"]),
        "--max-conns-per-host", str(run_config["client"].get("max_conns_per_host", 0)),
        "--num-go-workers", str(run_config["client"].get("num_go_workers", 2)),
        "--timeout", str(run_config["client"].get("timeout_s", 3600.0)),
        "--sat-model", str(sat_cfg.get("model", run_config["client"].get("model", "stub-model"))),
        "--sat-prompt-words", str(run_config["client"].get("prompt_words", 32)),
        "--sat-output-tokens", str(run_config["client"].get("output_tokens", 16)),
        "--sat-search-mode", str(sat_cfg.get("search_mode", "binary")),
        "--sat-initial-rate", str(sat_cfg.get("initial_rate", 100)),
        "--sat-max-rate", str(sat_cfg.get("max_rate", 0)),
        "--sat-step-duration", str(sat_cfg.get("step_duration_s", 10.0)),
        "--sat-warmup-duration", str(sat_cfg.get("warmup_duration_s", 3.0)),
        "--sat-cooldown-pause", str(sat_cfg.get("cooldown_pause_s", 2.0)),
        "--sat-tolerance", str(sat_cfg.get("tolerance", 0.05)),
        "--sat-max-error-rate", str(sat_cfg.get("max_error_rate", 0.01)),
        "--sat-plateau-ratio", str(sat_cfg.get("plateau_ratio", 0.95)),
        "--sat-step-up-start", str(sat_cfg.get("step_up_start", 0)),
        "--sat-step-up-end", str(sat_cfg.get("step_up_end", 0)),
        "--sat-step-up-increment", str(sat_cfg.get("step_up_increment", 0)),
        "--sat-output", str(output_path),
    ]
    if sat_cfg.get("verify", True):
        cmd.append("--sat-verify")
    else:
        cmd.extend(["--sat-verify=false"])
    if run_config["client"].get("enable_httptrace", True):
        cmd.append("--enable-httptrace")
    if target_rate is not None:
        cmd.extend(["--sat-target-rate", str(target_rate)])
    return cmd


def _run_saturation_single(go_bin, run_config, point_dir, base_urls, sat_cfg):
    """Single-process saturation: Go handles entire search."""
    output_path = point_dir / "saturation_output.json"
    cmd = _build_sat_cmd(go_bin, run_config, base_urls, sat_cfg, "saturation", output_path)

    stdout_log = (point_dir / "stdout.log").open("w", encoding="utf-8")
    stderr_log = (point_dir / "stderr.log").open("w", encoding="utf-8")
    try:
        proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            bufsize=1,
        )
        line = proc.stdout.readline().strip()
        stdout_log.write(f"[p0] {line}\n")
        stdout_log.flush()
        if line != "GO_CLI_READY":
            raise RuntimeError(f"go_dispatch saturation mode failed readiness: {line!r}")
        proc.stdin.close()

        # Generous timeout: search may run many steps.
        max_steps = 30
        step_time = float(sat_cfg.get("step_duration_s", 10)) + float(sat_cfg.get("warmup_duration_s", 3)) + float(sat_cfg.get("cooldown_pause_s", 2))
        timeout_s = max(max_steps * step_time + 120.0, 300.0)
        try:
            proc.wait(timeout=timeout_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
            remaining_stdout = proc.stdout.read() if proc.stdout else ""
            remaining_stderr = proc.stderr.read() if proc.stderr else ""
            if remaining_stdout:
                stdout_log.write(prefix_output("p0", remaining_stdout))
            if remaining_stderr:
                stderr_log.write(prefix_output("p0", remaining_stderr))
            stdout_log.flush()
            stderr_log.flush()
            raise RuntimeError(f"saturation process timed out after {timeout_s:.0f}s")

        remaining_stdout = proc.stdout.read() if proc.stdout else ""
        remaining_stderr = proc.stderr.read() if proc.stderr else ""
        if remaining_stdout:
            stdout_log.write(prefix_output("p0", remaining_stdout))
        if remaining_stderr:
            stderr_log.write(prefix_output("p0", remaining_stderr))
        stdout_log.flush()
        stderr_log.flush()

        if proc.returncode != 0:
            raise RuntimeError(f"go_dispatch saturation exited with {proc.returncode}")
    finally:
        stdout_log.close()
        stderr_log.close()

    if not output_path.exists():
        raise RuntimeError(f"saturation output file missing: {output_path} — Go process may have crashed before writing results")
    saturation_output = json.loads(output_path.read_text(encoding="utf-8"))
    if not saturation_output.get("steps"):
        raise RuntimeError(f"saturation output has no steps — output: {saturation_output}")
    return {"saturation_output": saturation_output, "client_metrics": {}, "result_summary": {}}


def _run_saturation_multi(go_bin, run_config, point_dir, base_urls, sat_cfg, num_go_procs):
    """Multi-process saturation: Python orchestrates binary search, launching N Go procs per step."""
    search_mode = sat_cfg.get("search_mode", "binary")
    tolerance = float(sat_cfg.get("tolerance", 0.05))
    max_error_rate = float(sat_cfg.get("max_error_rate", 0.01))
    plateau_ratio = float(sat_cfg.get("plateau_ratio", 0.95))

    lo = int(sat_cfg.get("initial_rate", 100))
    hi = int(sat_cfg.get("max_rate", 0))

    all_steps = []
    verification_steps = []
    saturation_rate = 0

    stdout_log = (point_dir / "stdout.log").open("w", encoding="utf-8")
    stderr_log = (point_dir / "stderr.log").open("w", encoding="utf-8")

    try:
        if search_mode == "binary":
            # Phase 1: Ceiling probe
            if hi <= 0:
                rate = lo
                last_healthy = 0
                while True:
                    result = _run_multi_step(go_bin, run_config, base_urls, sat_cfg, point_dir, num_go_procs, rate, len(all_steps), stdout_log, stderr_log)
                    healthy = _evaluate_merged_health(result, max_error_rate, plateau_ratio)
                    result["healthy"] = healthy
                    all_steps.append(result)
                    print(f"[clientlab]   probe {rate} rps → achieved={result.get('achieved_rate', 0):.1f} healthy={healthy}", flush=True)
                    if not healthy:
                        hi = rate
                        lo = last_healthy if last_healthy > 0 else rate // 2
                        break
                    last_healthy = rate
                    rate *= 2
                if hi <= 0:
                    saturation_rate = last_healthy

            # Phase 2: Binary search
            if hi > 0:
                while float(hi - lo) / float(max(hi, 1)) > tolerance:
                    mid = (lo + hi) // 2
                    if mid == lo:
                        break
                    result = _run_multi_step(go_bin, run_config, base_urls, sat_cfg, point_dir, num_go_procs, mid, len(all_steps), stdout_log, stderr_log)
                    healthy = _evaluate_merged_health(result, max_error_rate, plateau_ratio)
                    result["healthy"] = healthy
                    all_steps.append(result)
                    print(f"[clientlab]   search [{lo}, {hi}] → {mid} rps: achieved={result.get('achieved_rate', 0):.1f} healthy={healthy}", flush=True)
                    if healthy:
                        lo = mid
                    else:
                        hi = mid
                saturation_rate = lo

            # Phase 3: Verification
            if saturation_rate > 0 and sat_cfg.get("verify", True):
                v_start = int(saturation_rate * 0.7)
                v_end = int(saturation_rate * 1.3)
                v_inc = max(1, int(saturation_rate * 0.05))
                for rate in range(max(1, v_start), v_end + 1, v_inc):
                    result = _run_multi_step(go_bin, run_config, base_urls, sat_cfg, point_dir, num_go_procs, rate, len(all_steps) + len(verification_steps), stdout_log, stderr_log)
                    healthy = _evaluate_merged_health(result, max_error_rate, plateau_ratio)
                    result["healthy"] = healthy
                    verification_steps.append(result)
                    print(f"[clientlab]   verify {rate} rps: achieved={result.get('achieved_rate', 0):.1f} healthy={healthy}", flush=True)

        elif search_mode == "step-up":
            start = int(sat_cfg.get("step_up_start", 0))
            end = int(sat_cfg.get("step_up_end", 0))
            inc = int(sat_cfg.get("step_up_increment", 0))
            for rate in range(start, end + 1, max(1, inc)):
                result = _run_multi_step(go_bin, run_config, base_urls, sat_cfg, point_dir, num_go_procs, rate, len(all_steps), stdout_log, stderr_log)
                healthy = _evaluate_merged_health(result, max_error_rate, plateau_ratio)
                result["healthy"] = healthy
                all_steps.append(result)
                print(f"[clientlab]   step-up {rate} rps: achieved={result.get('achieved_rate', 0):.1f} healthy={healthy}", flush=True)
            for i in range(len(all_steps) - 1, -1, -1):
                if all_steps[i].get("healthy"):
                    saturation_rate = all_steps[i]["target_rate"]
                    break
    finally:
        stdout_log.close()
        stderr_log.close()

    saturation_output = {
        "mode": search_mode,
        "saturation_rate": saturation_rate,
        "tolerance": tolerance,
        "slo": {"max_error_rate": max_error_rate, "plateau_ratio": plateau_ratio},
        "steps": all_steps,
    }
    if verification_steps:
        saturation_output["verification_steps"] = verification_steps
    output_path = point_dir / "saturation_output.json"
    dump_json_file(output_path, saturation_output)
    return {"saturation_output": saturation_output, "client_metrics": {}, "result_summary": {}}


def _run_multi_step(go_bin, run_config, base_urls, sat_cfg, point_dir, num_procs, total_rate, step_idx, stdout_log, stderr_log):
    """Launch N Go processes at total_rate/N each, merge results."""
    if total_rate <= 0:
        return _merge_step_results([], total_rate)
    per_proc_rate = total_rate // num_procs
    remainder = total_rate % num_procs

    processes = []
    try:
        for proc_idx in range(num_procs):
            my_rate = per_proc_rate + (1 if proc_idx < remainder else 0)
            if my_rate <= 0:
                continue
            output_path = point_dir / f"step_{step_idx}_p{proc_idx}.json"
            cmd = _build_sat_cmd(go_bin, run_config, base_urls, sat_cfg, "saturation-step", output_path, target_rate=my_rate)
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
            )
            processes.append({"process": proc, "output_path": output_path, "prefix": f"s{step_idx}_p{proc_idx}", "rate": my_rate})

        # Wait for readiness
        for entry in processes:
            line = entry["process"].stdout.readline().strip()
            stdout_log.write(f"[{entry['prefix']}] {line}\n")
            stdout_log.flush()
            if line != "GO_CLI_READY":
                raise RuntimeError(f"saturation-step {entry['prefix']} failed readiness: {line!r}")
            entry["process"].stdin.close()

        # Wait for completion
        step_time = float(sat_cfg.get("step_duration_s", 10)) + float(sat_cfg.get("warmup_duration_s", 3)) + float(sat_cfg.get("cooldown_pause_s", 2))
        timeout_s = max(step_time + 60.0, 120.0)
        for entry in processes:
            try:
                entry["process"].wait(timeout=timeout_s)
            except subprocess.TimeoutExpired:
                entry["process"].kill()
                entry["process"].wait(timeout=10)
                raise RuntimeError(f"saturation-step {entry['prefix']} timed out after {timeout_s:.0f}s (target_rate={entry['rate']})")
            remaining_stdout = entry["process"].stdout.read() if entry["process"].stdout else ""
            remaining_stderr = entry["process"].stderr.read() if entry["process"].stderr else ""
            if remaining_stdout:
                stdout_log.write(prefix_output(entry["prefix"], remaining_stdout))
            if remaining_stderr:
                stderr_log.write(prefix_output(entry["prefix"], remaining_stderr))
            stdout_log.flush()
            stderr_log.flush()
            if entry["process"].returncode != 0:
                raise RuntimeError(f"saturation-step {entry['prefix']} exited with {entry['process'].returncode}")
    except Exception:
        for entry in processes:
            if entry["process"].poll() is None:
                entry["process"].kill()
                entry["process"].wait(timeout=5)
        raise

    # Merge step results
    step_results = []
    missing = []
    for entry in processes:
        p = Path(entry["output_path"])
        if p.exists():
            step_results.append(json.loads(p.read_text(encoding="utf-8")))
        else:
            missing.append(entry["prefix"])
    if missing:
        raise RuntimeError(f"saturation-step output files missing for: {missing} — Go processes may have crashed")
    if not step_results:
        raise RuntimeError("no saturation-step results collected — all processes failed to produce output")
    return _merge_step_results(step_results, total_rate)


def _merge_step_results(step_results, total_target_rate):
    """Merge N per-process StepResult dicts into one."""
    if not step_results:
        return {"target_rate": total_target_rate, "completed": 0, "failed": 0, "achieved_rate": 0, "error_rate": 0, "duration_s": 0, "healthy": False}
    merged = {
        "target_rate": total_target_rate,
        "completed": sum(int(r.get("completed", 0)) for r in step_results),
        "failed": sum(int(r.get("failed", 0)) for r in step_results),
        "duration_s": max(float(r.get("duration_s", 0)) for r in step_results),
        "p50_latency_s": max(float(r.get("p50_latency_s", 0)) for r in step_results),
        "p99_latency_s": max(float(r.get("p99_latency_s", 0)) for r in step_results),
        "mean_latency_s": sum(float(r.get("mean_latency_s", 0)) for r in step_results) / len(step_results),
        "new_connections": sum(int(r.get("new_connections", 0)) for r in step_results),
        "reused_connections": sum(int(r.get("reused_connections", 0)) for r in step_results),
        "max_observed_active": max(int(r.get("max_observed_active", 0)) for r in step_results),
    }
    total = merged["completed"] + merged["failed"]
    merged["error_rate"] = merged["failed"] / total if total > 0 else 0.0
    merged["achieved_rate"] = merged["completed"] / merged["duration_s"] if merged["duration_s"] > 0 else 0.0
    return merged


def _evaluate_merged_health(result, max_error_rate, plateau_ratio):
    """Evaluate SLO health on a merged step result."""
    if result.get("error_rate", 0) > max_error_rate:
        return False
    target = result.get("target_rate", 0)
    achieved = result.get("achieved_rate", 0)
    if target > 0 and achieved / target < plateau_ratio:
        return False
    return True


def ensure_go_binary():
    go_dir = Path("eval/go_client")
    go_bin = go_dir / "bin" / "go_dispatch"
    build_script = go_dir / "build.sh"
    sources = list(go_dir.glob("*.go")) + [build_script, go_dir / "go.mod"]
    needs_build = not (go_bin.is_file() and os.access(str(go_bin), os.X_OK))
    if not needs_build:
        bin_mtime = go_bin.stat().st_mtime
        needs_build = any(source.is_file() and source.stat().st_mtime > bin_mtime for source in sources)
    if not needs_build:
        return str(go_bin.resolve())
    build = subprocess.run(
        ["bash", str(build_script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if build.returncode != 0:
        raise RuntimeError(f"Failed to build go_dispatch:\n{build.stdout}\n{build.stderr}")
    if not go_bin.is_file():
        raise RuntimeError("go_dispatch build completed but binary was not found")
    return str(go_bin.resolve())


def ensure_cpp_server():
    cpp_dir = Path(__file__).resolve().parents[1] / "targets" / "cpp_server"
    cpp_bin = cpp_dir / "bin" / "synthetic_server"
    build_script = cpp_dir / "build.sh"
    sources = list(cpp_dir.glob("*.cpp")) + list(cpp_dir.glob("*.hpp")) + [build_script]
    needs_build = not (cpp_bin.is_file() and os.access(str(cpp_bin), os.X_OK))
    if not needs_build:
        bin_mtime = cpp_bin.stat().st_mtime
        needs_build = any(source.is_file() and source.stat().st_mtime > bin_mtime for source in sources)
    if not needs_build:
        return str(cpp_bin.resolve())
    build = subprocess.run(
        ["bash", str(build_script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if build.returncode != 0:
        raise RuntimeError(f"Failed to build cpp synthetic_server:\n{build.stdout}\n{build.stderr}")
    if not cpp_bin.is_file():
        raise RuntimeError("cpp synthetic_server build completed but binary was not found")
    return str(cpp_bin.resolve())


def load_summary_result(path):
    if not path.exists():
        return {"requests_completed": 0, "requests_scheduled": 0, "errors": 0, "p50_s": 0.0, "p99_s": 0.0}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        if payload.get("__type__") == "summary":
            return payload
    return {"requests_completed": 0, "requests_scheduled": 0, "errors": 0, "p50_s": 0.0, "p99_s": 0.0}


def merge_summary_results(results):
    merged = {
        "requests_completed": 0,
        "requests_scheduled": 0,
        "errors": 0,
        "p50_s": 0.0,
        "p99_s": 0.0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
    }
    for result in results:
        merged["requests_completed"] += int(result.get("requests_completed", 0))
        merged["requests_scheduled"] += int(result.get("requests_scheduled", 0))
        merged["errors"] += int(result.get("errors", 0))
        merged["p50_s"] = max(merged["p50_s"], float(result.get("p50_s", 0.0)))
        merged["p99_s"] = max(merged["p99_s"], float(result.get("p99_s", 0.0)))
        merged["total_input_tokens"] += int(result.get("total_input_tokens", 0))
        merged["total_output_tokens"] += int(result.get("total_output_tokens", 0))
    return merged


def merge_metrics_json(metrics_list):
    if not metrics_list:
        return {}
    merged = copy.deepcopy(metrics_list[0])
    for metrics in metrics_list[1:]:
        for key in ("requests_loaded", "requests_scheduled", "requests_completed", "requests_succeeded", "requests_failed", "new_connections", "reused_connections", "reused_idle_connections"):
            merged[key] = int(merged.get(key, 0)) + int(metrics.get(key, 0))
        for key in ("max_observed_active", "max_observed_outstanding", "max_observed_queue_depth", "last_request_start_at", "last_body_done_at", "completed_at"):
            merged[key] = max(float(merged.get(key, 0.0)), float(metrics.get(key, 0.0)))
        merged["status_counts"] = merge_count_maps(merged.get("status_counts", {}), metrics.get("status_counts", {}))
        merged["error_counts"] = merge_count_maps(merged.get("error_counts", {}), metrics.get("error_counts", {}))
        merged["histograms"] = merge_histograms(merged.get("histograms", {}), metrics.get("histograms", {}))
        merged["per_target"] = merge_per_target(merged.get("per_target", {}), metrics.get("per_target", {}))
    return merged


def merge_histograms(left, right):
    output = copy.deepcopy(left)
    for name, hist in right.items():
        if name not in output:
            output[name] = copy.deepcopy(hist)
            continue
        output[name]["count"] = int(output[name].get("count", 0)) + int(hist.get("count", 0))
        output[name]["sum_s"] = float(output[name].get("sum_s", 0.0)) + float(hist.get("sum_s", 0.0))
        output[name]["counts"] = [int(a) + int(b) for a, b in zip(output[name].get("counts", []), hist.get("counts", []))]
    return output


def merge_per_target(left, right):
    output = copy.deepcopy(left)
    for target, payload in right.items():
        if target not in output:
            output[target] = copy.deepcopy(payload)
            continue
        for key in ("requests", "successes", "failures", "new_connections", "reused_connections", "reused_idle_connections"):
            output[target][key] = int(output[target].get(key, 0)) + int(payload.get(key, 0))
        output[target]["status_counts"] = merge_count_maps(output[target].get("status_counts", {}), payload.get("status_counts", {}))
        output[target]["error_counts"] = merge_count_maps(output[target].get("error_counts", {}), payload.get("error_counts", {}))
    return output


def merge_count_maps(left, right):
    output = {str(key): int(value) for key, value in left.items()}
    for key, value in right.items():
        output[str(key)] = output.get(str(key), 0) + int(value)
    return output


def merge_phase_traces(paths, output_path):
    with output_path.open("w", encoding="utf-8") as out:
        for path in paths:
            if path.exists():
                out.write(path.read_text(encoding="utf-8"))


def prefix_output(prefix, text):
    return "".join(f"[{prefix}] {line}\n" for line in text.splitlines())


def launch_targets(run_config, point_dir):
    target_type = run_config["target"]["type"]
    if target_type in {"external", "proxy"}:
        base_urls = run_config["target"].get("base_urls") or []
        if not base_urls and run_config["target"].get("host") and run_config["target"].get("port"):
            base_urls = [f"http://{run_config['target']['host']}:{run_config['target']['port']}"]
        return [{"base_url": url, "process": None, "remote": False} for url in base_urls]
    if target_type != "synthetic":
        raise ValueError(f"Unsupported target.type: {target_type}")
    if run_config["execution"]["mode"] == "pbs_interactive":
        return launch_pbs_synthetic_targets(run_config, point_dir)
    return launch_local_synthetic_targets(run_config, point_dir)


def launch_local_synthetic_targets(run_config, point_dir):
    handles = []
    count = int(run_config["target"].get("synthetic_nodes", 1))
    host = str(run_config["target"].get("host", "127.0.0.1"))
    base_port = int(run_config["target"].get("port", 18100))
    server_cmd_prefix = [ensure_cpp_server()]
    try:
        for idx in range(count):
            target_config = copy.deepcopy(run_config)
            target_config["target"]["host"] = host
            target_config["target"]["port"] = base_port + idx
            config_path = point_dir / f"target_{idx}.json"
            config_path.write_text(json.dumps(target_config, indent=2), encoding="utf-8")
            stdout_log = (point_dir / f"target_{idx}.stdout.log").open("w", encoding="utf-8")
            stderr_log = (point_dir / f"target_{idx}.stderr.log").open("w", encoding="utf-8")
            proc = None
            handle = None
            try:
                proc = subprocess.Popen(
                    server_cmd_prefix + ["--config", str(config_path)],
                    stdout=stdout_log,
                    stderr=stderr_log,
                    universal_newlines=True,
                )
                base_url = f"http://{host}:{base_port + idx}"
                handle = {
                    "base_url": base_url,
                    "process": proc,
                    "stdout_log": stdout_log,
                    "stderr_log": stderr_log,
                    "remote": False,
                }
                handles.append(handle)
                wait_for_health(base_url)
            except Exception:
                if handle is None:
                    if proc and proc.poll() is None:
                        proc.kill()
                        proc.wait(timeout=5)
                    stdout_log.close()
                    stderr_log.close()
                raise
        return handles
    except Exception:
        stop_targets(handles)
        raise


def launch_pbs_synthetic_targets(run_config, point_dir):
    nodes = validate_pbs_session()
    client_nodes = int(run_config["execution"].get("client_nodes", 1))
    synthetic_nodes = int(run_config["target"].get("synthetic_nodes", 1))
    if len(nodes) < client_nodes + synthetic_nodes:
        raise RuntimeError("PBS allocation does not provide enough nodes for the requested synthetic target count")
    repo_root = str(Path(__file__).resolve().parents[2])
    env_script = str(run_config["execution"].get("env_script", "")).strip()
    server_cmd = shlex.quote(ensure_cpp_server())
    handles = []
    base_port = int(run_config["target"].get("port", 18100))
    try:
        for idx, node in enumerate(nodes[client_nodes : client_nodes + synthetic_nodes]):
            target_config = copy.deepcopy(run_config)
            target_config["target"]["host"] = "0.0.0.0"
            target_config["target"]["port"] = base_port + idx
            config_path = point_dir / f"target_{idx}.json"
            config_path.write_text(json.dumps(target_config, indent=2), encoding="utf-8")
            stdout_path = point_dir / f"target_{idx}.stdout.log"
            stderr_path = point_dir / f"target_{idx}.stderr.log"
            setup = f"source {shlex.quote(env_script)} && " if env_script else ""
            remote_cmd = (
                f"cd {shlex.quote(repo_root)} && "
                f"{setup}"
                f"nohup {server_cmd} "
                f"--config {shlex.quote(str(config_path))} "
                f"> {shlex.quote(str(stdout_path))} 2> {shlex.quote(str(stderr_path))} < /dev/null & echo $!"
            )
            pid = subprocess.check_output(["ssh", node, "bash", "-lc", remote_cmd], universal_newlines=True).strip()
            base_url = f"http://{resolve_hsn_host(node)}:{base_port + idx}"
            handles.append({"base_url": base_url, "node": node, "pid": pid, "remote": True})
            wait_for_health(base_url, timeout_s=30.0)
        return handles
    except Exception:
        stop_targets(handles)
        raise


def stop_targets(handles):
    for handle in handles:
        if handle.get("remote"):
            subprocess.run(
                ["ssh", handle["node"], f"kill -9 {handle['pid']}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
            )
        else:
            process = handle.get("process")
            if process and process.poll() is None:
                process.kill()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    pass
            # Ensure no synthetic_server lingers from this point.
            if process and process.poll() is None:
                import signal
                try:
                    os.kill(process.pid, signal.SIGKILL)
                except OSError:
                    pass
            if handle.get("stdout_log"):
                handle["stdout_log"].close()
            if handle.get("stderr_log"):
                handle["stderr_log"].close()


def fetch_target_metrics(base_urls):
    targets = []
    aggregate = {
        "total_requests": 0,
        "accepted": 0,
        "completed": 0,
        "rejections": 0,
        "errors": 0,
        "max_active": 0,
        "max_queue_depth": 0,
        "error_fraction": 0.0,
    }
    for base_url in base_urls:
        try:
            with build_opener(ProxyHandler({})).open(f"{base_url}/metrics", timeout=3.0) as response:
                payload = json.loads(response.read())
        except URLError:
            payload = {"error": "unreachable", "base_url": base_url}
        payload["base_url"] = base_url
        targets.append(payload)
        if "total_requests" in payload:
            aggregate["total_requests"] += int(payload.get("total_requests", 0))
            aggregate["accepted"] += int(payload.get("accepted", 0))
            aggregate["completed"] += int(payload.get("completed", 0))
            aggregate["rejections"] += int(payload.get("rejections", 0))
            aggregate["errors"] += int(payload.get("errors", 0))
            aggregate["max_active"] = max(aggregate["max_active"], int(payload.get("max_active", 0)))
            aggregate["max_queue_depth"] = max(aggregate["max_queue_depth"], int(payload.get("max_queue_depth", 0)))
    if aggregate["completed"] > 0:
        aggregate["error_fraction"] = aggregate["errors"] / aggregate["completed"]
    return {"targets": targets, "aggregate": aggregate}


def wait_for_health(base_url, timeout_s=15.0):
    # Bypass any http_proxy that module load frameworks may set on compute nodes.
    opener = build_opener(ProxyHandler({}))
    deadline = time.monotonic() + timeout_s
    last_error = None
    while time.monotonic() < deadline:
        try:
            with opener.open(f"{base_url}/health", timeout=1.0) as response:
                if response.status == 200:
                    return
        except Exception as exc:  # pragma: no cover - exercised in runtime
            last_error = exc
        time.sleep(0.25)
    raise RuntimeError(f"Target at {base_url} did not become healthy: {last_error}")


def validate_pbs_session():
    job_id = os.environ.get("PBS_JOBID")
    nodefile = os.environ.get("PBS_NODEFILE")
    if not job_id or not nodefile or not Path(nodefile).is_file():
        raise RuntimeError("ClientLab PBS execution requires a valid interactive PBS session (PBS_JOBID and PBS_NODEFILE).")
    nodes = []
    for line in Path(nodefile).read_text(encoding="utf-8").splitlines():
        node = line.strip()
        if node and node not in nodes:
            nodes.append(node)
    hostname = socket.gethostname().split(".")[0]
    if hostname not in {node.split(".")[0] for node in nodes}:
        raise RuntimeError("Current hostname is not part of PBS_NODEFILE; refusing to assume a valid interactive session.")
    return nodes


def resolve_hsn_host(node):
    candidate = f"{node}.hsn.cm.aurora.alcf.anl.gov"
    try:
        socket.getaddrinfo(candidate, None)
        return candidate
    except socket.gaierror:
        return node


def summarize_netstats(path):
    if not path.exists() or not path.read_text(encoding="utf-8").strip():
        return None
    max_rx_drops = 0
    max_tx_drops = 0
    by_interface = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        by_interface.setdefault(payload["interface"], []).append(payload)
        max_rx_drops = max(max_rx_drops, int(payload.get("rx_drops", 0)))
        max_tx_drops = max(max_tx_drops, int(payload.get("tx_drops", 0)))
    max_bandwidth_fraction = 0.0
    for records in by_interface.values():
        records = sorted(records, key=lambda row: row["timestamp"])
        for prev, cur in zip(records, records[1:]):
            delta_t = max(float(cur["timestamp"]) - float(prev["timestamp"]), 1e-9)
            tx_gbs = max(float(cur["tx_bytes"]) - float(prev["tx_bytes"]), 0.0) / delta_t / 1e9
            rx_gbs = max(float(cur["rx_bytes"]) - float(prev["rx_bytes"]), 0.0) / delta_t / 1e9
            max_bandwidth_fraction = max(max_bandwidth_fraction, tx_gbs / 25.0, rx_gbs / 25.0)
    return {
        "max_rx_drops": max_rx_drops,
        "max_tx_drops": max_tx_drops,
        "max_bandwidth_fraction": max_bandwidth_fraction,
    }
