import copy
import hashlib
import json
import math
import os
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path
from datetime import datetime, timezone
from urllib.error import URLError
from urllib.request import build_opener, ProxyHandler

from clientlab import SCHEMA_VERSION
from clientlab.analysis.diagnostics import (
    DIAGNOSES,
    build_operating_envelope,
    compare_point_summaries,
    summarize_point,
    summarize_saturation,
    write_json,
)
from clientlab.collectors.netstats import NetstatsProcess
from clientlab.collectors.ports import PortCollector, write_port_metrics
from clientlab.reports.html import render_report_html, write_html_report
from clientlab.reports.markdown import render_report, write_report
from clientlab.reports.plots import generate_plots
from clientlab.runner.spec_io import EXPECTED_POINT_ARTIFACTS, expand_matrix, load_study_spec
from clientlab.utils import dump_json_file, ensure_dir, utc_timestamp
from eval.site_config import get_site_config
from exaserve.control.process_handshake import (
    prepare_ready_handshake,
    ready_handshake_args,
    wait_ready_handshake,
)
from exaserve.control.supervisor import ManagedComponent, RuntimeSupervisor
from exaserve.exception_notes import add_exception_note
from exaserve.go_result_contract import LATENCY_QUANTILE_METHOD, read_go_result_stream
from exaserve.state.atomic import (
    ExclusiveLease,
    LeaseHeartbeat,
    atomic_create_json,
    atomic_create_or_verify_bytes,
    atomic_create_or_verify_json,
    atomic_create_or_verify_yaml,
    atomic_write_json,
    atomic_write_text,
    regular_file_reader,
    strict_json_load_path,
    strict_json_loads,
)

_MAX_SATURATION_CEILING_PROBES = 30
_STUDY_MANIFEST_FIELDS = {
    "schema_version",
    "study",
    "execution",
    "reporting",
    "spec_path",
    "created_at",
    "points",
}
_RESULTS_INDEX_FIELDS = {
    "schema_version",
    "study",
    "execution",
    "reporting",
    "points",
    "operating_envelope",
}
_POINT_RESULT_FIELDS = {"point_id", "axis_values", "run_config", "artifacts", "summary"}


def _load_clientlab_json(path: Path) -> object:
    try:
        return strict_json_load_path(path)
    except (OSError, ValueError) as exc:
        raise ValueError(f"invalid ClientLab artifact {path}: {exc}") from exc


def _persisted_number(value: object, *, path: str, nonnegative: bool = True) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{path} must be a finite number")
    result = float(value)
    if not math.isfinite(result) or (nonnegative and result < 0):
        raise ValueError(f"{path} must be a finite nonnegative number")
    return result


def _validate_point_results(value: object) -> list[dict]:
    if not isinstance(value, list):
        raise ValueError("ClientLab results points must be a list")
    points: list[dict] = []
    point_ids: set[str] = set()
    for index, point in enumerate(value):
        label = f"ClientLab results point {index}"
        if not isinstance(point, dict) or set(point) != _POINT_RESULT_FIELDS:
            raise ValueError(f"{label} fields are invalid")
        point_id = point["point_id"]
        if not isinstance(point_id, str) or not point_id or point_id in point_ids:
            raise ValueError(f"{label} point_id is invalid or duplicated")
        point_ids.add(point_id)
        if not isinstance(point["axis_values"], dict):
            raise ValueError(f"{label}.axis_values must be a mapping")
        if not isinstance(point["run_config"], dict):
            raise ValueError(f"{label}.run_config must be a mapping")
        client = point["run_config"].get("client")
        if not isinstance(client, dict):
            raise ValueError(f"{label}.run_config.client must be a mapping")
        active = client.get("max_active_requests")
        if type(active) is not int or active < 0:
            raise ValueError(f"{label}.run_config.client.max_active_requests is invalid")
        if not isinstance(point["artifacts"], dict) or any(
            not isinstance(key, str) or not isinstance(path, str)
            for key, path in point["artifacts"].items()
        ):
            raise ValueError(f"{label}.artifacts must map text keys to text paths")
        summary = point["summary"]
        if not isinstance(summary, dict):
            raise ValueError(f"{label}.summary must be a mapping")
        diagnosis = summary.get("diagnosis")
        if not isinstance(diagnosis, str) or diagnosis not in DIAGNOSES:
            raise ValueError(f"{label}.summary.diagnosis is invalid")
        for key in ("requested_rps", "achieved_rps", "success_fraction"):
            _persisted_number(summary.get(key), path=f"{label}.summary.{key}")
        for key in ("queue_fraction", "safe_active_budget_estimate"):
            if key in summary:
                _persisted_number(summary[key], path=f"{label}.summary.{key}")
        points.append(point)
    return points


def _validate_results_index(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != _RESULTS_INDEX_FIELDS:
        raise ValueError("ClientLab results index fields are invalid")
    if type(value["schema_version"]) is not str or value["schema_version"] != SCHEMA_VERSION:
        raise ValueError(f"unsupported ClientLab results schema {value['schema_version']!r}")
    for field in ("study", "execution", "reporting"):
        if not isinstance(value[field], dict):
            raise ValueError(f"ClientLab results {field} must be a mapping")
    value["points"] = _validate_point_results(value["points"])
    envelope = value["operating_envelope"]
    if not isinstance(envelope, dict) or set(envelope) != {
        "max_stable_rps",
        "safe_active_budget",
        "notes",
    }:
        raise ValueError("ClientLab operating envelope fields are invalid")
    _persisted_number(envelope["max_stable_rps"], path="operating_envelope.max_stable_rps")
    if type(envelope["safe_active_budget"]) is not int or envelope["safe_active_budget"] < 0:
        raise ValueError("operating_envelope.safe_active_budget must be a nonnegative integer")
    if not isinstance(envelope["notes"], list) or any(
        not isinstance(note, str) for note in envelope["notes"]
    ):
        raise ValueError("operating_envelope.notes must be a list of text")
    return value


def _validate_study_manifest(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != _STUDY_MANIFEST_FIELDS:
        raise ValueError("ClientLab study manifest fields are invalid")
    if type(value["schema_version"]) is not str or value["schema_version"] != SCHEMA_VERSION:
        raise ValueError(f"unsupported ClientLab study schema {value['schema_version']!r}")
    for field in ("study", "execution", "reporting"):
        if not isinstance(value[field], dict):
            raise ValueError(f"ClientLab study manifest {field} must be a mapping")
    study = value["study"]
    execution = value["execution"]
    if not isinstance(study.get("name"), str) or not isinstance(study.get("suite"), str):
        raise ValueError("ClientLab study identity is invalid")
    if execution.get("mode") not in {"local", "pbs_interactive"}:
        raise ValueError("ClientLab execution mode is invalid")
    if not isinstance(value["spec_path"], str) or not isinstance(value["created_at"], str):
        raise ValueError("ClientLab study provenance is invalid")
    points = value["points"]
    if not isinstance(points, list):
        raise ValueError("ClientLab study points must be a list")
    point_ids: set[str] = set()
    for index, point in enumerate(points):
        if not isinstance(point, dict) or set(point) != {"point_id", "axis_values"}:
            raise ValueError(f"ClientLab study point {index} fields are invalid")
        point_id = point["point_id"]
        if not isinstance(point_id, str) or not point_id or point_id in point_ids:
            raise ValueError(f"ClientLab study point {index} identity is invalid or duplicated")
        point_ids.add(point_id)
        if not isinstance(point["axis_values"], dict):
            raise ValueError(f"ClientLab study point {index}.axis_values must be a mapping")
    return value


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
        ensure_dir(plan_path.parent)
        atomic_create_or_verify_json(plan_path, plan)
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
        "points": [
            {"point_id": point["_point_id"], "axis_values": point.get("_axis_values", {})}
            for point in points
        ],
    }
    try:
        atomic_create_json(study_dir / "study_manifest.json", manifest)
    except FileExistsError as exc:
        raise RuntimeError(
            f"refusing to overwrite an existing ClientLab study: {study_dir}"
        ) from exc

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
            print(
                f"[clientlab] [{idx}/{total}] Done — expected={expected:.1f} achieved={achieved:.1f} rps, diagnosis={diag}",
                flush=True,
            )
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
    atomic_create_json(study_dir / "results_index.json", results_index)
    write_study_reports(study_dir, manifest, point_results, envelope)
    failed_points = [
        item["point_id"]
        for item in point_results
        if item.get("summary", {}).get("diagnosis") == "error"
    ]
    if failed_points:
        raise RuntimeError(
            f"ClientLab study failed at {len(failed_points)} point(s): {failed_points}"
        )
    return str(study_dir)


def report_study(study_dir):
    study_root = Path(study_dir)
    manifest = _validate_study_manifest(_load_clientlab_json(study_root / "study_manifest.json"))
    results_index = _validate_results_index(_load_clientlab_json(study_root / "results_index.json"))
    for field in ("schema_version", "study", "execution", "reporting"):
        if manifest[field] != results_index[field]:
            raise ValueError(f"ClientLab manifest/results {field} disagree")
    manifest_points = [point["point_id"] for point in manifest["points"]]
    result_points = [point["point_id"] for point in results_index["points"]]
    if manifest_points != result_points:
        raise ValueError("ClientLab manifest/results point identities disagree")
    write_study_reports(
        study_root,
        manifest,
        results_index["points"],
        results_index["operating_envelope"],
    )
    return str(study_root / "report.md")


def compare_studies(study_dir_a, study_dir_b):
    points_a = _validate_results_index(
        _load_clientlab_json(Path(study_dir_a) / "results_index.json")
    )["points"]
    points_b = _validate_results_index(
        _load_clientlab_json(Path(study_dir_b) / "results_index.json")
    )["points"]
    return compare_point_summaries(points_a, points_b)


def run_smoke(preset, output_dir=None):
    spec = load_study_spec(preset)
    if spec["target"]["type"] == "exaserve":
        raise ValueError(
            "ClientLab smoke rewriting is synthetic-only; an ExaServe study "
            "must preserve its canonical RunPlan semantics"
        )
    spec["study"]["repeats"] = 1
    spec["client"]["duration_s"] = min(float(spec["client"]["duration_s"]), 1.5)
    spec["client"]["rate"] = min(float(spec["client"]["rate"]), 25.0)
    spec["client"]["phase_trace_sample_rate"] = max(
        float(spec["client"]["phase_trace_sample_rate"]), 0.2
    )
    spec["execution"]["mode"] = "local"
    temp_spec = (
        Path(output_dir or default_study_dir(f"{spec['study']['name']}-smoke")) / "_smoke_spec.json"
    )
    ensure_dir(temp_spec.parent)
    # Internal fields are re-derived when the temporary spec is loaded.
    smoke_spec = {key: value for key, value in spec.items() if not str(key).startswith("_")}
    atomic_write_json(temp_spec, smoke_spec)
    return run_study(str(temp_spec), output_dir=output_dir, force_local=True)


def default_study_dir(study_name):
    root = Path(get_site_config().bench_results_dir) / "clientlab"
    return str(root / f"{study_name}_{utc_timestamp()}")


def run_point(point, point_dir):
    point_dir = Path(point_dir)
    with (
        ExclusiveLease(
            point_dir / ".run.lease",
            ttl_s=24 * 3600,
            owner_note=f"ClientLab point {point.get('_point_id', '?')}",
        ) as lease,
        LeaseHeartbeat(lease, interval_s=60.0) as heartbeat,
    ):
        if (point_dir / "result_manifest.json").exists():
            raise RuntimeError(f"refusing to replace immutable ClientLab result {point_dir}")
        return _run_point_owned(point, point_dir, lease_heartbeat=heartbeat)


def _run_point_owned(point, point_dir, *, lease_heartbeat=None):
    run_config = sanitize_runtime_point(point)
    atomic_create_or_verify_yaml(point_dir / "run_config.yaml", run_config)
    ensure_dir(point_dir / "profiles")

    target_handles = []
    base_urls = []
    port_collector = None
    netstats_proc = None
    sat_enabled = run_config["client"].get("saturation", {}).get("enabled", False)
    port_payload = {"samples": []}
    target_metrics = {}
    go_outputs = None
    workload_error = None
    try:
        target_handles = launch_targets(run_config, point_dir)
        base_urls = [handle["base_url"] for handle in target_handles]
        if not base_urls:
            raise RuntimeError("ClientLab point resolved no target base URLs")

        if run_config["collectors"].get("port_monitor"):
            port_collector = PortCollector(
                interval_s=float(run_config["collectors"].get("port_monitor_interval_s", 1.0))
            )
            port_collector.start()
        if run_config["collectors"].get("netstats") and run_config["execution"]["mode"] == "local":
            netstats_proc = NetstatsProcess(
                output_path=str(point_dir / "netstats.jsonl"),
                interval_s=float(run_config["collectors"].get("netstats_interval_s", 1.0)),
                interfaces=str(run_config["collectors"].get("netstats_interfaces", "")),
            )

        trace_rows = None
        request_count = 0
        if run_config["target"]["type"] == "exaserve":
            trace_rows, request_count = load_canonical_trace(run_config, point_dir)
        if sat_enabled:
            print("[clientlab]   Saturation mode enabled", flush=True)
            go_outputs = run_saturation_dispatch(run_config, point_dir, base_urls)
            target_metrics = (
                fetch_target_metrics(base_urls)
                if run_config["collectors"].get("target_metrics")
                else {}
            )
        else:
            t0 = time.monotonic()
            if trace_rows is None:
                trace_rows = generate_trace_rows(run_config)
                request_count = sum(row.get("__type__") != "metadata" for row in trace_rows)
                write_trace(point_dir / "trace.jsonl", trace_rows)
            t1 = time.monotonic()
            t2 = time.monotonic()
            print(
                f"[clientlab]   Trace: {request_count} requests "
                f"(prepare={t1 - t0:.2f}s, publish={t2 - t1:.2f}s)",
                flush=True,
            )
            duration = float(run_config["client"]["duration_s"])
            print(f"[clientlab]   Dispatching (duration={duration}s)...", flush=True)
            go_outputs = run_go_dispatch(trace_rows, run_config, point_dir, base_urls)
            target_metrics = (
                fetch_target_metrics(base_urls)
                if run_config["collectors"].get("target_metrics")
                else {}
            )
    except BaseException as exc:
        workload_error = exc
        raise
    finally:
        cleanup_errors = []
        try:
            stop_targets(target_handles)
        except Exception as exc:
            cleanup_errors.append(f"target cleanup: {exc}")
        try:
            port_payload = port_collector.stop() if port_collector else {"samples": []}
        except Exception as exc:
            port_payload = {"samples": [], "error": str(exc)}
            cleanup_errors.append(f"port collector cleanup: {exc}")
        if netstats_proc is not None:
            try:
                _stdout, _stderr = netstats_proc.stop()
            except Exception as exc:
                _stdout, _stderr = "", str(exc)
                cleanup_errors.append(f"netstats cleanup: {exc}")
        else:
            _stdout = _stderr = ""
        if cleanup_errors:
            detail = "; ".join(cleanup_errors)
            if workload_error is not None:
                add_exception_note(workload_error, detail)
                print(f"[clientlab] cleanup also failed: {detail}", file=sys.stderr, flush=True)
            else:
                raise RuntimeError(detail)

    if netstats_proc is None and not (point_dir / "netstats.jsonl").exists():
        atomic_write_text(point_dir / "netstats.jsonl", "")

    write_port_metrics(point_dir / "port_metrics.json", port_payload)
    write_json(point_dir / "target_metrics.json", target_metrics)

    if sat_enabled:
        # Write stub artifact files so downstream consumers don't hit missing paths.
        # Saturation mode doesn't produce per-request metrics or phase traces.
        if not (point_dir / "client_metrics.json").exists():
            write_json(point_dir / "client_metrics.json", {})
        if not (point_dir / "phase_trace.jsonl").exists():
            atomic_write_text(point_dir / "phase_trace.jsonl", "")
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
    if lease_heartbeat is not None:
        lease_heartbeat.ensure_held()
    manifest = publish_clientlab_result_manifest(run_config, point_dir, saturation=sat_enabled)
    if lease_heartbeat is not None:
        lease_heartbeat.ensure_held()
    if not manifest.complete:
        raise RuntimeError(
            "ClientLab result is incomplete: " + "; ".join(manifest.incomplete_reasons)
        )
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
            "result_manifest": str(point_dir / "result_manifest.json"),
        },
        "summary": summary,
    }


def publish_clientlab_result_manifest(run_config, point_dir, *, saturation):
    """Commit one immutable, completeness-checked ClientLab point result."""
    from exaserve.state.results import (
        ResultEntry,
        ResultManifest,
        load_result_manifest,
        write_result_manifest,
    )

    point_dir = Path(point_dir).resolve()
    canonical = run_config["canonical_run"]
    reasons = []
    paths = {
        "run_config": point_dir / "run_config.yaml",
        "client_metrics": point_dir / "client_metrics.json",
        "phase_trace": point_dir / "phase_trace.jsonl",
        "target_metrics": point_dir / "target_metrics.json",
        "port_metrics": point_dir / "port_metrics.json",
        "netstats": point_dir / "netstats.jsonl",
        "stdout": point_dir / "stdout.log",
        "stderr": point_dir / "stderr.log",
        "derived_features": point_dir / "derived_features.json",
        "diagnosis": point_dir / "diagnosis.json",
    }
    if saturation:
        paths["saturation_output"] = point_dir / "saturation_output.json"
        for step_path in sorted(point_dir.glob("step_*_p*.json")):
            paths[f"saturation_shard/{step_path.name}"] = step_path
    else:
        paths["dispatched_trace"] = point_dir / "trace.jsonl"
        for process_index in range(int(run_config["client"]["num_go_procs"])):
            paths[f"client_shard/result_p{process_index}"] = (
                point_dir / f"result_p{process_index}.jsonl"
            )
            paths[f"client_shard/metrics_p{process_index}"] = (
                point_dir / f"client_metrics_p{process_index}.json"
            )
            paths[f"client_shard/phase_p{process_index}"] = (
                point_dir / f"phase_trace_p{process_index}.jsonl"
            )
            paths[f"client_shard/trace_p{process_index}"] = (
                point_dir / f"trace_p{process_index}.jsonl"
            )

    if canonical["claim_scope"] == "EXASERVE_DEPLOYMENT":
        from exaserve.evidence import capture_ready_evidence
        from exaserve.plan.io import load_run_plan

        plan_dest = point_dir / "canonical_run_plan.json"
        try:
            with regular_file_reader(canonical["run_plan_path"], binary=True) as handle:
                plan_bytes = handle.read()
            atomic_create_or_verify_bytes(plan_dest, plan_bytes)
            copied_plan = load_run_plan(plan_dest)
            if copied_plan.run_semantic_hash != canonical["run_semantic_hash"]:
                raise RuntimeError("copied RunPlan identity changed")
            paths["canonical_run_plan"] = plan_dest
        except Exception as exc:
            reasons.append(f"canonical RunPlan capture failed: {exc}")
        paths["canonical_trace"] = point_dir / "canonical_trace.jsonl"
        try:
            evidence = capture_ready_evidence(
                status_dir=canonical["deployment_status_dir"],
                destination_dir=str(point_dir),
                expected_generation=int(canonical["expected_generation"]),
                expected_plan_hash=canonical["deployment_plan_hash"],
                expected_run_semantic_hash=canonical["run_semantic_hash"],
            )
            paths.update({name: Path(path) for name, path in evidence.items()})
        except Exception as exc:
            reasons.append(f"READY evidence capture failed: {exc}")
            paths.update(
                {
                    "deployment_ready_evidence": point_dir / "deployment_ready_evidence.json",
                    "compatibility_receipts": point_dir / "compatibility_receipts.json",
                    "run_provenance": point_dir / "run_provenance.json",
                }
            )

    for log_path in sorted(point_dir.glob("target*.stdout.log")):
        paths[f"target_log/{log_path.name}"] = log_path
    for profile_path in sorted((point_dir / "profiles").glob("**/*")):
        if profile_path.is_file() and not profile_path.is_symlink():
            paths[f"profile/{profile_path.relative_to(point_dir / 'profiles')}"] = profile_path

    entries = []
    for logical_id, path in sorted(paths.items()):
        try:
            entries.append(ResultEntry.from_file(logical_id, str(path), root=str(point_dir)))
        except (OSError, ValueError) as exc:
            reasons.append(f"{logical_id}: {exc}")
    expected_ids = tuple(sorted(paths))
    observed_ids = {entry.logical_id for entry in entries}
    for missing in sorted(set(expected_ids) - observed_ids):
        reasons.append(f"required result {missing} is missing")
    reasons = sorted(set(reasons))
    manifest = ResultManifest(
        schema_version=2,
        run_id=canonical["run_id"],
        run_semantic_hash=canonical["run_semantic_hash"],
        deployment_plan_hash=canonical["deployment_plan_hash"],
        expected_ids=expected_ids,
        entries=tuple(sorted(entries, key=lambda entry: entry.logical_id)),
        incomplete_reasons=tuple(reasons),
        generated_at=datetime.now(timezone.utc).isoformat(),
        complete=not reasons and observed_ids == set(expected_ids),
    ).finalize()
    manifest_path = point_dir / "result_manifest.json"
    write_result_manifest(str(manifest_path), manifest)
    return load_result_manifest(str(manifest_path))


def write_study_reports(study_dir, manifest, point_results, envelope):
    reporting = manifest.get("reporting", {})
    if reporting.get("generate_markdown", True):
        write_report(
            Path(study_dir) / "report.md", render_report(manifest, point_results, envelope)
        )
    plot_paths = {}
    if reporting.get("generate_plots", True):
        plot_paths = generate_plots(study_dir, point_results)
    write_html_report(
        Path(study_dir) / "report.html",
        render_report_html(manifest, point_results, envelope, plot_paths),
    )


def sanitize_runtime_point(point):
    sanitized = copy.deepcopy(point)
    point_id = str(sanitized.get("_point_id", "clientlab-point"))
    canonical_run = sanitized.pop("_canonical_run", None)
    for key in ("_point_id", "_axis_values", "_repeat", "_spec_path", "_spec_dir"):
        sanitized.pop(key, None)
    if canonical_run is None:
        from exaserve.plan.contracts import canonical_hash

        semantic_payload = {
            key: sanitized[key] for key in ("client", "target", "faults", "execution")
        }
        canonical_run = {
            "run_id": f"clientlab/{sanitized['study']['name']}/{point_id}",
            "run_semantic_hash": canonical_hash(semantic_payload),
            "deployment_id": f"clientlab-synthetic/{point_id}",
            "deployment_plan_hash": canonical_hash(
                {"target": sanitized["target"], "faults": sanitized["faults"]}
            ),
            "trace_content_hash": "",
            "claim_scope": "CLIENT_DIAGNOSTIC_ONLY",
            "stall_timeout_s": 120.0,
            "saturation_stream": False,
            "saturation_max_p99_ttft": 0.0,
        }
    sanitized["canonical_run"] = canonical_run
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
                "req_id": f"cl-{run_config['canonical_run']['run_semantic_hash'][:16]}-{idx}",
            }
        )
    return rows


def write_trace(path, rows):
    atomic_write_text(
        path,
        "".join(json.dumps(row, sort_keys=True, allow_nan=False) + "\n" for row in rows),
    )


def load_canonical_trace(run_config, destination):
    """Copy, verify, and adapt canonical trace rows for the Go client.

    The canonical bytes remain in ``canonical_trace.jsonl``.  Missing request
    IDs are filled deterministically in the dispatched trace because the eval
    trace format historically assigned IDs only at replay time.
    """
    canonical = run_config["canonical_run"]
    source = canonical["trace_path"]
    expected_hash = canonical["trace_content_hash"]
    with regular_file_reader(source, binary=True) as handle:
        source_bytes = handle.read()
    if hashlib.sha256(source_bytes).hexdigest() != expected_hash:
        raise RuntimeError("canonical trace changed after ClientLab planning")
    atomic_create_or_verify_bytes(destination / "canonical_trace.jsonl", source_bytes)

    rows = []
    request_index = 0
    request_ids = set()
    prior_timestamp = -1.0
    allowed_models = set(canonical["model_ids"])
    for line_number, raw_line in enumerate(source_bytes.splitlines(), 1):
        if not raw_line.strip():
            continue
        try:
            row = strict_json_loads(raw_line)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"canonical trace line {line_number} is invalid JSON: {exc}"
            ) from exc
        if not isinstance(row, dict):
            raise RuntimeError(f"canonical trace line {line_number} is not an object")
        if row.get("__type__") == "metadata":
            rows.append(row)
            continue
        required = {
            "timestamp",
            "model",
            "mode",
            "prompt",
            "input_len",
            "output_len",
            "tensor_parallel_size",
        }
        if not required <= set(row):
            raise RuntimeError(
                f"canonical trace line {line_number} misses {sorted(required - set(row))}"
            )
        timestamp = row["timestamp"]
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or not math.isfinite(float(timestamp))
            or float(timestamp) < prior_timestamp
        ):
            raise RuntimeError(f"canonical trace line {line_number} has invalid ordering")
        prior_timestamp = float(timestamp)
        if row["model"] not in allowed_models:
            raise RuntimeError(f"canonical trace line {line_number} names unplanned model")
        if row["mode"] not in {"chat", "completion"}:
            raise RuntimeError(f"canonical trace line {line_number} has unsupported mode")
        for name in ("input_len", "output_len", "tensor_parallel_size"):
            if isinstance(row[name], bool) or not isinstance(row[name], int) or row[name] < 0:
                raise RuntimeError(f"canonical trace line {line_number} has invalid {name}")
        request_id = row.get("req_id")
        if request_id is None or request_id == "":
            request_id = f"cl-{expected_hash[:16]}-{request_index}"
            row["req_id"] = request_id
        if not isinstance(request_id, str) or request_id in request_ids:
            raise RuntimeError(f"canonical trace line {line_number} has invalid/duplicate req_id")
        request_ids.add(request_id)
        request_index += 1
        rows.append(row)
    if request_index == 0:
        raise RuntimeError("canonical trace contains no requests")
    write_trace(destination / "trace.jsonl", rows)
    return rows, request_index


def _terminate_exact_process(process, *, grace_s=5.0, deadline=None):
    """Bounded cleanup of the exact process group created for one Go client."""
    if process is None:
        return
    grace_s = _finite_positive(grace_s, name="client cleanup grace_s")
    if deadline is not None and (
        isinstance(deadline, bool)
        or not isinstance(deadline, (int, float))
        or not math.isfinite(float(deadline))
        or deadline < 0
    ):
        raise ValueError("client cleanup deadline must be finite and nonnegative")
    deadline = float(deadline) if deadline is not None else time.monotonic() + grace_s

    def group_exists() -> bool:
        try:
            os.killpg(process.pid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def remaining() -> float:
        return max(0.0, deadline - time.monotonic())

    if not group_exists():
        if process.poll() is None:
            try:
                process.wait(timeout=remaining())
            except subprocess.TimeoutExpired as exc:
                raise RuntimeError(
                    f"client process {process.pid} did not reap by cleanup deadline"
                ) from exc
        return

    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    term_deadline = time.monotonic() + remaining() / 2.0
    while group_exists() and time.monotonic() < term_deadline:
        if process.poll() is None:
            try:
                process.wait(timeout=min(0.05, max(0.0, term_deadline - time.monotonic())))
            except subprocess.TimeoutExpired:
                pass
        else:
            time.sleep(min(0.02, max(0.0, term_deadline - time.monotonic())))
    # The launcher may exit after TERM while leaving descendants in its
    # session.  Group existence, not the leader's return code, decides whether
    # escalation is still required.
    if group_exists():
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    if process.poll() is None:
        try:
            process.wait(timeout=remaining())
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(
                f"client process {process.pid} survived SIGKILL by cleanup deadline"
            ) from exc
    while group_exists() and time.monotonic() < deadline:
        time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))
    if group_exists():
        raise RuntimeError(f"client process group {process.pid} survived bounded TERM/KILL cleanup")


class _DrainHandle:
    """A log-drain thread whose failure and liveness remain owner-visible."""

    def __init__(self, stream, log_handle, prefix, lock):
        self._stream = stream
        self._log_handle = log_handle
        self._prefix = prefix
        self._lock = lock
        self._error = None
        self._thread = threading.Thread(
            target=self._drain,
            name=f"clientlab-drain-{prefix}",
            daemon=True,
        )

    def _drain(self):
        if self._stream is None:
            return
        try:
            for line in self._stream:
                with self._lock:
                    self._log_handle.write(f"[{self._prefix}] {line}")
                    self._log_handle.flush()
        except BaseException as exc:
            self._error = exc

    def start(self):
        self._thread.start()
        return self

    def join(self, timeout):
        timeout = max(0.0, float(timeout))
        self._thread.join(timeout)
        if self._thread.is_alive():
            raise RuntimeError(f"log drain {self._prefix} did not stop within its deadline")
        if self._error is not None:
            raise RuntimeError(f"log drain {self._prefix} failed: {self._error}") from self._error

    def raise_if_failed(self):
        if self._error is not None:
            raise RuntimeError(f"log drain {self._prefix} failed: {self._error}") from self._error

    def is_alive(self):
        return self._thread.is_alive()


def _finish_client_processes(entries, *log_handles, cleanup_s=10.0):
    """Visit every process/drain/log and preserve an active workload cause."""
    cleanup_s = _finite_positive(cleanup_s, name="ClientLab cleanup_s")
    deadline = time.monotonic() + cleanup_s
    errors = []
    for entry in entries:
        try:
            _terminate_exact_process(entry.get("process"), deadline=deadline)
        except Exception as exc:
            errors.append(f"process cleanup: {type(exc).__name__}: {exc}")
    for entry in entries:
        for drain in entry.get("drains", ()):
            try:
                drain.join(max(0.0, deadline - time.monotonic()))
            except Exception as exc:
                errors.append(f"log cleanup: {type(exc).__name__}: {exc}")
    drains_alive = any(drain.is_alive() for entry in entries for drain in entry.get("drains", ()))
    if drains_alive:
        errors.append("log close skipped because a drain thread still owns the stream")
    else:
        for log_handle in log_handles:
            try:
                log_handle.close()
            except OSError as exc:
                errors.append(f"log close: {type(exc).__name__}: {exc}")
    if not errors:
        return
    detail = "; ".join(errors)
    active = sys.exc_info()[1]
    if active is not None:
        add_exception_note(active, f"ClientLab cleanup also failed: {detail}")
        print(f"[clientlab] cleanup also failed: {detail}", file=sys.stderr, flush=True)
        return
    raise RuntimeError(f"ClientLab cleanup failed: {detail}")


def _start_drain(stream, log_handle, prefix, lock):
    return _DrainHandle(stream, log_handle, prefix, lock).start()


def _finite_positive(value, *, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be finite and positive")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be finite and positive")
    return parsed


def _contract_mapping(value, *, label):
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be a JSON object")
    return value


def _contract_count(mapping, key, *, label=None, default=0):
    mapping = _contract_mapping(mapping, label=label or "metrics")
    value = mapping.get(key, default)
    if type(value) is not int or value < 0:
        raise RuntimeError(f"{label or 'metrics'}.{key} must be a nonnegative integer")
    return value


def _contract_number(mapping, key, *, label=None, default=0.0, nonnegative=True):
    mapping = _contract_mapping(mapping, label=label or "metrics")
    value = mapping.get(key, default)
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or (nonnegative and value < 0)
    ):
        raise RuntimeError(f"{label or 'metrics'}.{key} must be a finite number")
    return float(value)


def _wait_client_entries(entries, *, timeout_s, activity):
    """Wait for concurrent clients against one absolute deadline."""
    timeout_s = _finite_positive(timeout_s, name=f"{activity} timeout")
    deadline = time.monotonic() + timeout_s
    pending = list(entries)
    while pending:
        for entry in list(pending):
            for drain in entry.get("drains", ()):
                drain.raise_if_failed()
            process = entry["process"]
            returncode = process.poll()
            if returncode is None:
                continue
            pending.remove(entry)
            if returncode != 0:
                raise RuntimeError(f"{activity} {entry['prefix']} exited with {returncode}")
        if not pending:
            return
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            labels = [entry["prefix"] for entry in pending]
            raise RuntimeError(
                f"{activity} exceeded its {timeout_s:.0f}s group deadline; "
                f"pending processes: {labels}"
            )
        time.sleep(min(0.1, remaining))


def run_go_dispatch(trace_rows, run_config, point_dir, base_urls):
    go_bin = ensure_go_binary()
    num_go_procs = int(run_config["client"].get("num_go_procs", 1))
    metadata = [row for row in trace_rows if row.get("__type__") == "metadata"]
    requests = [row for row in trace_rows if row.get("__type__") != "metadata"]
    trace_partitions = [
        requests[idx :: max(1, num_go_procs)] for idx in range(max(1, num_go_procs))
    ]

    stdout_log = (point_dir / "stdout.log").open("w", encoding="utf-8")
    stderr_log = (point_dir / "stderr.log").open("w", encoding="utf-8")
    processes = []
    output_lock = threading.Lock()
    try:
        for proc_idx, partition in enumerate(trace_partitions):
            partition_trace = point_dir / f"trace_p{proc_idx}.jsonl"
            partition_trace_rows = [*metadata[:1], *partition]
            write_trace(partition_trace, partition_trace_rows)
            result_path = point_dir / f"result_p{proc_idx}.jsonl"
            metrics_path = point_dir / f"client_metrics_p{proc_idx}.json"
            phase_path = point_dir / f"phase_trace_p{proc_idx}.jsonl"
            ready_path, ready_token = prepare_ready_handshake(point_dir, f"go-dispatch-p{proc_idx}")
            cmd = [
                go_bin,
                "--base-urls",
                ",".join(base_urls),
                "--generation-mode",
                str(run_config["client"].get("generation_mode", "deterministic")),
                "--timeout",
                str(run_config["client"].get("timeout_s", 3600.0)),
                "--stall-timeout",
                str(run_config["canonical_run"].get("stall_timeout_s", 120.0)),
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
                *ready_handshake_args(ready_path, ready_token),
            ]
            if run_config["client"].get("enable_httptrace", True):
                cmd.append("--enable-httptrace")
            if run_config["client"].get("sum_only", True):
                cmd.append("--sum-only")
            if run_config["client"].get("streaming", False):
                cmd.append("--stream")
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
                start_new_session=True,
            )
            entry = {
                "process": proc,
                "result_path": result_path,
                "metrics_path": metrics_path,
                "phase_path": phase_path,
                "expected_request_ids": tuple(row["req_id"] for row in partition),
                "prefix": f"p{proc_idx}",
                "ready_path": ready_path,
                "ready_token": ready_token,
            }
            processes.append(entry)
            # Drain immediately: startup diagnostics must not fill a pipe before
            # the readiness handshake can be published.
            entry["drains"] = (
                _start_drain(proc.stdout, stdout_log, entry["prefix"], output_lock),
                _start_drain(proc.stderr, stderr_log, entry["prefix"], output_lock),
            )

        for entry in processes:
            wait_ready_handshake(
                entry["process"],
                path=entry["ready_path"],
                token=entry["ready_token"],
            )

        run_t0 = time.time() + 0.25
        for entry in processes:
            process = entry["process"]
            assert process.stdin is not None
            process.stdin.write(f"{run_t0!r}\n")
            process.stdin.flush()
            process.stdin.close()

        duration_s = _finite_positive(run_config["client"]["duration_s"], name="client duration_s")
        request_timeout_s = _finite_positive(
            run_config["client"].get("timeout_s", 3600.0), name="client timeout_s"
        )
        timeout_s = max(duration_s + request_timeout_s + 120.0, 180.0)
        _wait_client_entries(processes, timeout_s=timeout_s, activity="go_dispatch")
    finally:
        _finish_client_processes(processes, stdout_log, stderr_log)

    metrics_list = []
    for entry in processes:
        p = Path(entry["metrics_path"])
        try:
            metrics = strict_json_load_path(p)
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                f"client metrics shard {entry['prefix']} is missing or invalid: {exc}"
            ) from exc
        _validate_metrics_payload(metrics, label=f"client metrics {entry['prefix']}")
        expected_count = len(entry["expected_request_ids"])
        for field in ("requests_loaded", "requests_scheduled", "requests_completed"):
            if metrics[field] != expected_count:
                raise RuntimeError(
                    f"client metrics {entry['prefix']}.{field}={metrics[field]} "
                    f"does not cover its {expected_count} planned requests"
                )
        metrics_list.append(metrics)
    merged_metrics = merge_metrics_json(metrics_list)
    result_list = []
    for entry in processes:
        p = Path(entry["result_path"])
        result_list.append(
            load_summary_result(
                p,
                expected_request_ids=entry["expected_request_ids"],
                sum_only=bool(run_config["client"].get("sum_only", True)),
            )
        )
    merged_results = merge_summary_results(result_list)
    expected_requests = len(requests)
    if (
        merged_results["requests_scheduled"] != expected_requests
        or merged_results["requests_completed"] != expected_requests
    ):
        raise RuntimeError(
            "Go result shards are incomplete: "
            f"scheduled={merged_results['requests_scheduled']} "
            f"completed={merged_results['requests_completed']} expected={expected_requests}"
        )
    if (
        merged_metrics["requests_scheduled"] != merged_results["requests_scheduled"]
        or merged_metrics["requests_completed"] != merged_results["requests_completed"]
        or merged_metrics["requests_failed"] != merged_results["errors"]
        or merged_metrics["requests_succeeded"]
        != merged_results["requests_completed"] - merged_results["errors"]
    ):
        raise RuntimeError("Go metrics and terminal result shards disagree on request completeness")
    merged_metrics["requests_succeeded"] = (
        merged_results["requests_completed"] - merged_results["errors"]
    )
    merged_metrics["requests_failed"] = merged_results["errors"]
    dump_json_file(point_dir / "client_metrics.json", merged_metrics)
    merge_phase_traces(
        [Path(entry["phase_path"]) for entry in processes], point_dir / "phase_trace.jsonl"
    )
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
        "--mode",
        mode,
        "--base-urls",
        ",".join(base_urls),
        "--max-active-requests",
        str(run_config["client"]["max_active_requests"]),
        "--max-conns-per-host",
        str(run_config["client"].get("max_conns_per_host", 0)),
        "--num-go-workers",
        str(run_config["client"].get("num_go_workers", 2)),
        "--timeout",
        str(run_config["client"].get("timeout_s", 3600.0)),
        "--stall-timeout",
        str(run_config["canonical_run"].get("stall_timeout_s", 120.0)),
        "--sat-model",
        str(sat_cfg.get("model", run_config["client"].get("model", "stub-model"))),
        "--sat-prompt-words",
        str(run_config["client"].get("prompt_words", 32)),
        "--sat-output-tokens",
        str(run_config["client"].get("output_tokens", 16)),
        "--sat-search-mode",
        str(sat_cfg.get("search_mode", "binary")),
        "--sat-initial-rate",
        str(sat_cfg.get("initial_rate", 100)),
        "--sat-max-rate",
        str(sat_cfg.get("max_rate", 0)),
        "--sat-step-duration",
        str(sat_cfg.get("step_duration_s", 10.0)),
        "--sat-warmup-duration",
        str(sat_cfg.get("warmup_duration_s", 3.0)),
        "--sat-cooldown-pause",
        str(sat_cfg.get("cooldown_pause_s", 2.0)),
        "--sat-tolerance",
        str(sat_cfg.get("tolerance", 0.05)),
        "--sat-max-error-rate",
        str(sat_cfg.get("max_error_rate", 0.01)),
        "--sat-plateau-ratio",
        str(sat_cfg.get("plateau_ratio", 0.95)),
        "--sat-step-up-start",
        str(sat_cfg.get("step_up_start", 0)),
        "--sat-step-up-end",
        str(sat_cfg.get("step_up_end", 0)),
        "--sat-step-up-increment",
        str(sat_cfg.get("step_up_increment", 0)),
        "--sat-output",
        str(output_path),
    ]
    if sat_cfg.get("verify", True):
        cmd.append("--sat-verify")
    else:
        cmd.extend(["--sat-verify=false"])
    if run_config["client"].get("enable_httptrace", True):
        cmd.append("--enable-httptrace")
    if run_config["canonical_run"].get("saturation_stream", False):
        cmd.append("--sat-stream")
    max_ttft = float(run_config["canonical_run"].get("saturation_max_p99_ttft", 0.0))
    if max_ttft > 0:
        cmd.extend(["--sat-max-p99-ttft", str(max_ttft)])
    if target_rate is not None:
        cmd.extend(["--sat-target-rate", str(target_rate)])
    return cmd


def _run_saturation_single(go_bin, run_config, point_dir, base_urls, sat_cfg):
    """Single-process saturation: Go handles entire search."""
    output_path = point_dir / "saturation_output.json"
    cmd = _build_sat_cmd(go_bin, run_config, base_urls, sat_cfg, "saturation", output_path)
    ready_path, ready_token = prepare_ready_handshake(point_dir, "go-saturation-p0")
    cmd.extend(ready_handshake_args(ready_path, ready_token))

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
            start_new_session=True,
        )
        output_lock = threading.Lock()
        drains = (
            _start_drain(proc.stdout, stdout_log, "p0", output_lock),
            _start_drain(proc.stderr, stderr_log, "p0", output_lock),
        )
        wait_ready_handshake(proc, path=ready_path, token=ready_token)
        proc.stdin.close()

        # Generous timeout: search may run many steps.
        max_steps = 30
        step_time = (
            float(sat_cfg.get("step_duration_s", 10))
            + float(sat_cfg.get("warmup_duration_s", 3))
            + float(sat_cfg.get("cooldown_pause_s", 2))
        )
        request_timeout_s = _finite_positive(
            run_config["client"].get("timeout_s", 3600.0), name="client timeout_s"
        )
        timeout_s = max(max_steps * step_time + request_timeout_s + 120.0, 300.0)
        _wait_client_entries(
            [{"process": proc, "prefix": "p0", "drains": drains}],
            timeout_s=timeout_s,
            activity="saturation process",
        )
    finally:
        entries = []
        if "proc" in locals():
            entries.append({"process": proc, "drains": locals().get("drains", ())})
        _finish_client_processes(entries, stdout_log, stderr_log)

    try:
        saturation_output = strict_json_load_path(output_path)
    except (OSError, ValueError) as exc:
        raise RuntimeError(
            f"saturation output file is missing or invalid: {output_path}: {exc}"
        ) from exc
    from eval.lib.saturation import validate_saturation_output

    saturation_output = validate_saturation_output(saturation_output)
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
                for _probe_index in range(_MAX_SATURATION_CEILING_PROBES):
                    result = _run_multi_step(
                        go_bin,
                        run_config,
                        base_urls,
                        sat_cfg,
                        point_dir,
                        num_go_procs,
                        rate,
                        len(all_steps),
                        stdout_log,
                        stderr_log,
                    )
                    healthy = _evaluate_merged_health(result, max_error_rate, plateau_ratio)
                    result["healthy"] = healthy
                    all_steps.append(result)
                    print(
                        f"[clientlab]   probe {rate} rps → achieved={result.get('achieved_rate', 0):.1f} healthy={healthy}",
                        flush=True,
                    )
                    if not healthy:
                        hi = rate
                        lo = last_healthy if last_healthy > 0 else rate // 2
                        break
                    last_healthy = rate
                    rate *= 2
                else:
                    raise RuntimeError(
                        "saturation ceiling probe remained healthy for "
                        f"{_MAX_SATURATION_CEILING_PROBES} bounded attempts; "
                        "set client.saturation.max_rate explicitly"
                    )
                if hi <= 0:
                    saturation_rate = last_healthy

            # Phase 2: Binary search
            if hi > 0:
                while float(hi - lo) / float(max(hi, 1)) > tolerance:
                    mid = (lo + hi) // 2
                    if mid == lo:
                        break
                    result = _run_multi_step(
                        go_bin,
                        run_config,
                        base_urls,
                        sat_cfg,
                        point_dir,
                        num_go_procs,
                        mid,
                        len(all_steps),
                        stdout_log,
                        stderr_log,
                    )
                    healthy = _evaluate_merged_health(result, max_error_rate, plateau_ratio)
                    result["healthy"] = healthy
                    all_steps.append(result)
                    print(
                        f"[clientlab]   search [{lo}, {hi}] → {mid} rps: achieved={result.get('achieved_rate', 0):.1f} healthy={healthy}",
                        flush=True,
                    )
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
                    result = _run_multi_step(
                        go_bin,
                        run_config,
                        base_urls,
                        sat_cfg,
                        point_dir,
                        num_go_procs,
                        rate,
                        len(all_steps) + len(verification_steps),
                        stdout_log,
                        stderr_log,
                    )
                    healthy = _evaluate_merged_health(result, max_error_rate, plateau_ratio)
                    result["healthy"] = healthy
                    verification_steps.append(result)
                    print(
                        f"[clientlab]   verify {rate} rps: achieved={result.get('achieved_rate', 0):.1f} healthy={healthy}",
                        flush=True,
                    )

        elif search_mode == "step-up":
            start = int(sat_cfg.get("step_up_start", 0))
            end = int(sat_cfg.get("step_up_end", 0))
            inc = int(sat_cfg.get("step_up_increment", 0))
            for rate in range(start, end + 1, max(1, inc)):
                result = _run_multi_step(
                    go_bin,
                    run_config,
                    base_urls,
                    sat_cfg,
                    point_dir,
                    num_go_procs,
                    rate,
                    len(all_steps),
                    stdout_log,
                    stderr_log,
                )
                healthy = _evaluate_merged_health(result, max_error_rate, plateau_ratio)
                result["healthy"] = healthy
                all_steps.append(result)
                print(
                    f"[clientlab]   step-up {rate} rps: achieved={result.get('achieved_rate', 0):.1f} healthy={healthy}",
                    flush=True,
                )
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
    from eval.lib.saturation import validate_saturation_output

    validate_saturation_output(saturation_output)
    output_path = point_dir / "saturation_output.json"
    dump_json_file(output_path, saturation_output)
    return {"saturation_output": saturation_output, "client_metrics": {}, "result_summary": {}}


def _run_multi_step(
    go_bin,
    run_config,
    base_urls,
    sat_cfg,
    point_dir,
    num_procs,
    total_rate,
    step_idx,
    stdout_log,
    stderr_log,
):
    """Launch N Go processes at total_rate/N each, merge results."""
    if total_rate <= 0:
        return _merge_step_results([], total_rate)
    per_proc_rate = total_rate // num_procs
    remainder = total_rate % num_procs

    processes = []
    output_lock = threading.Lock()
    try:
        for proc_idx in range(num_procs):
            my_rate = per_proc_rate + (1 if proc_idx < remainder else 0)
            if my_rate <= 0:
                continue
            output_path = point_dir / f"step_{step_idx}_p{proc_idx}.json"
            cmd = _build_sat_cmd(
                go_bin,
                run_config,
                base_urls,
                sat_cfg,
                "saturation-step",
                output_path,
                target_rate=my_rate,
            )
            ready_path, ready_token = prepare_ready_handshake(
                point_dir, f"go-saturation-s{step_idx}-p{proc_idx}"
            )
            cmd.extend(ready_handshake_args(ready_path, ready_token))
            proc = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1,
                start_new_session=True,
            )
            entry = {
                "process": proc,
                "output_path": output_path,
                "prefix": f"s{step_idx}_p{proc_idx}",
                "rate": my_rate,
                "ready_path": ready_path,
                "ready_token": ready_token,
            }
            processes.append(entry)
            entry["drains"] = (
                _start_drain(proc.stdout, stdout_log, entry["prefix"], output_lock),
                _start_drain(proc.stderr, stderr_log, entry["prefix"], output_lock),
            )

        # Wait for readiness
        for entry in processes:
            wait_ready_handshake(
                entry["process"],
                path=entry["ready_path"],
                token=entry["ready_token"],
            )
            entry["process"].stdin.close()

        # Wait for completion
        step_time = (
            float(sat_cfg.get("step_duration_s", 10))
            + float(sat_cfg.get("warmup_duration_s", 3))
            + float(sat_cfg.get("cooldown_pause_s", 2))
        )
        request_timeout_s = _finite_positive(
            run_config["client"].get("timeout_s", 3600.0), name="client timeout_s"
        )
        timeout_s = max(step_time + request_timeout_s + 60.0, 120.0)
        _wait_client_entries(processes, timeout_s=timeout_s, activity="saturation-step")
    finally:
        _finish_client_processes(processes)

    # Merge step results
    step_results = []
    missing = []
    for entry in processes:
        p = Path(entry["output_path"])
        try:
            step_results.append(strict_json_load_path(p))
        except (OSError, ValueError):
            missing.append(entry["prefix"])
    if missing:
        raise RuntimeError(
            f"saturation-step output files missing for: {missing} — Go processes may have crashed"
        )
    if not step_results:
        raise RuntimeError(
            "no saturation-step results collected — all processes failed to produce output"
        )
    return _merge_step_results(step_results, total_rate)


def _merge_step_results(step_results, total_target_rate):
    """Merge N per-process StepResult dicts into one."""
    if not step_results:
        return {
            "target_rate": total_target_rate,
            "completed": 0,
            "failed": 0,
            "achieved_rate": 0,
            "error_rate": 0,
            "duration_s": 0,
            "healthy": False,
        }
    from eval.lib.saturation import validate_step_result

    for index, result in enumerate(step_results):
        validate_step_result(result, path=f"saturation result {index}")
    merged_latency = {}
    for result in step_results:
        merged_latency = merge_histograms(
            {"latency": merged_latency} if merged_latency else {},
            {"latency": result["latency_histogram"]},
        )["latency"]
    ttft_histograms = [result.get("ttft_histogram") for result in step_results]
    if any(item is not None for item in ttft_histograms) and not all(
        item is not None for item in ttft_histograms
    ):
        raise RuntimeError("saturation TTFT histogram coverage differs across processes")
    merged_ttft = None
    if all(item is not None for item in ttft_histograms):
        for histogram in ttft_histograms:
            merged_ttft = merge_histograms(
                {"ttft": merged_ttft} if merged_ttft else {}, {"ttft": histogram}
            )["ttft"]
    merged = {
        "target_rate": total_target_rate,
        "completed": sum(
            _contract_count(r, "completed", label=f"saturation result {index}")
            for index, r in enumerate(step_results)
        ),
        "failed": sum(
            _contract_count(r, "failed", label=f"saturation result {index}")
            for index, r in enumerate(step_results)
        ),
        "duration_s": max(
            _contract_number(r, "duration_s", label=f"saturation result {index}")
            for index, r in enumerate(step_results)
        ),
        "p50_latency_s": _percentile_from_histogram(merged_latency, 0.50),
        "p99_latency_s": _percentile_from_histogram(merged_latency, 0.99),
        "mean_latency_s": (
            merged_latency["sum_s"] / merged_latency["count"] if merged_latency["count"] else 0.0
        ),
        "latency_histogram": merged_latency,
        "new_connections": sum(
            _contract_count(r, "new_connections", label=f"saturation result {index}")
            for index, r in enumerate(step_results)
        ),
        "reused_connections": sum(
            _contract_count(r, "reused_connections", label=f"saturation result {index}")
            for index, r in enumerate(step_results)
        ),
        "max_observed_active": max(
            _contract_count(r, "max_observed_active", label=f"saturation result {index}")
            for index, r in enumerate(step_results)
        ),
    }
    total = merged["completed"] + merged["failed"]
    merged["error_rate"] = merged["failed"] / total if total > 0 else 0.0
    merged["achieved_rate"] = (
        merged["completed"] / merged["duration_s"] if merged["duration_s"] > 0 else 0.0
    )
    if merged_ttft is not None:
        merged["ttft_histogram"] = merged_ttft
        merged["p50_ttft_s"] = _percentile_from_histogram(merged_ttft, 0.50)
        merged["p99_ttft_s"] = _percentile_from_histogram(merged_ttft, 0.99)
        merged["mean_ttft_s"] = (
            merged_ttft["sum_s"] / merged_ttft["count"] if merged_ttft["count"] else 0.0
        )
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
    from exaserve.control.finite_process import run_finite

    go_dir = Path(__file__).resolve().parents[2] / "eval" / "go_client"
    go_bin = go_dir / "bin" / "go_dispatch"
    sources = list(go_dir.glob("*.go")) + [go_dir / "go.mod"]
    needs_build = not (go_bin.is_file() and os.access(str(go_bin), os.X_OK))
    if not needs_build:
        bin_mtime = go_bin.stat().st_mtime
        needs_build = any(
            source.is_file() and source.stat().st_mtime > bin_mtime for source in sources
        )
    if not needs_build:
        return str(go_bin.resolve())
    go = shutil.which("go")
    if go is None:
        raise RuntimeError(
            "Go is required to build go_dispatch; prepare the runtime environment first"
        )
    go_bin.parent.mkdir(parents=True, exist_ok=True)
    build = run_finite(
        [go, "build", "-trimpath", "-o", str(go_bin), "."],
        timeout_s=300.0,
        cwd=str(go_dir),
    )
    if build.returncode != 0:
        raise RuntimeError(f"Failed to build go_dispatch:\n{build.stdout}\n{build.stderr}")
    if not go_bin.is_file():
        raise RuntimeError("go_dispatch build completed but binary was not found")
    return str(go_bin.resolve())


_CPP_SERVER_SOURCES = (
    "main.cpp",
    "server.cpp",
    "server.hpp",
    "handler.cpp",
    "handler.hpp",
    "faults.cpp",
    "faults.hpp",
    "metrics.cpp",
    "metrics.hpp",
    "config.cpp",
    "config.hpp",
    "vendor/yyjson.c",
    "vendor/yyjson.h",
)


def _head_local_cpp_source(cpp_dir: Path) -> Path:
    """Copy one content-addressed C++ source tree to allocation-head /tmp."""
    inventory = []
    digest = hashlib.sha256()
    for relative in _CPP_SERVER_SOURCES:
        source = cpp_dir / relative
        with regular_file_reader(source, binary=True) as handle:
            content = handle.read()
        inventory.append((relative, content))
        digest.update(relative.encode("utf-8") + b"\0" + content)
    private_root = Path("/tmp") / f"clientlab-cpp-{os.getuid()}"
    private_root.mkdir(mode=0o700, exist_ok=True)
    root_metadata = os.lstat(private_root)
    if (
        not stat.S_ISDIR(root_metadata.st_mode)
        or stat.S_ISLNK(root_metadata.st_mode)
        or root_metadata.st_uid != os.getuid()
        or stat.S_IMODE(root_metadata.st_mode) & 0o077
    ):
        raise RuntimeError("ClientLab head-local build root is not a private real directory")
    build_dir = private_root / digest.hexdigest()
    build_dir.mkdir(mode=0o700, exist_ok=True)
    build_metadata = os.lstat(build_dir)
    if (
        not stat.S_ISDIR(build_metadata.st_mode)
        or stat.S_ISLNK(build_metadata.st_mode)
        or build_metadata.st_uid != os.getuid()
    ):
        raise RuntimeError("ClientLab content-addressed build directory is unsafe")
    for relative, content in inventory:
        destination = build_dir / relative
        destination.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        atomic_create_or_verify_bytes(destination, content)
    return build_dir


def ensure_cpp_server(*, head_local: bool = False):
    from exaserve.control.finite_process import run_finite

    shared_cpp_dir = Path(__file__).resolve().parents[1] / "targets" / "cpp_server"
    cpp_dir = _head_local_cpp_source(shared_cpp_dir) if head_local else shared_cpp_dir
    cpp_bin = cpp_dir / "bin" / "synthetic_server"
    sources = [cpp_dir / relative for relative in _CPP_SERVER_SOURCES]
    needs_build = not (cpp_bin.is_file() and os.access(str(cpp_bin), os.X_OK))
    if not needs_build:
        bin_mtime = cpp_bin.stat().st_mtime
        needs_build = any(
            source.is_file() and source.stat().st_mtime > bin_mtime for source in sources
        )
    if not needs_build:
        return str(cpp_bin.resolve())
    compiler_name = os.environ.get("CXX", "g++")
    if not compiler_name or "\x00" in compiler_name:
        raise RuntimeError("CXX must name one compiler executable")
    compiler = shutil.which(compiler_name)
    if compiler is None:
        raise RuntimeError(f"C++ compiler is unavailable: {compiler_name!r}")
    if head_local and any(
        Path(compiler).resolve().is_relative_to(root) for root in (Path("/home"), Path("/lus"))
    ):
        raise RuntimeError("PBS synthetic target compiler must not resolve on shared storage")
    cpp_bin.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    build_output = cpp_bin.with_name(f".{cpp_bin.name}.{os.getpid()}.{time.time_ns()}")
    build = run_finite(
        [
            compiler,
            "-std=c++20",
            "-O2",
            "-Wall",
            "-Wextra",
            "-pthread",
            "-o",
            str(build_output),
            "main.cpp",
            "server.cpp",
            "handler.cpp",
            "faults.cpp",
            "metrics.cpp",
            "config.cpp",
            "vendor/yyjson.c",
        ],
        timeout_s=300.0,
        cwd=str(cpp_dir),
    )
    if build.returncode != 0:
        try:
            build_output.unlink()
        except FileNotFoundError:
            pass
        raise RuntimeError(f"Failed to build cpp synthetic_server:\n{build.stdout}\n{build.stderr}")
    if not build_output.is_file():
        raise RuntimeError("cpp synthetic_server build completed but binary was not found")
    build_output.chmod(0o700)
    os.replace(build_output, cpp_bin)
    return str(cpp_bin.resolve())


def load_summary_result(path, *, expected_request_ids, sum_only):
    """Validate one complete Go result shard and return aggregate counts."""

    try:
        stream = read_go_result_stream(path)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"client result shard is missing or invalid: {path}: {exc}") from exc
    expected = tuple(expected_request_ids)
    if len(set(expected)) != len(expected):
        raise RuntimeError("planned ClientLab request IDs are duplicated")
    if stream.sum_only != bool(sum_only):
        expected_mode = "summary" if sum_only else "per-request"
        raise RuntimeError(f"client result shard did not use planned {expected_mode} mode")
    if stream.sum_only:
        summary = stream.terminal
        expected_count = len(expected)
        if (
            summary["requests_scheduled"] != expected_count
            or summary["requests_completed"] != expected_count
        ):
            raise RuntimeError(
                "client summary is incomplete: "
                f"scheduled={summary['requests_scheduled']} "
                f"completed={summary['requests_completed']} expected={expected_count}"
            )
        return summary

    observed = [record["req_id"] for record in stream.records]
    if set(observed) != set(expected) or len(observed) != len(expected):
        missing = sorted(set(expected) - set(observed))
        unexpected = sorted(set(observed) - set(expected))
        raise RuntimeError(
            f"client result shard request identity mismatch; missing={missing}, unexpected={unexpected}"
        )
    latencies = sorted(float(record["latency"]) for record in stream.records)

    def percentile(fraction):
        if not latencies:
            return 0.0
        index = min(len(latencies) - 1, max(0, math.ceil(fraction * len(latencies)) - 1))
        return latencies[index]

    return {
        "requests_completed": len(stream.records),
        "requests_scheduled": len(expected),
        "errors": sum(not record["success"] for record in stream.records),
        "p50_s": percentile(0.50),
        "p99_s": percentile(0.99),
        "total_input_tokens": sum(
            int(record["actual_prompt_tokens"] or 0) for record in stream.records
        ),
        "total_output_tokens": sum(
            int(record["actual_completion_tokens"] or 0) for record in stream.records
        ),
    }


def merge_summary_results(results):
    if not results:
        raise RuntimeError("client summary merge requires at least one result")
    merged = {
        "requests_completed": 0,
        "requests_scheduled": 0,
        "errors": 0,
        "p50_s": 0.0,
        "p99_s": 0.0,
        "total_input_tokens": 0,
        "total_output_tokens": 0,
    }
    latency_histogram = None
    for index, result in enumerate(results):
        label = f"client summary {index}"
        merged["requests_completed"] += _contract_count(result, "requests_completed", label=label)
        merged["requests_scheduled"] += _contract_count(result, "requests_scheduled", label=label)
        merged["errors"] += _contract_count(result, "errors", label=label)
        _contract_number(result, "p50_s", label=label)
        _contract_number(result, "p99_s", label=label)
        histogram = result.get("latency_histogram")
        method = result.get("latency_quantile_method")
        if len(results) > 1 and not isinstance(histogram, dict):
            raise RuntimeError("multi-process client summary lacks latency_histogram")
        if isinstance(histogram, dict) and method != LATENCY_QUANTILE_METHOD:
            raise RuntimeError("client summary has an unsupported latency quantile method")
        if isinstance(histogram, dict):
            latency_histogram = merge_histograms(
                {"latency": latency_histogram} if latency_histogram else {},
                {"latency": histogram},
            )["latency"]
        merged["total_input_tokens"] += _contract_count(result, "total_input_tokens", label=label)
        merged["total_output_tokens"] += _contract_count(result, "total_output_tokens", label=label)
    if len(results) == 1 and latency_histogram is None:
        merged["p50_s"] = _contract_number(results[0], "p50_s", label="client summary 0")
        merged["p99_s"] = _contract_number(results[0], "p99_s", label="client summary 0")
    else:
        assert latency_histogram is not None
        expected_successes = merged["requests_completed"] - merged["errors"]
        if latency_histogram["count"] != expected_successes:
            raise RuntimeError("client summary histogram count disagrees with successful requests")
        merged["latency_histogram"] = latency_histogram
        merged["p50_s"] = _percentile_from_histogram(latency_histogram, 0.50)
        merged["p99_s"] = _percentile_from_histogram(latency_histogram, 0.99)
        merged["latency_quantile_method"] = LATENCY_QUANTILE_METHOD
    return merged


def merge_metrics_json(metrics_list):
    if not metrics_list:
        return {}
    for index, metrics in enumerate(metrics_list):
        _validate_metrics_payload(metrics, label=f"client metrics {index}")
    merged = copy.deepcopy(metrics_list[0])
    for metrics in metrics_list[1:]:
        for key in (
            "requests_loaded",
            "requests_scheduled",
            "requests_completed",
            "requests_succeeded",
            "requests_failed",
            "new_connections",
            "reused_connections",
            "reused_idle_connections",
        ):
            merged[key] = _contract_count(merged, key) + _contract_count(metrics, key)
        for key in (
            "max_observed_active",
            "max_observed_outstanding",
            "max_observed_queue_depth",
            "last_request_start_at",
            "last_body_done_at",
            "completed_at",
        ):
            merged[key] = max(_contract_number(merged, key), _contract_number(metrics, key))
        merged["status_counts"] = merge_count_maps(
            merged.get("status_counts", {}), metrics.get("status_counts", {})
        )
        merged["error_counts"] = merge_count_maps(
            merged.get("error_counts", {}), metrics.get("error_counts", {})
        )
        merged["histograms"] = merge_histograms(
            merged.get("histograms", {}), metrics.get("histograms", {})
        )
        merged["per_target"] = merge_per_target(
            merged.get("per_target", {}), metrics.get("per_target", {})
        )
    return merged


def _validate_metrics_payload(metrics, *, label):
    _contract_mapping(metrics, label=label)
    for key in (
        "requests_loaded",
        "requests_scheduled",
        "requests_completed",
        "requests_succeeded",
        "requests_failed",
        "new_connections",
        "reused_connections",
        "reused_idle_connections",
        "max_observed_active",
        "max_observed_outstanding",
        "max_observed_queue_depth",
    ):
        _contract_count(metrics, key, label=label)
    for key in ("last_request_start_at", "last_body_done_at", "completed_at"):
        _contract_number(metrics, key, label=label)
    merge_count_maps({}, metrics.get("status_counts", {}))
    merge_count_maps({}, metrics.get("error_counts", {}))
    merge_histograms({}, metrics.get("histograms", {}))
    merge_per_target({}, metrics.get("per_target", {}))


def merge_histograms(left, right):
    left = _contract_mapping(left, label="left histograms")
    right = _contract_mapping(right, label="right histograms")
    output = copy.deepcopy(left)
    for name, hist in right.items():
        if not isinstance(name, str) or not name:
            raise RuntimeError("histogram names must be nonempty strings")
        _validate_histogram(hist, label=f"histogram {name}")
        if name not in output:
            output[name] = copy.deepcopy(hist)
            continue
        _validate_histogram(output[name], label=f"histogram {name}")
        left_bounds = output[name].get("bucket_upper_bounds_s", [])
        right_bounds = hist.get("bucket_upper_bounds_s", [])
        left_counts = output[name].get("counts", [])
        right_counts = hist.get("counts", [])
        if left_bounds != right_bounds or len(left_counts) != len(right_counts):
            raise RuntimeError(f"histogram {name} bucket layouts disagree")
        output[name]["count"] = _contract_count(output[name], "count") + _contract_count(
            hist, "count"
        )
        output[name]["sum_s"] = _contract_number(output[name], "sum_s") + _contract_number(
            hist, "sum_s"
        )
        output[name]["counts"] = [a + b for a, b in zip(left_counts, right_counts)]
    return output


def _validate_histogram(hist, *, label):
    _contract_mapping(hist, label=label)
    _contract_count(hist, "count", label=label)
    _contract_number(hist, "sum_s", label=label)
    counts = hist.get("counts", [])
    bounds = hist.get("bucket_upper_bounds_s", [])
    if not isinstance(counts, list) or any(type(value) is not int or value < 0 for value in counts):
        raise RuntimeError(f"{label}.counts must contain nonnegative integers")
    if not isinstance(bounds, list) or any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        for value in bounds
    ):
        raise RuntimeError(f"{label}.bucket_upper_bounds_s must contain finite numbers")
    if len(counts) != len(bounds):
        raise RuntimeError(f"{label} bucket counts and bounds disagree")
    if sum(counts) != hist["count"] or len(bounds) < 2 or bounds[-1] != -1.0:
        raise RuntimeError(f"{label} count disagrees with buckets")
    previous = -1.0
    for index, bound in enumerate(bounds):
        if index == len(bounds) - 1 and bound == -1.0:
            continue
        if bound < 0 or (index and bound <= previous):
            raise RuntimeError(f"{label} bucket bounds are not ordered")
        previous = bound


def _percentile_from_histogram(hist, fraction):
    _validate_histogram(hist, label="percentile histogram")
    count = hist["count"]
    if count == 0:
        return 0.0
    threshold = max(1, math.ceil(count * fraction))
    cumulative = 0
    for index, bucket_count in enumerate(hist["counts"]):
        cumulative += bucket_count
        if cumulative < threshold:
            continue
        upper = float(hist["bucket_upper_bounds_s"][index])
        if upper < 0:
            if index == 0:
                raise RuntimeError("percentile histogram overflow has no finite lower bound")
            return float(hist["bucket_upper_bounds_s"][index - 1])
        lower = float(hist["bucket_upper_bounds_s"][index - 1]) if index else 0.0
        prior = cumulative - bucket_count
        if bucket_count == 0:
            return upper
        return lower + ((threshold - prior) / bucket_count) * (upper - lower)
    raise RuntimeError("percentile histogram count exceeds bucket coverage")


def merge_per_target(left, right):
    left = _contract_mapping(left, label="left per_target")
    right = _contract_mapping(right, label="right per_target")
    output = copy.deepcopy(left)
    for target, payload in right.items():
        if not isinstance(target, str) or not target:
            raise RuntimeError("per_target names must be nonempty strings")
        _validate_per_target(payload, label=f"per_target {target}")
        if target not in output:
            output[target] = copy.deepcopy(payload)
            continue
        _validate_per_target(output[target], label=f"per_target {target}")
        for key in (
            "requests",
            "successes",
            "failures",
            "new_connections",
            "reused_connections",
            "reused_idle_connections",
        ):
            output[target][key] = _contract_count(output[target], key) + _contract_count(
                payload, key
            )
        output[target]["status_counts"] = merge_count_maps(
            output[target].get("status_counts", {}), payload.get("status_counts", {})
        )
        output[target]["error_counts"] = merge_count_maps(
            output[target].get("error_counts", {}), payload.get("error_counts", {})
        )
    return output


def _validate_per_target(payload, *, label):
    _contract_mapping(payload, label=label)
    for key in (
        "requests",
        "successes",
        "failures",
        "new_connections",
        "reused_connections",
        "reused_idle_connections",
    ):
        _contract_count(payload, key, label=label)
    merge_count_maps({}, payload.get("status_counts", {}))
    merge_count_maps({}, payload.get("error_counts", {}))


def merge_count_maps(left, right):
    left = _contract_mapping(left, label="left count map")
    right = _contract_mapping(right, label="right count map")
    output = {}
    for key, value in left.items():
        if not isinstance(key, str) or type(value) is not int or value < 0:
            raise RuntimeError("count maps must map text keys to nonnegative integers")
        output[key] = value
    for key, value in right.items():
        if not isinstance(key, str) or type(value) is not int or value < 0:
            raise RuntimeError("count maps must map text keys to nonnegative integers")
        output[key] = output.get(key, 0) + value
    return output


def merge_phase_traces(paths, output_path):
    parts = []
    for path in paths:
        try:
            with regular_file_reader(path) as handle:
                parts.append(handle.read())
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"phase trace shard is missing or invalid: {path}: {exc}") from exc
    atomic_write_text(output_path, "".join(parts))


def prefix_output(prefix, text):
    return "".join(f"[{prefix}] {line}\n" for line in text.splitlines())


def launch_targets(run_config, point_dir):
    target_type = run_config["target"]["type"]
    if target_type == "exaserve":
        from clientlab.targets.exaserve_target import wait_until_ready

        canonical = run_config["canonical_run"]
        target = wait_until_ready(
            canonical["deployment_status_dir"],
            timeout_s=float(run_config["client"].get("timeout_s", 3600.0)),
            expected_generation=int(canonical["expected_generation"]),
            expected_plan_hash=canonical["deployment_plan_hash"],
            expected_run_semantic_hash=canonical["run_semantic_hash"],
        )
        if target.deployment_id != canonical["deployment_id"]:
            raise RuntimeError("READY deployment identity disagrees with canonical RunPlan")
        return [
            {
                "base_url": target.base_url,
                "component": None,
                "supervisor": None,
                "remote": True,
                "deployment_target": target,
            }
        ]
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
    repo_root = str(Path(__file__).resolve().parents[2])
    child_env = dict(os.environ)
    child_env["PYTHONNOUSERSITE"] = "1"
    child_env["PYTHONPATH"] = os.pathsep.join(
        [repo_root, os.path.join(repo_root, "src"), child_env.get("PYTHONPATH", "")]
    )
    try:
        for idx in range(count):
            target_config = copy.deepcopy(run_config)
            target_config["target"]["host"] = host
            target_config["target"]["port"] = base_port + idx
            config_path = point_dir / f"target_{idx}.json"
            atomic_create_or_verify_json(config_path, target_config)
            stdout_log = (point_dir / f"target_{idx}.stdout.log").open("w", encoding="utf-8")
            supervisor = RuntimeSupervisor(poll_interval_s=0.1)
            handle = None
            try:
                component = supervisor.register(
                    ManagedComponent(
                        component_id=f"clientlab-target/{idx}",
                        argv=[
                            sys.executable,
                            "-m",
                            "clientlab.targets.synthetic_target",
                            "--config",
                            str(config_path),
                            "--binary",
                            server_cmd_prefix[0],
                        ],
                        env=child_env,
                        cwd=repo_root,
                        stdout=stdout_log,
                        long_lived=True,
                    )
                )
                supervisor.start_all(rollback_s=5.0)
                base_url = f"http://{host}:{base_port + idx}"
                handle = {
                    "base_url": base_url,
                    "component": component,
                    "supervisor": supervisor,
                    "stdout_log": stdout_log,
                    "remote": False,
                }
                handles.append(handle)
                wait_for_health(base_url)
            except BaseException as exc:
                if handle is None:
                    try:
                        if not supervisor.shutdown(drain_s=5.0):
                            raise RuntimeError(str(supervisor.first_cause or "cleanup incomplete"))
                    except BaseException as cleanup_exc:
                        add_exception_note(exc, f"synthetic target rollback failed: {cleanup_exc}")
                    try:
                        stdout_log.close()
                    except OSError as cleanup_exc:
                        add_exception_note(exc, f"synthetic target log close failed: {cleanup_exc}")
                raise
        return handles
    except BaseException as exc:
        try:
            stop_targets(handles)
        except BaseException as cleanup_exc:
            add_exception_note(exc, f"previous synthetic target cleanup failed: {cleanup_exc}")
        raise


def launch_pbs_synthetic_targets(run_config, point_dir):
    nodes = validate_pbs_session()
    client_nodes = int(run_config["execution"].get("client_nodes", 1))
    if client_nodes != 1:
        # Eval's MPI replay transport is bound to an EvalManifest, trace
        # partitions, and its own exact result-completeness protocol. A
        # ClientLab point drives adaptive/saturation steps and has no matching
        # distributed point contract yet. Launching one copy per node here
        # would therefore duplicate the control loop and mislabel its result.
        raise RuntimeError(
            "unsupported feature: ClientLab PBS execution currently supports exactly one client node; "
            "execution.client_nodes>1 would otherwise claim a distributed client "
            "while running every Go process on the allocation head"
        )
    synthetic_nodes = int(run_config["target"].get("synthetic_nodes", 1))
    if len(nodes) < client_nodes + synthetic_nodes:
        raise RuntimeError(
            "PBS allocation does not provide enough nodes for the requested synthetic target count"
        )
    server_binary = ensure_cpp_server(head_local=True)
    handles = []
    base_port = int(run_config["target"].get("port", 18100))
    if base_port < 1 or base_port + synthetic_nodes - 1 > 65535:
        raise RuntimeError("rank-adjusted synthetic target ports must remain in 1..65535")
    target_payload = copy.deepcopy(run_config)
    target_payload["target"]["host"] = "0.0.0.0"
    target_payload["target"]["port"] = base_port
    target_fields = ("host", "port", "response_tokens")
    client_fields = ("model", "prompt_words", "max_active_requests")
    fault_fields = (
        "max_inflight",
        "max_queue",
        "queue_delay_ms",
        "error_rate",
        "error_status",
        "reject_status",
        "close_after_response",
        "reset_after_response",
        "idle_timeout_s",
        "burst_every",
        "burst_duration",
    )
    projected_faults = {
        name: target_payload["faults"][name]
        for name in fault_fields
        if name in target_payload["faults"]
    }
    if "service_time" in target_payload["faults"]:
        projected_faults["service_time"] = {
            name: target_payload["faults"]["service_time"][name]
            for name in ("distribution", "value_ms", "stddev_ms")
            if name in target_payload["faults"]["service_time"]
        }
    inline_config = json.dumps(
        {
            "target": {
                name: target_payload["target"][name]
                for name in target_fields
                if name in target_payload["target"]
            },
            "client": {
                name: target_payload["client"][name]
                for name in client_fields
                if name in target_payload["client"]
            },
            "faults": projected_faults,
        },
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )
    if len(inline_config.encode("utf-8")) > 64 << 10 or "\x00" in inline_config:
        raise RuntimeError("synthetic target inline config exceeds its 64 KiB argv bound")
    stdout_log = (point_dir / "targets.stdout.log").open("w", encoding="utf-8")
    supervisor = RuntimeSupervisor(poll_interval_s=0.1)
    hostfile = None
    try:
        for idx, node in enumerate(nodes[client_nodes : client_nodes + synthetic_nodes]):
            base_url = f"http://{resolve_hsn_host(node)}:{base_port + idx}"
            handles.append(
                {
                    "base_url": base_url,
                    "node": node,
                    "remote": True,
                    "supervisor": supervisor,
                    "stdout_log": stdout_log if idx == 0 else None,
                }
            )
        hostfile_fd, hostfile_name = tempfile.mkstemp(
            prefix="clientlab-synthetic-hosts-", suffix=".txt", dir="/tmp"
        )
        hostfile = Path(hostfile_name)
        with os.fdopen(hostfile_fd, "w", encoding="utf-8") as handle:
            handle.write(
                "".join(
                    f"{node}\n" for node in nodes[client_nodes : client_nodes + synthetic_nodes]
                )
            )
            handle.flush()
            os.fsync(handle.fileno())
        env = dict(os.environ)
        env["PYTHONNOUSERSITE"] = "1"
        from exaserve.site import AURORA_PMIX_PREPARED_ENVIRONMENT

        pmix_argv = [
            item
            for name, value in AURORA_PMIX_PREPARED_ENVIRONMENT
            for item in ("--genv", f"{name}={value}")
        ]
        component = supervisor.register(
            ManagedComponent(
                component_id="clientlab-targets/mpi",
                argv=[
                    "mpiexec",
                    "--transfer",
                    "--genvnone",
                    "--envnone",
                    "--genv",
                    "PYTHONNOUSERSITE=1",
                    "--genv",
                    "HOME=/tmp",
                    "--genv",
                    "TMPDIR=/tmp",
                    *pmix_argv,
                    "--abort-on-failure",
                    "-n",
                    str(synthetic_nodes),
                    "--ppn",
                    "1",
                    "--cpu-bind",
                    "none",
                    "--hostfile",
                    str(hostfile),
                    "--wdir",
                    "/tmp",
                    server_binary,
                    "--config-json",
                    inline_config,
                    "--rank-port-offset",
                    "--require-aurora-local-runtime",
                ],
                env=env,
                cwd="/tmp",
                stdout=stdout_log,
                long_lived=True,
            )
        )
        supervisor.start_all(rollback_s=10.0)
        for handle in handles:
            handle["component"] = component
            handle["head_local_artifacts"] = [str(hostfile)]
            wait_for_health(handle["base_url"], timeout_s=30.0)
        return handles
    except BaseException as exc:
        try:
            stop_targets(handles)
        except BaseException as cleanup_exc:
            add_exception_note(exc, f"PBS synthetic target cleanup failed: {cleanup_exc}")
        if not handles:
            try:
                if not supervisor.shutdown(drain_s=10.0):
                    raise RuntimeError(str(supervisor.first_cause or "cleanup incomplete"))
            except BaseException as cleanup_exc:
                add_exception_note(exc, f"PBS target supervisor rollback failed: {cleanup_exc}")
            try:
                stdout_log.close()
            except OSError as cleanup_exc:
                add_exception_note(exc, f"PBS target log close failed: {cleanup_exc}")
        if hostfile is not None:
            try:
                hostfile.unlink()
            except FileNotFoundError:
                pass
            except OSError as cleanup_exc:
                add_exception_note(exc, f"PBS target hostfile cleanup failed: {cleanup_exc}")
        raise


def stop_targets(handles):
    supervisors = {
        id(handle.get("supervisor")): handle.get("supervisor")
        for handle in handles
        if handle.get("supervisor")
    }
    cleanup_errors = []
    cleanup_deadline = time.monotonic() + 10.0
    for supervisor in supervisors.values():
        try:
            if not supervisor.shutdown(deadline=cleanup_deadline):
                cleanup_errors.append(str(supervisor.first_cause or "cleanup incomplete"))
        except BaseException as exc:
            cleanup_errors.append(f"{type(exc).__name__}: {exc}")
    closed = set()
    local_artifacts = {
        path
        for handle in handles
        for path in handle.get("head_local_artifacts", ())
        if isinstance(path, str)
    }
    for handle in handles:
        log = handle.get("stdout_log")
        if log is not None and id(log) not in closed:
            closed.add(id(log))
            try:
                log.close()
            except OSError as exc:
                cleanup_errors.append(f"log close: {type(exc).__name__}: {exc}")
    for path in sorted(local_artifacts):
        try:
            Path(path).unlink()
        except FileNotFoundError:
            pass
        except OSError as exc:
            cleanup_errors.append(f"local launch artifact cleanup: {type(exc).__name__}: {exc}")
    if cleanup_errors:
        raise RuntimeError("ClientLab target cleanup failed: " + "; ".join(cleanup_errors))


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
            with build_opener(ProxyHandler({})).open(
                f"{base_url}/metrics", timeout=3.0
            ) as response:
                payload = strict_json_loads(response.read().decode("utf-8"))
        except URLError:
            payload = {"error": "unreachable", "base_url": base_url}
        if not isinstance(payload, dict):
            raise RuntimeError(f"target metrics from {base_url} must be a JSON object")
        payload["base_url"] = base_url
        targets.append(payload)
        if "total_requests" in payload:
            label = f"target metrics from {base_url}"
            aggregate["total_requests"] += _contract_count(payload, "total_requests", label=label)
            aggregate["accepted"] += _contract_count(payload, "accepted", label=label)
            aggregate["completed"] += _contract_count(payload, "completed", label=label)
            aggregate["rejections"] += _contract_count(payload, "rejections", label=label)
            aggregate["errors"] += _contract_count(payload, "errors", label=label)
            aggregate["max_active"] = max(
                aggregate["max_active"], _contract_count(payload, "max_active", label=label)
            )
            aggregate["max_queue_depth"] = max(
                aggregate["max_queue_depth"],
                _contract_count(payload, "max_queue_depth", label=label),
            )
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
        raise RuntimeError(
            "ClientLab PBS execution requires a valid interactive PBS session (PBS_JOBID and PBS_NODEFILE)."
        )
    nodes = []
    try:
        with regular_file_reader(nodefile) as handle:
            for line in handle:
                node = line.strip()
                if node and node not in nodes:
                    nodes.append(node)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"PBS_NODEFILE is not a readable regular file: {exc}") from exc
    hostname = socket.gethostname().split(".")[0]
    if hostname not in {node.split(".")[0] for node in nodes}:
        raise RuntimeError(
            "Current hostname is not part of PBS_NODEFILE; refusing to assume a valid interactive session."
        )
    return nodes


def resolve_hsn_host(node):
    candidate = f"{node}.hsn.cm.aurora.alcf.anl.gov"
    try:
        socket.getaddrinfo(candidate, None)
        return candidate
    except socket.gaierror:
        return node


def summarize_netstats(path):
    try:
        with regular_file_reader(path) as handle:
            text = handle.read()
    except FileNotFoundError:
        return None
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"netstats artifact is not a readable regular file: {exc}") from exc
    if not text.strip():
        return None
    max_rx_drops = 0
    max_tx_drops = 0
    by_interface = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        payload = strict_json_loads(line)
        label = "netstats sample"
        _contract_mapping(payload, label=label)
        interface = payload.get("interface")
        hostname = payload.get("hostname")
        if not isinstance(interface, str) or not interface:
            raise RuntimeError("netstats sample.interface must be nonempty text")
        if not isinstance(hostname, str) or not hostname:
            raise RuntimeError("netstats sample.hostname must be nonempty text")
        _contract_number(payload, "timestamp", label=label)
        for key in (
            "rx_bytes",
            "rx_packets",
            "rx_errors",
            "rx_drops",
            "tx_bytes",
            "tx_packets",
            "tx_errors",
            "tx_drops",
        ):
            _contract_count(payload, key, label=label)
        by_interface.setdefault(interface, []).append(payload)
        max_rx_drops = max(max_rx_drops, payload["rx_drops"])
        max_tx_drops = max(max_tx_drops, payload["tx_drops"])
    max_bandwidth_fraction = 0.0
    for records in by_interface.values():
        records = sorted(records, key=lambda row: row["timestamp"])
        for prev, cur in zip(records, records[1:]):
            delta_t = max(cur["timestamp"] - prev["timestamp"], 1e-9)
            tx_gbs = max(cur["tx_bytes"] - prev["tx_bytes"], 0) / delta_t / 1e9
            rx_gbs = max(cur["rx_bytes"] - prev["rx_bytes"], 0) / delta_t / 1e9
            max_bandwidth_fraction = max(max_bandwidth_fraction, tx_gbs / 25.0, rx_gbs / 25.0)
    return {
        "max_rx_drops": max_rx_drops,
        "max_tx_drops": max_tx_drops,
        "max_bandwidth_fraction": max_bandwidth_fraction,
    }
