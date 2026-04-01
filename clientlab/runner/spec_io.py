import copy
import os
from pathlib import Path
from typing import Any, Dict, List

from clientlab import SCHEMA_VERSION
from clientlab.utils import load_yaml_file, stable_hash


DEFAULT_SPEC = {
    "schema_version": SCHEMA_VERSION,
    "study": {
        "name": "clientlab-study",
        "suite": "client_microbench",
        "repeats": 1,
        "description": "",
    },
    "matrix": {
        "axes": [],
    },
    "client": {
        "duration_s": 5.0,
        "rate": 100.0,
        "prompt_words": 32,
        "output_tokens": 32,
        "mode": "chat",
        "model": "stub-model",
        "generation_mode": "deterministic",
        "timeout_s": 3600.0,
        "num_go_workers": 2,
        "num_go_procs": 1,
        "max_active_requests": 32,
        "queue_capacity": 0,
        "max_conns_per_host": 0,
        "enable_httptrace": True,
        "phase_trace_sample_rate": 0.0,
        "sum_only": True,
    },
    "target": {
        "type": "synthetic",
        "mode": "echo_fast",
        "host": "127.0.0.1",
        "port": 18100,
        "base_urls": [],
        "synthetic_nodes": 1,
        "response_tokens": 32,
    },
    "faults": {
        "service_time": {"distribution": "fixed", "value_ms": 0.0, "stddev_ms": 0.0},
        "max_inflight": 0,
        "max_queue": 0,
        "queue_delay_ms": 0.0,
        "error_rate": 0.0,
        "error_status": 500,
        "reject_status": 429,
        "close_after_response": False,
        "reset_after_response": False,
        "idle_timeout_s": 0.0,
        "burst_every": 0,
        "burst_duration": 0,
    },
    "collectors": {
        "port_monitor": True,
        "port_monitor_interval_s": 1.0,
        "netstats": False,
        "netstats_interval_s": 1.0,
        "netstats_interfaces": "",
        "target_metrics": True,
    },
    "execution": {
        "mode": "local",
        "python": "",
        "env_script": "",
        "client_nodes": 1,
    },
    "reporting": {
        "generate_markdown": True,
        "generate_plots": True,
    },
}

EXPECTED_POINT_ARTIFACTS = [
    "run_config.yaml",
    "trace.jsonl",
    "client_metrics.json",
    "phase_trace.jsonl",
    "target_metrics.json",
    "port_metrics.json",
    "netstats.jsonl",
    "stdout.log",
    "stderr.log",
    "profiles/",
    "derived_features.json",
    "diagnosis.json",
]


def resolve_spec_path(spec_ref):
    candidate = Path(spec_ref).expanduser()
    if candidate.is_file():
        return str(candidate.resolve())
    specs_dir = Path(__file__).resolve().parents[1] / "specs"
    for suffix in (".yaml", ".json"):
        builtin = specs_dir / f"{spec_ref}{suffix}"
        if builtin.is_file():
            return str(builtin)
    raise FileNotFoundError(f"Could not resolve ClientLab spec: {spec_ref}")


def deep_merge(base, override):
    merged = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def load_study_spec(spec_ref):
    spec_path = resolve_spec_path(spec_ref)
    payload = load_yaml_file(spec_path)
    spec = deep_merge(DEFAULT_SPEC, payload)
    spec["schema_version"] = SCHEMA_VERSION
    spec["_spec_path"] = spec_path
    spec["_spec_dir"] = os.path.dirname(spec_path)
    validate_spec(spec)
    return spec


def validate_spec(spec):
    for key in ("study", "matrix", "client", "target", "faults", "collectors", "execution", "reporting"):
        if key not in spec or not isinstance(spec[key], dict):
            raise ValueError(f"ClientLab spec missing mapping section: {key}")
    if int(spec["study"].get("repeats", 1)) < 1:
        raise ValueError("study.repeats must be >= 1")
    if float(spec["client"].get("duration_s", 0)) <= 0:
        raise ValueError("client.duration_s must be > 0")
    if float(spec["client"].get("rate", 0)) <= 0:
        raise ValueError("client.rate must be > 0")
    if int(spec["client"].get("max_active_requests", 0)) < 1:
        raise ValueError("client.max_active_requests must be >= 1")
    if float(spec["client"].get("phase_trace_sample_rate", 0.0)) < 0 or float(spec["client"].get("phase_trace_sample_rate", 0.0)) > 1:
        raise ValueError("client.phase_trace_sample_rate must be in [0, 1]")
    if spec["target"].get("type") not in {"synthetic", "proxy", "external"}:
        raise ValueError("target.type must be synthetic, proxy, or external")
    if spec["execution"].get("mode") not in {"local", "pbs_interactive"}:
        raise ValueError("execution.mode must be local or pbs_interactive")
    axes = spec["matrix"].get("axes", [])
    if not isinstance(axes, list):
        raise ValueError("matrix.axes must be a list")
    for axis in axes:
        if not isinstance(axis, dict):
            raise ValueError("matrix.axes entries must be mappings")
        if "name" not in axis or "path" not in axis or "values" not in axis:
            raise ValueError("matrix axis must contain name, path, and values")
        if not isinstance(axis["values"], list) or not axis["values"]:
            raise ValueError(f"matrix axis {axis['name']} must define a non-empty values list")


def dotted_get(mapping, dotted_path):
    current = mapping
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            raise KeyError(f"Unknown dotted path: {dotted_path}")
        current = current[part]
    return current


def dotted_set(mapping, dotted_path, value):
    current = mapping
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value


def expand_matrix(spec):
    repeats = int(spec["study"].get("repeats", 1))
    axes = spec["matrix"].get("axes", [])
    base = copy.deepcopy(spec)
    base["matrix"]["axes"] = []
    points = [base]
    axis_values_list = [{}]

    for axis in axes:
        next_points = []
        next_axis_values = []
        for point, axis_values in zip(points, axis_values_list):
            for value in axis["values"]:
                point_copy = copy.deepcopy(point)
                dotted_set(point_copy, axis["path"], value)
                axis_copy = dict(axis_values)
                axis_copy[axis["name"]] = value
                next_points.append(point_copy)
                next_axis_values.append(axis_copy)
        points = next_points
        axis_values_list = next_axis_values

    expanded = []
    for point, axis_values in zip(points, axis_values_list):
        for repeat_idx in range(repeats):
            point_copy = copy.deepcopy(point)
            point_copy["_axis_values"] = axis_values
            point_copy["_repeat"] = repeat_idx
            point_copy["_point_id"] = build_point_id(point_copy)
            expanded.append(point_copy)
    return expanded


def build_point_id(point):
    study_name = str(point["study"]["name"])
    axis_values = point.get("_axis_values", {})
    fragments = [study_name]
    for key in sorted(axis_values):
        fragments.append(f"{key}-{axis_values[key]}")
    fragments.append(f"repeat-{int(point.get('_repeat', 0))}")
    return stable_hash({"fragments": fragments}, length=10)
