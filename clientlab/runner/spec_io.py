import copy
import math
import os
from pathlib import Path

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
        "streaming": False,
        "saturation": {
            "enabled": False,
            "search_mode": "binary",
            "initial_rate": 100,
            "max_rate": 0,
            "step_duration_s": 10.0,
            "warmup_duration_s": 3.0,
            "cooldown_pause_s": 2.0,
            "tolerance": 0.05,
            "max_error_rate": 0.01,
            "plateau_ratio": 0.95,
            "verify": True,
            "step_up_start": 0,
            "step_up_end": 0,
            "step_up_increment": 0,
        },
    },
    "target": {
        "type": "synthetic",
        "mode": "echo_fast",
        "host": "127.0.0.1",
        "port": 18100,
        "base_urls": [],
        "synthetic_nodes": 1,
        "response_tokens": 32,
        "run_plan_path": "",
        "trace_path": "",
        "deployment_status_dir": "",
        "expected_generation": 0,
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
    "saturation_output.json",
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
    if "schema_version" in payload and (
        type(payload["schema_version"]) is not str or payload["schema_version"] != SCHEMA_VERSION
    ):
        raise ValueError(f"unsupported ClientLab schema_version {payload['schema_version']!r}")
    _reject_unknown(payload, DEFAULT_SPEC)
    target_payload = payload.get("target", {})
    if target_payload.get("type") == "exaserve":
        # A real deployment study selects one already-compiled RunPlan.  A
        # second client/workload mapping here would be an un-hashed competing
        # source of truth, even if it happened to agree today.
        if payload.get("client"):
            raise ValueError(
                "exaserve ClientLab specs must not restate client/workload "
                "semantics; compile them into target.run_plan_path"
            )
        if payload.get("faults"):
            raise ValueError(
                "target fault injection is synthetic-only; real deployment "
                "fault policy belongs in the canonical plan"
            )
    spec = deep_merge(DEFAULT_SPEC, payload)
    spec["schema_version"] = SCHEMA_VERSION
    spec["_spec_path"] = spec_path
    spec["_spec_dir"] = os.path.dirname(spec_path)
    validate_spec(spec)
    if spec["target"]["type"] == "exaserve":
        _hydrate_exaserve_plan(spec)
        validate_spec(spec)
    return spec


def validate_spec(spec):
    _validate_types(spec, DEFAULT_SPEC)
    for key in (
        "study",
        "matrix",
        "client",
        "target",
        "faults",
        "collectors",
        "execution",
        "reporting",
    ):
        if key not in spec or not isinstance(spec[key], dict):
            raise ValueError(f"ClientLab spec missing mapping section: {key}")
    if int(spec["study"].get("repeats", 1)) < 1:
        raise ValueError("study.repeats must be >= 1")
    client = spec["client"]
    for name in ("prompt_words", "output_tokens", "num_go_workers", "num_go_procs"):
        if int(client[name]) < 1:
            raise ValueError(f"client.{name} must be >= 1")
    for name in ("queue_capacity", "max_conns_per_host"):
        if int(client[name]) < 0:
            raise ValueError(f"client.{name} must be >= 0")
    if float(client["timeout_s"]) <= 0:
        raise ValueError("client.timeout_s must be > 0")
    sat_enabled = spec["client"].get("saturation", {}).get("enabled", False)
    if not sat_enabled:
        if float(spec["client"].get("duration_s", 0)) <= 0:
            raise ValueError("client.duration_s must be > 0")
        if float(spec["client"].get("rate", 0)) <= 0:
            raise ValueError("client.rate must be > 0")
    minimum_active = 0 if spec["target"].get("type") == "exaserve" else 1
    if int(spec["client"].get("max_active_requests", 0)) < minimum_active:
        raise ValueError(f"client.max_active_requests must be >= {minimum_active}")
    if (
        float(spec["client"].get("phase_trace_sample_rate", 0.0)) < 0
        or float(spec["client"].get("phase_trace_sample_rate", 0.0)) > 1
    ):
        raise ValueError("client.phase_trace_sample_rate must be in [0, 1]")
    if sat_enabled:
        sat = spec["client"]["saturation"]
        if sat.get("search_mode") not in {"binary", "step-up"}:
            raise ValueError("client.saturation.search_mode must be 'binary' or 'step-up'")
        if float(sat.get("step_duration_s", 0)) <= 0:
            raise ValueError("client.saturation.step_duration_s must be > 0")
        if float(sat.get("warmup_duration_s", 0)) < 0:
            raise ValueError("client.saturation.warmup_duration_s must be >= 0")
        if float(sat.get("cooldown_pause_s", 0)) < 0:
            raise ValueError("client.saturation.cooldown_pause_s must be >= 0")
        tol = float(sat.get("tolerance", 0))
        if tol <= 0 or tol >= 1:
            raise ValueError("client.saturation.tolerance must be in (0, 1)")
        if int(sat.get("initial_rate", 0)) <= 0:
            raise ValueError("client.saturation.initial_rate must be > 0")
        if int(sat.get("max_rate", 0)) < 0:
            raise ValueError("client.saturation.max_rate must be >= 0")
        if 0 < int(sat.get("max_rate", 0)) <= int(sat.get("initial_rate", 0)):
            raise ValueError("client.saturation.max_rate must exceed initial_rate when specified")
        max_error_rate = float(sat.get("max_error_rate", 0))
        if max_error_rate < 0 or max_error_rate > 1:
            raise ValueError("client.saturation.max_error_rate must be in [0, 1]")
        plateau_ratio = float(sat.get("plateau_ratio", 0))
        if plateau_ratio <= 0 or plateau_ratio > 1:
            raise ValueError("client.saturation.plateau_ratio must be in (0, 1]")
        if sat.get("search_mode") == "step-up":
            start = int(sat.get("step_up_start", 0))
            end = int(sat.get("step_up_end", 0))
            increment = int(sat.get("step_up_increment", 0))
            if start <= 0 or end <= 0 or increment <= 0:
                raise ValueError(
                    "client.saturation.step-up requires positive step_up_start, step_up_end, and step_up_increment"
                )
            if end < start:
                raise ValueError("client.saturation.step_up_end must be >= step_up_start")
    target_type = spec["target"].get("type")
    if target_type not in {"synthetic", "exaserve"}:
        raise ValueError(
            "target.type must be synthetic or exaserve; arbitrary raw endpoints "
            "are not a release-grade deployment identity"
        )
    if target_type == "exaserve":
        for name in ("run_plan_path", "trace_path", "deployment_status_dir"):
            if not spec["target"].get(name):
                raise ValueError(f"target.{name} is required for exaserve")
        if spec["target"].get("expected_generation", 0) < 1:
            raise ValueError("target.expected_generation must be positive for exaserve")
    else:
        target = spec["target"]
        if not 1 <= int(target["port"]) <= 65535:
            raise ValueError("target.port must be in [1, 65535]")
        if int(target["synthetic_nodes"]) < 1:
            raise ValueError("target.synthetic_nodes must be >= 1")
        if int(target["response_tokens"]) < 0:
            raise ValueError("target.response_tokens must be >= 0")
    faults = spec["faults"]
    if faults["service_time"]["distribution"] not in {"fixed", "normal", "uniform"}:
        raise ValueError("faults.service_time.distribution is unsupported")
    for name in ("value_ms", "stddev_ms"):
        if float(faults["service_time"][name]) < 0:
            raise ValueError(f"faults.service_time.{name} must be >= 0")
    for name in (
        "max_inflight",
        "max_queue",
        "queue_delay_ms",
        "idle_timeout_s",
        "burst_every",
        "burst_duration",
    ):
        if float(faults[name]) < 0:
            raise ValueError(f"faults.{name} must be >= 0")
    if not 0 <= float(faults["error_rate"]) <= 1:
        raise ValueError("faults.error_rate must be in [0, 1]")
    for name in ("error_status", "reject_status"):
        if not 400 <= int(faults[name]) <= 599:
            raise ValueError(f"faults.{name} must be in [400, 599]")
    collectors = spec["collectors"]
    for name in ("port_monitor_interval_s", "netstats_interval_s"):
        if float(collectors[name]) <= 0:
            raise ValueError(f"collectors.{name} must be > 0")
    if spec["execution"].get("mode") not in {"local", "pbs_interactive"}:
        raise ValueError("execution.mode must be local or pbs_interactive")
    if int(spec["execution"]["client_nodes"]) < 1:
        raise ValueError("execution.client_nodes must be >= 1")
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
        if set(axis) != {"name", "path", "values"}:
            raise ValueError(f"matrix axis {axis.get('name')} has unknown fields")
        try:
            baseline = dotted_get(spec, axis["path"])
        except KeyError as exc:
            raise ValueError(str(exc)) from exc
        if any(not _compatible_value(item, baseline) for item in axis["values"]):
            raise ValueError(f"matrix axis {axis['name']} values disagree with {axis['path']} type")
        if target_type == "exaserve":
            raise ValueError(
                "an exaserve ClientLab study binds exactly one canonical "
                "RunPlan; materialize each semantic matrix point as a distinct "
                "RunPlan and study spec"
            )
    names = [axis["name"] for axis in axes]
    paths = [axis["path"] for axis in axes]
    if len(names) != len(set(names)) or len(paths) != len(set(paths)):
        raise ValueError("matrix axis names and paths must be unique")


def _resolved_input_path(spec, value, field):
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = Path(spec["_spec_dir"]) / candidate
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError(f"target.{field} is not a regular file: {candidate}")
    return str(candidate.resolve())


def _hydrate_exaserve_plan(spec):
    """Project one verified RunPlan into ClientLab's execution vocabulary.

    This is an adapter, not a compiler: the semantic values are overwritten
    from the already-finalized plan, and the exact trace bytes are verified
    against that plan before any point can be expanded or launched.
    """
    from exaserve.plan.io import load_run_plan
    from exaserve.state.results import file_sha256

    target = spec["target"]
    run_plan_path = _resolved_input_path(spec, target["run_plan_path"], "run_plan_path")
    trace_path = _resolved_input_path(spec, target["trace_path"], "trace_path")
    status_dir = Path(target["deployment_status_dir"]).expanduser()
    if not status_dir.is_absolute():
        status_dir = Path(spec["_spec_dir"]) / status_dir
    status_dir = status_dir.resolve()

    plan = load_run_plan(run_plan_path)
    declared_trace_hash = plan.trace.trace_content_hash
    if not declared_trace_hash:
        raise ValueError("ClientLab requires a materialized RunPlan with trace_content_hash")
    observed_trace_hash = file_sha256(trace_path)
    if observed_trace_hash != declared_trace_hash:
        raise ValueError("target.trace_path content disagrees with canonical RunPlan")
    if plan.client.startup_only:
        raise ValueError("startup-only RunPlans do not define a ClientLab workload")
    if plan.client.dispatch_topologies:
        raise ValueError(
            "ClientLab accepts one dispatch topology per RunPlan; materialize "
            "each ablation arm as a distinct canonical plan"
        )
    if plan.client.nodes != plan.workload.client_nodes:
        raise ValueError("RunPlan client.nodes and workload.client_nodes disagree")
    if plan.client.nodes != 1:
        raise ValueError(
            "ClientLab's canonical ExaServe adapter currently supports exactly "
            "one client node; use eval for distributed replay"
        )
    expected_destination = (
        "proxy" if plan.deployment.exposure.mode == "PROXIED_INTERNAL" else "direct"
    )
    if plan.client.destination != expected_destination:
        raise ValueError("RunPlan client destination disagrees with deployment exposure")

    options = dict(plan.client.options)
    allowed_options = {
        "clientlab_enable_httptrace",
        "clientlab_max_conns_per_host",
        "clientlab_phase_trace_sample_rate",
        "clientlab_queue_capacity",
        "clientlab_timeout_s",
        "clientlab_stall_timeout_s",
    }
    unknown = sorted(set(options) - allowed_options)
    if unknown:
        raise ValueError(f"RunPlan client.options contains unsupported ClientLab keys: {unknown}")
    option_types = {
        "clientlab_enable_httptrace": bool,
        "clientlab_max_conns_per_host": int,
        "clientlab_phase_trace_sample_rate": (int, float),
        "clientlab_queue_capacity": int,
        "clientlab_timeout_s": (int, float),
        "clientlab_stall_timeout_s": (int, float),
    }
    for name, value in options.items():
        expected = option_types[name]
        if (isinstance(value, bool) and expected is not bool) or not isinstance(value, expected):
            raise ValueError(f"RunPlan client.options.{name} has wrong type")
    for name in ("clientlab_max_conns_per_host", "clientlab_queue_capacity"):
        if int(options.get(name, 0)) < 0:
            raise ValueError(f"RunPlan client.options.{name} must be nonnegative")
    for name in ("clientlab_timeout_s", "clientlab_stall_timeout_s"):
        value = float(
            options.get(
                name,
                3600.0 if name.endswith("timeout_s") and name == "clientlab_timeout_s" else 120.0,
            )
        )
        if not math.isfinite(value) or value <= 0:
            raise ValueError(f"RunPlan client.options.{name} must be positive")
    sample_rate = float(options.get("clientlab_phase_trace_sample_rate", 0.0))
    if not math.isfinite(sample_rate) or not 0 <= sample_rate <= 1:
        raise ValueError(
            "RunPlan client.options.clientlab_phase_trace_sample_rate must be in [0, 1]"
        )
    model_ids = [model.model_id for model in plan.deployment.models]
    if plan.client.saturation.enabled and len(model_ids) != 1:
        raise ValueError("ClientLab saturation requires a one-model canonical deployment")

    saturation = plan.client.saturation
    spec["client"].update(
        {
            "duration_s": float(plan.workload.duration_s),
            "rate": float(plan.workload.rate_per_node * plan.deployment.num_nodes),
            "prompt_words": int(plan.workload.input_len),
            "output_tokens": int(plan.workload.output_len),
            "model": model_ids[0],
            "generation_mode": plan.workload.generation_mode,
            "timeout_s": float(options.get("clientlab_timeout_s", 3600.0)),
            "num_go_workers": int(plan.client.workers),
            "num_go_procs": int(plan.client.processes),
            "max_active_requests": int(plan.client.concurrency),
            "queue_capacity": int(options.get("clientlab_queue_capacity", 0)),
            "max_conns_per_host": int(
                options.get("clientlab_max_conns_per_host", plan.client.concurrency)
            ),
            "enable_httptrace": bool(options.get("clientlab_enable_httptrace", True)),
            "phase_trace_sample_rate": float(options.get("clientlab_phase_trace_sample_rate", 0.0)),
            "sum_only": bool(plan.client.sum_only),
            "streaming": bool(plan.client.streaming),
            "saturation": {
                "enabled": bool(saturation.enabled),
                "search_mode": saturation.search_mode.replace("_", "-"),
                "initial_rate": int(saturation.initial_rate),
                "max_rate": int(saturation.max_rate),
                "step_duration_s": float(saturation.step_duration_s),
                "warmup_duration_s": float(saturation.warmup_duration_s),
                "cooldown_pause_s": float(saturation.cooldown_pause_s),
                "tolerance": float(saturation.tolerance),
                "max_error_rate": float(saturation.max_error_rate),
                "plateau_ratio": float(saturation.plateau_ratio),
                "verify": bool(saturation.verify),
                "step_up_start": int(saturation.step_up_start),
                "step_up_end": int(saturation.step_up_end),
                "step_up_increment": int(saturation.step_up_increment),
            },
        }
    )
    spec["execution"]["client_nodes"] = plan.client.nodes
    target.update(
        {
            "run_plan_path": run_plan_path,
            "trace_path": trace_path,
            "deployment_status_dir": str(status_dir),
        }
    )
    spec["_canonical_run"] = {
        "run_id": plan.run_id,
        "run_semantic_hash": plan.run_semantic_hash,
        "deployment_id": plan.deployment.deployment_id,
        "deployment_plan_hash": plan.deployment.deployment_plan_hash,
        "trace_content_hash": declared_trace_hash,
        "trace_kind": plan.trace.kind,
        "model_ids": model_ids,
        "run_plan_path": run_plan_path,
        "trace_path": trace_path,
        "deployment_status_dir": str(status_dir),
        "expected_generation": target["expected_generation"],
        "claim_scope": "EXASERVE_DEPLOYMENT",
        "stall_timeout_s": float(options.get("clientlab_stall_timeout_s", 120.0)),
        "saturation_stream": bool(saturation.stream),
        "saturation_max_p99_ttft": float(saturation.max_p99_ttft),
    }


def _reject_unknown(payload, template, path=""):
    if not isinstance(payload, dict):
        return
    unknown = sorted(set(payload) - set(template))
    if unknown:
        raise ValueError(f"ClientLab {path or 'spec'} has unknown fields: {unknown}")
    for key, value in payload.items():
        expected = template[key]
        if isinstance(value, dict) and isinstance(expected, dict):
            _reject_unknown(value, expected, f"{path}.{key}".strip("."))


def _validate_types(value, template, path="spec"):
    if isinstance(template, dict):
        if not isinstance(value, dict):
            raise ValueError(f"ClientLab {path} must be a mapping")
        for key, expected in template.items():
            _validate_types(value[key], expected, f"{path}.{key}")
    elif isinstance(template, bool):
        if not isinstance(value, bool):
            raise ValueError(f"ClientLab {path} must be boolean")
    elif isinstance(template, int):
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"ClientLab {path} must be an integer")
    elif isinstance(template, float):
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(float(value))
        ):
            raise ValueError(f"ClientLab {path} must be a finite number")
    elif isinstance(template, str) and not isinstance(value, str):
        raise ValueError(f"ClientLab {path} must be text")
    elif isinstance(template, list) and not isinstance(value, list):
        raise ValueError(f"ClientLab {path} must be a list")


def _compatible_value(value, baseline):
    if isinstance(baseline, bool):
        return isinstance(value, bool)
    if isinstance(baseline, int):
        return isinstance(value, int) and not isinstance(value, bool)
    if isinstance(baseline, float):
        return (
            isinstance(value, (int, float))
            and not isinstance(value, bool)
            and math.isfinite(float(value))
        )
    return isinstance(value, type(baseline))


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
    repeat = int(point.get("_repeat", 0))
    fragments.append(f"repeat-{repeat}")
    suffix = stable_hash({"fragments": fragments}, length=6)
    # Build human-readable prefix from axis values.
    parts = [f"{k}={v}" for k, v in sorted(axis_values.items())]
    if repeat > 0:
        parts.append(f"r{repeat}")
    prefix = "_".join(parts) if parts else "base"
    return f"{prefix}_{suffix}"
