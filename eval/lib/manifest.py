"""Eval-only runtime manifest bound to one canonical deployment artifact.

The manifest carries trace/replay and PBS presentation fields that do not
belong in :class:`exaserve.plan.contracts.DeploymentPlan`.  Serving topology,
models, Ray resources, exposure, and gateway settings are referenced by exact
plan path and hash instead of being copied into a second mutable schema.
"""

from __future__ import annotations

import os
import re
import hashlib
import json
import math
from dataclasses import asdict, dataclass, field
from functools import cached_property
from typing import Any, Dict, Union

from eval.lib.saturation import parse_saturation_spec
from exaserve.plan.io import load_deployment_plan, load_run_plan

_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SCHEMA_VERSION = 2
_TOP_FIELDS = {
    "pbs_result_dir",
    "pbs_stdout_dir",
    "pbs_stderr_dir",
    "pbs_num_nodes",
    "pbs_walltime",
    "pbs_queue_name",
    "pbs_job_name",
    "pbs_working_dir",
    "job_trace_config",
    "job_replay_client_config",
    "job_seed",
    "deployment_plan_path",
    "deployment_plan_hash",
    "run_plan_path",
    "run_semantic_hash",
    "trace_content_hash",
}


@dataclass
class TraceGeneratorConfig:
    input_trace_path: str
    input_prompt_path: str
    duration: float
    sampling_strategy: str
    speedup: float
    output_len: int
    output_trace_path: str
    input_len: int = 0
    modes: Dict[str, int] = field(default_factory=lambda: {"chat": 1, "completion": 0})

    def __post_init__(self) -> None:
        for name in (
            "input_trace_path",
            "input_prompt_path",
            "sampling_strategy",
            "output_trace_path",
        ):
            if not isinstance(getattr(self, name), str):
                raise ValueError(f"trace.{name} must be text")
        if not self.output_trace_path:
            raise ValueError("trace.output_trace_path must be non-empty")
        if not os.path.isabs(self.output_trace_path):
            raise ValueError("trace.output_trace_path must be absolute")
        for name in ("duration", "speedup"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or float(value) <= 0
            ):
                raise ValueError(f"trace.{name} must be finite and positive")
        for name, minimum in (("input_len", 0), ("output_len", 1)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"trace.{name} must be an integer >= {minimum}")
        if (
            not isinstance(self.modes, dict)
            or not self.modes
            or not set(self.modes) <= {"chat", "completion"}
            or any(
                not isinstance(key, str)
                or not key
                or isinstance(value, bool)
                or not isinstance(value, int)
                or value < 0
                for key, value in self.modes.items()
            )
            or sum(self.modes.values()) < 1
        ):
            raise ValueError(
                "trace.modes must contain non-negative integer weights and one positive weight"
            )


@dataclass
class WeakScalingConfig:
    input_prompt_path: str
    duration: float
    rpn: float
    input_len: int
    output_len: int
    output_trace_path: str

    def __post_init__(self) -> None:
        for name in ("input_prompt_path", "output_trace_path"):
            if not isinstance(getattr(self, name), str):
                raise ValueError(f"trace.{name} must be text")
        if not self.output_trace_path:
            raise ValueError("trace.output_trace_path must be non-empty")
        if not os.path.isabs(self.output_trace_path):
            raise ValueError("trace.output_trace_path must be absolute")
        if (
            isinstance(self.duration, bool)
            or not isinstance(self.duration, (int, float))
            or not math.isfinite(float(self.duration))
            or float(self.duration) <= 0
        ):
            raise ValueError("trace.duration must be finite and positive")
        if (
            isinstance(self.rpn, bool)
            or not isinstance(self.rpn, (int, float))
            or not math.isfinite(float(self.rpn))
            or float(self.rpn) < 0
        ):
            raise ValueError("trace.rpn must be finite and non-negative")
        for name in ("input_len", "output_len"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"trace.{name} must be a positive integer")


@dataclass
class ReplayClientConfig:
    config_path: str = "config.yaml"
    include_tp: bool = False
    early_stop: float = 0.0
    num_runs: int = 1
    generation_mode: str = "deterministic"
    dest: str = "proxy"
    num_nodes: int = 1
    num_go_procs: int = 1
    num_go_workers: int = 4
    go_concurrency: int = 0
    warmup_rps: int = 0
    warmup_duration_s: float = 0.0
    sum_only: bool = False
    stream: bool = False
    direct_dispatch: str = "local"
    dispatch_topologies: list[str] = field(default_factory=list)
    direct_pair_shift: int = 1
    request_timeout_s: float = 3600.0
    drain_wait_timeout_s: float = 3780.0
    shard_timeout_s: float = 600.0
    direct_target_ready_timeout_s: float = 300.0
    direct_target_probe_timeout_s: float = 2.0
    direct_target_interval_s: float = 5.0
    direct_target_max_workers: int = 64
    saturation: dict = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("config_path", "generation_mode", "dest", "direct_dispatch"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"replay.{name} must be non-empty text")
        if not os.path.isabs(self.config_path):
            raise ValueError("replay.config_path must be absolute")
        for name in ("include_tp", "sum_only", "stream"):
            if not isinstance(getattr(self, name), bool):
                raise ValueError(f"replay.{name} must be a boolean")
        for name in (
            "num_runs",
            "num_nodes",
            "num_go_procs",
            "num_go_workers",
            "direct_pair_shift",
            "direct_target_max_workers",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"replay.{name} must be a positive integer")
        for name in ("go_concurrency", "warmup_rps"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"replay.{name} must be a non-negative integer")
        for name in (
            "early_stop",
            "warmup_duration_s",
            "request_timeout_s",
            "drain_wait_timeout_s",
            "shard_timeout_s",
            "direct_target_ready_timeout_s",
            "direct_target_probe_timeout_s",
            "direct_target_interval_s",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise ValueError(f"replay.{name} must be finite numeric")
        if not 0 <= float(self.early_stop) <= 1:
            raise ValueError("replay.early_stop must be between 0.0 and 1.0")
        if float(self.warmup_duration_s) < 0:
            raise ValueError("replay.warmup_duration_s must be non-negative")
        for name in (
            "request_timeout_s",
            "drain_wait_timeout_s",
            "shard_timeout_s",
            "direct_target_ready_timeout_s",
            "direct_target_probe_timeout_s",
            "direct_target_interval_s",
        ):
            if float(getattr(self, name)) <= 0:
                raise ValueError(f"replay.{name} must be positive")
        if self.dest not in {"proxy", "direct"}:
            raise ValueError("replay.dest must be 'proxy' or 'direct'")
        if self.generation_mode not in {"deterministic", "natural"}:
            raise ValueError("replay.generation_mode must be deterministic or natural")
        allowed_topologies = {"local", "mesh", "paired"}
        if self.direct_dispatch not in allowed_topologies:
            raise ValueError("replay.direct_dispatch must be local, mesh, or paired")
        if (
            not isinstance(self.dispatch_topologies, list)
            or any(
                not isinstance(item, str) or item not in allowed_topologies
                for item in self.dispatch_topologies
            )
            or len(set(self.dispatch_topologies)) != len(self.dispatch_topologies)
        ):
            raise ValueError(
                "replay.dispatch_topologies must be a duplicate-free list of local/mesh/paired"
            )
        if self.dispatch_topologies and self.dest != "direct":
            raise ValueError("replay.dispatch_topologies requires replay.dest='direct'")
        if self.dest == "proxy" and self.direct_dispatch != "local":
            raise ValueError("replay.direct_dispatch must be local for replay.dest='proxy'")
        active_topologies = self.dispatch_topologies or [self.direct_dispatch]
        if self.dest == "direct" and "paired" in active_topologies and self.num_nodes < 2:
            raise ValueError("replay paired topology requires at least two client nodes")
        saturation = parse_saturation_spec(self.saturation, path="replay.saturation")
        self.saturation = asdict(saturation)
        if saturation.enabled:
            if self.num_go_procs != 1:
                raise ValueError(
                    "replay.saturation currently supports exactly one Go process; "
                    "use ClientLab for multi-process saturation"
                )
            if self.num_nodes != 1 or self.num_runs != 1:
                raise ValueError("replay.saturation requires num_nodes=1 and num_runs=1")


@dataclass
class EvalManifest:
    """Workload materialization plus an exact canonical deployment reference."""

    pbs_result_dir: str
    pbs_stdout_dir: str
    pbs_stderr_dir: str
    pbs_num_nodes: int
    pbs_walltime: str
    pbs_queue_name: str
    pbs_job_name: str
    pbs_working_dir: str
    job_trace_config: Union[TraceGeneratorConfig, WeakScalingConfig]
    job_replay_client_config: ReplayClientConfig
    job_seed: int
    deployment_plan_path: str
    deployment_plan_hash: str
    run_plan_path: str
    run_semantic_hash: str
    trace_content_hash: str

    def __post_init__(self) -> None:
        for name in (
            "pbs_result_dir",
            "pbs_stdout_dir",
            "pbs_stderr_dir",
            "pbs_walltime",
            "pbs_queue_name",
            "pbs_job_name",
            "pbs_working_dir",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be non-empty text")
        for name in (
            "pbs_result_dir",
            "pbs_stdout_dir",
            "pbs_stderr_dir",
            "pbs_working_dir",
        ):
            if not os.path.isabs(getattr(self, name)):
                raise ValueError(f"{name} must be absolute")
        if not re.fullmatch(r"\d+:[0-5]\d:[0-5]\d", self.pbs_walltime):
            raise ValueError("pbs_walltime must use HH:MM:SS with valid minute/second fields")
        for name in ("deployment_plan_path", "run_plan_path"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise ValueError(f"{name} must be non-empty")
            if not os.path.isabs(value):
                raise ValueError(f"{name} must be absolute")
        for name in ("deployment_plan_hash", "run_semantic_hash", "trace_content_hash"):
            value = getattr(self, name)
            if not isinstance(value, str) or not _SHA256.fullmatch(value):
                raise ValueError(f"{name} must be a lowercase SHA-256")
        if isinstance(self.pbs_num_nodes, bool) or not isinstance(self.pbs_num_nodes, int):
            raise ValueError("pbs_num_nodes must be an integer")
        if self.pbs_num_nodes < 1:
            raise ValueError("pbs_num_nodes must be positive")
        if isinstance(self.job_seed, bool) or not isinstance(self.job_seed, int):
            raise ValueError("job_seed must be an integer")
        if not isinstance(self.job_trace_config, (TraceGeneratorConfig, WeakScalingConfig)):
            raise ValueError("job_trace_config has an unsupported type")
        if not isinstance(self.job_replay_client_config, ReplayClientConfig):
            raise ValueError("job_replay_client_config has an unsupported type")
        if self.job_replay_client_config.num_nodes > self.pbs_num_nodes:
            raise ValueError("replay.num_nodes cannot exceed pbs_num_nodes")

    @cached_property
    def run_plan(self):
        """Load the canonical run and reject every duplicated-field drift."""
        plan = load_run_plan(self.run_plan_path)
        if plan.run_semantic_hash != self.run_semantic_hash:
            raise ValueError(
                "eval manifest run plan hash mismatch: "
                f"expected {self.run_semantic_hash}, got {plan.run_semantic_hash}"
            )
        if plan.deployment.deployment_plan_hash != self.deployment_plan_hash:
            raise ValueError("eval manifest RunPlan references a different DeploymentPlan")
        if plan.trace.trace_content_hash != self.trace_content_hash:
            raise ValueError("eval manifest trace hash disagrees with canonical RunPlan")
        if (
            plan.scheduler.nodes != self.pbs_num_nodes
            or plan.scheduler.walltime != self.pbs_walltime
            or plan.scheduler.queue != self.pbs_queue_name
        ):
            raise ValueError("eval manifest PBS policy disagrees with canonical RunPlan")
        if plan.workload.seed != self.job_seed:
            raise ValueError("eval manifest seed disagrees with canonical RunPlan")

        replay = self.job_replay_client_config
        client_projection = {
            "include_tp": plan.client.include_tp,
            "early_stop": plan.client.early_stop,
            "num_runs": plan.client.num_runs,
            "generation_mode": plan.workload.generation_mode,
            "dest": plan.client.destination,
            "num_nodes": plan.client.nodes,
            "num_go_procs": plan.client.processes,
            "num_go_workers": plan.client.workers,
            "go_concurrency": plan.client.concurrency,
            "warmup_rps": plan.client.warmup_rps,
            "warmup_duration_s": plan.client.warmup_duration_s,
            "sum_only": plan.client.sum_only,
            "stream": plan.client.streaming,
            "direct_dispatch": plan.client.dispatch_topology,
            "dispatch_topologies": list(plan.client.dispatch_topologies),
            "direct_pair_shift": plan.client.direct_pair_shift,
            "request_timeout_s": plan.client.request_timeout_s,
            "drain_wait_timeout_s": plan.client.drain_wait_timeout_s,
            "shard_timeout_s": plan.client.shard_timeout_s,
            "direct_target_ready_timeout_s": plan.client.direct_target_ready_timeout_s,
            "direct_target_probe_timeout_s": plan.client.direct_target_probe_timeout_s,
            "direct_target_interval_s": plan.client.direct_target_interval_s,
            "direct_target_max_workers": plan.client.direct_target_max_workers,
            "saturation": asdict(plan.client.saturation),
        }
        replay_payload = asdict(replay)
        replay_payload.pop("config_path")
        if replay_payload != client_projection:
            differing = sorted(
                key
                for key in client_projection
                if replay_payload.get(key) != client_projection[key]
            )
            raise ValueError(f"eval replay policy disagrees with canonical RunPlan: {differing}")

        trace = self.job_trace_config
        workload = plan.workload
        if isinstance(trace, WeakScalingConfig):
            if plan.trace.kind != "weak_scaling":
                raise ValueError("weak-scaling runtime trace disagrees with canonical RunPlan")
            trace_projection = {
                "duration": workload.duration_s,
                "rpn": workload.rate_per_node,
                "input_len": workload.input_len,
                "output_len": workload.output_len,
            }
        else:
            if plan.trace.kind == "weak_scaling":
                raise ValueError("runtime trace kind disagrees with canonical RunPlan")
            trace_projection = {
                "duration": workload.duration_s,
                "sampling_strategy": workload.sampling_strategy,
                "speedup": workload.speedup,
                "output_len": workload.output_len,
                "input_len": workload.input_len,
                "modes": dict(workload.modes),
            }
        differing_trace = sorted(
            key for key, value in trace_projection.items() if getattr(trace, key) != value
        )
        if differing_trace:
            raise ValueError(
                f"eval trace policy disagrees with canonical RunPlan: {differing_trace}"
            )
        return plan

    def verify_trace_artifact(self) -> None:
        """Verify the trace once when a caller is not consuming it itself.

        Replay consumes and hashes the trace in one streaming pass, so it calls
        :func:`load_eval_manifest` with ``verify_trace_artifact=False``.  Other
        callers retain the historical eager integrity check.
        """
        try:
            digest = hashlib.sha256()
            from exaserve.state.atomic import regular_file_reader

            with regular_file_reader(
                self.job_trace_config.output_trace_path, binary=True
            ) as handle:
                for chunk in iter(lambda: handle.read(1 << 20), b""):
                    digest.update(chunk)
        except OSError as exc:
            raise ValueError(f"eval trace artifact is unreadable: {exc}") from exc
        if digest.hexdigest() != self.trace_content_hash:
            raise ValueError("eval trace artifact content hash mismatch")

    @cached_property
    def deployment_plan(self):
        run_plan = self.run_plan
        plan = load_deployment_plan(self.deployment_plan_path)
        if plan.deployment_plan_hash != self.deployment_plan_hash:
            raise ValueError(
                "eval manifest deployment plan hash mismatch: "
                f"expected {self.deployment_plan_hash}, got {plan.deployment_plan_hash}"
            )
        if plan != run_plan.deployment:
            raise ValueError("standalone DeploymentPlan disagrees with canonical RunPlan")
        if plan.num_nodes != self.pbs_num_nodes:
            raise ValueError(
                "eval manifest PBS node count disagrees with canonical deployment plan"
            )
        saturation = self.job_replay_client_config.saturation
        if saturation.get("enabled", False) and len(plan.models) != 1:
            raise ValueError("replay.saturation requires exactly one deployed model")
        replay = self.job_replay_client_config
        if replay.dest == "direct":
            if len(plan.models) != 1:
                raise ValueError("direct replay requires exactly one deployed model")
            active_topologies = replay.dispatch_topologies or [replay.direct_dispatch]
            if any(item in {"local", "paired"} for item in active_topologies):
                if replay.num_nodes != plan.num_nodes:
                    raise ValueError(
                        "direct local/paired replay requires one client rank per deployment node"
                    )
                model = plan.models[0]
                target_ranks = {
                    replica.planned_ranks[0] for replica in model.replicas if replica.planned_ranks
                }
                if model.num_replicas > 1 and target_ranks != set(range(plan.num_nodes)):
                    raise ValueError(
                        "direct local/paired replay requires addressable replica targets "
                        "on every deployment node; use mesh for partial-node replica routing"
                    )
            if saturation.get("enabled", False) and plan.num_nodes != 1:
                raise ValueError("direct saturation supports exactly one deployment node")
        return plan

    def to_yaml_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return {
            "schema_version": _SCHEMA_VERSION,
            **payload,
            "eval_manifest_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        }

    def save_yaml(self, path: str) -> None:
        # Resolve and verify before publishing a manifest that a compute job may
        # consume hours later.
        self.deployment_plan
        self.verify_trace_artifact()
        parent = os.path.dirname(os.path.abspath(path))
        if parent:
            os.makedirs(parent, exist_ok=True)
        from exaserve.state.atomic import atomic_create_yaml

        atomic_create_yaml(path, self.to_yaml_dict(), default_flow_style=False)


def _mapping(value: Any, path: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be a mapping")
    return value


def _reject_unknown(value: dict, allowed: set[str], path: str) -> None:
    unknown = sorted(set(value) - allowed)
    if unknown:
        raise ValueError(f"{path} contains unknown fields: {unknown}")


def _text(value: Any, path: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{path} must be text")
    return value


def _integer(value: Any, path: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{path} must be an integer")
    return value


def _number(value: Any, path: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{path} must be numeric")
    value = float(value)
    if not math.isfinite(value):
        raise ValueError(f"{path} must be finite")
    return value


def _boolean(value: Any, path: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{path} must be a boolean")
    return value


def _text_list(value: Any, path: str) -> list[str]:
    if not isinstance(value, list):
        raise ValueError(f"{path} must be a list")
    return [_text(item, f"{path}[{index}]") for index, item in enumerate(value)]


def _trace_config_from_dict(d: Dict[str, Any]) -> Union[TraceGeneratorConfig, WeakScalingConfig]:
    d = _mapping(d, "job_trace_config")
    if "rpn" in d:
        allowed = {
            "input_prompt_path",
            "duration",
            "rpn",
            "input_len",
            "output_len",
            "output_trace_path",
        }
        _reject_unknown(d, allowed, "job_trace_config")
        return WeakScalingConfig(
            input_prompt_path=_text(d.get("input_prompt_path", ""), "trace.input_prompt_path"),
            duration=_number(d.get("duration", 0.0), "trace.duration"),
            rpn=_number(d.get("rpn", 0.0), "trace.rpn"),
            input_len=_integer(d.get("input_len", 0), "trace.input_len"),
            output_len=_integer(d.get("output_len", 0), "trace.output_len"),
            output_trace_path=_text(d.get("output_trace_path", ""), "trace.output_trace_path"),
        )
    allowed = {
        "input_trace_path",
        "input_prompt_path",
        "duration",
        "sampling_strategy",
        "speedup",
        "output_len",
        "output_trace_path",
        "input_len",
        "modes",
    }
    _reject_unknown(d, allowed, "job_trace_config")
    modes = d.get("modes", {"chat": 1, "completion": 0})
    if not isinstance(modes, dict) or any(
        not isinstance(key, str) or isinstance(value, bool) or not isinstance(value, int)
        for key, value in modes.items()
    ):
        raise ValueError("trace.modes must map text names to integers")
    return TraceGeneratorConfig(
        input_trace_path=_text(d.get("input_trace_path", ""), "trace.input_trace_path"),
        input_prompt_path=_text(d.get("input_prompt_path", ""), "trace.input_prompt_path"),
        duration=_number(d.get("duration", 0.0), "trace.duration"),
        sampling_strategy=_text(d.get("sampling_strategy", "peak"), "trace.sampling_strategy"),
        speedup=_number(d.get("speedup", 1.0), "trace.speedup"),
        output_len=_integer(d.get("output_len", 0), "trace.output_len"),
        output_trace_path=_text(d.get("output_trace_path", ""), "trace.output_trace_path"),
        input_len=_integer(d.get("input_len", 0), "trace.input_len"),
        modes=dict(modes),
    )


def _replay_config_from_dict(d: Dict[str, Any]) -> ReplayClientConfig:
    d = _mapping(d, "job_replay_client_config")
    allowed = set(ReplayClientConfig.__dataclass_fields__)
    _reject_unknown(d, allowed, "job_replay_client_config")
    return ReplayClientConfig(
        config_path=_text(d.get("config_path", "config.yaml"), "replay.config_path"),
        include_tp=_boolean(d.get("include_tp", False), "replay.include_tp"),
        early_stop=_number(d.get("early_stop", 0.0), "replay.early_stop"),
        num_runs=_integer(d.get("num_runs", 1), "replay.num_runs"),
        generation_mode=_text(d.get("generation_mode", "deterministic"), "replay.generation_mode"),
        dest=_text(d.get("dest", "proxy"), "replay.dest"),
        num_nodes=_integer(d.get("num_nodes", 1), "replay.num_nodes"),
        num_go_procs=_integer(d.get("num_go_procs", 1), "replay.num_go_procs"),
        num_go_workers=_integer(d.get("num_go_workers", 4), "replay.num_go_workers"),
        go_concurrency=_integer(d.get("go_concurrency", 0), "replay.go_concurrency"),
        warmup_rps=_integer(d.get("warmup_rps", 0), "replay.warmup_rps"),
        warmup_duration_s=_number(d.get("warmup_duration_s", 0.0), "replay.warmup_duration_s"),
        sum_only=_boolean(d.get("sum_only", False), "replay.sum_only"),
        stream=_boolean(d.get("stream", False), "replay.stream"),
        direct_dispatch=_text(d.get("direct_dispatch", "local"), "replay.direct_dispatch"),
        dispatch_topologies=_text_list(
            d.get("dispatch_topologies", []), "replay.dispatch_topologies"
        ),
        direct_pair_shift=_integer(d.get("direct_pair_shift", 1), "replay.direct_pair_shift"),
        request_timeout_s=_number(d.get("request_timeout_s", 3600.0), "replay.request_timeout_s"),
        drain_wait_timeout_s=_number(
            d.get("drain_wait_timeout_s", 3780.0), "replay.drain_wait_timeout_s"
        ),
        shard_timeout_s=_number(d.get("shard_timeout_s", 600.0), "replay.shard_timeout_s"),
        direct_target_ready_timeout_s=_number(
            d.get("direct_target_ready_timeout_s", 300.0),
            "replay.direct_target_ready_timeout_s",
        ),
        direct_target_probe_timeout_s=_number(
            d.get("direct_target_probe_timeout_s", 2.0),
            "replay.direct_target_probe_timeout_s",
        ),
        direct_target_interval_s=_number(
            d.get("direct_target_interval_s", 5.0),
            "replay.direct_target_interval_s",
        ),
        direct_target_max_workers=_integer(
            d.get("direct_target_max_workers", 64), "replay.direct_target_max_workers"
        ),
        saturation=dict(_mapping(d.get("saturation", {}), "replay.saturation")),
    )


def load_eval_manifest(
    path: str,
    *,
    verify_trace_artifact: bool = True,
    deployment_plan_path_override: str | None = None,
    run_plan_path_override: str | None = None,
) -> EvalManifest:
    from exaserve.state.atomic import regular_file_reader
    from exaserve.yaml_support import load_yaml_mapping_text

    with regular_file_reader(path) as handle:
        data = load_yaml_mapping_text(handle.read(), source=path)
    expected_keys = _TOP_FIELDS | {"schema_version", "eval_manifest_hash"}
    _reject_unknown(data, expected_keys, "eval runtime manifest")
    if type(data.get("schema_version")) is not int or data.get("schema_version") != _SCHEMA_VERSION:
        raise ValueError("eval runtime manifest schema_version is unsupported")
    supplied_hash = data.get("eval_manifest_hash")
    if not isinstance(supplied_hash, str) or not _SHA256.fullmatch(supplied_hash):
        raise ValueError("eval_manifest_hash must be a lowercase SHA-256")
    canonical_payload = {key: data[key] for key in _TOP_FIELDS if key in data}
    if set(canonical_payload) != _TOP_FIELDS:
        raise ValueError(
            f"eval runtime manifest is missing fields: {sorted(_TOP_FIELDS - set(data))}"
        )
    canonical = json.dumps(
        canonical_payload, sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    if hashlib.sha256(canonical.encode()).hexdigest() != supplied_hash:
        raise ValueError("eval runtime manifest hash mismatch")
    manifest = EvalManifest(
        pbs_result_dir=_text(data["pbs_result_dir"], "pbs_result_dir"),
        pbs_stdout_dir=_text(data["pbs_stdout_dir"], "pbs_stdout_dir"),
        pbs_stderr_dir=_text(data["pbs_stderr_dir"], "pbs_stderr_dir"),
        pbs_num_nodes=_integer(data["pbs_num_nodes"], "pbs_num_nodes"),
        pbs_walltime=_text(data["pbs_walltime"], "pbs_walltime"),
        pbs_queue_name=_text(data["pbs_queue_name"], "pbs_queue_name"),
        pbs_job_name=_text(data["pbs_job_name"], "pbs_job_name"),
        pbs_working_dir=_text(data["pbs_working_dir"], "pbs_working_dir"),
        job_trace_config=_trace_config_from_dict(data.get("job_trace_config", {})),
        job_replay_client_config=_replay_config_from_dict(data.get("job_replay_client_config", {})),
        job_seed=_integer(data["job_seed"], "job_seed"),
        deployment_plan_path=_text(data["deployment_plan_path"], "deployment_plan_path"),
        deployment_plan_hash=_text(data["deployment_plan_hash"], "deployment_plan_hash"),
        run_plan_path=_text(data["run_plan_path"], "run_plan_path"),
        run_semantic_hash=_text(data["run_semantic_hash"], "run_semantic_hash"),
        trace_content_hash=_text(data["trace_content_hash"], "trace_content_hash"),
    )
    if (deployment_plan_path_override is None) != (run_plan_path_override is None):
        raise ValueError("eval manifest artifact overrides must be supplied together")
    if deployment_plan_path_override is not None:
        from dataclasses import replace

        local = replace(
            manifest,
            deployment_plan_path=deployment_plan_path_override,
            run_plan_path=run_plan_path_override,
        )
        # Preserve the signed manifest's original location fields for result
        # provenance while caching objects validated from its certified local
        # capsule copies. No later property access can reopen the shared paths.
        manifest.__dict__["run_plan"] = local.run_plan
        manifest.__dict__["deployment_plan"] = local.deployment_plan
    else:
        manifest.deployment_plan
    if verify_trace_artifact:
        manifest.verify_trace_artifact()
    return manifest
