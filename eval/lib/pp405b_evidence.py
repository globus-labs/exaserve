"""Fail-closed evidence readers for the SC26 405B PP=2 figure.

The original streaming campaign predates the canonical ResultManifest.  Its
selected artifacts are therefore authenticated by a checked-in ledger, exact
file hashes, legacy plan semantics, and the producing PBS stdout.  Current
campaign points continue to use the canonical manifest/provenance acceptance
path.  Keeping both contracts here prevents plotting code from weakening one
to accommodate the other.

This is deliberately a paper-only adapter for a closed historical dataset. It
is not a runtime contract pattern and must not be used to qualify deployments.
"""

from __future__ import annotations

import hashlib
import math
import re
from ast import literal_eval
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping

import numpy as np

from exaserve.state.atomic import regular_file_reader, strict_json_loads

PP405B_NODE_COUNTS = (4, 8, 16, 32, 64, 128, 256)
_PBS_STDOUT_SUFFIX = "aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov.OU"
_PBS_SERVER = "aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov"
_STAGE_HOSTS = re.compile(
    r"^\[pp_stage\] bcast stage ([01]) \([^)]*\) -> (\d+) node\(s\): (\[.*\])$"
)
_REPLICA_PAIR = re.compile(r"^\s+replica (\d+): nodes=(\[.*\]) \(TP=8, PP=2\)$")


class PP405BLoaderKind(Enum):
    """Evidence contract used to load one complete PP=2 curve."""

    LEGACY_PINNED_V1 = "legacy_pinned_v1"
    CURRENT_MANIFEST_V2 = "current_manifest_v2"


class AllocationSubsetMethod(Enum):
    EXACT = "exact"
    NODEFILE_SUBSET = "nodefile_subset"


class AllocationEvidenceMarker(Enum):
    ALLOCFIX = "allocfix"
    PROD256 = "prod256"


@dataclass(frozen=True)
class LegacyPP405BRef:
    """One immutable selection from the pre-manifest experiment store."""

    allocated_nodes: int
    active_nodes: int
    subset_method: AllocationSubsetMethod
    evidence_marker: AllocationEvidenceMarker | None
    run_group_id: str
    result_name: str
    result_sha256: str
    run_yaml_sha256: str
    source_commit: str
    pbs_job_id: int
    stdout_sha256: str
    errors_by_run: tuple[int, ...]
    expected_successful_rps: float

    @property
    def run_id(self) -> str:
        return f"n{self.active_nodes}"

    @property
    def nodes(self) -> int:
        """Active serving/Ray nodes represented by the paper x-axis."""
        return self.active_nodes

    @property
    def expected_requests_per_run(self) -> int:
        return 24 * self.nodes

    @property
    def expected_run_indices(self) -> tuple[int, ...]:
        return tuple(range(len(self.errors_by_run)))

    @property
    def stdout_name(self) -> str:
        return f"{self.pbs_job_id}.{_PBS_STDOUT_SUFFIX}"


@dataclass(frozen=True)
class CurrentPP405BRef:
    nodes: int
    run_group_id: str
    source_snapshot_hash: str
    deployment_id_scheme: str
    run_semantic_hash: str

    @property
    def run_id(self) -> str:
        return f"n{self.nodes}"


@dataclass(frozen=True)
class PP405BSeries:
    stem: str
    key: str
    proxy: str
    mode: str
    label: str
    loader_kind: PP405BLoaderKind
    legacy_refs: tuple[LegacyPP405BRef, ...] = ()
    current_refs: tuple[CurrentPP405BRef, ...] = ()

    def __post_init__(self) -> None:
        legacy = self.loader_kind is PP405BLoaderKind.LEGACY_PINNED_V1
        current = self.loader_kind is PP405BLoaderKind.CURRENT_MANIFEST_V2
        if not (legacy or current):
            raise ValueError(f"unsupported PP=2 loader kind {self.loader_kind!r}")
        if legacy and (not self.legacy_refs or self.current_refs):
            raise ValueError("legacy PP=2 series must contain only legacy refs")
        if current and (not self.current_refs or self.legacy_refs):
            raise ValueError("current PP=2 series must contain only current refs")

    @classmethod
    def legacy(
        cls, *, stem: str, key: str, proxy: str, label: str, refs: tuple[LegacyPP405BRef, ...]
    ) -> "PP405BSeries":
        return cls(
            stem=stem,
            key=key,
            proxy=proxy,
            mode="stream",
            label=label,
            loader_kind=PP405BLoaderKind.LEGACY_PINNED_V1,
            legacy_refs=refs,
        )

    @classmethod
    def current(
        cls, *, stem: str, key: str, proxy: str, label: str, refs: tuple[CurrentPP405BRef, ...]
    ) -> "PP405BSeries":
        return cls(
            stem=stem,
            key=key,
            proxy=proxy,
            mode="nonstream",
            label=label,
            loader_kind=PP405BLoaderKind.CURRENT_MANIFEST_V2,
            current_refs=refs,
        )


def _is_lower_hex(value: object, length: int) -> bool:
    return (
        isinstance(value, str)
        and len(value) == length
        and value == value.lower()
        and all(character in "0123456789abcdef" for character in value)
    )


def _require_mapping(value: object, *, context: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RuntimeError(f"{context} must be an object")
    return value


def _require_exact_keys(document: Mapping[str, Any], expected: set[str], *, context: str) -> None:
    unknown = sorted(set(document) - expected)
    missing = sorted(expected - set(document))
    if unknown or missing:
        raise RuntimeError(f"{context} shape mismatch: unknown={unknown}, missing={missing}")


def _matches_expected(observed: object, expected: object) -> bool:
    if isinstance(expected, bool):
        return observed is expected
    if type(expected) is int:
        return type(observed) is int and observed == expected
    return observed == expected


def _require_exact_fields(checks: tuple[tuple[str, object, object], ...], *, context: str) -> None:
    for field, observed, expected in checks:
        if not _matches_expected(observed, expected):
            raise RuntimeError(f"{context}.{field} is {observed!r}, expected {expected!r}")


def _mapping_at(document: object, path: tuple[str, ...], *, context: str) -> object:
    value = document
    traversed = []
    for key in path:
        traversed.append(key)
        if not isinstance(value, dict) or key not in value:
            dotted = ".".join(traversed)
            raise RuntimeError(f"{context}.{dotted} is missing or not in an object")
        value = value[key]
    return value


def load_legacy_pp405b_ledger(path: str | Path) -> Mapping[str, tuple[LegacyPP405BRef, ...]]:
    """Strictly load the two version-controlled legacy point selections."""
    from exaserve.yaml_support import load_yaml_mapping, require_yaml

    yaml = require_yaml()
    try:
        document = load_yaml_mapping(path)
    except (OSError, UnicodeError, TypeError, ValueError, yaml.YAMLError) as exc:
        raise RuntimeError(f"legacy PP=2 evidence ledger is not strict YAML: {path}") from exc
    _require_exact_keys(
        document,
        {"schema_version", "active_node_counts", "series"},
        context=str(path),
    )
    _require_exact_fields(
        (("schema_version", document["schema_version"], 2),),
        context=str(path),
    )
    node_counts = document["active_node_counts"]
    if (
        not isinstance(node_counts, list)
        or any(type(nodes) is not int for nodes in node_counts)
        or tuple(node_counts) != PP405B_NODE_COUNTS
    ):
        raise RuntimeError(
            f"{path}.active_node_counts must be exactly the ordered ladder "
            f"{list(PP405B_NODE_COUNTS)}"
        )
    raw_series = _require_mapping(document["series"], context=f"{path}.series")
    expected_series = {
        "direct_stream": (
            "pp405b_pp2_scale_direct",
            "direct",
            {64: AllocationEvidenceMarker.ALLOCFIX},
        ),
        "haproxy_stream": (
            "pp405b_pp2_scale",
            "haproxy",
            {
                32: AllocationEvidenceMarker.ALLOCFIX,
                64: AllocationEvidenceMarker.ALLOCFIX,
                128: AllocationEvidenceMarker.PROD256,
            },
        ),
    }
    _require_exact_keys(raw_series, set(expected_series), context=f"{path}.series")
    parsed: dict[str, tuple[LegacyPP405BRef, ...]] = {}
    point_keys = {
        "allocated_nodes",
        "active_nodes",
        "subset_method",
        "evidence_marker",
        "run_group_id",
        "result_name",
        "result_sha256",
        "run_yaml_sha256",
        "source_commit",
        "pbs_job_id",
        "stdout_sha256",
        "errors_by_run",
        "expected_successful_rps",
    }
    for key, (expected_stem, expected_proxy, subset_overrides) in expected_series.items():
        raw = _require_mapping(raw_series[key], context=f"{path}.series.{key}")
        _require_exact_keys(raw, {"stem", "proxy", "points"}, context=f"{path}.series.{key}")
        _require_exact_fields(
            (("stem", raw["stem"], expected_stem), ("proxy", raw["proxy"], expected_proxy)),
            context=f"{path}.series.{key}",
        )
        points = raw["points"]
        if not isinstance(points, list):
            raise RuntimeError(f"{path}.series.{key}.points must be an array")
        refs = []
        for offset, point_value in enumerate(points):
            context = f"{path}.series.{key}.points[{offset}]"
            point = _require_mapping(point_value, context=context)
            _require_exact_keys(point, point_keys, context=context)
            allocated_nodes = point["allocated_nodes"]
            active_nodes = point["active_nodes"]
            errors = point["errors_by_run"]
            if type(active_nodes) is not int or active_nodes <= 0:
                raise RuntimeError(f"{context}.active_nodes must be a positive integer")
            if (
                not isinstance(errors, list)
                or len(errors) < 2
                or any(
                    type(value) is not int or value < 0 or value > 24 * active_nodes
                    for value in errors
                )
            ):
                raise RuntimeError(f"{context}.errors_by_run is invalid")
            try:
                subset_method = AllocationSubsetMethod(point["subset_method"])
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"{context}.subset_method is invalid") from exc
            marker_value = point["evidence_marker"]
            try:
                evidence_marker = (
                    None if marker_value is None else AllocationEvidenceMarker(marker_value)
                )
            except (TypeError, ValueError) as exc:
                raise RuntimeError(f"{context}.evidence_marker is invalid") from exc
            ref = LegacyPP405BRef(
                allocated_nodes=allocated_nodes,
                active_nodes=active_nodes,
                subset_method=subset_method,
                evidence_marker=evidence_marker,
                run_group_id=point["run_group_id"],
                result_name=point["result_name"],
                result_sha256=point["result_sha256"],
                run_yaml_sha256=point["run_yaml_sha256"],
                source_commit=point["source_commit"],
                pbs_job_id=point["pbs_job_id"],
                stdout_sha256=point["stdout_sha256"],
                errors_by_run=tuple(errors),
                expected_successful_rps=point["expected_successful_rps"],
            )
            _validate_legacy_ref(ref, context=context)
            refs.append(ref)
        if tuple(ref.nodes for ref in refs) != PP405B_NODE_COUNTS:
            raise RuntimeError(
                f"{path}.series.{key} must select exactly nodes {list(PP405B_NODE_COUNTS)}"
            )
        observed_subsets = {
            ref.nodes: ref.evidence_marker
            for ref in refs
            if ref.subset_method is not AllocationSubsetMethod.EXACT
        }
        if observed_subsets != subset_overrides:
            raise RuntimeError(
                f"{path}.series.{key} allocation subsets are {observed_subsets}, "
                f"expected {subset_overrides}"
            )
        parsed[key] = tuple(refs)
    return MappingProxyType(parsed)


def _validate_legacy_ref(ref: LegacyPP405BRef, *, context: str) -> None:
    if type(ref.allocated_nodes) is not int or ref.allocated_nodes <= 0:
        raise RuntimeError(f"{context}.allocated_nodes must be a positive integer")
    if type(ref.active_nodes) is not int or ref.active_nodes <= 0 or ref.active_nodes % 2:
        raise RuntimeError(f"{context}.active_nodes must be a positive even integer")
    if type(ref.subset_method) is not AllocationSubsetMethod:
        raise RuntimeError(f"{context}.subset_method must be typed")
    if ref.subset_method is AllocationSubsetMethod.EXACT:
        if ref.allocated_nodes != ref.active_nodes or ref.evidence_marker is not None:
            raise RuntimeError(f"{context} exact allocation sizes disagree")
    elif (
        ref.allocated_nodes <= ref.active_nodes
        or type(ref.evidence_marker) is not AllocationEvidenceMarker
    ):
        raise RuntimeError(f"{context} nodefile subset evidence is incomplete")
    if ref.evidence_marker is AllocationEvidenceMarker.PROD256 and ref.allocated_nodes != 256:
        raise RuntimeError(f"{context} prod256 evidence requires a 256-node allocation")
    if not isinstance(ref.run_group_id, str) or not (
        ref.run_group_id.startswith("run") and ref.run_group_id[3:].isdigit()
    ):
        raise RuntimeError(f"{context}.run_group_id is invalid")
    if ref.result_name not in {"result0.json", "result1.json"}:
        raise RuntimeError(f"{context}.result_name is invalid")
    for field in ("result_sha256", "run_yaml_sha256", "stdout_sha256"):
        if not _is_lower_hex(getattr(ref, field), 64):
            raise RuntimeError(f"{context}.{field} must be a lowercase SHA-256")
    if not _is_lower_hex(ref.source_commit, 40):
        raise RuntimeError(f"{context}.source_commit must be a lowercase Git commit")
    if type(ref.pbs_job_id) is not int or ref.pbs_job_id <= 0:
        raise RuntimeError(f"{context}.pbs_job_id must be a positive integer")
    if (
        type(ref.errors_by_run) is not tuple
        or len(ref.errors_by_run) < 2
        or any(
            type(value) is not int or value < 0 or value > ref.expected_requests_per_run
            for value in ref.errors_by_run
        )
    ):
        raise RuntimeError(f"{context}.errors_by_run is invalid")
    if (
        isinstance(ref.expected_successful_rps, bool)
        or not isinstance(ref.expected_successful_rps, (int, float))
        or not math.isfinite(float(ref.expected_successful_rps))
        or ref.expected_successful_rps <= 0
    ):
        raise RuntimeError(f"{context}.expected_successful_rps must be finite and positive")


def _validate_current_ref(ref: CurrentPP405BRef, *, context: str) -> None:
    if type(ref.nodes) is not int or ref.nodes <= 0:
        raise RuntimeError(f"{context}.nodes must be a positive integer")
    if not isinstance(ref.run_group_id, str) or not (
        ref.run_group_id.startswith("run") and ref.run_group_id[3:].isdigit()
    ):
        raise RuntimeError(f"{context}.run_group_id is invalid")
    for field in ("source_snapshot_hash", "run_semantic_hash"):
        if not _is_lower_hex(getattr(ref, field), 64):
            raise RuntimeError(f"{context}.{field} must be a lowercase SHA-256")
    if ref.deployment_id_scheme not in {"legacy_truncate_v1", "bounded_hash_v2"}:
        raise RuntimeError(f"{context}.deployment_id_scheme is invalid")


def _sha_bound_bytes(path: str | Path, expected_sha256: str, *, label: str) -> bytes:
    """Read and authenticate one artifact through a single descriptor."""
    if not _is_lower_hex(expected_sha256, 64):
        raise RuntimeError(f"{label} has an invalid pinned SHA-256: {expected_sha256!r}")
    try:
        with regular_file_reader(path, binary=True) as handle:
            payload = handle.read()
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"cannot read pinned {label}: {path}") from exc
    observed = hashlib.sha256(payload).hexdigest()
    if observed != expected_sha256:
        raise RuntimeError(
            f"pinned {label} SHA-256 mismatch at {path}: "
            f"observed {observed}, expected {expected_sha256}"
        )
    return payload


def _load_sha_bound_json(path: str | Path, expected_sha256: str, *, label: str) -> object:
    """Hash exact bytes before applying the repository's strict JSON decoder."""
    payload = _sha_bound_bytes(path, expected_sha256, label=label)
    try:
        return strict_json_loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise RuntimeError(f"pinned {label} is not strict UTF-8 JSON: {path}") from exc


def _load_sha_bound_yaml(path: str | Path, expected_sha256: str, *, label: str) -> dict[str, Any]:
    """Authenticate a legacy plan before strict, duplicate-safe YAML parsing."""
    from exaserve.yaml_support import load_yaml_mapping_text, require_yaml

    yaml = require_yaml()
    payload = _sha_bound_bytes(path, expected_sha256, label=label)
    try:
        return load_yaml_mapping_text(payload, source=path)
    except (UnicodeError, TypeError, ValueError, yaml.YAMLError) as exc:
        raise RuntimeError(f"pinned {label} is not strict YAML: {path}") from exc


def _validate_legacy_plan(
    document: object, *, path: Path, series: PP405BSeries, ref: LegacyPP405BRef
) -> Path:
    context = f"{path}:legacy RunPlan"
    plan = _require_mapping(document, context=context)
    deployment = _require_mapping(
        _mapping_at(plan, ("deployment",), context=context), context=context
    )
    models = _mapping_at(plan, ("deployment", "models"), context=context)
    if not isinstance(models, list) or len(models) != 1 or not isinstance(models[0], dict):
        raise RuntimeError(f"{context}.deployment.models must contain exactly one object")
    model = models[0]
    destination = "direct" if series.proxy == "direct" else "proxy"
    gateway = "none" if series.proxy == "direct" else "haproxy"
    go_processes = 8 if series.proxy == "direct" else 4
    _require_exact_fields(
        (
            ("run_id", plan.get("run_id"), ref.run_id),
            ("run_group_id", plan.get("run_group_id"), ref.run_group_id),
            ("spec_name", plan.get("spec_name"), series.stem),
            ("variant_name", plan.get("variant_name"), ref.run_id),
            ("axis_values", plan.get("axis_values"), {"num_nodes": ref.nodes}),
            ("backend_name", plan.get("backend_name"), "ray"),
            (
                "backend_args.proxy.type",
                _mapping_at(plan, ("backend_args", "proxy", "type"), context=context),
                gateway,
            ),
            ("deployment.num_nodes", deployment.get("num_nodes"), ref.nodes),
            ("deployment.engine", deployment.get("engine"), "vllm"),
            ("deployment.num_gpus_per_node", deployment.get("num_gpus_per_node"), 12),
            (
                "deployment.replica_max_ongoing_requests",
                deployment.get("replica_max_ongoing_requests"),
                16,
            ),
            (
                "deployment.models[0].model_id",
                model.get("model_id"),
                "meta-llama/Llama-3.1-405B-Instruct",
            ),
            ("deployment.models[0].tensor_parallel_size", model.get("tensor_parallel_size"), 8),
            ("deployment.models[0].pipeline_parallel_size", model.get("pipeline_parallel_size"), 2),
            ("deployment.models[0].num_replicas", model.get("num_replicas"), ref.nodes // 2),
            ("deployment.models[0].num_cpus_per_replica", model.get("num_cpus_per_replica"), 4),
            ("deployment.models[0].max_model_len", model.get("max_model_len"), 4096),
            (
                "deployment.models[0].gpu_memory_utilization",
                model.get("gpu_memory_utilization"),
                0.9,
            ),
            ("deployment.models[0].max_num_seqs", model.get("max_num_seqs"), 8),
            ("deployment.models[0].enforce_eager", model.get("enforce_eager"), True),
            ("client.dest", _mapping_at(plan, ("client", "dest"), context=context), destination),
            ("client.stream", _mapping_at(plan, ("client", "stream"), context=context), True),
            (
                "client.num_runs",
                _mapping_at(plan, ("client", "num_runs"), context=context),
                len(ref.errors_by_run),
            ),
            (
                "client.startup_only",
                _mapping_at(plan, ("client", "startup_only"), context=context),
                False,
            ),
            ("client.num_nodes", _mapping_at(plan, ("client", "num_nodes"), context=context), 4),
            (
                "client.num_go_procs",
                _mapping_at(plan, ("client", "num_go_procs"), context=context),
                go_processes,
            ),
            (
                "client.num_go_workers",
                _mapping_at(plan, ("client", "num_go_workers"), context=context),
                4,
            ),
            (
                "client.go_concurrency",
                _mapping_at(plan, ("client", "go_concurrency"), context=context),
                256,
            ),
            (
                "client.include_tp",
                _mapping_at(plan, ("client", "include_tp"), context=context),
                False,
            ),
            (
                "scheduler.nodes",
                _mapping_at(plan, ("scheduler", "nodes"), context=context),
                ref.nodes,
            ),
            (
                "workload.duration",
                _mapping_at(plan, ("workload", "duration"), context=context),
                120.0,
            ),
            (
                "workload.input_len",
                _mapping_at(plan, ("workload", "input_len"), context=context),
                64,
            ),
            (
                "workload.output_len",
                _mapping_at(plan, ("workload", "output_len"), context=context),
                64,
            ),
            (
                "workload.rate_per_node",
                _mapping_at(plan, ("workload", "rate_per_node"), context=context),
                0.2,
            ),
            (
                "workload.arrival",
                _mapping_at(plan, ("workload", "arrival"), context=context),
                "fixed",
            ),
            (
                "workload.generation_mode",
                _mapping_at(plan, ("workload", "generation_mode"), context=context),
                "deterministic",
            ),
            ("workload.seed", _mapping_at(plan, ("workload", "seed"), context=context), 42),
            ("trace.kind", _mapping_at(plan, ("trace", "kind"), context=context), "weak_scaling"),
        ),
        context=context,
    )
    if series.proxy == "haproxy":
        _require_exact_fields(
            (
                (
                    "backend_args.proxy.num_workers",
                    _mapping_at(plan, ("backend_args", "proxy", "num_workers"), context=context),
                    1,
                ),
                (
                    "backend_args.proxy.options.balance",
                    _mapping_at(
                        plan,
                        ("backend_args", "proxy", "options", "balance"),
                        context=context,
                    ),
                    "leastconn",
                ),
                (
                    "backend_args.proxy.options.maxconn",
                    _mapping_at(
                        plan,
                        ("backend_args", "proxy", "options", "maxconn"),
                        context=context,
                    ),
                    50000,
                ),
            ),
            context=context,
        )
    for field in ("repo_root", "snapshot_root"):
        source_path = plan.get(field)
        if not isinstance(source_path, str) or Path(source_path).name != ref.source_commit:
            raise RuntimeError(
                f"{context}.{field} does not name pinned source commit {ref.source_commit}"
            )
    bundle_root = _mapping_at(plan, ("bundle", "root_dir"), context=context)
    if not isinstance(bundle_root, str) or not Path(bundle_root).is_absolute():
        raise RuntimeError(f"{context}.bundle.root_dir must be an absolute path")
    original_cell = Path(bundle_root)
    if original_cell.parts[-3:] != (series.stem, ref.run_group_id, ref.run_id):
        raise RuntimeError(f"{context}.bundle.root_dir disagrees with the selected run")
    return original_cell


def _validate_producing_stdout(
    payload: bytes,
    *,
    path: Path,
    original_cell: Path,
    ref: LegacyPP405BRef,
) -> None:
    try:
        lines = payload.decode("utf-8").splitlines()
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"pinned legacy PBS stdout is not UTF-8: {path}") from exc
    expected = (
        f"[System] Total Nodes: {ref.nodes}",
        f"[AuroraServe] Wrote {ref.nodes} Ray node IP(s) -> "
        f"{original_cell / 'runtime' / 'ray_node_ips.txt'}",
        "  - meta-llama/Llama-3.1-405B-Instruct: "
        f"requested={ref.nodes // 2}, assigned={ref.nodes // 2}",
        f">>> [REPLAY] Saved results to {original_cell / 'results' / ref.result_name}",
    )
    for marker in expected:
        if lines.count(marker) != 1:
            raise RuntimeError(
                f"pinned legacy PBS stdout must contain exactly one {marker!r}: {path}"
            )
    subset_headers = [
        line for line in lines if line.startswith("[allocfix]") or line.startswith("[prod256]")
    ]
    if ref.subset_method is AllocationSubsetMethod.EXACT:
        if subset_headers:
            raise RuntimeError(f"unexpected allocation-subset evidence in legacy stdout: {path}")
    else:
        if ref.evidence_marker is AllocationEvidenceMarker.ALLOCFIX:
            header = (
                f"[allocfix] Ray cluster truncated to {ref.active_nodes} nodes "
                "to match the deployment:"
            )
        elif ref.evidence_marker is AllocationEvidenceMarker.PROD256:
            header = (
                f"[prod256] truncated allocation to {ref.active_nodes} nodes "
                f"for the n{ref.active_nodes} deployment:"
            )
        else:  # guarded by _validate_legacy_ref; keep this helper fail-closed
            raise RuntimeError(f"pinned legacy PBS stdout has no typed subset marker: {path}")
        subset_path = original_cell / f"nodefile_{ref.active_nodes}.txt"
        counts = [
            header,
            f"{ref.allocated_nodes:5d} /var/spool/pbs/aux/{ref.pbs_job_id}.{_PBS_SERVER}",
            f"{ref.active_nodes:5d} {subset_path}",
            f"{ref.allocated_nodes + ref.active_nodes:5d} total",
        ]
        if lines[:4] != counts or subset_headers != [header]:
            raise RuntimeError(f"pinned legacy PBS stdout has invalid subset evidence: {path}")

    stage_xnames: dict[int, tuple[str, ...]] = {}
    for line in lines:
        match = _STAGE_HOSTS.fullmatch(line)
        if match is None:
            continue
        stage = int(match.group(1))
        try:
            hosts = literal_eval(match.group(3))
        except (SyntaxError, ValueError) as exc:
            raise RuntimeError(f"invalid PP stage xname list in legacy stdout: {path}") from exc
        if (
            stage in stage_xnames
            or not isinstance(hosts, list)
            or any(not isinstance(host, str) or not host for host in hosts)
        ):
            raise RuntimeError(f"invalid PP stage xname list in legacy stdout: {path}")
        declared_count = int(match.group(2))
        if declared_count != ref.active_nodes // 2 or len(hosts) != declared_count:
            raise RuntimeError(f"PP stage xname count disagrees with active nodes: {path}")
        stage_xnames[stage] = tuple(hosts)
    if set(stage_xnames) != {0, 1}:
        raise RuntimeError(f"legacy stdout has incomplete PP stage xname evidence: {path}")
    stage0, stage1 = stage_xnames[0], stage_xnames[1]
    if (
        len(set(stage0)) != len(stage0)
        or len(set(stage1)) != len(stage1)
        or set(stage0) & set(stage1)
        or len(set(stage0) | set(stage1)) != ref.active_nodes
    ):
        raise RuntimeError(f"legacy stdout PP stage xnames are not exact and disjoint: {path}")

    replica_pairs = []
    for line in lines:
        match = _REPLICA_PAIR.fullmatch(line)
        if match is None:
            continue
        try:
            pair = literal_eval(match.group(2))
        except (SyntaxError, ValueError) as exc:
            raise RuntimeError(f"invalid PP replica pair in legacy stdout: {path}") from exc
        if (
            not isinstance(pair, list)
            or len(pair) != 2
            or any(not isinstance(node, str) or not node for node in pair)
        ):
            raise RuntimeError(f"invalid PP replica pair in legacy stdout: {path}")
        replica_pairs.append((int(match.group(1)), tuple(pair)))
    expected_replicas = ref.active_nodes // 2
    if [index for index, _ in replica_pairs] != list(range(expected_replicas)):
        raise RuntimeError(f"legacy stdout has incomplete PP replica indices: {path}")
    replica_ips = [node for _, pair in replica_pairs for node in pair]
    if len(set(replica_ips)) != ref.active_nodes:
        raise RuntimeError(f"legacy stdout PP replica IP pairs are not exact and disjoint: {path}")


def _finite_number(record: Mapping[str, Any], field: str, *, context: str, positive=False) -> float:
    value = record.get(field)
    valid_type = not isinstance(value, bool) and isinstance(value, (int, float))
    try:
        number = float(value) if valid_type else math.nan
    except (OverflowError, TypeError, ValueError):
        number = math.nan
    if not math.isfinite(number) or number < 0 or (positive and number == 0):
        qualifier = "positive" if positive else "non-negative"
        raise RuntimeError(f"{context}.{field} must be finite and {qualifier}")
    return number


def _validate_record(
    record: object,
    *,
    context: str,
    requests: int,
    errors: int,
    require_success_fields: bool,
) -> dict[str, Any]:
    item = _require_mapping(record, context=context)
    _require_exact_fields(
        (
            ("requests_scheduled", item.get("requests_scheduled"), requests),
            ("requests_completed", item.get("requests_completed"), requests),
            ("errors", item.get("errors"), errors),
        ),
        context=context,
    )
    duration = _finite_number(item, "duration_s", context=context, positive=True)
    rps = _finite_number(item, "rps", context=context)
    p50 = _finite_number(item, "p50_s", context=context)
    p99 = _finite_number(item, "p99_s", context=context)
    if p99 < p50:
        raise RuntimeError(f"{context}.p99_s must be greater than or equal to p50_s")
    expected_rps = requests / duration
    if not math.isclose(rps, expected_rps, rel_tol=1e-9, abs_tol=1e-12):
        raise RuntimeError(
            f"{context}.rps {rps!r} disagrees with requests_completed/duration_s {expected_rps!r}"
        )
    if require_success_fields:
        successes = requests - errors
        _require_exact_fields((("successes", item.get("successes"), successes),), context=context)
        success_rps = _finite_number(item, "success_rps", context=context)
        expected_success_rps = successes / duration
        if not math.isclose(success_rps, expected_success_rps, rel_tol=1e-9, abs_tol=1e-12):
            raise RuntimeError(
                f"{context}.success_rps {success_rps!r} disagrees with "
                f"successes/duration_s {expected_success_rps!r}"
            )
    return item


def _validate_legacy_result(
    document: object, *, result_path: Path, series: PP405BSeries, ref: LegacyPP405BRef
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    context = f"{result_path}:legacy result"
    result = _require_mapping(document, context=context)
    meta = _require_mapping(result.get("meta"), context=f"{context}.meta")
    run_count = len(ref.errors_by_run)
    destination = "direct" if series.proxy == "direct" else "proxy"
    _require_exact_fields(
        (
            ("meta.num_runs", meta.get("num_runs"), run_count),
            ("meta.completed_runs", meta.get("completed_runs"), run_count),
            ("meta.dest", meta.get("dest"), destination),
        ),
        context=context,
    )
    overall = _validate_record(
        result.get("overall"),
        context=f"{context}.overall",
        requests=ref.expected_requests_per_run,
        errors=ref.errors_by_run[-1],
        require_success_fields=False,
    )
    per_run = result.get("per_run")
    if not isinstance(per_run, list) or len(per_run) != run_count:
        raise RuntimeError(f"{context}.per_run must contain exactly {run_count} runs")
    records = []
    for offset, errors in enumerate(ref.errors_by_run):
        record = _validate_record(
            per_run[offset],
            context=f"{context}.per_run[{offset}]",
            requests=ref.expected_requests_per_run,
            errors=errors,
            require_success_fields=True,
        )
        records.append(record)
    indices = tuple(record.get("run_index") for record in records)
    if indices != ref.expected_run_indices or any(type(index) is not int for index in indices):
        raise RuntimeError(
            f"{context}.per_run indices are {list(indices)}, "
            f"expected {list(ref.expected_run_indices)}"
        )
    mirrored = (
        "requests_completed",
        "requests_scheduled",
        "errors",
        "duration_s",
        "rps",
        "p50_s",
        "p99_s",
    )
    mismatches = [field for field in mirrored if overall[field] != records[-1][field]]
    if mismatches:
        raise RuntimeError(f"{context}.overall does not mirror the final run fields {mismatches}")
    return overall, records[1:]


def _require_current_result(result_path: Path):
    from eval.lib.paper_acceptance import require_accepted_paper_run
    from exaserve.state.results import ResultEntry

    required = {
        "replay/default",
        "deployment_ready_evidence",
        "compatibility_receipts",
        "run_provenance",
    }
    accepted = require_accepted_paper_run(result_path.parents[1], required_result_ids=required)
    replay = [entry for entry in accepted.manifest.entries if entry.logical_id == "replay/default"]
    if len(replay) != 1 or type(replay[0]) is not ResultEntry or replay[0].path != result_path.name:
        raise RuntimeError(
            f"paper result manifest does not own result0.json: {result_path.parents[1]}"
        )
    return accepted, replay[0]


def _validate_current_plan(
    accepted: object, *, series: PP405BSeries, ref: CurrentPP405BRef
) -> None:
    run_plan = accepted.run_plan
    semantic = run_plan.semantic_plan
    deployment = semantic.deployment
    if len(deployment.models) != 1:
        raise RuntimeError(
            f"current PP=2 n{ref.nodes} RunPlan has {len(deployment.models)} models, expected 1"
        )
    model = deployment.models[0]
    gateway = None if deployment.gateway is None else deployment.gateway.kind
    _require_exact_fields(
        (
            ("spec_name", run_plan.spec_name, series.stem),
            ("run_id", run_plan.run_id, ref.run_id),
            ("run_group_id", run_plan.run_group_id, ref.run_group_id),
            ("variant_name", run_plan.variant_name, ref.run_id),
            ("axis_values", run_plan.axis_values, {"num_nodes": ref.nodes}),
            ("source_snapshot_hash", run_plan.source_snapshot_hash, ref.source_snapshot_hash),
            ("deployment_id_scheme", run_plan.deployment_id_scheme, ref.deployment_id_scheme),
            ("run_semantic_hash", run_plan.run_semantic_hash, ref.run_semantic_hash),
            ("deployment.num_nodes", deployment.num_nodes, ref.nodes),
            ("scheduler.nodes", semantic.scheduler.nodes, ref.nodes),
            ("deployment.runtime.null_compute", deployment.runtime.null_compute, False),
            ("deployment.models[0].model_id", model.model_id, "meta-llama/Llama-3.1-405B-Instruct"),
            ("deployment.models[0].tensor_parallel_size", model.tensor_parallel_size, 8),
            ("deployment.models[0].pipeline_parallel_size", model.pipeline_parallel_size, 2),
            ("deployment.models[0].num_replicas", model.num_replicas, ref.nodes // 2),
            ("deployment.models[0].max_model_len", model.max_model_len, 4096),
            ("deployment.models[0].gpu_memory_utilization", model.gpu_memory_utilization, 0.95),
            ("deployment.models[0].max_num_seqs", model.max_num_seqs, 8),
            ("deployment.models[0].enforce_eager", model.enforce_eager, True),
            ("deployment.gateway.kind", gateway, "haproxy"),
            ("client.destination", semantic.client.destination, "proxy"),
            ("client.streaming", semantic.client.streaming, False),
            ("client.num_runs", semantic.client.num_runs, 2),
            ("workload.client_dest", semantic.workload.client_dest, "proxy"),
            ("workload.rate_per_node", semantic.workload.rate_per_node, 0.2),
            ("workload.duration_s", semantic.workload.duration_s, 120.0),
        ),
        context=f"current PP=2 n{ref.nodes} RunPlan",
    )


def _validate_current_result(
    document: object, *, result_path: Path, nodes: int
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    context = f"{result_path}:current result"
    result = _require_mapping(document, context=context)
    requests = 24 * nodes
    overall = _validate_record(
        result.get("overall"),
        context=f"{context}.overall",
        requests=requests,
        errors=0,
        require_success_fields=False,
    )
    per_run = result.get("per_run")
    if not isinstance(per_run, list) or len(per_run) != 2:
        raise RuntimeError(f"{context}.per_run must contain exactly two runs")
    records = [
        _validate_record(
            record,
            context=f"{context}.per_run[{offset}]",
            requests=requests,
            errors=0,
            require_success_fields=True,
        )
        for offset, record in enumerate(per_run)
    ]
    indices = [record.get("run_index") for record in records]
    if indices != [0, 1] or any(type(index) is not int for index in indices):
        raise RuntimeError(f"{context}.per_run indices are {indices}, expected [0, 1]")
    mirrored = (
        "requests_completed",
        "requests_scheduled",
        "errors",
        "duration_s",
        "rps",
        "p50_s",
        "p99_s",
    )
    mismatches = [field for field in mirrored if overall[field] != records[-1][field]]
    if mismatches:
        raise RuntimeError(f"{context}.overall does not mirror per_run[1] fields {mismatches}")
    return overall, records[1:]


def _summarize_point(*, nodes: int, data_runs: list[dict[str, Any]]) -> dict[str, float | int]:
    """Convert already validated records into the figure's one point."""
    rates = [
        (record["requests_completed"] - record["errors"]) / record["duration_s"]
        for record in data_runs
    ]
    rate_array = np.asarray(rates, dtype=float)
    completed = sum(record["requests_completed"] for record in data_runs)
    errors = sum(record["errors"] for record in data_runs)
    return {
        "rep": nodes // 2,
        "nodes": nodes,
        "srps": float(rate_array.mean()),
        "srps_std": float(rate_array.std()),
        "n_iters": len(rates),
        "p50": float(np.mean([record["p50_s"] for record in data_runs])),
        "p99": float(np.mean([record["p99_s"] for record in data_runs])),
        "errfrac": errors / completed,
    }


def _validate_series(series: PP405BSeries) -> None:
    if not isinstance(series, PP405BSeries):
        raise TypeError("PP=2 series must be a PP405BSeries")
    if series.loader_kind is PP405BLoaderKind.LEGACY_PINNED_V1:
        if (
            series.current_refs
            or series.mode != "stream"
            or series.proxy not in {"direct", "haproxy"}
        ):
            raise RuntimeError(f"legacy PP=2 series {series.key} has invalid series semantics")
        refs: tuple[object, ...] = series.legacy_refs
        expected_type = LegacyPP405BRef
    elif series.loader_kind is PP405BLoaderKind.CURRENT_MANIFEST_V2:
        if (
            series.legacy_refs
            or series.stem != "pp405b_pp2_haproxy_nostream_v040"
            or series.proxy != "haproxy"
            or series.mode != "nonstream"
        ):
            raise RuntimeError(f"current PP=2 series {series.key} has invalid series semantics")
        refs = series.current_refs
        expected_type = CurrentPP405BRef
    else:
        raise RuntimeError(f"unsupported PP=2 loader kind {series.loader_kind!r}")
    if any(type(ref) is not expected_type for ref in refs):
        raise RuntimeError(f"PP=2 series {series.key} has an invalid typed selection")
    if tuple(ref.nodes for ref in refs) != PP405B_NODE_COUNTS:
        raise RuntimeError(
            f"PP=2 series {series.key} must select exactly nodes {list(PP405B_NODE_COUNTS)}"
        )
    if series.loader_kind is PP405BLoaderKind.LEGACY_PINNED_V1:
        for offset, ref in enumerate(series.legacy_refs):
            _validate_legacy_ref(ref, context=f"PP=2 series {series.key}.refs[{offset}]")
    else:
        for offset, ref in enumerate(series.current_refs):
            _validate_current_ref(ref, context=f"PP=2 series {series.key}.refs[{offset}]")


def load_pp405b_points(series: PP405BSeries, *, runs_root: str | Path) -> list[dict[str, Any]]:
    """Load one exact seven-node curve under its explicit evidence contract."""
    _validate_series(series)
    root = Path(runs_root)
    points = []
    if series.loader_kind is PP405BLoaderKind.LEGACY_PINNED_V1:
        for ref in series.legacy_refs:
            cell = root / series.stem / ref.run_group_id / ref.run_id
            plan_path = cell / "run.yaml"
            result_path = cell / "results" / ref.result_name
            stdout_path = cell / "logs" / "pbs" / "stdout" / ref.stdout_name
            plan = _load_sha_bound_yaml(plan_path, ref.run_yaml_sha256, label="legacy run.yaml")
            original_cell = _validate_legacy_plan(plan, path=plan_path, series=series, ref=ref)
            stdout = _sha_bound_bytes(stdout_path, ref.stdout_sha256, label="legacy PBS stdout")
            _validate_producing_stdout(
                stdout,
                path=stdout_path,
                original_cell=original_cell,
                ref=ref,
            )
            result = _load_sha_bound_json(result_path, ref.result_sha256, label="legacy result")
            _, data_runs = _validate_legacy_result(
                result,
                result_path=result_path,
                series=series,
                ref=ref,
            )
            point = _summarize_point(nodes=ref.nodes, data_runs=data_runs)
            if not math.isclose(
                point["srps"],
                ref.expected_successful_rps,
                rel_tol=1e-12,
                abs_tol=1e-15,
            ):
                raise RuntimeError(
                    f"legacy PP=2 successful RPS disagrees with the reviewed value "
                    f"for {series.key}/n{ref.nodes}"
                )
            points.append(point)
        return points

    for ref in series.current_refs:
        result_path = (
            root / series.stem / ref.run_group_id / ref.run_id / "results" / "result0.json"
        )
        accepted, replay_entry = _require_current_result(result_path)
        _validate_current_plan(accepted, series=series, ref=ref)
        from eval.lib.paper_acceptance import load_authenticated_result_json

        document = load_authenticated_result_json(result_path.parents[1], replay_entry)
        _, data_runs = _validate_current_result(document, result_path=result_path, nodes=ref.nodes)
        points.append(_summarize_point(nodes=ref.nodes, data_runs=data_runs))
    return points
