import argparse
import hashlib
import json
import math
import os
import re
import shutil
import socket
import stat
import sys
import tempfile
import time
import uuid
from importlib import resources
from pathlib import Path
from typing import Dict, List, Tuple

from .control.finite_process import FiniteProcessError, run_finite
from .exception_notes import add_exception_note
from .model_paths import get_model_storage_name, get_model_storage_path, iter_unique_model_ids
from .model_staging import (
    COMPLETION_MARKER,
    check_model_exists,
    get_model_dir_state,
    print_red,
    stage_models,
    validate_tensor_parallel_compatibility,
)

_RESULT_ENVELOPE_FIELDS = {"schema_version", "attempt_id", "result_id"}
_CACHE_PROBE_FIELDS = _RESULT_ENVELOPE_FIELDS | {
    "rank",
    "host",
    "path",
    "state",
    "generation",
}
_MODEL_RECEIPT_FIELDS = _RESULT_ENVELOPE_FIELDS | {
    "rank",
    "node",
    "generation",
    "model_id",
    "manifest_hash",
    "file_count",
    "total_bytes",
    "target",
    "verification_duration_s",
}
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}(?:-[A-Za-z0-9_.-]+)?")
_RESULT_ID = re.compile(r"[0-9a-f]{32}")
_MODEL_BCAST_FIELDS = {
    "schema_version",
    "deployment_id",
    "generation",
    "deployment_plan_hash",
    "site_profile_hash",
    "allocation_binding_hash",
    "model_bcast_total_s",
    "model_paths",
    "models",
}
_MODEL_TIMING_FIELDS = {
    "model_id",
    "cache_reused",
    "shard_aware",
    "manifest_hash",
    "stage_manifest_hashes",
    "rank_receipts",
    "duration_s",
}


def _nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def _valid_result_envelope(value: dict) -> bool:
    return (
        type(value["schema_version"]) is int
        and value["schema_version"] == 1
        and isinstance(value["attempt_id"], str)
        and _ATTEMPT_ID.fullmatch(value["attempt_id"]) is not None
        and isinstance(value["result_id"], str)
        and _RESULT_ID.fullmatch(value["result_id"]) is not None
    )


def _validate_cache_probe_result(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != _CACHE_PROBE_FIELDS:
        raise RuntimeError("[ModelBcast] Cache probe result fields are invalid")
    if (
        not _valid_result_envelope(value)
        or not _nonnegative_int(value["rank"])
        or not isinstance(value["host"], str)
        or not value["host"]
        or not isinstance(value["path"], str)
        or not os.path.isabs(value["path"])
        or not isinstance(value["state"], str)
        or value["state"] not in {"missing", "partial", "complete"}
        or not _nonnegative_int(value["generation"])
    ):
        raise RuntimeError("[ModelBcast] Cache probe result values are invalid")
    return value


def _validate_model_receipt(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != _MODEL_RECEIPT_FIELDS:
        raise RuntimeError("[ModelBcast] Model publication receipt fields are invalid")
    duration = value["verification_duration_s"]
    if (
        not _valid_result_envelope(value)
        or not _nonnegative_int(value["rank"])
        or not isinstance(value["node"], str)
        or not value["node"]
        or not _nonnegative_int(value["generation"])
        or not isinstance(value["model_id"], str)
        or not value["model_id"]
        or not isinstance(value["manifest_hash"], str)
        or not _SHA256.fullmatch(value["manifest_hash"])
        or not _nonnegative_int(value["file_count"])
        or not _nonnegative_int(value["total_bytes"])
        or not isinstance(value["target"], str)
        or not os.path.isabs(value["target"])
        or isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
    ):
        raise RuntimeError("[ModelBcast] Model publication receipt values are invalid")
    return value


def _validate_receipt_identity(
    receipts: list[dict],
    *,
    generation: int,
    model_id: str,
    target: str,
) -> None:
    result_ids = {receipt["result_id"] for receipt in receipts}
    if len(result_ids) != len(receipts):
        raise RuntimeError("[ModelBcast] Model receipts contain duplicate result identities")
    for receipt in receipts:
        if (
            receipt["generation"] != generation
            or receipt["model_id"] != model_id
            or receipt["target"] != target
        ):
            raise RuntimeError("[ModelBcast] Model receipt disagrees with aggregate identity")


def _validate_consistent_inventory(receipts: list[dict]) -> None:
    if len({(receipt["file_count"], receipt["total_bytes"]) for receipt in receipts}) != 1:
        raise RuntimeError("[ModelBcast] Model receipts disagree on content inventory")


def validate_model_bcast_result(value: object, *, plan, binding) -> dict:
    """Validate the complete aggregate model-staging evidence boundary."""
    if not isinstance(value, dict) or set(value) != _MODEL_BCAST_FIELDS:
        raise RuntimeError("[ModelBcast] Aggregate result fields are invalid")
    total_s = value["model_bcast_total_s"]
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or not isinstance(value["deployment_id"], str)
        or not value["deployment_id"]
        or not _nonnegative_int(value["generation"])
        or any(
            not isinstance(value[name], str) or not _SHA256.fullmatch(value[name])
            for name in (
                "deployment_plan_hash",
                "site_profile_hash",
                "allocation_binding_hash",
            )
        )
        or isinstance(total_s, bool)
        or not isinstance(total_s, (int, float))
        or not math.isfinite(total_s)
        or total_s < 0
        or not isinstance(value["model_paths"], dict)
        or not isinstance(value["models"], list)
    ):
        raise RuntimeError("[ModelBcast] Aggregate result values are invalid")
    expected_identity = {
        "deployment_id": plan.deployment_id,
        "generation": binding.generation,
        "deployment_plan_hash": plan.deployment_plan_hash,
        "site_profile_hash": plan.site_profile_hash,
        "allocation_binding_hash": binding.allocation_binding_hash,
    }
    mismatches = {
        name: (expected, value[name])
        for name, expected in expected_identity.items()
        if value[name] != expected
    }
    if mismatches:
        raise RuntimeError(f"[ModelBcast] Aggregate result identity mismatch: {mismatches}")

    expected_model_ids = [model.model_id for model in plan.models]
    if set(value["model_paths"]) != set(expected_model_ids):
        raise RuntimeError("[ModelBcast] Aggregate model paths do not cover the exact model set")
    expected_paths = {
        model_id: str(get_model_storage_path(model_id, plan.local_stage_path))
        for model_id in expected_model_ids
    }
    if value["model_paths"] != expected_paths or any(
        not isinstance(path, str) or not os.path.isabs(path)
        for path in value["model_paths"].values()
    ):
        raise RuntimeError("[ModelBcast] Aggregate model paths disagree with the plan")
    if len(value["models"]) != len(plan.models):
        raise RuntimeError("[ModelBcast] Aggregate result does not cover every model")

    from .plan.contracts import same_node

    rank_to_node = tuple(binding.rank_to_node)
    if [rank for rank, _ in rank_to_node] != list(range(plan.num_nodes)):
        raise RuntimeError("[ModelBcast] Allocation binding rank set is invalid")
    for model, item in zip(plan.models, value["models"]):
        if not isinstance(item, dict) or set(item) != _MODEL_TIMING_FIELDS:
            raise RuntimeError("[ModelBcast] Per-model result fields are invalid")
        duration = item["duration_s"]
        if (
            item["model_id"] != model.model_id
            or not isinstance(item["cache_reused"], bool)
            or not isinstance(item["shard_aware"], bool)
            or not isinstance(item["manifest_hash"], str)
            or not _SHA256.fullmatch(item["manifest_hash"])
            or not isinstance(item["stage_manifest_hashes"], list)
            or any(
                not isinstance(digest, str) or not _SHA256.fullmatch(digest)
                for digest in item["stage_manifest_hashes"]
            )
            or not isinstance(item["rank_receipts"], list)
            or not item["rank_receipts"]
            or isinstance(duration, bool)
            or not isinstance(duration, (int, float))
            or not math.isfinite(duration)
            or duration < 0
        ):
            raise RuntimeError("[ModelBcast] Per-model result values are invalid")
        expected_sharded = (
            plan.runtime.pp_shard_aware
            and model.pipeline_parallel_size > 1
            and model.num_replicas > 1
        )
        if item["shard_aware"] is not expected_sharded:
            raise RuntimeError("[ModelBcast] Per-model shard mode disagrees with the plan")
        target = expected_paths[model.model_id]
        if expected_sharded:
            from .pp_stage import _validate_pp_receipt

            stage_hashes = item["stage_manifest_hashes"]
            if len(stage_hashes) != model.pipeline_parallel_size or item["cache_reused"]:
                raise RuntimeError("[ModelBcast] PP stage manifest set is invalid")
            canonical = json.dumps(stage_hashes, separators=(",", ":"))
            if hashlib.sha256(canonical.encode()).hexdigest() != item["manifest_hash"]:
                raise RuntimeError("[ModelBcast] PP aggregate manifest hash is invalid")
            receipts = [_validate_pp_receipt(receipt) for receipt in item["rank_receipts"]]
            if len(receipts) != model.pipeline_parallel_size * model.num_replicas:
                raise RuntimeError("[ModelBcast] PP receipt count is invalid")
            _validate_receipt_identity(
                receipts,
                generation=binding.generation,
                model_id=model.model_id,
                target=target,
            )
            cursor = 0
            for stage, stage_hash in enumerate(stage_hashes):
                stage_receipts = receipts[cursor : cursor + model.num_replicas]
                cursor += model.num_replicas
                expected_nodes = [
                    dict(rank_to_node)[replica.planned_ranks[stage]] for replica in model.replicas
                ]
                if [receipt["rank"] for receipt in stage_receipts] != list(
                    range(model.num_replicas)
                ) or any(
                    receipt["pp_stage"] != stage
                    or receipt["manifest_hash"] != stage_hash
                    or not same_node(receipt["node"], expected_node)
                    for receipt, expected_node in zip(stage_receipts, expected_nodes)
                ):
                    raise RuntimeError("[ModelBcast] PP receipt topology/identity is invalid")
                if len({receipt["attempt_id"] for receipt in stage_receipts}) != 1:
                    raise RuntimeError("[ModelBcast] PP receipts disagree on stage attempt")
                _validate_consistent_inventory(stage_receipts)
        else:
            if item["stage_manifest_hashes"]:
                raise RuntimeError("[ModelBcast] Non-PP result invents stage manifests")
            receipts = [_validate_model_receipt(receipt) for receipt in item["rank_receipts"]]
            if len(receipts) != plan.num_nodes:
                raise RuntimeError("[ModelBcast] Model receipt count is invalid")
            _validate_receipt_identity(
                receipts,
                generation=binding.generation,
                model_id=model.model_id,
                target=target,
            )
            if (
                [receipt["rank"] for receipt in receipts] != list(range(plan.num_nodes))
                or len({receipt["attempt_id"] for receipt in receipts}) != 1
                or any(
                    receipt["manifest_hash"] != item["manifest_hash"]
                    or not same_node(receipt["node"], planned_node)
                    for receipt, (_, planned_node) in zip(receipts, rank_to_node)
                )
            ):
                raise RuntimeError("[ModelBcast] Model receipt topology/identity is invalid")
            _validate_consistent_inventory(receipts)
    return value


def _resource_bytes(name: str) -> bytes:
    return (resources.files("exaserve.resources") / name).read_bytes()


def _write_if_changed(path: Path, data: bytes) -> None:
    from .state.atomic import atomic_create_or_verify_bytes

    atomic_create_or_verify_bytes(path, data)


def _default_bcast_build_dir() -> Path:
    raw = os.environ.get("EXASERVE_BCAST_BUILD_DIR", "").strip()
    if raw:
        return Path(raw)

    run_log_dir = os.environ.get("EXASERVE_RUN_LOG_DIR", "").strip()
    if run_log_dir:
        return Path(run_log_dir) / "bcast_build"

    return Path.cwd() / ".exaserve_build" / "bcast"


def prepare_bcast_tools(build_dir: Path | None = None) -> Path:
    """Materialize the packaged MPI broadcast source into a writable build dir."""
    tools_dir = build_dir or _default_bcast_build_dir()
    tools_dir.mkdir(parents=True, exist_ok=True)
    _write_if_changed(tools_dir / "bcast.c", _resource_bytes("bcast.c"))
    _write_if_changed(tools_dir / "Makefile", _resource_bytes("bcast.Makefile"))
    return tools_dir


def _compile_target(tools_dir: Path, target: str) -> Path:
    binary_path = tools_dir / target
    source_path = tools_dir / f"{target}.c"
    makefile_path = tools_dir / "Makefile"

    if not source_path.is_file():
        raise FileNotFoundError(f"Missing {target} source: {source_path}")
    if not makefile_path.is_file():
        raise FileNotFoundError(f"Missing Makefile: {makefile_path}")

    binary_mtime = binary_path.stat().st_mtime if binary_path.exists() else -1
    source_mtime = max(source_path.stat().st_mtime, makefile_path.stat().st_mtime)
    if binary_mtime < source_mtime:
        print(f"[ModelBcast] Building {binary_path}...", flush=True)
        run_finite(
            ["make", "-C", str(tools_dir), target],
            timeout_s=300,
            check=True,
        )
    return binary_path


def compile_bcast(tools_dir: Path | None = None) -> Path:
    """Build the packaged MPI broadcast helper if the binary is missing or stale."""
    return _compile_target(prepare_bcast_tools(tools_dir), "bcast")


def probe_cache_locally(path: Path, *, generation: int) -> dict:
    """Probe one model directory on the current host."""
    return {
        "rank": _runtime_rank(),
        "host": socket.gethostname(),
        "path": str(path),
        "state": get_model_dir_state(path),
        "generation": generation,
    }


def mpi_launch_prefix(
    num_nodes: int, *, scheduler: str = "pbs", override: str | None = None
) -> List[str]:
    """Per-node launch prefix for a finite, supervisor-owned MPI boundary.

    PBS/PALS uses ``mpiexec`` and Slurm uses ``srun`` (Cray/Slurm sites have no
    mpiexec). A caller may pass an explicit validation override; inherited
    shell state is never a launch-policy source.
    """
    if override:
        import shlex

        return shlex.split(override)
    if scheduler == "slurm":
        return ["srun", f"--nodes={num_nodes}", "--ntasks-per-node=1", "--cpu-bind=none"]
    if scheduler != "pbs":
        raise ValueError(f"unsupported model-broadcast scheduler {scheduler!r}")
    return ["mpiexec", "-n", str(num_nodes), "-ppn", "1", "--cpu-bind", "none"]


def _run_result_root() -> Path:
    raw = os.environ.get("EXASERVE_RUN_LOG_DIR", "").strip()
    if not raw or not os.path.isabs(raw):
        raise RuntimeError("EXASERVE_RUN_LOG_DIR must name the absolute owned run directory")
    return Path(raw)


def run_cache_probe(path: Path, num_nodes: int, *, binding, scheduler: str = "pbs") -> List[dict]:
    """
    Probe the cache state on every allocated node via MPI.
    """
    # Invoke the module via -m so package-relative imports resolve. Running the
    # file by absolute path would set __package__ to None.
    from .staging_results import create_result_dir, load_rank_results

    attempt = uuid.uuid4().hex
    result_dir = create_result_dir(_run_result_root(), "model-cache-probe", attempt)
    cmd = mpi_launch_prefix(num_nodes, scheduler=scheduler) + [
        sys.executable,
        "-m",
        "exaserve.model_bcast",
        "--probe-cache",
        str(path),
        "--generation",
        str(binding.generation),
        "--result-dir",
        str(result_dir),
        "--attempt-id",
        attempt,
    ]
    try:
        result = run_finite(cmd, timeout_s=1800)
    except (OSError, FiniteProcessError) as exc:
        raise RuntimeError(f"[ModelBcast] Cache probe failed for {path}: {exc}") from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"[ModelBcast] Cache probe failed for {path}\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

    entries = [
        _validate_cache_probe_result(entry)
        for entry in load_rank_results(result_dir, attempt_id=attempt)
    ]

    if len(entries) != num_nodes:
        raise RuntimeError(
            f"[ModelBcast] Expected {num_nodes} cache probe result(s) for {path}, "
            f"got {len(entries)}.\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    by_rank = {entry.get("rank"): entry for entry in entries}
    if len(by_rank) != num_nodes or set(by_rank) != set(range(num_nodes)):
        raise RuntimeError("[ModelBcast] Cache probe has missing/duplicate rank identities")
    from .plan.contracts import same_node

    for rank, planned_node in binding.rank_to_node:
        entry = by_rank[rank]
        if (
            not same_node(entry["host"], planned_node)
            or entry.get("generation") != binding.generation
            or entry.get("path") != str(path)
        ):
            raise RuntimeError(f"[ModelBcast] Cache probe rank {rank} has wrong identity")
    return [by_rank[index] for index in range(num_nodes)]


def check_cache_state(
    model_id: str,
    local_stage_path: str,
    num_nodes: int,
    *,
    binding,
    scheduler: str = "pbs",
) -> str:
    """
    Return the aggregate cache state for a model across all nodes.
    """
    target_path = get_model_storage_path(model_id, local_stage_path)
    entries = run_cache_probe(target_path, num_nodes, binding=binding, scheduler=scheduler)
    states = {entry["state"] for entry in entries}

    if states == {"complete"}:
        return "complete"
    if states == {"missing"}:
        return "missing"

    detail = ", ".join(f"{entry['host']}={entry['state']}" for entry in entries)
    raise RuntimeError(
        f"[ModelBcast] Refusing to reuse staged cache for {model_id}. "
        f"Cache state across nodes is inconsistent or partial: {detail}"
    )


def _runtime_rank() -> int:
    for name in ("PALS_RANKID", "PMI_RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK", "PMIX_RANK"):
        if name in os.environ:
            value = os.environ[name]
            try:
                rank = int(value)
            except ValueError as exc:
                raise RuntimeError(f"{name} is not an integer: {value!r}") from exc
            if rank < 0:
                raise RuntimeError(f"{name} must be non-negative: {value!r}")
            return rank
    raise RuntimeError("model verifier has no MPI/srun rank identity")


def _marker(path: Path) -> dict:
    from .model_staging import validate_model_manifest
    from .state.atomic import strict_json_load_path

    marker = path / COMPLETION_MARKER
    try:
        data = strict_json_load_path(marker)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"model completion manifest is unreadable at {path}: {exc}") from exc
    try:
        return validate_model_manifest(data)
    except ValueError as exc:
        raise RuntimeError(f"model completion manifest is invalid at {path}: {exc}") from exc


def verify_and_publish_model(
    candidate: Path, target: Path, *, model_id: str, expected_manifest_hash: str, generation: int
) -> dict:
    from .state.atomic import fsync_directory

    """Validate a node-local candidate, then atomically publish it."""
    started = time.monotonic()
    try:
        candidate_mode = os.lstat(candidate).st_mode
    except OSError as exc:
        raise RuntimeError(f"candidate for {model_id} is unavailable: {candidate}: {exc}") from exc
    if not stat.S_ISDIR(candidate_mode):
        raise RuntimeError(f"candidate for {model_id} must be a real directory: {candidate}")
    if not check_model_exists(candidate):
        raise RuntimeError(f"candidate for {model_id} is incomplete: {candidate}")
    candidate_manifest = _marker(candidate)
    if candidate_manifest["manifest_hash"] != expected_manifest_hash:
        raise RuntimeError(
            f"candidate for {model_id} has manifest "
            f"{candidate_manifest['manifest_hash']}, expected {expected_manifest_hash}"
        )

    target_present = os.path.lexists(target)
    target_is_real_directory = False
    if target_present:
        try:
            target_is_real_directory = stat.S_ISDIR(os.lstat(target).st_mode)
        except OSError as exc:
            raise RuntimeError(f"model target is unavailable at {target}: {exc}") from exc
    if candidate.resolve() != target.resolve() or not target_is_real_directory:
        target.parent.mkdir(parents=True, exist_ok=True)
        if target_present:
            if (
                target_is_real_directory
                and check_model_exists(target)
                and _marker(target)["manifest_hash"] == expected_manifest_hash
            ):
                # A concurrent winner published identical content.
                shutil.rmtree(candidate)
            else:
                quarantine = target.with_name(
                    f".{target.name}.invalid.{generation}.{os.getpid()}.{time.time_ns()}"
                )
                os.rename(target, quarantine)
                try:
                    os.replace(candidate, target)
                    fsync_directory(target.parent)
                except BaseException as exc:
                    try:
                        if not os.path.lexists(target):
                            os.rename(quarantine, target)
                            fsync_directory(target.parent)
                    except OSError as rollback_exc:
                        add_exception_note(
                            exc, f"model publication rollback also failed: {rollback_exc}"
                        )
                    raise
        else:
            os.replace(candidate, target)
            fsync_directory(target.parent)
    if (
        not stat.S_ISDIR(os.lstat(target).st_mode)
        or not check_model_exists(target)
        or _marker(target)["manifest_hash"] != expected_manifest_hash
    ):
        raise RuntimeError(f"atomic model publication failed for {model_id} at {target}")
    return {
        "rank": _runtime_rank(),
        "node": socket.gethostname(),
        "generation": generation,
        "model_id": model_id,
        "manifest_hash": expected_manifest_hash,
        "file_count": candidate_manifest["file_count"],
        "total_bytes": candidate_manifest["total_bytes"],
        "target": str(target),
        "verification_duration_s": round(time.monotonic() - started, 6),
    }


def _model_receipts(
    *,
    candidate: Path,
    target: Path,
    model_id: str,
    manifest_hash: str,
    generation: int,
    num_nodes: int,
    binding,
    scheduler: str,
) -> list[dict]:
    from .plan.contracts import same_node
    from .staging_results import create_result_dir, load_rank_results

    attempt = uuid.uuid4().hex
    category = f"model-publish-{hashlib.sha256(model_id.encode()).hexdigest()[:12]}"
    result_dir = create_result_dir(_run_result_root(), category, attempt)

    command = mpi_launch_prefix(num_nodes, scheduler=scheduler) + [
        sys.executable,
        "-m",
        "exaserve.model_bcast",
        "--verify-model-candidate",
        str(candidate),
        "--publish-target",
        str(target),
        "--model-id",
        model_id,
        "--expected-manifest-hash",
        manifest_hash,
        "--generation",
        str(generation),
        "--result-dir",
        str(result_dir),
        "--attempt-id",
        attempt,
    ]
    try:
        result = run_finite(command, timeout_s=1800)
    except (OSError, FiniteProcessError) as exc:
        raise RuntimeError(f"model publication command failed: {exc}") from exc
    if result.stdout:
        print(result.stdout, end="" if result.stdout.endswith("\n") else "\n", flush=True)
    if result.stderr:
        print(
            result.stderr,
            end="" if result.stderr.endswith("\n") else "\n",
            file=sys.stderr,
            flush=True,
        )
    if result.returncode:
        raise RuntimeError(f"model publication for {model_id} exited {result.returncode}")
    receipts = [
        _validate_model_receipt(receipt)
        for receipt in load_rank_results(result_dir, attempt_id=attempt)
    ]
    by_rank = {item.get("rank"): item for item in receipts}
    if (
        len(receipts) != num_nodes
        or len(by_rank) != num_nodes
        or set(by_rank) != set(range(num_nodes))
    ):
        raise RuntimeError(
            f"model publication for {model_id} returned an incomplete/duplicate "
            f"receipt set ({len(receipts)} for {num_nodes} ranks)"
        )
    for rank, planned_node in binding.rank_to_node:
        receipt = by_rank[rank]
        if not same_node(receipt["node"], planned_node):
            raise RuntimeError(
                f"model receipt rank {rank} came from {receipt.get('node')!r}, "
                f"expected {planned_node!r}"
            )
        if (
            receipt.get("generation") != generation
            or receipt.get("model_id") != model_id
            or receipt.get("manifest_hash") != manifest_hash
        ):
            raise RuntimeError(f"model receipt rank {rank} has stale/wrong identity")
    return [by_rank[index] for index in range(num_nodes)]


def bcast_models(
    model_configs,
    lustre_path: str,
    local_path: str,
    num_nodes: int,
    *,
    shard_aware: bool = False,
    binding=None,
    deployment_plan=None,
    scheduler: str = "pbs",
) -> Tuple[Dict[str, str], list[dict]]:
    """
    Ensure every model exists on Lustre, then broadcast it to local storage.
    """
    if binding is None or len(binding.rank_to_node) != num_nodes:
        raise RuntimeError("model staging requires the exact AllocationBinding")
    binary_path = compile_bcast()

    lustre_model_paths = stage_models(model_configs, lustre_path)
    local_model_paths: Dict[str, str] = {}

    per_model_timings: list[dict] = []

    for model_id in iter_unique_model_ids(model_configs):
        model_t0 = time.monotonic()
        model_config = next(cfg for cfg in model_configs if cfg.model_id == model_id)
        # Shard-aware PP is still an outer, pre-launch transaction.  Allocation
        # binding order is the canonical node<->stage map, so Ray discovery is
        # neither necessary nor allowed to become a second staging authority.
        if (
            shard_aware
            and model_config.pipeline_parallel_size > 1
            and (model_config.num_replicas or 0) > 1
        ):
            from .pp_stage import stage_pp_sharded

            if deployment_plan is None:
                raise RuntimeError("shard-aware staging requires the canonical DeploymentPlan")
            plan_models = [model for model in deployment_plan.models if model.model_id == model_id]
            if len(plan_models) != 1:
                raise RuntimeError(
                    f"canonical plan has {len(plan_models)} entries for {model_id!r}"
                )
            plan_model = plan_models[0]
            if plan_model.num_replicas != int(
                model_config.num_replicas
            ) or plan_model.pipeline_parallel_size != int(model_config.pipeline_parallel_size):
                raise RuntimeError(
                    f"legacy staging view drifted from canonical plan for {model_id}"
                )

            source_path = Path(lustre_model_paths[model_id])
            validate_tensor_parallel_compatibility(
                model_id, source_path, model_config.tensor_parallel_size
            )
            safe_name = get_model_storage_name(model_id)
            by_rank = dict(binding.rank_to_node)
            ordered_nodes = [
                by_rank[rank] for replica in plan_model.replicas for rank in replica.planned_ranks
            ]
            needed = int(model_config.num_replicas) * int(model_config.pipeline_parallel_size)
            if len(ordered_nodes) != needed:
                raise RuntimeError(
                    f"shard-aware PP {model_id} needs {needed} bound nodes, "
                    f"found {len(ordered_nodes)}"
                )
            pp_plan = stage_pp_sharded(
                source_path,
                safe_name,
                Path(lustre_path) / "_pp_stage" / safe_name,
                local_path,
                model_config.pipeline_parallel_size,
                ordered_nodes,
                int(model_config.num_replicas),
                binary_path,
                generation=binding.generation,
                scheduler=scheduler,
                model_id=model_id,
            )
            receipts = [receipt for item in pp_plan for receipt in item["receipts"]]
            local_model_paths[model_id] = str(get_model_storage_path(model_id, local_path))
            per_model_timings.append(
                {
                    "model_id": model_id,
                    "cache_reused": False,
                    "shard_aware": True,
                    "manifest_hash": hashlib.sha256(
                        json.dumps(
                            [item["manifest_hash"] for item in pp_plan], separators=(",", ":")
                        ).encode()
                    ).hexdigest(),
                    "stage_manifest_hashes": [item["manifest_hash"] for item in pp_plan],
                    "rank_receipts": receipts,
                    "duration_s": round(time.monotonic() - model_t0, 4),
                }
            )
            continue
        validate_tensor_parallel_compatibility(
            model_id,
            Path(lustre_model_paths[model_id]),
            model_config.tensor_parallel_size,
        )
        target_path = get_model_storage_path(model_id, local_path)
        cache_state = check_cache_state(
            model_id,
            local_path,
            num_nodes,
            binding=binding,
            scheduler=scheduler,
        )

        source_path = Path(lustre_model_paths[model_id])
        safe_name = get_model_storage_name(model_id)
        source_manifest = _marker(source_path)
        manifest_hash = source_manifest["manifest_hash"]

        if cache_state == "complete":
            receipts = _model_receipts(
                candidate=target_path,
                target=target_path,
                model_id=model_id,
                manifest_hash=manifest_hash,
                generation=binding.generation,
                num_nodes=num_nodes,
                binding=binding,
                scheduler=scheduler,
            )
            print(
                f"[ModelBcast] ✓ Reusing verified staged cache for {model_id} at {target_path}",
                flush=True,
            )
            local_model_paths[model_id] = str(target_path)
            per_model_timings.append(
                {
                    "model_id": model_id,
                    "cache_reused": True,
                    "shard_aware": False,
                    "manifest_hash": manifest_hash,
                    "stage_manifest_hashes": [],
                    "rank_receipts": receipts,
                    "duration_s": round(time.monotonic() - model_t0, 4),
                }
            )
            continue

        candidate_root = Path(local_path) / (
            f".exaserve_stage.{safe_name}.{binding.generation}.{uuid.uuid4().hex}"
        )
        candidate_model = candidate_root / safe_name

        # HF cache snapshots use a revision hash as the directory name. Create a
        # temporary symlink with the stable model cache name so the extracted
        # node-local directory always lands at <local_stage_path>/<safe_name>.
        with tempfile.TemporaryDirectory(prefix=f"model-bcast-{safe_name}-") as tmpdir:
            bcast_source = source_path
            if source_path.name != safe_name:
                symlink_path = Path(tmpdir) / safe_name
                symlink_path.symlink_to(source_path, target_is_directory=True)
                bcast_source = symlink_path

            print(
                f"[ModelBcast] Broadcasting {model_id} from {source_path} to {target_path} "
                f"across {num_nodes} node(s)...",
                flush=True,
            )
            run_finite(
                mpi_launch_prefix(num_nodes, scheduler=scheduler)
                + [
                    str(binary_path),
                    str(bcast_source),
                    str(candidate_root),
                ],
                timeout_s=1800,
                check=True,
            )
        receipts = _model_receipts(
            candidate=candidate_model,
            target=target_path,
            model_id=model_id,
            manifest_hash=manifest_hash,
            generation=binding.generation,
            num_nodes=num_nodes,
            binding=binding,
            scheduler=scheduler,
        )
        print_red(f"[ModelBcast] ✓ Broadcast complete for {model_id}")
        local_model_paths[model_id] = str(target_path)
        per_model_timings.append(
            {
                "model_id": model_id,
                "cache_reused": False,
                "shard_aware": False,
                "manifest_hash": manifest_hash,
                "stage_manifest_hashes": [],
                "rank_receipts": receipts,
                "duration_s": round(time.monotonic() - model_t0, 4),
            }
        )

    return local_model_paths, per_model_timings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MPI model staging helper for Aurora")
    parser.add_argument("--plan", help="Verified canonical DeploymentPlan artifact")
    parser.add_argument("--binding", help="Verified AllocationBinding artifact")
    parser.add_argument("--num-nodes", type=int, help="Allocated node count")
    parser.add_argument(
        "--probe-cache",
        help="Internal mode: print local cache state for one model path as JSON",
    )
    parser.add_argument("--verify-model-candidate")
    parser.add_argument("--publish-target")
    parser.add_argument("--model-id")
    parser.add_argument("--expected-manifest-hash")
    parser.add_argument("--generation", type=int)
    parser.add_argument("--result-dir")
    parser.add_argument("--attempt-id")
    args = parser.parse_args(argv)

    if args.probe_cache:
        if args.generation is None or not args.result_dir or not args.attempt_id:
            raise SystemExit("cache probe requires generation/result-dir/attempt-id")
        payload = probe_cache_locally(Path(args.probe_cache), generation=args.generation)
        from .staging_results import write_rank_result

        write_rank_result(args.result_dir, attempt_id=args.attempt_id, payload=payload)
        return 0

    if args.verify_model_candidate:
        required = (
            args.publish_target,
            args.model_id,
            args.expected_manifest_hash,
            args.generation,
            args.result_dir,
            args.attempt_id,
        )
        if any(value is None for value in required):
            raise SystemExit("candidate verification requires target/model/hash/generation")
        receipt = verify_and_publish_model(
            Path(args.verify_model_candidate),
            Path(args.publish_target),
            model_id=args.model_id,
            expected_manifest_hash=args.expected_manifest_hash,
            generation=args.generation,
        )
        from .staging_results import write_rank_result

        write_rank_result(args.result_dir, attempt_id=args.attempt_id, payload=receipt)
        return 0

    if not args.plan:
        raise SystemExit("--plan is required")
    if not args.binding:
        raise SystemExit("--binding is required")
    if args.num_nodes is None or args.num_nodes < 1:
        raise SystemExit("--num-nodes must be >= 1")

    from .plan.io import load_allocation_binding, load_deployment_plan

    plan = load_deployment_plan(args.plan)
    binding = load_allocation_binding(args.binding)
    if (
        binding.deployment_plan_hash != plan.deployment_plan_hash
        or binding.site_profile_hash != plan.site_profile_hash
        or os.environ.get("EXASERVE_GENERATION") != str(binding.generation)
    ):
        raise SystemExit("AllocationBinding does not belong to model staging plan")
    # Allocation oversubscription belongs to SchedulerPlan.reservation_topology;
    # the deployment helper must see exactly the bound rank set.
    shard_aware = plan.runtime.pp_shard_aware
    if args.num_nodes != plan.num_nodes:
        raise SystemExit(
            f"--num-nodes ({args.num_nodes}) does not match plan.num_nodes ({plan.num_nodes})"
        )

    print(
        f"[ModelBcast] Preparing {len(list(iter_unique_model_ids(plan.models)))} unique "
        f"model(s) for {args.num_nodes} node(s)",
        flush=True,
    )
    overall_start = time.monotonic()
    local_model_paths, per_model_timings = bcast_models(
        plan.models,
        plan.model_storage_path,
        plan.local_stage_path,
        args.num_nodes,
        shard_aware=shard_aware,
        binding=binding,
        deployment_plan=plan,
        scheduler=plan.scale_envelope.scheduler_type,
    )
    overall_s = round(time.monotonic() - overall_start, 4)

    timing = validate_model_bcast_result(
        {
            "schema_version": 1,
            "deployment_id": plan.deployment_id,
            "generation": binding.generation,
            "deployment_plan_hash": plan.deployment_plan_hash,
            "site_profile_hash": plan.site_profile_hash,
            "allocation_binding_hash": binding.allocation_binding_hash,
            "model_bcast_total_s": overall_s,
            "model_paths": local_model_paths,
            "models": per_model_timings,
        },
        plan=plan,
        binding=binding,
    )
    run_log_dir = os.environ.get("EXASERVE_RUN_LOG_DIR", "")
    if not run_log_dir or not os.path.isabs(run_log_dir):
        raise SystemExit("EXASERVE_RUN_LOG_DIR must name the absolute owned run directory")
    timing_path = os.path.join(run_log_dir, "model_bcast_timing.json")
    from .state.atomic import atomic_create_json

    os.makedirs(os.path.dirname(timing_path), exist_ok=True)
    atomic_create_json(timing_path, timing)
    print(
        f"[ModelBcast] Timing: {overall_s:.1f}s total, {len(per_model_timings)} model(s)",
        flush=True,
    )
    print_red("[ModelBcast] ✓ All models are ready in node-local storage")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
