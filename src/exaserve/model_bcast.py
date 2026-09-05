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
from collections.abc import Mapping
from importlib import resources
from pathlib import Path
from typing import Dict, List, Tuple

from .control.finite_process import FiniteProcessError, run_finite
from .exception_notes import add_exception_note
from .model_paths import get_model_storage_name, get_model_storage_path, iter_unique_model_ids
from .model_staging import (
    COMPLETION_MARKER,
    check_model_exists,
    configured_shared_roots,
    content_addressed_model_path,
    ensure_node_local_directory,
    get_model_dir_state,
    print_red,
    stage_models,
    validate_node_local_root,
    validate_node_local_tree,
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
_CACHE_CLEAN_FIELDS = _RESULT_ENVELOPE_FIELDS | {
    "rank",
    "node",
    "generation",
    "targets",
    "removed_paths",
    "cleanup_duration_s",
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
    "model_device_id",
    "model_fs_type",
    "verification_duration_s",
}
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}(?:-[A-Za-z0-9_.-]+)?")
_RESULT_ID = re.compile(r"[0-9a-f]{32}")
_SHARED_FS_TYPES = frozenset({"lustre", "nfs", "nfs4", "gpfs", "beegfs", "cifs"})
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
    "cleanup_receipts",
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


def _validate_cache_clean_result(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != _CACHE_CLEAN_FIELDS:
        raise RuntimeError("[ModelBcast] Cache cleanup result fields are invalid")
    duration = value["cleanup_duration_s"]
    if (
        not _valid_result_envelope(value)
        or not _nonnegative_int(value["rank"])
        or not isinstance(value["node"], str)
        or not value["node"]
        or not _nonnegative_int(value["generation"])
        or not isinstance(value["targets"], list)
        or any(not isinstance(path, str) or not os.path.isabs(path) for path in value["targets"])
        or len(value["targets"]) != len(set(value["targets"]))
        or not isinstance(value["removed_paths"], list)
        or any(
            not isinstance(path, str) or not os.path.isabs(path) for path in value["removed_paths"]
        )
        or len(value["removed_paths"]) != len(set(value["removed_paths"]))
        or isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
    ):
        raise RuntimeError("[ModelBcast] Cache cleanup result values are invalid")
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
        or type(value["model_device_id"]) is not int
        or value["model_device_id"] < 0
        or not isinstance(value["model_fs_type"], str)
        or not value["model_fs_type"]
        or value["model_fs_type"].lower() in _SHARED_FS_TYPES
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
        or value["schema_version"] != 2
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
        or not isinstance(value["cleanup_receipts"], list)
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
    if any(
        not isinstance(path, str) or not os.path.isabs(path)
        for path in value["model_paths"].values()
    ):
        raise RuntimeError("[ModelBcast] Aggregate model paths must be absolute")
    if len(value["models"]) != len(plan.models):
        raise RuntimeError("[ModelBcast] Aggregate result does not cover every model")

    from .plan.contracts import same_node

    rank_to_node = tuple(binding.rank_to_node)
    if [rank for rank, _ in rank_to_node] != list(range(plan.num_nodes)):
        raise RuntimeError("[ModelBcast] Allocation binding rank set is invalid")
    cleanup_receipts = [_validate_cache_clean_result(item) for item in value["cleanup_receipts"]]
    expected_clean = bool(getattr(plan.runtime, "clean_stage", False))
    if bool(cleanup_receipts) != expected_clean or (
        cleanup_receipts
        and [item["rank"] for item in cleanup_receipts] != list(range(plan.num_nodes))
    ):
        raise RuntimeError("[ModelBcast] Clean-stage aggregate receipt set is invalid")
    for receipt, (_rank, planned_node) in zip(cleanup_receipts, rank_to_node):
        if (
            not same_node(receipt["node"], planned_node)
            or receipt["generation"] != binding.generation
            or receipt["targets"]
            or receipt["removed_paths"]
        ):
            raise RuntimeError("[ModelBcast] Clean-stage aggregate receipt identity is invalid")
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
        expected_sharded = model.pipeline_parallel_size > 1
        if item["shard_aware"] is not expected_sharded:
            raise RuntimeError("[ModelBcast] Per-model shard mode disagrees with the plan")
        target = str(
            content_addressed_model_path(
                model.model_id, plan.local_stage_path, item["manifest_hash"]
            )
        )
        if value["model_paths"][model.model_id] != target:
            raise RuntimeError("[ModelBcast] Aggregate model path is not content-addressed")
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
                expected_stage = sorted(
                    (
                        replica.planned_ranks[stage],
                        dict(rank_to_node)[replica.planned_ranks[stage]],
                    )
                    for replica in model.replicas
                )
                expected_ranks = [rank for rank, _node in expected_stage]
                expected_nodes = [node for _rank, node in expected_stage]
                if [receipt["rank"] for receipt in stage_receipts] != expected_ranks or any(
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
            expected_ranks = sorted(
                {rank for replica in model.replicas for rank in replica.planned_ranks}
            )
            if len(receipts) != len(expected_ranks):
                raise RuntimeError("[ModelBcast] Model receipt count is invalid")
            _validate_receipt_identity(
                receipts,
                generation=binding.generation,
                model_id=model.model_id,
                target=target,
            )
            if (
                [receipt["rank"] for receipt in receipts] != expected_ranks
                or len({receipt["attempt_id"] for receipt in receipts}) != 1
                or any(
                    receipt["manifest_hash"] != item["manifest_hash"]
                    or not same_node(receipt["node"], dict(rank_to_node)[receipt["rank"]])
                    for receipt in receipts
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
    """Return a head-local, content-addressed build directory.

    PALS transfers the resulting executable to every rank.  Building beneath
    the shared run directory would make the executable itself a worker-side
    Lustre dependency before the distribution invariant can be established.
    """

    raw = os.environ.get("EXASERVE_BCAST_BUILD_DIR", "").strip()
    if raw:
        root = Path(raw)
        if not root.is_absolute():
            raise RuntimeError("EXASERVE_BCAST_BUILD_DIR must be absolute and node-local")
        from .model_staging import configured_shared_roots

        resolved = Path(os.path.realpath(root))
        if any(resolved.is_relative_to(shared) for shared in configured_shared_roots()):
            raise RuntimeError("EXASERVE_BCAST_BUILD_DIR resolves beneath shared storage")
        return root
    identity = hashlib.sha256(
        _resource_bytes("bcast.c") + b"\0" + _resource_bytes("bcast.Makefile")
    ).hexdigest()
    return Path("/tmp/exaserve/bootstrap") / f"bcast-{os.getuid()}" / identity


def prepare_bcast_tools(build_dir: Path | None = None, *, site_profile=None) -> Path:
    """Materialize the packaged MPI broadcast source into a writable build dir."""
    tools_dir = build_dir or _default_bcast_build_dir()
    try:
        if site_profile is not None:
            from .plan.runtime_environment import shared_roots, validate_declared_filesystem
            from .site import require_complete_filesystem_policy

            declared_shared = tuple(shared_roots(site_profile))
            require_complete_filesystem_policy(site_profile)
            if (
                validate_declared_filesystem(tools_dir, policy=site_profile, root_kind="local_root")
                is None
            ):
                raise RuntimeError(
                    f"bcast build directory has no declared local filesystem identity: {tools_dir}"
                )
        else:
            declared_shared = configured_shared_roots()
        tools_dir = ensure_node_local_directory(tools_dir, shared_roots=declared_shared)
        if site_profile is not None:
            if (
                validate_declared_filesystem(tools_dir, policy=site_profile, root_kind="local_root")
                is None
            ):
                raise RuntimeError(
                    f"bcast build directory has no declared local filesystem identity: {tools_dir}"
                )
    except (RuntimeError, ValueError) as exc:
        raise RuntimeError(f"bcast build directory is not proven node-local: {exc}") from exc
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


def compile_bcast(tools_dir: Path | None = None, *, site_profile=None) -> Path:
    """Build the packaged MPI broadcast helper if the binary is missing or stale."""
    return _compile_target(
        prepare_bcast_tools(tools_dir, site_profile=site_profile),
        "bcast",
    )


def resolve_bcast_executable(*, scheduler: str, site_profile=None) -> Path:
    """Resolve a bootstrap that workers can execute without a shared open.

    On PBS/PALS the helper is compiled on allocation-head local storage and
    ``mpiexec --transfer`` moves it before exec.  Other schedulers need an
    explicitly qualified immutable site-local executable; silently assuming a
    head-local path is visible remotely would violate the distribution
    contract.
    """

    runtime_root = os.environ.get("EXASERVE_LOCAL_RUNTIME_ROOT", "").strip()
    if runtime_root:
        local = Path(runtime_root) / "bin" / "bcast"
        try:
            validate_node_local_tree(Path(runtime_root), local_root=Path(runtime_root).parents[2])
            metadata = os.lstat(local)
        except (OSError, ValueError) as exc:
            raise RuntimeError(f"staged bcast executable is not qualified: {exc}") from exc
        if not stat.S_ISREG(metadata.st_mode) or not os.access(local, os.X_OK):
            raise RuntimeError("staged bcast executable must be an executable regular file")
        return local
    configured = os.environ.get("EXASERVE_QUALIFIED_BCAST", "").strip()
    if configured:
        if site_profile is None:
            raise RuntimeError(
                "EXASERVE_QUALIFIED_BCAST requires a verified SiteProfile qualification"
            )
        from .site import qualify_site_local_bootstrap

        try:
            return Path(qualify_site_local_bootstrap(configured, site_profile))
        except RuntimeError as exc:
            raise RuntimeError(f"qualified bcast executable failed site evidence: {exc}") from exc
    if scheduler == "pbs":
        return compile_bcast(site_profile=site_profile)
    raise RuntimeError(
        f"scheduler {scheduler!r} has no qualified executable-transfer contract; "
        "configure EXASERVE_QUALIFIED_BCAST"
    )


def probe_cache_locally(
    path: Path,
    *,
    generation: int,
    local_root: Path | None = None,
    expected_manifest_hash: str | None = None,
) -> dict:
    """Probe one model directory on the current host."""
    state = get_model_dir_state(path)
    if local_root is not None:
        try:
            validate_node_local_tree(path, local_root=local_root, require_immutable=True)
            marker = _marker(path) if state == "complete" else None
        except (OSError, ValueError, RuntimeError):
            state = "partial" if os.path.lexists(path) else "missing"
            marker = None
        if (
            state == "complete"
            and expected_manifest_hash is not None
            and marker["manifest_hash"] != expected_manifest_hash
        ):
            state = "partial"
    receipt = {
        "rank": _runtime_rank(),
        "host": socket.gethostname(),
        "path": str(path),
        "state": state,
        "generation": generation,
    }
    return receipt


def mpi_launch_prefix(
    num_nodes: int,
    *,
    application_cwd: str | os.PathLike,
    scheduler: str = "pbs",
    transfer_executable: bool = False,
    application_environment: Mapping[str, str] | None = None,
) -> List[str]:
    """Per-node launch prefix for a finite, supervisor-owned MPI boundary.

    PBS/PALS uses ``mpiexec`` and Slurm uses ``srun`` (Cray/Slurm sites have no
    mpiexec). Inherited shell state is never a launch-policy source.
    """
    environment = dict(application_environment or {})
    cwd = Path(application_cwd)
    if not cwd.is_absolute() or ".." in cwd.parts:
        raise ValueError("MPI application cwd must be absolute and normalized")
    env_name = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
    if any(
        not isinstance(name, str)
        or env_name.fullmatch(name) is None
        or not isinstance(value, str)
        or "\x00" in value
        or "\n" in value
        for name, value in environment.items()
    ):
        raise ValueError("MPI application environment must contain safe string pairs")
    if scheduler == "slurm":
        exports = ["NONE", *(f"{name}={value}" for name, value in sorted(environment.items()))]
        if any("," in item for item in exports[1:]):
            raise ValueError("Slurm application environment values may not contain commas")
        return [
            "srun",
            f"--nodes={num_nodes}",
            "--ntasks-per-node=1",
            "--cpu-bind=none",
            f"--chdir={cwd}",
            f"--export={','.join(exports)}",
        ]
    if scheduler != "pbs":
        raise ValueError(f"unsupported model-broadcast scheduler {scheduler!r}")
    # PALS documents executable transfer but its default is not an adequate
    # correctness contract. Request it explicitly so a helper built under the
    # allocation head's /tmp is copied before any worker execs it.
    # PALS parses these as stateful options: both inherited-environment
    # suppressors must precede the explicit application allowlist.
    prefix = ["mpiexec", "--genvnone", "--envnone"]
    for name, value in sorted(environment.items()):
        prefix.extend(("--genv", f"{name}={value}"))
    if transfer_executable:
        prefix.append("--transfer")
    return [
        *prefix,
        "-n",
        str(num_nodes),
        "-ppn",
        "1",
        "--cpu-bind",
        "none",
        "--wdir",
        str(cwd),
    ]


def bootstrap_application_environment(
    *,
    profile=None,
    base_environment: Mapping[str, str] | None = None,
    additions: Mapping[str, str] | None = None,
) -> dict[str, str]:
    """Project the finite MPI application's explicit pre-capsule environment.

    Scheduler state remains in the head-side ``mpiexec`` environment. PALS
    injects its rank/PMI state itself; only qualified loader paths and bounded
    fabric controls cross the ``--genvnone --envnone`` application boundary.
    """

    from .plan.runtime_environment import path_is_shared
    from .site import AURORA_PMIX_PREPARED_ENVIRONMENT, declared_site_local_roots

    source = os.environ if base_environment is None else base_environment
    site_roots = declared_site_local_roots(profile) if profile is not None else ()
    runtime_root = source.get("EXASERVE_LOCAL_RUNTIME_ROOT", "")
    allowed_roots = [*site_roots]
    if runtime_root:
        allowed_roots.append(Path(runtime_root))

    # PMIx constructs ``$HOME/.pmix/components`` before the application can
    # sanitize itself.  Pre-capsule ranks therefore need an explicit
    # node-local home even when no Python runtime state exists yet.  Verified
    # post-capsule callers override these defaults through ``additions``.
    result: dict[str, str] = {"HOME": "/tmp", "TMPDIR": "/tmp"}
    loader_entries = []
    for entry in source.get("LD_LIBRARY_PATH", "").split(os.pathsep):
        if not entry or not os.path.isabs(entry) or path_is_shared(entry, profile):
            continue
        resolved = Path(os.path.realpath(entry))
        if any(resolved == root or resolved.is_relative_to(root) for root in allowed_roots):
            loader_entries.append(str(resolved))
    if loader_entries:
        result["LD_LIBRARY_PATH"] = os.pathsep.join(dict.fromkeys(loader_entries))

    allowed_prefixes = ("FI_", "MPICH_", "CXI_", "UCX_", "PSM2_")

    def names_shared_path(value: str) -> bool:
        return any(
            item.startswith("/") and path_is_shared(item, profile)
            for item in value.split(os.pathsep)
        )

    for name, value in source.items():
        if not name.startswith(allowed_prefixes):
            continue
        if names_shared_path(value):
            continue
        if value.startswith("/"):
            resolved = Path(os.path.realpath(value))
            if not any(resolved == root or resolved.is_relative_to(root) for root in allowed_roots):
                continue
            value = str(resolved)
        result[name] = value

    # PMIx otherwise consults ~/.pmix before the application can sanitize its
    # own environment. Only these two SiteProfile-owned search controls cross
    # the env-none MPI boundary; ambient PMIX rank/state variables never do.
    prepared = dict(getattr(profile, "prepared_environment", ()))
    for name, expected in AURORA_PMIX_PREPARED_ENVIRONMENT:
        value = prepared.get(name)
        if value is None:
            continue
        if value != expected or names_shared_path(value):
            raise RuntimeError(f"bootstrap PMIx setting {name} is not Aurora-qualified")
        result[name] = value

    for name, value in dict(additions or {}).items():
        if names_shared_path(value):
            raise RuntimeError(f"bootstrap application environment {name} names shared storage")
        result[name] = value
    return result


def cleanup_native_candidates(
    binary: Path,
    candidate: Path,
    *,
    local_root: Path,
    num_nodes: int,
    binding,
    scheduler: str,
    application_cwd: Path,
    application_environment: Mapping[str, str],
    recipient_ranks: list[int] | None = None,
    transfer_executable: bool = False,
    timeout_s: float = 300.0,
) -> None:
    """Boundedly remove one exact node-local candidate after native failure."""

    root = validate_node_local_root(local_root)
    if candidate == root or not candidate.is_absolute() or not candidate.is_relative_to(root):
        raise RuntimeError(f"native cleanup candidate is outside its local root: {candidate}")
    relative = candidate.relative_to(root).as_posix()
    owned_patterns = (
        r"candidates/g[0-9]+/source\.[0-9a-f]{32}",
        r"\.exaserve_stage\.[A-Za-z0-9_.-]+\.[0-9]+\.[0-9a-f]{32}",
        r"\.exaserve_pp_candidate\.[A-Za-z0-9_.-]+\.[0-9]+\.[0-9a-f]{32}\.stage[0-9]+",
    )
    if not any(re.fullmatch(pattern, relative) for pattern in owned_patterns):
        raise RuntimeError(
            f"native cleanup path is not an exact transaction candidate: {candidate}"
        )
    command = [
        *mpi_launch_prefix(
            num_nodes,
            scheduler=scheduler,
            application_cwd=application_cwd,
            application_environment=application_environment,
            transfer_executable=transfer_executable,
        ),
        str(binary),
        "--expected-root-host",
        dict(binding.rank_to_node)[0],
        "--expected-world-size",
        str(num_nodes),
    ]
    if recipient_ranks is not None:
        command.extend(("--recipients", ",".join(str(rank) for rank in recipient_ranks)))
    command.extend(("--cleanup-root", str(root), "--cleanup", str(candidate)))
    result = run_finite(command, timeout_s=timeout_s)
    if result.returncode:
        raise RuntimeError(
            f"native candidate cleanup exited {result.returncode}: {result.stderr.strip()}"
        )


def _collective_python_environment() -> tuple[Path, dict[str, str], dict[str, str]]:
    from .source_staging import _qualified_python

    runtime_root = os.environ.get("EXASERVE_LOCAL_RUNTIME_ROOT", "").strip()
    if not runtime_root:
        raise RuntimeError("EXASERVE_LOCAL_RUNTIME_ROOT is required for model collectives")
    runtime = Path(runtime_root)
    try:
        validate_node_local_tree(runtime, local_root=runtime.parents[2])
    except ValueError as exc:
        raise RuntimeError(f"model collective runtime is not proven node-local: {exc}") from exc
    launcher_environment = os.environ.copy()
    for name in ("PYTHONHOME", "VIRTUAL_ENV", "CONDA_PREFIX"):
        launcher_environment.pop(name, None)
    state_root = os.environ.get("EXASERVE_LOCAL_STATE_ROOT", "").strip()
    if not state_root:
        raise RuntimeError("EXASERVE_LOCAL_STATE_ROOT is required for model collectives")
    state = Path(state_root)
    additions = {
        "PYTHONNOUSERSITE": "1",
        "PYTHONSAFEPATH": "1",
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONPYCACHEPREFIX": str(state / "cache" / "pycache"),
        "PYTHONPATH": str(runtime / "python"),
        "HOME": str(state / "home"),
        "TMPDIR": str(state / "tmp"),
        "TMP": str(state / "tmp"),
        "TEMP": str(state / "tmp"),
        "XDG_CACHE_HOME": str(state / "cache"),
        "XDG_CONFIG_HOME": str(state / "config"),
        "XDG_DATA_HOME": str(state / "data"),
        "XDG_RUNTIME_DIR": str(state / "tmp"),
        "NUMBA_CACHE_DIR": str(state / "cache" / "numba"),
        "TORCH_EXTENSIONS_DIR": str(state / "cache" / "torch_extensions"),
        "MPLCONFIGDIR": str(state / "cache" / "matplotlib"),
        "IPYTHONDIR": str(state / "cache" / "ipython"),
        "JUPYTER_CONFIG_DIR": str(state / "config" / "jupyter"),
        "HF_HOME": str(state / "cache" / "huggingface"),
        "HF_HUB_CACHE": str(state / "cache" / "huggingface" / "hub"),
        "HUGGINGFACE_HUB_CACHE": str(state / "cache" / "huggingface" / "hub"),
        "TRANSFORMERS_CACHE": str(state / "cache" / "huggingface" / "hub"),
        "TORCH_HOME": str(state / "cache" / "torch"),
        "TRITON_CACHE_DIR": str(state / "cache" / "triton"),
        "VLLM_CACHE_ROOT": str(state / "cache" / "vllm"),
        "EXASERVE_LOCAL_RUNTIME_ROOT": str(runtime),
        "EXASERVE_LOCAL_STATE_ROOT": str(state),
        "EXASERVE_QUALIFIED_PYTHON": os.environ.get("EXASERVE_QUALIFIED_PYTHON", ""),
    }
    from .plan.io import load_site_profile

    profile = load_site_profile(str(runtime / "run" / "site.profile.json"))
    application_environment = bootstrap_application_environment(
        profile=profile,
        base_environment=launcher_environment,
        additions=additions,
    )
    return _qualified_python(profile), launcher_environment, application_environment


def _head_result_root() -> Path:
    """Resolve the head-owned durable result root; workers never call this."""

    raw = os.environ.get("EXASERVE_RUN_LOG_DIR", "").strip()
    if not raw or not os.path.isabs(raw):
        raise RuntimeError("EXASERVE_RUN_LOG_DIR must name the absolute head-owned run directory")
    return Path(raw)


def _run_python_collective(
    arguments: list[str],
    *,
    module: str = "exaserve.model_bcast",
    attempt_id: str,
    num_nodes: int,
    binding,
    scheduler: str,
    timeout_s: float = 1800.0,
) -> list[dict]:
    from .staging_results import load_collective_results

    python, launcher_environment, application_environment = _collective_python_environment()
    root_node = dict(binding.rank_to_node)[0]
    command = [
        *mpi_launch_prefix(
            num_nodes,
            scheduler=scheduler,
            application_cwd=Path(os.environ["EXASERVE_LOCAL_RUNTIME_ROOT"]) / "python",
            application_environment=application_environment,
        ),
        str(python),
        "-s",
        "-m",
        module,
        *arguments,
        "--attempt-id",
        attempt_id,
        "--expected-world-size",
        str(num_nodes),
        "--expected-root-node",
        root_node,
    ]
    try:
        result = run_finite(command, timeout_s=timeout_s, env=launcher_environment)
    except (OSError, FiniteProcessError) as exc:
        raise RuntimeError(f"[ModelBcast] MPI collective failed: {exc}") from exc
    if result.stderr:
        print(result.stderr, end="" if result.stderr.endswith("\n") else "\n", file=sys.stderr)
    if result.returncode:
        # Parse first when possible so the rank-local cause is preserved.
        try:
            load_collective_results(
                result.stdout,
                attempt_id=attempt_id,
                expected_world_size=num_nodes,
                expected_root_node=root_node,
            )
        except RuntimeError as exc:
            raise RuntimeError(str(exc)) from exc
        raise RuntimeError(f"[ModelBcast] MPI collective exited {result.returncode}")
    return load_collective_results(
        result.stdout,
        attempt_id=attempt_id,
        expected_world_size=num_nodes,
        expected_root_node=root_node,
    )


def run_cache_probe(
    path: Path,
    num_nodes: int,
    *,
    binding,
    local_root: Path,
    expected_manifest_hash: str,
    scheduler: str = "pbs",
) -> List[dict]:
    """
    Probe the cache state on every allocated node via MPI.
    """
    attempt = uuid.uuid4().hex
    entries = [
        _validate_cache_probe_result(entry)
        for entry in _run_python_collective(
            [
                "--probe-cache",
                str(path),
                "--local-root",
                str(local_root),
                "--expected-manifest-hash",
                expected_manifest_hash,
                "--generation",
                str(binding.generation),
            ],
            attempt_id=attempt,
            num_nodes=num_nodes,
            binding=binding,
            scheduler=scheduler,
        )
    ]

    if len(entries) != num_nodes:
        raise RuntimeError(
            f"[ModelBcast] Expected {num_nodes} cache probe result(s) for {path}, "
            f"got {len(entries)}."
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


def _cache_cleanup_paths(local_stage_path: str, model_ids) -> tuple[Path, tuple[str, ...]]:
    """Resolve model namespaces without selecting immutable stable caches."""
    root = Path(local_stage_path)
    if not root.is_absolute() or root == Path(root.anchor) or ".." in root.parts:
        raise RuntimeError(f"[ModelBcast] Refusing unsafe clean-stage root {local_stage_path!r}")
    safe_names: list[str] = []
    for model_id in model_ids:
        safe_name = get_model_storage_name(model_id)
        if not safe_name or safe_name in {".", ".."} or Path(safe_name).name != safe_name:
            raise RuntimeError(
                f"[ModelBcast] Model {model_id!r} has unsafe cache name {safe_name!r}"
            )
        safe_names.append(safe_name)
    if not safe_names or len(safe_names) != len(set(safe_names)):
        raise RuntimeError("[ModelBcast] Clean-stage model namespaces must be nonempty and unique")
    return root, tuple(safe_names)


def clean_model_caches_locally(local_stage_path: str, model_ids, *, generation: int) -> dict:
    """Validate cleanup scope without deleting concurrent immutable caches.

    Exact attempt owners remove their own candidates on failure. A normal run
    cannot distinguish another concurrent deployment's debris, so clean-stage
    is intentionally a non-destructive reconciliation barrier. Stable-cache
    purge belongs to a separate explicit operator action.
    """
    if type(generation) is not int or generation < 0:
        raise RuntimeError("[ModelBcast] Clean-stage generation must be non-negative")
    started = time.monotonic()
    root, _safe_names = _cache_cleanup_paths(local_stage_path, model_ids)
    try:
        root = ensure_node_local_directory(root)
    except ValueError as exc:
        raise RuntimeError(f"[ModelBcast] Clean-stage root is not node-local: {exc}") from exc
    receipt = {
        "rank": _runtime_rank(),
        "node": socket.gethostname(),
        "generation": generation,
        "targets": [],
        "removed_paths": [],
        "cleanup_duration_s": round(time.monotonic() - started, 6),
    }
    return receipt


def clean_model_caches(
    local_stage_path: str,
    model_ids,
    num_nodes: int,
    *,
    binding,
    scheduler: str = "pbs",
) -> list[dict]:
    """Run bounded clean-stage on every bound rank and validate every receipt."""
    from .plan.contracts import same_node

    model_ids = list(model_ids)
    _root, _safe_names = _cache_cleanup_paths(local_stage_path, model_ids)
    attempt = uuid.uuid4().hex
    arguments = [
        "--clean-cache-root",
        local_stage_path,
        "--generation",
        str(binding.generation),
    ]
    for model_id in model_ids:
        arguments.extend(("--clean-model-id", model_id))

    receipts = [
        _validate_cache_clean_result(item)
        for item in _run_python_collective(
            arguments,
            attempt_id=attempt,
            num_nodes=num_nodes,
            binding=binding,
            scheduler=scheduler,
        )
    ]
    by_rank = {item["rank"]: item for item in receipts}
    if (
        len(receipts) != num_nodes
        or len(by_rank) != num_nodes
        or set(by_rank) != set(range(num_nodes))
    ):
        raise RuntimeError(
            f"[ModelBcast] Clean-stage returned {len(receipts)} receipt(s) "
            f"for {num_nodes} bound ranks"
        )
    for rank, planned_node in binding.rank_to_node:
        receipt = by_rank[rank]
        if (
            not same_node(receipt["node"], planned_node)
            or receipt["generation"] != binding.generation
            or receipt["targets"]
            or receipt["removed_paths"]
        ):
            raise RuntimeError(f"[ModelBcast] Clean-stage rank {rank} has wrong identity")
    print(
        f"[ModelBcast] CLEAN-STAGE (candidate debris only; immutable caches preserved): "
        f"{num_nodes}/{num_nodes} rank receipts; removed "
        f"{sum(len(item['removed_paths']) for item in receipts)} owned path(s)",
        flush=True,
    )
    return [by_rank[index] for index in range(num_nodes)]


def check_cache_state(
    model_id: str,
    local_stage_path: str,
    num_nodes: int,
    *,
    manifest_hash: str,
    recipient_ranks: list[int],
    binding,
    scheduler: str = "pbs",
) -> str:
    """
    Return the aggregate cache state for a model across all nodes.
    """
    local_root = Path(local_stage_path)
    target_path = content_addressed_model_path(model_id, local_root, manifest_hash)
    entries = run_cache_probe(
        target_path,
        num_nodes,
        binding=binding,
        local_root=local_root,
        expected_manifest_hash=manifest_hash,
        scheduler=scheduler,
    )
    entries = [entry for entry in entries if entry["rank"] in recipient_ranks]
    states = {entry["state"] for entry in entries}

    if states == {"complete"}:
        return "complete"
    if states <= {"missing", "partial", "complete"}:
        return "missing"
    raise RuntimeError(f"[ModelBcast] Cache probe returned unknown states for {model_id}: {states}")


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


def _parse_recipient_ranks(value: str, *, world_size: int) -> list[int]:
    try:
        ranks = [int(item) for item in value.split(",")]
    except ValueError as exc:
        raise ValueError("recipient ranks must be comma-separated integers") from exc
    if not ranks or ranks != sorted(set(ranks)) or ranks[0] < 0 or ranks[-1] >= world_size:
        raise ValueError("recipient ranks must be a sorted allocation subset")
    return ranks


def _model_skip_payload() -> dict:
    return {
        "rank": _runtime_rank(),
        "node": socket.gethostname(),
        "participating": False,
    }


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


def _source_model_manifest(path: Path, *, model_id: str) -> dict:
    """Return a verified source manifest without requiring source-store writes.

    Shared site model stores are commonly mounted read-only.  Legacy snapshots
    in those stores can be structurally complete without carrying ExaServe's
    completion marker. Preserve the strict manifest boundary by deriving a
    full-file content inventory, recording it in the run-owned evidence
    directory, and later injecting that exact manifest only into the temporary
    broadcast view and node-local candidate.
    """
    from .model_staging import (
        build_model_manifest,
        validate_model_manifest,
        verified_model_manifest,
    )
    from .state.atomic import atomic_create_or_verify_json, strict_json_load_path

    marker_path = path / COMPLETION_MARKER
    source_identity = f"external-read-only:{path.resolve()}"
    cached_manifest = verified_model_manifest(path)
    if os.path.lexists(marker_path):
        try:
            legacy = validate_model_manifest(
                strict_json_load_path(marker_path), allow_legacy_sampled=True
            )
            source_identity = legacy["source_identity"]
        except (OSError, ValueError) as exc:
            raise RuntimeError(
                f"model source for {model_id} has an invalid manifest: {exc}"
            ) from exc
        if cached_manifest is None and not check_model_exists(path):
            raise RuntimeError(f"model source for {model_id} has a stale/corrupt manifest")
        cached_manifest = verified_model_manifest(path)
        try:
            return _marker(path)  # writable legacy stores may migrate in place
        except RuntimeError:
            pass  # read-only sampled marker: derive full run-owned identity below
    elif cached_manifest is None:
        if not check_model_exists(path):
            raise RuntimeError(f"model source for {model_id} is incomplete at {path}")
        cached_manifest = verified_model_manifest(path)

    manifest = cached_manifest or build_model_manifest(path, source_identity=source_identity)
    evidence_dir = _head_result_root() / "model-source-manifests"
    evidence_dir.mkdir(parents=True, exist_ok=True)
    evidence_name = (
        hashlib.sha256(model_id.encode()).hexdigest() + "." + manifest["manifest_hash"] + ".json"
    )
    atomic_create_or_verify_json(evidence_dir / evidence_name, manifest)
    return manifest


def _prepare_model_bcast_source(
    source_path: Path,
    *,
    safe_name: str,
    source_manifest: dict,
    temporary_root: Path,
) -> Path:
    """Create a stable-name, marker-bearing broadcast view without copying weights."""
    from .state.atomic import atomic_create_json

    overlay = temporary_root / safe_name
    overlay.mkdir()
    for entry in sorted(source_path.iterdir(), key=lambda item: item.name):
        if entry.name == COMPLETION_MARKER:
            continue
        (overlay / entry.name).symlink_to(entry, target_is_directory=entry.is_dir())
    atomic_create_json(overlay / COMPLETION_MARKER, source_manifest)
    return overlay


def verify_and_publish_model(
    candidate: Path,
    target: Path,
    *,
    local_root: Path,
    model_id: str,
    expected_manifest_hash: str,
    generation: int,
    cache_key_hash: str | None = None,
) -> dict:
    from .state.atomic import fsync_directory

    """Validate a node-local candidate, then atomically publish it."""
    started = time.monotonic()
    try:
        local_root = validate_node_local_root(local_root)
        validate_node_local_tree(candidate, local_root=local_root)
    except ValueError as exc:
        raise RuntimeError(f"candidate for {model_id} is not proven node-local: {exc}") from exc
    expected_target = content_addressed_model_path(
        model_id, local_root, cache_key_hash or expected_manifest_hash
    )
    if target != expected_target:
        raise RuntimeError(
            f"candidate for {model_id} targets {target}, expected content address {expected_target}"
        )
    if not check_model_exists(candidate):
        raise RuntimeError(f"candidate for {model_id} is incomplete: {candidate}")
    candidate_manifest = _marker(candidate)
    if candidate_manifest["manifest_hash"] != expected_manifest_hash:
        raise RuntimeError(
            f"candidate for {model_id} has manifest "
            f"{candidate_manifest['manifest_hash']}, expected {expected_manifest_hash}"
        )
    _freeze_model_tree(candidate)
    try:
        validate_node_local_tree(candidate, local_root=local_root, require_immutable=True)
    except ValueError as exc:
        raise RuntimeError(f"candidate for {model_id} could not be made immutable: {exc}") from exc

    target_present = os.path.lexists(target)
    target_is_real_directory = False
    if target_present:
        try:
            target_is_real_directory = stat.S_ISDIR(os.lstat(target).st_mode)
        except OSError as exc:
            raise RuntimeError(f"model target is unavailable at {target}: {exc}") from exc
    # Path equality is intentionally lexical here. Resolving a stale symlink
    # target would itself perform worker-side metadata I/O on the escaped
    # filesystem before we quarantine it.
    if candidate != target or not target_is_real_directory:
        try:
            ensure_node_local_directory(target.parent)
        except (RuntimeError, ValueError) as exc:
            raise RuntimeError(f"model publication parent is unsafe: {exc}") from exc
        if target_present:
            if (
                target_is_real_directory
                and _validated_local_model(target, local_root=local_root)
                and _marker(target)["manifest_hash"] == expected_manifest_hash
            ):
                # A concurrent winner published identical content.
                _thaw_model_tree(candidate)
                shutil.rmtree(candidate)
            else:
                quarantine = target.with_name(
                    f".{target.name}.invalid.{generation}.{os.getpid()}.{time.time_ns()}"
                )
                if target_is_real_directory:
                    target.chmod(stat.S_IMODE(os.lstat(target).st_mode) | stat.S_IWUSR)
                os.rename(target, quarantine)
                if target_is_real_directory:
                    quarantine.chmod(stat.S_IMODE(os.lstat(quarantine).st_mode) & ~0o222)
                try:
                    candidate.chmod(stat.S_IMODE(os.lstat(candidate).st_mode) | stat.S_IWUSR)
                    os.replace(candidate, target)
                    target.chmod(stat.S_IMODE(os.lstat(target).st_mode) & ~0o222)
                    fsync_directory(target.parent)
                    if target_is_real_directory:
                        _thaw_model_tree(quarantine)
                        shutil.rmtree(quarantine)
                    else:
                        quarantine.unlink()
                except BaseException as exc:
                    try:
                        if os.path.lexists(target):
                            target.chmod(stat.S_IMODE(os.lstat(target).st_mode) & ~0o222)
                        if not os.path.lexists(target):
                            if target_is_real_directory:
                                quarantine.chmod(
                                    stat.S_IMODE(os.lstat(quarantine).st_mode) | stat.S_IWUSR
                                )
                            os.rename(quarantine, target)
                            if target_is_real_directory:
                                target.chmod(stat.S_IMODE(os.lstat(target).st_mode) & ~0o222)
                            fsync_directory(target.parent)
                    except OSError as rollback_exc:
                        add_exception_note(
                            exc, f"model publication rollback also failed: {rollback_exc}"
                        )
                    raise
        else:
            candidate.chmod(stat.S_IMODE(os.lstat(candidate).st_mode) | stat.S_IWUSR)
            os.replace(candidate, target)
            target.chmod(stat.S_IMODE(os.lstat(target).st_mode) & ~0o222)
            fsync_directory(target.parent)
    if (
        not stat.S_ISDIR(os.lstat(target).st_mode)
        or not _validated_local_model(target, local_root=local_root)
        or _marker(target)["manifest_hash"] != expected_manifest_hash
    ):
        raise RuntimeError(f"atomic model publication failed for {model_id} at {target}")
    receipt = {
        "rank": _runtime_rank(),
        "node": socket.gethostname(),
        "generation": generation,
        "model_id": model_id,
        "manifest_hash": expected_manifest_hash,
        "file_count": candidate_manifest["file_count"],
        "total_bytes": candidate_manifest["total_bytes"],
        "target": str(target),
        "model_device_id": int(os.stat(target, follow_symlinks=False).st_dev),
        "model_fs_type": _model_filesystem_type(target),
        "verification_duration_s": round(time.monotonic() - started, 6),
    }
    _cleanup_model_candidate(candidate, local_root=local_root, failed=False)
    return receipt


def _model_filesystem_type(path: Path) -> str:
    from .source_staging import _filesystem_type

    return _filesystem_type(path)


def _freeze_model_tree(root: Path) -> None:
    for current, directories, files in os.walk(root, topdown=False, followlinks=False):
        current_path = Path(current)
        for name in files:
            path = current_path / name
            mode = stat.S_IMODE(os.lstat(path).st_mode)
            path.chmod(0o555 if mode & 0o111 else 0o444)
        for name in directories:
            directory = current_path / name
            directory.chmod(0o555)
    root.chmod(0o555)


def _thaw_model_tree(root: Path) -> None:
    from .model_staging import chmod_real_directory

    for current, directories, _files in os.walk(root, topdown=True, followlinks=False):
        current_path = Path(current)
        current_metadata = os.lstat(current_path)
        if not stat.S_ISDIR(current_metadata.st_mode) or not chmod_real_directory(
            current_path,
            stat.S_IMODE(current_metadata.st_mode) | stat.S_IWUSR,
        ):
            continue
        for name in directories:
            directory = current_path / name
            metadata = os.lstat(directory)
            if stat.S_ISDIR(metadata.st_mode) and not stat.S_ISLNK(metadata.st_mode):
                chmod_real_directory(
                    directory,
                    stat.S_IMODE(metadata.st_mode) | stat.S_IWUSR,
                )


def _validated_local_model(path: Path, *, local_root: Path) -> bool:
    """Return completeness only after every descendant is proven local."""

    try:
        validate_node_local_tree(path, local_root=local_root, require_immutable=True)
    except ValueError:
        return False
    return check_model_exists(path)


def _cleanup_model_candidate(candidate: Path, *, local_root: Path, failed: bool) -> None:
    """Remove only an exact transaction-owned local candidate directory."""

    parent = candidate.parent
    patterns = (
        r"\.exaserve_stage\.[A-Za-z0-9_.-]+\.\d+\.[0-9a-f]{32}",
        r"\.exaserve_pp_candidate\.[A-Za-z0-9_.-]+\.\d+\.[0-9a-f]{32}\.stage\d+",
    )
    if parent.parent != local_root or not any(
        re.fullmatch(pattern, parent.name) for pattern in patterns
    ):
        return
    try:
        validate_node_local_tree(parent, local_root=local_root)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"model candidate cleanup path is unsafe: {parent}") from exc
    if failed:
        if candidate.exists():
            _thaw_model_tree(candidate)
        shutil.rmtree(parent)
        return
    try:
        parent.rmdir()
    except OSError as exc:
        raise RuntimeError(f"published model candidate parent is unexpectedly nonempty: {exc}")


def _rollback_model_candidate(candidate: Path, *, local_root: Path) -> None:
    """Remove only the unique attempt candidate, never a reusable content address."""

    _cleanup_model_candidate(candidate, local_root=local_root, failed=True)


def _model_receipts(
    *,
    candidate: Path,
    target: Path,
    local_root: Path,
    model_id: str,
    manifest_hash: str,
    generation: int,
    num_nodes: int,
    recipient_ranks: list[int],
    binding,
    scheduler: str,
    binary_path: Path,
    native_application_environment: Mapping[str, str],
) -> list[dict]:
    from .plan.contracts import same_node

    attempt = uuid.uuid4().hex
    try:
        payloads = _run_python_collective(
            [
                "--verify-model-candidate",
                str(candidate),
                "--publish-target",
                str(target),
                "--local-root",
                str(local_root),
                "--model-id",
                model_id,
                "--expected-manifest-hash",
                manifest_hash,
                "--generation",
                str(generation),
                "--recipient-ranks",
                ",".join(str(rank) for rank in recipient_ranks),
            ],
            attempt_id=attempt,
            num_nodes=num_nodes,
            binding=binding,
            scheduler=scheduler,
        )
    except BaseException as exc:
        candidate_root = candidate.parent if candidate.parent.parent == local_root else candidate
        cleanup_paths = [candidate_root] if candidate_root != target else []
        for cleanup_path in cleanup_paths:
            try:
                cleanup_native_candidates(
                    binary_path,
                    cleanup_path,
                    local_root=local_root,
                    num_nodes=num_nodes,
                    binding=binding,
                    scheduler=scheduler,
                    application_cwd=(Path(os.environ["EXASERVE_LOCAL_RUNTIME_ROOT"]) / "python"),
                    application_environment=native_application_environment,
                    recipient_ranks=recipient_ranks,
                )
            except BaseException as cleanup_exc:
                add_exception_note(
                    exc, f"model verifier cleanup for {cleanup_path} also failed: {cleanup_exc}"
                )
        raise
    receipts: list[dict] = []
    for rank, payload in enumerate(payloads):
        if rank in recipient_ranks:
            receipt = _validate_model_receipt(payload)
            if receipt["rank"] != rank:
                raise RuntimeError(f"model publication receipt rank {rank} has wrong identity")
            receipts.append(receipt)
            continue
        if (
            not isinstance(payload, dict)
            or set(payload)
            != {
                "schema_version",
                "attempt_id",
                "result_id",
                "rank",
                "node",
                "participating",
            }
            or payload["rank"] != rank
            or payload["participating"] is not False
        ):
            raise RuntimeError(f"model publication non-recipient rank {rank} result is invalid")
    by_rank = {item.get("rank"): item for item in receipts}
    if (
        len(receipts) != len(recipient_ranks)
        or len(by_rank) != len(recipient_ranks)
        or set(by_rank) != set(recipient_ranks)
    ):
        raise RuntimeError(
            f"model publication for {model_id} returned an incomplete/duplicate "
            f"receipt set ({len(receipts)} for {len(recipient_ranks)} recipients)"
        )
    for rank, planned_node in binding.rank_to_node:
        if rank not in by_rank:
            continue
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
    return [by_rank[index] for index in recipient_ranks]


def bcast_models(
    model_configs,
    lustre_path: str,
    local_path: str,
    num_nodes: int,
    *,
    shard_aware: bool = False,
    clean_stage: bool = False,
    binding=None,
    deployment_plan=None,
    scheduler: str = "pbs",
) -> Tuple[Dict[str, str], list[dict]]:
    """
    Ensure every model exists on Lustre, then broadcast it to local storage.
    """
    if binding is None or len(binding.rank_to_node) != num_nodes:
        raise RuntimeError("model staging requires the exact AllocationBinding")
    if deployment_plan is None:
        raise RuntimeError("model staging requires the canonical DeploymentPlan")
    if not isinstance(clean_stage, bool):
        raise TypeError("clean_stage must be a boolean")
    profile = None
    profile_path = os.environ.get("EXASERVE_SITE_PROFILE_PATH", "").strip()
    if profile_path:
        from .plan.io import load_site_profile

        profile = load_site_profile(profile_path)
    try:
        if profile is not None:
            from .plan.runtime_environment import shared_roots, validate_declared_filesystem
            from .site import require_complete_filesystem_policy

            require_complete_filesystem_policy(profile)
            declared_shared = tuple(shared_roots(profile))
            if (
                validate_declared_filesystem(
                    Path(local_path), policy=profile, root_kind="local_root"
                )
                is None
            ):
                raise RuntimeError(
                    f"model stage root has no declared local filesystem identity: {local_path}"
                )
        else:
            declared_shared = configured_shared_roots()
        local_root = ensure_node_local_directory(Path(local_path), shared_roots=declared_shared)
        if profile is not None and (
            validate_declared_filesystem(local_root, policy=profile, root_kind="local_root") is None
        ):
            raise RuntimeError(
                f"model stage root has no declared local filesystem identity: {local_root}"
            )
    except (RuntimeError, ValueError) as exc:
        raise RuntimeError(f"model stage root is not proven node-local: {exc}") from exc
    binary_path = resolve_bcast_executable(scheduler=scheduler, site_profile=profile)
    native_application_environment = bootstrap_application_environment(
        profile=profile,
        base_environment=os.environ,
    )

    lustre_model_paths = stage_models(model_configs, lustre_path)
    local_model_paths: Dict[str, str] = {}

    per_model_timings: list[dict] = []
    unique_model_ids = list(iter_unique_model_ids(model_configs))
    cleanup_receipts: list[dict] = []
    if clean_stage:
        cleanup_receipts = clean_model_caches(
            local_path,
            unique_model_ids,
            num_nodes,
            binding=binding,
            scheduler=scheduler,
        )

    for model_id in unique_model_ids:
        model_t0 = time.monotonic()
        model_config = next(cfg for cfg in model_configs if cfg.model_id == model_id)
        plan_models = [model for model in deployment_plan.models if model.model_id == model_id]
        if len(plan_models) != 1:
            raise RuntimeError(f"canonical plan has {len(plan_models)} entries for {model_id!r}")
        plan_model = plan_models[0]
        recipient_ranks = sorted(
            {rank for replica in plan_model.replicas for rank in replica.planned_ranks}
        )
        if not recipient_ranks or recipient_ranks[-1] >= num_nodes:
            raise RuntimeError(f"canonical model {model_id!r} has invalid recipient ranks")
        if model_config.pipeline_parallel_size > 1 and not shard_aware:
            raise RuntimeError(
                f"pipeline-parallel model {model_id!r} requires shard-aware staging; "
                "full-model or shared-storage fallback is prohibited"
            )
        # Shard-aware PP is still an outer, pre-launch transaction.  Allocation
        # binding order is the canonical node<->stage map, so Ray discovery is
        # neither necessary nor allowed to become a second staging authority.
        if (
            shard_aware
            and model_config.pipeline_parallel_size > 1
            and (model_config.num_replicas or 0) >= 1
        ):
            from .pp_stage import stage_pp_sharded

            if plan_model.num_replicas != int(
                model_config.num_replicas
            ) or plan_model.pipeline_parallel_size != int(model_config.pipeline_parallel_size):
                raise RuntimeError(
                    f"legacy staging view drifted from canonical plan for {model_id}"
                )

            source_path = Path(lustre_model_paths[model_id])
            source_manifest = _source_model_manifest(source_path, model_id=model_id)
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
                None,
                str(local_root),
                model_config.pipeline_parallel_size,
                ordered_nodes,
                int(model_config.num_replicas),
                binary_path,
                generation=binding.generation,
                scheduler=scheduler,
                model_id=model_id,
                binding=binding,
                application_environment=native_application_environment,
                source_manifest=source_manifest,
            )
            receipts = [receipt for item in pp_plan for receipt in item["receipts"]]
            aggregate_hash = hashlib.sha256(
                json.dumps(
                    [item["manifest_hash"] for item in pp_plan], separators=(",", ":")
                ).encode()
            ).hexdigest()
            local_model_paths[model_id] = str(
                content_addressed_model_path(model_id, local_root, aggregate_hash)
            )
            per_model_timings.append(
                {
                    "model_id": model_id,
                    "cache_reused": False,
                    "shard_aware": True,
                    "manifest_hash": aggregate_hash,
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
        source_path = Path(lustre_model_paths[model_id])
        safe_name = get_model_storage_name(model_id)
        source_manifest = _source_model_manifest(source_path, model_id=model_id)
        manifest_hash = source_manifest["manifest_hash"]
        target_path = content_addressed_model_path(model_id, local_root, manifest_hash)
        cache_state = check_cache_state(
            model_id,
            str(local_root),
            num_nodes,
            manifest_hash=manifest_hash,
            recipient_ranks=recipient_ranks,
            binding=binding,
            scheduler=scheduler,
        )

        if cache_state == "complete":
            receipts = _model_receipts(
                candidate=target_path,
                target=target_path,
                local_root=local_root,
                model_id=model_id,
                manifest_hash=manifest_hash,
                generation=binding.generation,
                num_nodes=num_nodes,
                recipient_ranks=recipient_ranks,
                binding=binding,
                scheduler=scheduler,
                binary_path=binary_path,
                native_application_environment=native_application_environment,
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

        candidate_root = local_root / (
            f".exaserve_stage.{safe_name}.{binding.generation}.{uuid.uuid4().hex}"
        )
        candidate_model = candidate_root / safe_name

        # HF cache snapshots use a revision hash as the directory name.  The
        # temporary view also supplies a run-owned manifest when a read-only
        # legacy snapshot could not be upgraded in place.
        with tempfile.TemporaryDirectory(
            prefix=f"model-bcast-{safe_name}-", dir=local_root
        ) as tmpdir:
            bcast_source = _prepare_model_bcast_source(
                source_path,
                safe_name=safe_name,
                source_manifest=source_manifest,
                temporary_root=Path(tmpdir),
            )

            print(
                f"[ModelBcast] Broadcasting {model_id} from {source_path} to {target_path} "
                f"across {num_nodes} node(s)...",
                flush=True,
            )
            command = mpi_launch_prefix(
                num_nodes,
                scheduler=scheduler,
                application_cwd=Path(os.environ["EXASERVE_LOCAL_RUNTIME_ROOT"]) / "python",
                application_environment=native_application_environment,
                transfer_executable=(
                    scheduler == "pbs"
                    and not os.environ.get("EXASERVE_LOCAL_RUNTIME_ROOT")
                    and not os.environ.get("EXASERVE_QUALIFIED_BCAST")
                ),
            ) + [
                str(binary_path),
                "--expected-root-host",
                dict(binding.rank_to_node)[0],
                "--expected-world-size",
                str(num_nodes),
                "--recipients",
                ",".join(str(rank) for rank in recipient_ranks),
                str(bcast_source),
                str(candidate_root),
            ]
            try:
                run_finite(command, timeout_s=1800, check=True)
            except BaseException as exc:
                for cleanup_path in (candidate_root,):
                    try:
                        cleanup_native_candidates(
                            binary_path,
                            cleanup_path,
                            local_root=local_root,
                            num_nodes=num_nodes,
                            binding=binding,
                            scheduler=scheduler,
                            application_cwd=(
                                Path(os.environ["EXASERVE_LOCAL_RUNTIME_ROOT"]) / "python"
                            ),
                            application_environment=native_application_environment,
                            recipient_ranks=recipient_ranks,
                        )
                    except BaseException as cleanup_exc:
                        add_exception_note(
                            exc,
                            f"model native cleanup for {cleanup_path} also failed: {cleanup_exc}",
                        )
                raise
        receipts = _model_receipts(
            candidate=candidate_model,
            target=target_path,
            local_root=local_root,
            model_id=model_id,
            manifest_hash=manifest_hash,
            generation=binding.generation,
            num_nodes=num_nodes,
            recipient_ranks=recipient_ranks,
            binding=binding,
            scheduler=scheduler,
            binary_path=binary_path,
            native_application_environment=native_application_environment,
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

    return local_model_paths, per_model_timings, cleanup_receipts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MPI model staging helper for Aurora")
    parser.add_argument("--plan", help="Verified canonical DeploymentPlan artifact")
    parser.add_argument("--binding", help="Verified AllocationBinding artifact")
    parser.add_argument("--num-nodes", type=int, help="Allocated node count")
    parser.add_argument(
        "--probe-cache",
        help="Internal mode: print local cache state for one model path as JSON",
    )
    parser.add_argument("--clean-cache-root")
    parser.add_argument("--clean-model-id", action="append", default=[])
    parser.add_argument("--verify-model-candidate")
    parser.add_argument("--publish-target")
    parser.add_argument("--model-id")
    parser.add_argument("--expected-manifest-hash")
    parser.add_argument("--cache-key-hash")
    parser.add_argument("--generation", type=int)
    parser.add_argument("--local-root")
    parser.add_argument("--attempt-id")
    parser.add_argument("--expected-world-size", type=int)
    parser.add_argument("--expected-root-node")
    parser.add_argument("--recipient-ranks")
    args = parser.parse_args(argv)

    if args.probe_cache:
        if any(
            value is None
            for value in (
                args.generation,
                args.local_root,
                args.expected_manifest_hash,
                args.attempt_id,
                args.expected_world_size,
                args.expected_root_node,
            )
        ):
            raise SystemExit("cache probe requires local/hash/generation/collective identity")
        from .staging_results import run_collective_operation

        success = run_collective_operation(
            lambda: probe_cache_locally(
                Path(args.probe_cache),
                generation=args.generation,
                local_root=Path(args.local_root),
                expected_manifest_hash=args.expected_manifest_hash,
            ),
            attempt_id=args.attempt_id,
            expected_world_size=args.expected_world_size,
            expected_root_node=args.expected_root_node,
        )
        return 0 if success else 1

    if args.clean_cache_root:
        if (
            args.generation is None
            or not args.attempt_id
            or not args.clean_model_id
            or args.expected_world_size is None
            or not args.expected_root_node
        ):
            raise SystemExit("cache cleanup requires model/generation/collective identity")
        from .staging_results import run_collective_operation

        success = run_collective_operation(
            lambda: clean_model_caches_locally(
                args.clean_cache_root,
                args.clean_model_id,
                generation=args.generation,
            ),
            attempt_id=args.attempt_id,
            expected_world_size=args.expected_world_size,
            expected_root_node=args.expected_root_node,
        )
        return 0 if success else 1

    if args.verify_model_candidate:
        required = (
            args.publish_target,
            args.model_id,
            args.expected_manifest_hash,
            args.generation,
            args.local_root,
            args.attempt_id,
            args.expected_world_size,
            args.expected_root_node,
            args.recipient_ranks,
        )
        if any(value is None for value in required):
            raise SystemExit("candidate verification requires target/model/hash/generation")
        from .staging_results import run_collective_operation

        recipients = _parse_recipient_ranks(
            args.recipient_ranks, world_size=args.expected_world_size
        )
        rank = _runtime_rank()

        def publish_operation() -> dict:
            candidate = Path(args.verify_model_candidate)
            local_root = Path(args.local_root)
            try:
                return verify_and_publish_model(
                    candidate,
                    Path(args.publish_target),
                    local_root=local_root,
                    model_id=args.model_id,
                    expected_manifest_hash=args.expected_manifest_hash,
                    cache_key_hash=args.cache_key_hash,
                    generation=args.generation,
                )
            except BaseException as exc:
                try:
                    _cleanup_model_candidate(candidate, local_root=local_root, failed=True)
                except BaseException as cleanup_exc:
                    add_exception_note(exc, f"model candidate cleanup also failed: {cleanup_exc}")
                raise

        success = run_collective_operation(
            lambda: (publish_operation() if rank in recipients else _model_skip_payload()),
            attempt_id=args.attempt_id,
            expected_world_size=args.expected_world_size,
            expected_root_node=args.expected_root_node,
        )
        if not success and rank in recipients:
            try:
                _rollback_model_candidate(
                    Path(args.verify_model_candidate),
                    local_root=Path(args.local_root),
                )
            except BaseException as cleanup_exc:
                print(
                    f"[ModelBcast] rollback cleanup failed: {cleanup_exc}",
                    file=sys.stderr,
                    flush=True,
                )
        return 0 if success else 1

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
    local_model_paths, per_model_timings, cleanup_receipts = bcast_models(
        plan.models,
        plan.model_storage_path,
        plan.local_stage_path,
        args.num_nodes,
        shard_aware=shard_aware,
        clean_stage=plan.runtime.clean_stage,
        binding=binding,
        deployment_plan=plan,
        scheduler=plan.scale_envelope.scheduler_type,
    )
    overall_s = round(time.monotonic() - overall_start, 4)

    timing = validate_model_bcast_result(
        {
            "schema_version": 2,
            "deployment_id": plan.deployment_id,
            "generation": binding.generation,
            "deployment_plan_hash": plan.deployment_plan_hash,
            "site_profile_hash": plan.site_profile_hash,
            "allocation_binding_hash": binding.allocation_binding_hash,
            "model_bcast_total_s": overall_s,
            "model_paths": local_model_paths,
            "models": per_model_timings,
            "cleanup_receipts": cleanup_receipts,
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
