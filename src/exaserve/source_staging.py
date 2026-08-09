"""Transactional distribution of the immutable ExaServe rank artifact.

The Python composition root owns this finite helper.  It retains the native
MPI broadcast implementation, but replaces the former 200-line shell
orchestrator and hand-built Ray overlay with one checked transaction:

1. copy a clean package snapshot into a unique shared staging directory;
2. compute a complete path/size/content manifest;
3. broadcast it into a unique node-local candidate directory;
4. have every planned rank verify the candidate and atomically publish the
   stable ``/tmp/exaserve_src`` symlink; and
5. atomically persist an aggregate per-rank result manifest.

MPI exit zero is necessary but not sufficient.  Missing, duplicate, stale, or
wrong-node receipts fail the step, so ranks can never launch from a partially
published source tree.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import socket
import sys
import time
import uuid
from importlib import resources
from pathlib import Path
from pathlib import PurePosixPath
from typing import Iterable

from .exception_notes import add_exception_note


class SourceStagingError(RuntimeError):
    """The source artifact could not be published on every planned rank."""


_SOURCE_RECEIPT_FIELDS = {
    "schema_version",
    "attempt_id",
    "result_id",
    "rank",
    "node",
    "generation",
    "source_manifest_hash",
    "file_count",
    "total_bytes",
    "published_path",
    "published_target",
    "verification_duration_s",
}
_SOURCE_RESULT_FIELDS = {
    "schema_version",
    "deployment_id",
    "generation",
    "deployment_plan_hash",
    "site_profile_hash",
    "allocation_binding_hash",
    "source_manifest_hash",
    "file_count",
    "total_bytes",
    "files",
    "duration_s",
    "rank_result_dir",
    "rank_receipts",
}
_SOURCE_FILE_FIELDS = {"path", "size", "sha256"}
_SHA256 = re.compile(r"[0-9a-f]{64}")
_ATTEMPT_ID = re.compile(r"[0-9a-f]{32}")
_RESULT_ID = re.compile(r"[0-9a-f]{32}")


def _validate_source_receipt(value: object) -> dict:
    if not isinstance(value, dict) or set(value) != _SOURCE_RECEIPT_FIELDS:
        raise SourceStagingError("source receipt fields are invalid")
    duration = value["verification_duration_s"]
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or not isinstance(value["attempt_id"], str)
        or not _ATTEMPT_ID.fullmatch(value["attempt_id"])
        or not isinstance(value["result_id"], str)
        or not _RESULT_ID.fullmatch(value["result_id"])
        or type(value["rank"]) is not int
        or value["rank"] < 0
        or not isinstance(value["node"], str)
        or not value["node"]
        or type(value["generation"]) is not int
        or value["generation"] < 0
        or not isinstance(value["source_manifest_hash"], str)
        or not _SHA256.fullmatch(value["source_manifest_hash"])
        or type(value["file_count"]) is not int
        or value["file_count"] < 0
        or type(value["total_bytes"]) is not int
        or value["total_bytes"] < 0
        or not isinstance(value["published_path"], str)
        or not os.path.isabs(value["published_path"])
        or not isinstance(value["published_target"], str)
        or not os.path.isabs(value["published_target"])
        or isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
    ):
        raise SourceStagingError("source receipt values are invalid")
    return value


def _validate_source_files(value: object) -> tuple[list[dict], dict]:
    if not isinstance(value, list):
        raise SourceStagingError("source result files must be an array")
    files: list[dict] = []
    paths: list[str] = []
    total_bytes = 0
    for index, item in enumerate(value):
        if not isinstance(item, dict) or set(item) != _SOURCE_FILE_FIELDS:
            raise SourceStagingError(f"source result file {index} fields are invalid")
        path = item["path"]
        if not isinstance(path, str) or not path or "\\" in path:
            raise SourceStagingError(f"source result file {index} path is invalid")
        parsed = PurePosixPath(path)
        if (
            parsed.is_absolute()
            or parsed.as_posix() != path
            or any(part in {"", ".", ".."} for part in parsed.parts)
        ):
            raise SourceStagingError(f"source result file {index} path is unsafe")
        if (
            type(item["size"]) is not int
            or item["size"] < 0
            or not isinstance(item["sha256"], str)
            or not _SHA256.fullmatch(item["sha256"])
        ):
            raise SourceStagingError(f"source result file {index} values are invalid")
        paths.append(path)
        total_bytes += item["size"]
        files.append(item)
    if paths != sorted(paths) or len(paths) != len(set(paths)):
        raise SourceStagingError("source result file paths must be unique and sorted")
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"))
    inventory = {
        "source_manifest_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        "file_count": len(files),
        "total_bytes": total_bytes,
    }
    return files, inventory


def validate_source_staging_result(
    value: object,
    *,
    expected_deployment_id: str | None = None,
    expected_generation: int | None = None,
    expected_plan_hash: str | None = None,
    expected_site_profile_hash: str | None = None,
    expected_binding_hash: str | None = None,
    expected_rank_to_node: Iterable[tuple[int, str]] | None = None,
    expected_run_dir: str | os.PathLike | None = None,
) -> dict:
    """Validate the complete immutable source-staging evidence boundary."""
    if not isinstance(value, dict) or set(value) != _SOURCE_RESULT_FIELDS:
        raise SourceStagingError("source staging result fields are invalid")
    duration = value["duration_s"]
    if (
        type(value["schema_version"]) is not int
        or value["schema_version"] != 1
        or not isinstance(value["deployment_id"], str)
        or not value["deployment_id"]
        or type(value["generation"]) is not int
        or value["generation"] < 0
        or any(
            not isinstance(value[name], str) or not _SHA256.fullmatch(value[name])
            for name in (
                "deployment_plan_hash",
                "site_profile_hash",
                "allocation_binding_hash",
                "source_manifest_hash",
            )
        )
        or type(value["file_count"]) is not int
        or value["file_count"] < 0
        or type(value["total_bytes"]) is not int
        or value["total_bytes"] < 0
        or isinstance(duration, bool)
        or not isinstance(duration, (int, float))
        or not math.isfinite(duration)
        or duration < 0
        or not isinstance(value["rank_result_dir"], str)
        or not os.path.isabs(value["rank_result_dir"])
        or not isinstance(value["rank_receipts"], list)
        or not value["rank_receipts"]
    ):
        raise SourceStagingError("source staging result values are invalid")

    _, inventory = _validate_source_files(value["files"])
    if any(value[name] != inventory[name] for name in inventory):
        raise SourceStagingError("source staging result inventory hash/count/bytes mismatch")

    expected_identity = {
        "deployment_id": expected_deployment_id,
        "generation": expected_generation,
        "deployment_plan_hash": expected_plan_hash,
        "site_profile_hash": expected_site_profile_hash,
        "allocation_binding_hash": expected_binding_hash,
    }
    mismatches = {
        name: (expected, value[name])
        for name, expected in expected_identity.items()
        if expected is not None and value[name] != expected
    }
    if mismatches:
        raise SourceStagingError(f"source staging result identity mismatch: {mismatches}")

    receipts = [_validate_source_receipt(item) for item in value["rank_receipts"]]
    ranks = [item["rank"] for item in receipts]
    if ranks != list(range(len(receipts))):
        raise SourceStagingError("source receipts must be one ordered, contiguous rank set")
    attempt_ids = {item["attempt_id"] for item in receipts}
    result_ids = {item["result_id"] for item in receipts}
    if len(attempt_ids) != 1 or len(result_ids) != len(receipts):
        raise SourceStagingError("source receipts have inconsistent attempt/result identity")
    attempt_id = next(iter(attempt_ids))
    expected_target = (
        f"/tmp/exaserve_stage.{value['generation']}."
        f"{value['source_manifest_hash'][:12]}.{attempt_id}"
    )
    for receipt in receipts:
        if (
            receipt["generation"] != value["generation"]
            or receipt["source_manifest_hash"] != value["source_manifest_hash"]
            or receipt["file_count"] != value["file_count"]
            or receipt["total_bytes"] != value["total_bytes"]
            or receipt["published_path"] != "/tmp/exaserve_src"
            or receipt["published_target"] != expected_target
        ):
            raise SourceStagingError(f"source receipt rank {receipt['rank']} disagrees with result")

    result_dir = Path(value["rank_result_dir"])
    if result_dir.name != f"source.{attempt_id}":
        raise SourceStagingError("source rank-result directory disagrees with attempt identity")
    if expected_run_dir is not None:
        expected_root = Path(expected_run_dir).resolve()
        try:
            result_dir.resolve().relative_to(expected_root)
        except ValueError as exc:
            raise SourceStagingError("source rank-result directory is outside the run") from exc

    if expected_rank_to_node is not None:
        from .plan.contracts import same_node

        planned = tuple(expected_rank_to_node)
        if len(planned) != len(receipts) or [rank for rank, _ in planned] != ranks:
            raise SourceStagingError("source receipt rank set disagrees with allocation binding")
        for receipt, (_, node) in zip(receipts, planned):
            if not isinstance(node, str) or not node or not same_node(receipt["node"], node):
                raise SourceStagingError(
                    f"source receipt rank {receipt['rank']} came from the wrong node"
                )
    return value


def _rank() -> int:
    for name in ("PALS_RANKID", "PMI_RANK", "SLURM_PROCID", "OMPI_COMM_WORLD_RANK", "PMIX_RANK"):
        value = os.environ.get(name)
        if value is not None:
            try:
                rank = int(value)
            except ValueError:
                raise SourceStagingError(f"{name} is not an integer: {value!r}")
            if rank < 0:
                raise SourceStagingError(f"{name} must be non-negative: {value!r}")
            return rank
    raise SourceStagingError("source verifier has no MPI/srun rank identity")


def _iter_files(root: Path) -> Iterable[Path]:
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.is_symlink():
            raise SourceStagingError(f"source artifact contains an unresolved symlink: {path}")
        if path.is_file():
            yield path
        elif not path.is_dir():
            raise SourceStagingError(f"source artifact contains an unsupported entry: {path}")


def tree_manifest(root: Path) -> dict:
    """Return a complete deterministic file manifest for ``root``."""
    if not root.is_dir():
        raise SourceStagingError(f"source artifact is not a directory: {root}")
    files = []
    total_bytes = 0
    from .state.atomic import regular_file_reader

    for path in _iter_files(root):
        digest = hashlib.sha256()
        size = 0
        with regular_file_reader(path, binary=True) as handle:
            while True:
                chunk = handle.read(1 << 20)
                if not chunk:
                    break
                digest.update(chunk)
                size += len(chunk)
        total_bytes += size
        files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "size": size,
                "sha256": digest.hexdigest(),
            }
        )
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"))
    return {
        "source_manifest_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        "file_count": len(files),
        "total_bytes": total_bytes,
        "files": files,
    }


def _ignore(directory: str, names: list[str]) -> set[str]:
    del directory
    return {name for name in names if name == "__pycache__" or name.endswith((".pyc", ".pyo"))}


def _publish(candidate_root: Path, stable: Path) -> None:
    """Atomically point ``stable`` at a verified generation candidate."""
    from .state.atomic import fsync_directory

    stable.parent.mkdir(parents=True, exist_ok=True)
    quarantine = None
    if stable.exists() and not stable.is_symlink():
        # Preserve the pre-transaction legacy directory for diagnosis instead
        # of recursively deleting material that this generation did not make.
        quarantine = stable.with_name(f"{stable.name}.legacy.{os.getpid()}.{time.time_ns()}")
        os.rename(stable, quarantine)
    temporary = stable.with_name(f".{stable.name}.new.{os.getpid()}.{time.time_ns()}")
    try:
        os.symlink(candidate_root, temporary, target_is_directory=True)
        os.replace(temporary, stable)
        fsync_directory(stable.parent)
    except BaseException as exc:
        try:
            if os.path.lexists(temporary):
                os.unlink(temporary)
            if quarantine is not None and not os.path.lexists(stable):
                os.rename(quarantine, stable)
                fsync_directory(stable.parent)
        except OSError as rollback_exc:
            add_exception_note(exc, f"source publication rollback also failed: {rollback_exc}")
        raise


def verify_and_publish(
    candidate_package: Path,
    expected_hash: str,
    expected_files: int,
    expected_bytes: int,
    generation: int,
    stable: Path,
) -> dict:
    started = time.monotonic()
    observed = tree_manifest(candidate_package)
    if observed["source_manifest_hash"] != expected_hash:
        raise SourceStagingError(
            "node-local source manifest mismatch: "
            f"expected={expected_hash}, observed={observed['source_manifest_hash']}"
        )
    if observed["file_count"] != expected_files or observed["total_bytes"] != expected_bytes:
        raise SourceStagingError("node-local source inventory disagrees with declared count/bytes")
    candidate_root = candidate_package.parent
    _publish(candidate_root, stable)
    if not stable.is_symlink() or stable.resolve() != candidate_root.resolve():
        raise SourceStagingError(f"atomic source publication failed at {stable}")
    return {
        "rank": _rank(),
        "node": socket.gethostname(),
        "generation": generation,
        "source_manifest_hash": expected_hash,
        "file_count": expected_files,
        "total_bytes": expected_bytes,
        "published_path": str(stable),
        "published_target": str(candidate_root),
        "verification_duration_s": round(time.monotonic() - started, 6),
    }


def _clean_package_snapshot(destination: Path, *, vendor: str) -> Path:
    package = Path(str(resources.files("exaserve"))).resolve()
    if not package.is_dir():
        raise SourceStagingError("installed ExaServe package is not a filesystem artifact")
    clean = destination / "exaserve"
    shutil.copytree(package, clean, symlinks=False, ignore=_ignore)
    from .compat.generated_overlay import materialize
    from .compat.profile import default_profile

    profile = default_profile(vendor)
    materialize(profile, clean / "_compat_runtime" / profile.profile_id)
    return clean


def _run_checked(argv: list[str], *, timeout_s: float):
    from .control.finite_process import FiniteProcessError, run_finite

    try:
        completed = run_finite(argv, timeout_s=timeout_s)
    except (OSError, FiniteProcessError) as exc:
        raise SourceStagingError(f"native staging command failed: {exc}") from exc
    if completed.stdout:
        print(completed.stdout, end="" if completed.stdout.endswith("\n") else "\n", flush=True)
    if completed.stderr:
        print(
            completed.stderr,
            end="" if completed.stderr.endswith("\n") else "\n",
            file=sys.stderr,
            flush=True,
        )
    if completed.returncode != 0:
        raise SourceStagingError(f"native staging command exited {completed.returncode}: {argv[0]}")
    return completed


def stage(
    plan_path: str, binding_path: str, result_path: str, *, operation_timeout_s: float = 1800.0
) -> dict:
    from .control.rank_launcher import resolve_launch_prefix
    from .model_bcast import compile_bcast
    from .plan.io import load_allocation_binding, load_deployment_plan
    from .plan.contracts import same_node
    from .state.atomic import atomic_create_json
    from .staging_results import create_result_dir, load_rank_results

    plan = load_deployment_plan(plan_path)
    binding = load_allocation_binding(binding_path)
    if (
        binding.deployment_plan_hash != plan.deployment_plan_hash
        or binding.site_profile_hash != plan.site_profile_hash
        or len(binding.rank_to_node) != plan.num_nodes
    ):
        raise SourceStagingError("allocation binding does not belong to the source staging plan")

    run_dir = Path(result_path).resolve().parent
    run_dir.mkdir(parents=True, exist_ok=True)
    source_root = run_dir / f".source-input.{uuid.uuid4().hex}"
    source_root.mkdir(mode=0o700)
    started = time.monotonic()
    try:
        clean_package = _clean_package_snapshot(source_root, vendor=plan.vendor)
        manifest = tree_manifest(clean_package)
        attempt = uuid.uuid4().hex
        rank_result_dir = create_result_dir(run_dir, "source", attempt)
        candidate_root = Path(
            f"/tmp/exaserve_stage.{binding.generation}."
            f"{manifest['source_manifest_hash'][:12]}.{attempt}"
        )
        candidate_package = candidate_root / "exaserve"
        prefix = resolve_launch_prefix(plan.num_nodes, scheduler=plan.scale_envelope.scheduler_type)
        binary = compile_bcast(run_dir / "bcast_build")
        _run_checked(
            [*prefix, str(binary), str(clean_package), str(candidate_root)],
            timeout_s=operation_timeout_s,
        )
        _run_checked(
            [
                *prefix,
                sys.executable,
                "-m",
                "exaserve.source_staging",
                "--verify-and-publish",
                str(candidate_package),
                "--expected-hash",
                manifest["source_manifest_hash"],
                "--expected-files",
                str(manifest["file_count"]),
                "--expected-bytes",
                str(manifest["total_bytes"]),
                "--generation",
                str(binding.generation),
                "--stable-path",
                "/tmp/exaserve_src",
                "--result-dir",
                str(rank_result_dir),
                "--attempt-id",
                attempt,
            ],
            timeout_s=operation_timeout_s,
        )
        try:
            receipts = [
                _validate_source_receipt(receipt)
                for receipt in load_rank_results(rank_result_dir, attempt_id=attempt)
            ]
        except RuntimeError as exc:
            raise SourceStagingError(str(exc)) from exc
        if len(receipts) != plan.num_nodes:
            raise SourceStagingError(
                f"expected {plan.num_nodes} per-rank source receipts, received {len(receipts)}"
            )
        by_rank = {item.get("rank"): item for item in receipts}
        if len(by_rank) != plan.num_nodes or set(by_rank) != set(range(plan.num_nodes)):
            raise SourceStagingError("source receipts contain missing/duplicate rank IDs")
        for rank, planned_node in binding.rank_to_node:
            receipt = by_rank[rank]
            if not same_node(receipt["node"], planned_node):
                raise SourceStagingError(
                    f"source receipt rank {rank} came from {receipt.get('node')!r}, "
                    f"planned node is {planned_node!r}"
                )
            if (
                receipt.get("generation") != binding.generation
                or receipt.get("source_manifest_hash") != manifest["source_manifest_hash"]
            ):
                raise SourceStagingError(f"source receipt rank {rank} has stale/wrong identity")
        result = validate_source_staging_result(
            {
                "schema_version": 1,
                "deployment_id": plan.deployment_id,
                "generation": binding.generation,
                "deployment_plan_hash": plan.deployment_plan_hash,
                "site_profile_hash": plan.site_profile_hash,
                "allocation_binding_hash": binding.allocation_binding_hash,
                "source_manifest_hash": manifest["source_manifest_hash"],
                "file_count": manifest["file_count"],
                "total_bytes": manifest["total_bytes"],
                "files": manifest["files"],
                "duration_s": round(time.monotonic() - started, 6),
                "rank_result_dir": str(rank_result_dir),
                "rank_receipts": [by_rank[index] for index in range(plan.num_nodes)],
            },
            expected_deployment_id=plan.deployment_id,
            expected_generation=binding.generation,
            expected_plan_hash=plan.deployment_plan_hash,
            expected_site_profile_hash=plan.site_profile_hash,
            expected_binding_hash=binding.allocation_binding_hash,
            expected_rank_to_node=binding.rank_to_node,
            expected_run_dir=run_dir,
        )
    except BaseException as exc:
        try:
            shutil.rmtree(source_root)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            add_exception_note(exc, f"source staging cleanup also failed: {cleanup_exc}")
        raise
    try:
        shutil.rmtree(source_root)
    except OSError as exc:
        raise SourceStagingError(f"source staging input cleanup failed: {exc}") from exc
    atomic_create_json(result_path, result)
    return result


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Transactional ExaServe source staging")
    parser.add_argument("--plan")
    parser.add_argument("--binding")
    parser.add_argument("--result")
    parser.add_argument("--operation-timeout-s", type=float, default=1800.0)
    parser.add_argument("--verify-and-publish")
    parser.add_argument("--expected-hash")
    parser.add_argument("--expected-files", type=int)
    parser.add_argument("--expected-bytes", type=int)
    parser.add_argument("--generation", type=int)
    parser.add_argument("--stable-path", default="/tmp/exaserve_src")
    parser.add_argument("--result-dir")
    parser.add_argument("--attempt-id")
    args = parser.parse_args(argv)

    if args.verify_and_publish:
        required = (
            args.expected_hash,
            args.expected_files,
            args.expected_bytes,
            args.generation,
            args.result_dir,
            args.attempt_id,
        )
        if any(value is None for value in required):
            parser.error("verification mode requires expected identity fields")
        receipt = verify_and_publish(
            Path(args.verify_and_publish),
            args.expected_hash,
            args.expected_files,
            args.expected_bytes,
            args.generation,
            Path(args.stable_path),
        )
        from .staging_results import write_rank_result

        write_rank_result(args.result_dir, attempt_id=args.attempt_id, payload=receipt)
        return 0
    if not args.plan or not args.binding or not args.result:
        parser.error("--plan, --binding, and --result are required")
    stage(args.plan, args.binding, args.result, operation_timeout_s=args.operation_timeout_s)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SourceStagingError as exc:
        print(f"[SourceStaging] ERROR: {exc}", file=sys.stderr, flush=True)
        raise SystemExit(1) from None
