"""Run planner: materialize experiment specs into executable run bundles.

This is the core materialization layer. Given a spec path, it:

  1. Loads and validates the spec (spec_io).
  2. Expands the matrix into concrete variants (matrix.py).
  3. Creates a run group directory under runs/<spec>/runN with shared metadata.
  4. Creates or reuses a commit snapshot of the repo under site_config.snapshot_dir.
  5. For each variant:
     a. Generates / reuses a content-addressed trace artifact (trace_store).
     b. Creates a run bundle directory tree (job, logs, results, state, runtime).
     c. Asks the backend adapter to build a runtime manifest and validate.
     d. Renders a PBS job script that will invoke `eval.cli run execute`.
     e. Writes canonical plan artifacts plus run.yaml materialization metadata.
"""

from __future__ import annotations

import multiprocessing
import hashlib
import errno
import json
import math
import os
import re
import shutil
import sys
import tarfile
import tempfile
from dataclasses import replace
from pathlib import Path
import time
from typing import Any, Callable

from eval.site_config import get_site_config
from exaserve.exception_notes import add_exception_note

from .backends import get_backend_adapter
from .matrix import expand_matrix
from .models import (
    RunBundle,
    RunMaterialization,
    SchedulerSpec,
    TraceArtifact,
    VariantSpec,
)
from exaserve.schedulers import JobSpec, default_queue_and_walltime, get_scheduler
from .catalog import spec_group_relpath
from .plan_adapter import compile_shared_run_plan
from .spec_io import load_experiment_spec
from .trace_store import materialize_trace_artifact
from .utils import (
    dataclass_to_dict,
    dump_json_file,
    ensure_dir,
    slugify,
    utc_timestamp,
)


_MP_CONTEXT = multiprocessing.get_context("forkserver")
_DEFAULT_MAX_WORKERS = 8
_DEFAULT_MATERIALIZATION_TIMEOUT_S = 3600.0
_RUN_GROUP_RE = re.compile(r"^run(\d+)$")
# v2 snapshots created before the job boundary disabled bytecode writes may
# contain interpreter-generated __pycache__ entries.  Preserve those trees as
# evidence and publish clean materializations under a new location tag.  The
# snapshot metadata and semantic hash policy remain schema v2 because the
# archived source/tool bytes and their meaning are unchanged.
_SNAPSHOT_LOCATION_TAG = "v2b"


def scheduler_run_identity(
    *, run_semantic_hash: str, spec_name: str, run_group_id: str, run_id: str
) -> str:
    """Return the exact scheduler-visible identity of one materialization.

    The semantic hash deliberately excludes retry/presentation identity. Its
    prefix alone therefore makes every rerun share one PBS Job_Name, so exact
    recovery can find historical jobs from older run groups. Hash both semantic
    and materialization identity into the site's 15-character name limit.
    """
    if not re.fullmatch(r"[0-9a-f]{64}", run_semantic_hash):
        raise ValueError("scheduler identity requires a lowercase run semantic SHA-256")
    for name, value in (
        ("spec_name", spec_name),
        ("run_group_id", run_group_id),
        ("run_id", run_id),
    ):
        if not isinstance(value, str) or not value:
            raise ValueError(f"scheduler identity {name} must be non-empty text")
    payload = json.dumps(
        {
            "run_semantic_hash": run_semantic_hash,
            "spec_name": spec_name,
            "run_group_id": run_group_id,
            "run_id": run_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"es-{hashlib.sha256(payload).hexdigest()[:12]}"


def _bounded_process_map(
    function: Callable[..., Any],
    jobs: list[dict[str, Any]],
    *,
    labels: list[str],
    workers: int,
    timeout_s: float,
) -> list[Any]:
    """Run independent materializers with one finite owner deadline.

    ``ProcessPoolExecutor`` waits forever in both ``as_completed`` and its
    context-manager shutdown when a forkserver worker disappears.  A
    ``multiprocessing.Pool`` exposes an explicit terminate operation, letting
    the planner cancel the whole transaction on timeout, worker failure, or
    operator interruption instead of leaving a materialization wedged.
    """
    if len(jobs) != len(labels):
        raise ValueError("parallel materialization jobs and labels must have equal length")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise ValueError("parallel materialization workers must be a positive integer")
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s <= 0
    ):
        raise ValueError("materialization timeout must be finite and positive")
    if not jobs:
        return []

    pool = _MP_CONTEXT.Pool(processes=min(workers, len(jobs)))
    try:
        pending = {
            index: pool.apply_async(function, kwds=kwargs) for index, kwargs in enumerate(jobs)
        }
        pool.close()
        deadline = time.monotonic() + float(timeout_s)
        results: list[Any] = [None] * len(jobs)
        done_count = 0
        while pending:
            progressed = False
            for index, result in list(pending.items()):
                if not result.ready():
                    continue
                results[index] = result.get()
                del pending[index]
                done_count += 1
                progressed = True
                _progress(f"  [{done_count}/{len(jobs)}] {labels[index]} done")
            if pending and time.monotonic() >= deadline:
                waiting = [labels[index] for index in sorted(pending)]
                raise TimeoutError(
                    f"parallel materialization exceeded {float(timeout_s):g}s; "
                    f"unfinished variants: {waiting}"
                )
            if pending and not progressed:
                time.sleep(min(0.05, max(0.0, deadline - time.monotonic())))
        pool.join()
        return results
    except BaseException:
        pool.terminate()
        pool.join()
        raise


def runs_root(root: str | None = None) -> str:
    base_root = root or os.path.join(get_site_config().experiments_root, "runs")
    return ensure_dir(base_root)


def spec_runs_dir(spec_name: str, *, experiments_root: str | None = None) -> str:
    """Canonical run-group root mirroring the spec folder under ``eval/specs``."""
    root = runs_root(experiments_root)
    return os.path.join(root, spec_group_relpath(spec_name), spec_name)


def list_run_group_ids(spec_name: str, *, experiments_root: str | None = None) -> list[str]:
    spec_dir = spec_runs_dir(spec_name, experiments_root=experiments_root)
    if not os.path.isdir(spec_dir):
        return []

    group_ids: list[tuple[int, str]] = []
    for entry in os.listdir(spec_dir):
        match = _RUN_GROUP_RE.fullmatch(entry)
        if match is None:
            continue
        group_ids.append((int(match.group(1)), entry))
    group_ids.sort()
    return [entry for _, entry in group_ids]


def resolve_run_group_dir(
    spec_name: str,
    *,
    run_group: str,
    experiments_root: str | None = None,
) -> str:
    spec_dir = spec_runs_dir(spec_name, experiments_root=experiments_root)
    if not os.path.isdir(spec_dir):
        raise FileNotFoundError(f"No runs directory found for spec {spec_name!r}: {spec_dir}")

    group_ids = list_run_group_ids(spec_name, experiments_root=experiments_root)
    if not group_ids:
        raise FileNotFoundError(
            f"No run groups found for spec {spec_name!r} under {spec_dir}. Expected runs/<spec>/runN/."
        )

    if _RUN_GROUP_RE.fullmatch(run_group) is None:
        raise ValueError(
            "run_group must be an explicit runN identity; 'latest' is not authoritative"
        )
    if run_group not in group_ids:
        raise FileNotFoundError(
            f"Run group {run_group!r} not found for spec {spec_name!r}. Available: {', '.join(group_ids)}"
        )
    return os.path.join(spec_dir, run_group)


def _next_run_group_id(spec_name: str, *, experiments_root: str | None = None) -> str:
    group_ids = list_run_group_ids(spec_name, experiments_root=experiments_root)
    if not group_ids:
        return "run0"
    latest = max(int(_RUN_GROUP_RE.fullmatch(group_id).group(1)) for group_id in group_ids)  # type: ignore[union-attr]
    return f"run{latest + 1}"


def _claim_run_group_dir(
    spec_dir: str, spec_name: str, *, experiments_root: str | None = None
) -> tuple[str, str]:
    """Atomically allocate the next runN directory (WP2.2 / PR-018).

    ``os.mkdir`` is the exclusive-create primitive: two concurrent
    materializers computing the same candidate ID collide on EEXIST and the
    loser advances to the next index instead of silently sharing the
    directory.
    """
    while True:
        run_group_id = _next_run_group_id(spec_name, experiments_root=experiments_root)
        group_root_dir = os.path.join(spec_dir, run_group_id)
        try:
            os.mkdir(group_root_dir)
        except FileExistsError:
            continue
        return run_group_id, group_root_dir


def _progress(msg: str) -> None:
    sys.stderr.write(msg + "\n")
    sys.stderr.flush()


def _project_root_fallback() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _resolve_repo_root(repo_root: str | None = None) -> str:
    from exaserve.control.finite_process import run_finite

    fallback = os.path.abspath(repo_root or _project_root_fallback())
    result = run_finite(
        ["git", "-C", fallback, "rev-parse", "--show-toplevel"],
        timeout_s=60.0,
    )
    if result.returncode == 0 and result.stdout.strip():
        return os.path.abspath(result.stdout.strip())
    return fallback


def _git_output(repo_root: str, *args: str) -> str:
    from exaserve.control.finite_process import run_finite

    result = run_finite(["git", "-C", repo_root, *args], timeout_s=60.0)
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Git command failed in {repo_root}: git {' '.join(args)}\n{stderr}")
    return result.stdout.strip()


def _detect_repo_state(repo_root: str) -> tuple[str, list[str]]:
    commit_sha = _git_output(repo_root, "rev-parse", "HEAD")
    status_output = _git_output(repo_root, "status", "--porcelain=v1")
    dirty_files = []
    for line in status_output.splitlines():
        if not line:
            continue
        if len(line) >= 3 and line[2] == " ":
            dirty_files.append(line[3:])
        else:
            dirty_files.append(line[2:].lstrip())
    return commit_sha, dirty_files


def _warn_dirty_repo(repo_root: str, dirty_files: list[str]) -> None:
    _progress(
        f"WARNING: repo has {len(dirty_files)} uncommitted file(s); "
        f"snapshot will use committed HEAD only: {repo_root}"
    )
    for path in dirty_files:
        _progress(f"  dirty: {path}")


def _build_go_client(snapshot_root: str) -> dict[str, str]:
    from exaserve.control.finite_process import run_finite

    go_client_dir = os.path.join(snapshot_root, "eval", "go_client")
    if not os.path.isdir(go_client_dir):
        return {"go_version": "not-present", "go_dispatch_sha256": "0" * 64}
    go = shutil.which("go")
    if go is None:
        raise RuntimeError(
            "Go is required to materialize the immutable client artifact; "
            "prepare the planner environment before creating a run group"
        )
    version_result = run_finite([go, "env", "GOVERSION"], timeout_s=60.0)
    version = version_result.stdout.strip()
    if version_result.returncode != 0 or not re.fullmatch(r"go\d+\.\d+(?:\.\d+)?", version):
        detail = version_result.stderr.strip() or version_result.stdout.strip()
        raise RuntimeError(f"Go toolchain identity is invalid: {detail[:200]}")
    result = run_finite(
        [go, "build", "-trimpath", "-o", os.path.join("bin", "go_dispatch"), "."],
        timeout_s=600.0,
        cwd=go_client_dir,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RuntimeError(f"Failed to build go_client in snapshot:\n{detail}")
    binary = os.path.join(go_client_dir, "bin", "go_dispatch")
    if not os.path.isfile(binary) or not os.access(binary, os.X_OK):
        raise RuntimeError("go_client build returned zero without an executable go_dispatch")
    _progress("  go_client built successfully")
    return {
        "go_version": version,
        "go_dispatch_sha256": _sha256_file(binary),
    }


def _extract_git_archive(archive_path: str, destination: str) -> None:
    """Extract a trusted Git archive without relying on an ambient tar binary."""
    root = Path(destination).resolve()
    with tarfile.open(archive_path, "r:") as archive:
        members = archive.getmembers()
        for member in members:
            relative = Path(member.name)
            if (
                not member.name
                or relative.is_absolute()
                or ".." in relative.parts
                or not (member.isdir() or member.isfile())
            ):
                raise RuntimeError(f"Git archive contains an unsafe entry: {member.name!r}")
            target = (root / relative).resolve()
            if os.path.commonpath((str(root), str(target))) != str(root):
                raise RuntimeError(f"Git archive entry escapes the snapshot: {member.name!r}")
        # Extract explicitly instead of calling TarFile.extractall().  The
        # latter's safety semantics differ across supported Python releases;
        # these archives only need directories and regular files, including
        # Git's executable bit.
        for member in members:
            target = root / member.name
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True, mode=0o755)
                continue
            target.parent.mkdir(parents=True, exist_ok=True, mode=0o755)
            source = archive.extractfile(member)
            if source is None:
                raise RuntimeError(f"Git archive file has no payload: {member.name!r}")
            flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
            if hasattr(os, "O_NOFOLLOW"):
                flags |= os.O_NOFOLLOW
            with source, os.fdopen(os.open(target, flags, 0o600), "wb") as output:
                shutil.copyfileobj(source, output)
                output.flush()
                os.fsync(output.fileno())
            os.chmod(target, 0o755 if member.mode & 0o111 else 0o644)


def _snapshot_inventory(snapshot_root: str) -> dict[str, Any]:
    """Hash every immutable snapshot file except its self-describing metadata."""
    root = Path(snapshot_root)
    files = []
    total_bytes = 0
    for path in sorted(root.rglob("*"), key=lambda item: item.as_posix()):
        if path.name == "snapshot_meta.json" and path.parent == root:
            continue
        if path.is_symlink():
            raise RuntimeError(f"repository snapshot contains symlink {path}")
        if path.is_dir():
            continue
        if not path.is_file():
            raise RuntimeError(f"repository snapshot contains unsupported entry {path}")
        size = path.stat().st_size
        total_bytes += size
        files.append(
            {
                "path": path.relative_to(root).as_posix(),
                "size": size,
                "sha256": _sha256_file(str(path)),
            }
        )
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"))
    return {
        "artifact_manifest_hash": hashlib.sha256(canonical.encode()).hexdigest(),
        "file_count": len(files),
        "total_bytes": total_bytes,
    }


def _validate_repo_snapshot(snapshot_root: str, expected_commit: str | None = None) -> dict:
    metadata_path = os.path.join(snapshot_root, "snapshot_meta.json")
    from exaserve.state.atomic import strict_json_load_path

    metadata = strict_json_load_path(metadata_path)
    fields = {
        "schema_version",
        "commit_sha",
        "created_at",
        "source_repo_root",
        "artifact_manifest_hash",
        "file_count",
        "total_bytes",
        "go_version",
        "go_dispatch_sha256",
    }
    if not isinstance(metadata, dict) or set(metadata) != fields:
        raise RuntimeError(f"snapshot metadata shape mismatch: {metadata_path}")
    if type(metadata["schema_version"]) is not int or metadata["schema_version"] != 2:
        raise RuntimeError(f"unsupported snapshot schema at {metadata_path}")
    if expected_commit and metadata["commit_sha"] != expected_commit:
        raise RuntimeError("snapshot commit identity mismatch")
    observed = _snapshot_inventory(snapshot_root)
    for key in ("artifact_manifest_hash", "file_count", "total_bytes"):
        if metadata[key] != observed[key]:
            raise RuntimeError(f"snapshot {key} mismatch at {snapshot_root}")
    go_binary = os.path.join(snapshot_root, "eval", "go_client", "bin", "go_dispatch")
    if metadata["go_dispatch_sha256"] == "0" * 64:
        if metadata["go_version"] != "not-present" or os.path.exists(go_binary):
            raise RuntimeError("snapshot absent-Go identity mismatch")
    elif (
        not os.path.isfile(go_binary)
        or not os.access(go_binary, os.X_OK)
        or _sha256_file(go_binary) != metadata["go_dispatch_sha256"]
    ):
        raise RuntimeError("snapshot Go client identity mismatch")
    return metadata


def _ensure_repo_snapshot(repo_root: str, commit_sha: str) -> str:
    snapshot_parent = ensure_dir(get_site_config().snapshot_dir)
    snapshot_root = os.path.join(snapshot_parent, f"{commit_sha}-{_SNAPSHOT_LOCATION_TAG}")
    metadata_path = os.path.join(snapshot_root, "snapshot_meta.json")
    if os.path.lexists(metadata_path):
        _validate_repo_snapshot(snapshot_root, commit_sha)
        return snapshot_root

    temp_root = tempfile.mkdtemp(prefix=f"{commit_sha[:12]}_", dir=snapshot_parent)
    from exaserve.control.finite_process import run_finite

    archive_path = os.path.join(temp_root, ".git-archive.tar")
    try:
        archive = run_finite(
            [
                "git",
                "-C",
                repo_root,
                "archive",
                "--format=tar",
                f"--output={archive_path}",
                "HEAD",
            ],
            timeout_s=300.0,
        )
        if archive.returncode != 0:
            raise RuntimeError(
                f"Failed to export git snapshot for {commit_sha}: {archive.stderr.strip()}"
            )
        _extract_git_archive(archive_path, temp_root)
    except BaseException as exc:
        try:
            shutil.rmtree(temp_root)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            add_exception_note(exc, f"repository snapshot cleanup also failed: {cleanup_exc}")
        raise
    finally:
        active_error = sys.exc_info()[1]
        try:
            os.unlink(archive_path)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            if active_error is None:
                raise
            add_exception_note(active_error, f"snapshot archive cleanup also failed: {cleanup_exc}")

    try:
        go_identity = _build_go_client(temp_root)
        inventory = _snapshot_inventory(temp_root)

        dump_json_file(
            os.path.join(temp_root, "snapshot_meta.json"),
            {
                "schema_version": 2,
                "commit_sha": commit_sha,
                "created_at": utc_timestamp(),
                "source_repo_root": repo_root,
                **inventory,
                **go_identity,
            },
        )
        _validate_repo_snapshot(temp_root, commit_sha)

        try:
            os.replace(temp_root, snapshot_root)
            from exaserve.state.atomic import fsync_directory

            fsync_directory(os.path.dirname(snapshot_root))
        except OSError as exc:
            if exc.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                raise
            shutil.rmtree(temp_root)
            _validate_repo_snapshot(snapshot_root, commit_sha)
    except BaseException as exc:
        try:
            shutil.rmtree(temp_root)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            add_exception_note(exc, f"repository snapshot cleanup also failed: {cleanup_exc}")
        raise
    return snapshot_root


def _source_snapshot_hash(snapshot_root: str) -> str:
    """SHA-256 identity for the explicitly selected committed-HEAD artifact."""
    metadata = _validate_repo_snapshot(snapshot_root)
    commit = str(metadata.get("commit_sha", ""))
    if not commit:
        raise RuntimeError(f"snapshot metadata lacks commit_sha: {snapshot_root}")
    from exaserve.plan.contracts import canonical_hash

    return canonical_hash(
        {
            "policy": "committed_head_git_archive_plus_built_tools_v2",
            "git_commit": commit,
            "artifact_manifest_hash": metadata["artifact_manifest_hash"],
            "go_dispatch_sha256": metadata["go_dispatch_sha256"],
            "go_version": metadata["go_version"],
        }
    )


def _sha256_file(path: str) -> str:
    from exaserve.state.atomic import regular_file_reader

    digest = hashlib.sha256()
    with regular_file_reader(path, binary=True) as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _create_run_group(
    spec,
    *,
    spec_path: str,
    experiments_root: str | None,
    created_at: str,
    commit_sha: str,
    snapshot_root: str,
    dirty_files: list[str],
) -> tuple[str, str, str]:
    spec_dir = ensure_dir(spec_runs_dir(spec.name, experiments_root=experiments_root))
    run_group_id, group_root_dir = _claim_run_group_dir(
        spec_dir, spec.name, experiments_root=experiments_root
    )
    meta_dir = ensure_dir(os.path.join(group_root_dir, "meta"))
    group_spec_path = os.path.join(meta_dir, "spec.yaml")
    from exaserve.state.atomic import atomic_create_bytes, atomic_create_json, regular_file_reader

    with regular_file_reader(spec_path, binary=True) as source:
        atomic_create_bytes(group_spec_path, source.read())
    atomic_create_json(
        os.path.join(meta_dir, "run_group.json"),
        {
            "run_group_id": run_group_id,
            "created_at": created_at,
            "spec_name": spec.name,
            "spec_source_path": os.path.abspath(spec_path),
            "git_commit": commit_sha,
            "git_dirty": bool(dirty_files),
            "dirty_files": dirty_files,
            "snapshot_path": snapshot_root,
        },
    )
    return run_group_id, group_root_dir, group_spec_path


def materialize_run_bundles(
    spec_path: str,
    *,
    backend_name: str | None = None,
    experiments_root: str | None = None,
    trace_root: str | None = None,
    max_workers: int | None = None,
    repo_root: str | None = None,
    force_trace: bool = False,
    allow_dirty: bool = False,
    timeout_s: float = _DEFAULT_MATERIALIZATION_TIMEOUT_S,
) -> list[RunMaterialization]:
    spec = load_experiment_spec(spec_path)
    variants = expand_matrix(spec)
    run_ids: dict[str, str] = {}
    for variant in variants:
        run_id = slugify(variant.variant_name)
        prior = run_ids.get(run_id)
        if prior is not None:
            raise ValueError(
                "variant names collide after filesystem-safe normalization: "
                f"{prior!r} and {variant.variant_name!r} both map to {run_id!r}"
            )
        run_ids[run_id] = variant.variant_name
    total = len(variants)
    resolved_backend = backend_name or spec.backend.default
    repo_root_live = _resolve_repo_root(repo_root)
    commit_sha, dirty_files = _detect_repo_state(repo_root_live)
    if dirty_files:
        _warn_dirty_repo(repo_root_live, dirty_files)
        # PR-034: the snapshot uses committed HEAD and SILENTLY excludes
        # uncommitted edits. Require an explicit acknowledgement so a user
        # cannot believe they evaluated the code in their editor. This is an
        # explicit API/CLI choice, never an inherited environment bypass.
        if not allow_dirty:
            raise RuntimeError(
                f"repo has {len(dirty_files)} uncommitted file(s); the snapshot "
                "uses committed HEAD and would EXCLUDE them. Commit your changes, "
                "or acknowledge stale-code execution with --allow-dirty "
                "(the recorded run_group.json "
                "keeps git_dirty/dirty_files for provenance)."
            )
    snapshot_root = _ensure_repo_snapshot(repo_root_live, commit_sha)
    created_at = utc_timestamp()
    run_group_id, group_root_dir, group_spec_path = _create_run_group(
        spec,
        spec_path=spec_path,
        experiments_root=experiments_root,
        created_at=created_at,
        commit_sha=commit_sha,
        snapshot_root=snapshot_root,
        dirty_files=dirty_files,
    )
    _progress(
        f"Materializing {total} variant(s) for {spec.name!r} in {run_group_id!r} "
        f"(backend={resolved_backend})"
    )

    if total <= 1:
        run_plans = []
        for variant in variants:
            _progress(f"  [1/{total}] {variant.variant_name} ...")
            rp = _materialize_variant(
                variant,
                backend_name=resolved_backend,
                group_root_dir=group_root_dir,
                run_group_id=run_group_id,
                group_created_at=created_at,
                group_spec_path=group_spec_path,
                snapshot_root=snapshot_root,
                trace_root=trace_root,
                force_trace=force_trace,
            )
            _progress(f"  [1/{total}] {variant.variant_name} done")
            run_plans.append(rp)
        return run_plans

    workers = min(max_workers or _DEFAULT_MAX_WORKERS, total)
    _progress(f"  using {workers} workers")
    return _bounded_process_map(
        _materialize_variant,
        [
            {
                "variant": variant,
                "backend_name": resolved_backend,
                "group_root_dir": group_root_dir,
                "run_group_id": run_group_id,
                "group_created_at": created_at,
                "group_spec_path": group_spec_path,
                "snapshot_root": snapshot_root,
                "trace_root": trace_root,
                "force_trace": force_trace,
            }
            for variant in variants
        ],
        labels=[variant.variant_name for variant in variants],
        workers=workers,
        timeout_s=timeout_s,
    )


def materialize_traces(
    spec_path: str,
    *,
    trace_root: str | None = None,
    max_workers: int | None = None,
    force: bool = False,
    timeout_s: float = _DEFAULT_MATERIALIZATION_TIMEOUT_S,
) -> list[TraceArtifact]:
    spec = load_experiment_spec(spec_path)
    variants = expand_matrix(spec)
    total = len(variants)
    _progress(f"Materializing {total} trace(s) for {spec.name!r}")

    if total <= 1:
        artifacts = []
        for variant in variants:
            _progress(f"  [1/{total}] {variant.variant_name} ...")
            art = materialize_trace_artifact(variant, store_root=trace_root, force=force)
            _progress(f"  [1/{total}] {variant.variant_name} done")
            artifacts.append(art)
        return artifacts

    workers = min(max_workers or _DEFAULT_MAX_WORKERS, total)
    _progress(f"  using {workers} workers")
    return _bounded_process_map(
        materialize_trace_artifact,
        [{"variant": variant, "store_root": trace_root, "force": force} for variant in variants],
        labels=[variant.variant_name for variant in variants],
        workers=workers,
        timeout_s=timeout_s,
    )


def _materialization_text(value: object, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        qualifier = "text" if allow_empty else "non-empty text"
        raise ValueError(f"run.yaml {field} must be {qualifier}")
    return value


def load_run_plan(path: str) -> RunMaterialization:
    from exaserve.state.atomic import regular_file_reader
    from exaserve.yaml_support import load_yaml_mapping_text

    with regular_file_reader(path) as handle:
        data = load_yaml_mapping_text(handle.read(), source=path)
    if not isinstance(data, dict):
        raise ValueError("run.yaml must be a mapping")
    location_keys = {
        "run_id",
        "run_group_id",
        "created_at",
        "repo_root",
        "snapshot_root",
        "bundle",
        "spec_name",
        "variant_name",
        "axis_values",
        "trace_artifact",
        "runtime_manifest_path",
        "semantic_plan_path",
        "deployment_plan_path",
        "site_profile_path",
        "run_semantic_hash",
        "deployment_plan_hash",
        "source_snapshot_hash",
        "input_prompt_path",
        "input_trace_path",
        "spec_path",
    }
    unknown = sorted(set(data) - location_keys)
    missing = sorted((location_keys - {"input_prompt_path", "input_trace_path"}) - set(data))
    if unknown or missing:
        raise ValueError(f"run.yaml shape mismatch: unknown={unknown}, missing={missing}")
    bundle_raw = data["bundle"]
    trace_raw = data["trace_artifact"]
    if not isinstance(bundle_raw, dict) or set(bundle_raw) != set(RunBundle.__dataclass_fields__):
        raise ValueError("run.yaml bundle shape mismatch")
    if not isinstance(trace_raw, dict) or set(trace_raw) != set(TraceArtifact.__dataclass_fields__):
        raise ValueError("run.yaml trace_artifact shape mismatch")

    bundle = RunBundle(
        **{
            field: _materialization_text(bundle_raw[field], f"bundle.{field}")
            for field in RunBundle.__dataclass_fields__
        }
    )
    trace_artifact = TraceArtifact(
        **{
            field: _materialization_text(trace_raw[field], f"trace_artifact.{field}")
            for field in TraceArtifact.__dataclass_fields__
        }
    )
    _validate_materialization_paths(
        source_path=path,
        bundle=bundle,
        trace_artifact=trace_artifact,
        data=data,
    )
    from exaserve.plan.io import (
        load_deployment_plan,
        load_run_plan as load_semantic_run_plan,
        load_site_profile,
    )

    semantic = load_semantic_run_plan(
        _materialization_text(data["semantic_plan_path"], "semantic_plan_path")
    )
    variant_name = _materialization_text(data["variant_name"], "variant_name")
    _validate_materialized_trace(trace_artifact, semantic)
    observed_snapshot_hash = _source_snapshot_hash(
        _materialization_text(data["snapshot_root"], "snapshot_root")
    )
    if data["source_snapshot_hash"] != observed_snapshot_hash:
        raise ValueError("run.yaml source_snapshot_hash disagrees with immutable snapshot")
    axis_values = data.get("axis_values")
    if not isinstance(axis_values, dict) or any(not isinstance(key, str) for key in axis_values):
        raise ValueError("run.yaml axis_values must be a mapping with string keys")
    from exaserve.plan.contracts import canonical_hash

    canonical_hash(axis_values)  # validates the complete JSON value family
    materialization = RunMaterialization(
        run_id=_materialization_text(data["run_id"], "run_id"),
        run_group_id=_materialization_text(data["run_group_id"], "run_group_id"),
        created_at=_materialization_text(data["created_at"], "created_at"),
        repo_root=_materialization_text(data["repo_root"], "repo_root"),
        snapshot_root=_materialization_text(data["snapshot_root"], "snapshot_root"),
        bundle=bundle,
        spec_name=_materialization_text(data["spec_name"], "spec_name"),
        variant_name=variant_name,
        axis_values=dict(axis_values),
        trace_artifact=trace_artifact,
        runtime_manifest_path=_materialization_text(
            data["runtime_manifest_path"], "runtime_manifest_path"
        ),
        semantic_plan_path=_materialization_text(data["semantic_plan_path"], "semantic_plan_path"),
        deployment_plan_path=_materialization_text(
            data["deployment_plan_path"], "deployment_plan_path"
        ),
        site_profile_path=_materialization_text(data["site_profile_path"], "site_profile_path"),
        run_semantic_hash=_materialization_text(data["run_semantic_hash"], "run_semantic_hash"),
        deployment_plan_hash=_materialization_text(
            data["deployment_plan_hash"], "deployment_plan_hash"
        ),
        source_snapshot_hash=_materialization_text(
            data["source_snapshot_hash"], "source_snapshot_hash"
        ),
        input_prompt_path=_materialization_text(
            data.get("input_prompt_path", ""), "input_prompt_path", allow_empty=True
        ),
        input_trace_path=_materialization_text(
            data.get("input_trace_path", ""), "input_trace_path", allow_empty=True
        ),
        spec_path=_materialization_text(data.get("spec_path", ""), "spec_path"),
        semantic_plan=semantic,
    )
    deployment = load_deployment_plan(materialization.deployment_plan_path)
    site_profile = load_site_profile(materialization.site_profile_path)
    if semantic.run_semantic_hash != materialization.run_semantic_hash:
        raise ValueError("run.yaml run_semantic_hash disagrees with canonical RunPlan")
    if deployment.deployment_plan_hash != materialization.deployment_plan_hash:
        raise ValueError("run.yaml deployment_plan_hash disagrees with DeploymentPlan")
    if semantic.deployment.deployment_plan_hash != deployment.deployment_plan_hash:
        raise ValueError("canonical RunPlan embeds a different DeploymentPlan")
    if deployment.site_profile_hash != site_profile.site_profile_hash:
        raise ValueError("DeploymentPlan and SiteProfile hashes disagree")
    if not _RUN_GROUP_RE.fullmatch(materialization.run_group_id):
        raise ValueError("run.yaml run_group_id must be an exact runN identity")
    if not re.fullmatch(r"\d{8}T\d{6}Z", materialization.created_at):
        raise ValueError("run.yaml created_at must be a canonical UTC timestamp")
    if materialization.run_id != slugify(materialization.variant_name):
        raise ValueError("run.yaml run_id disagrees with the canonical variant slug")
    if Path(bundle.group_root_dir).name != materialization.run_group_id:
        raise ValueError("run.yaml run_group_id disagrees with its bundle directory")
    if Path(bundle.root_dir).name != materialization.run_id:
        raise ValueError("run.yaml run_id disagrees with its bundle directory")
    expected_run_identity = (
        f"{materialization.spec_name}/{materialization.run_group_id}/{materialization.run_id}"
    )
    if semantic.run_id != expected_run_identity:
        raise ValueError("run.yaml identity disagrees with canonical RunPlan.run_id")
    expected_deployment_id = slugify(
        f"{materialization.spec_name}-{materialization.run_group_id}-{materialization.run_id}"
    )[:40]
    if deployment.deployment_id != expected_deployment_id:
        raise ValueError("run.yaml identity disagrees with DeploymentPlan.deployment_id")
    return materialization


def _canonical_absolute_path(value: object, field: str) -> Path:
    if not isinstance(value, str) or not value:
        raise ValueError(f"run.yaml {field} must be non-empty text")
    path = Path(value)
    if not path.is_absolute():
        raise ValueError(f"run.yaml {field} must be absolute")
    return Path(os.path.realpath(path))


def _require_child(child: Path, parent: Path, field: str) -> None:
    try:
        child.relative_to(parent)
    except ValueError as exc:
        raise ValueError(f"run.yaml {field} escapes its owning run directory") from exc


def _validate_materialization_paths(
    *,
    source_path: str,
    bundle: RunBundle,
    trace_artifact: TraceArtifact,
    data: dict[str, Any],
) -> None:
    """Reject path substitution before a materialization can launch or write.

    `run.yaml` is location metadata rather than a semantic plan, so its paths
    are not covered by `run_semantic_hash`. Their exact containment contract is
    therefore verified independently at every load.
    """
    group = _canonical_absolute_path(bundle.group_root_dir, "bundle.group_root_dir")
    root = _canonical_absolute_path(bundle.root_dir, "bundle.root_dir")
    _require_child(root, group, "bundle.root_dir")
    if root == group:
        raise ValueError("run.yaml bundle.root_dir must be a variant below group_root_dir")

    expected_dirs = {
        "bundle.logs_dir": (bundle.logs_dir, root / "logs"),
        "bundle.pbs_stdout_dir": (bundle.pbs_stdout_dir, root / "logs" / "pbs" / "stdout"),
        "bundle.pbs_stderr_dir": (bundle.pbs_stderr_dir, root / "logs" / "pbs" / "stderr"),
        "bundle.results_dir": (bundle.results_dir, root / "results"),
        "bundle.state_dir": (bundle.state_dir, root / "state"),
        "bundle.runtime_dir": (bundle.runtime_dir, root / "runtime"),
    }
    for field, (raw, expected) in expected_dirs.items():
        if _canonical_absolute_path(raw, field) != expected:
            raise ValueError(f"run.yaml {field} does not match the canonical bundle layout")

    exact_files = {
        "bundle.run_yaml_path": (bundle.run_yaml_path, root / "run.yaml"),
        "bundle.job_path": (bundle.job_path, root / "job" / "job.pbs"),
        "bundle.state_path": (bundle.state_path, root / "state" / "status.json"),
        "semantic_plan_path": (data["semantic_plan_path"], root / "runtime" / "run.plan.json"),
        "deployment_plan_path": (
            data["deployment_plan_path"],
            root / "runtime" / "deployment.plan.json",
        ),
        "site_profile_path": (data["site_profile_path"], root / "runtime" / "site.profile.json"),
    }
    for field, (raw, expected) in exact_files.items():
        if _canonical_absolute_path(raw, field) != expected:
            raise ValueError(f"run.yaml {field} does not match the canonical bundle layout")

    if _canonical_absolute_path(source_path, "source_path") != root / "run.yaml":
        raise ValueError("run.yaml self path disagrees with the file being loaded")
    runtime_manifest = _canonical_absolute_path(
        data["runtime_manifest_path"], "runtime_manifest_path"
    )
    _require_child(runtime_manifest, root / "runtime", "runtime_manifest_path")
    if not runtime_manifest.name.endswith("_runtime.yaml"):
        raise ValueError("run.yaml runtime_manifest_path has an unexpected filename")

    snapshot = _canonical_absolute_path(data["snapshot_root"], "snapshot_root")
    repo_root = _canonical_absolute_path(data["repo_root"], "repo_root")
    if repo_root != snapshot:
        raise ValueError("run.yaml repo_root must be the immutable snapshot_root")
    spec_path = _canonical_absolute_path(data.get("spec_path", ""), "spec_path")
    if spec_path != group / "meta" / "spec.yaml":
        raise ValueError("run.yaml spec_path does not match the run-group snapshot")

    trace_store = _canonical_absolute_path(trace_artifact.store_dir, "trace_artifact.store_dir")
    if not re.fullmatch(r"[0-9a-f]{64}", trace_artifact.trace_id):
        raise ValueError("run.yaml trace_artifact.trace_id must be a lowercase SHA-256")
    if trace_artifact.spec_hash != trace_artifact.trace_id:
        raise ValueError("run.yaml trace_artifact.spec_hash disagrees with trace_id")
    if trace_store.name != trace_artifact.trace_id:
        raise ValueError("run.yaml trace_artifact.store_dir disagrees with trace_id")
    expected_trace_files = {
        "trace_artifact.trace_path": (trace_artifact.trace_path, trace_store / "trace.jsonl"),
        "trace_artifact.metadata_path": (
            trace_artifact.metadata_path,
            trace_store / "metadata.json",
        ),
    }
    for field, (raw, expected) in expected_trace_files.items():
        if _canonical_absolute_path(raw, field) != expected:
            raise ValueError(f"run.yaml {field} does not match the trace-store layout")


def _validate_materialized_trace(
    trace_artifact: TraceArtifact,
    semantic_plan: Any,
) -> None:
    from .trace_store import validate_trace_artifact

    try:
        metadata = validate_trace_artifact(trace_artifact)
    except RuntimeError as exc:
        raise ValueError(f"materialized trace artifact is invalid: {exc}") from exc
    expected_hash = semantic_plan.trace.trace_content_hash
    observed_trace_hash = metadata["trace_sha256"]
    if expected_hash is None or observed_trace_hash != expected_hash:
        raise ValueError("materialized trace content disagrees with canonical RunPlan")


def write_run_state(run_plan: RunMaterialization, status: str, **extra: Any) -> None:
    """Publish one canonical RunStatus transition/update, fail closed."""
    from exaserve.state.status import RunState, StatusStore

    store = StatusStore.run(run_plan.bundle.state_path)
    phase = str(status).strip().lower()
    _validate_run_state_payload(run_plan, phase, extra)
    record = store.load()
    if phase == "materialized":
        if record is not None:
            raise RuntimeError(f"run status already exists at {run_plan.bundle.state_path}")
        store.initialize(
            f"{run_plan.run_group_id}/{run_plan.run_id}",
            RunState.PLANNED,
            provenance={
                "run_id": run_plan.run_id,
                "run_group_id": run_plan.run_group_id,
                "run_semantic_hash": run_plan.run_semantic_hash,
                "deployment_plan_hash": run_plan.deployment_plan_hash,
                "source_snapshot_hash": run_plan.source_snapshot_hash,
            },
            data={"phase": phase, **extra},
        )
        return
    if record is None:
        raise RuntimeError("run status was not initialized during materialization")
    expected_provenance = {
        "run_id": run_plan.run_id,
        "run_group_id": run_plan.run_group_id,
        "run_semantic_hash": run_plan.run_semantic_hash,
        "deployment_plan_hash": run_plan.deployment_plan_hash,
        "source_snapshot_hash": run_plan.source_snapshot_hash,
    }
    if (
        record.record_id != f"{run_plan.run_group_id}/{run_plan.run_id}"
        or record.provenance != expected_provenance
    ):
        raise RuntimeError("RunStatus identity/provenance disagrees with RunPlan")
    state = RunState(record.state)
    data = {"phase": phase, **extra}
    target_by_phase = {
        "submitted": RunState.SUBMITTED,
        "running": RunState.RUNNING,
        "succeeded": RunState.SUCCEEDED,
        "partial": RunState.PARTIAL,
        "failed": RunState.FAILED,
        "cancelled": RunState.CANCELLED,
        "invalid": RunState.INVALID,
    }
    target = target_by_phase.get(phase)
    if target is None or target == state:
        store.update(
            state, reason_code=phase.upper(), data_update=data, expected_revision=record.revision
        )
        return
    # A direct in-allocation execution may legitimately begin before a
    # scheduler observer saw SUBMITTED; preserve that fact as an explicit edge.
    if phase == "running" and state == RunState.PLANNED:
        record = store.transition(
            state,
            RunState.SUBMITTED,
            reason_code="EXECUTION_STARTED_WITHOUT_SUBMIT_OBSERVATION",
            data_update={"phase": "submitted_unobserved"},
            expected_revision=record.revision,
        )
        state = RunState.SUBMITTED
    store.transition(
        state,
        target,
        reason_code=phase.upper(),
        data_update=data,
        expected_revision=record.revision,
    )


def _validate_run_state_payload(
    run_plan: RunMaterialization, phase: str, extra: dict[str, Any]
) -> None:
    """Strict phase data and terminal ResultManifest coupling."""
    allowed = {
        "materialized": {"scheduler_run_identity"},
        "dry-run": set(),
        "submitting": {"submit_attempt"},
        "planned": {"last_submit_error"},
        "submitted": {"scheduler_job_id", "scheduler_run_identity", "reconciled"},
        "running": {"backend"},
        "replaying": {"base_urls"},
        "succeeded": {
            "base_urls",
            "exit_code",
            "result_path",
            "requests_completed",
            "requests_scheduled",
            "errors",
            "incomplete_reasons",
            "result_manifest_hash",
        },
        "partial": {
            "base_urls",
            "exit_code",
            "result_path",
            "requests_completed",
            "requests_scheduled",
            "errors",
            "incomplete_reasons",
            "result_manifest_hash",
        },
        "failed": {"base_urls", "exit_code", "error", "cleanup_error"},
        "cancelled": {"error"},
        "invalid": {"last_submit_error", "submit_attempts", "error"},
    }
    if phase not in allowed:
        raise ValueError(f"unknown RunStatus phase {phase!r}")
    unknown = sorted(set(extra) - allowed[phase])
    if unknown:
        raise ValueError(f"RunStatus {phase} has unknown fields: {unknown}")
    for key in (
        "submit_attempt",
        "submit_attempts",
        "exit_code",
        "requests_completed",
        "requests_scheduled",
        "errors",
    ):
        if key in extra and (
            isinstance(extra[key], bool)
            or not isinstance(extra[key], int)
            or (key != "exit_code" and extra[key] < 0)
        ):
            raise ValueError(f"RunStatus {phase}.{key} has an invalid type/value")
    if "reconciled" in extra and not isinstance(extra["reconciled"], bool):
        raise ValueError("RunStatus submitted.reconciled must be boolean")
    for key in (
        "scheduler_run_identity",
        "scheduler_job_id",
        "backend",
        "last_submit_error",
        "error",
        "cleanup_error",
        "result_path",
        "result_manifest_hash",
    ):
        if key in extra and (not isinstance(extra[key], str) or not extra[key]):
            raise ValueError(f"RunStatus {phase}.{key} must be non-empty text")
    if "base_urls" in extra and (
        not isinstance(extra["base_urls"], list)
        or any(not isinstance(item, str) or not item for item in extra["base_urls"])
    ):
        raise ValueError(f"RunStatus {phase}.base_urls must be a string array")
    if (
        "incomplete_reasons" in extra
        and extra["incomplete_reasons"] is not None
        and (
            not isinstance(extra["incomplete_reasons"], list)
            or any(not isinstance(item, str) or not item for item in extra["incomplete_reasons"])
        )
    ):
        raise ValueError(f"RunStatus {phase}.incomplete_reasons must be null or a string array")
    if phase in {"succeeded", "partial"}:
        required = {"exit_code", "result_manifest_hash"}
        missing = sorted(required - set(extra))
        if missing:
            raise ValueError(f"RunStatus {phase} missing fields: {missing}")
        if phase == "succeeded" and extra["exit_code"] != 0:
            raise ValueError("a succeeded RunStatus must carry exit_code 0")
        from exaserve.state.results import load_result_manifest

        manifest = load_result_manifest(
            os.path.join(run_plan.bundle.results_dir, "result_manifest.json")
        )
        if (
            manifest.run_id != run_plan.run_id
            or manifest.run_semantic_hash != run_plan.run_semantic_hash
            or manifest.deployment_plan_hash != run_plan.deployment_plan_hash
            or manifest.manifest_hash != extra["result_manifest_hash"]
        ):
            raise ValueError("terminal RunStatus disagrees with ResultManifest identity")
        if manifest.complete != (phase == "succeeded"):
            raise ValueError(f"RunStatus {phase} disagrees with ResultManifest completeness")


def _materialize_variant(
    variant: VariantSpec,
    *,
    backend_name: str,
    group_root_dir: str,
    run_group_id: str,
    group_created_at: str,
    group_spec_path: str,
    snapshot_root: str,
    trace_root: str | None,
    force_trace: bool = False,
) -> RunMaterialization:
    spec = variant.spec
    artifact = materialize_trace_artifact(variant, store_root=trace_root, force=force_trace)
    run_id = slugify(variant.variant_name)
    root_dir = os.path.join(group_root_dir, run_id)
    bundle = RunBundle(
        group_root_dir=group_root_dir,
        root_dir=ensure_dir(root_dir),
        run_yaml_path=os.path.join(root_dir, "run.yaml"),
        job_path=os.path.join(root_dir, "job", "job.pbs"),
        logs_dir=ensure_dir(os.path.join(root_dir, "logs")),
        pbs_stdout_dir=ensure_dir(os.path.join(root_dir, "logs", "pbs", "stdout")),
        pbs_stderr_dir=ensure_dir(os.path.join(root_dir, "logs", "pbs", "stderr")),
        results_dir=ensure_dir(os.path.join(root_dir, "results")),
        state_dir=ensure_dir(os.path.join(root_dir, "state")),
        state_path=os.path.join(root_dir, "state", "status.json"),
        runtime_dir=ensure_dir(os.path.join(root_dir, "runtime")),
    )
    runtime_manifest_path = os.path.join(bundle.runtime_dir, f"{backend_name}_runtime.yaml")
    scheduler = _resolve_scheduler(spec.scheduler)
    semantic_plan_path = os.path.join(bundle.runtime_dir, "run.plan.json")
    deployment_plan_path = os.path.join(bundle.runtime_dir, "deployment.plan.json")
    site_profile_path = os.path.join(bundle.runtime_dir, "site.profile.json")
    source_snapshot_hash = _source_snapshot_hash(snapshot_root)

    from exaserve.plan.contracts import ArtifactPolicy
    from exaserve.plan.io import (
        write_deployment_plan,
        write_run_plan as write_semantic_run_plan,
        write_site_profile,
    )
    from exaserve.site import default_site_profile

    site_profile = default_site_profile()
    resolved_spec = replace(
        spec,
        scheduler=scheduler,
        backend=replace(spec.backend, default=backend_name),
    )
    semantic_plan = compile_shared_run_plan(
        resolved_spec,
        run_id=f"{spec.name}/{run_group_id}/{run_id}",
        deployment_id=slugify(f"{spec.name}-{run_group_id}-{run_id}")[:40],
        site=site_profile,
    )
    # The trace is already materialized at this point, so its semantic identity
    # is the real content digest rather than a path or a cache identity.
    semantic_plan = replace(
        semantic_plan,
        trace=replace(semantic_plan.trace, trace_content_hash=_sha256_file(artifact.trace_path)),
        artifacts=ArtifactPolicy(
            source_snapshot_policy="committed_head_git_archive_plus_built_tools_v2",
            trace_store_ref="content-addressed-sha256",
            output_policy="manifest_complete",
            retention_policy="site_default",
            expected_artifacts=(
                "deployment_status.json",
                "run_provenance.json",
                "result_manifest.json",
                "replay_result",
            ),
        ),
    ).finalize()
    write_semantic_run_plan(semantic_plan_path, semantic_plan)
    write_deployment_plan(deployment_plan_path, semantic_plan.deployment)
    write_site_profile(site_profile_path, site_profile)

    run_plan = RunMaterialization(
        run_id=run_id,
        run_group_id=run_group_id,
        created_at=group_created_at,
        repo_root=snapshot_root,
        snapshot_root=snapshot_root,
        bundle=bundle,
        spec_name=spec.name,
        variant_name=variant.variant_name,
        axis_values=dict(variant.axis_values),
        trace_artifact=artifact,
        runtime_manifest_path=runtime_manifest_path,
        semantic_plan_path=semantic_plan_path,
        deployment_plan_path=deployment_plan_path,
        site_profile_path=site_profile_path,
        run_semantic_hash=semantic_plan.run_semantic_hash,
        deployment_plan_hash=semantic_plan.deployment.deployment_plan_hash,
        source_snapshot_hash=source_snapshot_hash,
        input_prompt_path=spec.trace.input_prompt_path,
        input_trace_path=spec.trace.input_trace_path,
        spec_path=group_spec_path,
        semantic_plan=semantic_plan,
    )

    adapter = get_backend_adapter(backend_name)
    adapter.validate(run_plan)
    adapter.build_runtime_manifest(run_plan)
    runtime_env = adapter.runtime_env(run_plan)

    ensure_dir(os.path.dirname(bundle.job_path))
    job_exports = dict(adapter.job_env_exports(run_plan) or {})
    # PBS sites commonly cap Job_Name at 15 characters.  Use a collision-
    # resistant, exact scheduler-visible identity so an ambiguous submit can
    # be reconciled without fuzzy/truncated-name matching.
    scheduler_identity = scheduler_run_identity(
        run_semantic_hash=semantic_plan.run_semantic_hash,
        spec_name=spec.name,
        run_group_id=run_group_id,
        run_id=run_id,
    )
    job_text = get_scheduler(getattr(scheduler, "type", "pbs")).render_job(
        JobSpec(
            job_name=scheduler_identity,
            num_nodes=scheduler.nodes,
            queue=scheduler.queue,
            walltime=scheduler.walltime,
            account=scheduler.project,
            filesystems=scheduler.filesystems,
            keep_flag=scheduler.keep_output,
            stdout_dir=bundle.pbs_stdout_dir,
            stderr_dir=bundle.pbs_stderr_dir,
            mail_user=scheduler.mail_user,
            mail_events=scheduler.mail_events,
            cwd=run_plan.repo_root,
            source_env_script=Path(runtime_env.env_script),
            pythonpath=(Path(run_plan.repo_root), Path(run_plan.repo_root) / "src"),
            environment_unset=tuple(site_profile.environment_unset),
            command_argv=("python3", "-m", "eval.cli", "run", "execute", bundle.run_yaml_path),
            environment=job_exports,
            gpus_per_node=getattr(run_plan.deployment, "num_gpus_per_node", None),
            run_identity=scheduler_identity,
        )
    )
    from exaserve.state.atomic import atomic_create_text, atomic_create_yaml

    atomic_create_text(bundle.job_path, job_text)

    atomic_create_yaml(bundle.run_yaml_path, dataclass_to_dict(run_plan))
    write_run_state(run_plan, "materialized", scheduler_run_identity=scheduler_identity)
    return run_plan


def _resolve_scheduler(spec: SchedulerSpec) -> SchedulerSpec:
    queue = spec.queue
    walltime = spec.walltime
    if not queue or not walltime:
        default_queue, default_walltime = default_queue_and_walltime(spec.nodes)
        if not queue:
            queue = default_queue
        if not walltime:
            walltime = default_walltime
    return replace(spec, queue=queue, walltime=walltime)
