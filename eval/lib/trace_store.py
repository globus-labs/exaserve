"""Content-addressable trace store with deduplication.

Traces are expensive to generate (tokenizer loading, prompt truncation),
so this module caches them by a stable hash of their "identity" — the set
of spec fields that affect trace content (workload params, deployment
topology, trace kind). Two specs that differ only in scheduler or client
settings will share the same cached trace.

Trace generation logic lives in trace_generators.py. This module handles
caching, identity hashing, and artifact management.
"""

from __future__ import annotations

import os
import pathlib
import re
import tempfile
from typing import Any

from eval.site_config import get_site_config

from .models import ExperimentSpec, TraceArtifact, VariantSpec
from .trace_generators import (
    TRACE_GENERATOR_VERSION,
    generate_rows,
    tokenizer_source_identity,
    write_trace,
)
from .utils import ensure_dir, stable_hash


def trace_store_root(root: str | None = None) -> str:
    base_root = root or os.path.join(get_site_config().experiments_root, "traces")
    return ensure_dir(base_root)


def _content_digest(path: str) -> str:
    """SHA-256 of an input file's CONTENT (PR-017: paths alone allow stale
    cache reuse after in-place edits). Empty path -> empty digest."""
    if not path:
        return ""
    import hashlib

    digest = hashlib.sha256()
    from exaserve.state.atomic import regular_file_reader

    with regular_file_reader(path, binary=True) as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _trace_identity(spec: ExperimentSpec) -> dict[str, Any]:
    return {
        "version": TRACE_GENERATOR_VERSION,
        "trace": {
            "kind": spec.trace.kind,
            "input_prompt_digest": _content_digest(spec.trace.input_prompt_path),
            "input_trace_digest": _content_digest(spec.trace.input_trace_path),
            "tokenizer_builder": getattr(spec.trace, "tokenizer_builder", ""),
            "tokenizer_source": tokenizer_source_identity(spec),
        },
        "workload": {
            "duration": spec.workload.duration,
            "input_len": spec.workload.input_len,
            "output_len": spec.workload.output_len,
            "rate_per_node": spec.workload.rate_per_node,
            "speedup": spec.workload.speedup,
            "sampling_strategy": spec.workload.sampling_strategy,
            "seed": spec.workload.seed,
            "modes": spec.workload.modes,
            # PR-017: arrival changes generated timestamps; fixed/poisson
            # variants must not collide on one cached trace.
            "arrival": getattr(spec.workload, "arrival", "fixed"),
        },
        "deployment": {
            "num_nodes": spec.deployment.num_nodes,
            "models": [
                {
                    "model_id": model.model_id,
                    "tensor_parallel_size": model.tensor_parallel_size,
                    "pipeline_parallel_size": model.pipeline_parallel_size,
                    "max_model_len": model.max_model_len,
                    "size": model.size,
                }
                for model in spec.deployment.models
            ],
        },
    }


def _trace_content_stats(path: str) -> tuple[str, int]:
    import hashlib

    from exaserve.state.atomic import regular_file_reader, strict_json_loads

    digest = hashlib.sha256()
    line_count = 0
    first_line = None
    with regular_file_reader(path, binary=True) as handle:
        for line in handle:
            digest.update(line)
            line_count += 1
            if first_line is None:
                first_line = line
    if first_line is None:
        raise RuntimeError("trace artifact is empty")
    try:
        header = strict_json_loads(first_line.decode("utf-8"))
    except (UnicodeError, ValueError) as exc:
        raise RuntimeError(f"trace artifact metadata row is invalid: {exc}") from exc
    if (
        not isinstance(header, dict)
        or header.get("__type__") != "metadata"
        or header.get("generator_version") != TRACE_GENERATOR_VERSION
    ):
        raise RuntimeError("trace artifact metadata row has the wrong generator identity")
    return digest.hexdigest(), line_count - 1


def validate_trace_artifact(
    artifact: TraceArtifact, *, expected_identity: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Validate one completed immutable cache entry from descriptor-bound reads."""
    from exaserve.state.atomic import strict_json_load_path

    if not re.fullmatch(r"[0-9a-f]{64}", artifact.trace_id):
        raise RuntimeError("trace artifact id must be a lowercase SHA-256")
    if artifact.spec_hash != artifact.trace_id:
        raise RuntimeError("trace artifact spec hash disagrees with its trace id")
    if pathlib.Path(artifact.store_dir).name != artifact.trace_id:
        raise RuntimeError("trace artifact directory disagrees with its trace id")
    try:
        metadata = strict_json_load_path(artifact.metadata_path)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"trace artifact completion metadata is invalid: {exc}") from exc
    expected_fields = {
        "schema_version",
        "trace_id",
        "trace_identity",
        "trace_sha256",
        "row_count",
    }
    if not isinstance(metadata, dict) or set(metadata) != expected_fields:
        raise RuntimeError("trace artifact completion metadata has an unexpected shape")
    if type(metadata["schema_version"]) is not int or metadata["schema_version"] != 1:
        raise RuntimeError("trace artifact completion metadata schema is unsupported")
    if metadata["trace_id"] != artifact.trace_id:
        raise RuntimeError("trace artifact completion metadata has the wrong trace id")
    identity_digest = stable_hash(metadata["trace_identity"], length=64)
    if identity_digest != artifact.trace_id:
        raise RuntimeError("trace artifact identity does not derive its trace id")
    if (
        expected_identity is not None
        and stable_hash(expected_identity, length=64) != identity_digest
    ):
        raise RuntimeError("trace artifact identity disagrees with the requested trace inputs")
    row_count = metadata["row_count"]
    if isinstance(row_count, bool) or not isinstance(row_count, int) or row_count < 1:
        raise RuntimeError("trace artifact row_count must be a positive integer")
    digest, observed_rows = _trace_content_stats(artifact.trace_path)
    if metadata["trace_sha256"] != digest:
        raise RuntimeError("trace artifact content hash disagrees with completion metadata")
    if row_count != observed_rows:
        raise RuntimeError("trace artifact row count disagrees with completion metadata")
    return metadata


def materialize_trace_artifact(
    variant: VariantSpec,
    *,
    store_root: str | None = None,
    force: bool = False,
) -> TraceArtifact:
    from exaserve.state.atomic import ExclusiveLease, LeaseHeartbeat, atomic_create_json

    spec = variant.spec
    identity = _trace_identity(spec)
    trace_id = stable_hash(identity, length=64)
    artifact_dir = ensure_dir(os.path.join(trace_store_root(store_root), trace_id))
    trace_path = os.path.join(artifact_dir, "trace.jsonl")
    metadata_path = os.path.join(artifact_dir, "metadata.json")
    artifact = TraceArtifact(
        trace_id=trace_id,
        trace_path=trace_path,
        metadata_path=metadata_path,
        spec_hash=trace_id,
        store_dir=artifact_dir,
    )
    # WP2.6: metadata.json is the completion marker (published atomically
    # AFTER the trace); its absence means the artifact is incomplete even if
    # trace.jsonl exists. Generation runs under an exclusive lease so
    # concurrent materializers cannot double-write.
    if os.path.lexists(metadata_path) and not force:
        validate_trace_artifact(artifact, expected_identity=identity)
        return artifact

    with ExclusiveLease(
        os.path.join(artifact_dir, ".generate.lease"),
        ttl_s=1800,
        owner_note=f"trace {trace_id}",
    ) as lease:
        if os.path.lexists(metadata_path) and not force:
            validate_trace_artifact(artifact, expected_identity=identity)
            return artifact
        with LeaseHeartbeat(lease, interval_s=60.0) as heartbeat:
            rows = generate_rows(spec)
            if not rows:
                raise RuntimeError("trace generation produced no requests")
            if stable_hash(_trace_identity(spec), length=64) != stable_hash(identity, length=64):
                raise RuntimeError("trace inputs changed while the artifact was being generated")
            heartbeat.ensure_held()
            if os.path.lexists(metadata_path):
                # --force verifies determinism without mutating the immutable
                # content-addressed entry referenced by prior run bundles.
                fd, candidate = tempfile.mkstemp(prefix=".trace-force-", dir=artifact_dir)
                os.close(fd)
                os.unlink(candidate)
                try:
                    write_trace(candidate, spec, rows)
                    candidate_digest, candidate_rows = _trace_content_stats(candidate)
                    existing = validate_trace_artifact(artifact, expected_identity=identity)
                    if (
                        candidate_digest != existing["trace_sha256"]
                        or candidate_rows != existing["row_count"]
                    ):
                        raise RuntimeError(
                            "forced trace regeneration was not byte-identical; bump the "
                            "generator version instead of replacing an immutable artifact"
                        )
                finally:
                    try:
                        os.unlink(candidate)
                    except FileNotFoundError:
                        pass
                return artifact

            # A trace without completion metadata is not published state and
            # may be replaced after a prior interrupted generation.
            write_trace(trace_path, spec, rows)
            digest, observed_rows = _trace_content_stats(trace_path)
            heartbeat.ensure_held()
            atomic_create_json(
                metadata_path,
                {
                    "schema_version": 1,
                    "trace_id": trace_id,
                    "trace_identity": identity,
                    "trace_sha256": digest,
                    "row_count": observed_rows,
                },
            )
    validate_trace_artifact(artifact, expected_identity=identity)
    return artifact
