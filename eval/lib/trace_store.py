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
from typing import Any

from eval.site_config import get_site_config

from .models import ExperimentSpec, TraceArtifact, VariantSpec
from .trace_generators import TRACE_GENERATOR_VERSION, generate_rows, write_trace
from .utils import dump_json_file, ensure_dir, stable_hash


def trace_store_root(root: str | None = None) -> str:
    base_root = root or os.path.join(get_site_config().experiments_root, "traces")
    return ensure_dir(base_root)


def _trace_identity(spec: ExperimentSpec) -> dict[str, Any]:
    return {
        "version": TRACE_GENERATOR_VERSION,
        "trace": {
            "kind": spec.trace.kind,
            "input_prompt_path": spec.trace.input_prompt_path,
            "input_trace_path": spec.trace.input_trace_path,
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
        },
        "deployment": {
            "num_nodes": spec.deployment.num_nodes,
            "model_storage_path": spec.deployment.model_storage_path,
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


def materialize_trace_artifact(
    variant: VariantSpec,
    *,
    store_root: str | None = None,
    force: bool = False,
) -> TraceArtifact:
    spec = variant.spec
    trace_id = stable_hash(_trace_identity(spec), length=16)
    artifact_dir = ensure_dir(os.path.join(trace_store_root(store_root), trace_id))
    trace_path = os.path.join(artifact_dir, "trace.jsonl")
    metadata_path = os.path.join(artifact_dir, "metadata.json")
    if force or not os.path.exists(trace_path):
        rows = generate_rows(spec)
        write_trace(trace_path, spec, rows)
        dump_json_file(
            metadata_path,
            {
                "trace_id": trace_id,
                "variant_name": variant.variant_name,
                "trace_identity": _trace_identity(spec),
                "row_count": len(rows),
            },
        )
    return TraceArtifact(
        trace_id=trace_id,
        trace_path=trace_path,
        metadata_path=metadata_path,
        spec_hash=stable_hash(_trace_identity(spec), length=16),
        store_dir=artifact_dir,
    )
