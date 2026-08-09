"""Shard-aware pipeline-parallel (PP) staging.

Build a per-PP-stage *pruned view* of a HuggingFace safetensors model dir: a
symlink farm containing only the shard files that the given pipeline stage's
workers load, plus a pruned `*.safetensors.index.json` and the small shared
files (config, tokenizer, ...). Each node then stages only ~model_size / PP of
weights instead of the whole model, so a model too big for node-local tmpfs
(e.g. Llama-3.1-405B = 756 GB > ~504 GB tmpfs) fits node-local, and only the PP
"seed" nodes read the shared store (no Lustre read storm at scale).

How it plugs into bcast: tools/bcast.c tars its source with `-h` (follows
symlinks) and broadcasts to MPI_COMM_WORLD. So staging is two bcast calls on
DISJOINT node subsets — stage-0 nodes get the stage-0 pruned dir, stage-1 nodes
the stage-1 dir — with no change to bcast.c. The pruned dirs are symlink farms,
so rank 0's `tar -h` dereferences them and streams the real shard bytes.

CRITICAL: the PP layer partition here MUST match vLLM's get_pp_indices
(vllm/distributed/utils.py), or a node would stage shards the worker doesn't load
(and miss ones it does). Mirrored below, including VLLM_PP_LAYER_PARTITION.
"""

from __future__ import annotations

import os
import re
from pathlib import Path

from .model_staging import COMPLETION_MARKER, MODEL_WEIGHT_SUFFIXES
from .state.atomic import atomic_write_json, strict_json_load_path

_LAYER_RE = re.compile(r"\.layers\.(\d+)\.")
_IGNORED_METADATA_DIRS = frozenset({".cache", ".git", "__pycache__"})


def pp_partitions(num_layers: int, pp_size: int, partition_env: str | None = None) -> list[int]:
    """Per-stage layer counts, matching vLLM get_pp_indices: even split, with any
    remainder added to all-but-the-last stage. VLLM_PP_LAYER_PARTITION overrides."""
    if type(num_layers) is not int or num_layers < 1:
        raise ValueError("num_layers must be a positive integer")
    if type(pp_size) is not int or not 1 <= pp_size <= num_layers:
        raise ValueError("pp_size must be a positive integer no larger than num_layers")
    s = partition_env if partition_env is not None else os.environ.get("VLLM_PP_LAYER_PARTITION")
    if s:
        try:
            parts = [int(x) for x in s.split(",")]
        except ValueError as exc:
            raise ValueError("VLLM_PP_LAYER_PARTITION must contain integers") from exc
        if len(parts) != pp_size or sum(parts) != num_layers or any(part < 1 for part in parts):
            raise ValueError(
                f"VLLM_PP_LAYER_PARTITION={parts} inconsistent with "
                f"pp_size={pp_size}, num_layers={num_layers}"
            )
        return parts
    per = num_layers // pp_size
    parts = [per] * pp_size
    for i in range(2, (num_layers % pp_size) + 2):  # remainder -> all but last
        parts[-i] += 1
    return parts


def stage_of_layer(layer: int, parts: list[int]) -> int:
    acc = 0
    for s, p in enumerate(parts):
        acc += p
        if layer < acc:
            return s
    return len(parts) - 1


def weights_for_stage(
    weight_map: dict,
    num_layers: int,
    pp_size: int,
    stage: int,
    *,
    tie_word_embeddings: bool = False,
    partition_env: str | None = None,
) -> set[str]:
    """Weight names that PP `stage` instantiates: its transformer layers, plus
    embed_tokens on stage 0 and the final norm + lm_head on the last stage. If
    word embeddings are tied (no separate lm_head weight), the last stage also
    needs embed_tokens. Unknown non-layer globals default to stage 0 (with a
    flag in the return-side check)."""
    parts = pp_partitions(num_layers, pp_size, partition_env)
    last = pp_size - 1
    keep: set[str] = set()
    unknown: set[str] = set()
    for name in weight_map:
        m = _LAYER_RE.search(name)
        if m:
            if stage_of_layer(int(m.group(1)), parts) == stage:
                keep.add(name)
            continue
        if "embed_tokens" in name:
            if stage == 0 or (tie_word_embeddings and stage == last):
                keep.add(name)
        elif "lm_head" in name or name.startswith("model.norm") or name.startswith("norm."):
            if stage == last:
                keep.add(name)
        else:
            unknown.add(name)
            if stage == 0:  # safe default
                keep.add(name)
    if unknown:
        print(
            f"[shard_prune] WARNING: non-layer weights with no PP rule "
            f"(assigned to stage 0): {sorted(unknown)[:8]}{'...' if len(unknown) > 8 else ''}"
        )
    return keep


def _index_path(model_dir: Path) -> Path:
    cands = sorted(model_dir.glob("*.safetensors.index.json"))
    if not cands:
        raise FileNotFoundError(f"no *.safetensors.index.json in {model_dir}")
    if len(cands) != 1:
        raise ValueError(
            f"expected exactly one *.safetensors.index.json in {model_dir}, found {len(cands)}"
        )
    return cands[0]


def _stage_inputs(model_dir: Path, pp_size: int, stage: int, partition_env: str | None):
    if type(pp_size) is not int or pp_size < 1:
        raise ValueError("pp_size must be a positive integer")
    if type(stage) is not int or not 0 <= stage < pp_size:
        raise ValueError("stage must be an integer in [0, pp_size)")
    cfg = strict_json_load_path(model_dir / "config.json")
    if not isinstance(cfg, dict):
        raise ValueError("model config.json must be an object")
    num_layers = cfg.get("num_hidden_layers")
    if type(num_layers) is not int or num_layers < 1:
        raise ValueError("model config num_hidden_layers must be a positive integer")
    tied = cfg.get("tie_word_embeddings", False)
    if not isinstance(tied, bool):
        raise ValueError("model config tie_word_embeddings must be boolean")
    index_path = _index_path(model_dir)
    index = strict_json_load_path(index_path)
    if (
        not isinstance(index, dict)
        or not set(index) <= {"metadata", "weight_map"}
        or not isinstance(index.get("weight_map"), dict)
    ):
        raise ValueError("safetensors index has an invalid shape")
    weight_map = index["weight_map"]
    if not weight_map or any(
        not isinstance(weight, str) or not weight or not isinstance(shard, str) or not shard
        for weight, shard in weight_map.items()
    ):
        raise ValueError("safetensors index weight_map must map nonempty strings")
    for shard in set(weight_map.values()):
        relative = Path(shard)
        if relative.is_absolute() or ".." in relative.parts or "\x00" in shard:
            raise ValueError(f"safetensors index contains an unsafe shard path: {shard!r}")
        try:
            resolved = (model_dir / relative).resolve(strict=True)
        except OSError as exc:
            raise ValueError(f"safetensors shard is missing: {shard!r}: {exc}") from exc
        if not resolved.is_file():
            raise ValueError(f"safetensors shard is not a regular file: {shard!r}")
    # Validate a configured manual partition even when the caller only needs
    # the inventory; this prevents a staged bundle from diverging from vLLM.
    pp_partitions(num_layers, pp_size, partition_env)
    return index_path, index, weight_map, num_layers, tied


def plan_stage_shards(model_dir, pp_size, stage, partition_env=None):
    """Return (shards, kept_weights, total_bytes) for a stage WITHOUT writing
    anything — used for offline validation."""
    model_dir = Path(model_dir)
    _index, _payload, wm, num_layers, tied = _stage_inputs(model_dir, pp_size, stage, partition_env)
    keep = weights_for_stage(
        wm, num_layers, pp_size, stage, tie_word_embeddings=tied, partition_env=partition_env
    )
    shards = sorted({wm[w] for w in keep})
    total = sum((model_dir / shard).stat().st_size for shard in shards)
    return shards, keep, total


def build_stage_dir(model_dir, pp_size, stage, out_dir, partition_env=None) -> dict:
    """Materialize one exact, cache-free PP-stage broadcast inventory.

    Files are linked individually into real directories.  In particular, a
    directory symlink is never emitted: ``bcast`` archives with ``tar -h``, so
    such a link would make the transmitted inventory larger than the manifest
    written over the stage tree.  Runtime caches, source-control metadata,
    stale completion markers, non-selected weights, and source indices are not
    part of a deployable model bundle.
    """
    model_dir = Path(model_dir).resolve()
    requested_out = Path(out_dir)
    if requested_out.is_symlink():
        raise ValueError("PP stage output must not be a symlink")
    out = requested_out.resolve()
    if out == model_dir or out.is_relative_to(model_dir):
        raise ValueError("PP stage output must be outside the source model directory")
    if out.exists():
        if not out.is_dir():
            raise FileExistsError(f"PP stage output is not a directory: {out}")
        if any(out.iterdir()):
            raise FileExistsError(f"PP stage output is not empty: {out}")
    else:
        out.mkdir(parents=True)
    index_path, index, wm, num_layers, tied = _stage_inputs(
        model_dir, pp_size, stage, partition_env
    )
    keep = weights_for_stage(
        wm, num_layers, pp_size, stage, tie_word_embeddings=tied, partition_env=partition_env
    )
    shards = sorted({wm[weight] for weight in keep})
    total = sum((model_dir / shard).stat().st_size for shard in shards)

    # symlink the stage's shard files
    for sh in shards:
        link = out / sh
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(model_dir / sh)
    # pruned index (only this stage's weights/shards)
    pruned = {"metadata": index.get("metadata", {}), "weight_map": {w: wm[w] for w in keep}}
    atomic_write_json(out / index_path.name, pruned)
    # Shared runtime metadata.  Link files, not directory roots, so manifest
    # traversal and tar -h dereferencing describe exactly the same inventory.
    selected_shards = set(shards)
    for source in sorted(model_dir.rglob("*"), key=lambda path: path.as_posix()):
        relative = source.relative_to(model_dir)
        if any(part in _IGNORED_METADATA_DIRS for part in relative.parts):
            continue
        if not source.is_file():
            continue
        relative_name = relative.as_posix()
        if (
            relative_name in selected_shards
            or source.name == COMPLETION_MARKER
            or source.name.endswith(MODEL_WEIGHT_SUFFIXES)
            or source.name.endswith(".index.json")
        ):
            continue
        link = out / relative
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(source)
    return {
        "stage": stage,
        "pp_size": pp_size,
        "out": str(out),
        "n_shards": len(shards),
        "n_weights": len(keep),
        "bytes": total,
        "shards": shards,
    }


def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("model_dir")
    ap.add_argument("--pp", type=int, required=True)
    ap.add_argument(
        "--out-base",
        default=None,
        help="If set, write pruned dirs to <out-base>/stage{n}. Otherwise dry-run (plan only).",
    )
    ap.add_argument("--partition", default=None, help="VLLM_PP_LAYER_PARTITION override")
    args = ap.parse_args(argv)

    def gib(byte_count):
        return byte_count / (1024**3)

    for stage in range(args.pp):
        if args.out_base:
            s = build_stage_dir(
                args.model_dir,
                args.pp,
                stage,
                Path(args.out_base) / f"stage{stage}",
                args.partition,
            )
        else:
            shards, keep, total = plan_stage_shards(args.model_dir, args.pp, stage, args.partition)
            s = {"stage": stage, "n_shards": len(shards), "n_weights": len(keep), "bytes": total}
        print(
            f"  stage {s['stage']}: {s['n_shards']} shards, {s['n_weights']} weights, "
            f"{gib(s['bytes']):.1f} GiB" + (f"  -> {s['out']}" if args.out_base else "")
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
