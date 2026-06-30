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

import json
import os
import re
from pathlib import Path

_LAYER_RE = re.compile(r"\.layers\.(\d+)\.")


def pp_partitions(num_layers: int, pp_size: int, partition_env: str | None = None) -> list[int]:
    """Per-stage layer counts, matching vLLM get_pp_indices: even split, with any
    remainder added to all-but-the-last stage. VLLM_PP_LAYER_PARTITION overrides."""
    s = partition_env if partition_env is not None else os.environ.get("VLLM_PP_LAYER_PARTITION")
    if s:
        parts = [int(x) for x in s.split(",")]
        if len(parts) != pp_size or sum(parts) != num_layers:
            raise ValueError(
                f"VLLM_PP_LAYER_PARTITION={parts} inconsistent with "
                f"pp_size={pp_size}, num_layers={num_layers}")
        return parts
    per = num_layers // pp_size
    parts = [per] * pp_size
    for i in range(2, (num_layers % pp_size) + 2):   # remainder -> all but last
        parts[-i] += 1
    return parts


def stage_of_layer(layer: int, parts: list[int]) -> int:
    acc = 0
    for s, p in enumerate(parts):
        acc += p
        if layer < acc:
            return s
    return len(parts) - 1


def weights_for_stage(weight_map: dict, num_layers: int, pp_size: int, stage: int,
                      *, tie_word_embeddings: bool = False,
                      partition_env: str | None = None) -> set[str]:
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
            if stage == 0:                            # safe default
                keep.add(name)
    if unknown:
        print(f"[shard_prune] WARNING: non-layer weights with no PP rule "
              f"(assigned to stage 0): {sorted(unknown)[:8]}{'...' if len(unknown) > 8 else ''}")
    return keep


def _index_path(model_dir: Path) -> Path:
    cands = list(model_dir.glob("*.safetensors.index.json"))
    if not cands:
        raise FileNotFoundError(f"no *.safetensors.index.json in {model_dir}")
    return cands[0]


def plan_stage_shards(model_dir, pp_size, stage, partition_env=None):
    """Return (shards, kept_weights, total_bytes) for a stage WITHOUT writing
    anything — used for offline validation."""
    model_dir = Path(model_dir)
    cfg = json.loads((model_dir / "config.json").read_text())
    num_layers = cfg["num_hidden_layers"]
    tied = bool(cfg.get("tie_word_embeddings", False))
    index = json.loads(_index_path(model_dir).read_text())
    wm = index["weight_map"]
    keep = weights_for_stage(wm, num_layers, pp_size, stage,
                             tie_word_embeddings=tied, partition_env=partition_env)
    shards = sorted({wm[w] for w in keep})
    total = 0
    for sh in shards:
        try:
            total += (model_dir / sh).stat().st_size
        except OSError:
            pass
    return shards, keep, total


def build_stage_dir(model_dir, pp_size, stage, out_dir, partition_env=None) -> dict:
    """Materialize the pruned stage dir: symlink the stage's shards + all small
    shared files (config/tokenizer/...), and write a pruned index. Returns a
    summary dict."""
    model_dir = Path(model_dir).resolve()
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    index_path = _index_path(model_dir)
    index = json.loads(index_path.read_text())
    wm = index["weight_map"]
    shards, keep, total = plan_stage_shards(model_dir, pp_size, stage, partition_env)

    # symlink the stage's shard files
    for sh in shards:
        link = out / sh
        if link.exists() or link.is_symlink():
            link.unlink()
        link.symlink_to(model_dir / sh)
    # pruned index (only this stage's weights/shards)
    pruned = {"metadata": index.get("metadata", {}),
              "weight_map": {w: wm[w] for w in keep}}
    (out / index_path.name).write_text(json.dumps(pruned))
    # small shared files: everything that is not a shard and not the index
    for f in model_dir.iterdir():
        if f.suffix == ".safetensors" or f.name.endswith(".index.json"):
            continue
        link = out / f.name
        if link.exists() or link.is_symlink():
            link.unlink()
        link.symlink_to(f)
    return {"stage": stage, "pp_size": pp_size, "out": str(out),
            "n_shards": len(shards), "n_weights": len(keep),
            "bytes": total, "shards": shards}


def main(argv=None):
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("model_dir")
    ap.add_argument("--pp", type=int, required=True)
    ap.add_argument("--out-base", default=None,
                    help="If set, write pruned dirs to <out-base>/stage{n}. "
                         "Otherwise dry-run (plan only).")
    ap.add_argument("--partition", default=None, help="VLLM_PP_LAYER_PARTITION override")
    args = ap.parse_args(argv)
    gib = lambda b: b / (1024 ** 3)
    for stage in range(args.pp):
        if args.out_base:
            s = build_stage_dir(args.model_dir, args.pp, stage,
                                Path(args.out_base) / f"stage{stage}", args.partition)
        else:
            shards, keep, total = plan_stage_shards(args.model_dir, args.pp, stage, args.partition)
            s = {"stage": stage, "n_shards": len(shards), "n_weights": len(keep), "bytes": total}
        print(f"  stage {s['stage']}: {s['n_shards']} shards, {s['n_weights']} weights, "
              f"{gib(s['bytes']):.1f} GiB" + (f"  -> {s['out']}" if args.out_base else ""))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
