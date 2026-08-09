"""Trace row generation algorithms.

This module contains the actual logic for generating synthetic request traces
(weak_scaling and azure_trace kinds). The caching/dedup layer lives in
trace_store.py.
"""

from __future__ import annotations

import csv
import importlib
import importlib.metadata
import importlib.util
import hashlib
import json
import math
import os
import platform
import random
import stat
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any

from exaserve.model_paths import get_model_storage_path

from .models import ExperimentSpec

TRACE_GENERATOR_VERSION = 5

# Chat templates (e.g. Llama-3 Instruct) prepend/append special tokens around
# the user message.  Reserve this many tokens so input + output + template
# overhead stays within max_model_len.
_CHAT_TEMPLATE_MARGIN: int = 16


def generate_rows(spec: ExperimentSpec) -> list[dict[str, Any]]:
    if spec.trace.kind == "weak_scaling":
        return generate_weak_scaling_rows(spec)
    if spec.trace.kind == "azure_trace":
        return generate_azure_rows(spec)
    if spec.trace.kind == "dataset_replay":
        return generate_dataset_replay_rows(spec)
    raise ValueError(f"Unsupported trace.kind: {spec.trace.kind}")


def write_trace(
    path: str,
    spec: ExperimentSpec,
    rows: list[dict[str, Any]],
) -> None:
    # WP2.6 (PR-017): stream to a same-dir temp file, then publish atomically.
    import tempfile

    dirpath = os.path.dirname(os.path.abspath(path))
    os.makedirs(dirpath, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".trace.", suffix=".tmp", dir=dirpath)
    try:
        _write_trace_stream(os.fdopen(fd, "w", encoding="utf-8"), spec, rows)
        os.replace(tmp_path, path)
        directory_fd = os.open(dirpath, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException as exc:
        try:
            os.unlink(tmp_path)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            from exaserve.exception_notes import add_exception_note

            add_exception_note(exc, f"trace temporary cleanup also failed: {cleanup_exc}")
        raise


def _write_trace_stream(handle, spec: ExperimentSpec, rows: list[dict[str, Any]]) -> None:
    with handle:
        metadata = {
            "__type__": "metadata",
            "trace_kind": spec.trace.kind,
            "generator_version": TRACE_GENERATOR_VERSION,
            "workload": {
                "duration": spec.workload.duration,
                "input_len": spec.workload.input_len,
                "output_len": spec.workload.output_len,
                "rate_per_node": spec.workload.rate_per_node,
                "seed": spec.workload.seed,
                "modes": {name: spec.workload.modes[name] for name in sorted(spec.workload.modes)},
            },
            "deployment": {
                "num_nodes": spec.deployment.num_nodes,
                "models": [model.model_id for model in spec.deployment.models],
            },
        }
        handle.write(json.dumps(metadata, allow_nan=False) + "\n")
        for row in rows:
            handle.write(json.dumps(row, allow_nan=False) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


# ---------------------------------------------------------------------------
# Prompt handling
# ---------------------------------------------------------------------------


def load_prompt_bank(path: str) -> list[str]:
    if not path or not os.path.exists(path):
        return []
    from exaserve.state.atomic import regular_file_reader, strict_json_load

    with regular_file_reader(path) as handle:
        data = strict_json_load(handle)
    prompts = []
    for entry in data:
        conversations = entry.get("conversations", [])
        first_prompt = next(
            (item.get("value") for item in conversations if item.get("from") == "human"),
            None,
        )
        if first_prompt:
            prompts.append(str(first_prompt))
    return prompts


def _model_search_roots(storage_path: str) -> list[Path]:
    """Return candidate root directories where models may live, in priority order."""
    roots: list[Path] = []
    if storage_path:
        roots.append(Path(storage_path))
    home_models = Path.home() / "agpt" / "models"
    if home_models.is_dir():
        roots.append(home_models)
    hf_home = os.environ.get("HF_HOME")
    if hf_home:
        roots.append(Path(hf_home))
    return roots


def _find_local_model_path(model_id: str, search_roots: list[Path]) -> str | None:
    for root in search_roots:
        flat_path = get_model_storage_path(model_id, str(root))
        if flat_path.is_dir():
            return str(flat_path)
        hub_name = "models--" + model_id.replace("/", "--")
        for sub in (root, root / "hub"):
            cache_dir = sub / hub_name / "snapshots"
            if cache_dir.is_dir():
                snapshots = sorted(p for p in cache_dir.iterdir() if p.is_dir())
                if snapshots:
                    return str(snapshots[0])
    return None


def _regular_file_digest(path: Path) -> tuple[str, int]:
    """Hash one input file from the descriptor that was type-checked.

    Tokenizer caches commonly use symlinks into a content-addressed blob store,
    so the input side intentionally follows that final symlink.  ``fstat`` then
    rejects devices/FIFOs/directories and binds the bytes and size to the same
    opened descriptor.  The trace identity is recomputed after generation to
    detect a target that changed during the operation.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        metadata = os.fstat(handle.fileno())
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError(f"tokenizer provenance input is not a regular file: {path}")
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest(), metadata.st_size


def _source_tree_identity(root: Path, *, suffixes: frozenset[str]) -> dict[str, Any]:
    """Return a path-independent digest inventory for a source/config tree."""
    if not root.is_dir():
        raise ValueError(f"provenance root is not a directory: {root}")
    files: list[dict[str, Any]] = []
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        dirnames[:] = sorted(name for name in dirnames if name != "__pycache__")
        directory_path = Path(directory)
        for name in sorted(filenames):
            path = directory_path / name
            if path.suffix.lower() not in suffixes:
                continue
            digest, size = _regular_file_digest(path)
            files.append(
                {
                    "path": path.relative_to(root).as_posix(),
                    "sha256": digest,
                    "size": size,
                }
            )
    if not files:
        raise ValueError(f"provenance root contains no qualifying files: {root}")
    return {"files": files}


def _distribution_versions(names: tuple[str, ...]) -> dict[str, str | None]:
    versions: dict[str, str | None] = {}
    for name in names:
        try:
            versions[name] = importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            versions[name] = None
    return versions


def _builder_source_identity(builder: str) -> dict[str, Any]:
    module_name, _, function_name = builder.partition(":")
    if not module_name or not function_name:
        raise ValueError(f"trace.tokenizer_builder must be 'module:function', got {builder!r}")
    module_spec = importlib.util.find_spec(module_name)
    if (
        module_spec is None
        or not module_spec.origin
        or module_spec.origin in {"built-in", "frozen"}
    ):
        raise ValueError(f"tokenizer builder module has no hashable source: {module_name!r}")
    origin = Path(module_spec.origin).resolve()
    if not origin.is_file():
        raise ValueError(f"tokenizer builder source is not a file: {origin}")

    # Hash the complete top-level Python package rather than only the named
    # module.  A builder commonly delegates to sibling helpers; hashing one file
    # would permit stale reuse after such a helper changed.  Relative paths, not
    # installation paths, enter the identity.
    top_level = module_name.split(".", 1)[0]
    top_spec = importlib.util.find_spec(top_level)
    if top_spec is not None and top_spec.submodule_search_locations:
        roots = sorted(Path(item).resolve() for item in top_spec.submodule_search_locations)
        source = [
            {
                "tree": _source_tree_identity(root, suffixes=frozenset({".py", ".pyi"})),
            }
            for root in roots
        ]
    else:
        digest, size = _regular_file_digest(origin)
        source = [{"file": {"sha256": digest, "size": size}}]
    return {
        "reference": builder,
        "python": platform.python_version(),
        "source": source,
    }


_TOKENIZER_PROVENANCE_SUFFIXES = frozenset(
    {
        ".json",
        ".jinja",
        ".jinja2",
        ".model",
        ".py",
        ".pyi",
        ".tiktoken",
        ".txt",
    }
)


def tokenizer_source_identity(spec: ExperimentSpec) -> dict[str, Any]:
    """Bind trace reuse to the code/files that determine tokenization.

    Model weight blobs are intentionally excluded: they do not affect prompt
    serialization and can be hundreds of gigabytes.  Configuration, vocabulary,
    sentencepiece, template, and remote-code files are included by content.
    Default Hugging Face tokenization must resolve locally; an unpinned network
    model identifier is not a production-safe content identity.
    """
    builder = getattr(spec.trace, "tokenizer_builder", "")
    if builder:
        return {"kind": "custom_builder", **_builder_source_identity(builder)}

    search_roots = _model_search_roots(spec.deployment.model_storage_path)
    models: list[dict[str, Any]] = []
    for model in spec.deployment.models:
        source = _find_local_model_path(model.model_id, search_roots)
        if source is None:
            raise ValueError(
                f"model {model.model_id!r} has no local tokenizer source; stage a pinned "
                "snapshot before materializing a content-addressed trace"
            )
        models.append(
            {
                "model_id": model.model_id,
                "content": _source_tree_identity(
                    Path(source), suffixes=_TOKENIZER_PROVENANCE_SUFFIXES
                ),
            }
        )
    return {
        "kind": "huggingface_local",
        "python": platform.python_version(),
        "libraries": _distribution_versions(("transformers", "tokenizers", "sentencepiece")),
        "models": models,
    }


def build_tokenizer_map(spec: ExperimentSpec) -> dict[str, Any]:
    builder = getattr(spec.trace, "tokenizer_builder", "")
    if builder:
        module_name, _, func_name = builder.partition(":")
        if not module_name or not func_name:
            raise ValueError(f"trace.tokenizer_builder must be 'module:function', got {builder!r}")
        module = importlib.import_module(module_name)
        return getattr(module, func_name)(spec)

    from transformers import AutoTokenizer

    search_roots = _model_search_roots(spec.deployment.model_storage_path)

    tokenizers = {}
    for model in spec.deployment.models:
        source = _find_local_model_path(model.model_id, search_roots) or model.model_id
        tokenizer = AutoTokenizer.from_pretrained(
            source,
            trust_remote_code=True,
        )
        tokenizer.model_max_length = 100_000_000
        tokenizers[model.model_id] = tokenizer
    return tokenizers


def _truncate_prompt(
    text: str,
    target_len: int,
    hard_limit: int,
    model_id: str,
    tokenizers: dict[str, Any],
) -> tuple[str, int]:
    tokenizer = tokenizers[model_id]
    tokens = tokenizer.encode(text, add_special_tokens=False)
    if len(tokens) > hard_limit:
        tokens = tokens[:hard_limit]
    elif len(tokens) < target_len:
        while len(tokens) < target_len and tokens:
            tokens = (tokens + tokens)[:target_len]
    prompt = tokenizer.decode(tokens, skip_special_tokens=True)
    # Re-encode to verify: decode→encode round-trip can change token count.
    # Iteratively trim until the re-encoded count fits within hard_limit.
    verified_tokens = tokenizer.encode(prompt, add_special_tokens=False)
    while len(verified_tokens) > hard_limit:
        verified_tokens = verified_tokens[:hard_limit]
        prompt = tokenizer.decode(verified_tokens, skip_special_tokens=True)
        verified_tokens = tokenizer.encode(prompt, add_special_tokens=False)
    return prompt, len(verified_tokens)


def _synthetic_prompt(seed: int, target_len: int) -> str:
    rng = random.Random(seed)
    vocab = ["scientific", "distributed", "system", "latency", "gpu", "kernel", "queue"]
    return " ".join(rng.choice(vocab) for _ in range(max(1, target_len)))


def _choose_prompt(prompt_bank: list[str], seed: int, target_len: int) -> str:
    if prompt_bank:
        rng = random.Random(seed)
        return str(rng.choice(prompt_bank))
    return _synthetic_prompt(seed, target_len)


# ---------------------------------------------------------------------------
# Weak scaling trace generation
# ---------------------------------------------------------------------------


def _arrival_times(spec: ExperimentSpec, total_qps: float, total_requests: int) -> list[float]:
    """Per-request arrival offsets (seconds from t0).

    arrival="fixed"   -> evenly spaced at 1/total_qps (constant rate).
    arrival="poisson" -> exponential inter-arrivals, mean 1/total_qps, seeded.
    """
    if total_qps <= 0 or total_requests <= 0:
        return [0.0] * max(0, total_requests)
    if getattr(spec.workload, "arrival", "fixed") == "poisson":
        rng = random.Random(spec.workload.seed)
        times, t = [], 0.0
        for _ in range(total_requests):
            t += rng.expovariate(total_qps)
            times.append(t)
        return times
    inter = 1.0 / total_qps
    return [(i + 1) * inter for i in range(total_requests)]


def _request_mode(spec: ExperimentSpec, index: int) -> str:
    """Select a deterministic protocol mode from the typed workload weights."""
    weighted = [
        (name, weight) for name, weight in sorted(spec.workload.modes.items()) if weight > 0
    ]
    if not weighted:
        raise ValueError("workload.modes has no positive request protocol weight")
    total = sum(weight for _, weight in weighted)
    slot = random.Random(spec.workload.seed + index).randrange(total)
    for name, weight in weighted:
        if slot < weight:
            return name
        slot -= weight
    raise AssertionError("request-mode selection fell outside validated weights")


def generate_weak_scaling_rows(spec: ExperimentSpec) -> list[dict[str, Any]]:
    prompt_bank = load_prompt_bank(spec.trace.input_prompt_path)
    tokenizers = build_tokenizer_map(spec)
    model_ids = [model.model_id for model in spec.deployment.models]
    tp_by_model = {model.model_id: model.tensor_parallel_size for model in spec.deployment.models}
    max_model_len_by_model = {
        model.model_id: model.max_model_len for model in spec.deployment.models
    }
    total_qps = spec.deployment.num_nodes * spec.workload.rate_per_node
    total_requests = int(total_qps * spec.workload.duration)
    arrival_times = _arrival_times(spec, total_qps, total_requests)

    def build_chunk(start: int, end: int) -> list[dict[str, Any]]:
        rows = []
        for index in range(start, end):
            model_id = model_ids[index % len(model_ids)]
            max_input = max(
                1,
                max_model_len_by_model[model_id] - spec.workload.output_len - _CHAT_TEMPLATE_MARGIN,
            )
            capped_input_len = min(spec.workload.input_len, max_input)
            prompt_seed = spec.workload.seed + index
            prompt_text = _choose_prompt(
                prompt_bank,
                prompt_seed,
                capped_input_len,
            )
            prompt_text, input_len = _truncate_prompt(
                prompt_text,
                capped_input_len,
                capped_input_len,
                model_id,
                tokenizers,
            )
            rows.append(
                {
                    "timestamp": round(arrival_times[index], 6),
                    "model": model_id,
                    "mode": _request_mode(spec, index),
                    "prompt": prompt_text,
                    "input_len": input_len,
                    "output_len": spec.workload.output_len,
                    "tensor_parallel_size": tp_by_model[model_id],
                }
            )
        return rows

    if total_requests <= 0:
        return []

    max_workers = min(os.cpu_count() or 4, 16)
    chunk_size = max(1, math.ceil(total_requests / max_workers))
    chunk_ranges = [
        (start, min(total_requests, start + chunk_size))
        for start in range(0, total_requests, chunk_size)
    ]

    rows = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for chunk_rows in executor.map(lambda item: build_chunk(*item), chunk_ranges):
            rows.extend(chunk_rows)
    return rows


# ---------------------------------------------------------------------------
# Azure trace generation
# ---------------------------------------------------------------------------


def _parse_timestamp(raw_value: str) -> float:
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        pass

    text = str(raw_value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError as exc:
        raise ValueError(f"Could not parse timestamp value {raw_value!r}") from exc


def generate_azure_rows(spec: ExperimentSpec) -> list[dict[str, Any]]:
    if not spec.trace.input_trace_path:
        raise ValueError("trace.input_trace_path is required for azure_trace")

    prompt_bank = load_prompt_bank(spec.trace.input_prompt_path)
    tokenizers = build_tokenizer_map(spec)

    from exaserve.state.atomic import regular_file_reader

    with regular_file_reader(spec.trace.input_trace_path) as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    if not rows:
        return []

    normalized = []
    for row in rows:
        lowered = {str(key).strip().lower(): value for key, value in row.items()}
        normalized.append(lowered)

    column_names = list(normalized[0])
    ts_col = next((name for name in column_names if "timestamp" in name), None)
    in_col = next((name for name in column_names if "context" in name or "request" in name), None)
    out_col = next(
        (name for name in column_names if "generated" in name or "response" in name), None
    )
    if ts_col is None:
        raise ValueError(f"Could not find timestamp column in {column_names}")

    parsed_rows = []
    for row in normalized:
        timestamp = _parse_timestamp(row[ts_col])
        parsed_rows.append(
            {
                "timestamp": timestamp,
                "input_tokens": int(
                    float(row.get(in_col, spec.workload.input_len) or spec.workload.input_len)
                ),
                "output_tokens": int(
                    float(row.get(out_col, spec.workload.output_len) or spec.workload.output_len)
                ),
            }
        )
    parsed_rows.sort(key=lambda item: item["timestamp"])

    start_time = parsed_rows[0]["timestamp"]
    end_time = parsed_rows[-1]["timestamp"]
    target_duration = spec.workload.duration
    if end_time - start_time <= target_duration:
        window_start = start_time
    else:
        bins = defaultdict(int)
        for row in parsed_rows:
            bin_key = int((row["timestamp"] - start_time) // 10)
            bins[bin_key] += 1
        window_bins = max(1, int(target_duration / 10))
        max_bin = int((end_time - start_time) // 10)
        rolling = []
        for bin_index in range(max_bin + 1):
            window_total = 0
            for offset in range(window_bins):
                window_total += bins.get(bin_index + offset, 0)
            rolling.append((bin_index, window_total))
        strategy = spec.workload.sampling_strategy
        if strategy == "peak":
            selected_bin = max(rolling, key=lambda item: item[1])[0]
        elif strategy == "sparse":
            positive = [item for item in rolling if item[1] > 0]
            selected_bin = min(positive, key=lambda item: item[1])[0] if positive else 0
        else:
            rng = random.Random(spec.workload.seed)
            selected_bin = rng.choice([item[0] for item in rolling])
        window_start = start_time + (selected_bin * 10)

    window_end = window_start + target_duration
    selected = [row for row in parsed_rows if window_start <= row["timestamp"] < window_end]

    inv_sizes = [1.0 / max(model.size, 1) for model in spec.deployment.models]
    total_weight = sum(inv_sizes)
    weights = [value / total_weight for value in inv_sizes]
    model_ids = [model.model_id for model in spec.deployment.models]
    tp_by_model = {model.model_id: model.tensor_parallel_size for model in spec.deployment.models}
    mode_keys = sorted(spec.workload.modes) or ["chat"]
    mode_weights = [spec.workload.modes[key] for key in mode_keys] or [1]

    rng = random.Random(spec.workload.seed)
    output_rows = []
    for index, row in enumerate(selected):
        model_id = rng.choices(model_ids, weights=weights, k=1)[0]
        output_len = max(1, row["output_tokens"])
        available_input = max(
            spec.deployment.models[model_ids.index(model_id)].max_model_len
            - output_len
            - _CHAT_TEMPLATE_MARGIN,
            1,
        )
        final_input_len = min(max(1, row["input_tokens"]), available_input)
        prompt_text = _choose_prompt(prompt_bank, spec.workload.seed + index, final_input_len)
        prompt_text, input_len = _truncate_prompt(
            prompt_text,
            final_input_len,
            final_input_len,
            model_id,
            tokenizers,
        )
        mode = rng.choices(mode_keys, weights=mode_weights, k=1)[0]
        output_rows.append(
            {
                "timestamp": float(
                    f"{((row['timestamp'] - window_start) / spec.workload.speedup):.4f}"
                ),
                "model": model_id,
                "mode": mode,
                "prompt": prompt_text,
                "input_len": input_len,
                "output_len": output_len,
                "tensor_parallel_size": tp_by_model[model_id],
            }
        )
    return output_rows


# ---------------------------------------------------------------------------
# Dataset replay (HumanEval / CNN-DailyMail / natural ShareGPT)
# ---------------------------------------------------------------------------


def generate_dataset_replay_rows(spec: ExperimentSpec) -> list[dict[str, Any]]:
    """Replay a normalized dataset JSONL (eval/tools/fetch_paper_datasets.py).

    Each line: {"prompt": str, "output_len": int, "input_len": int|null, ...}.
    Prompts are sent verbatim (the serving tokenizer truncates against
    max_model_len); per-row output_len is honoured (variable for natural
    ShareGPT) and capped so prompt+output fit the model. Arrival follows
    workload.arrival (fixed|poisson) at rate_per_node, like weak_scaling.
    """
    path = spec.trace.input_prompt_path
    if not path or not os.path.exists(path):
        raise ValueError(
            f"dataset_replay needs trace.input_prompt_path to point at a dataset "
            f"JSONL (got {path!r}); see eval/tools/fetch_paper_datasets.py"
        )
    from exaserve.state.atomic import regular_file_reader, strict_json_loads

    records: list[dict[str, Any]] = []
    with regular_file_reader(path) as handle:
        for line in handle:
            line = line.strip()
            if line:
                records.append(strict_json_loads(line))
    if not records:
        return []

    model_ids = [model.model_id for model in spec.deployment.models]
    tp_by_model = {m.model_id: m.tensor_parallel_size for m in spec.deployment.models}
    max_len_by_model = {m.model_id: m.max_model_len for m in spec.deployment.models}

    total_qps = spec.deployment.num_nodes * spec.workload.rate_per_node
    total_requests = int(total_qps * spec.workload.duration)
    if total_requests <= 0:
        return []
    arrival_times = _arrival_times(spec, total_qps, total_requests)

    rows = []
    for index in range(total_requests):
        rec = records[index % len(records)]
        model_id = model_ids[index % len(model_ids)]
        prompt_text = str(rec.get("prompt") or "")
        out_len = int(rec.get("output_len") or spec.workload.output_len)
        out_cap = max(1, max_len_by_model[model_id] - _CHAT_TEMPLATE_MARGIN - 1)
        out_len = max(1, min(out_len, out_cap))
        in_len = rec.get("input_len")
        if in_len is None:
            in_len = len(prompt_text.split())  # rough; server tokenizes for real
        rows.append(
            {
                "timestamp": round(arrival_times[index], 6),
                "model": model_id,
                "mode": _request_mode(spec, index),
                "prompt": prompt_text,
                "input_len": int(in_len),
                "output_len": out_len,
                "tensor_parallel_size": tp_by_model[model_id],
            }
        )
    return rows
