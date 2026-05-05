"""Trace row generation algorithms.

This module contains the actual logic for generating synthetic request traces
(weak_scaling and azure_trace kinds). The caching/dedup layer lives in
trace_store.py.
"""
from __future__ import annotations

import csv
import json
import math
import os
import random
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from aurora_rayserver.model_paths import get_model_storage_path

from .models import ExperimentSpec

TRACE_GENERATOR_VERSION = 2

# Chat templates (e.g. Llama-3 Instruct) prepend/append special tokens around
# the user message.  Reserve this many tokens so input + output + template
# overhead stays within max_model_len.
_CHAT_TEMPLATE_MARGIN: int = 16


def generate_rows(spec: ExperimentSpec) -> list[dict[str, Any]]:
    if spec.trace.kind == "weak_scaling":
        return generate_weak_scaling_rows(spec)
    if spec.trace.kind == "azure_trace":
        return generate_azure_rows(spec)
    raise ValueError(f"Unsupported trace.kind: {spec.trace.kind}")


def write_trace(
    path: str,
    spec: ExperimentSpec,
    rows: list[dict[str, Any]],
) -> None:
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        metadata = {
            "__type__": "metadata",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "trace_kind": spec.trace.kind,
            "generator_version": TRACE_GENERATOR_VERSION,
            "workload": {
                "duration": spec.workload.duration,
                "input_len": spec.workload.input_len,
                "output_len": spec.workload.output_len,
                "rate_per_node": spec.workload.rate_per_node,
                "seed": spec.workload.seed,
            },
            "deployment": {
                "num_nodes": spec.deployment.num_nodes,
                "models": [model.model_id for model in spec.deployment.models],
            },
        }
        handle.write(json.dumps(metadata) + "\n")
        for row in rows:
            handle.write(json.dumps(row) + "\n")


# ---------------------------------------------------------------------------
# Prompt handling
# ---------------------------------------------------------------------------


def load_prompt_bank(path: str) -> list[str]:
    if not path or not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
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


def build_tokenizer_map(spec: ExperimentSpec) -> dict[str, Any]:
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


def generate_weak_scaling_rows(spec: ExperimentSpec) -> list[dict[str, Any]]:
    prompt_bank = load_prompt_bank(spec.trace.input_prompt_path)
    tokenizers = build_tokenizer_map(spec)
    model_ids = [model.model_id for model in spec.deployment.models]
    tp_by_model = {
        model.model_id: model.tensor_parallel_size for model in spec.deployment.models
    }
    max_model_len_by_model = {
        model.model_id: model.max_model_len for model in spec.deployment.models
    }
    total_qps = spec.deployment.num_nodes * spec.workload.rate_per_node
    total_requests = int(total_qps * spec.workload.duration)
    inter_arrival = (1.0 / total_qps) if total_qps > 0 else 0.0

    def build_chunk(start: int, end: int) -> list[dict[str, Any]]:
        rows = []
        for index in range(start, end):
            model_id = model_ids[index % len(model_ids)]
            max_input = max(
                1,
                max_model_len_by_model[model_id]
                - spec.workload.output_len
                - _CHAT_TEMPLATE_MARGIN,
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
                    "timestamp": float(f"{(index + 1) * inter_arrival:.6f}"),
                    "model": model_id,
                    "mode": "chat",
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

    with open(spec.trace.input_trace_path, "r", encoding="utf-8") as handle:
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
    in_col = next((name for name in column_names if "context" in name), None)
    out_col = next((name for name in column_names if "generated" in name), None)
    if ts_col is None:
        raise ValueError(f"Could not find timestamp column in {column_names}")

    parsed_rows = []
    for row in normalized:
        timestamp = _parse_timestamp(row[ts_col])
        parsed_rows.append(
            {
                "timestamp": timestamp,
                "input_tokens": int(float(row.get(in_col, spec.workload.input_len) or spec.workload.input_len)),
                "output_tokens": int(float(row.get(out_col, spec.workload.output_len) or spec.workload.output_len)),
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
            selected_bin = (min(positive, key=lambda item: item[1])[0] if positive else 0)
        else:
            rng = random.Random(spec.workload.seed)
            selected_bin = rng.choice([item[0] for item in rolling])
        window_start = start_time + (selected_bin * 10)

    window_end = window_start + target_duration
    selected = [
        row for row in parsed_rows if window_start <= row["timestamp"] < window_end
    ]

    inv_sizes = [1.0 / max(model.size, 1) for model in spec.deployment.models]
    total_weight = sum(inv_sizes)
    weights = [value / total_weight for value in inv_sizes]
    model_ids = [model.model_id for model in spec.deployment.models]
    tp_by_model = {
        model.model_id: model.tensor_parallel_size for model in spec.deployment.models
    }
    mode_keys = list(spec.workload.modes) or ["chat"]
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
                "timestamp": float(f"{((row['timestamp'] - window_start) / spec.workload.speedup):.4f}"),
                "model": model_id,
                "mode": mode,
                "prompt": prompt_text,
                "input_len": input_len,
                "output_len": output_len,
                "tensor_parallel_size": tp_by_model[model_id],
            }
        )
    return output_rows
