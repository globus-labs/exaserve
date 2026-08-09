#!/usr/bin/env python3
"""Fetch + normalise the paper's EXP SET 2 workload datasets to Lustre.

Produces, under <data-root>/paper_datasets/, one JSONL per dataset where each
line is a candidate request:

    {"prompt": <str>, "output_len": <int>, "input_len": <int|null>, "source": <str>}

`output_len` encodes the per-dataset generation policy agreed for the paper:
  - humaneval        : natural stop, cap 512        (code generation)
  - cnn_dailymail    : cap 256                       (summarisation)
  - sharegpt_natural : the *real* assistant-turn token length (variable)

BurstGPT is handled separately: the raw release CSV is downloaded to
<data-root>/input_traces/ for the trace-replay path (column mapping happens in
the trace generator, not here).

This is NODE-ONLY work: it reads the 670 MB ShareGPT bank into memory and runs
the HF tokenizer, which would get a login node killed. Run it inside a subjob:

    subjob 1
    # on the node:
    source ~/script/env_aurora
    python -m eval.tools.fetch_paper_datasets --sample 200 --datasets humaneval burstgpt
    #   ^ validate small first, then drop --sample / widen --datasets

Network egress uses the proxy that env_aurora exports (HTTPS_PROXY).
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import sys
import urllib.request
from pathlib import Path

from eval.site_config import get_site_config

DEFAULT_DATA_ROOT = get_site_config().user_data_root

HUMANEVAL_URL = "https://github.com/openai/human-eval/raw/master/data/HumanEval.jsonl.gz"
# Cleaned single-region BurstGPT trace (≈1.4M rows); closest analogue to the
# Azure code trace already wired into burstgpt_v1.yaml.
BURSTGPT_URL = (
    "https://github.com/HPMLL/BurstGPT/releases/download/v2.0/BurstGPT_without_fails_1.csv"
)
BURSTGPT_FALLBACK_URL = "https://github.com/HPMLL/BurstGPT/raw/master/data/BurstGPT_1.csv"

# Output-length policy (see module docstring).
HUMANEVAL_OUTPUT_LEN = 512
CNN_OUTPUT_LEN = 256
CNN_PROMPT_PREFIX = "Summarize the following news article:\n\n"


def _log(msg: str) -> None:
    print(f"[fetch_paper_datasets] {msg}", flush=True)


def _download(url: str, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    _log(f"GET {url}")
    req = urllib.request.Request(url, headers={"User-Agent": "exaserve/eval"})
    with urllib.request.urlopen(req) as resp, open(dest, "wb") as out:
        # Stream in chunks; these files are 100s of MB.
        while True:
            chunk = resp.read(1 << 20)
            if not chunk:
                break
            out.write(chunk)
    _log(f"  -> {dest} ({dest.stat().st_size / 1e6:.1f} MB)")


def _write_jsonl(rows: list[dict], dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w", encoding="utf-8") as fh:
        for r in rows:
            fh.write(json.dumps(r) + "\n")
    _log(f"  wrote {len(rows)} rows -> {dest}")


# --------------------------------------------------------------------------- #
# datasets
# --------------------------------------------------------------------------- #


def fetch_humaneval(out_dir: Path, sample: int | None) -> None:
    _log("humaneval: downloading")
    req = urllib.request.Request(HUMANEVAL_URL, headers={"User-Agent": "exaserve/eval"})
    with urllib.request.urlopen(req) as resp:
        raw = resp.read()
    rows = []
    with gzip.open(io.BytesIO(raw), "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            prompt = obj.get("prompt")
            if prompt:
                rows.append(
                    {
                        "prompt": prompt,
                        "output_len": HUMANEVAL_OUTPUT_LEN,
                        "input_len": None,
                        "source": "humaneval",
                    }
                )
    if sample:
        rows = rows[:sample]
    _write_jsonl(rows, out_dir / "humaneval.jsonl")


def fetch_cnn_dailymail(out_dir: Path, sample: int | None) -> None:
    try:
        from datasets import load_dataset
    except ImportError:
        _log(
            "cnn_dailymail: ERROR — `datasets` not importable. "
            "Run under `module load frameworks` / env_aurora, or `pip install datasets`."
        )
        raise
    n = sample or 5000
    _log(f"cnn_dailymail: loading test split (target {n} articles)")
    ds = load_dataset("abisee/cnn_dailymail", "3.0.0", split=f"test[:{n}]")
    rows = []
    for ex in ds:
        article = (ex.get("article") or "").strip()
        if article:
            rows.append(
                {
                    "prompt": CNN_PROMPT_PREFIX + article,
                    "output_len": CNN_OUTPUT_LEN,
                    "input_len": None,
                    "source": "cnn_dailymail",
                }
            )
    _write_jsonl(rows, out_dir / "cnn_dailymail.jsonl")


def fetch_sharegpt_natural(
    out_dir: Path, sample: int | None, sharegpt_path: Path, tokenizer_id: str
) -> None:
    if not sharegpt_path.exists():
        _log(f"sharegpt_natural: ERROR — bank not found at {sharegpt_path}")
        raise FileNotFoundError(sharegpt_path)
    try:
        from transformers import AutoTokenizer
    except ImportError:
        _log("sharegpt_natural: ERROR — `transformers` not importable (need frameworks env).")
        raise
    _log(f"sharegpt_natural: loading tokenizer {tokenizer_id}")
    tok = AutoTokenizer.from_pretrained(tokenizer_id, trust_remote_code=True)
    tok.model_max_length = 100_000_000
    _log(f"sharegpt_natural: reading bank {sharegpt_path} (large; node-only)")
    with open(sharegpt_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    limit = sample or 20000
    rows = []
    for entry in data:
        convs = entry.get("conversations", [])
        human = next((c.get("value") for c in convs if c.get("from") == "human"), None)
        gpt = next((c.get("value") for c in convs if c.get("from") in ("gpt", "assistant")), None)
        if not human or not gpt:
            continue
        in_len = len(tok.encode(human, add_special_tokens=False))
        out_len = len(tok.encode(gpt, add_special_tokens=False))
        if in_len == 0 or out_len == 0:
            continue
        rows.append(
            {
                "prompt": human,
                "output_len": out_len,
                "input_len": in_len,
                "source": "sharegpt_natural",
            }
        )
        if len(rows) >= limit:
            break
    _write_jsonl(rows, out_dir / "sharegpt_natural.jsonl")


def fetch_burstgpt(data_root: Path) -> None:
    dest = data_root / "input_traces" / "BurstGPT_without_fails_1.csv"
    if dest.exists() and dest.stat().st_size > 0:
        _log(f"burstgpt: already present at {dest}, skipping")
        return
    try:
        _download(BURSTGPT_URL, dest)
    except Exception as exc:  # noqa: BLE001 — fall back to the repo /data copy
        _log(f"burstgpt: release URL failed ({exc}); trying repo /data copy")
        _download(BURSTGPT_FALLBACK_URL, dest)


# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--data-root",
        default=DEFAULT_DATA_ROOT,
        help=f"Lustre data root (default: {DEFAULT_DATA_ROOT})",
    )
    p.add_argument(
        "--datasets",
        nargs="*",
        default=["humaneval", "cnn_dailymail", "sharegpt_natural", "burstgpt"],
        choices=["humaneval", "cnn_dailymail", "sharegpt_natural", "burstgpt"],
        help="Subset to fetch (default: all). Use a small subset to validate first.",
    )
    p.add_argument(
        "--sample",
        type=int,
        default=None,
        help="Cap rows per dataset (for a quick validation pass).",
    )
    p.add_argument(
        "--tokenizer",
        default="meta-llama/Meta-Llama-3-8B-Instruct",
        help="Tokenizer for natural ShareGPT output-length measurement.",
    )
    args = p.parse_args(argv)

    data_root = Path(args.data_root)
    out_dir = data_root / "paper_datasets"
    sharegpt_path = (
        data_root / "input_traces" / "ShareGPT_V3_unfiltered_cleaned_split_no_imsorry.json"
    )

    _log(
        f"data_root={data_root}  out_dir={out_dir}  datasets={args.datasets}  sample={args.sample}"
    )
    for name in args.datasets:
        if name == "humaneval":
            fetch_humaneval(out_dir, args.sample)
        elif name == "cnn_dailymail":
            fetch_cnn_dailymail(out_dir, args.sample)
        elif name == "sharegpt_natural":
            fetch_sharegpt_natural(out_dir, args.sample, sharegpt_path, args.tokenizer)
        elif name == "burstgpt":
            fetch_burstgpt(data_root)
    _log("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
