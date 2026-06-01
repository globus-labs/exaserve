#!/usr/bin/env python3
"""Goodput / SLO-attainment post-processor.

Given a run group dir (the same layout consumed by weakscaling.py — i.e.
`<run_group>/<N>_nodes/results/result*.json`), compute goodput per SLO preset:

  goodput = RPS x P(meets SLO)

where the SLO is one or more of (TTFT_SLO, TPOT_SLO, E2E_SLO). TTFT/TPOT are
only available when the run was executed with client.stream=true (the Go
client populates `ttft_s` on per-request records).

Outputs:
  - A summary table (stdout): per node count and per SLO preset.
  - Optional plot: --plot saves a goodput-vs-num_nodes line plot.
  - Optional CSV: --csv writes a long-format table for further analysis.

Usage:
  python -m eval.plot.goodput -e weakscaling_haproxy_short_v3 -b ray
  python -m eval.plot.goodput -e <experiment> --preset mlperf_8b interactive
  python -m eval.plot.goodput --run-group <path>
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

# Match the env-flag pattern used by weakscaling.py for thread caps.
os.environ.setdefault("OPENBLAS_NUM_THREADS", "1")
os.environ.setdefault("MKL_NUM_THREADS", "1")
os.environ.setdefault("OMP_NUM_THREADS", "1")

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from eval.lib.catalog import find_spec_path  # noqa: E402
from eval.lib.run_planner import resolve_run_group_dir  # noqa: E402


# ----------------------------- SLO presets --------------------------------

@dataclass(frozen=True)
class SLOPreset:
    name: str
    ttft_s: Optional[float]
    tpot_s: Optional[float]
    e2e_s: Optional[float]
    description: str
    # P99 time-between-tokens bound (seconds). When set, the request must carry
    # a per-request `tbt_p99_s` field (Go client streaming path) and that value
    # must satisfy the bound. This is the paper's decode-phase metric and is
    # preferred over the mean-TPOT approximation in `tpot_s`.
    tbt_p99_s: Optional[float] = None


SLO_PRESETS: dict[str, SLOPreset] = {
    "interactive": SLOPreset(
        name="interactive",
        ttft_s=0.5, tpot_s=0.050, e2e_s=None,
        description="Chatbot: TTFT<=500ms, TPOT<=50ms/tok",
    ),
    "mlperf_8b": SLOPreset(
        name="mlperf_8b",
        ttft_s=2.0, tpot_s=0.100, e2e_s=None,
        description="MLPerf Inference 5.1 Llama-3.1-8B: TTFT<=2s, TPOT<=100ms/tok",
    ),
    "code_assist": SLOPreset(
        name="code_assist",
        ttft_s=0.2, tpot_s=0.030, e2e_s=None,
        description="Code assistant: TTFT<=200ms, TPOT<=30ms/tok",
    ),
    "e2e_2s": SLOPreset(
        name="e2e_2s",
        ttft_s=None, tpot_s=None, e2e_s=2.0,
        description="End-to-end<=2s (compatible with current non-streaming weakscaling)",
    ),
    "paper": SLOPreset(
        name="paper",
        ttft_s=1.0, tpot_s=None, e2e_s=None, tbt_p99_s=0.250,
        description="Paper SLO: TTFT<=1s, P99 TBT<=250ms (Sarathi-Serve style; "
                    "needs client.stream=true + per-request tbt_p99_s)",
    ),
}


# ----------------------------- core compute --------------------------------

def _extract_node_count(name: str) -> int:
    # Matches: "16_nodes", "16-nodes", "n16", "n16-rps110", "n16_in4096"
    m = re.match(r"n?(\d+)(?:[_-]|nodes?|$)", name)
    return int(m.group(1)) if m else 0


def _request_meets_slo(req: dict, preset: SLOPreset) -> bool:
    """Return True iff a per-request record satisfies the SLO preset.

    Skips records that errored. For SLO components not present in the record,
    that component is treated as 'pass' so a preset can be evaluated even when
    e.g. TTFT data is missing — but the script will warn when this happens.
    """
    if not req.get("success", True):
        return False
    if preset.e2e_s is not None:
        lat = req.get("latency")
        if lat is None or lat > preset.e2e_s:
            return False
    if preset.ttft_s is not None:
        ttft = req.get("ttft_s")
        if ttft is not None and ttft > preset.ttft_s:
            return False
    if preset.tpot_s is not None:
        ttft = req.get("ttft_s")
        lat = req.get("latency")
        out_tok = req.get("actual_completion_tokens") or req.get("output_len")
        if ttft is not None and lat is not None and out_tok and out_tok > 1:
            tpot = (lat - ttft) / (out_tok - 1)
            if tpot > preset.tpot_s:
                return False
    if preset.tbt_p99_s is not None:
        tbt = req.get("tbt_p99_s")
        if tbt is not None and tbt > preset.tbt_p99_s:
            return False
    return True


def _has_ttft(reqs: list[dict]) -> bool:
    return any(r.get("ttft_s") is not None for r in reqs[:200])


def _has_tbt(reqs: list[dict]) -> bool:
    return any(r.get("tbt_p99_s") is not None for r in reqs[:200])


def _compute_goodput(result: dict, preset: SLOPreset) -> dict:
    overall = result.get("overall", {})
    requests = result.get("requests", []) or []
    rps = overall.get("rps", 0.0)
    if not requests:
        return {"rps": rps, "attainment": float("nan"), "goodput": float("nan"),
                "n_requests": 0, "has_ttft": False, "has_tbt": False}
    meets = sum(1 for r in requests if _request_meets_slo(r, preset))
    attainment = meets / len(requests)
    has_ttft = _has_ttft(requests)
    needs_stream = (preset.ttft_s is not None) or (preset.tpot_s is not None) \
        or (preset.tbt_p99_s is not None)
    if needs_stream and not has_ttft:
        # The preset has a TTFT/TPOT term but the run was non-streaming.
        # Caller should warn; we still report attainment based only on E2E
        # components (effectively just success/error check).
        pass
    return {
        "rps": rps,
        "attainment": attainment,
        "goodput": rps * attainment,
        "n_requests": len(requests),
        "has_ttft": has_ttft,
        "has_tbt": _has_tbt(requests),
    }


# ----------------------------- run-group walk -----------------------------

def _latest_result(subdir: Path, index: Optional[int]) -> Optional[Path]:
    rdir = subdir / "results"
    if not rdir.is_dir():
        return None
    if index is not None:
        candidate = rdir / f"result{index}.json"
        return candidate if candidate.exists() else None
    candidates = sorted(rdir.glob("result*.json"))
    if not candidates:
        return None
    best, best_idx = None, -2
    for p in candidates:
        m = re.match(r"result(\d+)\.json", p.name)
        idx = int(m.group(1)) if m else (-1 if p.name == "result.json" else None)
        if idx is not None and idx > best_idx:
            best, best_idx = p, idx
    return best


def walk_run_group(run_group_dir: Path, index: Optional[int],
                   excluded: Iterable[int] = ()) -> list[tuple[int, Path]]:
    out = []
    for child in sorted(run_group_dir.iterdir()):
        if not child.is_dir():
            continue
        n = _extract_node_count(child.name)
        if n == 0 or n in excluded:
            continue
        rfile = _latest_result(child, index)
        if rfile is None:
            print(f"  ! no result file in {child.name}, skipping", file=sys.stderr)
            continue
        out.append((n, rfile))
    return out


# ----------------------------- driver --------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("-e", "--experiment", help="Experiment spec name")
    src.add_argument("--run-group", help="Path to a materialized run group directory")
    parser.add_argument("-b", "--backend", default="ray")
    parser.add_argument("--run-group-name", default=None,
                        help="Specific run group within an experiment (defaults to latest)")
    parser.add_argument("--index", type=int, default=None,
                        help="Pick result{INDEX}.json instead of the latest")
    parser.add_argument("--exclude-nodes", type=int, nargs="*", default=[])
    parser.add_argument("--preset", nargs="*", default=list(SLO_PRESETS.keys()),
                        help=f"SLO presets to evaluate. Defaults to all of: "
                             f"{', '.join(SLO_PRESETS)}")
    parser.add_argument("--csv", default=None, help="Write a long-format CSV here")
    parser.add_argument("--plot", default=None,
                        help="Save a goodput-vs-num_nodes plot at this path")
    parser.add_argument("--ttft-required", action="store_true",
                        help="Exit non-zero if any preset needs TTFT but data is missing")
    args = parser.parse_args(argv)

    # Resolve presets.
    unknown = [p for p in args.preset if p not in SLO_PRESETS]
    if unknown:
        parser.error(f"Unknown preset(s): {unknown}. Available: {list(SLO_PRESETS)}")
    presets = [SLO_PRESETS[name] for name in args.preset]

    # Resolve run group dir.
    if args.run_group:
        run_group_dir = Path(args.run_group)
    else:
        run_group_dir = Path(resolve_run_group_dir(args.experiment,
                                                  run_group=args.run_group_name))
    if not run_group_dir.is_dir():
        parser.error(f"Run group dir not found: {run_group_dir}")
    print(f"Run group : {run_group_dir}")
    print(f"Presets   : {[p.name for p in presets]}")

    pairs = walk_run_group(run_group_dir, args.index, set(args.exclude_nodes))
    if not pairs:
        print("No usable result files found.", file=sys.stderr)
        return 1

    rows: list[dict] = []
    ttft_missing_for: set[str] = set()
    tbt_missing_for: set[str] = set()
    for n_nodes, rfile in pairs:
        with open(rfile, "r") as fh:
            data = json.load(fh)
        for preset in presets:
            stats = _compute_goodput(data, preset)
            stats.update({
                "num_nodes": n_nodes,
                "preset": preset.name,
                "ttft_slo_s": preset.ttft_s,
                "tpot_slo_s": preset.tpot_s,
                "e2e_slo_s": preset.e2e_s,
                "file": str(rfile.relative_to(run_group_dir)),
            })
            rows.append(stats)
            needs_stream = (preset.ttft_s is not None) or (preset.tpot_s is not None) \
                or (preset.tbt_p99_s is not None)
            if needs_stream and not stats["has_ttft"]:
                ttft_missing_for.add(preset.name)
            if preset.tbt_p99_s is not None and not stats.get("has_tbt"):
                tbt_missing_for.add(preset.name)

    # Print a compact table.
    print()
    header = f"{'nodes':>6} {'preset':<14} {'rps':>10} {'attainment':>11} {'goodput':>10} {'n_req':>9} ttft"
    print(header)
    print("-" * len(header))
    for r in rows:
        print(f"{r['num_nodes']:>6} {r['preset']:<14} "
              f"{r['rps']:>10.2f} {r['attainment']:>11.3f} "
              f"{r['goodput']:>10.2f} {r['n_requests']:>9} "
              f"{'yes' if r['has_ttft'] else 'NO'}")

    if ttft_missing_for:
        print()
        print("WARNING: TTFT/TPOT data missing for presets: "
              f"{sorted(ttft_missing_for)}. These presets only checked success+E2E. "
              "Re-run with client.stream=true to get TTFT.")
        if args.ttft_required:
            return 2
    if tbt_missing_for:
        print()
        print("WARNING: P99-TBT data missing for presets: "
              f"{sorted(tbt_missing_for)}. Reported attainment IGNORES the TBT bound "
              "(decode-phase SLO UNVERIFIED) — do not trust these numbers. Re-run with a "
              "go_dispatch built from the TBT-capture commit so per-request tbt_p99_s is emitted.")
        if args.ttft_required:
            return 2

    if args.csv:
        with open(args.csv, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\nWrote CSV: {args.csv}")

    if args.plot:
        _save_plot(rows, args.plot)
        print(f"Wrote plot: {args.plot}")

    return 0


def _save_plot(rows: list[dict], out_path: str) -> None:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    by_preset: dict[str, list[tuple[int, float]]] = {}
    for r in rows:
        by_preset.setdefault(r["preset"], []).append((r["num_nodes"], r["goodput"]))

    fig, ax = plt.subplots(figsize=(10, 6))
    for preset, pts in sorted(by_preset.items()):
        pts.sort()
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ax.plot(xs, ys, marker="o", label=preset)
    ax.set_xlabel("num_nodes")
    ax.set_ylabel("goodput (RPS at SLO)")
    ax.set_title("Goodput per SLO preset vs cluster size")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_path, dpi=120)


if __name__ == "__main__":
    sys.exit(main())
