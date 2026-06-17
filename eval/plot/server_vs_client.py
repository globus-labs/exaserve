#!/usr/bin/env python3
"""Server-side vs client-side TTFT/TBT comparison.

For a run dir, read:
  - client-side per-request (results/result0.json: ttft_s, tbt_p99_s, latency),
  - server-side fleet stats (results/server_stats.json: server_ttft, server_tbt),
and print a comparison. Optionally compare two runs (e.g. http-no-delay on vs off)
to quantify the coalescing TTFT inflation and confirm server-TBT invariance while
client-TBT bursts.

Server-side metrics are proxy-immune (queued+prefill, decode/(gen-1)); client-side
reflects delivery (and is distorted by coalescing when http-no-delay is off).

Usage:
  python -m eval.plot.server_vs_client <run_dir> [<run_dir2> ...]
  python -m eval.plot.server_vs_client --label on:<dirA> off:<dirB>
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import ijson


def client_side(run_dir: Path) -> dict:
    f = run_dir / "results" / "result0.json"
    if not f.exists():
        return {}
    ttft, tbt, lat = [], [], []
    n = nerr = 0
    with open(f, "rb") as fh:
        for r in ijson.items(fh, "requests.item"):
            if int(r.get("run_index", 0)) < 1:
                continue
            n += 1
            if not r.get("success", True):
                nerr += 1
                continue
            t = r.get("ttft_s")
            b = r.get("tbt_p99_s")
            l = r.get("latency")
            if t is not None:
                ttft.append(float(t))
            if b is not None:
                tbt.append(float(b))
            if l is not None:
                lat.append(float(l))
    def pp(v):
        if not v:
            return None
        a = np.asarray(v)
        return {"n": len(a), "mean": float(a.mean()),
                "p50": float(np.percentile(a, 50)), "p90": float(np.percentile(a, 90)),
                "p99": float(np.percentile(a, 99)), "max": float(a.max())}
    return {"n_req": n, "n_err": nerr,
            "client_ttft": pp(ttft), "client_tbt_p99perreq": pp(tbt), "client_e2e": pp(lat)}


def server_side(run_dir: Path) -> dict:
    f = run_dir / "results" / "server_stats.json"
    if not f.exists():
        return {"_missing": True}
    d = json.loads(f.read_text())
    # Prefer data-run-only (warm-up dropped) when the run split was detected.
    return {
        "replica_count": d.get("replica_count"),
        "total_requests": d.get("total_requests"),
        "load_imbalance": d.get("load_imbalance_ratio"),
        "mean_batch": round(d.get("mean_batch_size_across_replicas", 0), 2),
        "run_split_detected": d.get("run_split_detected"),
        "server_ttft": d.get("server_ttft_data") or d.get("server_ttft"),
        "server_tbt": d.get("server_tbt_data") or d.get("server_tbt"),
        "server_e2e": d.get("server_e2e_data") or d.get("server_e2e"),
    }


def _fmt(d, *keys):
    if not d:
        return "  (none)"
    out = []
    for k in keys:
        v = d.get(k)
        if isinstance(v, dict):
            out.append(f"{k}: p50={_n(v.get('p50'))} p99={_n(v.get('p99'))} "
                       f"mean={_n(v.get('mean'))} n={v.get('n')}")
        else:
            out.append(f"{k}={v}")
    return "\n  ".join(out)


def _n(x):
    return f"{x:.4f}" if isinstance(x, (int, float)) else str(x)


def report(label: str, run_dir: Path) -> None:
    print(f"\n===== {label}: {run_dir} =====")
    c = client_side(run_dir)
    s = server_side(run_dir)
    print(f"client n_req={c.get('n_req')} n_err={c.get('n_err')}")
    print("CLIENT-side (delivery; distorted by coalescing when no-delay off):")
    print("  " + _fmt(c, "client_ttft", "client_tbt_p99perreq", "client_e2e"))
    print("SERVER-side (proxy-immune; queued+prefill, decode/(gen-1)):")
    if s.get("_missing"):
        print("  server_stats.json MISSING")
    else:
        print(f"  replicas={s.get('replica_count')} total_req={s.get('total_requests')} "
              f"load_imbalance={s.get('load_imbalance')} mean_batch={s.get('mean_batch')}")
        print("  " + _fmt(s, "server_ttft", "server_tbt", "server_e2e"))


def main(argv):
    args = argv or sys.argv[1:]
    if not args:
        print(__doc__)
        return 1
    for a in args:
        if ":" in a and Path(a.split(":", 1)[1]).exists():
            label, path = a.split(":", 1)
        else:
            label, path = "run", a
        report(label, Path(path))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
