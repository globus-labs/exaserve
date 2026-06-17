"""Server-side stats collection from vLLM replicas via Ray actor handles.

After a benchmark run completes, iterate all Ray Serve replicas and call
collect_stats() on each one to retrieve buffered per-replica scheduler and
per-request statistics. Results are written to per-replica JSON files and
an aggregate server_stats.json.
"""

from __future__ import annotations

import json
import os
from pathlib import Path


def _head_address_from_runtime(results_dir: str):
    """Read head_ip:port from the run's runtime/ray_runtime.yaml (sibling of
    results/). Returns 'ip:port' or None. Avoids a yaml dep with a tiny parser."""
    rt = Path(results_dir).parent / "runtime" / "ray_runtime.yaml"
    if not rt.is_file():
        return None
    # NOTE: the yaml has several `port:` keys (proxy 4001, backend 8000, ray GCS
    # 6379). Take the one that FOLLOWS head_ip (the ray cluster port); default 6379.
    head_ip = port = None
    try:
        seen_head = False
        for line in rt.read_text().splitlines():
            s = line.strip()
            if s.startswith("head_ip:"):
                head_ip = s.split(":", 1)[1].strip().strip("'\"")
                seen_head = True
            elif s.startswith("port:") and seen_head and port is None:
                port = s.split(":", 1)[1].strip().strip("'\"")
        if head_ip:
            return f"{head_ip}:{port or '6379'}"
    except Exception:
        pass
    return None


def collect_server_stats(results_dir: str, app_name: str = "default") -> dict:
    """Fan out to all replicas via Ray actor handles, collect stats exactly once each.

    Requires: Ray initialized, Serve deployment running, collect_stats=True in deployment config.
    Must be called BEFORE the cluster is torn down.
    """
    import ray

    # run_executor (the orchestrator) is NOT inside the Ray driver, so connect to
    # the running head-node cluster first. address="auto" only discovers a cluster
    # whose Ray session dir is on the LOCAL node — true at n1 (run_executor shares
    # the head node) but NOT at multi-node (the head GCS is a specific node). So use
    # the EXPLICIT head_ip:port from the run's ray_runtime.yaml (same address the
    # driver uses), falling back to auto.
    explicit = _head_address_from_runtime(results_dir)
    addr = explicit or "auto"
    if not ray.is_initialized():
        try:
            ray.init(address=addr, ignore_reinit_error=True, log_to_driver=False)
            print(f"[server_stats] connected to Ray (address={addr})", flush=True)
        except Exception as e:
            print(f"[server_stats] ERROR: could not connect to Ray (address={addr}): {e}",
                  flush=True)
            return {"error": f"ray connect failed: {e}", "replicas": []}

    # Replicas push their server-side summaries to a named head actor (see
    # server.py:_serving_stats_push_loop). We read that, instead of enumerating
    # replica actor handles (serve.status() exposes no handles in this Ray ver).
    try:
        actor = ray.get_actor("ServingStatsCollector", namespace="serve")
        pushed = ray.get(actor.get_all.remote(), timeout=60)
    except Exception as e:
        print(f"[server_stats] WARNING: no ServingStatsCollector actor "
              f"(no replica pushed? collect_stats off / logger not recording?): {e}", flush=True)
        return {"error": f"no serving-stats actor: {e}", "replicas": []}

    all_stats = {}
    for key, payload in (pushed or {}).items():
        all_stats[key] = {
            "replica_id": key,
            "node_ip": payload.get("node_ip"),
            "pid": payload.get("pid"),
            "summary": payload.get("summary"),
            "sample": payload.get("sample"),
        }
    print(f"[server_stats] read {len(all_stats)} replica summaries from ServingStatsCollector",
          flush=True)

    # Write ONE combined per-replica file (NOT one-per-replica: 3072 small files
    # at 256n would storm the Lustre MDS, ~5s/file under contention).
    results_path = Path(results_dir)
    results_path.mkdir(parents=True, exist_ok=True)
    with open(results_path / "replica_stats_all.json", "w", encoding="utf-8") as f:
        json.dump(all_stats, f)

    # Aggregate (pooled over the whole job AND data-run-only via the cooldown gap).
    aggregate = aggregate_replica_stats(all_stats)
    agg_path = results_path / "server_stats.json"
    with open(agg_path, "w", encoding="utf-8") as f:
        json.dump(aggregate, f, indent=2)

    print(
        f"[server_stats] Collected stats from {aggregate['replica_count']} replicas, "
        f"total_requests={aggregate['total_requests']}, "
        f"load_imbalance={aggregate['load_imbalance_ratio']:.2f}, "
        f"batching_detected={aggregate['batching_detected']}",
        flush=True,
    )
    return aggregate


def _pct(sorted_vals, q):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = min(len(sorted_vals) - 1, max(0, int(round(q * (len(sorted_vals) - 1)))))
    return sorted_vals[idx]


def aggregate_replica_stats(all_stats: dict) -> dict:
    """Aggregate per-replica stats into a fleet summary.

    Per-replica `summary` (server-TTFT/TBT/e2e/batch) comes from the replica.
    Fleet-wide percentiles are computed from the pooled per-request `sample`
    (capped/strided per replica) so they are true pooled percentiles, not an
    average-of-percentiles. server-TTFT/TBT are proxy-immune by construction.
    """
    replicas = []
    pool = []  # (finished_at, ttft, tbt, e2e) across all replicas, for run split
    for replica_id, stats in all_stats.items():
        summ = stats.get("summary")
        if not summ:
            continue
        replicas.append({
            "replica_id": replica_id,
            "pid": stats.get("pid"),
            "node_ip": stats.get("node_ip"),
            "total_requests": summ.get("total_requests", 0),
            "mean_batch_size": summ.get("mean_batch_size", 0),
            "max_batch_size": summ.get("max_batch_size", 0),
            "server_ttft_p99": (summ.get("server_ttft") or {}).get("p99"),
            "server_tbt_p99": (summ.get("server_tbt") or {}).get("p99"),
            "e2e_p99": (summ.get("e2e") or {}).get("p99"),
            "kv_cache_peak": summ.get("kv_cache_peak", 0),
        })
        for s in stats.get("sample", []) or []:
            pool.append((s.get("finished_at"), s.get("ttft"), s.get("tbt"), s.get("e2e")))

    req_counts = [r["total_requests"] for r in replicas if r["total_requests"] > 0]
    total = sum(req_counts) if req_counts else 0
    imbalance = max(req_counts) / max(min(req_counts), 1) if len(req_counts) >= 2 else 1.0

    def fleet(rows):
        out = {}
        for i, name in ((1, "ttft"), (2, "tbt"), (3, "e2e")):
            v = sorted(r[i] for r in rows if r[i] is not None)
            out[name] = ({"n": len(v), "mean": sum(v) / len(v), "p50": _pct(v, 0.50),
                          "p90": _pct(v, 0.90), "p99": _pct(v, 0.99), "max": v[-1]}
                         if v else {"n": 0})
        return out

    # Data-run-only: warm-up (run 0) and data (run 1) are separated by the fixed
    # 75 s cooldown, so the finished_at stream has a >~40 s gap with no finishes.
    # Split there and keep the LAST segment (the data run). Reviewer-proof: server
    # metrics then exclude the cold warm-up run.
    ts = sorted(t for (t, *_ ) in pool if t)
    data_rows = pool
    split_at = None
    if len(ts) > 10:
        gaps = [(ts[i + 1] - ts[i], ts[i + 1]) for i in range(len(ts) - 1)]
        big = max(gaps, key=lambda g: g[0]) if gaps else (0, None)
        if big[0] >= 40.0:  # cooldown gap detected
            split_at = big[1]
            data_rows = [r for r in pool if r[0] and r[0] >= split_at]

    all_fleet = fleet(pool)
    data_fleet = fleet(data_rows)
    return {
        "replica_count": len(replicas),
        "total_requests": total,
        "load_imbalance_ratio": imbalance,
        "batching_detected": any(r["max_batch_size"] > 1 for r in replicas),
        "mean_batch_size_across_replicas": (
            sum(r["mean_batch_size"] for r in replicas) / max(len(replicas), 1)
        ),
        "run_split_detected": split_at is not None,
        # DATA-RUN-only fleet distributions (warm-up dropped) — use these.
        "server_ttft_data": data_fleet["ttft"],
        "server_tbt_data": data_fleet["tbt"],
        "server_e2e_data": data_fleet["e2e"],
        # Pooled-over-job (incl. warm-up) — kept for reference.
        "server_ttft": all_fleet["ttft"],   # queued+prefill, s
        "server_tbt": all_fleet["tbt"],     # decode/(gen-1), s  (true decode cadence)
        "server_e2e": all_fleet["e2e"],
        "replicas": replicas,
    }
