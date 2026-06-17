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


def collect_server_stats(results_dir: str, app_name: str = "default") -> dict:
    """Fan out to all replicas via Ray actor handles, collect stats exactly once each.

    Requires: Ray initialized, Serve deployment running, collect_stats=True in deployment config.
    Must be called BEFORE the cluster is torn down.
    """
    import ray
    from ray import serve
    from ray.serve._private.constants import SERVE_NAMESPACE

    # run_executor (the orchestrator) is NOT inside the Ray driver, so connect to
    # the running head-node cluster first. Without this, serve.status() raises and
    # the whole collection silently no-ops.
    if not ray.is_initialized():
        try:
            ray.init(address="auto", ignore_reinit_error=True, log_to_driver=False)
            print("[server_stats] connected to Ray (address=auto)", flush=True)
        except Exception as e:
            print(f"[server_stats] ERROR: could not connect to Ray: {e}", flush=True)
            return {"error": f"ray connect failed: {e}", "replicas": []}

    status = serve.status()
    if app_name not in status.applications:
        print(f"[server_stats] WARNING: app '{app_name}' not found in serve.status()", flush=True)
        return {"error": f"app {app_name} not found", "replicas": []}

    app_status = status.applications[app_name]
    all_stats = {}

    for dep_name, dep_status in app_status.deployments.items():
        for replica in dep_status.replicas:
            if replica.state != "RUNNING":
                continue
            try:
                handle = ray.get_actor(replica.actor_name, namespace=SERVE_NAMESPACE)
                stats = ray.get(handle.collect_stats.remote(), timeout=30)
                all_stats[replica.replica_id] = {
                    "replica_id": replica.replica_id,
                    "node_id": replica.node_id,
                    "node_ip": replica.node_ip,
                    "pid": replica.pid,
                    **stats,
                }
            except Exception as e:
                print(f"[server_stats] WARNING: failed to collect from {replica.replica_id}: {e}", flush=True)
                all_stats[replica.replica_id] = {
                    "replica_id": replica.replica_id,
                    "pid": replica.pid,
                    "error": str(e),
                }

    # Write per-replica files. Use replica_id (globally unique) instead of PID
    # since PIDs can collide across nodes.
    results_path = Path(results_dir)
    results_path.mkdir(parents=True, exist_ok=True)
    for replica_id, stats in all_stats.items():
        safe_id = str(replica_id).replace("/", "_").replace(":", "_")
        path = results_path / f"replica_stats_{safe_id}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)

    # Aggregate
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
    pool_ttft, pool_tbt, pool_e2e = [], [], []
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
            if s.get("ttft") is not None:
                pool_ttft.append(s["ttft"])
            if s.get("tbt") is not None:
                pool_tbt.append(s["tbt"])
            if s.get("e2e") is not None:
                pool_e2e.append(s["e2e"])

    req_counts = [r["total_requests"] for r in replicas if r["total_requests"] > 0]
    total = sum(req_counts) if req_counts else 0
    imbalance = max(req_counts) / max(min(req_counts), 1) if len(req_counts) >= 2 else 1.0

    def fleet(v):
        if not v:
            return {"n": 0}
        s = sorted(v)
        return {"n": len(s), "mean": sum(s) / len(s),
                "p50": _pct(s, 0.50), "p90": _pct(s, 0.90),
                "p99": _pct(s, 0.99), "max": s[-1]}

    return {
        "replica_count": len(replicas),
        "total_requests": total,
        "load_imbalance_ratio": imbalance,
        "batching_detected": any(r["max_batch_size"] > 1 for r in replicas),
        "mean_batch_size_across_replicas": (
            sum(r["mean_batch_size"] for r in replicas) / max(len(replicas), 1)
        ),
        # Fleet-wide, proxy-immune server-side distributions (pooled sample).
        "server_ttft": fleet(pool_ttft),   # queued+prefill, s
        "server_tbt": fleet(pool_tbt),     # decode/(gen-1), s  (true decode cadence)
        "server_e2e": fleet(pool_e2e),
        "replicas": replicas,
    }
