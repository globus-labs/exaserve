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


def aggregate_replica_stats(all_stats: dict) -> dict:
    """Aggregate per-replica stats into a summary."""
    replicas = []
    for replica_id, stats in all_stats.items():
        if "error" in stats and "summary" not in stats:
            continue
        summary = stats.get("summary", {})
        replicas.append({
            "replica_id": replica_id,
            "pid": stats.get("pid"),
            "node_ip": stats.get("node_ip"),
            "total_requests": summary.get("total_requests", 0),
            "mean_batch_size": summary.get("mean_batch_size", 0),
            "max_batch_size": summary.get("max_batch_size", 0),
            "mean_e2e_latency": summary.get("mean_e2e_latency", 0),
            "mean_queued_time": summary.get("mean_queued_time", 0),
            "mean_prefill_time": summary.get("mean_prefill_time", 0),
            "kv_cache_peak": summary.get("kv_cache_peak", 0),
        })

    req_counts = [r["total_requests"] for r in replicas if r["total_requests"] > 0]
    total = sum(req_counts) if req_counts else 0
    imbalance = max(req_counts) / max(min(req_counts), 1) if len(req_counts) >= 2 else 1.0

    return {
        "replicas": replicas,
        "replica_count": len(replicas),
        "total_requests": total,
        "load_imbalance_ratio": imbalance,
        "batching_detected": any(r["max_batch_size"] > 1 for r in replicas),
        "mean_batch_size_across_replicas": (
            sum(r["mean_batch_size"] for r in replicas) / max(len(replicas), 1)
        ),
    }
