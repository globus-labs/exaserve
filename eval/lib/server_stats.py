"""Server-side stats collection from vLLM replicas via Ray actor handles.

After a benchmark run completes, iterate all Ray Serve replicas and call
collect_stats() on each one to retrieve buffered per-replica scheduler and
per-request statistics. Results are written to per-replica JSON files and
an aggregate server_stats.json.
"""

from __future__ import annotations

import ipaddress
from pathlib import Path

from exaserve.state.atomic import atomic_create_json
from exaserve.telemetry import (
    TelemetryContractError,
    TelemetryIdentity,
    telemetry_actor_name,
    validate_serving_snapshot,
)


def ray_address_from_status(status_dir: str, *, status, plan) -> str:
    """Resolve Ray GCS from exact binding + READY node evidence, never YAML."""
    from exaserve.plan.contracts import same_node
    from exaserve.status_api import load_status_allocation_binding

    binding = load_status_allocation_binding(status_dir, status)
    if (
        binding.allocation_binding_hash != status.allocation_binding_hash
        or plan.deployment_plan_hash != status.deployment_plan_hash
    ):
        raise RuntimeError("Ray-address binding/plan identity disagrees with DeploymentStatus")
    head_node = binding.node_for(0)
    matches = [
        item
        for item in status.readiness_snapshot.get("nodes", [])
        if isinstance(item, dict)
        and item.get("alive") is True
        and isinstance(item.get("node_name"), str)
        and same_node(item["node_name"], head_node or "")
    ]
    if len(matches) != 1:
        raise RuntimeError(f"READY evidence maps Ray head rank to {len(matches)} nodes")
    address = matches[0].get("node_address")
    if not isinstance(address, str):
        raise RuntimeError("READY Ray head address is not text")
    try:
        parsed = ipaddress.ip_address(address)
    except ValueError as exc:
        raise RuntimeError("READY Ray head address is not an IP address") from exc
    if parsed.is_loopback or parsed.is_unspecified or parsed.is_multicast:
        raise RuntimeError("READY Ray head address is not allocation-reachable")
    rendered = f"[{address}]" if parsed.version == 6 else address
    return f"{rendered}:{plan.ray_port}"


def collect_server_stats(
    results_dir: str, *, identity: TelemetryIdentity, expected_replicas: int, ray_address: str
) -> dict:
    """Fan out to all replicas via Ray actor handles, collect stats exactly once each.

    Requires: Ray initialized, Serve deployment running, collect_stats=True in deployment config.
    Must be called BEFORE the cluster is torn down.
    """
    import ray

    if not isinstance(ray_address, str) or not ray_address:
        raise ValueError("canonical Ray address is required for stats collection")
    if not ray.is_initialized():
        try:
            ray.init(address=ray_address, ignore_reinit_error=True, log_to_driver=False)
            print(f"[server_stats] connected to Ray (address={ray_address})", flush=True)
        except Exception as e:
            print(
                f"[server_stats] ERROR: could not connect to Ray (address={ray_address}): {e}",
                flush=True,
            )
            return {"error": f"ray connect failed: {e}", "replicas": []}

    # Replicas push their server-side summaries to a named head actor (see
    # server.py:_serving_stats_push_loop). We read that, instead of enumerating
    # replica actor handles (serve.status() exposes no handles in this Ray ver).
    actor = None
    collection_error = None
    cleanup_error = None
    try:
        actor = ray.get_actor(telemetry_actor_name("serving", identity), namespace="serve")
        pushed = ray.get(actor.snapshot.remote(), timeout=60)
    except Exception as e:
        print(
            f"[server_stats] WARNING: no ServingStatsCollector actor "
            f"(no replica pushed? collect_stats off / logger not recording?): {e}",
            flush=True,
        )
        collection_error = f"no serving-stats actor: {e}"
    finally:
        # The actor is driver-owned and would disappear with the deployment,
        # but explicit collection is its successful terminal operation.
        if actor is not None:
            try:
                ray.kill(actor, no_restart=True)
            except (RuntimeError, ValueError) as exc:
                cleanup_error = (
                    "could not terminate the consumed ServingStatsCollector: "
                    f"{type(exc).__name__}: {exc}"
                )
                print(
                    f"[server_stats] WARNING: {cleanup_error}",
                    flush=True,
                )
    if collection_error is not None:
        return {"error": collection_error, "replicas": []}
    if cleanup_error is not None:
        return {"error": cleanup_error, "replicas": []}

    try:
        pushed = validate_serving_snapshot(
            pushed,
            expected_identity=identity,
            expected_replicas=expected_replicas,
        )
    except TelemetryContractError as exc:
        return {"error": f"invalid serving-stats snapshot: {exc}", "replicas": []}
    if not pushed["complete"]:
        return {
            "error": (
                f"serving stats incomplete: {pushed['received_replicas']}/{expected_replicas}"
            ),
            "replicas": [],
        }

    all_stats = {}
    for key, envelope in pushed["replicas"].items():
        payload = envelope["payload"]
        all_stats[key] = {
            "replica_id": key,
            "node_ip": payload.get("node_ip"),
            "pid": payload.get("pid"),
            "model_id": payload.get("model_id"),
            "summary": payload.get("summary"),
            "sample": payload.get("sample"),
        }
    print(
        f"[server_stats] read {len(all_stats)} replica summaries from ServingStatsCollector",
        flush=True,
    )

    # Write ONE combined per-replica file (NOT one-per-replica: 3072 small files
    # at 256n would storm the Lustre MDS, ~5s/file under contention).
    results_path = Path(results_dir)
    results_path.mkdir(parents=True, exist_ok=True)
    atomic_create_json(
        results_path / "replica_stats_all.json",
        {
            "schema_version": 1,
            "identity": identity.to_dict(),
            "expected_replicas": expected_replicas,
            "received_replicas": len(all_stats),
            "replicas": all_stats,
        },
    )

    # Aggregate (pooled over the whole job AND data-run-only via the cooldown gap).
    aggregate = aggregate_replica_stats(all_stats)
    agg_path = results_path / "server_stats.json"
    aggregate = {
        "schema_version": 1,
        "identity": identity.to_dict(),
        "expected_replicas": expected_replicas,
        "received_replicas": len(all_stats),
        "complete": len(all_stats) == expected_replicas,
        **aggregate,
    }
    atomic_create_json(agg_path, aggregate)

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
        replicas.append(
            {
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
            }
        )
        for s in stats.get("sample", []) or []:
            pool.append((s.get("finished_at"), s.get("ttft"), s.get("tbt"), s.get("e2e")))

    req_counts = [r["total_requests"] for r in replicas if r["total_requests"] > 0]
    total = sum(req_counts) if req_counts else 0
    imbalance = max(req_counts) / max(min(req_counts), 1) if len(req_counts) >= 2 else 1.0

    def fleet(rows):
        out = {}
        for i, name in ((1, "ttft"), (2, "tbt"), (3, "e2e")):
            v = sorted(r[i] for r in rows if r[i] is not None)
            out[name] = (
                {
                    "n": len(v),
                    "mean": sum(v) / len(v),
                    "p50": _pct(v, 0.50),
                    "p90": _pct(v, 0.90),
                    "p99": _pct(v, 0.99),
                    "max": v[-1],
                }
                if v
                else {"n": 0}
            )
        return out

    # Data-run-only: warm-up (run 0) and data (run 1) are separated by the fixed
    # 75 s cooldown, so the finished_at stream has a >~40 s gap with no finishes.
    # Split there and keep the LAST segment (the data run). Reviewer-proof: server
    # metrics then exclude the cold warm-up run.
    ts = sorted(t for (t, *_) in pool if t)
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
        "server_ttft": all_fleet["ttft"],  # queued+prefill, s
        "server_tbt": all_fleet["tbt"],  # decode/(gen-1), s  (true decode cadence)
        "server_e2e": all_fleet["e2e"],
        "replicas": replicas,
    }
