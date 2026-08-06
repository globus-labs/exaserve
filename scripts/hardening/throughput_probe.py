#!/usr/bin/env python3
"""Direct-mode aggregate-throughput probe for the scaling smoke.

Drives concurrent completion requests round-robin across every node's direct
Ray Serve route (http://<node>:8000/<route>/v1/completions) for a fixed
duration and reports aggregate + per-node RPS. Weak scaling: per-node RPS
should stay ~flat as nodes grow (the recorded direct baseline is ~110 rps/node
offered, ~6.8k aggregate at 64n for 8B 64-in/64-out).

This bypasses the eval harness (which snapshots committed HEAD) so it exercises
the LIVE hardened code staged to /tmp/exaserve_src by launch_cluster.

Usage: throughput_probe.py --nodefile <file> --route <route> --duration 30
                           --concurrency-per-node 24 --max-tokens 64 --prompt-len 64
"""

from __future__ import annotations

import argparse
import asyncio
import json
import time
import urllib.request

_DIRECT = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def _aggregate(shard_dir: str, out: str) -> int:
    """Sum per-rank shards (MPI mode) into one weak-scaling result."""
    import glob
    shards = []
    for path in sorted(glob.glob(f"{shard_dir}/shard_*.json")):
        with open(path) as fh:
            shards.append(json.load(fh))
    if not shards:
        print(json.dumps({"error": "no shards found", "shard_dir": shard_dir}))
        return 1
    n = len(shards)
    total_ok = sum(s["ok"] for s in shards)
    total_err = sum(s["err"] for s in shards)
    elapsed = max(s["elapsed_s"] for s in shards)
    lat = sorted(x for s in shards for x in s.get("latencies", []))

    def pct(p):
        return round(lat[int(len(lat) * p)], 3) if lat else 0.0

    result = {
        "nodes": n,
        "duration_s": round(elapsed, 1),
        "total_ok": total_ok,
        "total_err": total_err,
        "aggregate_rps": round(total_ok / elapsed, 1),
        "per_node_rps": round(total_ok / elapsed / n, 2),
        "p50_latency_s": pct(0.50),
        "p99_latency_s": pct(0.99),
        "error_rate": round(total_err / max(total_ok + total_err, 1), 4),
    }
    print(json.dumps(result, indent=2))
    if out:
        with open(out, "w") as fh:
            json.dump(result, fh, indent=2)
    return 0


def _post(url: str, body: bytes, timeout: float) -> tuple[bool, float]:
    t0 = time.monotonic()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    try:
        with _DIRECT.open(req, timeout=timeout) as resp:
            resp.read()
            return resp.status == 200, time.monotonic() - t0
    except Exception:
        return False, time.monotonic() - t0


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodefile", default="")
    ap.add_argument("--ips-file", default="",
                    help="ray_node_ips.txt (preferred): Ray Serve binds the "
                         "HTTP proxy to the Ray node IP, not the PBS hostname")
    ap.add_argument("--route", default="")  # "" = root route (single model)
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--duration", type=float, default=30.0)
    ap.add_argument("--concurrency-per-node", type=int, default=24)
    ap.add_argument("--max-tokens", type=int, default=64)
    ap.add_argument("--prompt-len", type=int, default=64)
    ap.add_argument("--out", default="")
    ap.add_argument("--mpi", action="store_true",
                    help="Distributed mode: launched via mpiexec -ppn 1, each "
                         "rank drives load against ONE node (avoids the "
                         "single-fat-client ceiling; = baseline Direct-MPI). "
                         "Writes a per-rank shard; aggregate with --aggregate.")
    ap.add_argument("--shard-dir", default="")
    ap.add_argument("--aggregate", default="",
                    help="Aggregate shard-dir into a final result (no load).")
    args = ap.parse_args()

    if args.aggregate:
        return _aggregate(args.aggregate, args.out)

    # Prefer the Ray node IPs; fall back to hostnames only if no ips-file.
    if args.ips_file:
        with open(args.ips_file) as fh:
            nodes = [ln.strip() for ln in fh if ln.strip()]
    else:
        with open(args.nodefile) as fh:
            nodes = sorted({ln.split(".")[0] for ln in fh.read().split() if ln.strip()})
    route = args.route.rstrip("/")
    urls = [f"http://{n}:{args.port}{route}/v1/completions" for n in nodes]
    prompt = "The capital of France is " * max(1, args.prompt_len // 5)
    # Omit "model": a model-specific deployment infers it, and PR-011 now
    # correctly 400s an unknown model name.
    body = json.dumps({
        "prompt": prompt, "max_tokens": args.max_tokens, "temperature": 0.0,
    }).encode()

    # MPI mode: this rank drives load against exactly ONE node so that N
    # independent client processes (one per node) replace a single fat client.
    if args.mpi:
        import os as _os
        rank = int(_os.environ.get("PALS_RANKID")
                   or _os.environ.get("PMI_RANK")
                   or _os.environ.get("SLURM_PROCID") or 0)
        my = [rank % len(nodes)]
    else:
        my = list(range(len(nodes)))

    loop = asyncio.get_event_loop()
    stop_at = time.monotonic() + args.duration
    per_node_ok = {nodes[i]: 0 for i in my}
    per_node_err = {nodes[i]: 0 for i in my}
    latencies: list[float] = []

    async def worker(node_idx: int):
        url = urls[node_idx]
        node = nodes[node_idx]
        while time.monotonic() < stop_at:
            ok, lat = await loop.run_in_executor(None, _post, url, body, 60.0)
            if ok:
                per_node_ok[node] += 1
                latencies.append(lat)
            else:
                per_node_err[node] += 1

    tasks = []
    for i in my:
        for _ in range(args.concurrency_per_node):
            tasks.append(asyncio.create_task(worker(i)))

    t0 = time.monotonic()
    await asyncio.gather(*tasks)
    elapsed = time.monotonic() - t0

    total_ok = sum(per_node_ok.values())
    total_err = sum(per_node_err.values())

    if args.mpi and args.shard_dir:
        # Write a per-rank shard; the harness aggregates after mpiexec returns.
        import os as _os
        rank = int(_os.environ.get("PALS_RANKID")
                   or _os.environ.get("PMI_RANK")
                   or _os.environ.get("SLURM_PROCID") or 0)
        _os.makedirs(args.shard_dir, exist_ok=True)
        shard = {"ok": total_ok, "err": total_err, "elapsed_s": elapsed,
                 "latencies": latencies[:2000]}
        tmp = _os.path.join(args.shard_dir, f".shard_{rank}.tmp")
        with open(tmp, "w") as fh:
            json.dump(shard, fh)
        _os.replace(tmp, _os.path.join(args.shard_dir, f"shard_{rank}.json"))
        return 0

    agg_rps = total_ok / elapsed
    per_node_rps = agg_rps / len(my)
    latencies.sort()

    def pct(p):
        return latencies[int(len(latencies) * p)] if latencies else 0.0

    result = {
        "nodes": len(nodes),
        "duration_s": round(elapsed, 1),
        "concurrency_per_node": args.concurrency_per_node,
        "total_ok": total_ok,
        "total_err": total_err,
        "aggregate_rps": round(agg_rps, 1),
        "per_node_rps": round(per_node_rps, 2),
        "p50_latency_s": round(pct(0.50), 3),
        "p99_latency_s": round(pct(0.99), 3),
        "error_rate": round(total_err / max(total_ok + total_err, 1), 4),
    }
    print(json.dumps(result, indent=2))
    if args.out:
        with open(args.out, "w") as fh:
            json.dump(result, fh, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
