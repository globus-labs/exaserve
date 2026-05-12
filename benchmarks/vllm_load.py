"""Benchmark vLLM model load time, single node, K parallel replicas.

Each replica is a separate Python process pinned to its own PVC via
ZE_AFFINITY_MASK. The model lives in /tmp (tmpfs, DDR-backed) so we measure
DDR -> XPU load + vLLM init, not Lustre I/O.

Per-replica timing recorded:
  import_s        : Python import of vllm
  engine_args_s   : AsyncEngineArgs construction
  engine_create_s : weight load + GPU init + KV cache + warmup (the big one)
  weight_load_s   : sub-phase parsed from vLLM stdout (just safetensors load)
  kv_cache_init_s : sub-phase parsed from vLLM stdout
  total_init_s    : engine_args + engine_create

Use:
  python -m benchmarks.vllm_load worker --model-path /tmp/llama --out-dir DIR \
      --replica-id N --gpu-id G [--profile]

  python -m benchmarks.vllm_load launcher --model-path /tmp/llama \
      --replicas K --out-dir DIR
"""
from __future__ import annotations

import argparse
import json
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path


def worker(args) -> None:
    """One replica. Loads vLLM, times each phase, writes JSON."""
    pid = os.getpid()
    host = socket.gethostname()

    os.environ["ZE_AFFINITY_MASK"] = str(args.gpu_id)
    os.environ["MASTER_ADDR"] = "127.0.0.1"
    os.environ["MASTER_PORT"] = str(29500 + args.replica_id)
    os.environ["VLLM_LOGGING_LEVEL"] = "INFO"

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    log_path = out_dir / f"replica_{args.replica_id}.log"
    json_path = out_dir / f"replica_{args.replica_id}.json"

    # OS-level redirect so subprocesses (vLLM forks an EngineCore) inherit
    # the same target. Python sys.stdout/stderr are also rebound for symmetry.
    log = open(log_path, "w", buffering=1)
    os.dup2(log.fileno(), 1)
    os.dup2(log.fileno(), 2)
    sys.stdout = sys.stderr = log

    if args.profile:
        import cProfile
        prof = cProfile.Profile()
        prof.enable()

    t_spawn = time.perf_counter()
    print(f"[rep {args.replica_id} pid={pid}] start gpu={args.gpu_id} "
          f"model={args.model_path}", flush=True)

    t0 = time.perf_counter()
    from vllm import LLM, SamplingParams
    t_import = time.perf_counter() - t0

    t0 = time.perf_counter()
    llm = LLM(
        model=args.model_path,
        tensor_parallel_size=1,
        gpu_memory_utilization=0.85,
        max_model_len=4096,
        enforce_eager=True,
        trust_remote_code=False,
    )
    t_engine_create = time.perf_counter() - t0

    t_total = time.perf_counter() - t_spawn

    if args.profile:
        prof.disable()
        prof.dump_stats(str(out_dir / f"profile_rep{args.replica_id}.pstats"))

    log.flush()
    log_text = log_path.read_text(errors="ignore")
    weight_load_s = None
    model_load_s = None
    kv_cache_init_s = None
    m = re.search(r"loading weights took ([\d.]+) seconds", log_text, re.IGNORECASE)
    if m:
        weight_load_s = float(m.group(1))
    m = re.search(r"model loading took [\d.]+ ?gi?b memory and ([\d.]+) seconds",
                  log_text, re.IGNORECASE)
    if m:
        model_load_s = float(m.group(1))
    m = re.search(r"init engine.*? took ([\d.]+) seconds", log_text, re.IGNORECASE)
    if m:
        kv_cache_init_s = float(m.group(1))

    rec = {
        "replica_id": args.replica_id,
        "gpu_id": args.gpu_id,
        "host": host,
        "pid": pid,
        "model_path": args.model_path,
        "import_s": round(t_import, 3),
        "engine_create_s": round(t_engine_create, 3),
        "total_s": round(t_total, 3),
        "weight_load_s": weight_load_s,         # safetensors -> CPU/GPU
        "model_load_s": model_load_s,           # vLLM "model loading" (incl. alloc)
        "kv_cache_init_s": kv_cache_init_s,     # profile + KV alloc + warmup
    }
    json_path.write_text(json.dumps(rec, indent=2))
    print(f"[rep {args.replica_id}] DONE: {rec}", flush=True)


def launcher(args) -> None:
    """Spawn K parallel worker processes, wait, aggregate."""
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    procs = []
    spawn_start = time.perf_counter()
    for i in range(args.replicas):
        gpu_id = i % 6  # Aurora: 6 PVC per node
        cmd = [
            sys.executable, "-m", "benchmarks.vllm_load", "worker",
            "--model-path", args.model_path,
            "--out-dir", str(out_dir),
            "--replica-id", str(i),
            "--gpu-id", str(gpu_id),
        ]
        if args.profile and i == 0:
            cmd.append("--profile")
        env = {**os.environ}
        # Mute Triton/dpctl chattiness for cleaner logs.
        env.setdefault("ONEAPI_DEVICE_SELECTOR", "opencl:gpu;level_zero:gpu")
        p = subprocess.Popen(cmd, env=env)
        procs.append(p)

    rcs = [p.wait() for p in procs]
    spawn_total = time.perf_counter() - spawn_start

    records = []
    for p in sorted(out_dir.glob("replica_*.json")):
        records.append(json.loads(p.read_text()))

    summary = {
        "replicas": args.replicas,
        "model_path": args.model_path,
        "spawn_total_s": round(spawn_total, 3),
        "max_engine_create_s": max(r["engine_create_s"] for r in records) if records else None,
        "mean_engine_create_s": (sum(r["engine_create_s"] for r in records) / len(records)) if records else None,
        "max_weight_load_s": max((r.get("weight_load_s") or 0) for r in records) if records else None,
        "exit_codes": rcs,
        "records": records,
    }
    summary_path = out_dir / f"summary_k{args.replicas}.json"
    summary_path.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)

    w = sub.add_parser("worker")
    w.add_argument("--model-path", required=True)
    w.add_argument("--out-dir", required=True)
    w.add_argument("--replica-id", type=int, required=True)
    w.add_argument("--gpu-id", type=int, required=True)
    w.add_argument("--profile", action="store_true")

    ll = sub.add_parser("launcher")
    ll.add_argument("--model-path", required=True)
    ll.add_argument("--out-dir", required=True)
    ll.add_argument("--replicas", type=int, required=True)
    ll.add_argument("--profile", action="store_true")

    args = ap.parse_args()
    if args.cmd == "worker":
        worker(args)
    else:
        launcher(args)


if __name__ == "__main__":
    main()
