from __future__ import annotations

import asyncio
import concurrent.futures
from contextlib import ExitStack
import glob
import hashlib
import json
import math
import os
import pathlib
import re
import shutil
import signal
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid
import urllib.error
import urllib.parse
import urllib.request

from eval.lib.manifest import EvalManifest, load_eval_manifest
from exaserve.control.process_handshake import (
    prepare_ready_handshake,
    ready_handshake_args,
    wait_ready_handshake,
)
from exaserve.exception_notes import add_exception_note
from exaserve.go_result_contract import (
    read_go_result_stream,
    validate_go_summary as _validate_go_summary,
)

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except ImportError:  # pragma: no cover - optional dependency
    pass


def _init_mpi():
    # Importing ``mpi4py.MPI`` can initialize the site MPI runtime. Keep that
    # side effect inside the executable path rather than module import so
    # planners, tests, and analysis tools remain safe on a login node.
    try:
        from mpi4py import MPI
    except ImportError:  # pragma: no cover - optional dependency
        return None, 0, 1
    comm = MPI.COMM_WORLD
    return comm, comm.Get_rank(), comm.Get_size()


def _mpi_barrier(comm):
    if comm is not None and comm.Get_size() > 1:
        comm.Barrier()


def _mpi_bcast(comm, value, root=0):
    if comm is not None and comm.Get_size() > 1:
        return comm.bcast(value, root=root)
    return value


def _mpi_gather(comm, value, root=0):
    if comm is not None and comm.Get_size() > 1:
        return comm.gather(value, root=root)
    return [value]


def _gather_results_via_shards(
    comm,
    local_results,
    *,
    run_index,
    shard_dir,
    rank,
    mpi_size,
    is_root,
    timeout_s=600.0,
):
    """Robust replacement for a single-root MPI collective gather of large
    per-request result sets (dest=direct, multi-node).

    The collective ``comm.gather`` pickles every rank's records onto root in
    one all-ranks operation; at 64 nodes (~hundreds of thousands of records)
    it is slow, memory-heavy on root, and — being a barrier — hangs forever if
    any node slows or drops ("Application not found"), so root never writes
    results. Instead, each rank writes its shard to shared storage
    independently (atomic rename, no collective), and root polls for the
    shards, reading whatever arrives within ``timeout_s`` and logging any
    ranks that never showed (their data is dropped, not the whole run).

    For ``mpi_size <= 1`` (proxy mode: single client) this is a no-op that
    returns ``[local_results]`` — identical to the old path.

    Design note — why not reuse ``gather.c`` (the log-archive gather): it is a
    standalone MPI binary (``mpiexec gather ...``) intended for a separate,
    supervised post-replay step. It cannot be called here because this gather
    runs INSIDE the replay, which is itself ``mpiexec -n N python
    replay_client``, and PALS does not support nested mpiexec (a likely source
    of the original "Application not found"). The replay client also holds no
    Ray handle, so the scaling-trace collection path (Ray, server-side) is
    unavailable. This shard write therefore reproduces gather.c's own
    sanctioned data path — per its header, "rank -> Lustre (N concurrent
    distinct-name creates; MDS handles this fine); no rank-0 buffer
    collection" — using the replay's existing ranks instead of a fresh,
    un-nestable mpiexec. Keep it this way unless results are restructured into
    a post-replay gather.c artifact (which would move merge/summary out of
    replay_engine into a new post-finalize step).
    """
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s <= 0
    ):
        raise ValueError("result shard timeout must be finite and positive")
    if (
        isinstance(run_index, bool)
        or not isinstance(run_index, int)
        or run_index < 0
        or isinstance(rank, bool)
        or not isinstance(rank, int)
        or isinstance(mpi_size, bool)
        or not isinstance(mpi_size, int)
        or mpi_size < 1
        or not 0 <= rank < mpi_size
    ):
        raise ValueError("result shard rank/run identity is invalid")
    _LAST_GATHER_META.clear()
    if comm is None or mpi_size <= 1:
        encoded = _encode_gather_payload(local_results)
        _LAST_GATHER_META.update(
            {
                "schema_version": 1,
                "expected_ranks": 1,
                "collected_ranks": [0],
                "missing_ranks": [],
                "complete": True,
                "shards": [
                    {
                        "rank": 0,
                        "size_bytes": len(encoded),
                        "sha256": hashlib.sha256(encoded).hexdigest(),
                        "transport": "in_memory",
                    }
                ],
            }
        )
        return [local_results]

    os.makedirs(shard_dir, mode=0o700, exist_ok=True)
    directory_metadata = os.lstat(shard_dir)
    if not stat.S_ISDIR(directory_metadata.st_mode) or directory_metadata.st_uid != os.getuid():
        raise RuntimeError("replay shard directory must be a user-owned real directory")
    os.chmod(shard_dir, 0o700)
    shard = os.path.join(shard_dir, f"run{run_index}_rank{rank}.json")
    encoded = _encode_gather_payload(local_results)
    from exaserve.state.atomic import atomic_create_bytes

    atomic_create_bytes(shard, encoded)

    if not is_root:
        return None

    # Root already holds its own shard in memory; poll only for the others.
    collected = {rank: local_results}
    shard_evidence = {
        rank: {
            "rank": rank,
            "size_bytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "transport": "shared_file",
        }
    }
    deadline = time.monotonic() + timeout_s
    shard_errors: dict[int, str] = {}
    while len(collected) < mpi_size and time.monotonic() < deadline:
        for other in range(mpi_size):
            if other in collected:
                continue
            path = os.path.join(shard_dir, f"run{run_index}_rank{other}.json")
            try:
                from exaserve.state.atomic import regular_file_reader

                with regular_file_reader(path, binary=True) as handle:
                    content = handle.read()
                collected[other] = _decode_gather_payload(content)
                shard_errors.pop(other, None)
                shard_evidence[other] = {
                    "rank": other,
                    "size_bytes": len(content),
                    "sha256": hashlib.sha256(content).hexdigest(),
                    "transport": "shared_file",
                }
            except FileNotFoundError:
                continue
            except (OSError, UnicodeError, ValueError, TypeError) as exc:
                # Each attempt and rank owns one create-once artifact, so a
                # malformed shard is final evidence for this attempt. Continue
                # polling the remaining ranks and publish the exact rejection.
                shard_errors[other] = f"{type(exc).__name__}: {exc}"
        if len(collected) < mpi_size:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(1.0, remaining))

    missing = [r for r in range(mpi_size) if r not in collected]
    if missing:
        print(
            f"[replay_engine] WARNING: run {run_index} gather collected "
            f"{len(collected)}/{mpi_size} rank shards after {timeout_s:.0f}s; "
            f"missing ranks {missing} — their requests are dropped from this run.",
            flush=True,
        )
    # Completeness is result data, not just a log line. The outer run executor
    # validates the published result manifest and never labels a partial run
    # succeeded.
    _LAST_GATHER_META.clear()
    gather_meta = {
        "schema_version": 1,
        "expected_ranks": mpi_size,
        "collected_ranks": sorted(collected),
        "missing_ranks": missing,
        "complete": not missing,
        "shards": [shard_evidence[item] for item in sorted(shard_evidence)],
    }
    invalid_shards = {
        str(item): shard_errors[item] for item in sorted(shard_errors) if item in missing
    }
    if invalid_shards:
        gather_meta["invalid_shards"] = invalid_shards
    _LAST_GATHER_META.update(gather_meta)
    return [collected[r] for r in sorted(collected)]


def _encode_gather_payload(results) -> bytes:
    if isinstance(results, dict):
        payload = {
            "schema_version": 1,
            "kind": "summary",
            "summary": _validate_go_summary(results),
        }
    elif isinstance(results, list):
        records = []
        for index, item in enumerate(results):
            if not isinstance(item, (tuple, list)) or len(item) != 13:
                raise ValueError(f"gather result {index} must contain 13 fields")
            request = item[0]
            if not isinstance(request, TraceRequest):
                raise TypeError(f"gather result {index} request is not TraceRequest")
            records.append(
                {
                    "request": {
                        "timestamp": request.timestamp,
                        "model": request.model,
                        "prompt": request.prompt,
                        "input_len": request.input_len,
                        "output_len": request.output_len,
                        "tensor_parallel_size": request.tensor_parallel_size,
                        "req_id": request.req_id,
                        "mode": request.mode,
                    },
                    "measurements": list(item[1:]),
                }
            )
        payload = {"schema_version": 1, "kind": "records", "records": records}
    else:
        raise TypeError("gather payload must be a summary mapping or result list")
    return (
        json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    ).encode("utf-8")


def _decode_gather_payload(content: bytes):
    from exaserve.state.atomic import strict_json_loads

    payload = strict_json_loads(content.decode("utf-8"))
    if (
        not isinstance(payload, dict)
        or type(payload.get("schema_version")) is not int
        or payload.get("schema_version") != 1
    ):
        raise ValueError("gather shard schema_version is missing or unsupported")
    kind = payload.get("kind")
    if kind == "summary":
        if set(payload) != {"schema_version", "kind", "summary"} or not isinstance(
            payload["summary"], dict
        ):
            raise ValueError("gather summary shard has an invalid shape")
        return _validate_go_summary(payload["summary"])
    if kind != "records" or set(payload) != {"schema_version", "kind", "records"}:
        raise ValueError("gather record shard has an invalid shape")
    if not isinstance(payload["records"], list):
        raise ValueError("gather records must be a list")
    results = []
    request_fields = {
        "timestamp",
        "model",
        "prompt",
        "input_len",
        "output_len",
        "tensor_parallel_size",
        "req_id",
        "mode",
    }
    for index, record in enumerate(payload["records"]):
        if not isinstance(record, dict) or set(record) != {"request", "measurements"}:
            raise ValueError(f"gather record {index} has an invalid shape")
        request = record["request"]
        measurements = record["measurements"]
        if not isinstance(request, dict) or set(request) != request_fields:
            raise ValueError(f"gather record {index} request has an invalid shape")
        if not isinstance(measurements, list) or len(measurements) != 12:
            raise ValueError(f"gather record {index} measurements must contain 12 fields")
        if (
            isinstance(request["timestamp"], bool)
            or not isinstance(request["timestamp"], (int, float))
            or not math.isfinite(float(request["timestamp"]))
            or any(
                isinstance(request[field], bool) or not isinstance(request[field], int)
                for field in ("input_len", "output_len", "tensor_parallel_size")
            )
            or request["input_len"] < 0
            or request["output_len"] < 0
            or request["tensor_parallel_size"] < 1
            or any(
                not isinstance(request[field], str) or not request[field]
                for field in ("model", "req_id", "mode")
            )
            or not isinstance(request["prompt"], str)
            or request["mode"] not in {"chat", "completion"}
        ):
            raise ValueError(f"gather record {index} request fields are invalid")
        rebuilt = TraceRequest(**request)
        results.append((rebuilt, *_validate_gather_measurements(measurements, index=index)))
    return results


# Written by _gather_results_via_shards on the root rank; merged into the
# result meta by _save_results. Single-threaded per-process access.
_LAST_GATHER_META: dict = {}


class TraceRequest(object):
    def __init__(
        self,
        timestamp: float,
        model: str,
        prompt: str,
        input_len: int,
        output_len: int,
        tensor_parallel_size: int,
        req_id: str,
        mode: str = "chat",
    ) -> None:
        if (
            isinstance(timestamp, bool)
            or not isinstance(timestamp, (int, float))
            or not math.isfinite(float(timestamp))
            or timestamp < 0
        ):
            raise ValueError("trace request timestamp must be finite and non-negative")
        for name, value, minimum in (
            ("input_len", input_len, 0),
            ("output_len", output_len, 1),
            ("tensor_parallel_size", tensor_parallel_size, 1),
        ):
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"trace request {name} must be an integer >= {minimum}")
        if not isinstance(model, str) or not model:
            raise ValueError("trace request model must be non-empty text")
        if not isinstance(prompt, str):
            raise ValueError("trace request prompt must be text")
        if not isinstance(req_id, str) or not req_id:
            raise ValueError("trace request req_id must be non-empty text")
        if mode not in {"chat", "completion"}:
            raise ValueError("trace request mode must be chat or completion")
        self.timestamp = float(timestamp)
        self.model = model
        self.prompt = prompt
        self.input_len = input_len
        self.output_len = output_len
        self.tensor_parallel_size = tensor_parallel_size
        self.req_id = req_id
        self.mode = mode


def _percentile(values: list[float], fraction: float) -> float:
    if not values:
        raise ValueError("percentile requires at least one value")
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(value) for value in values)
    rank = (len(ordered) - 1) * fraction
    low = math.floor(rank)
    high = math.ceil(rank)
    if low == high:
        return ordered[int(rank)]
    return ordered[low] * (high - rank) + ordered[high] * (rank - low)


def _summarize_run_results(
    run_index: int,
    run_results,
    requests_scheduled: int,
    duration_s: float | None,
) -> dict[str, float | int | None]:
    if duration_s is None:
        duration = 1e-6
    else:
        _result_number(duration_s, field="run duration_s")
        duration = max(float(duration_s), 1e-6)
    if isinstance(run_results, dict):
        expected = {
            "requests_completed",
            "requests_scheduled",
            "errors",
            "total_input_tokens",
            "total_output_tokens",
            "p50_s",
            "p99_s",
        }
        if set(run_results) != expected:
            raise ValueError("merged Go replay summary fields are invalid")
        for field in (
            "requests_completed",
            "requests_scheduled",
            "errors",
            "total_input_tokens",
            "total_output_tokens",
        ):
            _result_count(run_results[field], field=f"merged summary.{field}")
        for field in ("p50_s", "p99_s"):
            _result_number(run_results[field], field=f"merged summary.{field}")
        completed = run_results["requests_completed"]
        errors = run_results["errors"]
        scheduled = run_results["requests_scheduled"]
        if not 0 <= errors <= completed <= scheduled:
            raise ValueError("merged Go replay summary request counts are inconsistent")
        successes = max(completed - errors, 0)
        return {
            "run_index": run_index,
            "duration_s": duration,
            "requests_completed": completed,
            "requests_scheduled": scheduled,
            "successes": successes,
            "errors": errors,
            "rps": completed / duration,
            "success_rps": successes / duration,
            "p50_s": run_results.get("p50_s"),
            "p99_s": run_results.get("p99_s"),
        }

    successful_latencies = [float(item[1]) for item in run_results if item[2]]
    completed = len(run_results)
    successes = len(successful_latencies)
    errors = completed - successes
    return {
        "run_index": run_index,
        "duration_s": duration,
        "requests_completed": completed,
        "requests_scheduled": requests_scheduled,
        "successes": successes,
        "errors": errors,
        "rps": completed / duration,
        "success_rps": successes / duration,
        "p50_s": (_percentile(successful_latencies, 0.50) if successful_latencies else None),
        "p99_s": (_percentile(successful_latencies, 0.99) if successful_latencies else None),
    }


def _trace_path(exp_config: EvalManifest) -> str:
    trace_cfg = exp_config.job_trace_config
    return str(trace_cfg.output_trace_path)


def _result_dir(exp_config: EvalManifest, result_subdir: str | None = None) -> str:
    base = exp_config.pbs_result_dir
    subdir = (result_subdir or "").strip()
    return os.path.join(base, subdir) if (base and subdir) else base


def _local_addresses() -> set[str]:
    """Every address this host answers to: hostnames plus bound interface IPs."""
    import socket

    names: set[str] = set()
    for name in (socket.gethostname(), socket.getfqdn()):
        if name:
            names.add(name)
            names.add(name.split(".")[0])
    try:
        from exaserve.control.finite_process import run_finite

        completed = run_finite(["ip", "-o", "-4", "addr", "show"], timeout_s=20)
        if completed.returncode != 0:
            raise OSError(completed.stderr.strip() or "ip address query failed")
        out = completed.stdout
        for line in out.splitlines():
            fields = line.split()
            if "inet" in fields:
                names.add(fields[fields.index("inet") + 1].split("/")[0])
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[replay_engine] interface enumeration unavailable: {exc}", flush=True)
    for name in list(names):
        try:
            names.update(socket.gethostbyname_ex(name)[2])
        except OSError as exc:
            print(f"[replay_engine] address resolution failed for {name!r}: {exc}", flush=True)
    return {n for n in names if n}


def _url_host(url: str) -> str:
    import urllib.parse

    host = urllib.parse.urlsplit(url).hostname or ""
    return host


def _apply_direct_topology(
    base_urls: list[str],
    rank: int,
    mpi_size: int = 0,
    *,
    topology: str,
    pair_shift: int,
) -> list[str]:
    """Narrow a direct-mode rank's target list per the immutable replay policy.

    `local` (THE DEFAULT) pins the rank to its own node: that is what direct
    dispatch means -- no routing layer and no cross-node hop. `mesh` leaves the
    list alone so the rank hash-routes across every node, which makes all but
    1/N of its traffic remote and every server node field streams from N
    distinct peers; that is a different experiment and must be asked for.
    `paired` pins the rank to exactly one *remote* node (one peer, still
    remote), which separates crossing the fabric from per-node peer fan-out.
    """
    if topology == "mesh":
        return base_urls
    targets_by_host: dict[str, list[str]] = {}
    for url in base_urls:
        targets_by_host.setdefault(_url_host(url), []).append(url)
    target_hosts = list(targets_by_host)
    if topology in ("local", "paired") and mpi_size and mpi_size != len(target_hosts):
        raise RuntimeError(
            f"direct topology {topology} pins each rank to one node, but there "
            f"are {mpi_size} client ranks for {len(target_hosts)} target nodes; "
            f"{abs(len(target_hosts) - mpi_size)} node(s) would be mis-loaded. "
            "Set client.num_nodes == deployment.num_nodes."
        )
    if topology not in ("local", "paired"):
        raise RuntimeError(f"direct topology {topology!r} is not one of mesh/local/paired")
    local = _local_addresses()
    my_index = next((idx for idx, host in enumerate(target_hosts) if host in local), None)
    if my_index is None:
        raise RuntimeError(
            f"rank {rank}: direct topology {topology} could not match this host "
            f"({sorted(local)[:6]}...) against any of the {len(target_hosts)} target hosts"
        )
    if topology == "local":
        chosen = targets_by_host[target_hosts[my_index]]
    else:
        if len(target_hosts) < 2:
            raise RuntimeError("direct topology paired needs at least 2 nodes")
        shift = pair_shift % len(target_hosts) or 1
        chosen = targets_by_host[target_hosts[(my_index + shift) % len(target_hosts)]]
    print(
        f"[replay] rank {rank}: topology={topology} node_index={my_index}/{len(target_hosts)} "
        f"-> {len(chosen)} target(s) on {', '.join(chosen)}",
        flush=True,
    )
    return chosen


def _find_go_binary() -> str | None:
    script_dir = pathlib.Path(__file__).resolve().parent.parent
    candidates = [
        script_dir / "go_client" / "bin" / "go_dispatch",
        script_dir.parent / "eval" / "go_client" / "bin" / "go_dispatch",
    ]
    for path in candidates:
        if not (path.is_file() and os.access(path, os.X_OK)):
            continue
        source_dir = path.parent.parent
        sources = [item for item in source_dir.glob("*.go") if not item.name.endswith("_test.go")]
        sources.extend((source_dir / "go.mod", source_dir / "Makefile"))
        binary_mtime = path.stat().st_mtime_ns
        if any(item.is_file() and item.stat().st_mtime_ns > binary_mtime for item in sources):
            continue
        return str(path.resolve())
    return None


def _direct_health_paths(exp_config: EvalManifest) -> list[str]:
    # Direct target discovery is restricted to one model and returns either a
    # root application or an already-routed /<model>_rN base URL.
    if len(exp_config.deployment_plan.models) != 1:
        raise RuntimeError("direct replay requires exactly one canonical model")
    return ["/health"]


def _probe_direct_target(base_url: str, health_paths: list[str], timeout_s: float) -> bool:
    for path in health_paths:
        try:
            with urllib.request.urlopen(f"{base_url}{path}", timeout=timeout_s) as response:
                if response.status != 200:
                    return False
        except (urllib.error.URLError, OSError, ValueError):
            return False
    return True


def _wait_for_direct_targets(
    base_urls: list[str],
    health_paths: list[str],
    timeout_s: float,
    probe_timeout_s: float,
    interval_s: float,
    max_workers: int,
) -> None:
    pending = list(dict.fromkeys(base_urls))
    if not pending:
        return

    total = len(pending)
    deadline = time.monotonic() + timeout_s
    attempt = 0
    print(
        f"[replay_engine] Waiting for {total} direct target(s) to pass "
        f"health checks on {', '.join(health_paths)}",
        flush=True,
    )

    while pending and time.monotonic() < deadline:
        attempt += 1
        ready = []
        worker_count = max(1, min(max_workers, len(pending)))
        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            futures = {
                pool.submit(_probe_direct_target, base_url, health_paths, probe_timeout_s): base_url
                for base_url in pending
            }
            for future in concurrent.futures.as_completed(futures):
                base_url = futures[future]
                try:
                    if future.result():
                        ready.append(base_url)
                except Exception as exc:
                    # Connection failures are represented by the probe's
                    # False result. Anything escaping the probe is a program
                    # or executor failure and must not be disguised as an
                    # ordinary readiness timeout.
                    raise RuntimeError(
                        f"direct target health probe crashed for {base_url}: {exc}"
                    ) from exc
        if ready:
            ready_set = set(ready)
            pending = [base_url for base_url in pending if base_url not in ready_set]

        print(
            f"[replay_engine] Direct target health attempt {attempt}: "
            f"{total - len(pending)}/{total} ready",
            flush=True,
        )
        if pending:
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(interval_s, remaining))

    if pending:
        sample = ", ".join(pending[:5])
        suffix = "" if len(pending) <= 5 else f" ... ({len(pending)} total pending)"
        raise RuntimeError(
            f"Timed out waiting for direct targets to become healthy: {sample}{suffix}"
        )


def _write_trace_partition(requests: list[TraceRequest], path: str) -> None:
    from exaserve.state.atomic import atomic_text_writer

    with atomic_text_writer(path) as handle:
        for request in requests:
            record = {
                "timestamp": request.timestamp,
                "model": request.model,
                "mode": request.mode,
                "prompt": request.prompt,
                "input_len": request.input_len,
                "output_len": request.output_len,
                "tensor_parallel_size": request.tensor_parallel_size,
                "req_id": request.req_id,
            }
            handle.write(json.dumps(record, allow_nan=False) + "\n")


def _spawn_go_procs(
    go_bin: str,
    base_urls: list[str],
    rank_requests: list[TraceRequest],
    generation_mode: str,
    include_tp: bool,
    concurrency: int,
    num_go_workers: int,
    rank: int,
    tmp_dir: str,
    sum_only: bool = False,
    num_go_procs: int = 1,
    warmup_rps: int = 0,
    warmup_duration_s: float = 0.0,
    stream: bool = False,
    request_timeout_s: float = 3600.0,
):
    # When concurrency=0 (auto-derive), the Go client derives from the ephemeral
    # port range. But with multiple Go procs sharing the same port range, each proc
    # must use a fraction to avoid port exhaustion.
    # For multi-proc, divide the total budget across procs so we don't exceed
    # system thread/port limits. Each Go process auto-derives 10240 independently,
    # but 12 × 10240 = 122K goroutines crashes the Go runtime (pthread_create fails).
    if concurrency == 0 and num_go_procs > 1:
        # Divide 80% of ephemeral port range across procs for safety headroom.
        try:
            with open("/proc/sys/net/ipv4/ip_local_port_range") as f:
                lo, hi = map(int, f.read().split())
            total_budget = int((hi - lo + 1) * 0.8)
        except (OSError, ValueError) as exc:
            print(
                f"[replay_engine] could not read ephemeral port range ({exc}); "
                "using the conservative documented fallback",
                flush=True,
            )
            total_budget = 22586  # 28232 * 0.8
        concurrency = max(80, total_budget // num_go_procs)

    request_map = {request.req_id: request for request in rank_requests}
    if len(request_map) != len(rank_requests):
        raise ValueError("rank trace contains duplicate request ids")
    partitions = [
        rank_requests[index :: max(1, num_go_procs)] for index in range(max(1, num_go_procs))
    ]
    processes = []
    handshakes = {}
    result_paths = []
    try:
        for proc_index, partition in enumerate(partitions):
            trace_path = os.path.join(tmp_dir, f"rank{rank}_p{proc_index}_trace.jsonl")
            result_path = os.path.join(tmp_dir, f"rank{rank}_p{proc_index}_results.jsonl")
            result_paths.append(result_path)
            _write_trace_partition(partition, trace_path)
            cmd = [
                go_bin,
                "--base-urls",
                ",".join(base_urls),
                "--generation-mode",
                generation_mode,
                "--timeout",
                str(request_timeout_s),
                "--max-active-requests",
                str(concurrency),
                "--queue-capacity",
                "0",
                "--max-conns-per-host",
                str(concurrency),
                "--num-go-workers",
                str(num_go_workers),
                "--worker-id",
                f"rank{rank}_p{proc_index}",
                "--trace-file",
                trace_path,
                "--result-file",
                result_path,
            ]
            if warmup_rps > 0 and warmup_duration_s > 0:
                cmd.extend(
                    [
                        "--warmup-rps",
                        str(warmup_rps),
                        "--warmup-duration",
                        str(warmup_duration_s),
                    ]
                )
            if sum_only:
                cmd.append("--sum-only")
            if include_tp:
                cmd.append("--include-tp")
            if stream:
                cmd.append("--stream")
            ready_path, ready_token = prepare_ready_handshake(
                tmp_dir, f"go-dispatch-r{rank}-p{proc_index}"
            )
            cmd.extend(ready_handshake_args(ready_path, ready_token))
            # File-backed captures cannot fill a pipe and deadlock a worker while
            # the parent is waiting for every worker to exit. They are private,
            # unlinked temporary files and are consumed only after bounded reap.
            stdout_capture = tempfile.TemporaryFile(mode="w+b")
            stderr_capture = tempfile.TemporaryFile(mode="w+b")
            try:
                process = subprocess.Popen(
                    cmd,
                    stdin=subprocess.PIPE,
                    stdout=stdout_capture,
                    stderr=stderr_capture,
                    start_new_session=True,
                )
            except BaseException:
                stdout_capture.close()
                stderr_capture.close()
                raise
            processes.append(
                {
                    "index": proc_index,
                    "process": process,
                    "stdout": stdout_capture,
                    "stderr": stderr_capture,
                }
            )
            handshakes[proc_index] = (ready_path, ready_token)
        for entry in processes:
            proc_index = entry["index"]
            ready_path, ready_token = handshakes[proc_index]
            wait_ready_handshake(entry["process"], path=ready_path, token=ready_token)
        return processes, result_paths, request_map
    except BaseException as exc:
        cleanup_errors = []
        cleanup_deadline = time.monotonic() + 5.0
        for entry in processes:
            try:
                _stop_replay_process(
                    entry["process"],
                    label=f"go dispatch rank {rank} process {entry['index']}",
                    deadline=cleanup_deadline,
                )
            except Exception as cleanup_exc:
                cleanup_errors.append(str(cleanup_exc))
            finally:
                for stream_name in ("stdout", "stderr"):
                    try:
                        entry[stream_name].close()
                    except OSError as cleanup_exc:
                        cleanup_errors.append(
                            f"process {entry['index']} {stream_name} close failed: {cleanup_exc}"
                        )
        if cleanup_errors:
            add_exception_note(
                exc, "go worker spawn cleanup failures: " + "; ".join(cleanup_errors)
            )
        raise


def _process_group_exists(process: subprocess.Popen[bytes]) -> bool:
    try:
        os.killpg(process.pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _stop_replay_process(
    process: subprocess.Popen[bytes],
    *,
    label: str,
    grace_s: float = 5.0,
    deadline: float | None = None,
) -> None:
    """Stop and reap one process and every descendant in its owned session."""
    if (
        isinstance(grace_s, bool)
        or not isinstance(grace_s, (int, float))
        or not math.isfinite(float(grace_s))
        or grace_s <= 0
    ):
        raise ValueError("replay cleanup grace must be finite and positive")
    if deadline is not None and (
        isinstance(deadline, bool)
        or not isinstance(deadline, (int, float))
        or not math.isfinite(float(deadline))
        or deadline < 0
    ):
        raise ValueError("replay cleanup deadline must be finite and nonnegative")
    deadline = float(deadline) if deadline is not None else time.monotonic() + float(grace_s)
    if process.stdin is not None and not process.stdin.closed:
        process.stdin.close()

    if _process_group_exists(process):
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    term_deadline = time.monotonic() + max(0.0, deadline - time.monotonic()) / 2.0
    while _process_group_exists(process) and time.monotonic() < term_deadline:
        process.poll()
        time.sleep(min(0.02, max(0.0, term_deadline - time.monotonic())))

    if _process_group_exists(process):
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    while _process_group_exists(process) and time.monotonic() < deadline:
        process.poll()
        time.sleep(min(0.02, max(0.0, deadline - time.monotonic())))

    if process.poll() is None:
        try:
            process.wait(timeout=max(0.0, deadline - time.monotonic()))
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError(f"{label} did not exit after SIGKILL") from exc
    if _process_group_exists(process):
        raise RuntimeError(f"{label} left descendants after SIGKILL")


def _print_process_capture(capture, *, prefix: str) -> None:
    capture.flush()
    capture.seek(0)
    for raw_line in capture:
        line = raw_line.decode(errors="replace").rstrip("\n")
        print(f"{prefix} {line}", flush=True)


def _result_number(value, *, field: str, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise ValueError(f"Go replay {field} must be a finite nonnegative number")


def _result_count(value, *, field: str, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if type(value) is not int or value < 0:
        raise ValueError(f"Go replay {field} must be a nonnegative integer")


def _validate_gather_measurements(value: list, *, index: int) -> list:
    if not isinstance(value[1], bool):
        raise ValueError(f"gather record {index} success must be boolean")
    if not isinstance(value[2], str):
        raise ValueError(f"gather record {index} error must be text")
    for position, name in ((0, "latency"), (3, "end_time")):
        _result_number(value[position], field=f"gather[{index}].{name}")
    for position, name in (
        (4, "actual_prompt_tokens"),
        (5, "actual_completion_tokens"),
        (11, "decode_tokens"),
    ):
        _result_count(value[position], field=f"gather[{index}].{name}", nullable=True)
    for position, name in (
        (6, "ttft_s"),
        (7, "first_token_at"),
        (8, "tbt_p50_s"),
        (9, "tbt_p99_s"),
        (10, "tbt_max_s"),
    ):
        _result_number(value[position], field=f"gather[{index}].{name}", nullable=True)
    return value


def _read_go_results(result_path: str, request_map: dict[str, TraceRequest]):
    results = []
    stream = read_go_result_stream(result_path)
    terminal = stream.terminal
    last_fire_time = terminal["last_request_start_at"]
    adjusted_run_t0 = terminal["adjusted_run_t0"]
    if stream.sum_only:
        return terminal, last_fire_time, adjusted_run_t0
    for result in stream.records:
        request = request_map.get(result["req_id"])
        if request is None:
            raise ValueError(f"Go replay result references unknown request id {result['req_id']!r}")
        results.append(
            (
                request,
                result["latency"],
                result["success"],
                result["error"],
                result["end_time"],
                result["actual_prompt_tokens"],
                result["actual_completion_tokens"],
                result.get("ttft_s"),
                result.get("first_token_at"),
                result.get("tbt_p50_s"),
                result.get("tbt_p99_s"),
                result.get("tbt_max_s"),
                result.get("decode_tokens"),
            )
        )
    return results, last_fire_time, adjusted_run_t0


def _send_run_t0_and_wait(
    processes,
    run_t0: float,
    result_paths: list[str],
    request_map: dict[str, TraceRequest],
    interrupt_event,
    rank: int,
    sum_only: bool = False,
    drain_wait_timeout_s: float = 3780.0,
):
    if (
        isinstance(drain_wait_timeout_s, bool)
        or not isinstance(drain_wait_timeout_s, (int, float))
        or not math.isfinite(float(drain_wait_timeout_s))
        or drain_wait_timeout_s <= 0
    ):
        raise ValueError("go dispatch drain deadline must be finite and positive")
    alive = {entry["index"] for entry in processes}
    cleanup_errors = []
    cleanup_deadline = None

    def shared_cleanup_deadline() -> float:
        nonlocal cleanup_deadline
        if cleanup_deadline is None:
            cleanup_deadline = time.monotonic() + 5.0
        return cleanup_deadline

    try:
        for entry in processes:
            process = entry["process"]
            if process.stdin is None:
                raise RuntimeError(f"go process {entry['index']} has no control stdin")
            process.stdin.write(f"{run_t0!r}\n".encode())
            process.stdin.flush()
            process.stdin.close()

        drain_deadline = time.monotonic() + drain_wait_timeout_s
        while alive:
            for entry in processes:
                proc_index = entry["index"]
                if proc_index in alive and entry["process"].poll() is not None:
                    alive.discard(proc_index)
            if not alive:
                break
            if interrupt_event is not None and interrupt_event.is_set():
                for entry in processes:
                    if entry["index"] in alive:
                        _stop_replay_process(
                            entry["process"],
                            label=f"interrupted go dispatch rank {rank} process {entry['index']}",
                            deadline=shared_cleanup_deadline(),
                        )
                alive.clear()
                break
            if time.monotonic() > drain_deadline:
                timed_out = sorted(alive)
                for entry in processes:
                    if entry["index"] in alive:
                        _stop_replay_process(
                            entry["process"],
                            label=f"timed-out go dispatch rank {rank} process {entry['index']}",
                            deadline=shared_cleanup_deadline(),
                        )
                alive.clear()
                raise RuntimeError(
                    f"go dispatch rank {rank} exceeded its {drain_wait_timeout_s:.0f}s "
                    f"drain deadline; stopped processes {timed_out}"
                )
            time.sleep(min(0.1, max(0.0, drain_deadline - time.monotonic())))

        all_results = []
        seen_request_ids: set[str] = set()
        max_last_fire_time = 0.0
        for entry in processes:
            proc_index = entry["index"]
            process = entry["process"]
            _stop_replay_process(
                process,
                label=f"completed go dispatch rank {rank} process {proc_index}",
                deadline=shared_cleanup_deadline(),
            )
            _print_process_capture(
                entry["stdout"], prefix=f"[go_dispatch rank {rank} p{proc_index} stdout]"
            )
            _print_process_capture(
                entry["stderr"], prefix=f"[go_dispatch rank {rank} p{proc_index} stderr]"
            )
            if process.returncode != 0 and not (
                interrupt_event is not None and interrupt_event.is_set()
            ):
                raise RuntimeError(
                    f"go dispatch rank {rank} process {proc_index} exited with "
                    f"status {process.returncode}"
                )
            parsed, last_fire_time, _adjusted_run_t0 = _read_go_results(
                result_paths[proc_index],
                request_map,
            )
            max_last_fire_time = max(max_last_fire_time, last_fire_time)
            if sum_only != isinstance(parsed, dict):
                expected = "summary" if sum_only else "per-request"
                raise ValueError(
                    f"go dispatch process {proc_index} returned the wrong result mode; "
                    f"expected {expected} output"
                )
            if sum_only:
                if not all_results:
                    all_results = parsed
                else:
                    for key in (
                        "requests_completed",
                        "requests_scheduled",
                        "errors",
                        "total_input_tokens",
                        "total_output_tokens",
                    ):
                        all_results[key] = all_results.get(key, 0) + parsed.get(key, 0)
                    all_results["p50_s"] = max(
                        all_results.get("p50_s", 0.0), parsed.get("p50_s", 0.0)
                    )
                    all_results["p99_s"] = max(
                        all_results.get("p99_s", 0.0), parsed.get("p99_s", 0.0)
                    )
            else:
                duplicate_ids = sorted(
                    item[0].req_id for item in parsed if item[0].req_id in seen_request_ids
                )
                if duplicate_ids:
                    raise ValueError(
                        "go dispatch processes returned duplicate request ids: "
                        f"{duplicate_ids[:10]}"
                    )
                seen_request_ids.update(item[0].req_id for item in parsed)
                all_results.extend(parsed)
        if sum_only and isinstance(all_results, dict):
            all_results["last_fire_time"] = max_last_fire_time
        return all_results, max_last_fire_time, run_t0
    finally:
        for entry in processes:
            try:
                _stop_replay_process(
                    entry["process"],
                    label=f"go dispatch rank {rank} process {entry['index']}",
                    deadline=shared_cleanup_deadline(),
                )
            except Exception as exc:
                cleanup_errors.append(str(exc))
            finally:
                for stream_name in ("stdout", "stderr"):
                    try:
                        entry[stream_name].close()
                    except OSError as exc:
                        cleanup_errors.append(
                            f"process {entry['index']} {stream_name} close failed: {exc}"
                        )
        if cleanup_errors:
            active_error = sys.exc_info()[1]
            message = "go dispatch cleanup failures: " + "; ".join(cleanup_errors)
            if active_error is not None:
                add_exception_note(active_error, message)
            else:
                raise RuntimeError(message)


def _next_result_path(result_dir: str) -> str:
    """Claim the sole immutable result identity for one materialized run.

    Re-execution must use a newly materialized run group. Retaining resultN
    generations forces consumers to guess which attempt is authoritative and
    previously led to newest-file selection bugs.
    """
    os.makedirs(result_dir, exist_ok=True)
    candidates = sorted(
        os.path.basename(path) for path in glob.glob(os.path.join(result_dir, "result*.json"))
    )
    if candidates:
        raise FileExistsError(
            "result identity already exists; materialize a new run instead of "
            f"creating another result generation: {candidates}"
        )
    return os.path.join(result_dir, "result0.json")


def _get_cluster_nodes() -> list[str]:
    nodefile = os.environ.get("PBS_NODEFILE")
    if not nodefile:
        raise RuntimeError("PBS_NODEFILE is required for direct mode without base_urls")
    from exaserve.state.atomic import regular_file_reader

    with regular_file_reader(nodefile) as handle:
        nodes = []
        for line in handle:
            node = line.strip()
            if node and node not in nodes:
                nodes.append(node)
    if not nodes:
        raise RuntimeError("PBS_NODEFILE did not contain any hostnames")
    return nodes


def _port_from_manifest(exp_config: EvalManifest) -> int:
    plan = exp_config.deployment_plan
    return plan.gateway.port if plan.gateway is not None else plan.exposure.serve_port


def _validate_base_urls(exp_config: EvalManifest, base_urls: list[str]) -> list[str]:
    """Validate allocation-bound endpoints before MPI or network activity."""
    if not base_urls:
        raise ValueError("base URL discovery produced no targets")
    if len(set(base_urls)) != len(base_urls):
        raise ValueError("base URL discovery produced duplicate targets")
    expected_port = _port_from_manifest(exp_config)
    for index, url in enumerate(base_urls):
        try:
            parsed = urllib.parse.urlsplit(url)
            port = parsed.port
        except ValueError as exc:
            raise ValueError(f"base_urls[{index}] is malformed: {exc}") from exc
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                f"base_urls[{index}] must be an HTTP(S) target without credentials, "
                "query, or fragment"
            )
        if port != expected_port:
            raise ValueError(
                f"base_urls[{index}] port {port!r} disagrees with canonical port {expected_port}"
            )
    replay = exp_config.job_replay_client_config
    plan = exp_config.deployment_plan
    if replay.dest == "proxy" and len(base_urls) != 1:
        raise ValueError("proxy replay requires exactly one discovered gateway endpoint")
    if replay.dest == "direct":
        model = plan.models[0]
        expected_targets = model.num_replicas if model.num_replicas > 1 else plan.num_nodes
        if len(base_urls) != expected_targets:
            raise ValueError(
                f"direct replay expected {expected_targets} canonical targets, "
                f"received {len(base_urls)}"
            )
    return base_urls


def _trace_shard_dir(trace_path: str, mpi_size: int, trace_hash: str) -> str:
    if not isinstance(trace_hash, str) or not re.fullmatch(r"[0-9a-f]{64}", trace_hash):
        raise ValueError("trace shard identity requires a lowercase SHA-256")
    return os.path.join(os.path.dirname(trace_path), f"shards_{trace_hash[:16]}_n{mpi_size}")


def _load_trace_shard_manifest(
    shard_dir: str, *, trace_hash: str, mpi_size: int
) -> dict[str, object]:
    from exaserve.state.atomic import strict_json_load_path

    marker_path = pathlib.Path(shard_dir) / "_COMPLETE"
    try:
        marker = strict_json_load_path(marker_path)
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"trace shard manifest is invalid: {exc}") from exc
    expected_keys = {
        "schema_version",
        "trace_sha256",
        "mpi_size",
        "total",
        "last_timestamp",
        "shards",
    }
    if not isinstance(marker, dict) or set(marker) != expected_keys:
        raise RuntimeError("trace shard manifest has an unexpected shape")
    if (
        type(marker["schema_version"]) is not int
        or marker["schema_version"] != 1
        or marker["trace_sha256"] != trace_hash
    ):
        raise RuntimeError("trace shard manifest has the wrong trace identity")
    if isinstance(marker["mpi_size"], bool) or marker["mpi_size"] != mpi_size:
        raise RuntimeError("trace shard manifest has the wrong MPI size")
    if (
        isinstance(marker["total"], bool)
        or not isinstance(marker["total"], int)
        or marker["total"] < 0
        or isinstance(marker["last_timestamp"], bool)
        or not isinstance(marker["last_timestamp"], (int, float))
        or not math.isfinite(float(marker["last_timestamp"]))
        or marker["last_timestamp"] < 0
    ):
        raise RuntimeError("trace shard manifest has invalid totals")
    shards = marker["shards"]
    if not isinstance(shards, list) or len(shards) != mpi_size:
        raise RuntimeError("trace shard manifest has the wrong shard cardinality")
    total = 0
    expected_names = {f"rank{rank}.jsonl" for rank in range(mpi_size)}
    observed_names: set[str] = set()
    for index, shard in enumerate(shards):
        if not isinstance(shard, dict) or set(shard) != {"name", "sha256", "requests"}:
            raise RuntimeError(f"trace shard manifest entry {index} has an unexpected shape")
        name = shard["name"]
        digest = shard["sha256"]
        requests = shard["requests"]
        if not isinstance(name, str) or name not in expected_names or name in observed_names:
            raise RuntimeError(f"trace shard manifest entry {index} has an invalid name")
        if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
            raise RuntimeError(f"trace shard manifest entry {index} has an invalid checksum")
        if isinstance(requests, bool) or not isinstance(requests, int) or requests < 0:
            raise RuntimeError(f"trace shard manifest entry {index} has an invalid count")
        shard_path = pathlib.Path(shard_dir) / name
        if shard_path.is_symlink() or not shard_path.is_file():
            raise RuntimeError(f"trace shard is missing or unsafe: {shard_path}")
        observed_names.add(name)
        total += requests
    if observed_names != expected_names or total != marker["total"]:
        raise RuntimeError("trace shard manifest is incomplete")
    actual_files = {
        path.name for path in pathlib.Path(shard_dir).iterdir() if path.name != "_COMPLETE"
    }
    if actual_files != expected_names:
        raise RuntimeError("trace shard directory contains undeclared files")
    return marker


def _stage_trace_shards(trace_path: str, mpi_size: int, trace_hash: str) -> str:
    """Split the trace into per-rank shards once, next to the trace itself.

    Without this every rank streams and JSON-parses the WHOLE trace and then
    keeps requests[rank::mpi_size] -- at 256 nodes that is 256 x 790 MB off one
    Lustre file (~200 GB) plus 256x redundant parsing of 1.7 M records. The
    shards preserve the rank::mpi_size partition exactly, are content-addressed
    with the trace, and are reused by every later run at the same rank count.

    Returns the shard directory. Safe under concurrent jobs: build into a
    private temp dir, then rename into place; a loser just discards its copy.
    Failure is fatal because a whole-trace-per-rank fallback causes an avoidable
    shared-filesystem storm at production scale.
    """
    if isinstance(mpi_size, bool) or not isinstance(mpi_size, int) or mpi_size < 2:
        raise ValueError("trace sharding requires an integer MPI size >= 2")
    shard_dir = _trace_shard_dir(trace_path, mpi_size, trace_hash)
    done_marker = os.path.join(shard_dir, "_COMPLETE")
    if os.path.isfile(done_marker):
        _load_trace_shard_manifest(shard_dir, trace_hash=trace_hash, mpi_size=mpi_size)
        return shard_dir
    tmp_dir = tempfile.mkdtemp(prefix=f".shards_n{mpi_size}_", dir=os.path.dirname(trace_path))
    try:
        from exaserve.state.atomic import atomic_write_json, regular_file_reader, strict_json_loads

        shard_digests = [hashlib.sha256() for _ in range(mpi_size)]
        shard_counts = [0] * mpi_size
        source_digest = hashlib.sha256()
        with ExitStack() as stack:
            handles = [
                stack.enter_context(open(os.path.join(tmp_dir, f"rank{idx}.jsonl"), "xb"))
                for idx in range(mpi_size)
            ]
            index = 0
            last_line = b""
            with regular_file_reader(trace_path, binary=True) as source:
                for line in source:
                    source_digest.update(line)
                    # Cheap prefilter: only the metadata line carries __type__,
                    # so avoid json.loads on the ~millions of request lines.
                    if (
                        b'"__type__"' in line
                        and strict_json_loads(line.decode("utf-8")).get("__type__") == "metadata"
                    ):
                        continue
                    rank = index % mpi_size
                    handles[rank].write(line)
                    shard_digests[rank].update(line)
                    shard_counts[rank] += 1
                    last_line = line
                    index += 1
            for handle in handles:
                handle.flush()
                os.fsync(handle.fileno())
        observed_trace_hash = source_digest.hexdigest()
        if observed_trace_hash != trace_hash:
            raise RuntimeError(
                "trace changed between manifest validation and shard staging: "
                f"expected {trace_hash}, observed {observed_trace_hash}"
            )
        # The trace is emitted in arrival order, so the last request carries the
        # trace span the dispatch-overhead diagnostic compares against.
        last_timestamp = (
            float(strict_json_loads(last_line.decode("utf-8"))["timestamp"]) if last_line else 0.0
        )
        marker = {
            "schema_version": 1,
            "trace_sha256": trace_hash,
            "mpi_size": mpi_size,
            "total": index,
            "last_timestamp": last_timestamp,
            "shards": [
                {
                    "name": f"rank{rank}.jsonl",
                    "sha256": shard_digests[rank].hexdigest(),
                    "requests": shard_counts[rank],
                }
                for rank in range(mpi_size)
            ],
        }
        atomic_write_json(os.path.join(tmp_dir, "_COMPLETE"), marker)
        directory_fd = os.open(tmp_dir, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        published = False
        try:
            os.rename(tmp_dir, shard_dir)
            published = True
        except OSError:
            # Another job staged it first: theirs is equivalent, drop ours.
            if not os.path.isfile(done_marker):
                raise
            shutil.rmtree(tmp_dir)
        if published:
            parent_fd = os.open(
                os.path.dirname(shard_dir), os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            )
            try:
                os.fsync(parent_fd)
            finally:
                os.close(parent_fd)
        _load_trace_shard_manifest(shard_dir, trace_hash=trace_hash, mpi_size=mpi_size)
        return shard_dir
    except BaseException as exc:
        try:
            shutil.rmtree(tmp_dir)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            add_exception_note(exc, f"trace shard staging cleanup also failed: {cleanup_exc}")
        raise


def _load_trace_requests(
    trace_path: str, *, expected_hash: str | None = None
) -> list[TraceRequest]:
    from exaserve.state.atomic import regular_file_reader, strict_json_loads

    requests = []
    digest = hashlib.sha256() if expected_hash is not None else None
    parse_error: BaseException | None = None
    with regular_file_reader(trace_path, binary=True) as handle:
        for line in handle:
            if digest is not None:
                digest.update(line)
            if parse_error is not None:
                continue
            try:
                data = strict_json_loads(line.decode("utf-8"))
                if not isinstance(data, dict):
                    raise ValueError("trace row must be a JSON object")
                if data.get("__type__") == "metadata":
                    continue
                required = {"timestamp", "model", "prompt", "output_len"}
                allowed = required | {"input_len", "tensor_parallel_size", "mode"}
                if not required <= set(data) or not set(data) <= allowed:
                    raise ValueError(
                        "trace request row has invalid fields: "
                        f"missing={sorted(required - set(data))}, "
                        f"unknown={sorted(set(data) - allowed)}"
                    )
                requests.append(
                    TraceRequest(
                        timestamp=data["timestamp"],
                        model=data["model"],
                        prompt=data["prompt"],
                        input_len=data.get("input_len", 0),
                        output_len=data["output_len"],
                        tensor_parallel_size=data.get("tensor_parallel_size", 1),
                        req_id=uuid.uuid4().hex,
                        mode=data.get("mode", "chat"),
                    )
                )
            except (KeyError, TypeError, UnicodeError, ValueError) as exc:
                if digest is None:
                    raise
                parse_error = exc
    if digest is not None and digest.hexdigest() != expected_hash:
        raise RuntimeError(
            f"trace shard checksum mismatch for {trace_path}: "
            f"expected {expected_hash}, observed {digest.hexdigest()}"
        )
    if parse_error is not None:
        raise parse_error
    return requests


def _resolve_saturation_request_shape(
    exp_config: EvalManifest, sat_cfg: dict
) -> dict[str, int | str]:
    deployment_models = exp_config.deployment_plan.models
    trace_cfg = exp_config.job_trace_config
    return {
        "model": str(
            sat_cfg.get("model")
            or (deployment_models[0].model_id if deployment_models else "stub-model")
        ),
        # Saturation uses a synthetic prompt, so use the configured workload input/output
        # lengths as the closest available request-shape proxy.
        "prompt_words": int(getattr(trace_cfg, "input_len", 0) or 32),
        "output_tokens": int(getattr(trace_cfg, "output_len", 0) or 16),
    }


def _build_sat_go_cmd(
    go_bin, base_urls, replay_cfg, sat_cfg, exp_config, mode, output_path, target_rate=None
):
    """Build the Go client command for saturation or saturation-step mode."""
    sat_shape = _resolve_saturation_request_shape(exp_config, sat_cfg)
    cmd = [
        go_bin,
        "--mode",
        mode,
        "--base-urls",
        ",".join(base_urls),
        "--max-active-requests",
        str(replay_cfg.go_concurrency),
        "--num-go-workers",
        str(replay_cfg.num_go_workers),
        "--timeout",
        str(replay_cfg.request_timeout_s),
        "--sat-model",
        str(sat_shape["model"]),
        "--sat-prompt-words",
        str(sat_shape["prompt_words"]),
        "--sat-output-tokens",
        str(sat_shape["output_tokens"]),
        "--sat-search-mode",
        str(sat_cfg.get("search_mode", "binary")),
        "--sat-initial-rate",
        str(sat_cfg.get("initial_rate", 100)),
        "--sat-max-rate",
        str(sat_cfg.get("max_rate", 0)),
        "--sat-step-duration",
        str(sat_cfg.get("step_duration_s", 10.0)),
        "--sat-warmup-duration",
        str(sat_cfg.get("warmup_duration_s", 3.0)),
        "--sat-cooldown-pause",
        str(sat_cfg.get("cooldown_pause_s", 2.0)),
        "--sat-tolerance",
        str(sat_cfg.get("tolerance", 0.05)),
        "--sat-max-error-rate",
        str(sat_cfg.get("max_error_rate", 0.01)),
        "--sat-plateau-ratio",
        str(sat_cfg.get("plateau_ratio", 0.95)),
        "--sat-output",
        str(output_path),
    ]
    if sat_cfg.get("verify") is False:
        cmd.append("--sat-verify=false")
    if sat_cfg.get("stream"):
        cmd.append("--sat-stream")
    max_ttft = float(sat_cfg.get("max_p99_ttft", 0.0))
    if max_ttft > 0:
        cmd.extend(["--sat-max-p99-ttft", str(max_ttft)])
    if mode == "saturation-step" and target_rate is not None:
        cmd.extend(["--sat-target-rate", str(target_rate)])
    # step-up params
    for key, flag in [
        ("step_up_start", "--sat-step-up-start"),
        ("step_up_end", "--sat-step-up-end"),
        ("step_up_increment", "--sat-step-up-increment"),
    ]:
        val = int(sat_cfg.get(key, 0))
        if val > 0:
            cmd.extend([flag, str(val)])
    return cmd


def _run_saturation_from_manifest(
    go_bin, base_urls, replay_cfg, sat_cfg, exp_config, output_path, num_go_procs, go_concurrency
):
    """Run the eval pipeline's explicitly single-process saturation finder."""
    print(f"[replay_engine] Saturation mode: num_go_procs={num_go_procs}", flush=True)

    if num_go_procs != 1:
        raise ValueError(
            "eval saturation requires exactly one Go process; "
            "runtime manifest validation should have rejected this configuration"
        )
    # The Go client handles the entire search autonomously.
    if num_go_procs == 1:
        cmd = _build_sat_go_cmd(
            go_bin, base_urls, replay_cfg, sat_cfg, exp_config, "saturation", output_path
        )
        ready_path, ready_token = prepare_ready_handshake(
            pathlib.Path(output_path).parent, "go-saturation-p0"
        )
        cmd.extend(ready_handshake_args(ready_path, ready_token))
        print(f"[replay_engine] cmd: {' '.join(cmd)}", flush=True)

        # Stream both output channels to a file so neither can back-pressure the
        # child while the parent waits.
        sat_log_path = pathlib.Path(output_path).parent / "saturation_stderr.log"
        with open(sat_log_path, "w", encoding="utf-8") as sat_log:
            proc = subprocess.Popen(
                cmd,
                stdout=sat_log,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                start_new_session=True,
            )
            try:
                wait_ready_handshake(proc, path=ready_path, token=ready_token)

                max_steps = 30
                step_time = (
                    float(sat_cfg.get("step_duration_s", 10))
                    + float(sat_cfg.get("warmup_duration_s", 3))
                    + float(sat_cfg.get("cooldown_pause_s", 2))
                )
                timeout_s = max(max_steps * step_time + 120.0, 300.0)
                try:
                    proc.wait(timeout=timeout_s)
                except subprocess.TimeoutExpired as exc:
                    raise RuntimeError(
                        f"saturation process timed out after {timeout_s:.0f}s — "
                        f"check {sat_log_path}"
                    ) from exc
            finally:
                _stop_replay_process(proc, label="saturation process")

        if proc.returncode != 0:
            print(f"[replay_engine] saturation stderr log: {sat_log_path}", flush=True)
            raise RuntimeError(
                f"saturation process exited with {proc.returncode} — check {sat_log_path}"
            )

        if not pathlib.Path(output_path).exists():
            raise RuntimeError(f"saturation output missing: {output_path}")
        print(f"[replay_engine] Saturation output written to {output_path}", flush=True)

        # Write a minimal result file for compatibility with _validate_replay_results.
        from exaserve.state.atomic import strict_json_load_path
        from eval.lib.saturation import validate_saturation_output

        sat_output = validate_saturation_output(strict_json_load_path(output_path))
        result_path = pathlib.Path(output_path).parent / "result0.json"
        sat_rate = sat_output["saturation_rate"]
        steps = sat_output["steps"]
        # Use best step by achieved rate (not just healthy ones — all may be unhealthy
        # when the server is slow and plateau ratio is never met).
        best = max(steps, key=lambda s: s.get("achieved_rate", 0)) if steps else {}
        completed = best["completed"]
        failed = best["failed"]
        summary = {
            "requests_completed": completed + failed,
            "requests_scheduled": completed + failed,
            "errors": failed,
            "p50_s": best["p50_latency_s"],
            "p99_s": best["p99_latency_s"],
            "total_input_tokens": 0,
            "total_output_tokens": 0,
            "saturation_rate": sat_rate,
            "saturation_mode": sat_output["mode"],
        }
        encoded = json.dumps(
            summary, sort_keys=True, separators=(",", ":"), allow_nan=False
        ).encode()
        gather = {
            "schema_version": 1,
            "expected_ranks": 1,
            "collected_ranks": [0],
            "missing_ranks": [],
            "complete": True,
            "shards": [
                {
                    "rank": 0,
                    "size_bytes": len(encoded),
                    "sha256": hashlib.sha256(encoded).hexdigest(),
                    "transport": "in_memory",
                }
            ],
        }
        payload = {
            "meta": {
                "num_runs": 1,
                "completed_runs": 1,
                "gather": gather,
                "gather_by_run": [gather],
                "saturation": True,
            },
            "per_run": [{"run_index": 0, **summary}],
            "overall": summary,
        }
        from exaserve.state.atomic import atomic_create_json

        atomic_create_json(result_path, payload)
        print(f"[replay_engine] Result summary written to {result_path}", flush=True)


async def replay_from_manifest(
    config_path: str,
    *,
    include_tp_override: bool | None = None,
    early_stop_override: float | None = None,
    num_runs_override: int | None = None,
    dest_override: str | None = None,
    base_urls_override: str | None = None,
    dispatch_topology_override: str | None = None,
    result_subdir: str | None = None,
) -> None:
    exp_config = load_eval_manifest(config_path)
    replay_cfg = exp_config.job_replay_client_config
    if os.path.abspath(config_path) != os.path.abspath(replay_cfg.config_path):
        raise ValueError("replay config path disagrees with immutable manifest config_path")

    for name, override, expected in (
        ("include_tp", include_tp_override, replay_cfg.include_tp),
        ("early_stop", early_stop_override, replay_cfg.early_stop),
        ("num_runs", num_runs_override, replay_cfg.num_runs),
        ("dest", dest_override, replay_cfg.dest),
    ):
        if override is not None and override != expected:
            raise ValueError(
                f"replay override {name}={override!r} disagrees with immutable manifest "
                f"value {expected!r}"
            )
    include_tp = replay_cfg.include_tp
    early_stop = replay_cfg.early_stop
    num_runs = replay_cfg.num_runs
    dest = replay_cfg.dest
    topology = replay_cfg.direct_dispatch
    if dispatch_topology_override is not None:
        if dispatch_topology_override not in replay_cfg.dispatch_topologies:
            raise ValueError(
                "dispatch topology override is not a declared ablation arm: "
                f"{dispatch_topology_override!r}"
            )
        topology = dispatch_topology_override
        if result_subdir != dispatch_topology_override:
            raise ValueError("a topology ablation result_subdir must exactly match its arm")
    elif result_subdir is not None:
        raise ValueError("result_subdir is only valid for a declared topology ablation arm")
    generation_mode = replay_cfg.generation_mode
    num_go_procs = replay_cfg.num_go_procs
    num_go_workers = replay_cfg.num_go_workers
    go_concurrency = replay_cfg.go_concurrency
    warmup_rps = replay_cfg.warmup_rps
    warmup_duration_s = replay_cfg.warmup_duration_s
    sum_only = replay_cfg.sum_only

    trace_path = _trace_path(exp_config)
    port = _port_from_manifest(exp_config)
    if base_urls_override is not None:
        cluster_nodes = []
        base_urls = [item.strip() for item in base_urls_override.split(",") if item.strip()]
    elif dest == "direct":
        cluster_nodes = _get_cluster_nodes()
        base_urls = [f"http://{node}:{port}" for node in cluster_nodes]
    else:
        cluster_nodes = []
        base_urls = [f"http://0.0.0.0:{port}"]
    base_urls = _validate_base_urls(exp_config, base_urls)

    comm, rank, mpi_size = _init_mpi()
    is_root = rank == 0
    gather_attempt_id = _mpi_bcast(comm, uuid.uuid4().hex if is_root else None, root=0)
    if not isinstance(gather_attempt_id, str) or not re.fullmatch(
        r"[0-9a-f]{32}", gather_attempt_id
    ):
        raise RuntimeError("MPI result gather attempt identity is invalid")

    if dest == "direct":
        health_error = None
        if is_root:
            try:
                _wait_for_direct_targets(
                    base_urls,
                    _direct_health_paths(exp_config),
                    timeout_s=replay_cfg.direct_target_ready_timeout_s,
                    probe_timeout_s=replay_cfg.direct_target_probe_timeout_s,
                    interval_s=replay_cfg.direct_target_interval_s,
                    max_workers=replay_cfg.direct_target_max_workers,
                )
            except Exception as exc:
                health_error = str(exc)
        health_error = _mpi_bcast(comm, health_error, root=0)
        if health_error:
            raise RuntimeError(health_error)
        _mpi_barrier(comm)
        # Health-check the whole fleet first, then narrow to this rank's arm.
        base_urls = _apply_direct_topology(
            base_urls,
            rank,
            mpi_size,
            topology=topology,
            pair_shift=replay_cfg.direct_pair_shift,
        )

    go_bin = _find_go_binary()
    if go_bin is None:
        raise RuntimeError(
            "go_dispatch binary is missing or older than its sources; rebuild the "
            "immutable snapshot with `make -C eval/go_client build`"
        )

    # Saturation mode: skip trace loading, run saturation finder instead.
    sat_cfg = getattr(replay_cfg, "saturation", {}) or {}
    if isinstance(sat_cfg, dict) and sat_cfg.get("enabled"):
        if is_root:
            result_dir = (
                pathlib.Path(_result_dir(exp_config, result_subdir))
                if exp_config.pbs_result_dir
                else pathlib.Path(exp_config.pbs_working_dir) / "results"
            )
            result_dir.mkdir(parents=True, exist_ok=True)
            sat_output_path = result_dir / "saturation_output.json"
            _run_saturation_from_manifest(
                go_bin,
                base_urls,
                replay_cfg,
                sat_cfg,
                exp_config,
                sat_output_path,
                num_go_procs,
                go_concurrency,
            )
        _mpi_barrier(comm)
        return

    # Per-rank trace shards: staged once by the root, then every rank reads only
    # its own ~1/N slice instead of the whole multi-hundred-MB trace.
    shard_dir = None
    if mpi_size > 1:
        stage_error = None
        if is_root:
            try:
                shard_dir = _stage_trace_shards(trace_path, mpi_size, exp_config.trace_content_hash)
            except Exception as exc:  # pragma: no cover - defensive
                stage_error = str(exc)
        shard_dir = _mpi_bcast(comm, shard_dir, root=0)
        stage_error = _mpi_bcast(comm, stage_error, root=0)
        if stage_error:
            raise RuntimeError(f"trace staging failed on root: {stage_error}")
        _mpi_barrier(comm)

    if shard_dir:
        marker = _load_trace_shard_manifest(
            shard_dir,
            trace_hash=exp_config.trace_content_hash,
            mpi_size=mpi_size,
        )
        shard_entry = marker["shards"][rank]
        expected_name = f"rank{rank}.jsonl"
        if shard_entry["name"] != expected_name:
            raise RuntimeError("trace shard manifest order disagrees with MPI rank")
        rank_requests = _load_trace_requests(
            os.path.join(shard_dir, expected_name),
            expected_hash=shard_entry["sha256"],
        )
        if len(rank_requests) != shard_entry["requests"]:
            raise RuntimeError("trace shard request count disagrees with manifest")
        total_requests = marker["total"]
        trace_span_s = marker["last_timestamp"]
    else:
        requests = _load_trace_requests(trace_path)
        rank_requests = requests[rank::mpi_size]
        total_requests = len(requests)
        trace_span_s = requests[-1].timestamp if requests else 0.0
    target_responses = int(total_requests * early_stop) if early_stop and early_stop > 0 else None

    interrupted = False
    interrupt_event = threading.Event()

    def signal_handler(_sig, _frame):
        nonlocal interrupted
        interrupted = True
        interrupt_event.set()

    old_handler = signal.signal(signal.SIGINT, signal_handler)
    tmp_dir = tempfile.mkdtemp(prefix=f"replay_rank{rank}_")
    all_runs_results = []
    run_durations = []
    dispatch_timings = []
    gather_by_run = []
    t0 = time.time()

    try:
        loop = asyncio.get_running_loop()
        for run_index in range(num_runs):
            _mpi_barrier(comm)
            if interrupted:
                break

            run_warmup_rps = warmup_rps if run_index == 0 else 0
            run_warmup_duration = warmup_duration_s if run_index == 0 else 0.0
            go_processes, result_paths, request_map = await loop.run_in_executor(
                None,
                _spawn_go_procs,
                go_bin,
                base_urls,
                rank_requests,
                generation_mode,
                include_tp,
                go_concurrency,
                num_go_workers,
                rank,
                tmp_dir,
                sum_only,
                num_go_procs,
                run_warmup_rps,
                run_warmup_duration,
                replay_cfg.stream,
                replay_cfg.request_timeout_s,
            )
            _mpi_barrier(comm)
            run_t0 = _mpi_bcast(comm, time.time() if is_root else None, root=0)
            local_results, last_fire_time, effective_run_t0 = await loop.run_in_executor(
                None,
                _send_run_t0_and_wait,
                go_processes,
                run_t0,
                result_paths,
                request_map,
                interrupt_event,
                rank,
                sum_only,
                replay_cfg.drain_wait_timeout_s,
            )
            if is_root and last_fire_time > 0:
                trace_span = trace_span_s
                actual_dispatch_s = last_fire_time - effective_run_t0
                dispatch_timings.append(
                    {
                        "run_index": run_index,
                        "trace_span_s": trace_span,
                        "actual_dispatch_s": actual_dispatch_s,
                        "overhead_s": actual_dispatch_s - trace_span,
                    }
                )
            # Persist per-rank shards to shared storage and let root read them,
            # rather than a single-root MPI collective over large per-request
            # data (it hung / lost all results at 64 nodes when a node dropped).
            # No-op for proxy mode (mpi_size == 1). See _gather_results_via_shards.
            _shard_base = (
                str(_result_dir(exp_config, result_subdir))
                if exp_config.pbs_result_dir
                else os.path.join(str(exp_config.pbs_working_dir), "results")
            )
            gathered = _gather_results_via_shards(
                comm,
                local_results,
                run_index=run_index,
                shard_dir=os.path.join(_shard_base, "_shards", gather_attempt_id),
                rank=rank,
                mpi_size=mpi_size,
                is_root=is_root,
                timeout_s=replay_cfg.shard_timeout_s,
            )
            if is_root:
                gather_by_run.append(dict(_LAST_GATHER_META))
            if is_root and isinstance(local_results, dict):
                merged = {
                    "requests_completed": 0,
                    "requests_scheduled": 0,
                    "errors": 0,
                    "total_input_tokens": 0,
                    "total_output_tokens": 0,
                    "p50_s": 0.0,
                    "p99_s": 0.0,
                }
                for item in gathered:
                    item = _validate_go_summary(item)
                    for key in (
                        "requests_completed",
                        "requests_scheduled",
                        "errors",
                        "total_input_tokens",
                        "total_output_tokens",
                    ):
                        merged[key] += item[key]
                    merged["p50_s"] = max(merged["p50_s"], item["p50_s"])
                    merged["p99_s"] = max(merged["p99_s"], item["p99_s"])
                run_results = merged
            elif is_root:
                run_results = [item for rank_results in gathered for item in rank_results]
                if target_responses is not None and len(run_results) > target_responses:
                    run_results = run_results[:target_responses]
            else:
                run_results = []
            all_runs_results.append(run_results)
            duration_t0 = effective_run_t0 if effective_run_t0 is not None else run_t0
            if is_root:
                if isinstance(run_results, dict):
                    run_durations.append(max(time.time() - duration_t0, 0.0))
                else:
                    end_time = max((item[4] for item in run_results), default=time.time())
                    run_durations.append(max(end_time - duration_t0, 0.0))
            if run_index < num_runs - 1:
                cooldown_s = 75
                if is_root:
                    print(
                        f"[replay_engine] Cooldown {cooldown_s}s before run "
                        f"{run_index + 2}/{num_runs}...",
                        flush=True,
                    )
                await asyncio.sleep(cooldown_s)
        if is_root:
            _save_results(
                exp_config,
                total_requests,
                all_runs_results[-1] if all_runs_results else [],
                all_runs_results,
                run_durations,
                dispatch_timings,
                num_runs,
                generation_mode,
                dest,
                cluster_nodes,
                mpi_size,
                num_go_procs,
                num_go_workers,
                go_concurrency,
                warmup_rps,
                warmup_duration_s,
                t0,
                gather_by_run,
                result_subdir,
            )
    finally:
        signal.signal(signal.SIGINT, old_handler)
        active_error = sys.exc_info()[1]
        try:
            shutil.rmtree(tmp_dir)
        except FileNotFoundError:
            pass
        except OSError as cleanup_exc:
            if active_error is None:
                raise RuntimeError(
                    f"replay temporary cleanup failed: {cleanup_exc}"
                ) from cleanup_exc
            add_exception_note(active_error, f"replay temporary cleanup also failed: {cleanup_exc}")


def _save_results(
    exp_config: EvalManifest,
    total_requests: int,
    results,
    all_runs_results,
    run_durations,
    dispatch_timings,
    num_runs,
    generation_mode,
    dest,
    cluster_nodes,
    mpi_size,
    num_go_procs,
    num_go_workers,
    go_concurrency,
    warmup_rps,
    warmup_duration_s,
    t0,
    gather_by_run,
    result_subdir,
) -> None:
    result_dir = _result_dir(exp_config, result_subdir)
    final_save_path = _next_result_path(result_dir)
    config_dict = exp_config.to_yaml_dict()
    plan = exp_config.deployment_plan
    config_dict["deployment_plan"] = {
        **plan.canonical(),
        "deployment_plan_hash": plan.deployment_plan_hash,
    }
    per_run = [
        _summarize_run_results(
            run_index,
            run_results,
            total_requests,
            run_durations[run_index] if run_index < len(run_durations) else None,
        )
        for run_index, run_results in enumerate(all_runs_results)
    ]
    meta = {
        "num_runs": num_runs,
        "completed_runs": len(all_runs_results),
        "generation_mode": generation_mode,
        "dest": dest,
        "cluster_nodes": cluster_nodes,
        "mpi_size": mpi_size,
        "num_go_procs": num_go_procs,
        "num_go_workers": num_go_workers,
        "go_concurrency": go_concurrency,
        "warmup_rps": warmup_rps,
        "warmup_duration_s": warmup_duration_s,
        "dispatch_timings": dispatch_timings,
        "gather": gather_by_run[-1] if gather_by_run else None,
        "gather_by_run": gather_by_run,
    }
    duration = run_durations[-1] if run_durations else max(time.time() - t0, 1e-6)
    duration = max(duration, 1e-6)

    if isinstance(results, dict):
        completed_requests = results.get("requests_completed", 0)
        total_input_tokens = results.get("total_input_tokens", 0)
        total_output_tokens = results.get("total_output_tokens", 0)
        payload = {
            "config": config_dict,
            "meta": dict(meta, sum_only=True),
            "per_run": per_run,
            "overall": {
                "duration_s": duration,
                "rps": completed_requests / duration,
                "processed_tps": total_input_tokens / duration,
                "generated_tps": total_output_tokens / duration,
                "tps": (total_input_tokens + total_output_tokens) / duration,
                "total_tokens": total_input_tokens + total_output_tokens,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "requests_completed": completed_requests,
                "requests_scheduled": results.get("requests_scheduled", total_requests),
                "errors": results.get("errors", 0),
                "p50_s": results.get("p50_s", 0.0),
                "p99_s": results.get("p99_s", 0.0),
            },
        }
    else:
        raw_results = []
        model_groups = {}
        total_input_tokens = 0
        total_output_tokens = 0
        usage_count = 0
        trace_count = 0
        successful_latencies = []
        for run_index, run_results in enumerate(all_runs_results):
            for item in run_results:
                (
                    request,
                    latency,
                    success,
                    error_msg,
                    _end_time,
                    actual_prompt_tokens,
                    actual_completion_tokens,
                    ttft_s,
                    first_token_at,
                    tbt_p50_s,
                    tbt_p99_s,
                    tbt_max_s,
                    decode_tokens,
                ) = item
                raw_results.append(
                    {
                        "run_index": run_index,
                        "model": request.model,
                        "latency": latency,
                        "success": success,
                        "error": error_msg,
                        "input_len": request.input_len,
                        "output_len": request.output_len,
                        "actual_prompt_tokens": actual_prompt_tokens,
                        "actual_completion_tokens": actual_completion_tokens,
                        "tensor_parallel_size": request.tensor_parallel_size,
                        "req_id": request.req_id,
                        "ttft_s": ttft_s,
                        "first_token_at": first_token_at,
                        "tbt_p50_s": tbt_p50_s,
                        "tbt_p99_s": tbt_p99_s,
                        "tbt_max_s": tbt_max_s,
                        "decode_tokens": decode_tokens,
                    }
                )
        for item in results:
            (
                request,
                latency,
                success,
                error_msg,
                _end_time,
                actual_prompt_tokens,
                actual_completion_tokens,
                _ttft_s,
                _first_token_at,
                *_,
            ) = item
            model_groups.setdefault(request.model, []).append(item)
            if success:
                successful_latencies.append(float(latency))
                if actual_prompt_tokens is not None and actual_completion_tokens is not None:
                    total_input_tokens += int(actual_prompt_tokens)
                    total_output_tokens += int(actual_completion_tokens)
                    usage_count += 1
                else:
                    total_input_tokens += int(request.input_len)
                    total_output_tokens += int(request.output_len)
                    trace_count += 1

        per_model = {}
        for model_name, model_rows in model_groups.items():
            latencies = [float(item[1]) for item in model_rows if item[2]]
            per_model[model_name] = {
                "count": len(model_rows),
                "errors": len(model_rows) - len(latencies),
                "p50_s": (_percentile(latencies, 0.50) if latencies else None),
                "p99_s": (_percentile(latencies, 0.99) if latencies else None),
            }

        payload = {
            "config": config_dict,
            "meta": dict(
                meta,
                token_counts_from_usage_api=usage_count,
                token_counts_from_trace_spec=trace_count,
            ),
            "per_run": per_run,
            "summary": {model_name: len(rows) for model_name, rows in model_groups.items()},
            "per_model": per_model,
            "overall": {
                "duration_s": duration,
                "rps": len(results) / duration,
                "processed_tps": total_input_tokens / duration,
                "generated_tps": total_output_tokens / duration,
                "tps": (total_input_tokens + total_output_tokens) / duration,
                "total_tokens": total_input_tokens + total_output_tokens,
                "total_input_tokens": total_input_tokens,
                "total_output_tokens": total_output_tokens,
                "requests_completed": len(results),
                "requests_scheduled": total_requests,
                "errors": sum(item["errors"] for item in per_model.values()),
                "p50_s": (
                    _percentile(successful_latencies, 0.50) if successful_latencies else None
                ),
                "p99_s": (
                    _percentile(successful_latencies, 0.99) if successful_latencies else None
                ),
            },
            "requests": raw_results,
        }
    # PR-035: atomic publish — a crash/walltime-kill or concurrent reader
    # sees either no file or the complete result, never a truncated one.
    from exaserve.state.atomic import atomic_create_text

    atomic_create_text(final_save_path, json.dumps(payload, indent=2, allow_nan=False))
    print(f">>> [REPLAY] Saved results to {final_save_path}", flush=True)
