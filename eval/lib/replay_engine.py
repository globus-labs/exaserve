from __future__ import annotations

from array import array
import asyncio
import concurrent.futures
import glob
import hashlib
import json
import math
import os
import pathlib
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

from eval.lib.manifest import EvalManifest, ReplayClientConfig, load_eval_manifest
from exaserve.control.process_handshake import (
    prepare_ready_handshake,
    ready_handshake_args,
    wait_ready_handshake,
)
from exaserve.exception_notes import add_exception_note
from exaserve.go_result_contract import (
    LATENCY_QUANTILE_METHOD,
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
    launcher_sizes = []
    for name in ("PMI_SIZE", "PMIX_SIZE", "OMPI_COMM_WORLD_SIZE", "SLURM_NTASKS"):
        raw = os.environ.get(name, "")
        if raw:
            try:
                launcher_sizes.append(int(raw))
            except ValueError as exc:
                raise RuntimeError(f"launcher {name} is not an integer: {raw!r}") from exc
    launcher_rank_names = (
        "PALS_RANKID",
        "PMI_RANK",
        "PMIX_RANK",
        "OMPI_COMM_WORLD_RANK",
        "SLURM_PROCID",
    )
    launcher_rank_present = any(name in os.environ for name in launcher_rank_names)
    launcher_requires_mpi = launcher_rank_present or any(size > 1 for size in launcher_sizes)

    from importlib import metadata

    try:
        observed_mpi4py = metadata.version("mpi4py")
    except metadata.PackageNotFoundError:
        observed_mpi4py = None
    if observed_mpi4py is None:
        if launcher_requires_mpi:
            raise RuntimeError(
                "multi-rank replay requires mpi4py from the qualified compatibility profile"
            )
        return None, 0, 1

    from exaserve.compat.profile import default_profile

    profile = default_profile(os.environ.get("EXASERVE_VENDOR", "xpu"))
    if observed_mpi4py != profile.mpi4py:
        raise RuntimeError(
            "replay MPI environment does not match the compatibility profile: "
            f"mpi4py profile={profile.mpi4py!r} runtime={observed_mpi4py!r}"
        )
    declared_profile = os.environ.get("EXASERVE_COMPAT_PROFILE_ID", "")
    if declared_profile and declared_profile != profile.profile_id:
        raise RuntimeError("replay MPI compatibility profile identity mismatch")
    try:
        from mpi4py import MPI
    except ImportError as exc:  # pragma: no cover - optional dependency
        if launcher_rank_present or any(size > 1 for size in launcher_sizes):
            raise RuntimeError(
                "multi-rank replay requires mpi4py; refusing independent roots"
            ) from exc
        return None, 0, 1
    comm = MPI.COMM_WORLD
    if comm.Get_size() > 1 and declared_profile != profile.profile_id:
        raise RuntimeError(
            "multi-rank replay requires its exact plan-bound compatibility profile identity"
        )
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


_RESULT_TAG = 27181
_SUMMARY_TAG = 27182
_RESULT_CHUNK_BYTES = 1 << 20
_SUMMARY_BYTES = 1 << 20
_TRACE_BATCH_BYTES = 1 << 20
_TRACE_ROW_BYTES = 16 << 20
_UINT64_MAX = (1 << 64) - 1
_MPI_HEADER_BYTES = 4 << 10
_MPI_FRAME_BYTES = 4 + _MPI_HEADER_BYTES + max(_RESULT_CHUNK_BYTES, _SUMMARY_BYTES)
_CANCELLED_MPI_MESSAGES = []
_FAILED_MPI_COLLECTIVE = None


def _require_mpi_collective_ready() -> None:
    if _FAILED_MPI_COLLECTIVE is not None:
        raise RuntimeError(
            "cannot start MPI collective after an unfinished collective failed; "
            "the replay process must terminate"
        )


def _retain_failed_mpi_collective(request, send_buffer, receive_buffer) -> None:
    global _FAILED_MPI_COLLECTIVE
    # A collective cannot portably be cancelled. Keep its request and arrays
    # alive until supervised process teardown. This single fail-stop slot also
    # prevents a caller from accumulating abandoned collectives by retrying.
    if _FAILED_MPI_COLLECTIVE is None:
        _FAILED_MPI_COLLECTIVE = (request, send_buffer, receive_buffer)


def _encode_mpi_message(message: tuple) -> bytes:
    """Frame the existing result protocol without implicit pickle buffers."""
    if not isinstance(message, tuple) or not message:
        raise ValueError("MPI message must be a protocol tuple")
    kind = message[0]
    header = {"schema_version": 1, "kind": kind}
    if kind in ("data", "summary") and len(message) == 4:
        _, run_index, index, content = message
        if type(content) is not bytes:
            raise ValueError("MPI message payload must be bytes")
        header.update(
            run_index=run_index,
            **{"sequence" if kind == "data" else "rank": index},
        )
    elif kind == "end" and len(message) == 5:
        _, run_index, chunks, records, evidence = message
        header.update(run_index=run_index, chunks=chunks, records=records, evidence=evidence)
        content = b""
    else:
        raise ValueError("MPI message has an unknown kind or invalid shape")
    if len(content) > max(_RESULT_CHUNK_BYTES, _SUMMARY_BYTES):
        raise ValueError("MPI message payload exceeds its byte bound")
    header["payload_bytes"] = len(content)
    encoded = json.dumps(header, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
        "utf-8"
    )
    if not encoded or len(encoded) > _MPI_HEADER_BYTES:
        raise ValueError("MPI message header exceeds its byte bound")
    frame = struct.pack("!I", len(encoded)) + encoded + content
    # Sender and receiver use the same schema checks, including integer types.
    _decode_mpi_message(frame)
    return frame


def _decode_mpi_message(frame: bytes) -> tuple:
    from exaserve.state.atomic import strict_json_loads

    if type(frame) is not bytes or not 4 < len(frame) <= _MPI_FRAME_BYTES:
        raise ValueError("MPI message frame is truncated or exceeds its byte bound")
    header_size = struct.unpack("!I", frame[:4])[0]
    if not 0 < header_size <= _MPI_HEADER_BYTES or 4 + header_size > len(frame):
        raise ValueError("MPI message header is truncated or exceeds its byte bound")
    encoded = frame[4 : 4 + header_size]
    header = strict_json_loads(encoded.decode("utf-8"))
    if not isinstance(header, dict):
        raise ValueError("MPI message header must be an object")
    kind = header.get("kind")
    fields = {"schema_version", "kind", "run_index", "payload_bytes"}
    if kind == "data":
        fields.add("sequence")
        integer_fields = ("run_index", "sequence", "payload_bytes")
    elif kind == "summary":
        fields.add("rank")
        integer_fields = ("run_index", "rank", "payload_bytes")
    elif kind == "end":
        fields.update(("chunks", "records", "evidence"))
        integer_fields = ("run_index", "chunks", "records", "payload_bytes")
    else:
        raise ValueError("MPI message header has an unknown kind")
    if (
        set(header) != fields
        or type(header["schema_version"]) is not int
        or header["schema_version"] != 1
        or any(type(header[field]) is not int or header[field] < 0 for field in integer_fields)
        or json.dumps(header, sort_keys=True, separators=(",", ":"), allow_nan=False).encode(
            "utf-8"
        )
        != encoded
    ):
        raise ValueError("MPI message header has invalid or non-canonical fields")
    content = frame[4 + header_size :]
    if len(content) != header["payload_bytes"]:
        raise ValueError("MPI message payload length disagrees with its header")
    if kind in ("data", "summary"):
        bound = _RESULT_CHUNK_BYTES if kind == "data" else _SUMMARY_BYTES
        if not content or len(content) > bound:
            raise ValueError("MPI message payload is empty or exceeds its byte bound")
        return (
            kind,
            header["run_index"],
            header["sequence" if kind == "data" else "rank"],
            content,
        )
    if content or not isinstance(header["evidence"], dict):
        raise ValueError("MPI terminal must have evidence and no payload")
    return (kind, header["run_index"], header["chunks"], header["records"], header["evidence"])


class _MPIMessageRequest:
    """Own a typed MPI request and its buffer through completion or shutdown."""

    def __init__(self, request, buffer, mpi, *, receiving: bool):
        self.request = request
        self.buffer = buffer
        self.status = mpi.Status()
        self.datatype = mpi.BYTE
        self.receiving = receiving
        self.complete = False
        self.value = None

    def test(self):
        if self.complete:
            return True, self.value
        if not self.request.Test(self.status):
            return False, None
        self.complete = True
        if self.receiving:
            count = self.status.Get_count(self.datatype)
            if type(count) is not int or not 0 <= count <= len(self.buffer):
                raise RuntimeError("MPI message receive count exceeds its explicit byte buffer")
            self.value = _decode_mpi_message(bytes(memoryview(self.buffer)[:count]))
        return True, self.value

    def cancel(self):
        if self.complete:
            return
        # Cancel is only a request, not proof that MPI has stopped using the
        # buffer.  Retain failed transfers until process-group teardown rather
        # than freeing storage still reachable by MPI.  Never block in Wait.
        if self not in _CANCELLED_MPI_MESSAGES:
            _CANCELLED_MPI_MESSAGES.append(self)
        self.request.Cancel()
        if self.request.Test(self.status):
            self.complete = True
            _CANCELLED_MPI_MESSAGES.remove(self)


def _mpi_isend_message(comm, message, *, dest: int, tag: int):
    from mpi4py import MPI

    frame = _encode_mpi_message(message)
    return _MPIMessageRequest(
        comm.Isend([frame, MPI.BYTE], dest=dest, tag=tag), frame, MPI, receiving=False
    )


def _mpi_irecv_message(comm, *, source: int, tag: int):
    from mpi4py import MPI

    buffer = bytearray(_MPI_FRAME_BYTES)
    return _MPIMessageRequest(
        comm.Irecv([buffer, MPI.BYTE], source=source, tag=tag), buffer, MPI, receiving=True
    )


def _request_test(request):
    outcome = request.test()
    if isinstance(outcome, tuple) and len(outcome) == 2:
        return bool(outcome[0]), outcome[1]
    return bool(outcome), None


def _buffer_request_test(request) -> bool:
    """Test a typed-buffer collective without object-request semantics."""

    outcome = request.Test()
    if isinstance(outcome, tuple):
        if not outcome:
            raise RuntimeError("MPI buffer request returned an empty Test result")
        outcome = outcome[0]
    return bool(outcome)


def _cancel_requests(requests) -> None:
    for request in requests:
        cancel = getattr(request, "cancel", None) or getattr(request, "Cancel", None)
        if cancel is not None:
            try:
                cancel()
            except Exception:
                pass


def _wait_request(request, *, deadline: float, label: str):
    try:
        while time.monotonic() < deadline:
            complete, value = _request_test(request)
            if complete:
                return value
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
    except BaseException:
        _cancel_requests([request])
        raise
    _cancel_requests([request])
    raise RuntimeError(f"{label} exceeded its absolute MPI deadline")


def _result_record(item, *, index: int) -> dict:
    if not isinstance(item, (tuple, list)) or len(item) != 13:
        raise ValueError(f"gather result {index} must contain 13 fields")
    request = item[0]
    if not isinstance(request, TraceRequest):
        raise TypeError(f"gather result {index} request is not TraceRequest")
    return {
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


def _iter_result_chunks(results, *, max_chunk_bytes: int = _RESULT_CHUNK_BYTES):
    if isinstance(max_chunk_bytes, bool) or not isinstance(max_chunk_bytes, int):
        raise ValueError("MPI result chunk bound must be an integer")
    if max_chunk_bytes < 1024:
        raise ValueError("MPI result chunk bound must be at least 1024 bytes")
    chunk = bytearray()
    for index, item in enumerate(results):
        encoded = (
            json.dumps(
                _result_record(item, index=index),
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
        if len(encoded) > max_chunk_bytes:
            raise ValueError(
                f"gather result {index} exceeds the {max_chunk_bytes}-byte MPI chunk bound"
            )
        if chunk and len(chunk) + len(encoded) > max_chunk_bytes:
            yield bytes(chunk)
            chunk.clear()
        chunk.extend(encoded)
    if chunk:
        yield bytes(chunk)


def _decode_result_chunk(content: bytes):
    if not isinstance(content, bytes) or len(content) > _RESULT_CHUNK_BYTES:
        raise ValueError("MPI result chunk is missing or exceeds its bound")
    from exaserve.state.atomic import strict_json_loads

    records = []
    for index, line in enumerate(content.splitlines()):
        if not line:
            raise ValueError("MPI result chunk contains an empty row")
        record = strict_json_loads(line.decode("utf-8"))
        records.extend(
            _decode_gather_payload(
                (
                    json.dumps(
                        {"schema_version": 1, "kind": "records", "records": [record]},
                        sort_keys=True,
                        separators=(",", ":"),
                        allow_nan=False,
                    )
                    + "\n"
                ).encode("utf-8")
            )
        )
    return records


def _gather_raw_results_via_mpi(
    comm,
    local_results,
    *,
    run_index,
    rank,
    mpi_size,
    is_root,
    timeout_s=600.0,
):
    """Transfer raw records in bounded, sequenced MPI messages.

    Only rank 0 retains the merged records.  Each sender has at most one
    nonblocking message in flight, and root has at most one posted receive per
    rank.  A missing, malformed, duplicate, or late terminal message rejects
    the whole run; partial request sets are never published.
    """
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s <= 0
    ):
        raise ValueError("MPI result deadline must be finite and positive")
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
        raise ValueError("MPI result rank/run identity is invalid")
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

    deadline = time.monotonic() + timeout_s
    if not is_root:
        digest = hashlib.sha256()
        size_bytes = 0
        chunk_count = 0
        for sequence, content in enumerate(_iter_result_chunks(local_results)):
            request = _mpi_isend_message(
                comm, ("data", run_index, sequence, content), dest=0, tag=_RESULT_TAG
            )
            _wait_request(
                request,
                deadline=deadline,
                label=f"rank {rank} result chunk {sequence}",
            )
            digest.update(content)
            size_bytes += len(content)
            chunk_count += 1
        local_evidence = {
            "rank": rank,
            "size_bytes": size_bytes,
            "sha256": digest.hexdigest(),
            "transport": "mpi_chunked",
        }
        terminal = (
            "end",
            run_index,
            chunk_count,
            len(local_results),
            local_evidence,
        )
        _wait_request(
            _mpi_isend_message(comm, terminal, dest=0, tag=_RESULT_TAG),
            deadline=deadline,
            label=f"rank {rank} result terminal",
        )
        return None

    root_digest = hashlib.sha256()
    root_size = 0
    for content in _iter_result_chunks(local_results):
        root_digest.update(content)
        root_size += len(content)
    local_evidence = {
        "rank": 0,
        "size_bytes": root_size,
        "sha256": root_digest.hexdigest(),
        "transport": "mpi_chunked",
    }
    collected = {0: local_results}
    evidence = {0: local_evidence}
    states = {
        other: {
            "next": 0,
            "records": [],
            "digest": hashlib.sha256(),
            "size": 0,
        }
        for other in range(1, mpi_size)
    }
    pending = {}
    try:
        for other in range(1, mpi_size):
            pending[other] = _mpi_irecv_message(comm, source=other, tag=_RESULT_TAG)
        while pending and time.monotonic() < deadline:
            progressed = False
            for other, request in list(pending.items()):
                complete, message = _request_test(request)
                if not complete:
                    continue
                progressed = True
                if not isinstance(message, tuple) or len(message) < 2:
                    raise RuntimeError(f"rank {other} sent a malformed result message")
                state = states[other]
                if message[0] == "data":
                    if (
                        len(message) != 4
                        or message[1] != run_index
                        or message[2] != state["next"]
                        or not isinstance(message[3], bytes)
                    ):
                        raise RuntimeError(f"rank {other} sent an invalid result chunk")
                    content = message[3]
                    decoded = _decode_result_chunk(content)
                    state["records"].extend(decoded)
                    state["digest"].update(content)
                    state["size"] += len(content)
                    state["next"] += 1
                    pending[other] = _mpi_irecv_message(comm, source=other, tag=_RESULT_TAG)
                elif message[0] == "end":
                    if len(message) != 5 or message[1] != run_index:
                        raise RuntimeError(f"rank {other} sent an invalid result terminal")
                    expected_chunks, expected_records, claimed = message[2:]
                    observed = {
                        "rank": other,
                        "size_bytes": state["size"],
                        "sha256": state["digest"].hexdigest(),
                        "transport": "mpi_chunked",
                    }
                    if (
                        expected_chunks != state["next"]
                        or expected_records != len(state["records"])
                        or claimed != observed
                    ):
                        raise RuntimeError(f"rank {other} result terminal is inconsistent")
                    collected[other] = state["records"]
                    evidence[other] = observed
                    del pending[other]
                else:
                    raise RuntimeError(f"rank {other} sent an unknown result message")
            if pending and not progressed:
                time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
    except BaseException:
        _cancel_requests(pending.values())
        raise
    if pending:
        missing = sorted(pending)
        _cancel_requests(pending.values())
        _LAST_GATHER_META.update(
            {
                "schema_version": 1,
                "expected_ranks": mpi_size,
                "collected_ranks": sorted(collected),
                "missing_ranks": missing,
                "complete": False,
                "shards": [evidence[item] for item in sorted(evidence)],
            }
        )
        raise RuntimeError(
            f"run {run_index} MPI result transfer timed out after {timeout_s:.3f}s; "
            f"missing terminal messages from ranks {missing}"
        )
    _LAST_GATHER_META.update(
        {
            "schema_version": 1,
            "expected_ranks": mpi_size,
            "collected_ranks": list(range(mpi_size)),
            "missing_ranks": [],
            "complete": True,
            "shards": [evidence[item] for item in range(mpi_size)],
        }
    )
    return [collected[item] for item in range(mpi_size)]


def _merge_latency_histograms(histograms: list[dict]) -> dict:
    if not histograms:
        raise ValueError("latency histogram merge requires at least one rank")
    bounds = histograms[0].get("bucket_upper_bounds_s")
    if not isinstance(bounds, list) or len(bounds) < 2 or bounds[-1] != -1.0:
        raise ValueError("latency histogram has no bucket layout")
    merged_counts = [0] * len(bounds)
    total_count = 0
    total_sum = 0.0
    for rank, histogram in enumerate(histograms):
        if not isinstance(histogram, dict) or set(histogram) != {
            "bucket_upper_bounds_s",
            "counts",
            "count",
            "sum_s",
        }:
            raise ValueError(f"rank {rank} latency histogram fields are invalid")
        counts = histogram["counts"]
        if (
            histogram["bucket_upper_bounds_s"] != bounds
            or not isinstance(counts, list)
            or len(counts) != len(bounds)
            or any(type(value) is not int or value < 0 for value in counts)
            or type(histogram["count"]) is not int
            or histogram["count"] < 0
            or sum(counts) != histogram["count"]
            or isinstance(histogram["sum_s"], bool)
            or not isinstance(histogram["sum_s"], (int, float))
            or not math.isfinite(float(histogram["sum_s"]))
            or histogram["sum_s"] < 0
        ):
            raise ValueError(f"rank {rank} latency histogram values are invalid")
        merged_counts = [left + right for left, right in zip(merged_counts, counts)]
        total_count += histogram["count"]
        total_sum += float(histogram["sum_s"])
    return {
        "bucket_upper_bounds_s": list(bounds),
        "counts": merged_counts,
        "count": total_count,
        "sum_s": total_sum,
    }


def _histogram_percentile(histogram: dict, fraction: float) -> float:
    count = histogram["count"]
    if count == 0:
        return 0.0
    threshold = max(1, math.ceil(count * fraction))
    cumulative = 0
    bounds = histogram["bucket_upper_bounds_s"]
    for index, bucket_count in enumerate(histogram["counts"]):
        cumulative += bucket_count
        if cumulative < threshold:
            continue
        upper = float(bounds[index])
        if upper < 0:
            if index == 0:
                raise ValueError("latency histogram overflow has no finite lower bound")
            return float(bounds[index - 1])
        lower = float(bounds[index - 1]) if index else 0.0
        prior = cumulative - bucket_count
        if bucket_count == 0:
            return upper
        position = (threshold - prior) / bucket_count
        return lower + position * (upper - lower)
    raise ValueError("latency histogram count exceeds bucket coverage")


def _merge_go_process_summaries(summaries: list[dict]) -> dict:
    """Compose every same-rank Go process with the global histogram contract."""
    if not summaries:
        raise ValueError("Go process summary merge requires at least one summary")
    validated = [_validate_go_summary(summary) for summary in summaries]
    if any(
        summary.get("latency_quantile_method") != LATENCY_QUANTILE_METHOD
        or not isinstance(summary.get("latency_histogram"), dict)
        for summary in validated
    ):
        raise ValueError("Go process summary lacks the supported latency histogram estimator")
    run_t0s = {summary["adjusted_run_t0"] for summary in validated}
    if len(run_t0s) != 1:
        raise ValueError("Go process summaries disagree on adjusted_run_t0")
    histogram = _merge_latency_histograms([summary["latency_histogram"] for summary in validated])
    merged = {
        "__type__": "summary",
        **{
            field: sum(summary[field] for summary in validated)
            for field in (
                "requests_completed",
                "requests_scheduled",
                "errors",
                "total_input_tokens",
                "total_output_tokens",
            )
        },
        "p50_s": _histogram_percentile(histogram, 0.50),
        "p99_s": _histogram_percentile(histogram, 0.99),
        "latency_quantile_method": LATENCY_QUANTILE_METHOD,
        "latency_histogram": histogram,
        "last_fire_time": max(summary["last_fire_time"] for summary in validated),
        "last_request_start_at": max(summary["last_request_start_at"] for summary in validated),
        "last_body_done_at": max(summary["last_body_done_at"] for summary in validated),
        "adjusted_run_t0": next(iter(run_t0s)),
    }
    expected_successes = merged["requests_completed"] - merged["errors"]
    if histogram["count"] != expected_successes:
        raise ValueError("Go process summary histogram count disagrees with successful requests")
    return _validate_go_summary(merged)


def _reduce_summary_via_mpi(
    comm,
    local_summary,
    *,
    run_index: int,
    rank: int,
    mpi_size: int,
    is_root: bool,
    timeout_s: float,
):
    """Reduce fixed-size totals and transfer bounded exact rank summaries.

    Keep the fixed counters in one ``UINT64_T`` reduction and carry each
    canonical summary as one bounded, explicitly sized ``MPI.BYTE`` message.
    Root validates both paths against one another before it publishes
    completeness evidence.
    """
    _LAST_GATHER_META.clear()
    if comm is None or mpi_size <= 1:
        summary = _validate_go_summary(local_summary)
        encoded = _encode_gather_payload(summary)
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
        return {
            field: summary[field]
            for field in (
                "requests_completed",
                "requests_scheduled",
                "errors",
                "total_input_tokens",
                "total_output_tokens",
                "p50_s",
                "p99_s",
                "latency_quantile_method",
            )
        }
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s <= 0
    ):
        raise ValueError("MPI summary deadline must be finite and positive")
    if (
        isinstance(run_index, bool)
        or not isinstance(run_index, int)
        or run_index < 0
        or isinstance(rank, bool)
        or not isinstance(rank, int)
        or isinstance(mpi_size, bool)
        or not isinstance(mpi_size, int)
        or mpi_size < 2
        or not 0 <= rank < mpi_size
        or not isinstance(is_root, bool)
        or is_root != (rank == 0)
    ):
        raise ValueError("MPI summary rank/run identity is invalid")
    summary = _validate_go_summary(local_summary)
    try:
        from mpi4py import MPI
    except ImportError as exc:  # pragma: no cover - MPI communicator implies mpi4py
        raise RuntimeError("mpi4py disappeared after communicator initialization") from exc
    sum_fields = (
        "requests_completed",
        "requests_scheduled",
        "errors",
        "total_input_tokens",
        "total_output_tokens",
    )
    histogram = summary.get("latency_histogram")
    if not isinstance(histogram, dict):
        raise ValueError("multi-rank summary requires a mergeable latency_histogram")
    if summary.get("latency_quantile_method") != LATENCY_QUANTILE_METHOD:
        raise ValueError("multi-rank summary has no supported latency quantile method")
    encoded = _encode_gather_payload(summary)
    if not encoded or len(encoded) > _SUMMARY_BYTES:
        raise ValueError(
            f"rank {rank} MPI summary is empty or exceeds the {_SUMMARY_BYTES}-byte bound"
        )
    totals = [summary[field] for field in sum_fields]
    if any(value > _UINT64_MAX for value in totals):
        raise ValueError(f"rank {rank} MPI summary counter exceeds UINT64_MAX")
    _require_mpi_collective_ready()
    send_totals = array("Q", totals)
    if send_totals.itemsize != 8:
        raise RuntimeError("platform unsigned-long-long is not a 64-bit MPI counter")
    receive_totals = array("Q", [0] * len(sum_fields)) if is_root else None
    deadline = time.monotonic() + timeout_s
    summaries = {0: summary} if is_root else {}
    evidence = (
        {
            0: {
                "rank": 0,
                "size_bytes": len(encoded),
                "sha256": hashlib.sha256(encoded).hexdigest(),
                "transport": "mpi_summary_p2p",
            }
        }
        if is_root
        else {}
    )
    pending = {}
    reduction = comm.Ireduce(
        [send_totals, MPI.UINT64_T],
        [receive_totals, MPI.UINT64_T] if receive_totals is not None else None,
        op=MPI.SUM,
        root=0,
    )
    reduction_pending = True
    try:
        if is_root:
            for other in range(1, mpi_size):
                pending[other] = _mpi_irecv_message(comm, source=other, tag=_SUMMARY_TAG)
        else:
            pending[rank] = _mpi_isend_message(
                comm,
                ("summary", run_index, rank, encoded),
                dest=0,
                tag=_SUMMARY_TAG,
            )
        while (reduction_pending or pending) and time.monotonic() < deadline:
            progressed = False
            if reduction_pending and _buffer_request_test(reduction):
                reduction_pending = False
                progressed = True
            for other, request in list(pending.items()):
                complete, message = _request_test(request)
                if complete:
                    progressed = True
                    del pending[other]
                    if not is_root:
                        continue
                    if (
                        not isinstance(message, tuple)
                        or len(message) != 4
                        or message[0] != "summary"
                        or message[1] != run_index
                        or message[2] != other
                        or type(message[3]) is not bytes
                        or not message[3]
                        or len(message[3]) > _SUMMARY_BYTES
                    ):
                        raise RuntimeError(f"rank {other} sent an invalid MPI summary message")
                    content = message[3]
                    remote = _decode_gather_payload(content)
                    if not isinstance(remote, dict) or _encode_gather_payload(remote) != content:
                        raise RuntimeError(f"rank {other} sent a non-canonical MPI summary payload")
                    if remote.get(
                        "latency_quantile_method"
                    ) != LATENCY_QUANTILE_METHOD or not isinstance(
                        remote.get("latency_histogram"), dict
                    ):
                        raise RuntimeError(
                            f"rank {other} sent an unsupported MPI summary estimator"
                        )
                    summaries[other] = remote
                    evidence[other] = {
                        "rank": other,
                        "size_bytes": len(content),
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "transport": "mpi_summary_p2p",
                    }
            if (reduction_pending or pending) and not progressed:
                time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
    except BaseException:
        # MPI does not portably cancel a nonblocking collective.  Cancel only
        # framed point-to-point requests; the enclosing replay process-group
        # supervisor supplies the hard failure/termination bound.
        if reduction_pending:
            _retain_failed_mpi_collective(reduction, send_totals, receive_totals)
        _cancel_requests(pending.values())
        raise
    if reduction_pending or pending:
        if reduction_pending:
            _retain_failed_mpi_collective(reduction, send_totals, receive_totals)
        _cancel_requests(pending.values())
        if is_root:
            _LAST_GATHER_META.update(
                {
                    "schema_version": 1,
                    "expected_ranks": mpi_size,
                    "collected_ranks": sorted(summaries),
                    "missing_ranks": sorted(pending),
                    "complete": False,
                    "shards": [evidence[item] for item in sorted(evidence)],
                }
            )
        operations = (["uint64-reduction"] if reduction_pending else []) + [
            f"summary-rank-{other}" for other in sorted(pending)
        ]
        raise RuntimeError(
            f"run {run_index} MPI summary reduction exceeded its {timeout_s:.3f}s deadline; "
            f"pending operations {operations}"
        )
    if not is_root:
        return None
    if sorted(summaries) != list(range(mpi_size)):
        raise RuntimeError("MPI summary reduction returned incomplete rank evidence")
    python_totals = [
        sum(summaries[item][field] for item in range(mpi_size)) for field in sum_fields
    ]
    if any(value > _UINT64_MAX for value in python_totals):
        raise RuntimeError("MPI summary aggregate exceeds UINT64_MAX")
    assert receive_totals is not None
    reduced_totals = list(receive_totals)
    if reduced_totals != python_totals:
        raise RuntimeError("typed MPI summary reduction disagrees with exact rank evidence")
    values = dict(zip(sum_fields, reduced_totals))
    merged_histogram = _merge_latency_histograms(
        [summaries[item]["latency_histogram"] for item in range(mpi_size)]
    )
    expected_successes = values["requests_completed"] - values["errors"]
    if merged_histogram["count"] != expected_successes:
        raise RuntimeError("MPI summary latency histogram count disagrees with successful requests")
    _LAST_GATHER_META.clear()
    _LAST_GATHER_META.update(
        {
            "schema_version": 1,
            "expected_ranks": mpi_size,
            "collected_ranks": list(range(mpi_size)),
            "missing_ranks": [],
            "complete": True,
            "shards": [evidence[item] for item in range(mpi_size)],
        }
    )
    return {
        **{field: values[field] for field in sum_fields},
        "p50_s": _histogram_percentile(merged_histogram, 0.50),
        "p99_s": _histogram_percentile(merged_histogram, 0.99),
        "latency_quantile_method": LATENCY_QUANTILE_METHOD,
    }


def _reduce_dispatch_end_via_mpi(
    comm,
    local_last_fire_time: float,
    *,
    mpi_size: int,
    is_root: bool,
    timeout_s: float,
) -> float | None:
    _result_number(local_last_fire_time, field="dispatch last_fire_time")
    if (
        isinstance(timeout_s, bool)
        or not isinstance(timeout_s, (int, float))
        or not math.isfinite(float(timeout_s))
        or timeout_s <= 0
    ):
        raise ValueError("MPI dispatch-end deadline must be finite and positive")
    if comm is None or mpi_size <= 1:
        return float(local_last_fire_time)
    from mpi4py import MPI

    _require_mpi_collective_ready()
    send_value = array("d", [float(local_last_fire_time)])
    receive_value = array("d", [0.0]) if is_root else None
    deadline = time.monotonic() + timeout_s
    request = comm.Ireduce(
        [send_value, MPI.DOUBLE],
        [receive_value, MPI.DOUBLE] if receive_value is not None else None,
        op=MPI.MAX,
        root=0,
    )
    complete = False
    try:
        while time.monotonic() < deadline:
            if _buffer_request_test(request):
                complete = True
                if not is_root:
                    return None
                assert receive_value is not None
                reduced = float(receive_value[0])
                _result_number(reduced, field="global dispatch last_fire_time")
                return reduced
            time.sleep(min(0.01, max(0.0, deadline - time.monotonic())))
    except BaseException:
        if not complete:
            _retain_failed_mpi_collective(request, send_value, receive_value)
        raise
    # Nonblocking collective cancellation is not portable.  Raising lets the
    # supervised mpiexec boundary terminate the complete rank set.
    _retain_failed_mpi_collective(request, send_value, receive_value)
    raise RuntimeError("distributed dispatch-end reduction exceeded its absolute MPI deadline")


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


# Written by the MPI result transfer on the root rank and merged into result
# metadata by _save_results. Single-threaded per-process access.
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
            "latency_quantile_method",
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
        if run_results["latency_quantile_method"] != LATENCY_QUANTILE_METHOD:
            raise ValueError("merged Go replay summary quantile method is unsupported")
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
            "latency_quantile_method": run_results["latency_quantile_method"],
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


def _contained_real_path(path: str, root: str, *, label: str) -> str:
    if not isinstance(path, str) or not os.path.isabs(path):
        raise RuntimeError(f"{label} must be an absolute path")
    if not isinstance(root, str) or not os.path.isabs(root):
        raise RuntimeError("EXASERVE_LOCAL_RUNTIME_ROOT must be an absolute path")
    resolved_root = os.path.realpath(root)
    resolved = os.path.realpath(path)
    if os.path.commonpath((resolved_root, resolved)) != resolved_root:
        raise RuntimeError(f"{label} escapes EXASERVE_LOCAL_RUNTIME_ROOT")
    return resolved


def _find_go_binary(*, require_local: bool = False) -> str | None:
    local = os.environ.get("EXASERVE_LOCAL_GO_DISPATCH")
    runtime_root = os.environ.get("EXASERVE_LOCAL_RUNTIME_ROOT")
    if local is not None or runtime_root is not None or require_local:
        if not local or not runtime_root:
            raise RuntimeError(
                "multi-rank replay requires EXASERVE_LOCAL_RUNTIME_ROOT and "
                "EXASERVE_LOCAL_GO_DISPATCH"
            )
        resolved = _contained_real_path(local, runtime_root, label="local Go replay binary")
        expected_bin_root = os.path.join(os.path.realpath(runtime_root), "bin")
        if os.path.commonpath((expected_bin_root, resolved)) != expected_bin_root:
            raise RuntimeError("local Go replay binary must be inside runtime-root/bin")
        if not os.path.isfile(resolved) or not os.access(resolved, os.X_OK):
            raise RuntimeError("local Go replay binary is missing or not executable")
        return resolved

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


def _local_replay_state_root(*, require_local: bool) -> str | None:
    root = os.environ.get("EXASERVE_LOCAL_STATE_ROOT")
    if not root:
        if require_local:
            raise RuntimeError("multi-rank replay requires EXASERVE_LOCAL_STATE_ROOT")
        return None
    if not os.path.isabs(root):
        raise RuntimeError("EXASERVE_LOCAL_STATE_ROOT must be absolute")
    resolved = os.path.realpath(root)
    metadata = os.stat(resolved, follow_symlinks=False)
    if not os.path.isdir(resolved) or metadata.st_uid != os.getuid():
        raise RuntimeError("EXASERVE_LOCAL_STATE_ROOT must be a user-owned real directory")
    return resolved


def _verify_local_replay_filesystem(*, rank: int) -> None:
    import socket

    from exaserve.plan.contracts import same_node
    from exaserve.plan.io import (
        load_allocation_binding,
        load_deployment_plan,
        load_site_profile,
    )
    from exaserve.plan.runtime_environment import RuntimePaths

    runtime_root = os.environ.get("EXASERVE_LOCAL_RUNTIME_ROOT", "")
    state_root = os.environ.get("EXASERVE_LOCAL_STATE_ROOT", "")
    plan_path = os.environ.get("EXASERVE_LOCAL_PLAN_PATH", "")
    site_path = os.environ.get("EXASERVE_SITE_PROFILE_PATH", "")
    binding_path = os.environ.get("EXASERVE_ALLOCATION_BINDING_PATH", "")
    if not all((runtime_root, state_root, plan_path, site_path, binding_path)):
        raise RuntimeError("multi-rank replay local runtime identity is incomplete")
    plan = load_deployment_plan(plan_path)
    profile = load_site_profile(site_path)
    binding = load_allocation_binding(binding_path)
    if (
        profile.site_id != plan.site_profile_id
        or profile.site_profile_hash != plan.site_profile_hash
        or binding.deployment_plan_hash != plan.deployment_plan_hash
        or binding.site_profile_hash != plan.site_profile_hash
    ):
        raise RuntimeError("multi-rank replay capsule identities disagree")
    planned_node = binding.node_for(rank)
    if not planned_node or not same_node(socket.gethostname(), planned_node):
        raise RuntimeError(
            f"replay MPI rank {rank} is not running on AllocationBinding node {planned_node!r}"
        )
    paths = RuntimePaths.from_roots(
        runtime_root,
        state_root,
        policy=profile,
        require_runtime=True,
    )
    paths.verify_capsule(policy=profile)
    paths.prepare_state(policy=profile)


def _direct_health_paths(exp_config: EvalManifest) -> list[str]:
    # Direct target discovery is restricted to one model and returns either a
    # root application or an already-routed /<model>_rN base URL.
    if len(exp_config.deployment_plan.models) != 1:
        raise RuntimeError("direct replay requires exactly one canonical model")
    return ["/health"]


def _probe_direct_target(base_url: str, health_paths: list[str], timeout_s: float) -> bool:
    # These URLs are allocation-internal Ray Serve endpoints.  The Aurora
    # environment deliberately configures an outbound HTTP proxy, and compute
    # node IPs are not covered by its no_proxy host patterns.  Relying on the
    # process-global urllib opener would therefore send 10.x health probes to
    # the site proxy and report every healthy replica as unavailable.
    direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for path in health_paths:
        try:
            with direct_opener.open(f"{base_url}{path}", timeout=timeout_s) as response:
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
        process_summaries = []
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
                process_summaries.append(parsed)
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
        if sum_only:
            all_results = _merge_go_process_summaries(process_summaries)
            if all_results["last_fire_time"] != max_last_fire_time:
                raise RuntimeError("merged Go summary dispatch endpoint is inconsistent")
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


def _trace_request_from_line(line: bytes, *, request_index: int) -> TraceRequest | None:
    from exaserve.state.atomic import strict_json_loads

    if len(line) > _TRACE_ROW_BYTES:
        raise ValueError(f"trace row exceeds the {_TRACE_ROW_BYTES}-byte safety bound")
    data = strict_json_loads(line.decode("utf-8"))
    if not isinstance(data, dict):
        raise ValueError("trace row must be a JSON object")
    if data.get("__type__") == "metadata":
        return None
    required = {"timestamp", "model", "prompt", "output_len"}
    allowed = required | {"input_len", "tensor_parallel_size", "mode"}
    if not required <= set(data) or not set(data) <= allowed:
        raise ValueError(
            "trace request row has invalid fields: "
            f"missing={sorted(required - set(data))}, "
            f"unknown={sorted(set(data) - allowed)}"
        )
    return TraceRequest(
        timestamp=data["timestamp"],
        model=data["model"],
        prompt=data["prompt"],
        input_len=data.get("input_len", 0),
        output_len=data["output_len"],
        tensor_parallel_size=data.get("tensor_parallel_size", 1),
        req_id=f"{request_index:032x}",
        mode=data.get("mode", "chat"),
    )


def _distribute_trace_requests(
    comm,
    *,
    rank: int,
    mpi_size: int,
    trace_path: str | None,
    expected_hash: str | None,
) -> tuple[list[TraceRequest], int, float]:
    """Read/hash once on root and scatter bounded in-memory partitions."""
    if mpi_size <= 1:
        if rank != 0 or not trace_path or not expected_hash:
            raise RuntimeError("single-rank trace input is missing on root")
        requests = _load_trace_requests(trace_path, expected_hash=expected_hash)
        return requests, len(requests), requests[-1].timestamp if requests else 0.0

    local_requests: list[TraceRequest] = []
    if rank != 0:
        while True:
            part = comm.scatter(None, root=0)
            if part is None:
                break
            if not isinstance(part, list) or any(
                not isinstance(item, TraceRequest) for item in part
            ):
                raise RuntimeError("root sent a malformed trace partition")
            local_requests.extend(part)
        status = _mpi_bcast(comm, None, root=0)
        if not isinstance(status, dict) or set(status) != {
            "ok",
            "error",
            "total",
            "last_timestamp",
        }:
            raise RuntimeError("root sent a malformed trace terminal")
        if not status["ok"]:
            raise RuntimeError(f"root trace validation failed: {status['error']}")
        return local_requests, status["total"], status["last_timestamp"]

    if not trace_path or not expected_hash:
        raise RuntimeError("root trace input is missing")
    from exaserve.state.atomic import regular_file_reader

    digest = hashlib.sha256()
    partitions: list[list[TraceRequest]] = [[] for _ in range(mpi_size)]
    buffered_bytes = 0
    total = 0
    last_timestamp = 0.0
    error = None
    try:
        with regular_file_reader(trace_path, binary=True) as handle:
            for line in handle:
                digest.update(line)
                request = _trace_request_from_line(line, request_index=total)
                if request is None:
                    continue
                target = total % mpi_size
                partitions[target].append(request)
                buffered_bytes += len(line)
                total += 1
                last_timestamp = request.timestamp
                if buffered_bytes >= _TRACE_BATCH_BYTES:
                    own = comm.scatter(partitions, root=0)
                    local_requests.extend(own)
                    partitions = [[] for _ in range(mpi_size)]
                    buffered_bytes = 0
        if any(partitions):
            own = comm.scatter(partitions, root=0)
            local_requests.extend(own)
        observed = digest.hexdigest()
        if observed != expected_hash:
            raise RuntimeError(
                f"trace content hash mismatch: expected {expected_hash}, observed {observed}"
            )
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
    finally:
        comm.scatter([None] * mpi_size, root=0)
    status = {
        "ok": error is None,
        "error": error,
        "total": total,
        "last_timestamp": last_timestamp,
    }
    _mpi_bcast(comm, status, root=0)
    if error is not None:
        raise RuntimeError(f"root trace validation failed: {error}")
    return local_requests, total, last_timestamp


def _load_trace_requests(
    trace_path: str, *, expected_hash: str | None = None
) -> list[TraceRequest]:
    from exaserve.state.atomic import regular_file_reader

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
                request = _trace_request_from_line(line, request_index=len(requests))
                if request is not None:
                    requests.append(request)
            except (KeyError, TypeError, UnicodeError, ValueError) as exc:
                if digest is None:
                    raise
                parse_error = exc
    if digest is not None and digest.hexdigest() != expected_hash:
        raise RuntimeError(
            f"trace checksum mismatch for {trace_path}: "
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


def _prepare_root_replay_context(
    config_path: str,
    *,
    include_tp_override: bool | None,
    early_stop_override: float | None,
    num_runs_override: int | None,
    dest_override: str | None,
    base_urls_override: str | None,
    dispatch_topology_override: str | None,
    result_subdir: str | None,
) -> tuple[EvalManifest, dict]:
    local_manifest = os.environ.get("EXASERVE_LOCAL_EVAL_MANIFEST", "").strip()
    local_plan = os.environ.get("EXASERVE_LOCAL_PLAN_PATH", "").strip()
    local_run_plan = os.environ.get("EXASERVE_LOCAL_RUN_PLAN_PATH", "").strip()
    using_capsule = bool(local_manifest)
    if using_capsule and not (local_plan and local_run_plan):
        raise ValueError("local replay capsule is missing deployment or RunPlan artifacts")
    if using_capsule and os.path.realpath(config_path) != os.path.realpath(local_manifest):
        raise ValueError("replay config path is not the certified local eval manifest")
    exp_config = load_eval_manifest(
        config_path,
        verify_trace_artifact=False,
        deployment_plan_path_override=(local_plan or None) if using_capsule else None,
        run_plan_path_override=(local_run_plan or None) if using_capsule else None,
    )
    replay_cfg = exp_config.job_replay_client_config
    if not using_capsule and os.path.abspath(config_path) != os.path.abspath(
        replay_cfg.config_path
    ):
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

    port = _port_from_manifest(exp_config)
    if base_urls_override is not None:
        cluster_nodes = []
        base_urls = [item.strip() for item in base_urls_override.split(",") if item.strip()]
    elif replay_cfg.dest == "direct":
        cluster_nodes = _get_cluster_nodes()
        base_urls = [f"http://{node}:{port}" for node in cluster_nodes]
    else:
        cluster_nodes = []
        base_urls = [f"http://0.0.0.0:{port}"]
    base_urls = _validate_base_urls(exp_config, base_urls)
    if replay_cfg.saturation.get("enabled"):
        # Saturation does not consume trace rows, so retain one explicit root
        # integrity pass. Ordinary replay verifies while streaming the trace.
        exp_config.verify_trace_artifact()
    return exp_config, {
        "schema_version": 1,
        "replay": replay_cfg,
        "topology": topology,
        "base_urls": base_urls,
        "cluster_nodes": cluster_nodes,
        "saturation": dict(replay_cfg.saturation),
    }


def _validate_replay_context(context) -> dict:
    expected = {
        "schema_version",
        "replay",
        "topology",
        "base_urls",
        "cluster_nodes",
        "saturation",
    }
    if (
        not isinstance(context, dict)
        or set(context) != expected
        or context.get("schema_version") != 1
        or not isinstance(context.get("replay"), ReplayClientConfig)
        or not isinstance(context.get("topology"), str)
        or not isinstance(context.get("base_urls"), list)
        or not context["base_urls"]
        or any(not isinstance(item, str) or not item for item in context["base_urls"])
        or not isinstance(context.get("cluster_nodes"), list)
        or any(not isinstance(item, str) or not item for item in context["cluster_nodes"])
        or not isinstance(context.get("saturation"), dict)
    ):
        raise RuntimeError("root broadcast a malformed validated replay context")
    return context


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
    # MPI must exist before any rank can touch an argument that may name shared
    # storage.  Only rank 0 loads/validates the manifest, plans, nodefile, and
    # URLs; workers receive one already-validated in-memory projection.
    comm, rank, mpi_size = _init_mpi()
    is_root = rank == 0
    exp_config = None
    root_error = None
    context = None
    if is_root:
        try:
            exp_config, context = _prepare_root_replay_context(
                config_path,
                include_tp_override=include_tp_override,
                early_stop_override=early_stop_override,
                num_runs_override=num_runs_override,
                dest_override=dest_override,
                base_urls_override=base_urls_override,
                dispatch_topology_override=dispatch_topology_override,
                result_subdir=result_subdir,
            )
        except Exception as exc:
            root_error = exc
    packet = _mpi_bcast(
        comm,
        {
            "context": context,
            "error": (None if root_error is None else f"{type(root_error).__name__}: {root_error}"),
        }
        if is_root
        else None,
        root=0,
    )
    if not isinstance(packet, dict) or set(packet) != {"context", "error"}:
        raise RuntimeError("root broadcast a malformed replay initialization packet")
    if packet["error"] is not None:
        if is_root and root_error is not None:
            raise root_error
        raise RuntimeError(f"root replay initialization failed: {packet['error']}")
    context = _validate_replay_context(packet["context"])
    replay_cfg = context["replay"]
    if mpi_size != replay_cfg.num_nodes or (replay_cfg.num_nodes > 1 and comm is None):
        raise RuntimeError(
            f"replay MPI world size {mpi_size} does not equal canonical "
            f"client.num_nodes {replay_cfg.num_nodes}"
        )
    if mpi_size > 1:
        qualified_python = os.environ.get("EXASERVE_QUALIFIED_PYTHON")
        if (
            not qualified_python
            or not os.path.isabs(qualified_python)
            or os.path.realpath(qualified_python) != os.path.realpath(sys.executable)
        ):
            raise RuntimeError("multi-rank replay is not running under EXASERVE_QUALIFIED_PYTHON")
        _verify_local_replay_filesystem(rank=rank)
    include_tp = replay_cfg.include_tp
    early_stop = replay_cfg.early_stop
    num_runs = replay_cfg.num_runs
    dest = replay_cfg.dest
    topology = context["topology"]
    generation_mode = replay_cfg.generation_mode
    num_go_procs = replay_cfg.num_go_procs
    num_go_workers = replay_cfg.num_go_workers
    go_concurrency = replay_cfg.go_concurrency
    warmup_rps = replay_cfg.warmup_rps
    warmup_duration_s = replay_cfg.warmup_duration_s
    sum_only = replay_cfg.sum_only

    base_urls = context["base_urls"]
    cluster_nodes = context["cluster_nodes"]

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

    go_bin = _find_go_binary(require_local=mpi_size > 1)
    if go_bin is None:
        raise RuntimeError(
            "go_dispatch binary is missing or older than its sources; rebuild the "
            "immutable snapshot with `make -C eval/go_client build`"
        )

    # Saturation mode: skip trace loading, run saturation finder instead.
    sat_cfg = context["saturation"]
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

    rank_requests, total_requests, trace_span_s = _distribute_trace_requests(
        comm,
        rank=rank,
        mpi_size=mpi_size,
        trace_path=_trace_path(exp_config) if is_root else None,
        expected_hash=exp_config.trace_content_hash if is_root else None,
    )
    target_responses = int(total_requests * early_stop) if early_stop and early_stop > 0 else None

    interrupted = False
    interrupt_event = threading.Event()

    def signal_handler(_sig, _frame):
        nonlocal interrupted
        interrupted = True
        interrupt_event.set()

    old_handler = signal.signal(signal.SIGINT, signal_handler)
    local_state_root = _local_replay_state_root(require_local=mpi_size > 1)
    tmp_dir = tempfile.mkdtemp(prefix=f"replay_rank{rank}_", dir=local_state_root)
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
            global_last_fire_time = _reduce_dispatch_end_via_mpi(
                comm,
                last_fire_time,
                mpi_size=mpi_size,
                is_root=is_root,
                timeout_s=replay_cfg.shard_timeout_s,
            )
            if is_root and global_last_fire_time is not None and global_last_fire_time > 0:
                trace_span = trace_span_s
                actual_dispatch_s = global_last_fire_time - effective_run_t0
                dispatch_timings.append(
                    {
                        "run_index": run_index,
                        "trace_span_s": trace_span,
                        "actual_dispatch_s": actual_dispatch_s,
                        "overhead_s": actual_dispatch_s - trace_span,
                    }
                )
            if isinstance(local_results, dict):
                run_results = _reduce_summary_via_mpi(
                    comm,
                    local_results,
                    run_index=run_index,
                    rank=rank,
                    mpi_size=mpi_size,
                    is_root=is_root,
                    timeout_s=replay_cfg.shard_timeout_s,
                )
                gathered = None
            else:
                gathered = _gather_raw_results_via_mpi(
                    comm,
                    local_results,
                    run_index=run_index,
                    rank=rank,
                    mpi_size=mpi_size,
                    is_root=is_root,
                    timeout_s=replay_cfg.shard_timeout_s,
                )
                run_results = None
            if is_root:
                gather_by_run.append(dict(_LAST_GATHER_META))
            if is_root and isinstance(local_results, dict):
                assert isinstance(run_results, dict)
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


def _classified_timing_fields(
    semantics: str,
    *,
    first_byte_s,
    first_byte_at,
    interchunk_p50_s,
    interchunk_p99_s,
    interchunk_max_s,
) -> dict:
    """Expose TTFT/TBT only for real incremental token delivery."""
    from exaserve.capabilities import TIMING_INCREMENTAL_SSE, TIMING_SEMANTICS

    if semantics not in TIMING_SEMANTICS:
        raise ValueError(f"unknown timing semantics {semantics!r}")
    incremental = semantics == TIMING_INCREMENTAL_SSE
    return {
        "timing_semantics": semantics,
        "observed_first_byte_s": first_byte_s,
        "observed_first_byte_at": first_byte_at,
        "observed_interchunk_p50_s": interchunk_p50_s,
        "observed_interchunk_p99_s": interchunk_p99_s,
        "observed_interchunk_max_s": interchunk_max_s,
        "ttft_s": first_byte_s if incremental else None,
        "first_token_at": first_byte_at if incremental else None,
        "tbt_p50_s": interchunk_p50_s if incremental else None,
        "tbt_p99_s": interchunk_p99_s if incremental else None,
        "tbt_max_s": interchunk_max_s if incremental else None,
    }


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
    from exaserve.capabilities import deployment_timing_semantics

    timing_semantics = deployment_timing_semantics(plan)
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
        "timing_semantics": timing_semantics,
        "latency_quantile_method": (
            results["latency_quantile_method"] if isinstance(results, dict) else "exact_raw_samples"
        ),
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
                "latency_quantile_method": results["latency_quantile_method"],
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
                        **_classified_timing_fields(
                            timing_semantics,
                            first_byte_s=ttft_s,
                            first_byte_at=first_token_at,
                            interchunk_p50_s=tbt_p50_s,
                            interchunk_p99_s=tbt_p99_s,
                            interchunk_max_s=tbt_max_s,
                        ),
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
