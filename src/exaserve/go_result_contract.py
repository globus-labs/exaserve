"""Shared, fail-closed grammar for ``go_dispatch`` replay result streams.

Both eval and ClientLab execute the same Go client.  A zero exit code is not a
complete result by itself: the result file must contain exactly one terminal
row, every preceding request row must be valid, and no bytes may follow the
terminal row.  Keeping that grammar here prevents the two consumers from
silently assigning different meanings to a truncated client shard.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from os import PathLike
from typing import Any

from .state.atomic import regular_file_reader, strict_json_loads

LATENCY_QUANTILE_METHOD = "mergeable_histogram_estimate_2pct_through_7200s"


def _number(value: object, *, field: str, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise ValueError(f"Go replay {field} must be a finite nonnegative number")


def _count(value: object, *, field: str, nullable: bool = False) -> None:
    if nullable and value is None:
        return
    if type(value) is not int or value < 0:
        raise ValueError(f"Go replay {field} must be a nonnegative integer")


def validate_go_summary(record: object) -> dict[str, Any]:
    required = {
        "__type__",
        "requests_completed",
        "requests_scheduled",
        "errors",
        "p50_s",
        "p99_s",
        "total_input_tokens",
        "total_output_tokens",
        "last_fire_time",
        "last_request_start_at",
        "last_body_done_at",
        "adjusted_run_t0",
    }
    optional = {
        "error_counts",
        "error_samples",
        "dispatch_health",
        "dispatch_warnings",
        "dispatch_lag_p99_s",
        "max_observed_active",
        "new_connections",
        "reused_connections",
        "latency_histogram",
        "latency_quantile_method",
    }
    if (
        not isinstance(record, dict)
        or not required <= set(record)
        or not set(record) <= (required | optional)
    ):
        raise ValueError("Go replay summary fields are invalid")
    if record["__type__"] != "summary":
        raise ValueError("Go replay summary type is invalid")
    for field in (
        "requests_completed",
        "requests_scheduled",
        "errors",
        "total_input_tokens",
        "total_output_tokens",
    ):
        _count(record[field], field=f"summary.{field}")
    if not 0 <= record["errors"] <= record["requests_completed"] <= record["requests_scheduled"]:
        raise ValueError("Go replay summary request counts are inconsistent")
    for field in (
        "p50_s",
        "p99_s",
        "last_fire_time",
        "last_request_start_at",
        "last_body_done_at",
        "adjusted_run_t0",
    ):
        _number(record[field], field=f"summary.{field}")
    for field in ("max_observed_active", "new_connections", "reused_connections"):
        if field in record:
            _count(record[field], field=f"summary.{field}")
    if "dispatch_lag_p99_s" in record:
        _number(record["dispatch_lag_p99_s"], field="summary.dispatch_lag_p99_s")
    if "dispatch_health" in record and (
        not isinstance(record["dispatch_health"], str) or not record["dispatch_health"]
    ):
        raise ValueError("Go replay summary.dispatch_health is invalid")
    if "dispatch_warnings" in record and (
        not isinstance(record["dispatch_warnings"], list)
        or any(not isinstance(item, str) for item in record["dispatch_warnings"])
    ):
        raise ValueError("Go replay summary.dispatch_warnings is invalid")
    for field, value_type in (("error_counts", int), ("error_samples", str)):
        if field not in record:
            continue
        mapping = record[field]
        if not isinstance(mapping, dict) or any(
            not isinstance(key, str)
            or not key
            or not isinstance(value, value_type)
            or isinstance(value, bool)
            or (value_type is int and value < 0)
            for key, value in mapping.items()
        ):
            raise ValueError(f"Go replay summary.{field} is invalid")
    has_histogram = "latency_histogram" in record
    has_method = "latency_quantile_method" in record
    if has_histogram != has_method:
        raise ValueError(
            "Go replay summary latency histogram and quantile method must be published together"
        )
    if has_method and record["latency_quantile_method"] != LATENCY_QUANTILE_METHOD:
        raise ValueError("Go replay summary.latency_quantile_method is unsupported")
    if has_histogram:
        histogram = record["latency_histogram"]
        if not isinstance(histogram, dict) or set(histogram) != {
            "bucket_upper_bounds_s",
            "counts",
            "count",
            "sum_s",
        }:
            raise ValueError("Go replay summary.latency_histogram fields are invalid")
        bounds = histogram["bucket_upper_bounds_s"]
        counts = histogram["counts"]
        if (
            not isinstance(bounds, list)
            or not isinstance(counts, list)
            or len(bounds) != len(counts)
            or len(bounds) < 2
            or bounds[-1] != -1.0
            or type(histogram["count"]) is not int
            or histogram["count"] < 0
            or any(type(value) is not int or value < 0 for value in counts)
            or sum(counts) != histogram["count"]
        ):
            raise ValueError("Go replay summary.latency_histogram values are invalid")
        for index, value in enumerate(bounds):
            if index == len(bounds) - 1 and value == -1.0:
                continue
            _number(value, field=f"summary.latency_histogram.bounds[{index}]")
            if index and value <= bounds[index - 1]:
                raise ValueError("Go replay summary.latency_histogram bounds are not ordered")
        _number(histogram["sum_s"], field="summary.latency_histogram.sum_s")
    return record


def validate_dispatch_done(record: object) -> dict[str, Any]:
    fields = {
        "__type__",
        "last_fire_time",
        "last_request_start_at",
        "last_body_done_at",
        "adjusted_run_t0",
    }
    if (
        not isinstance(record, dict)
        or set(record) != fields
        or record["__type__"] != "dispatch_done"
    ):
        raise ValueError("Go replay dispatch_done fields are invalid")
    for field in fields - {"__type__"}:
        _number(record[field], field=f"dispatch_done.{field}")
    return record


def validate_go_result_record(record: object) -> dict[str, Any]:
    required = {
        "req_id",
        "model",
        "latency",
        "success",
        "error",
        "end_time",
        "input_len",
        "output_len",
        "actual_prompt_tokens",
        "actual_completion_tokens",
        "tensor_parallel_size",
    }
    optional = {
        "error_class",
        "status_code",
        "scheduled_at",
        "enqueued_at",
        "dequeued_at",
        "request_start_at",
        "headers_at",
        "first_token_at",
        "body_done_at",
        "ttft_s",
        "tbt_p50_s",
        "tbt_p99_s",
        "tbt_max_s",
        "decode_tokens",
    }
    if (
        not isinstance(record, dict)
        or not required <= set(record)
        or not set(record) <= (required | optional)
    ):
        raise ValueError("Go replay result row fields are invalid")
    for field in ("req_id", "model"):
        if not isinstance(record[field], str) or not record[field]:
            raise ValueError(f"Go replay result.{field} is invalid")
    for field in ("error", "error_class"):
        if field in record and not isinstance(record[field], str):
            raise ValueError(f"Go replay result.{field} is invalid")
    if not isinstance(record["success"], bool):
        raise ValueError("Go replay result.success must be boolean")
    for field in ("input_len", "output_len"):
        _count(record[field], field=f"result.{field}")
    _count(record["tensor_parallel_size"], field="result.tensor_parallel_size")
    if record["tensor_parallel_size"] < 1:
        raise ValueError("Go replay result.tensor_parallel_size must be positive")
    for field in ("actual_prompt_tokens", "actual_completion_tokens"):
        _count(record[field], field=f"result.{field}", nullable=True)
    for field in ("status_code", "decode_tokens"):
        if field in record:
            _count(record[field], field=f"result.{field}")
    for field in (
        "latency",
        "end_time",
        "scheduled_at",
        "enqueued_at",
        "dequeued_at",
        "request_start_at",
        "headers_at",
        "first_token_at",
        "body_done_at",
        "ttft_s",
        "tbt_p50_s",
        "tbt_p99_s",
        "tbt_max_s",
    ):
        if field in record:
            _number(record[field], field=f"result.{field}")
    return record


@dataclass(frozen=True)
class GoResultStream:
    records: tuple[dict[str, Any], ...]
    terminal: dict[str, Any]

    @property
    def sum_only(self) -> bool:
        return self.terminal["__type__"] == "summary"


def read_go_result_stream(path: str | PathLike[str]) -> GoResultStream:
    """Read one result shard from a descriptor-verified regular file."""

    records: list[dict[str, Any]] = []
    seen_request_ids: set[str] = set()
    terminal: dict[str, Any] | None = None
    with regular_file_reader(path) as handle:
        for line_number, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = strict_json_loads(line)
            except (UnicodeDecodeError, ValueError) as exc:
                raise ValueError(
                    f"Go replay result line {line_number} is invalid JSON: {exc}"
                ) from exc
            if not isinstance(record, dict):
                raise ValueError("Go replay result row must be a JSON object")
            if terminal is not None:
                raise ValueError(
                    f"Go replay result contains data after terminal {terminal['__type__']} row"
                )
            row_type = record.get("__type__")
            if row_type == "summary":
                if records:
                    raise ValueError("Go replay summary cannot follow per-request rows")
                terminal = validate_go_summary(record)
            elif row_type == "dispatch_done":
                terminal = validate_dispatch_done(record)
            else:
                result = validate_go_result_record(record)
                request_id = result["req_id"]
                if request_id in seen_request_ids:
                    raise ValueError(f"Go replay result repeats request id {request_id!r}")
                seen_request_ids.add(request_id)
                records.append(result)
    if terminal is None:
        raise ValueError("Go replay result is missing its terminal row")
    return GoResultStream(records=tuple(records), terminal=terminal)
