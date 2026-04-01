import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional


DIAGNOSES = {
    "client_dispatch_bound",
    "client_queue_bound",
    "concurrency_saturated",
    "transport_conn_bound",
    "connection_churn_bound",
    "server_capacity_bound",
    "proxy_distribution_bound",
    "network_packet_bound",
    "network_bandwidth_bound",
    "mixed",
    "healthy",
    "error",
    "inconclusive",
}


def histogram_mean_seconds(histogram):
    count = float(histogram.get("count") or 0.0)
    if count <= 0:
        return 0.0
    return float(histogram.get("sum_s") or 0.0) / count


def summarize_point(run_config, client_metrics, target_metrics, port_metrics, netstats_summary=None):
    histograms = client_metrics.get("histograms", {})
    queue_wait_mean = histogram_mean_seconds(histograms.get("queue_wait", {}))
    slot_hold_mean = histogram_mean_seconds(histograms.get("slot_hold", {}))
    dispatch_lag_mean = histogram_mean_seconds(histograms.get("dispatch_lag", {}))
    connect_mean = histogram_mean_seconds(histograms.get("connect", {}))
    time_to_headers_mean = histogram_mean_seconds(histograms.get("time_to_headers", {}))
    body_read_mean = histogram_mean_seconds(histograms.get("body_read", {}))

    completed = float(client_metrics.get("requests_succeeded") or client_metrics.get("requests_completed") or 0.0)
    configured_duration_s = max(float(run_config["client"]["duration_s"]), 1e-9)
    run_t0 = float(client_metrics.get("run_t0") or 0.0)
    last_request_start_at = float(client_metrics.get("last_request_start_at") or 0.0)
    last_body_done_at = float(client_metrics.get("last_body_done_at") or 0.0)
    measured_dispatch_s = 0.0
    measured_completion_s = 0.0
    if run_t0 > 0 and last_request_start_at >= run_t0:
        measured_dispatch_s = last_request_start_at - run_t0
    if run_t0 > 0 and last_body_done_at >= run_t0:
        measured_completion_s = last_body_done_at - run_t0
    effective_duration_s = measured_completion_s if measured_completion_s > 0 else configured_duration_s
    achieved_rps = completed / max(effective_duration_s, 1e-9)
    requested_rps = float(run_config["client"]["rate"])
    success_fraction = 1.0
    total_completed = float(client_metrics.get("requests_completed") or 0.0)
    if total_completed > 0:
        success_fraction = float(client_metrics.get("requests_succeeded", 0)) / total_completed

    queue_fraction = 0.0
    if slot_hold_mean > 0:
        queue_fraction = queue_wait_mean / slot_hold_mean

    new_connections = float(client_metrics.get("new_connections") or 0.0)
    reused_connections = float(client_metrics.get("reused_connections") or 0.0)
    connection_churn_ratio = new_connections / max(total_completed, 1.0)
    reuse_ratio = reused_connections / max(total_completed, 1.0)
    max_queue_depth = int(client_metrics.get("max_observed_queue_depth") or 0)
    max_active = int(client_metrics.get("max_observed_active") or 0)
    configured_active = int(client_metrics.get("max_active_requests") or run_config["client"]["max_active_requests"])
    max_time_wait = int(port_metrics.get("max_time_wait") or 0)

    target_queue_peak = int(target_metrics.get("aggregate", {}).get("max_queue_depth", 0))
    target_rejections = int(target_metrics.get("aggregate", {}).get("rejections", 0))
    target_error_rate = float(target_metrics.get("aggregate", {}).get("error_fraction", 0.0))

    # Expected RPS based on Little's Law: C / T when service_time > 0.
    service_time_s = float(run_config["faults"].get("service_time", {}).get("value_ms", 0.0)) / 1000.0
    if service_time_s > 0 and configured_active > 0:
        expected_rps = configured_active / service_time_s
    else:
        expected_rps = requested_rps

    diagnosis = "inconclusive"
    reasons = []
    if netstats_summary:
        if int(netstats_summary.get("max_rx_drops", 0)) > 0 or int(netstats_summary.get("max_tx_drops", 0)) > 0:
            diagnosis = "network_packet_bound"
            reasons.append("Non-zero NIC drops were observed.")
        elif float(netstats_summary.get("max_bandwidth_fraction", 0.0)) >= 0.80:
            diagnosis = "network_bandwidth_bound"
            reasons.append("Measured NIC bandwidth reached the configured saturation threshold.")
    if diagnosis == "inconclusive" and target_rejections > 0:
        diagnosis = "server_capacity_bound"
        reasons.append("Synthetic target rejected requests under configured capacity limits.")
    if diagnosis == "inconclusive" and target_queue_peak > 0 and success_fraction < 0.99:
        diagnosis = "server_capacity_bound"
        reasons.append("Target queue grew while client success fraction fell.")
    if diagnosis == "inconclusive" and queue_fraction >= 0.25 and max_queue_depth > 0:
        diagnosis = "client_queue_bound"
        reasons.append("A significant fraction of slot hold time was spent waiting in the client queue.")
    # Check concurrency_saturated and healthy BEFORE transport/churn diagnoses.
    # When achieved ≈ expected (Little's Law ceiling), the system is working correctly
    # even if max_active == configured_active — that's expected, not a bottleneck.
    if diagnosis == "inconclusive" and max_active >= configured_active and requested_rps > expected_rps * 1.1 and achieved_rps >= expected_rps * 0.85:
        diagnosis = "concurrency_saturated"
        reasons.append(
            f"Arrival rate ({requested_rps:.0f} req/s) exceeds concurrency ceiling "
            f"({expected_rps:.0f} req/s = {configured_active} slots / {service_time_s:.3f}s). "
            f"Achieved {achieved_rps:.0f} req/s — concurrency is the limiting factor, client is healthy."
        )
    # Healthy: achieved is near the effective ceiling (min of rate limit and concurrency ceiling).
    effective_ceiling = min(expected_rps, requested_rps) if expected_rps > 0 else requested_rps
    if diagnosis == "inconclusive" and effective_ceiling > 0 and achieved_rps >= effective_ceiling * 0.85:
        diagnosis = "healthy"
        reasons.append(
            f"Achieved {achieved_rps:.0f} req/s is within 15% of effective ceiling "
            f"({effective_ceiling:.0f} req/s). No dominant bottleneck."
        )
    if diagnosis == "inconclusive" and connect_mean > 0 and configured_active > 0 and max_active >= configured_active and achieved_rps < requested_rps * 0.9:
        diagnosis = "transport_conn_bound"
        reasons.append("Configured active request slots saturated while throughput stayed below target.")
    if diagnosis == "inconclusive" and connection_churn_ratio >= 0.25 and reuse_ratio < 0.5 and max_time_wait > configured_active:
        diagnosis = "connection_churn_bound"
        reasons.append("Connection churn and TIME_WAIT exceeded the active request budget.")
    if diagnosis == "inconclusive" and dispatch_lag_mean >= 0.05 and queue_fraction < 0.10:
        diagnosis = "client_dispatch_bound"
        reasons.append("Dispatch lag dominated while queue wait stayed low.")
    if diagnosis == "inconclusive" and target_error_rate > 0:
        diagnosis = "mixed"
        reasons.append("The target injected or returned errors during the run.")

    little_law_demand = requested_rps * slot_hold_mean
    safe_budget = math.ceil((achieved_rps * max(slot_hold_mean, 1e-6)) * 1.20)
    summary = {
        "diagnosis": diagnosis,
        "reasons": reasons or ["No dominant bottleneck crossed the configured heuristics."],
        "requested_rps": requested_rps,
        "expected_rps": expected_rps,
        "achieved_rps": achieved_rps,
        "configured_duration_s": configured_duration_s,
        "measured_dispatch_s": measured_dispatch_s,
        "measured_completion_s": measured_completion_s,
        "success_fraction": success_fraction,
        "queue_fraction": queue_fraction,
        "dispatch_lag_mean_s": dispatch_lag_mean,
        "queue_wait_mean_s": queue_wait_mean,
        "time_to_headers_mean_s": time_to_headers_mean,
        "body_read_mean_s": body_read_mean,
        "connect_mean_s": connect_mean,
        "slot_hold_mean_s": slot_hold_mean,
        "little_law_active_demand": little_law_demand,
        "safe_active_budget_estimate": max(safe_budget, 1),
        "connection_churn_ratio": connection_churn_ratio,
        "reuse_ratio": reuse_ratio,
        "max_queue_depth": max_queue_depth,
        "max_active": max_active,
        "configured_active": configured_active,
        "configured_queue": int(run_config["client"]["queue_capacity"]),
        "target_queue_peak": target_queue_peak,
        "target_rejections": target_rejections,
        "target_error_fraction": target_error_rate,
        "network": netstats_summary or {},
    }
    if summary["diagnosis"] not in DIAGNOSES:
        summary["diagnosis"] = "inconclusive"
    return summary


def build_operating_envelope(point_summaries):
    stable_points = [
        item
        for item in point_summaries
        if float(item.get("success_fraction", 1.0)) >= 0.99 and float(item.get("queue_fraction", 0.0)) <= 0.25
    ]
    if not stable_points:
        return {"max_stable_rps": 0.0, "safe_active_budget": 0, "notes": ["No stable points met the default envelope criteria."]}
    best = max(stable_points, key=lambda item: item["achieved_rps"])
    return {
        "max_stable_rps": best["achieved_rps"],
        "safe_active_budget": best["safe_active_budget_estimate"],
        "notes": [
            f"Selected point with diagnosis={best['diagnosis']} and queue_fraction={best['queue_fraction']:.3f}.",
        ],
    }


def compare_point_summaries(points_a, points_b):
    by_id_a = {point["point_id"]: point for point in points_a}
    by_id_b = {point["point_id"]: point for point in points_b}
    point_ids = sorted(set(by_id_a) | set(by_id_b))
    rows = []
    for point_id in point_ids:
        item_a = by_id_a.get(point_id)
        item_b = by_id_b.get(point_id)
        rows.append(
            {
                "point_id": point_id,
                "rps_a": item_a["summary"]["achieved_rps"] if item_a else None,
                "rps_b": item_b["summary"]["achieved_rps"] if item_b else None,
                "diagnosis_a": item_a["summary"]["diagnosis"] if item_a else None,
                "diagnosis_b": item_b["summary"]["diagnosis"] if item_b else None,
            }
        )
    return {"rows": rows}


def write_json(path, payload):
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
