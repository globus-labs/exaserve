"""Render the two-trial v0.4 HAProxy null-compute startup curve.

The run groups are deliberately pinned. A later retry receives a new immutable
identity and must be reviewed before this consumer is changed; it never selects
"latest" data. Result manifests and terminal run state are verified before any
number is returned.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from statistics import mean, stdev

from eval.lib.paper_acceptance import require_accepted_paper_run
from eval.site_config import get_runs_root
from exaserve.state.atomic import strict_json_load_path

SPEC_NAME = "nullcompute_haproxy_scale_to256_v040"
RUN_GROUPS = ("run4", "run5")
NODE_COUNTS = (32, 64, 128, 256)
EXPECTED_RESULT_IDS = {
    "deployment_ready_evidence",
    "compatibility_receipts",
    "run_provenance",
    "startup_scaling_trace",
    "startup_metrics",
    "deployment_shutdown_report",
    "deployment_terminal_status",
}


def _finite_nonnegative(value, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise RuntimeError(f"{label} must be finite and non-negative")
    return float(value)


def _load_trial(root: Path, run_group: str, nodes: int) -> dict:
    cell = root / SPEC_NAME / run_group / f"n{nodes}"
    accepted = require_accepted_paper_run(cell, required_result_ids=EXPECTED_RESULT_IDS)
    manifest = accepted.manifest
    run_plan = accepted.run_plan
    run_provenance = accepted.run_provenance
    if set(manifest.expected_ids) != EXPECTED_RESULT_IDS:
        raise RuntimeError(f"null-compute paper cell has an incomplete manifest: {cell}")
    entries = {entry.logical_id: entry for entry in manifest.entries}
    metrics_path = cell / "results" / entries["startup_metrics"].path
    metrics = strict_json_load_path(metrics_path)
    if (
        not isinstance(metrics, dict)
        or metrics.get("schema_version") != 2
        or metrics.get("num_nodes") != nodes
        or metrics.get("null_compute") is not True
        or metrics.get("serve_application_layout") != "node_grouped_null"
        or metrics.get("expected_model_replicas") != 12 * nodes
        or metrics.get("replica_measurement_count") != 12 * nodes
        or metrics.get("expected_serve_applications") != 2 * nodes
        or metrics.get("expected_receipt_requirements") != 14 * nodes + 2
        or metrics.get("generation") != run_provenance.generation
        or metrics.get("run_semantic_hash") != run_plan.run_semantic_hash
        or metrics.get("deployment_plan_hash") != run_plan.deployment_plan_hash
        or metrics.get("source_snapshot_hash") != run_plan.source_snapshot_hash
        or metrics.get("deployment_ready_evidence_sha256")
        != entries["deployment_ready_evidence"].sha256
    ):
        raise RuntimeError(f"null-compute startup metrics identity is invalid: {cell}")
    ready = strict_json_load_path(cell / "results" / entries["deployment_ready_evidence"].path)
    terminal = strict_json_load_path(cell / "results" / entries["deployment_terminal_status"].path)
    for label, evidence, state in (
        ("READY", ready, "READY"),
        ("terminal", terminal, "STOPPED"),
    ):
        if (
            not isinstance(evidence, dict)
            or evidence.get("schema_version") != 2
            or evidence.get("state") != state
            or evidence.get("generation") != run_provenance.generation
            or evidence.get("deployment_plan_hash") != run_plan.deployment_plan_hash
            or evidence.get("run_semantic_hash") != run_plan.run_semantic_hash
            or evidence.get("run_provenance_hash") != run_provenance.run_provenance_hash
        ):
            raise RuntimeError(f"null-compute {label} evidence identity is invalid: {cell}")
    source_hash = metrics.get("source_snapshot_hash")
    if (
        not isinstance(source_hash, str)
        or len(source_hash) != 64
        or any(char not in "0123456789abcdef" for char in source_hash)
    ):
        raise RuntimeError(f"null-compute source snapshot identity is invalid: {cell}")
    phase_timings = metrics.get("phase_timings")
    if not isinstance(phase_timings, list):
        raise RuntimeError(f"null-compute phase timings are invalid: {cell}")
    phases = {}
    for index, phase in enumerate(phase_timings):
        if (
            not isinstance(phase, dict)
            or set(phase) != {"name", "duration_s"}
            or not isinstance(phase["name"], str)
            or not phase["name"]
            or phase["name"] in phases
        ):
            raise RuntimeError(f"null-compute phase[{index}] is invalid: {cell}")
        phases[phase["name"]] = _finite_nonnegative(
            phase["duration_s"], f"{cell} phase {phase['name']}"
        )
    for required in ("deploy_from_canonical_plan", "stage3.total"):
        if required not in phases:
            raise RuntimeError(f"null-compute phase {required!r} is missing: {cell}")
    return {
        "run_group": run_group,
        "nodes": nodes,
        "source_snapshot_hash": source_hash,
        "generation": run_provenance.generation,
        "run_provenance_hash": run_provenance.run_provenance_hash,
        "ready_s": _finite_nonnegative(metrics.get("ready_after_trace_start_s"), f"{cell} ready_s"),
        "trace_s": _finite_nonnegative(metrics.get("trace_total_duration_s"), f"{cell} trace_s"),
        "deploy_s": phases["deploy_from_canonical_plan"],
        "stage3_s": phases["stage3.total"],
    }


def load_curve(runs_root: Path | None = None) -> list[dict]:
    root = (runs_root or get_runs_root()) / "sc26workshop" / "full"
    trials = {
        nodes: [_load_trial(root, run_group, nodes) for run_group in RUN_GROUPS]
        for nodes in NODE_COUNTS
    }
    source_hashes = {
        trial["source_snapshot_hash"] for node_trials in trials.values() for trial in node_trials
    }
    if len(source_hashes) != 1:
        raise RuntimeError(f"null-compute trials mix source snapshots: {sorted(source_hashes)}")
    for nodes, node_trials in trials.items():
        generations = {trial["generation"] for trial in node_trials}
        provenance_hashes = {trial["run_provenance_hash"] for trial in node_trials}
        if len(generations) != len(RUN_GROUPS) or len(provenance_hashes) != len(RUN_GROUPS):
            raise RuntimeError(f"null-compute n{nodes} trials are not independent lifecycles")
    rows = []
    for nodes, node_trials in trials.items():
        ready = [trial["ready_s"] for trial in node_trials]
        trace = [trial["trace_s"] for trial in node_trials]
        deploy = [trial["deploy_s"] for trial in node_trials]
        stage3 = [trial["stage3_s"] for trial in node_trials]
        rows.append(
            {
                "nodes": nodes,
                "replicas": nodes * 12,
                "run_groups": list(RUN_GROUPS),
                "ready_trials_s": ready,
                "ready_mean_s": mean(ready),
                "ready_sample_std_s": stdev(ready),
                "trace_trials_s": trace,
                "trace_mean_s": mean(trace),
                "deploy_trials_s": deploy,
                "deploy_mean_s": mean(deploy),
                "stage3_trials_s": stage3,
                "stage3_mean_s": mean(stage3),
                "generations": [trial["generation"] for trial in node_trials],
                "run_provenance_hashes": [trial["run_provenance_hash"] for trial in node_trials],
                "source_snapshot_hash": next(iter(source_hashes)),
            }
        )
    return rows


def _markdown(rows: list[dict]) -> str:
    lines = [
        "| nodes | replicas | READY trials (s) | READY mean ± sample std (s) | trace trials (s) | stage3 trials (s) | deploy trials (s) |",
        "|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in rows:
        lines.append(
            f"| {row['nodes']} | {row['replicas']} | "
            f"{', '.join(f'{value:.3f}' for value in row['ready_trials_s'])} | "
            f"{row['ready_mean_s']:.3f} ± {row['ready_sample_std_s']:.3f} | "
            f"{', '.join(f'{value:.3f}' for value in row['trace_trials_s'])} | "
            f"{', '.join(f'{value:.3f}' for value in row['stage3_trials_s'])} | "
            f"{', '.join(f'{value:.3f}' for value in row['deploy_trials_s'])} |"
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit JSON instead of Markdown")
    args = parser.parse_args()
    rows = load_curve()
    print(json.dumps(rows, indent=2, sort_keys=True) if args.json else _markdown(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
