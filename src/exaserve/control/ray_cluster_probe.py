"""Bounded typed Ray membership/resource startup probe.

This finite adapter runs after every planned Ray child is observed RUNNING and
before the deployment child exists.  It uses only Ray's public driver API,
persists a versioned snapshot, and never derives the expected cluster from the
observed survivors.
"""

from __future__ import annotations

import argparse
import math
import time
from collections.abc import Mapping, Sequence
from typing import Any

from ..plan.contracts import canonical_node_id

PROBE_SCHEMA_VERSION = 1


class RayClusterProbeError(RuntimeError):
    """The probe artifact is absent, malformed, stale, or not ready."""


def _resource_number(value: object, *, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or value < 0
    ):
        raise RayClusterProbeError(f"{label} must be a finite nonnegative number")
    return float(value)


def _required_ray_text(node: Mapping[str, Any], keys: tuple[str, ...], *, label: str) -> str:
    for key in keys:
        value = node.get(key)
        if value is None or value == "":
            continue
        if not isinstance(value, str):
            raise RayClusterProbeError(f"Ray {label} field {key} must be a string")
        return value
    raise RayClusterProbeError(f"Ray node is missing {label}")


def _validate_normalized_nodes(nodes: object) -> list[Mapping[str, Any]]:
    if not isinstance(nodes, list):
        raise RayClusterProbeError("Ray nodes must be a list")
    fields = {"node_id", "node_name", "node_address", "alive", "cpu", "gpu"}
    seen: set[str] = set()
    result: list[Mapping[str, Any]] = []
    for index, node in enumerate(nodes):
        if not isinstance(node, Mapping) or set(node) != fields:
            raise RayClusterProbeError(f"Ray node {index} fields are invalid")
        for key in ("node_id", "node_name", "node_address"):
            if not isinstance(node[key], str) or not node[key]:
                raise RayClusterProbeError(f"Ray node {index} {key} is invalid")
        if node["node_id"] in seen:
            raise RayClusterProbeError(f"duplicate Ray NodeID {node['node_id']!r}")
        seen.add(node["node_id"])
        if type(node["alive"]) is not bool:
            raise RayClusterProbeError(f"Ray node {index} alive must be a boolean")
        _resource_number(node["cpu"], label=f"Ray node {index} CPU")
        _resource_number(node["gpu"], label=f"Ray node {index} GPU")
        result.append(node)
    return result


def _validate_cluster_resources(resources: object) -> Mapping[str, Any]:
    if not isinstance(resources, Mapping):
        raise RayClusterProbeError("Ray cluster resources must be an object")
    for key, value in resources.items():
        if not isinstance(key, str) or not key:
            raise RayClusterProbeError("Ray cluster resource names must be nonempty strings")
        _resource_number(value, label=f"Ray cluster resource {key}")
    return resources


def normalize_nodes(raw_nodes: Sequence[Mapping[str, Any]]) -> list[dict]:
    """Normalize public ``ray.nodes()`` data and reject malformed evidence."""
    if not isinstance(raw_nodes, Sequence) or isinstance(raw_nodes, (str, bytes)):
        raise RayClusterProbeError("ray.nodes() did not return a sequence")
    result = []
    for index, node in enumerate(raw_nodes):
        if not isinstance(node, Mapping):
            raise RayClusterProbeError(f"ray.nodes()[{index}] is not an object")
        resources = node.get("Resources", {})
        if not isinstance(resources, Mapping):
            raise RayClusterProbeError(f"ray.nodes()[{index}].Resources is not an object")
        alive = node.get("Alive")
        if type(alive) is not bool:
            raise RayClusterProbeError(f"ray.nodes()[{index}].Alive must be a boolean")
        result.append(
            {
                "node_id": _required_ray_text(node, ("NodeID",), label="NodeID"),
                "node_name": _required_ray_text(
                    node,
                    ("NodeManagerHostname", "NodeName", "NodeManagerAddress"),
                    label="node name",
                ),
                "node_address": _required_ray_text(
                    node, ("NodeManagerAddress", "ip"), label="node address"
                ),
                "alive": alive,
                "cpu": _resource_number(resources.get("CPU", 0), label=f"node {index} CPU"),
                "gpu": _resource_number(resources.get("GPU", 0), label=f"node {index} GPU"),
            }
        )
    _validate_normalized_nodes(result)
    return sorted(result, key=lambda item: (item["node_name"], item["node_id"]))


def evaluate_cluster(
    plan, binding, nodes: Sequence[Mapping[str, Any]], cluster_resources: Mapping[str, Any]
) -> tuple[bool, list[str]]:
    """Pure exact-plan predicate used by the finite adapter and unit tests."""
    blockers: list[str] = []
    validated_nodes = _validate_normalized_nodes(list(nodes))
    validated_resources = _validate_cluster_resources(cluster_resources)
    alive = [dict(node) for node in validated_nodes if node["alive"] is True]
    by_name: dict[str, list[dict]] = {}
    for node in alive:
        by_name.setdefault(canonical_node_id(node["node_name"]), []).append(node)
    matched_ids: set[str] = set()
    for rank, expected_name in binding.rank_to_node:
        matches = by_name.get(canonical_node_id(expected_name), [])
        if len(matches) != 1:
            blockers.append(f"rank {rank} node {expected_name}: {len(matches)} live matches")
            continue
        node = matches[0]
        node_id = node["node_id"]
        matched_ids.add(node_id)
        cpu = float(node["cpu"])
        gpu = float(node["gpu"])
        expected_cpu = float(plan.node_cpus)
        expected_gpu = float(plan.num_gpus_per_node)
        if cpu < expected_cpu or gpu < expected_gpu:
            blockers.append(
                f"rank {rank} resources CPU={cpu:g}/{expected_cpu:g} GPU={gpu:g}/{expected_gpu:g}"
            )
        elif not plan.readiness.allow_excess_resources and (
            cpu != expected_cpu or gpu != expected_gpu
        ):
            blockers.append(f"rank {rank} has forbidden excess resources CPU={cpu:g} GPU={gpu:g}")
    extras = sorted(node["node_id"] for node in alive if node["node_id"] not in matched_ids)
    if extras:
        blockers.append(f"unplanned live Ray nodes: {extras[:8]}")

    expected_cpu_total = float(plan.node_cpus * plan.num_nodes)
    expected_gpu_total = float(plan.num_gpus_per_node * plan.num_nodes)
    observed_cpu = _resource_number(validated_resources.get("CPU", 0), label="cluster CPU")
    observed_gpu = _resource_number(validated_resources.get("GPU", 0), label="cluster GPU")
    if observed_cpu < expected_cpu_total or observed_gpu < expected_gpu_total:
        blockers.append(
            f"cluster totals CPU={observed_cpu:g}/{expected_cpu_total:g} "
            f"GPU={observed_gpu:g}/{expected_gpu_total:g}"
        )
    elif not plan.readiness.allow_excess_resources and (
        observed_cpu != expected_cpu_total or observed_gpu != expected_gpu_total
    ):
        blockers.append(
            f"cluster totals have forbidden excess CPU={observed_cpu:g} GPU={observed_gpu:g}"
        )
    return not blockers, blockers


def collect_until_ready(*, plan, binding, address: str, timeout_s: float, poll_s: float) -> dict:
    """Connect once, then perform bounded startup-only public-state queries."""
    import ray

    deadline = time.monotonic() + timeout_s
    ray.init(
        address=address,
        namespace="exaserve-cluster-probe",
        ignore_reinit_error=False,
        log_to_driver=False,
    )
    try:
        snapshot: dict[str, Any] = {}
        while True:
            nodes = normalize_nodes(ray.nodes())
            raw_resources = ray.cluster_resources()
            resources = {
                key: _resource_number(value, label=f"cluster resource {key!r}")
                for key, value in _validate_cluster_resources(raw_resources).items()
            }
            ready, blockers = evaluate_cluster(plan, binding, nodes, resources)
            snapshot = {
                "schema_version": PROBE_SCHEMA_VERSION,
                "deployment_id": plan.deployment_id,
                "generation": binding.generation,
                "deployment_plan_hash": plan.deployment_plan_hash,
                "site_profile_hash": plan.site_profile_hash,
                "allocation_binding_hash": binding.allocation_binding_hash,
                "observed_at": time.time(),
                "ready": ready,
                "blockers": blockers,
                "nodes": nodes,
                "cluster_resources": resources,
            }
            if ready or time.monotonic() >= deadline:
                return snapshot
            time.sleep(min(poll_s, max(0.0, deadline - time.monotonic())))
    finally:
        ray.shutdown()


def load_probe_snapshot(path: str, *, plan, binding) -> dict:
    """Strictly bind a finite probe result to this exact generation."""
    from ..state.atomic import strict_json_load_path

    try:
        payload = strict_json_load_path(path)
    except (OSError, ValueError) as exc:
        raise RayClusterProbeError(f"Ray cluster probe artifact could not be read: {exc}") from exc
    expected = {
        "schema_version": PROBE_SCHEMA_VERSION,
        "deployment_id": plan.deployment_id,
        "generation": binding.generation,
        "deployment_plan_hash": plan.deployment_plan_hash,
        "site_profile_hash": plan.site_profile_hash,
        "allocation_binding_hash": binding.allocation_binding_hash,
    }
    if not isinstance(payload, dict):
        raise RayClusterProbeError("Ray cluster probe must be an object")
    if type(payload.get("schema_version")) is not int:
        raise RayClusterProbeError("Ray cluster probe schema_version must be an integer")
    if type(payload.get("generation")) is not int:
        raise RayClusterProbeError("Ray cluster probe generation must be an integer")
    mismatches = [key for key, value in expected.items() if payload.get(key) != value]
    if mismatches:
        raise RayClusterProbeError(f"Ray cluster probe identity mismatch: {mismatches}")
    allowed = {*expected, "observed_at", "ready", "blockers", "nodes", "cluster_resources"}
    if set(payload) != allowed:
        raise RayClusterProbeError("Ray cluster probe fields do not match schema version 1")
    if (
        not isinstance(payload.get("ready"), bool)
        or not isinstance(payload.get("blockers"), list)
        or not all(isinstance(item, str) for item in payload["blockers"])
        or not isinstance(payload.get("nodes"), list)
        or not isinstance(payload.get("cluster_resources"), dict)
        or isinstance(payload.get("observed_at"), bool)
        or not isinstance(payload.get("observed_at"), (int, float))
        or not math.isfinite(float(payload["observed_at"]))
        or payload["observed_at"] <= 0
    ):
        raise RayClusterProbeError("Ray cluster probe payload shape is invalid")
    _validate_normalized_nodes(payload["nodes"])
    _validate_cluster_resources(payload["cluster_resources"])
    ready, blockers = evaluate_cluster(
        plan, binding, payload["nodes"], payload["cluster_resources"]
    )
    if ready != payload["ready"] or blockers != payload["blockers"]:
        raise RayClusterProbeError("Ray cluster probe verdict does not match its observations")
    return payload


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="exaserve-ray-cluster-probe")
    parser.add_argument("--plan", required=True)
    parser.add_argument("--binding", required=True)
    parser.add_argument("--address", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--timeout", required=True, type=float)
    parser.add_argument("--poll", required=True, type=float)
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.poll <= 0:
        parser.error("timeout and poll must be positive")

    from ..plan.io import load_allocation_binding, load_deployment_plan
    from ..state.atomic import atomic_write_json

    plan = load_deployment_plan(args.plan)
    binding = load_allocation_binding(args.binding)
    if (
        binding.deployment_id != plan.deployment_id
        or binding.deployment_plan_hash != plan.deployment_plan_hash
        or binding.site_profile_hash != plan.site_profile_hash
    ):
        raise RuntimeError("allocation binding does not belong to deployment plan")
    snapshot = collect_until_ready(
        plan=plan, binding=binding, address=args.address, timeout_s=args.timeout, poll_s=args.poll
    )
    atomic_write_json(args.output, snapshot)
    return 0 if snapshot["ready"] else 3


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "PROBE_SCHEMA_VERSION",
    "RayClusterProbeError",
    "collect_until_ready",
    "evaluate_cluster",
    "load_probe_snapshot",
    "normalize_nodes",
]
