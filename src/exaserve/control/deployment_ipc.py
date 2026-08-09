"""Versioned fault-isolation IPC for Serve application observations.

The canonical design prefers an in-process DeploymentManager.  Until that
cutover is complete, WP4 permits exactly one isolated deployment child behind
a narrow structured local protocol. This module is that protocol. It is not
the cross-rank authority: the allocation-head supervisor validates the kernel
peer PID and complete identity tuple and feeds its sole readiness coordinator.
"""

from __future__ import annotations

import math
import os
import re
from typing import Callable, Optional

from .local_ipc import (
    MAX_FRAME_BYTES,
    BoundedUnixIngress,
    deliver_object,
    scoped_socket_path,
)

SOCKET_ENV = "EXASERVE_DEPLOYMENT_OBSERVATION_SOCKET"
PAYLOAD_VERSION = 2
KIND = "SERVE_APPLICATION_SNAPSHOT"
MAX_APPLICATIONS = 4096
_HASH = re.compile(r"[0-9a-f]{64}")


class DeploymentIPCError(ValueError):
    pass


def socket_path_for(deployment_id: str, generation: int, root: Optional[str] = None) -> str:
    return scoped_socket_path(deployment_id, generation, "deployment-observations.sock", root=root)


class DeploymentObservationIngress(BoundedUnixIngress):
    def __init__(
        self,
        path: str,
        *,
        max_frame_bytes: int = MAX_FRAME_BYTES,
        max_queued: int = 128,
        log: Callable[[str], None] = print,
    ) -> None:
        super().__init__(
            path,
            max_frame_bytes=max_frame_bytes,
            max_queued=max_queued,
            log=log,
            label="DeploymentIPC",
        )


def deliver_snapshot(payload: dict, *, path: Optional[str] = None, timeout_s: float = 10.0) -> bool:
    target = path or os.environ.get(SOCKET_ENV, "")
    return deliver_object(payload, path=target, timeout_s=timeout_s)


def validate_snapshot(
    payload: dict,
    *,
    deployment_id: str,
    generation: int,
    deployment_plan_hash: str,
    site_profile_hash: str,
    allocation_binding_hash: str,
) -> dict:
    """Validate the complete v2 contract and exact generation identity."""
    fields = {
        "payload_version",
        "kind",
        "deployment_id",
        "generation",
        "deployment_plan_hash",
        "site_profile_hash",
        "allocation_binding_hash",
        "applications",
        "nodes",
        "proxies",
        "observed_at",
    }
    if not isinstance(payload, dict) or set(payload) != fields:
        raise DeploymentIPCError("deployment snapshot fields do not match v2")
    if (
        type(payload["payload_version"]) is not int
        or payload["payload_version"] != PAYLOAD_VERSION
        or not isinstance(payload["kind"], str)
        or payload["kind"] != KIND
    ):
        raise DeploymentIPCError("unsupported deployment snapshot version/kind")
    identities = {
        "deployment_id": deployment_id,
        "generation": generation,
        "deployment_plan_hash": deployment_plan_hash,
        "site_profile_hash": site_profile_hash,
        "allocation_binding_hash": allocation_binding_hash,
    }
    for key, expected in identities.items():
        if key == "generation":
            if type(payload[key]) is not int:
                raise DeploymentIPCError("deployment snapshot generation must be an integer")
        elif not isinstance(payload[key], str) or not payload[key]:
            raise DeploymentIPCError(f"deployment snapshot {key} must be a nonempty string")
        if payload[key] != expected:
            raise DeploymentIPCError(f"deployment snapshot {key} mismatch")
    for key in ("deployment_plan_hash", "site_profile_hash", "allocation_binding_hash"):
        if not isinstance(payload[key], str) or not _HASH.fullmatch(payload[key]):
            raise DeploymentIPCError(f"deployment snapshot {key} is not SHA-256")
    observed_at = payload["observed_at"]
    if (
        isinstance(observed_at, bool)
        or not isinstance(observed_at, (int, float))
        or not math.isfinite(observed_at)
        or observed_at <= 0
    ):
        raise DeploymentIPCError("deployment snapshot observed_at is invalid")
    nodes = payload["nodes"]
    if not isinstance(nodes, list) or len(nodes) > MAX_APPLICATIONS:
        raise DeploymentIPCError("deployment snapshot nodes are invalid")
    node_fields = {
        "node_id",
        "node_name",
        "node_address",
        "alive",
        "cpu",
        "gpu",
    }
    normalized_nodes = []
    seen_node_ids = set()
    for index, node in enumerate(nodes):
        if not isinstance(node, dict) or set(node) != node_fields:
            raise DeploymentIPCError(f"deployment snapshot nodes[{index}] fields are invalid")
        for key in ("node_id", "node_name", "node_address"):
            if not isinstance(node[key], str) or not node[key]:
                raise DeploymentIPCError(f"deployment snapshot nodes[{index}].{key} is invalid")
        if node["node_id"] in seen_node_ids:
            raise DeploymentIPCError("deployment snapshot has duplicate node_id")
        seen_node_ids.add(node["node_id"])
        if not isinstance(node["alive"], bool):
            raise DeploymentIPCError(f"deployment snapshot nodes[{index}].alive is invalid")
        for key in ("cpu", "gpu"):
            value = node[key]
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise DeploymentIPCError(f"deployment snapshot nodes[{index}].{key} is invalid")
        normalized_nodes.append(dict(node))
    proxies = payload["proxies"]
    if not isinstance(proxies, list) or len(proxies) > MAX_APPLICATIONS:
        raise DeploymentIPCError("deployment snapshot proxies are invalid")
    normalized_proxies = []
    seen_proxy_nodes = set()
    for index, proxy in enumerate(proxies):
        if (
            not isinstance(proxy, dict)
            or set(proxy) != {"node_id", "status"}
            or not isinstance(proxy["node_id"], str)
            or not proxy["node_id"]
            or not isinstance(proxy["status"], str)
            or not proxy["status"]
        ):
            raise DeploymentIPCError(f"deployment snapshot proxies[{index}] is invalid")
        if proxy["node_id"] in seen_proxy_nodes:
            raise DeploymentIPCError("deployment snapshot has duplicate proxy node_id")
        seen_proxy_nodes.add(proxy["node_id"])
        normalized_proxies.append(dict(proxy))
    applications = payload["applications"]
    if not isinstance(applications, dict) or len(applications) > MAX_APPLICATIONS:
        raise DeploymentIPCError("deployment snapshot applications are invalid")
    normalized = {}
    app_fields = {"running", "target", "route_prefix", "status"}
    for name, info in applications.items():
        if not isinstance(name, str) or not name or len(name) > 256:
            raise DeploymentIPCError("deployment snapshot app name is invalid")
        if not isinstance(info, dict) or set(info) != app_fields:
            raise DeploymentIPCError(f"application {name!r} fields are invalid")
        running, target = info["running"], info["target"]
        if (
            isinstance(running, bool)
            or not isinstance(running, int)
            or running < 0
            or isinstance(target, bool)
            or not isinstance(target, int)
            or target < 0
        ):
            raise DeploymentIPCError(f"application {name!r} counts are invalid")
        route_prefix = info["route_prefix"]
        if route_prefix is not None and (
            not isinstance(route_prefix, str)
            or not route_prefix.startswith("/")
            or len(route_prefix) > 1024
        ):
            raise DeploymentIPCError(f"application {name!r} route is invalid")
        if not isinstance(info["status"], str) or not info["status"] or len(info["status"]) > 128:
            raise DeploymentIPCError(f"application {name!r} status is invalid")
        normalized[name] = dict(info)
    return {
        **payload,
        "applications": normalized,
        "nodes": normalized_nodes,
        "proxies": normalized_proxies,
    }
