"""The shared deployment-status surface (plan §3.4 boundary table, WP9).

The plan's boundary table is explicit about how evaluation and ClientLab may
learn what a deployment is doing:

    Eval/ClientLab -> deployment | Shared `DeploymentStatus`/event API |
    Generation-specific terminal/readiness state; no private process monitor
    or log grep

`state/status.py` already implemented the durable, lease-guarded, CAS-checked
record that boundary needs — and nothing wrote to it or read from it. The
composition root persisted its own `readiness.json`, so every consumer that
wanted deployment state either parsed that file's private shape or fell back to
grepping a log line, which is the untyped coupling the whole migration exists
to remove.

This module is the two halves of that boundary:

* `DeploymentStatusPublisher` — the writer. Only the composition root holds
  one; each transition is CAS-guarded, so a stale writer fails closed instead
  of overwriting a newer generation's state.
* `read_deployment_status` / `require_ready_endpoint` — the reader. Consumers
  get a typed record and the *compiled advertised endpoint*, and a deployment
  that is not READY raises rather than handing back an endpoint that may be
  serving nothing.

The record is generation-specific by construction: the generation, plan hash
and binding hash live in its provenance, so a consumer that reads a status
file left over from a previous generation can detect it rather than trusting
a filename.
"""

from __future__ import annotations

import os
import re
import stat
import time
import math
from dataclasses import dataclass
from typing import Any, Optional

from .state.status import (
    DeploymentState,
    IllegalTransition,
    StatusConflict,
    StatusRecord,
    StatusStore,
)

STATUS_FILENAME = "deployment_status.json"
ALLOCATION_BINDING_FILENAME = "allocation_binding.json"


class DeploymentNotReady(RuntimeError):
    """The deployment exists but is not in a state a client may use."""


class StatusPublicationError(RuntimeError):
    """The authoritative lifecycle record could not be durably advanced."""


class InvalidDeploymentStatus(RuntimeError):
    """A status artifact exists but violates the public typed contract."""


_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_READY_SNAPSHOT_FIELDS = {
    "ready",
    "phase",
    "blockers",
    "satisfied",
    "advertised_endpoint",
    "generation",
    "deployment_plan_hash",
    "allocation_binding_hash",
    "missing_identities",
    "unhealthy_identities",
    "receipt_hashes",
    "model_map",
    "capability_map",
    "nodes",
    "proxies",
    "observed_at",
    "receipt_manifest_path",
    "receipt_manifest_hash",
    "lease_expires_at",
}


def _string_sequence(value: Any, name: str, *, sorted_unique: bool) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) or not item for item in value):
        raise ValueError(f"{name} must be a list of non-empty strings")
    if len(value) != len(set(value)):
        raise ValueError(f"{name} must not contain duplicates")
    if sorted_unique and value != sorted(value):
        raise ValueError(f"{name} must be sorted")
    return value


def _validate_model_map(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("model_map must be an object")
    for model_id, info in value.items():
        if not isinstance(model_id, str) or not model_id:
            raise ValueError("model_map keys must be non-empty strings")
        if not isinstance(info, dict) or set(info) != {
            "route_name",
            "expected_replicas",
            "observed_replicas",
            "observed_target",
        }:
            raise ValueError(f"model_map[{model_id!r}] has invalid fields")
        if not isinstance(info["route_name"], str) or not info["route_name"]:
            raise ValueError(f"model_map[{model_id!r}].route_name is invalid")
        for field in ("expected_replicas", "observed_replicas", "observed_target"):
            count = info[field]
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise ValueError(f"model_map[{model_id!r}].{field} is invalid")
    return value


def _validate_capability_map(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError("capability_map must be an object")
    for component_id, capabilities in value.items():
        if not isinstance(component_id, str) or not component_id:
            raise ValueError("capability_map keys must be non-empty strings")
        _string_sequence(capabilities, f"capability_map[{component_id!r}]", sorted_unique=True)
    return value


def _validate_cluster_projection(
    nodes: Any,
    proxies: Any,
    *,
    expected_nodes: int,
    expected_proxy_count: int,
) -> None:
    if not isinstance(nodes, list) or len(nodes) != expected_nodes:
        raise ValueError(f"nodes must contain exactly {expected_nodes} planned entries")
    node_fields = {"node_id", "node_name", "node_address", "alive", "cpu", "gpu"}
    node_ids: list[str] = []
    for index, node in enumerate(nodes):
        if not isinstance(node, dict) or set(node) != node_fields:
            raise ValueError(f"nodes[{index}] has invalid fields")
        for field in ("node_id", "node_name", "node_address"):
            if not isinstance(node[field], str) or not node[field]:
                raise ValueError(f"nodes[{index}].{field} is invalid")
        if node["alive"] is not True:
            raise ValueError(f"nodes[{index}] is not alive")
        for field in ("cpu", "gpu"):
            amount = node[field]
            if (
                isinstance(amount, bool)
                or not isinstance(amount, (int, float))
                or not math.isfinite(float(amount))
                or amount < 0
            ):
                raise ValueError(f"nodes[{index}].{field} is invalid")
        node_ids.append(node["node_id"])
    if len(node_ids) != len(set(node_ids)):
        raise ValueError("nodes contain duplicate node_id values")
    if not isinstance(proxies, list) or len(proxies) != expected_proxy_count:
        raise ValueError(f"proxies must contain exactly {expected_proxy_count} planned entries")
    proxy_ids: list[str] = []
    for index, proxy in enumerate(proxies):
        if (
            not isinstance(proxy, dict)
            or set(proxy) != {"node_id", "status"}
            or not isinstance(proxy["node_id"], str)
            or not proxy["node_id"]
            or proxy["status"] != "HEALTHY"
        ):
            raise ValueError(f"proxies[{index}] is not a typed healthy proxy")
        proxy_ids.append(proxy["node_id"])
    if len(proxy_ids) != len(set(proxy_ids)):
        raise ValueError("proxies contain duplicate node_id values")
    if not set(proxy_ids).issubset(node_ids):
        raise ValueError("proxy node identities are not planned node identities")
    if expected_proxy_count == expected_nodes and set(proxy_ids) != set(node_ids):
        raise ValueError("proxy node identities do not exactly match nodes")


def _validate_ready_payload(
    snapshot: Any,
    *,
    model_map: Any,
    capability_map: Any,
    num_nodes: int,
    exposure_mode: str,
    plan=None,
) -> None:
    """Validate the complete public READY snapshot, without coercion."""
    if not isinstance(snapshot, dict) or set(snapshot) != _READY_SNAPSHOT_FIELDS:
        raise ValueError("READY readiness_snapshot fields do not match schema v1")
    if snapshot["ready"] is not True or snapshot["phase"] != "READY":
        raise ValueError("READY snapshot state is inconsistent")
    for field, sorted_unique in (
        ("blockers", False),
        ("satisfied", False),
        ("missing_identities", True),
        ("unhealthy_identities", True),
        ("receipt_hashes", True),
    ):
        _string_sequence(snapshot[field], field, sorted_unique=sorted_unique)
    if snapshot["blockers"] or snapshot["missing_identities"] or snapshot["unhealthy_identities"]:
        raise ValueError("READY snapshot retains blocking identities")
    if any(not _SHA256.fullmatch(item) for item in snapshot["receipt_hashes"]):
        raise ValueError("READY snapshot receipt_hashes are invalid")
    observed_at = snapshot["observed_at"]
    if (
        isinstance(observed_at, bool)
        or not isinstance(observed_at, (int, float))
        or not math.isfinite(float(observed_at))
        or observed_at <= 0
    ):
        raise ValueError("READY snapshot observed_at is invalid")
    _validate_model_map(model_map)
    _validate_capability_map(capability_map)
    if snapshot["model_map"] != model_map:
        raise ValueError("READY snapshot model_map disagrees with status data")
    if snapshot["capability_map"] != capability_map:
        raise ValueError("READY snapshot capability_map disagrees with status data")
    if plan is not None and exposure_mode != plan.exposure.mode:
        raise ValueError("READY exposure mode disagrees with the plan")
    expected_proxy_count = 1 if exposure_mode == "RAY_SERVE_HEAD_ONLY" else num_nodes
    _validate_cluster_projection(
        snapshot["nodes"],
        snapshot["proxies"],
        expected_nodes=num_nodes,
        expected_proxy_count=expected_proxy_count,
    )
    if plan is not None:
        expected_models = {model.model_id: model for model in plan.models}
        if set(model_map) != set(expected_models):
            raise ValueError("READY model_map does not exactly match the plan")
        for model_id, model in expected_models.items():
            info = model_map[model_id]
            if (
                info["route_name"] != model.route_name
                or info["expected_replicas"] != model.num_replicas
                or info["observed_target"] != model.num_replicas
                or info["observed_replicas"] < model.num_replicas
            ):
                raise ValueError(f"READY model_map[{model_id!r}] disagrees with the plan")


def status_path(run_dir: str) -> str:
    return os.path.join(run_dir, STATUS_FILENAME)


class DeploymentStatusPublisher:
    """Writer side. One per generation, owned by the composition root."""

    def __init__(
        self,
        run_dir: str,
        *,
        plan,
        binding=None,
        generation: int = 0,
        run_provenance=None,
        log=print,
    ) -> None:
        self.path = status_path(run_dir)
        self.store = StatusStore.deployment(self.path)
        self.plan = plan
        self.binding = binding
        self.generation = generation
        self.run_provenance = run_provenance
        self._log = log
        self._state = DeploymentState.PLANNED
        self._revision = -1

    # -- lifecycle ---------------------------------------------------------
    def initialize(self) -> StatusRecord:
        """Create the record. A pre-existing one is NOT silently reused.

        Two generations writing one status file is how a stale READY survives
        into a run that never reached it.  A conflict is therefore fatal to the
        new generation; continuing with an inert publisher would run a serving
        deployment whose state no client can safely observe.
        """
        provenance = {
            "deployment_id": self.plan.deployment_id,
            "generation": self.generation,
            "deployment_plan_hash": self.plan.deployment_plan_hash,
            "site_profile_hash": self.plan.site_profile_hash,
            "allocation_binding_hash": (
                self.binding.allocation_binding_hash if self.binding else ""
            ),
            "run_id": (self.run_provenance.run_id if self.run_provenance is not None else ""),
            "run_semantic_hash": (
                self.run_provenance.run_semantic_hash if self.run_provenance is not None else None
            ),
            "run_provenance_hash": (
                self.run_provenance.run_provenance_hash if self.run_provenance is not None else ""
            ),
        }
        try:
            record = self.store.initialize(
                f"{self.plan.deployment_id}/gen{self.generation}",
                DeploymentState.PLANNED,
                provenance=provenance,
                data={"exposure_mode": self.plan.exposure.mode, "num_nodes": self.plan.num_nodes},
            )
        except (StatusConflict, OSError) as exc:
            raise StatusPublicationError(
                f"could not initialize authoritative deployment status: {exc}"
            ) from exc
        self._state = DeploymentState.PLANNED
        self._revision = record.revision
        return record

    def advance(
        self, new: DeploymentState, *, reason_code: str, detail: str = "", **data: Any
    ) -> StatusRecord:
        """One fail-closed CAS-guarded lifecycle step."""
        if self._revision < 0:
            raise StatusPublicationError("authoritative deployment status was not initialized")
        try:
            target = DeploymentState(new)
            if target is DeploymentState.READY:
                if self.binding is None:
                    raise ValueError("READY requires an AllocationBinding")
                endpoint = data.get("advertised_endpoint")
                if not isinstance(endpoint, str) or not endpoint:
                    raise ValueError("READY requires a non-empty advertised endpoint")
                hashes = data.get("receipt_hashes", [])
                _string_sequence(hashes, "READY receipt_hashes", sorted_unique=True)
                if any(not _SHA256.fullmatch(value) for value in hashes):
                    raise ValueError("READY receipt_hashes must contain SHA-256 values")
                snapshot = data.get("readiness_snapshot")
                if not isinstance(snapshot, dict):
                    raise ValueError(
                        "READY requires a readiness_snapshot object with a "
                        "receipt manifest identity"
                    )
                manifest_path = snapshot.get("receipt_manifest_path")
                manifest_hash = snapshot.get("receipt_manifest_hash")
                if (
                    not isinstance(manifest_path, str)
                    or not manifest_path
                    or not isinstance(manifest_hash, str)
                    or not _SHA256.fullmatch(manifest_hash)
                ):
                    raise ValueError("READY requires an immutable receipt manifest path/hash")
                from .state.receipts import (
                    ReceiptManifestError,
                    load_receipt_manifest,
                )

                try:
                    manifest = load_receipt_manifest(manifest_path)
                except ReceiptManifestError as exc:
                    raise ValueError(f"READY receipt manifest is invalid: {exc}") from exc
                if (
                    manifest.manifest_hash != manifest_hash
                    or manifest.deployment_id != self.plan.deployment_id
                    or manifest.generation != self.generation
                    or manifest.deployment_plan_hash != self.plan.deployment_plan_hash
                    or manifest.allocation_binding_hash != self.binding.allocation_binding_hash
                    or list(manifest.receipt_hashes) != hashes
                ):
                    raise ValueError("READY receipt manifest disagrees with current identity")
                snapshot.update(
                    {
                        "ready": True,
                        "phase": DeploymentState.READY.value,
                        "advertised_endpoint": endpoint,
                        "generation": self.generation,
                        "deployment_plan_hash": self.plan.deployment_plan_hash,
                        "allocation_binding_hash": self.binding.allocation_binding_hash,
                        "receipt_hashes": hashes,
                        "lease_expires_at": self._ready_lease_expiry(),
                    }
                )
                model_map = data.get("model_map")
                capability_map = data.get("capability_map")
                _validate_ready_payload(
                    snapshot,
                    model_map=model_map,
                    capability_map=capability_map,
                    num_nodes=self.plan.num_nodes,
                    exposure_mode=self.plan.exposure.mode,
                    plan=self.plan,
                )
                data["readiness_snapshot"] = snapshot
            else:
                snapshot = data.get("readiness_snapshot")
                if not isinstance(snapshot, dict):
                    current = self.store.load()
                    previous = (
                        (current.data or {}).get("readiness_snapshot", {})
                        if current is not None
                        else {}
                    )
                    snapshot = dict(previous) if isinstance(previous, dict) else {}
                snapshot.update(
                    {
                        "ready": False,
                        "phase": target.value,
                        "observed_at": time.time(),
                    }
                )
                data["readiness_snapshot"] = snapshot
            record = self.store.transition(
                self._state,
                new,
                reason_code=reason_code,
                detail=detail or None,
                data_update=data or None,
                expected_revision=self._revision,
            )
        except (StatusConflict, IllegalTransition, OSError, ValueError) as exc:
            raise StatusPublicationError(
                f"authoritative status {self._state.value} -> "
                f"{DeploymentState(new).value} failed: {exc}"
            ) from exc
        self._state = DeploymentState(new)
        self._revision = record.revision
        return record

    def _ready_lease_expiry(self) -> float:
        interval = float(self.plan.readiness.validation_interval_s)
        freshness = float(self.plan.readiness.observation_freshness_s)
        return time.time() + max(freshness, 3.0 * interval)

    def refresh_ready(
        self,
        *,
        readiness_snapshot: dict[str, Any],
        model_map: dict[str, Any],
        capability_map: dict[str, Any],
        receipt_hashes: list[str],
    ) -> StatusRecord:
        """Renew READY evidence without fabricating a lifecycle transition."""
        if self._state is not DeploymentState.READY:
            raise StatusPublicationError(
                f"cannot refresh READY while publisher state is {self._state.value}"
            )
        current = self.store.load()
        if current is None:
            raise StatusPublicationError("READY status disappeared before refresh")
        previous = dict(current.data.get("readiness_snapshot", {}))
        for field in ("receipt_manifest_path", "receipt_manifest_hash"):
            if not previous.get(field):
                raise StatusPublicationError(f"READY status lost immutable {field}")
        snapshot = dict(readiness_snapshot)
        snapshot.update(
            {
                "ready": True,
                "phase": DeploymentState.READY.value,
                "advertised_endpoint": current.data.get("advertised_endpoint", ""),
                "generation": self.generation,
                "deployment_plan_hash": self.plan.deployment_plan_hash,
                "allocation_binding_hash": self.binding.allocation_binding_hash,
                "receipt_hashes": receipt_hashes,
                "receipt_manifest_path": previous["receipt_manifest_path"],
                "receipt_manifest_hash": previous["receipt_manifest_hash"],
                "lease_expires_at": self._ready_lease_expiry(),
            }
        )
        try:
            _validate_ready_payload(
                snapshot,
                model_map=model_map,
                capability_map=capability_map,
                num_nodes=self.plan.num_nodes,
                exposure_mode=self.plan.exposure.mode,
                plan=self.plan,
            )
        except ValueError as exc:
            raise StatusPublicationError(f"READY heartbeat payload is invalid: {exc}") from exc
        try:
            record = self.store.update(
                DeploymentState.READY,
                reason_code="READINESS_HEARTBEAT",
                data_update={
                    "readiness_snapshot": snapshot,
                    "model_map": model_map,
                    "capability_map": capability_map,
                    "receipt_hashes": receipt_hashes,
                },
                expected_revision=self._revision,
                # Lease renewals advance CAS identity and updated_at but are
                # not lifecycle transitions. Keeping each one in the status
                # history would make a long-lived deployment grow forever.
                record_history=False,
            )
        except (StatusConflict, OSError, ValueError) as exc:
            raise StatusPublicationError(f"READY heartbeat publication failed: {exc}") from exc
        self._revision = record.revision
        return record

    def advance_through(
        self, *states: DeploymentState, reason_code: str, detail: str = "", **data: Any
    ) -> StatusRecord:
        """Walk several legal steps, e.g. PLANNED -> ... -> DEPLOYING."""
        record = None
        for state in states:
            record = self.advance(state, reason_code=reason_code, detail=detail, **data)
        if record is None:  # no states is a caller error, not a valid record
            raise StatusPublicationError("advance_through requires at least one state")
        return record

    @property
    def state(self) -> str:
        return self._state.value


# -- reader side ----------------------------------------------------------
@dataclass(frozen=True)
class DeploymentStatus:
    """What a consumer is allowed to know, typed."""

    state: str
    revision: int
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    site_profile_hash: str
    allocation_binding_hash: str
    num_nodes: int
    run_id: str
    run_semantic_hash: str
    run_provenance_hash: str
    advertised_endpoint: str
    exposure_mode: str
    reason_code: Optional[str]
    detail: Optional[str]
    readiness_snapshot: dict[str, Any]
    model_map: dict[str, Any]
    capability_map: dict[str, Any]
    receipt_hashes: tuple[str, ...]
    receipt_manifest_path: str
    receipt_manifest_hash: str

    @property
    def ready(self) -> bool:
        expiry = self.readiness_snapshot.get("lease_expires_at", 0)
        return (
            self.state == DeploymentState.READY.value
            and isinstance(expiry, (int, float))
            and not isinstance(expiry, bool)
            and math.isfinite(float(expiry))
            and time.time() <= float(expiry)
        )

    @property
    def terminal(self) -> bool:
        return self.state in (
            DeploymentState.FAILED.value,
            DeploymentState.STOPPED.value,
            DeploymentState.CANCELLED.value,
        )


def read_deployment_status(run_dir: str) -> Optional[DeploymentStatus]:
    """Read the shared record. None when no deployment published one."""
    record = StatusStore.deployment(status_path(run_dir)).load()
    if record is None:
        return None
    provenance = record.provenance
    data = record.data
    required_provenance = {
        "deployment_id": str,
        "generation": int,
        "deployment_plan_hash": str,
        "site_profile_hash": str,
        "allocation_binding_hash": str,
        "run_id": str,
        "run_semantic_hash": (str, type(None)),
        "run_provenance_hash": str,
    }
    for name, kind in required_provenance.items():
        if name not in provenance or not isinstance(provenance[name], kind):
            raise InvalidDeploymentStatus(f"deployment status provenance.{name} has invalid type")
    if isinstance(provenance["generation"], bool) or provenance["generation"] < 0:
        raise InvalidDeploymentStatus("deployment status generation is invalid")
    for name in (
        "deployment_plan_hash",
        "site_profile_hash",
        "allocation_binding_hash",
    ):
        if not _SHA256.fullmatch(provenance[name]):
            raise InvalidDeploymentStatus(f"deployment status provenance.{name} is not SHA-256")
    if not provenance["deployment_id"]:
        raise InvalidDeploymentStatus("deployment status provenance.deployment_id is empty")
    for name in ("run_semantic_hash", "run_provenance_hash"):
        value = provenance[name] or ""
        if value and not _SHA256.fullmatch(value):
            raise InvalidDeploymentStatus(f"deployment status provenance.{name} is not SHA-256")
    for name in ("exposure_mode", "advertised_endpoint"):
        if name in data and not isinstance(data[name], str):
            raise InvalidDeploymentStatus(f"deployment status data.{name} must be string")
    for name in ("readiness_snapshot", "model_map", "capability_map"):
        if name in data and not isinstance(data[name], dict):
            raise InvalidDeploymentStatus(f"deployment status data.{name} must be object")
    num_nodes = data.get("num_nodes")
    if isinstance(num_nodes, bool) or not isinstance(num_nodes, int) or num_nodes <= 0:
        raise InvalidDeploymentStatus("deployment status data.num_nodes must be a positive integer")
    hashes = data.get("receipt_hashes", [])
    if (
        not isinstance(hashes, list)
        or any(not isinstance(value, str) or not _SHA256.fullmatch(value) for value in hashes)
        or hashes != sorted(set(hashes))
    ):
        raise InvalidDeploymentStatus(
            "deployment status receipt_hashes must be sorted unique SHA-256 values"
        )
    snapshot = dict(data.get("readiness_snapshot", {}))
    if record.state == DeploymentState.READY.value:
        try:
            _validate_ready_payload(
                snapshot,
                model_map=data.get("model_map"),
                capability_map=data.get("capability_map"),
                num_nodes=num_nodes,
                exposure_mode=data.get("exposure_mode", ""),
            )
        except ValueError as exc:
            raise InvalidDeploymentStatus(
                f"READY payload violates the public schema: {exc}"
            ) from exc
        if snapshot.get("ready") is not True or snapshot.get("phase") != "READY":
            raise InvalidDeploymentStatus("READY state lacks a matching true readiness snapshot")
        endpoint = data.get("advertised_endpoint", "")
        if (
            not isinstance(endpoint, str)
            or not endpoint
            or snapshot.get("advertised_endpoint") != endpoint
        ):
            raise InvalidDeploymentStatus("READY state has no matching advertised endpoint")
        for field in (
            "generation",
            "deployment_plan_hash",
            "allocation_binding_hash",
        ):
            if snapshot.get(field) != provenance[field]:
                raise InvalidDeploymentStatus(f"READY snapshot {field} disagrees with provenance")
        if snapshot.get("receipt_hashes") != hashes:
            raise InvalidDeploymentStatus("READY snapshot receipt hashes disagree with status data")
        lease_expires_at = snapshot.get("lease_expires_at")
        if (
            isinstance(lease_expires_at, bool)
            or not isinstance(lease_expires_at, (int, float))
            or not math.isfinite(float(lease_expires_at))
            or lease_expires_at <= record.updated_at
        ):
            raise InvalidDeploymentStatus("READY snapshot has an invalid readiness lease")
        manifest_path = snapshot.get("receipt_manifest_path")
        manifest_hash = snapshot.get("receipt_manifest_hash")
        if (
            not isinstance(manifest_path, str)
            or not manifest_path
            or not isinstance(manifest_hash, str)
            or not _SHA256.fullmatch(manifest_hash)
        ):
            raise InvalidDeploymentStatus(
                "READY snapshot lacks an immutable receipt manifest identity"
            )
        run_root = os.path.realpath(run_dir)
        resolved_manifest = os.path.realpath(manifest_path)
        if os.path.commonpath((run_root, resolved_manifest)) != run_root:
            raise InvalidDeploymentStatus(
                "READY receipt manifest escapes the deployment run directory"
            )
        try:
            mode = os.lstat(manifest_path).st_mode
        except OSError as exc:
            raise InvalidDeploymentStatus(f"READY receipt manifest is unavailable: {exc}") from exc
        if not stat.S_ISREG(mode):
            raise InvalidDeploymentStatus(
                "READY receipt manifest must be a regular, non-symlink file"
            )
        from .state.receipts import ReceiptManifestError, load_receipt_manifest

        try:
            receipt_manifest = load_receipt_manifest(manifest_path)
        except ReceiptManifestError as exc:
            raise InvalidDeploymentStatus(f"READY receipt manifest is invalid: {exc}") from exc
        if (
            receipt_manifest.manifest_hash != manifest_hash
            or receipt_manifest.deployment_id != provenance["deployment_id"]
            or receipt_manifest.generation != provenance["generation"]
            or receipt_manifest.deployment_plan_hash != provenance["deployment_plan_hash"]
            or receipt_manifest.allocation_binding_hash != provenance["allocation_binding_hash"]
            or list(receipt_manifest.receipt_hashes) != hashes
        ):
            raise InvalidDeploymentStatus("READY receipt manifest disagrees with status identity")
    elif snapshot.get("ready") is True:
        raise InvalidDeploymentStatus(
            f"non-READY state {record.state} retains a true readiness snapshot"
        )
    receipt_manifest_path = snapshot.get("receipt_manifest_path", "")
    receipt_manifest_hash = snapshot.get("receipt_manifest_hash", "")
    if not isinstance(receipt_manifest_path, str) or not isinstance(receipt_manifest_hash, str):
        raise InvalidDeploymentStatus("deployment status receipt manifest identity must be text")
    return DeploymentStatus(
        state=record.state,
        revision=record.revision,
        deployment_id=provenance["deployment_id"],
        generation=provenance["generation"],
        deployment_plan_hash=provenance["deployment_plan_hash"],
        site_profile_hash=provenance["site_profile_hash"],
        allocation_binding_hash=provenance["allocation_binding_hash"],
        num_nodes=num_nodes,
        run_id=provenance["run_id"],
        run_semantic_hash=provenance["run_semantic_hash"] or "",
        run_provenance_hash=provenance["run_provenance_hash"],
        advertised_endpoint=data.get("advertised_endpoint", ""),
        exposure_mode=data.get("exposure_mode", ""),
        reason_code=record.reason_code,
        detail=record.detail,
        readiness_snapshot=snapshot,
        model_map=dict(data.get("model_map", {})),
        capability_map=dict(data.get("capability_map", {})),
        receipt_hashes=tuple(hashes),
        receipt_manifest_path=receipt_manifest_path,
        receipt_manifest_hash=receipt_manifest_hash,
    )


def load_status_allocation_binding(run_dir: str, status: Optional[DeploymentStatus] = None):
    """Load the canonical binding and prove it belongs to ``status``.

    The binding is intentionally a separate generation-scoped artifact rather
    than an unverified path embedded in DeploymentStatus. Consumers must use
    this boundary instead of deriving and trusting a private filename.
    """
    if status is None:
        status = read_deployment_status(run_dir)
    if status is None:
        raise InvalidDeploymentStatus("cannot load an allocation binding without deployment status")

    run_root = os.path.realpath(run_dir)
    binding_path = os.path.join(run_root, ALLOCATION_BINDING_FILENAME)
    resolved_binding = os.path.realpath(binding_path)
    if os.path.commonpath((run_root, resolved_binding)) != run_root:
        raise InvalidDeploymentStatus("allocation binding escapes the deployment run directory")
    try:
        mode = os.lstat(binding_path).st_mode
    except OSError as exc:
        raise InvalidDeploymentStatus(f"allocation binding is unavailable: {exc}") from exc
    if not stat.S_ISREG(mode):
        raise InvalidDeploymentStatus("allocation binding must be a regular, non-symlink file")

    from .plan.io import PlanError, load_allocation_binding

    try:
        binding = load_allocation_binding(binding_path)
    except PlanError as exc:
        raise InvalidDeploymentStatus(f"allocation binding is invalid: {exc}") from exc
    expected = (
        status.deployment_id,
        status.generation,
        status.deployment_plan_hash,
        status.site_profile_hash,
        status.allocation_binding_hash,
        status.num_nodes,
    )
    observed = (
        binding.deployment_id,
        binding.generation,
        binding.deployment_plan_hash,
        binding.site_profile_hash,
        binding.allocation_binding_hash,
        len(binding.rank_to_node),
    )
    if observed != expected:
        raise InvalidDeploymentStatus(
            "allocation binding identity disagrees with DeploymentStatus: "
            f"expected={expected!r}, observed={observed!r}"
        )
    return binding


def require_ready_status(
    run_dir: str, *, expected_generation: Optional[int] = None, expected_plan_hash: str = ""
) -> DeploymentStatus:
    """Return one atomically read status that is READY *right now*.

    Every failure mode raises with the reason instead of returning a URL a
    client would then hammer: no record, wrong generation, wrong plan, not
    READY, or READY with no endpoint recorded.
    """
    status = read_deployment_status(run_dir)
    if status is None:
        raise DeploymentNotReady(
            f"no deployment status published under {run_dir}; a client must not "
            "guess an endpoint or grep a log for one"
        )
    if expected_generation is not None and status.generation != expected_generation:
        raise DeploymentNotReady(
            f"status is for generation {status.generation}, expected "
            f"{expected_generation}; this record is from another run"
        )
    if expected_plan_hash and status.deployment_plan_hash != expected_plan_hash:
        raise DeploymentNotReady(
            f"status plan {status.deployment_plan_hash[:12]} != expected {expected_plan_hash[:12]}"
        )
    if not status.ready:
        if status.state == DeploymentState.READY.value:
            raise DeploymentNotReady(
                "deployment READY evidence lease expired; the owner is no "
                "longer proving current readiness"
            )
        raise DeploymentNotReady(
            f"deployment is {status.state}"
            + (f" ({status.reason_code}: {status.detail})" if status.reason_code else "")
        )
    if not status.advertised_endpoint:
        raise DeploymentNotReady("deployment is READY but published no endpoint")
    return status


def require_ready_endpoint(
    run_dir: str, *, expected_generation: Optional[int] = None, expected_plan_hash: str = ""
) -> str:
    """Return the endpoint from one atomically validated READY status."""
    return require_ready_status(
        run_dir,
        expected_generation=expected_generation,
        expected_plan_hash=expected_plan_hash,
    ).advertised_endpoint
