"""The one shared compiled plan contract (plan §3.2.1, packet P01, IMP-H01).

Every component previously carried its own idea of "the configuration": core
loaded YAML into `DeploymentConfig`, eval built an eval-shaped `RunPlan`,
ClientLab had a third. Nothing could prove two of them meant the same thing,
and the audit found readiness binding to the literal string ``"plan"`` because
there was no compiled identity to bind to.

This module is that identity. Five artifacts with **distinct hash boundaries**,
so a change is attributable to the thing that changed:

    SiteProfile        site capabilities/defaults      -> site_profile_hash
    SchedulerPlan      this run's queue/account/nodes  -> (part of run semantics)
    DeploymentPlan     what is served, and how         -> deployment_plan_hash
    RunPlan            eval/ClientLab workload policy  -> run_semantic_hash
    AllocationBinding  rank -> node, after allocation  -> allocation_binding_hash

The boundary rule, stated once: **paths, timestamps, commands, allocation
hostnames and output locations affect binding/provenance identity only.** A
restart on different nodes increments the generation and mints a new binding
without touching a semantic hash. Serving changes move `deployment_plan_hash`;
workload/scheduler changes move `run_semantic_hash`.

Gateway and exposure are deliberately separate types (§3.2.1 Q3): `GatewayPlan`
names a *real managed gateway* and has no `none`/`direct` member, so "no
gateway" cannot be spelled as a gateway. Direct exposure is an `ExposurePlan`
mode that compilation accepts only in explicit validation mode.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field, replace
from enum import Enum
from typing import Any, Optional

SCHEMA_VERSION = 2


class PlanError(ValueError):
    """A configuration cannot be compiled into a valid plan."""


def canonical_hash(payload: Any) -> str:
    """Lowercase SHA-256 over canonical JSON. One hashing rule everywhere."""
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"),
                      default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------- gateway ---

class GatewayKind(str, Enum):
    """Real managed gateways only.

    There is deliberately no `none`/`direct` member: the audit found `none`
    acting as a production gateway *and* the implicit default, which let a
    deployment with no front door look like a configured one.
    """

    HAPROXY = "haproxy"
    NGINX = "nginx"
    ENVOY = "envoy"
    PINGORA = "pingora"


class ExposureMode(str, Enum):
    PROXIED_INTERNAL = "PROXIED_INTERNAL"   # production: via the gateway
    DIRECT_VALIDATION = "DIRECT_VALIDATION"  # validation/benchmark only


# First-release production gateway (§3.2.1 Q3). Others compile only under
# validation mode until they carry their own WP7/WP12 evidence.
PRODUCTION_GATEWAY_KINDS = frozenset({GatewayKind.HAPROXY})


@dataclass(frozen=True)
class GatewayPlan:
    kind: str
    port: int
    options: tuple[tuple[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in {k.value for k in GatewayKind}:
            raise PlanError(
                f"gateway.kind {self.kind!r} is not a managed gateway "
                f"({sorted(k.value for k in GatewayKind)}). 'none'/'direct' is "
                "an exposure mode, not a gateway.")
        if not 1 <= int(self.port) <= 65535:
            raise PlanError(f"gateway.port out of range: {self.port}")


@dataclass(frozen=True)
class ExposurePlan:
    """How clients reach the deployment, and what the advertised endpoint is."""

    mode: str
    advertised_scheme: str = "http"
    advertised_path: str = "/v1"
    # The canonical advertised endpoint is resolved at bind time from the
    # gateway (PROXIED_INTERNAL) or the declared Serve endpoint
    # (DIRECT_VALIDATION); the plan fixes only its SHAPE.
    serve_port: int = 8000

    def __post_init__(self) -> None:
        if self.mode not in {m.value for m in ExposureMode}:
            raise PlanError(f"exposure.mode {self.mode!r} is not valid")


# ------------------------------------------------------------ control ---

@dataclass(frozen=True)
class ControlLimits:
    """Resolved control-plane deadlines and bounds (§3.2.1 Q4).

    These are compiled values. The plan is explicit that environment variables
    must not silently override them: a deployment's timing behaviour has to be
    attributable to its plan hash, not to whatever was exported on the node.

    No production defaults are invented here. The values below are the
    injectable *test* defaults; a SiteProfile is not production-qualified until
    its values carry measured evidence at the required tiers.
    """

    registration_deadline_s: float = 300.0
    reconnect_grace_s: float = 60.0
    heartbeat_interval_s: float = 5.0
    lease_timeout_s: float = 30.0
    snapshot_assembly_deadline_s: float = 60.0
    watchdog_cleanup_deadline_s: float = 120.0
    max_frame_bytes: int = 1 << 20
    max_snapshot_chunks: int = 256
    max_snapshot_bytes: int = 64 << 20
    max_snapshot_items: int = 65536
    evidence_backed: bool = False       # set only by a qualified SiteProfile

    def __post_init__(self) -> None:
        for name in ("registration_deadline_s", "reconnect_grace_s",
                     "heartbeat_interval_s", "lease_timeout_s",
                     "snapshot_assembly_deadline_s",
                     "watchdog_cleanup_deadline_s"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or value <= 0:
                raise PlanError(f"control.{name} must be positive, got {value!r}")
        for name in ("max_frame_bytes", "max_snapshot_chunks",
                     "max_snapshot_bytes", "max_snapshot_items"):
            value = getattr(self, name)
            if not isinstance(value, int) or value <= 0:
                raise PlanError(f"control.{name} must be a positive int, got {value!r}")
        # A lease shorter than a few heartbeats expires a healthy rank.
        if self.lease_timeout_s < 3 * self.heartbeat_interval_s:
            raise PlanError(
                f"control.lease_timeout_s ({self.lease_timeout_s}) must be >= "
                f"3 * heartbeat_interval_s ({3 * self.heartbeat_interval_s})")


# ------------------------------------------------------------ site ---

@dataclass(frozen=True)
class SiteProfile:
    """Immutable, versioned site capabilities and defaults.

    Describes what a site CAN do; never a particular allocation request. Its
    hash participates in the DeploymentPlan hash, so site drift changes the
    bound plan identity rather than silently altering behaviour.
    """

    schema_version: int
    site_id: str
    max_nodes: int
    gpus_per_node: int
    cpus_per_node: int
    scheduler_types: tuple[str, ...]
    gateway_kinds: tuple[str, ...]
    vendors: tuple[str, ...]
    engines: tuple[str, ...]
    model_storage_path: str
    local_stage_path: str
    control: ControlLimits = field(default_factory=ControlLimits)
    site_profile_hash: str = ""

    def canonical(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("site_profile_hash", None)
        return data

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "SiteProfile":
        return replace(self, site_profile_hash=self.compute_hash())

    def supports_gateway(self, kind: str) -> bool:
        return kind in self.gateway_kinds


@dataclass(frozen=True)
class SchedulerPlan:
    """This run's allocation REQUEST — not the allocation itself."""

    type: str
    nodes: int
    queue: str = ""
    account: str = ""
    walltime: str = ""
    reservation_topology: Optional[str] = None

    def __post_init__(self) -> None:
        if self.nodes < 1:
            raise PlanError(f"scheduler.nodes must be >= 1, got {self.nodes}")


# ------------------------------------------------------- receipt slots ---

@dataclass(frozen=True)
class ReceiptRequirement:
    """One exactly-planned process/actor that must produce a receipt.

    Identity is the SLOT, not the instance: a restart fills the same slot with
    a new instance. Stable planned rank may be part of the identity; an
    allocation hostname must never be, because the plan is compiled before the
    allocation exists and has to survive a restart on different nodes.
    """

    receipt_requirement_id: str
    role: str
    component_slot: str
    owner_scope: str                  # GLOBAL | RANK
    planned_rank: Optional[int] = None
    placement: str = ""

    def __post_init__(self) -> None:
        if self.owner_scope not in ("GLOBAL", "RANK"):
            raise PlanError(f"receipt owner_scope {self.owner_scope!r} invalid")
        if self.owner_scope == "RANK" and self.planned_rank is None:
            raise PlanError(
                f"{self.receipt_requirement_id}: RANK requirement needs planned_rank")
        if self.owner_scope == "GLOBAL" and self.planned_rank is not None:
            raise PlanError(
                f"{self.receipt_requirement_id}: GLOBAL requirement must not pin a rank")


@dataclass(frozen=True)
class ModelPlan:
    model_id: str
    storage_name: str
    route_name: str
    tensor_parallel_size: int
    pipeline_parallel_size: int
    max_model_len: int
    size_b: int
    gpu_memory_utilization: float
    enforce_eager: bool
    max_num_seqs: int
    num_replicas: Optional[int]


@dataclass(frozen=True)
class DeploymentPlan:
    """WHAT is served and HOW. No allocation hostnames, no queue/account."""

    schema_version: int
    deployment_id: str
    site_profile_id: str
    site_profile_hash: str
    compatibility_profile_hash: str
    manifest_hash: str
    num_nodes: int
    num_gpus_per_node: int
    vendor: str
    engine: str
    model_storage_path: str
    local_stage_path: str
    models: tuple[ModelPlan, ...]
    exposure: ExposurePlan
    gateway: Optional[GatewayPlan]
    receipt_requirements: tuple[ReceiptRequirement, ...]
    control: ControlLimits
    validation_mode: bool = False
    deployment_plan_hash: str = ""

    # -- identity ----------------------------------------------------------
    def canonical(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("deployment_plan_hash", None)
        return data

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "DeploymentPlan":
        return replace(self, deployment_plan_hash=self.compute_hash())

    # -- queries -----------------------------------------------------------
    def requirement_keys(self) -> frozenset[str]:
        return frozenset(r.receipt_requirement_id for r in self.receipt_requirements)

    def requirements_for_rank(self, rank: int) -> tuple[ReceiptRequirement, ...]:
        return tuple(r for r in self.receipt_requirements
                     if r.owner_scope == "RANK" and r.planned_rank == rank)

    def global_requirements(self) -> tuple[ReceiptRequirement, ...]:
        return tuple(r for r in self.receipt_requirements if r.owner_scope == "GLOBAL")

    def is_production_exposure(self) -> bool:
        return self.exposure.mode == ExposureMode.PROXIED_INTERNAL.value


@dataclass(frozen=True)
class WorkloadPolicy:
    """Semantic workload intent for eval/ClientLab (not core serving)."""

    kind: str = "synthetic"
    duration_s: float = 0.0
    input_len: int = 0
    output_len: int = 0
    rate_per_node: float = 0.0
    seed: int = 0
    arrival: str = "fixed"
    client_nodes: int = 1
    client_dest: str = "proxy"


@dataclass(frozen=True)
class RunPlan:
    """Eval/ClientLab only: an exact DeploymentPlan + scheduler + workload.

    Core serving uses `DeploymentPlan` directly — a serving-only launch must not
    fabricate an empty-workload RunPlan just to have something to pass around.
    """

    schema_version: int
    run_id: str
    deployment: DeploymentPlan
    scheduler: SchedulerPlan
    workload: WorkloadPolicy
    run_semantic_hash: str = ""

    def canonical(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "deployment_plan_hash": self.deployment.deployment_plan_hash,
            "scheduler": asdict(self.scheduler),
            "workload": asdict(self.workload),
        }

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "RunPlan":
        return replace(self, run_semantic_hash=self.compute_hash())


# -------------------------------------------------------- bindings ---

@dataclass(frozen=True)
class AllocationBinding:
    """Generation-scoped rank -> node map, minted AFTER the nodefile resolves.

    Separate from the plan on purpose: a restart on different nodes must change
    this and nothing semantic.
    """

    schema_version: int
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    site_profile_hash: str
    scheduler_allocation_id: str
    rank_to_node: tuple[tuple[int, str], ...]
    allocation_binding_hash: str = ""

    def canonical(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("allocation_binding_hash", None)
        return data

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "AllocationBinding":
        return replace(self, allocation_binding_hash=self.compute_hash())

    def node_for(self, rank: int) -> Optional[str]:
        for planned_rank, node in self.rank_to_node:
            if planned_rank == rank:
                return node
        return None

    def is_bound_node(self, rank: int, node_id: str) -> bool:
        """Does ``node_id`` name the node this rank is bound to?

        The binding holds whatever the scheduler wrote in its node file — on
        this site, fully qualified — while a process reports
        ``socket.gethostname()``, which is the short name. A literal string
        comparison therefore rejected every receipt from every correctly-placed
        rank. Compare canonical forms instead: the short name is unique within
        an allocation, so this loses no discrimination between real hosts.
        """
        bound = self.node_for(rank)
        return bound is not None and same_node(bound, node_id)

    def ranks(self) -> tuple[int, ...]:
        return tuple(sorted(rank for rank, _ in self.rank_to_node))


@dataclass(frozen=True)
class ComponentInstanceBinding:
    """Binds one live process/actor instance to exactly one unfilled slot."""

    schema_version: int
    deployment_id: str
    generation: int
    receipt_requirement_id: str
    component_id: str
    instance_id: str
    owner_scope: str
    owner_rank: Optional[int]
    node_id: str
    bound_at: float

    def key(self) -> str:
        return self.receipt_requirement_id


def canonical_node_id(node_id: str) -> str:
    """The comparable form of a node name: lowercase, domain stripped.

    Nothing is *stored* in this form — receipts and bindings keep exactly what
    their producer observed, so the record stays faithful. This is only how two
    names are compared.
    """
    return str(node_id).strip().lower().split(".", 1)[0]


def same_node(left: str, right: str) -> bool:
    canonical = canonical_node_id(left)
    return bool(canonical) and canonical == canonical_node_id(right)


def build_allocation_binding(*, plan: DeploymentPlan, generation: int,
                             scheduler_allocation_id: str,
                             nodes: list[str]) -> AllocationBinding:
    """Bind planned ranks to actual nodes, in nodefile order.

    Raises rather than truncating or padding: a node count that disagrees with
    the plan is a different deployment, not a smaller one.
    """
    unique: list[str] = []
    for node in nodes:
        if node and node not in unique:
            unique.append(node)
    if len(unique) != plan.num_nodes:
        raise PlanError(
            f"allocation has {len(unique)} unique node(s) but the plan requires "
            f"{plan.num_nodes}; refusing to bind a different deployment")
    return AllocationBinding(
        schema_version=SCHEMA_VERSION,
        deployment_id=plan.deployment_id,
        generation=generation,
        deployment_plan_hash=plan.deployment_plan_hash,
        site_profile_hash=plan.site_profile_hash,
        scheduler_allocation_id=scheduler_allocation_id,
        rank_to_node=tuple((index, node) for index, node in enumerate(unique)),
    ).finalize()


def provenance_hash(*, deployment_plan_hash: str, allocation_binding_hash: str,
                    source_path: str = "", started_at: str = "",
                    argv: tuple[str, ...] = ()) -> str:
    """Everything that is NOT semantic intent: paths, times, commands."""
    return canonical_hash({
        "deployment_plan_hash": deployment_plan_hash,
        "allocation_binding_hash": allocation_binding_hash,
        "source_path": source_path,
        "started_at": started_at,
        "argv": list(argv),
    })
