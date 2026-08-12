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
import math
import os
import re
from dataclasses import asdict, dataclass, field, is_dataclass, replace
from enum import Enum
from pathlib import PurePath
from typing import Any, Mapping, Optional

SCHEMA_VERSION = 3
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")


class PlanError(ValueError):
    """A configuration cannot be compiled into a valid plan."""


def require_schema_version(value: Any, path: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value != SCHEMA_VERSION:
        raise PlanError(f"{path} must be supported schema version {SCHEMA_VERSION}, got {value!r}")


def require_sha256(value: Any, path: str, *, allow_empty: bool = False) -> None:
    if allow_empty and value == "":
        return
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise PlanError(f"{path} must be a lowercase SHA-256 digest")


def require_bool(value: Any, path: str) -> None:
    if not isinstance(value, bool):
        raise PlanError(f"{path} must be a boolean, got {value!r}")


def require_absolute_path(value: Any, path: str) -> None:
    """Require an absolute path whose lexical spelling cannot escape its root."""
    if not isinstance(value, str) or not value or not os.path.isabs(value):
        raise PlanError(f"{path} must be an absolute path")
    if ".." in PurePath(value).parts:
        raise PlanError(f"{path} must not contain parent traversal ('..')")


def _canonical_value(value: Any, path: str = "$") -> Any:
    """Return a JSON value or fail closed with the exact offending path.

    ``json.dumps(default=str)`` made arbitrary objects hashable by their
    unstable representation.  A semantic identity must never depend on an
    object's ``repr`` or silently serialize a secret/container type we did not
    model.  Canonical plans therefore admit only explicit JSON families.
    """
    if is_dataclass(value):
        if isinstance(value, type):
            raise PlanError(f"{path} must be a dataclass instance, not a class")
        value = asdict(value)
    if isinstance(value, Enum):
        return value.value
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PlanError(f"{path} must be finite, got {value!r}")
        return value
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise PlanError(f"{path} has non-string map key {key!r}")
            result[key] = _canonical_value(item, f"{path}.{key}")
        return result
    if isinstance(value, (tuple, list)):
        return [_canonical_value(item, f"{path}[{index}]") for index, item in enumerate(value)]
    raise PlanError(f"{path} contains unsupported semantic value {type(value).__name__}")


def canonical_hash(payload: Any) -> str:
    """Lowercase SHA-256 over strict canonical JSON. One rule everywhere."""
    blob = json.dumps(
        _canonical_value(payload), sort_keys=True, separators=(",", ":"), allow_nan=False
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def deep_freeze(value: Any, path: str = "value") -> Any:
    """Copy a JSON-family value into recursively immutable tuples."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise PlanError(f"{path} must be finite, got {value!r}")
        return value
    if isinstance(value, Mapping):
        if any(not isinstance(key, str) for key in value):
            raise PlanError(f"{path} must have string keys")
        return tuple(
            (key, deep_freeze(item, f"{path}.{key}")) for key, item in sorted(value.items())
        )
    if isinstance(value, (tuple, list)):
        return tuple(deep_freeze(item, f"{path}[{index}]") for index, item in enumerate(value))
    raise PlanError(f"{path} contains unsupported value {type(value).__name__}")


def freeze_mapping(value: Any, path: str) -> tuple[tuple[str, Any], ...]:
    """Validate mapping pairs before freezing; never collapse duplicates."""
    try:
        raw_items = tuple(value.items()) if isinstance(value, Mapping) else tuple(value)
    except TypeError as exc:
        raise PlanError(f"{path} must be mapping pairs") from exc
    items: list[tuple[str, Any]] = []
    names: set[str] = set()
    for index, item in enumerate(raw_items):
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            raise PlanError(f"{path}[{index}] must be a (string, value) pair")
        key, nested = item
        if not isinstance(key, str):
            raise PlanError(f"{path}[{index}] key must be a string")
        if key in names:
            raise PlanError(f"{path} contains duplicate key {key!r}")
        names.add(key)
        items.append((key, nested))
    return deep_freeze(dict(items), path)


def freeze_sequence(value: Any, path: str) -> tuple[Any, ...]:
    """Copy one declared sequence without treating text or mappings as iterables."""
    if not isinstance(value, (tuple, list)):
        raise PlanError(f"{path} must be a sequence")
    return tuple(value)


def freeze_string_sequence(value: Any, path: str, *, unique: bool = False) -> tuple[str, ...]:
    """Validate a tuple/list of non-empty strings and return an immutable copy."""
    items = freeze_sequence(value, path)
    if any(not isinstance(item, str) or not item for item in items):
        raise PlanError(f"{path} must contain non-empty strings")
    if unique and len(items) != len(set(items)):
        raise PlanError(f"{path} must contain unique non-empty strings")
    return items


# ---------------------------------------------------------------- gateway ---


class GatewayKind(str, Enum):
    """Real managed gateways only.

    There is deliberately no `none`/`direct` member: the audit found `none`
    acting as a production gateway *and* the implicit default, which let a
    deployment with no front door look like a configured one.
    """

    HAPROXY = "haproxy"
    LITELLM = "litellm"
    NGINX = "nginx"
    ENVOY = "envoy"
    PINGORA = "pingora"


class ExposureMode(str, Enum):
    PROXIED_INTERNAL = "PROXIED_INTERNAL"  # production: via the gateway
    DIRECT_VALIDATION = "DIRECT_VALIDATION"  # validation/benchmark only
    # Native Ray Serve's single head-node proxy is a benchmark topology, not a
    # managed external gateway and not the per-node direct-validation shape.
    RAY_SERVE_HEAD_ONLY = "RAY_SERVE_HEAD_ONLY"


# First-release production gateway (§3.2.1 Q3). Others compile only under
# validation mode until they carry their own WP7/WP12 evidence.
PRODUCTION_GATEWAY_KINDS = frozenset({GatewayKind.HAPROXY})


@dataclass(frozen=True)
class GatewayPlan:
    kind: str
    port: int
    backend_port: int = 8000
    executable_ref: str = "PATH:haproxy"
    worker_count: int = 1
    options: tuple[tuple[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in {k.value for k in GatewayKind}:
            raise PlanError(
                f"gateway.kind {self.kind!r} is not a managed gateway "
                f"({sorted(k.value for k in GatewayKind)}). 'none'/'direct' is "
                "an exposure mode, not a gateway."
            )
        if (
            isinstance(self.port, bool)
            or not isinstance(self.port, int)
            or not 1 <= self.port <= 65535
        ):
            raise PlanError(f"gateway.port out of range: {self.port}")
        if (
            isinstance(self.backend_port, bool)
            or not isinstance(self.backend_port, int)
            or not 1 <= self.backend_port <= 65535
        ):
            raise PlanError(f"gateway.backend_port out of range: {self.backend_port}")
        if (
            isinstance(self.worker_count, bool)
            or not isinstance(self.worker_count, int)
            or self.worker_count < 1
        ):
            raise PlanError("gateway.worker_count must be a positive integer")
        if not isinstance(self.executable_ref, str) or not self.executable_ref:
            raise PlanError("gateway.executable_ref must be a non-empty reference")
        if self.executable_ref.startswith("PATH:"):
            executable_name = self.executable_ref.removeprefix("PATH:")
            if not re.fullmatch(r"[A-Za-z0-9_.+-]+", executable_name):
                raise PlanError(
                    "gateway.executable_ref PATH reference must name one safe executable"
                )
        elif not os.path.isabs(self.executable_ref):
            raise PlanError(
                "gateway.executable_ref must be an absolute path or a PATH:<name> reference"
            )
        # Defensive copy/validation for direct construction, not only compiler use.
        object.__setattr__(self, "options", freeze_mapping(self.options, "gateway.options"))


@dataclass(frozen=True)
class ExposurePlan:
    """How clients reach the deployment, and what the advertised endpoint is."""

    mode: str
    advertised_scheme: str = "http"
    advertised_path: str = "/v1"
    network_boundary: str = "trusted_allocation"
    auth_policy_ref: Optional[str] = None
    request_body_limit_bytes: int = 16 << 20
    # The canonical advertised endpoint is resolved at bind time from the
    # gateway (PROXIED_INTERNAL) or the declared Serve endpoint
    # (DIRECT_VALIDATION); the plan fixes only its SHAPE.
    serve_port: int = 8000

    def __post_init__(self) -> None:
        for name in (
            "mode",
            "advertised_scheme",
            "advertised_path",
            "network_boundary",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"exposure.{name} must be a non-empty string")
        if self.auth_policy_ref is not None and (
            not isinstance(self.auth_policy_ref, str) or not self.auth_policy_ref
        ):
            raise PlanError("exposure.auth_policy_ref must be null or a non-empty string")
        if self.mode not in {m.value for m in ExposureMode}:
            raise PlanError(f"exposure.mode {self.mode!r} is not valid")
        if self.advertised_scheme not in ("http", "https"):
            raise PlanError("exposure.advertised_scheme must be http or https")
        if not self.advertised_path.startswith("/"):
            raise PlanError("exposure.advertised_path must start with '/'")
        if (
            isinstance(self.serve_port, bool)
            or not isinstance(self.serve_port, int)
            or not 1 <= self.serve_port <= 65535
        ):
            raise PlanError(f"exposure.serve_port out of range: {self.serve_port}")
        if (
            isinstance(self.request_body_limit_bytes, bool)
            or not isinstance(self.request_body_limit_bytes, int)
            or self.request_body_limit_bytes < 1
        ):
            raise PlanError("exposure.request_body_limit_bytes must be positive")


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
    evidence_backed: bool = False  # set only by a qualified SiteProfile

    def __post_init__(self) -> None:
        require_bool(self.evidence_backed, "control.evidence_backed")
        for name in (
            "registration_deadline_s",
            "reconnect_grace_s",
            "heartbeat_interval_s",
            "lease_timeout_s",
            "snapshot_assembly_deadline_s",
            "watchdog_cleanup_deadline_s",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise PlanError(f"control.{name} must be positive, got {value!r}")
        for name in (
            "max_frame_bytes",
            "max_snapshot_chunks",
            "max_snapshot_bytes",
            "max_snapshot_items",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise PlanError(f"control.{name} must be a positive int, got {value!r}")
        # A lease shorter than a few heartbeats expires a healthy rank.
        if self.lease_timeout_s < 3 * self.heartbeat_interval_s:
            raise PlanError(
                f"control.lease_timeout_s ({self.lease_timeout_s}) must be >= "
                f"3 * heartbeat_interval_s ({3 * self.heartbeat_interval_s})"
            )


@dataclass(frozen=True)
class ReadinessLimits:
    """Resolved readiness/canary deadlines, distinct from control leases.

    A rank reconnect grace answers "how long may this authenticated session be
    absent?"; a readiness recovery deadline answers "how long may a live
    deployment fail route/canary validation?".  Reusing one value for both
    silently coupled unrelated recovery policies and made neither attributable
    to the plan that clients consume.
    """

    initial_deadline_s: float = 3600.0
    recovery_deadline_s: float = 60.0
    validation_interval_s: float = 5.0
    observation_freshness_s: float = 30.0
    gateway_start_deadline_s: float = 30.0
    canary_timeout_s: float = 60.0
    # Ray Serve exposes the per-proxy check deadlines as pre-import
    # environment settings and replica health deadlines as public deployment
    # options.  The startup wait remains a pinned compatibility capability.
    # They are nevertheless plan semantics: changing any one changes which
    # deployment failures are tolerated and for how long.
    serve_start_proxy_timeout_s: float = 3600.0
    serve_proxy_health_check_timeout_s: float = 300.0
    serve_proxy_ready_check_timeout_s: float = 60.0
    serve_replica_health_check_period_s: float = 30.0
    serve_replica_health_check_timeout_s: float = 120.0
    allow_excess_resources: bool = True
    evidence_backed: bool = False

    def __post_init__(self) -> None:
        require_bool(self.evidence_backed, "readiness.evidence_backed")
        require_bool(self.allow_excess_resources, "readiness.allow_excess_resources")
        for name in (
            "initial_deadline_s",
            "recovery_deadline_s",
            "validation_interval_s",
            "observation_freshness_s",
            "gateway_start_deadline_s",
            "canary_timeout_s",
            "serve_start_proxy_timeout_s",
            "serve_proxy_health_check_timeout_s",
            "serve_proxy_ready_check_timeout_s",
            "serve_replica_health_check_period_s",
            "serve_replica_health_check_timeout_s",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value <= 0
            ):
                raise PlanError(f"readiness.{name} must be positive and finite, got {value!r}")
        if self.observation_freshness_s < 3 * self.validation_interval_s:
            raise PlanError(
                "readiness.observation_freshness_s must be at least three validation intervals"
            )


@dataclass(frozen=True)
class ScaleEnvelope:
    """One independently qualified support combination (WP1/AC-SCALE-01).

    The candidate target and evidence-backed supported maximum are deliberately
    different fields.  A validation plan may exercise the candidate ladder;
    it does not convert those runs into a production support claim.
    """

    schema_version: int
    envelope_id: str
    site_id: str
    scheduler_type: str
    vendor: str
    accelerator: str
    engine: str
    compatibility_profile_ref: str
    gateway_kind: Optional[str]
    exposure_mode: str
    request_mode: str
    streaming_mode: str
    min_nodes: int
    supported_max_nodes: int
    qualification_target_nodes: int
    qualification_target_approved: bool
    max_replicas_per_model: int
    max_total_replicas: int
    max_models: int
    validation_tier: str
    validation_mode: bool = False
    evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version, "scale_envelope.schema_version")
        require_bool(
            self.qualification_target_approved, "scale_envelope.qualification_target_approved"
        )
        require_bool(self.validation_mode, "scale_envelope.validation_mode")
        object.__setattr__(
            self,
            "evidence_refs",
            freeze_string_sequence(self.evidence_refs, "scale_envelope.evidence_refs"),
        )
        for name in (
            "envelope_id",
            "site_id",
            "scheduler_type",
            "vendor",
            "accelerator",
            "engine",
            "compatibility_profile_ref",
            "exposure_mode",
            "request_mode",
            "streaming_mode",
            "validation_tier",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"scale_envelope.{name} must be a non-empty string")
        if self.exposure_mode not in {mode.value for mode in ExposureMode}:
            raise PlanError(f"scale_envelope.exposure_mode {self.exposure_mode!r} is invalid")
        if self.gateway_kind is not None:
            if not isinstance(self.gateway_kind, str) or self.gateway_kind not in {
                kind.value for kind in GatewayKind
            }:
                raise PlanError(f"scale_envelope.gateway_kind {self.gateway_kind!r} is invalid")
        if self.gateway_kind is None:
            if self.exposure_mode not in {
                ExposureMode.DIRECT_VALIDATION.value,
                ExposureMode.RAY_SERVE_HEAD_ONLY.value,
            }:
                raise PlanError(
                    "scale_envelope without a gateway requires DIRECT_VALIDATION or "
                    "RAY_SERVE_HEAD_ONLY exposure"
                )
            if not self.validation_mode:
                raise PlanError("gateway-free scale envelope requires validation_mode")
        elif self.exposure_mode != ExposureMode.PROXIED_INTERNAL.value:
            raise PlanError("managed gateway scale envelope requires PROXIED_INTERNAL exposure")
        elif not self.validation_mode and self.gateway_kind != GatewayKind.HAPROXY.value:
            raise PlanError("production scale envelope requires the HAProxy gateway")
        for name in (
            "min_nodes",
            "supported_max_nodes",
            "qualification_target_nodes",
            "max_replicas_per_model",
            "max_total_replicas",
            "max_models",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise PlanError(f"scale_envelope.{name} must be a positive integer")
        if self.supported_max_nodes < self.min_nodes:
            raise PlanError("scale_envelope.supported_max_nodes is below min_nodes")
        if self.qualification_target_nodes < self.supported_max_nodes:
            raise PlanError(
                "scale_envelope.qualification_target_nodes is below supported_max_nodes"
            )
        if self.max_total_replicas < self.max_replicas_per_model:
            raise PlanError("scale_envelope.max_total_replicas is below max_replicas_per_model")
        # Validation mode may still exercise a managed gateway; it does not
        # imply DIRECT_VALIDATION exposure.

    def check_nodes(self, nodes: int, path: str = "deployment.num_nodes") -> None:
        if nodes < self.min_nodes:
            raise PlanError(f"{path} {nodes} is below envelope minimum {self.min_nodes}")
        if nodes > self.qualification_target_nodes:
            raise PlanError(
                f"{path} {nodes} exceeds candidate qualification target "
                f"{self.qualification_target_nodes}; an approved envelope expansion is required"
            )
        if nodes > self.supported_max_nodes and not self.validation_mode:
            raise PlanError(
                f"{path} {nodes} exceeds evidence-backed supported maximum "
                f"{self.supported_max_nodes}; use an explicit validation-mode "
                "envelope for qualification work"
            )


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
    readiness: ReadinessLimits = field(default_factory=ReadinessLimits)
    launcher_capabilities: tuple[str, ...] = ()
    filesystem_semantics: tuple[tuple[str, str], ...] = ()
    accelerator_inventory: tuple[str, ...] = ()
    network_boundary: str = "trusted_allocation"
    environment_profile_ref: str = ""
    # Exact process-environment preparation performed before any Ray/engine
    # import.  These are site facts, not submission-shell suggestions: the
    # launcher overwrites inherited values from this hash-bearing mapping and
    # explicitly removes every name in ``environment_unset``.  Secret and
    # allocation-specific values are forbidden here.
    prepared_environment: tuple[tuple[str, str], ...] = ()
    environment_unset: tuple[str, ...] = ()
    stack_size_kb: int = 8192
    scale_envelopes: tuple[ScaleEnvelope, ...] = ()
    site_profile_hash: str = ""

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version, "site.schema_version")
        if not isinstance(self.control, ControlLimits):
            raise PlanError("site.control must be a ControlLimits")
        if not isinstance(self.readiness, ReadinessLimits):
            raise PlanError("site.readiness must be a ReadinessLimits")
        for name in (
            "site_id",
            "model_storage_path",
            "local_stage_path",
            "network_boundary",
            "environment_profile_ref",
        ):
            if not isinstance(getattr(self, name), str):
                raise PlanError(f"site.{name} must be a string")
        if not self.site_id:
            raise PlanError("site.site_id must be non-empty")
        for name in ("max_nodes", "gpus_per_node", "cpus_per_node"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise PlanError(f"site.{name} must be a positive integer")
        for name in ("scheduler_types", "gateway_kinds", "vendors", "engines"):
            values = freeze_string_sequence(getattr(self, name), f"site.{name}", unique=True)
            object.__setattr__(self, name, values)
        for name in ("launcher_capabilities", "accelerator_inventory"):
            values = freeze_string_sequence(getattr(self, name), f"site.{name}", unique=True)
            object.__setattr__(self, name, values)
        filesystem_semantics = freeze_mapping(
            self.filesystem_semantics, "site.filesystem_semantics"
        )
        if any(not isinstance(value, str) or not value for _, value in filesystem_semantics):
            raise PlanError("site.filesystem_semantics values must be non-empty strings")
        object.__setattr__(self, "filesystem_semantics", filesystem_semantics)
        environment = freeze_mapping(self.prepared_environment, "site.prepared_environment")
        names = [key for key, _ in environment]
        env_name = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
        if len(names) != len(set(names)):
            raise PlanError("site.prepared_environment contains duplicate names")
        if any(
            not isinstance(key, str) or not env_name.fullmatch(key) or not isinstance(value, str)
            for key, value in environment
        ):
            raise PlanError("site.prepared_environment must contain valid string environment pairs")
        forbidden_prefixes = ("PBS_", "SLURM_", "PALS_", "PMI_", "SSH_")
        forbidden_names = {"EXASERVE_CONTROL_SECRET"}
        if any(key in forbidden_names or key.startswith(forbidden_prefixes) for key in names):
            raise PlanError("site.prepared_environment may not contain allocation state or secrets")
        unset = freeze_string_sequence(
            self.environment_unset, "site.environment_unset", unique=True
        )
        if any(not env_name.fullmatch(key) for key in unset):
            raise PlanError("site.environment_unset must contain unique valid environment names")
        overlap = sorted(set(names).intersection(unset))
        if overlap:
            raise PlanError(f"site environment names cannot be both set and unset: {overlap}")
        if (
            isinstance(self.stack_size_kb, bool)
            or not isinstance(self.stack_size_kb, int)
            or self.stack_size_kb < 1024
        ):
            raise PlanError("site.stack_size_kb must be an integer >= 1024")
        object.__setattr__(self, "prepared_environment", tuple(sorted(environment)))
        object.__setattr__(self, "environment_unset", tuple(sorted(unset)))
        scale_envelopes = freeze_sequence(self.scale_envelopes, "site.scale_envelopes")
        if any(not isinstance(item, ScaleEnvelope) for item in scale_envelopes):
            raise PlanError("site.scale_envelopes must contain ScaleEnvelope values")
        object.__setattr__(self, "scale_envelopes", scale_envelopes)
        require_sha256(self.site_profile_hash, "site.site_profile_hash", allow_empty=True)
        if not self.model_storage_path or not self.local_stage_path:
            raise PlanError("site model_storage_path/local_stage_path must be non-empty")
        for name in ("model_storage_path", "local_stage_path"):
            require_absolute_path(getattr(self, name), f"site.{name}")
        ids = [item.envelope_id for item in self.scale_envelopes]
        if len(ids) != len(set(ids)):
            raise PlanError("site.scale_envelopes contains duplicate envelope_id values")
        for envelope in self.scale_envelopes:
            if envelope.site_id != self.site_id:
                raise PlanError(
                    f"scale envelope {envelope.envelope_id!r} belongs to "
                    f"{envelope.site_id!r}, not {self.site_id!r}"
                )

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
    launcher: str = "mpi"
    resources: tuple[tuple[str, Any], ...] = ()
    filesystem_refs: tuple[str, ...] = ()
    policy: tuple[tuple[str, Any], ...] = ()
    secret_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in ("type", "queue", "account", "walltime", "launcher"):
            if not isinstance(getattr(self, name), str):
                raise PlanError(f"scheduler.{name} must be text")
        if not self.type:
            raise PlanError("scheduler.type must be non-empty")
        if isinstance(self.nodes, bool) or not isinstance(self.nodes, int) or self.nodes < 1:
            raise PlanError(f"scheduler.nodes must be >= 1, got {self.nodes}")
        if not self.launcher:
            raise PlanError("scheduler.launcher must be non-empty")
        if self.reservation_topology is not None and (
            not isinstance(self.reservation_topology, str) or not self.reservation_topology
        ):
            raise PlanError("scheduler.reservation_topology must be null or non-empty text")
        object.__setattr__(self, "resources", freeze_mapping(self.resources, "scheduler.resources"))
        object.__setattr__(self, "policy", freeze_mapping(self.policy, "scheduler.policy"))
        for name in ("filesystem_refs", "secret_refs"):
            object.__setattr__(
                self,
                name,
                freeze_string_sequence(getattr(self, name), f"scheduler.{name}"),
            )


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
    owner_scope: str  # GLOBAL | RANK
    planned_rank: Optional[int] = None
    placement: str = ""
    attestation_type: str = "SELF"

    def __post_init__(self) -> None:
        for name in (
            "receipt_requirement_id",
            "role",
            "component_slot",
            "owner_scope",
            "placement",
            "attestation_type",
        ):
            if name == "placement":
                if not isinstance(self.placement, str):
                    raise PlanError("receipt.placement must be text")
                continue
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"receipt.{name} must be a non-empty string")
        if self.owner_scope not in ("GLOBAL", "RANK"):
            raise PlanError(f"receipt owner_scope {self.owner_scope!r} invalid")
        if self.owner_scope == "RANK" and self.planned_rank is None:
            raise PlanError(f"{self.receipt_requirement_id}: RANK requirement needs planned_rank")
        if self.owner_scope == "GLOBAL" and self.planned_rank is not None:
            raise PlanError(
                f"{self.receipt_requirement_id}: GLOBAL requirement must not pin a rank"
            )
        if self.planned_rank is not None and (
            isinstance(self.planned_rank, bool)
            or not isinstance(self.planned_rank, int)
            or self.planned_rank < 0
        ):
            raise PlanError(f"{self.receipt_requirement_id}: planned_rank must be non-negative")
        if self.attestation_type not in ("SELF", "SUPERVISOR"):
            raise PlanError(
                f"{self.receipt_requirement_id}: invalid attestation_type {self.attestation_type!r}"
            )
        if self.attestation_type == "SUPERVISOR" and self.owner_scope != "GLOBAL":
            raise PlanError(f"{self.receipt_requirement_id}: SUPERVISOR attestation must be GLOBAL")


@dataclass(frozen=True)
class ReplicaPlan:
    """Stable logical replica slot resolved before allocation hostnames exist."""

    replica_id: str
    replica_index: int
    planned_ranks: tuple[int, ...]
    planned_device_ids: tuple[tuple[int, ...], ...]
    tensor_parallel_size: int
    pipeline_parallel_size: int
    gpu_demand: int
    cpu_demand: int

    def __post_init__(self) -> None:
        planned_ranks = freeze_sequence(self.planned_ranks, "replica.planned_ranks")
        raw_device_ids = freeze_sequence(self.planned_device_ids, "replica.planned_device_ids")
        planned_device_ids = tuple(
            freeze_sequence(group, f"replica.planned_device_ids[{index}]")
            for index, group in enumerate(raw_device_ids)
        )
        object.__setattr__(self, "planned_ranks", planned_ranks)
        object.__setattr__(self, "planned_device_ids", planned_device_ids)
        if not isinstance(self.replica_id, str) or not self.replica_id:
            raise PlanError("replica.replica_id must be non-empty")
        if (
            isinstance(self.replica_index, bool)
            or not isinstance(self.replica_index, int)
            or self.replica_index < 0
        ):
            raise PlanError("replica.replica_index must be non-negative")
        for name in ("tensor_parallel_size", "pipeline_parallel_size", "gpu_demand", "cpu_demand"):
            value = getattr(self, name)
            minimum = 0 if name == "cpu_demand" else 1
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise PlanError(f"replica.{name} must be an integer >= {minimum}")
        if any(
            isinstance(rank, bool) or not isinstance(rank, int) or rank < 0
            for rank in self.planned_ranks
        ):
            raise PlanError("replica.planned_ranks must be non-negative integers")
        if any(
            isinstance(device, bool) or not isinstance(device, int) or device < 0
            for group in self.planned_device_ids
            for device in group
        ):
            raise PlanError("replica.planned_device_ids must contain non-negative integers")
        if not self.planned_ranks or len(set(self.planned_ranks)) != len(self.planned_ranks):
            raise PlanError("replica.planned_ranks must be non-empty and unique")
        if len(self.planned_ranks) != self.pipeline_parallel_size:
            raise PlanError(
                f"replica {self.replica_id}: planned rank count must equal pipeline_parallel_size"
            )
        if len(self.planned_device_ids) != len(self.planned_ranks):
            raise PlanError(
                f"replica {self.replica_id}: device groups must align with planned ranks"
            )
        if any(
            len(group) != self.tensor_parallel_size or len(set(group)) != len(group)
            for group in self.planned_device_ids
        ):
            raise PlanError(
                f"replica {self.replica_id}: every stage needs exactly "
                "tensor_parallel_size unique device ids"
            )
        if self.gpu_demand != self.tensor_parallel_size * self.pipeline_parallel_size:
            raise PlanError(f"replica {self.replica_id}: gpu_demand does not equal TP*PP")


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
    enable_log_requests: bool
    max_num_seqs: Optional[int]
    num_cpus_per_replica: int
    num_replicas: int
    replicas: tuple[ReplicaPlan, ...]

    def __post_init__(self) -> None:
        replicas = freeze_sequence(self.replicas, "model.replicas")
        if any(not isinstance(replica, ReplicaPlan) for replica in replicas):
            raise PlanError("model.replicas must contain ReplicaPlan values")
        object.__setattr__(self, "replicas", replicas)
        if any(
            not isinstance(value, str) or not value
            for value in (self.model_id, self.storage_name, self.route_name)
        ):
            raise PlanError("model identity fields must be non-empty")
        for name in (
            "tensor_parallel_size",
            "pipeline_parallel_size",
            "max_model_len",
            "size_b",
            "num_cpus_per_replica",
            "num_replicas",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise PlanError(f"model.{name} must be a positive integer")
        if self.max_num_seqs is not None and (
            isinstance(self.max_num_seqs, bool)
            or not isinstance(self.max_num_seqs, int)
            or self.max_num_seqs < 1
        ):
            raise PlanError("model.max_num_seqs must be null or a positive integer")
        if (
            not isinstance(self.gpu_memory_utilization, float)
            or not math.isfinite(self.gpu_memory_utilization)
            or not 0 < self.gpu_memory_utilization <= 1
        ):
            raise PlanError("model.gpu_memory_utilization must be finite in (0, 1]")
        require_bool(self.enforce_eager, "model.enforce_eager")
        require_bool(self.enable_log_requests, "model.enable_log_requests")
        if len(self.replicas) != self.num_replicas:
            raise PlanError(
                f"model {self.model_id}: num_replicas {self.num_replicas} does not "
                f"match {len(self.replicas)} logical slots"
            )


@dataclass(frozen=True)
class RuntimePolicy:
    """Hash-bearing runtime switches that used to be ambient environment.

    These values change what is launched, placed, staged, or accepted.  They
    therefore belong to the DeploymentPlan; an environment variable may no
    longer silently select a different program after compilation.
    """

    null_compute: bool = False
    null_compute_latency_s: float = 1.0
    instrumentation: bool = False
    pp_shard_aware: bool = False
    clean_stage: bool = False
    stats_retention: int = 2000
    stats_push_period_s: float = 10.0
    stats_sample_cap: int = 1500

    def __post_init__(self) -> None:
        for name in (
            "null_compute",
            "instrumentation",
            "pp_shard_aware",
            "clean_stage",
        ):
            require_bool(getattr(self, name), f"runtime.{name}")
        if (
            isinstance(self.null_compute_latency_s, bool)
            or not isinstance(self.null_compute_latency_s, (int, float))
            or not math.isfinite(float(self.null_compute_latency_s))
            or self.null_compute_latency_s < 0
        ):
            raise PlanError("runtime.null_compute_latency_s must be finite and non-negative")
        for name, maximum in (("stats_retention", 10_000), ("stats_sample_cap", 10_000)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= maximum:
                raise PlanError(f"runtime.{name} must be an integer in [1, {maximum}]")
        if (
            isinstance(self.stats_push_period_s, bool)
            or not isinstance(self.stats_push_period_s, (int, float))
            or not math.isfinite(float(self.stats_push_period_s))
            or self.stats_push_period_s <= 0
        ):
            raise PlanError("runtime.stats_push_period_s must be finite and positive")


def build_receipt_requirements(
    *,
    num_nodes: int,
    models: tuple[ModelPlan, ...],
    gateway: Optional[GatewayPlan],
    include_engine_processes: bool = True,
) -> tuple[ReceiptRequirement, ...]:
    """Enumerate the exact evidence slots implied by a deployment contract.

    This derivation belongs beside :class:`DeploymentPlan`, not only in the
    one-way compiler. Persisted plans are loaded directly by the production
    composition root, so their claimed receipt set must be re-derived and
    compared before a self-consistent artifact hash is accepted.
    """

    requirements: list[ReceiptRequirement] = [
        ReceiptRequirement(
            receipt_requirement_id="global/supervisor",
            role="supervisor",
            component_slot="supervisor",
            owner_scope="GLOBAL",
        )
    ]
    if gateway is not None:
        requirements.append(
            ReceiptRequirement(
                receipt_requirement_id=f"global/gateway/{gateway.kind}",
                role="gateway",
                component_slot=f"gateway/{gateway.kind}",
                owner_scope="GLOBAL",
                attestation_type="SUPERVISOR",
            )
        )
    for rank in range(num_nodes):
        role = "ray_head" if rank == 0 else "ray_worker"
        requirements.extend(
            (
                ReceiptRequirement(
                    receipt_requirement_id=f"rank{rank}/{role}",
                    role=role,
                    component_slot="ray",
                    owner_scope="RANK",
                    planned_rank=rank,
                    placement=f"rank:{rank}",
                ),
                ReceiptRequirement(
                    receipt_requirement_id=f"rank{rank}/node_supervisor",
                    role="node_supervisor",
                    component_slot="node_supervisor",
                    owner_scope="RANK",
                    planned_rank=rank,
                    placement=f"rank:{rank}",
                ),
            )
        )
    for model in models:
        for replica in model.replicas:
            owner = replica.planned_ranks[0]
            base = f"model/{model.route_name}/replica/{replica.replica_index}"
            requirements.append(
                ReceiptRequirement(
                    receipt_requirement_id=base,
                    role="replica",
                    component_slot=replica.replica_id,
                    owner_scope="RANK",
                    planned_rank=owner,
                    placement=f"rank:{owner}",
                )
            )
            if not include_engine_processes:
                continue
            requirements.append(
                ReceiptRequirement(
                    receipt_requirement_id=f"{base}/engine/core",
                    role="engine_core",
                    component_slot=f"{replica.replica_id}/engine/core",
                    owner_scope="RANK",
                    planned_rank=owner,
                    placement=f"rank:{owner}",
                )
            )
            # A one-device/one-stage UniProcExecutor runs the model runner
            # inside EngineCore. Larger topologies have one worker per device.
            worker_count = sum(len(ids) for ids in replica.planned_device_ids)
            if worker_count <= 1:
                continue
            for stage, worker_rank in enumerate(replica.planned_ranks):
                for device_id in replica.planned_device_ids[stage]:
                    requirements.append(
                        ReceiptRequirement(
                            receipt_requirement_id=(
                                f"{base}/engine/worker/stage{stage}/device{device_id}"
                            ),
                            role="engine_worker",
                            component_slot=(
                                f"{replica.replica_id}/engine/worker/stage{stage}/device{device_id}"
                            ),
                            owner_scope="RANK",
                            planned_rank=worker_rank,
                            placement=f"rank:{worker_rank}/device:{device_id}",
                        )
                    )
    return tuple(requirements)


@dataclass(frozen=True)
class DeploymentPlan:
    """WHAT is served and HOW. No allocation hostnames, no queue/account."""

    schema_version: int
    deployment_id: str
    site_profile_id: str
    site_profile_hash: str
    compatibility_profile_hash: str
    manifest_hash: str
    scale_envelope: ScaleEnvelope
    num_nodes: int
    num_gpus_per_node: int
    node_cpus: int
    ray_port: int
    vendor: str
    engine: str
    model_storage_path: str
    local_stage_path: str
    deployment_name: str
    replica_max_ongoing_requests: int
    collect_stats: bool
    models: tuple[ModelPlan, ...]
    exposure: ExposurePlan
    gateway: Optional[GatewayPlan]
    receipt_requirements: tuple[ReceiptRequirement, ...]
    control: ControlLimits
    readiness: ReadinessLimits = field(default_factory=ReadinessLimits)
    runtime: RuntimePolicy = field(default_factory=RuntimePolicy)
    validation_mode: bool = False
    deployment_plan_hash: str = ""

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version, "deployment.schema_version")
        nested_contracts = (
            ("scale_envelope", self.scale_envelope, ScaleEnvelope),
            ("exposure", self.exposure, ExposurePlan),
            ("control", self.control, ControlLimits),
            ("readiness", self.readiness, ReadinessLimits),
            ("runtime", self.runtime, RuntimePolicy),
        )
        for name, value, contract in nested_contracts:
            if not isinstance(value, contract):
                raise PlanError(f"deployment.{name} must be a {contract.__name__}")
        if self.gateway is not None and not isinstance(self.gateway, GatewayPlan):
            raise PlanError("deployment.gateway must be null or a GatewayPlan")
        models = freeze_sequence(self.models, "deployment.models")
        if any(not isinstance(model, ModelPlan) for model in models):
            raise PlanError("deployment.models must contain ModelPlan values")
        requirements = freeze_sequence(self.receipt_requirements, "deployment.receipt_requirements")
        if any(not isinstance(item, ReceiptRequirement) for item in requirements):
            raise PlanError(
                "deployment.receipt_requirements must contain ReceiptRequirement values"
            )
        object.__setattr__(self, "models", models)
        object.__setattr__(self, "receipt_requirements", requirements)
        require_bool(self.collect_stats, "deployment.collect_stats")
        require_bool(self.validation_mode, "deployment.validation_mode")
        for name in (
            "deployment_id",
            "site_profile_id",
            "site_profile_hash",
            "compatibility_profile_hash",
            "manifest_hash",
            "vendor",
            "engine",
            "model_storage_path",
            "local_stage_path",
            "deployment_name",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"deployment.{name} must be a non-empty string")
        for name in ("model_storage_path", "local_stage_path"):
            require_absolute_path(getattr(self, name), f"deployment.{name}")
        for name in ("num_nodes", "num_gpus_per_node", "node_cpus", "replica_max_ongoing_requests"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise PlanError(f"deployment.{name} must be a positive integer")
        if (
            isinstance(self.ray_port, bool)
            or not isinstance(self.ray_port, int)
            or not 1 <= self.ray_port <= 65535
        ):
            raise PlanError("deployment.ray_port must be in 1..65535")
        if not self.models:
            raise PlanError("deployment.models must be non-empty")
        if self.uses_head_only_serve_proxy():
            if len(self.models) != 1:
                raise PlanError("RAY_SERVE_HEAD_ONLY supports exactly one model")
            if self.models[0].pipeline_parallel_size != 1:
                raise PlanError(
                    "RAY_SERVE_HEAD_ONLY supports tensor-parallel replicas only; "
                    "pipeline stage placement requires canonical per-replica applications"
                )
        if len(self.receipt_requirements) != len(self.requirement_keys()):
            raise PlanError("deployment.receipt_requirements contains duplicate slots")
        if self.gateway is None:
            if self.exposure.mode not in {
                ExposureMode.DIRECT_VALIDATION.value,
                ExposureMode.RAY_SERVE_HEAD_ONLY.value,
            }:
                raise PlanError(
                    "deployment without a gateway requires DIRECT_VALIDATION or "
                    "RAY_SERVE_HEAD_ONLY exposure"
                )
            if not self.validation_mode:
                raise PlanError("gateway-free deployment requires validation_mode")
        elif self.exposure.mode != ExposureMode.PROXIED_INTERNAL.value:
            raise PlanError("managed deployment gateway requires PROXIED_INTERNAL exposure")
        elif not self.validation_mode and self.gateway.kind != GatewayKind.HAPROXY.value:
            raise PlanError("production deployment requires the HAProxy gateway")

        envelope_dimensions = {
            "site_id": self.site_profile_id,
            "vendor": self.vendor,
            "engine": self.engine,
            "compatibility_profile_ref": self.compatibility_profile_hash,
            "gateway_kind": None if self.gateway is None else self.gateway.kind,
            "exposure_mode": self.exposure.mode,
        }
        for name, expected in envelope_dimensions.items():
            if getattr(self.scale_envelope, name) != expected:
                raise PlanError(
                    f"deployment.{name} disagrees with scale_envelope.{name}: "
                    f"{expected!r} != {getattr(self.scale_envelope, name)!r}"
                )
        self.scale_envelope.check_nodes(self.num_nodes)
        if self.scale_envelope.validation_mode != self.validation_mode:
            raise PlanError("deployment.validation_mode disagrees with scale envelope")
        expected_requirements = build_receipt_requirements(
            num_nodes=self.num_nodes,
            models=self.models,
            gateway=self.gateway,
            include_engine_processes=not self.runtime.null_compute,
        )
        if self.receipt_requirements != expected_requirements:
            expected_ids = {item.receipt_requirement_id for item in expected_requirements}
            observed_ids = set(self.requirement_keys())
            raise PlanError(
                "deployment.receipt_requirements disagrees with derived runtime topology: "
                f"missing={sorted(expected_ids - observed_ids)}, "
                f"unexpected={sorted(observed_ids - expected_ids)}"
            )
        for name in ("site_profile_hash", "compatibility_profile_hash", "manifest_hash"):
            require_sha256(getattr(self, name), f"deployment.{name}")
        require_sha256(
            self.deployment_plan_hash, "deployment.deployment_plan_hash", allow_empty=True
        )
        identities = [model.model_id for model in self.models]
        routes = [model.route_name for model in self.models]
        if len(identities) != len(set(identities)):
            raise PlanError("deployment.models contains duplicate model_id values")
        if len(routes) != len(set(routes)):
            raise PlanError("deployment.models contains duplicate route_name values")
        if any(
            rank >= self.num_nodes
            for model in self.models
            for replica in model.replicas
            for rank in replica.planned_ranks
        ):
            raise PlanError("deployment replica placement names an unplanned rank")
        occupied: set[tuple[int, int]] = set()
        cpu_by_rank: dict[int, int] = {}
        for model in self.models:
            for replica in model.replicas:
                for stage, rank in enumerate(replica.planned_ranks):
                    for device in replica.planned_device_ids[stage]:
                        key = (rank, device)
                        if device >= self.num_gpus_per_node:
                            raise PlanError(
                                f"replica {replica.replica_id} plans device {device} "
                                f"outside rank {rank} capacity"
                            )
                        if key in occupied:
                            raise PlanError(f"deployment topology double-books rank/device {key}")
                        occupied.add(key)
                primary = replica.planned_ranks[0]
                cpu_by_rank[primary] = cpu_by_rank.get(primary, 0) + replica.cpu_demand
        over_cpu = {rank: demand for rank, demand in cpu_by_rank.items() if demand > self.node_cpus}
        if over_cpu:
            raise PlanError(f"deployment topology exceeds per-rank CPU capacity: {over_cpu}")
        diagnostic = self.runtime.null_compute or self.runtime.instrumentation
        if diagnostic and not self.validation_mode:
            raise PlanError("diagnostic/degraded runtime policies require validation_mode")
        if self.collect_stats and (self.engine != "vllm" or self.runtime.null_compute):
            raise PlanError(
                "collect_stats requires the real vllm engine; the selected runtime "
                "cannot emit complete serving telemetry"
            )
        has_multi_replica_pp = any(
            model.pipeline_parallel_size > 1 and model.num_replicas > 1 for model in self.models
        )
        if has_multi_replica_pp != self.runtime.pp_shard_aware:
            raise PlanError("runtime.pp_shard_aware must be enabled exactly for multi-replica PP")

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
        return tuple(
            r
            for r in self.receipt_requirements
            if r.owner_scope == "RANK" and r.planned_rank == rank
        )

    def global_requirements(self) -> tuple[ReceiptRequirement, ...]:
        return tuple(r for r in self.receipt_requirements if r.owner_scope == "GLOBAL")

    def is_production_exposure(self) -> bool:
        return self.exposure.mode == ExposureMode.PROXIED_INTERNAL.value

    def uses_head_only_serve_proxy(self) -> bool:
        return self.exposure.mode == ExposureMode.RAY_SERVE_HEAD_ONLY.value


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
    speedup: float = 1.0
    sampling_strategy: str = "peak"
    generation_mode: str = "deterministic"
    modes: tuple[tuple[str, int], ...] = (("chat", 1), ("completion", 0))
    client_nodes: int = 1
    client_dest: str = "proxy"

    def __post_init__(self) -> None:
        for name in ("duration_s", "rate_per_node", "speedup"):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < (0 if name != "speedup" else 0.000000001)
            ):
                qualifier = "positive" if name == "speedup" else "non-negative"
                raise PlanError(f"workload.{name} must be finite and {qualifier}")
        for name in ("input_len", "output_len", "seed", "client_nodes"):
            value = getattr(self, name)
            minimum = 1 if name == "client_nodes" else 0
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise PlanError(f"workload.{name} must be an integer >= {minimum}")
        if self.arrival not in {"fixed", "poisson"}:
            raise PlanError("workload.arrival must be fixed or poisson")
        for name in ("kind", "sampling_strategy", "generation_mode", "client_dest"):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"workload.{name} must be a non-empty string")
        frozen_modes = freeze_mapping(self.modes, "workload.modes")
        if any(name not in {"chat", "completion"} for name, _ in frozen_modes):
            raise PlanError("workload.modes supports only chat and completion")
        if not frozen_modes or any(
            isinstance(weight, bool) or not isinstance(weight, int) or weight < 0
            for _, weight in frozen_modes
        ):
            raise PlanError("workload.modes must contain non-negative integer weights")
        if sum(weight for _, weight in frozen_modes) < 1:
            raise PlanError("workload.modes must contain at least one positive weight")
        object.__setattr__(self, "modes", frozen_modes)


@dataclass(frozen=True)
class TracePolicy:
    kind: str = "synthetic"
    prompt_content_hash: Optional[str] = None
    trace_content_hash: Optional[str] = None
    tokenizer_builder_ref: str = "default"
    generation_policy: tuple[tuple[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, str) or not value
            for value in (self.kind, self.tokenizer_builder_ref)
        ):
            raise PlanError("trace kind and tokenizer_builder_ref must be non-empty")
        for name in ("prompt_content_hash", "trace_content_hash"):
            value = getattr(self, name)
            if value is not None and (
                not isinstance(value, str)
                or len(value) != 64
                or any(char not in "0123456789abcdef" for char in value)
            ):
                raise PlanError(f"trace.{name} must be null or lowercase sha256")
        object.__setattr__(
            self,
            "generation_policy",
            freeze_mapping(self.generation_policy, "trace.generation_policy"),
        )


@dataclass(frozen=True)
class SaturationPolicy:
    enabled: bool = False
    search_mode: str = "binary"
    initial_rate: int = 100
    max_rate: int = 0
    step_duration_s: float = 10.0
    warmup_duration_s: float = 3.0
    cooldown_pause_s: float = 2.0
    tolerance: float = 0.05
    max_error_rate: float = 0.01
    plateau_ratio: float = 0.95
    max_p99_ttft: float = 0.0
    verify: bool = True
    step_up_start: int = 0
    step_up_end: int = 0
    step_up_increment: int = 0
    stream: bool = False

    def __post_init__(self) -> None:
        require_bool(self.enabled, "client.saturation.enabled")
        require_bool(self.verify, "client.saturation.verify")
        require_bool(self.stream, "client.saturation.stream")
        if self.search_mode not in {"binary", "step_up"}:
            raise PlanError("client.saturation.search_mode must be binary or step_up")
        for name in (
            "initial_rate",
            "max_rate",
            "step_up_start",
            "step_up_end",
            "step_up_increment",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise PlanError(f"client.saturation.{name} must be a non-negative integer")
        for name in (
            "step_duration_s",
            "warmup_duration_s",
            "cooldown_pause_s",
            "tolerance",
            "max_error_rate",
            "plateau_ratio",
            "max_p99_ttft",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < 0
            ):
                raise PlanError(f"client.saturation.{name} must be finite and non-negative")
        for name in ("tolerance", "max_error_rate", "plateau_ratio"):
            if getattr(self, name) > 1:
                raise PlanError(f"client.saturation.{name} must be <= 1")


@dataclass(frozen=True)
class ClientPolicy:
    destination: str = "proxy"
    nodes: int = 1
    streaming: bool = False
    dispatch_topology: str = "local"
    direct_pair_shift: int = 1
    request_timeout_s: float = 3600.0
    drain_wait_timeout_s: float = 3780.0
    shard_timeout_s: float = 600.0
    direct_target_ready_timeout_s: float = 300.0
    direct_target_probe_timeout_s: float = 2.0
    direct_target_interval_s: float = 5.0
    direct_target_max_workers: int = 64
    concurrency: int = 0
    workers: int = 1
    startup_only: bool = False
    num_runs: int = 1
    include_tp: bool = False
    early_stop: float = 0.0
    processes: int = 1
    warmup_rps: int = 0
    warmup_duration_s: float = 0.0
    sum_only: bool = False
    dispatch_topologies: tuple[str, ...] = ()
    saturation: SaturationPolicy = field(default_factory=SaturationPolicy)
    options: tuple[tuple[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if any(
            not isinstance(value, str) or not value
            for value in (self.destination, self.dispatch_topology)
        ):
            raise PlanError("client destination/dispatch_topology must be non-empty")
        for name, minimum in (
            ("nodes", 1),
            ("concurrency", 0),
            ("workers", 1),
            ("num_runs", 1),
            ("processes", 1),
            ("warmup_rps", 0),
            ("direct_pair_shift", 1),
            ("direct_target_max_workers", 1),
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise PlanError(f"client.{name} must be an integer >= {minimum}")
        for name in (
            "early_stop",
            "warmup_duration_s",
            "request_timeout_s",
            "drain_wait_timeout_s",
            "shard_timeout_s",
            "direct_target_ready_timeout_s",
            "direct_target_probe_timeout_s",
            "direct_target_interval_s",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
                or value < (0 if name in {"early_stop", "warmup_duration_s"} else 0.000000001)
            ):
                qualifier = (
                    "non-negative" if name in {"early_stop", "warmup_duration_s"} else "positive"
                )
                raise PlanError(f"client.{name} must be finite and {qualifier}")
        if self.early_stop > 1:
            raise PlanError("client.early_stop must be <= 1")
        if self.destination not in {"proxy", "direct"}:
            raise PlanError("client.destination must be proxy or direct")
        allowed_topologies = {"local", "mesh", "paired"}
        if self.dispatch_topology not in allowed_topologies:
            raise PlanError("client.dispatch_topology must be local, mesh, or paired")
        object.__setattr__(
            self,
            "dispatch_topologies",
            freeze_string_sequence(self.dispatch_topologies, "client.dispatch_topologies"),
        )
        if any(value not in allowed_topologies for value in self.dispatch_topologies):
            raise PlanError("client.dispatch_topologies contains an unsupported topology")
        if len(set(self.dispatch_topologies)) != len(self.dispatch_topologies):
            raise PlanError("client.dispatch_topologies must not contain duplicates")
        if self.dispatch_topologies and self.destination != "direct":
            raise PlanError("client.dispatch_topologies requires destination=direct")
        if self.destination == "proxy" and self.dispatch_topology != "local":
            raise PlanError("client.dispatch_topology must be local for destination=proxy")
        active_topologies = self.dispatch_topologies or (self.dispatch_topology,)
        if self.destination == "direct" and "paired" in active_topologies and self.nodes < 2:
            raise PlanError("client paired topology requires at least two client nodes")
        if not isinstance(self.saturation, SaturationPolicy):
            raise PlanError("client.saturation must be a SaturationPolicy")
        if self.saturation.enabled and (self.nodes != 1 or self.num_runs != 1):
            raise PlanError("client.saturation requires nodes=1 and num_runs=1")
        object.__setattr__(self, "options", freeze_mapping(self.options, "client.options"))
        require_bool(self.streaming, "client.streaming")
        require_bool(self.startup_only, "client.startup_only")
        require_bool(self.include_tp, "client.include_tp")
        require_bool(self.sum_only, "client.sum_only")


@dataclass(frozen=True)
class BackendPolicy:
    """Hash-bearing execution backend selection and adapter options."""

    name: str = "ray"
    options: tuple[tuple[str, Any], ...] = ()

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise PlanError("backend.name must be a non-empty string")
        object.__setattr__(self, "options", freeze_mapping(self.options, "backend.options"))


@dataclass(frozen=True)
class ArtifactPolicy:
    source_snapshot_policy: str = "packaged_artifact"
    trace_store_ref: str = "content-addressed"
    output_policy: str = "manifest_complete"
    retention_policy: str = "site_default"
    expected_artifacts: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for name in (
            "source_snapshot_policy",
            "trace_store_ref",
            "output_policy",
            "retention_policy",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"artifacts.{name} must be a non-empty string")
        object.__setattr__(
            self,
            "expected_artifacts",
            freeze_string_sequence(self.expected_artifacts, "artifacts.expected_artifacts"),
        )


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
    trace: TracePolicy = field(default_factory=TracePolicy)
    client: ClientPolicy = field(default_factory=ClientPolicy)
    backend: BackendPolicy = field(default_factory=BackendPolicy)
    artifacts: ArtifactPolicy = field(default_factory=ArtifactPolicy)
    run_semantic_hash: str = ""

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version, "run.schema_version")
        if not isinstance(self.run_id, str) or not self.run_id:
            raise PlanError("run.run_id must be a non-empty string")
        nested_contracts = (
            ("deployment", self.deployment, DeploymentPlan),
            ("scheduler", self.scheduler, SchedulerPlan),
            ("workload", self.workload, WorkloadPolicy),
            ("trace", self.trace, TracePolicy),
            ("client", self.client, ClientPolicy),
            ("backend", self.backend, BackendPolicy),
            ("artifacts", self.artifacts, ArtifactPolicy),
        )
        for name, value, contract in nested_contracts:
            if not isinstance(value, contract):
                raise PlanError(f"run.{name} must be a {contract.__name__}")
        require_sha256(self.deployment.deployment_plan_hash, "run.deployment.deployment_plan_hash")
        require_sha256(self.run_semantic_hash, "run.run_semantic_hash", allow_empty=True)
        if self.scheduler.type != self.deployment.scale_envelope.scheduler_type:
            raise PlanError("run scheduler type disagrees with deployment scale envelope")
        if self.client.nodes > self.scheduler.nodes:
            raise PlanError("run client nodes cannot exceed scheduler nodes")
        if (
            self.workload.client_nodes != self.client.nodes
            or self.workload.client_dest != self.client.destination
        ):
            raise PlanError("run workload client identity disagrees with client policy")
        if self.client.destination == "proxy" and self.deployment.exposure.mode not in {
            ExposureMode.PROXIED_INTERNAL.value,
            ExposureMode.RAY_SERVE_HEAD_ONLY.value,
        }:
            raise PlanError(
                "run proxy destination requires a PROXIED_INTERNAL or "
                "RAY_SERVE_HEAD_ONLY deployment"
            )
        if (
            self.deployment.exposure.mode == ExposureMode.RAY_SERVE_HEAD_ONLY.value
            and self.client.destination != "proxy"
        ):
            raise PlanError("RAY_SERVE_HEAD_ONLY deployment requires run proxy destination")
        active_topologies = self.client.dispatch_topologies or (self.client.dispatch_topology,)
        if (
            self.client.destination == "direct"
            and any(item in {"local", "paired"} for item in active_topologies)
            and self.client.nodes != self.deployment.num_nodes
        ):
            raise PlanError(
                "run direct local/paired topology requires one client rank per deployment node"
            )
        if self.client.destination == "direct" and any(
            item in {"local", "paired"} for item in active_topologies
        ):
            if len(self.deployment.models) != 1:
                raise PlanError("run direct replay requires exactly one deployment model")
            model = self.deployment.models[0]
            target_ranks = {
                replica.planned_ranks[0] for replica in model.replicas if replica.planned_ranks
            }
            if model.num_replicas > 1 and target_ranks != set(range(self.deployment.num_nodes)):
                raise PlanError(
                    "run direct local/paired topology requires addressable replica targets "
                    "on every deployment node; use mesh for partial-node replica routing"
                )
        if (
            self.client.saturation.enabled
            and self.client.destination == "direct"
            and self.deployment.num_nodes != 1
        ):
            raise PlanError("run direct saturation supports exactly one deployment node")
        positive_modes = {name for name, weight in self.workload.modes if weight > 0}
        request_mode = "mixed" if len(positive_modes) > 1 else next(iter(positive_modes))
        if request_mode != self.deployment.scale_envelope.request_mode:
            raise PlanError("run workload request mode disagrees with deployment scale envelope")
        streaming_mode = "streaming" if self.client.streaming else "non_streaming"
        if streaming_mode != self.deployment.scale_envelope.streaming_mode:
            raise PlanError("run client streaming mode disagrees with deployment scale envelope")

    def canonical(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "deployment_plan_hash": self.deployment.deployment_plan_hash,
            "scheduler": asdict(self.scheduler),
            "workload": asdict(self.workload),
            "trace": asdict(self.trace),
            "client": asdict(self.client),
            "backend": asdict(self.backend),
            "artifacts": asdict(self.artifacts),
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

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version, "binding.schema_version")
        if not isinstance(self.deployment_id, str) or not self.deployment_id:
            raise PlanError("binding.deployment_id must be non-empty")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 0
        ):
            raise PlanError("binding.generation must be a non-negative integer")
        if not isinstance(self.scheduler_allocation_id, str) or not self.scheduler_allocation_id:
            raise PlanError("binding.scheduler_allocation_id must be non-empty")
        require_sha256(self.deployment_plan_hash, "binding.deployment_plan_hash")
        require_sha256(self.site_profile_hash, "binding.site_profile_hash")
        require_sha256(
            self.allocation_binding_hash, "binding.allocation_binding_hash", allow_empty=True
        )
        try:
            normalized = tuple((rank, node) for rank, node in self.rank_to_node)
        except (TypeError, ValueError):
            raise PlanError("binding.rank_to_node must contain rank/node pairs") from None
        object.__setattr__(self, "rank_to_node", normalized)
        ranks = [rank for rank, _ in normalized]
        nodes = [canonical_node_id(node) for _, node in normalized]
        if any(isinstance(rank, bool) or not isinstance(rank, int) or rank < 0 for rank in ranks):
            raise PlanError("binding ranks must be non-negative integers")
        if ranks != list(range(len(ranks))):
            raise PlanError("binding ranks must be ordered and contiguous from zero")
        if any(not isinstance(node, str) or not canonical_node_id(node) for _, node in normalized):
            raise PlanError("binding node names must be non-empty strings")
        if len(nodes) != len(set(nodes)):
            raise PlanError("binding contains duplicate canonical node names")

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
    deployment_plan_hash: str
    site_profile_hash: str
    allocation_binding_hash: str
    receipt_requirement_id: str
    component_id: str
    instance_id: str
    owner_scope: str
    owner_rank: Optional[int]
    node_id: str
    bound_at: float
    state: str = "ACTIVE"
    binding_sequence: int = 0
    supersedes_instance_id: Optional[str] = None
    component_instance_binding_hash: str = ""

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version, "instance_binding.schema_version")
        for name in (
            "deployment_id",
            "receipt_requirement_id",
            "component_id",
            "instance_id",
            "node_id",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"instance_binding.{name} must be a non-empty string")
        for name in (
            "deployment_plan_hash",
            "site_profile_hash",
            "allocation_binding_hash",
        ):
            require_sha256(getattr(self, name), f"instance_binding.{name}")
        require_sha256(
            self.component_instance_binding_hash,
            "instance_binding.component_instance_binding_hash",
            allow_empty=True,
        )
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 0
        ):
            raise PlanError("instance_binding.generation must be a non-negative integer")
        if (
            isinstance(self.binding_sequence, bool)
            or not isinstance(self.binding_sequence, int)
            or self.binding_sequence < 0
        ):
            raise PlanError("instance_binding.binding_sequence must be non-negative")
        if (
            isinstance(self.bound_at, bool)
            or not isinstance(self.bound_at, (int, float))
            or not math.isfinite(float(self.bound_at))
            or self.bound_at < 0
        ):
            raise PlanError("instance_binding.bound_at must be finite and non-negative")
        if self.owner_scope not in {"GLOBAL", "RANK"}:
            raise PlanError("instance_binding.owner_scope must be GLOBAL or RANK")
        if self.owner_scope == "GLOBAL":
            if self.owner_rank is not None:
                raise PlanError("instance_binding.owner_rank must be null for GLOBAL")
        elif (
            isinstance(self.owner_rank, bool)
            or not isinstance(self.owner_rank, int)
            or self.owner_rank < 0
        ):
            raise PlanError("instance_binding RANK owner requires a non-negative owner_rank")
        if self.state not in {"ACTIVE", "SUPERSEDED", "REVOKED"}:
            raise PlanError("instance_binding.state must be ACTIVE, SUPERSEDED, or REVOKED")
        if self.supersedes_instance_id is not None and (
            not isinstance(self.supersedes_instance_id, str)
            or not self.supersedes_instance_id
            or self.supersedes_instance_id == self.instance_id
        ):
            raise PlanError(
                "instance_binding.supersedes_instance_id must name a distinct non-empty instance"
            )

    def key(self) -> str:
        return self.receipt_requirement_id

    def canonical(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("component_instance_binding_hash", None)
        return data

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "ComponentInstanceBinding":
        return replace(self, component_instance_binding_hash=self.compute_hash())


@dataclass(frozen=True)
class RunProvenance:
    """Generation/execution evidence kept outside every semantic hash."""

    schema_version: int
    run_id: str
    deployment_id: str
    generation: int
    deployment_plan_hash: str
    allocation_binding_hash: str
    run_semantic_hash: Optional[str]
    source_snapshot_hash: str
    resolved_input_paths: tuple[str, ...]
    argv: tuple[str, ...]
    prepared_environment_hash: str
    started_at: str
    output_locations: tuple[str, ...]
    run_provenance_hash: str = ""

    def __post_init__(self) -> None:
        require_schema_version(self.schema_version, "provenance.schema_version")
        for name in (
            "run_id",
            "deployment_id",
            "source_snapshot_hash",
            "prepared_environment_hash",
            "started_at",
        ):
            if not isinstance(getattr(self, name), str) or not getattr(self, name):
                raise PlanError(f"provenance.{name} must be a non-empty string")
        for name in (
            "deployment_plan_hash",
            "allocation_binding_hash",
            "source_snapshot_hash",
            "prepared_environment_hash",
        ):
            require_sha256(getattr(self, name), f"provenance.{name}")
        if self.run_semantic_hash is not None:
            require_sha256(self.run_semantic_hash, "provenance.run_semantic_hash")
        require_sha256(self.run_provenance_hash, "provenance.run_provenance_hash", allow_empty=True)
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 0
        ):
            raise PlanError("provenance.generation must be non-negative")
        for name in ("resolved_input_paths", "argv", "output_locations"):
            object.__setattr__(
                self,
                name,
                freeze_string_sequence(getattr(self, name), f"provenance.{name}"),
            )

    def canonical(self) -> dict[str, Any]:
        data = asdict(self)
        data.pop("run_provenance_hash", None)
        return data

    def compute_hash(self) -> str:
        return canonical_hash(self.canonical())

    def finalize(self) -> "RunProvenance":
        return replace(self, run_provenance_hash=self.compute_hash())


def canonical_node_id(node_id: str) -> str:
    """The comparable form of a node name: lowercase, domain stripped.

    Nothing is *stored* in this form — receipts and bindings keep exactly what
    their producer observed, so the record stays faithful. This is only how two
    names are compared.
    """
    if not isinstance(node_id, str):
        return ""
    return node_id.strip().lower().split(".", 1)[0]


def same_node(left: str, right: str) -> bool:
    canonical = canonical_node_id(left)
    return bool(canonical) and canonical == canonical_node_id(right)


def build_allocation_binding(
    *, plan: DeploymentPlan, generation: int, scheduler_allocation_id: str, nodes: list[str]
) -> AllocationBinding:
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
            f"{plan.num_nodes}; refusing to bind a different deployment"
        )
    return AllocationBinding(
        schema_version=SCHEMA_VERSION,
        deployment_id=plan.deployment_id,
        generation=generation,
        deployment_plan_hash=plan.deployment_plan_hash,
        site_profile_hash=plan.site_profile_hash,
        scheduler_allocation_id=scheduler_allocation_id,
        rank_to_node=tuple((index, node) for index, node in enumerate(unique)),
    ).finalize()


def provenance_hash(
    *,
    deployment_plan_hash: str,
    allocation_binding_hash: str,
    source_path: str = "",
    started_at: str = "",
    argv: tuple[str, ...] = (),
) -> str:
    """Everything that is NOT semantic intent: paths, times, commands."""
    return canonical_hash(
        {
            "deployment_plan_hash": deployment_plan_hash,
            "allocation_binding_hash": allocation_binding_hash,
            "source_path": source_path,
            "started_at": started_at,
            "argv": list(argv),
        }
    )
