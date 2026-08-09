"""Site capabilities as one artifact (plan §3.2.1 Q5, packet P01, TD-CONSTS).

Site constants were scattered across a shell script, a Python config module,
and several literals. `TD-CONSTS` was previously dismissed as cosmetic; the
plan is explicit that it is not — the SiteProfile is a hash-bearing input to
the DeploymentPlan, so a site value that lives somewhere else is a value that
can drift without changing any plan identity.

The control-limit values here are **not production-qualified**. They are
structurally valid defaults so the contract can be exercised; `evidence_backed`
stays False until measured values are recorded at the required tiers, and a
plan compiled against an unqualified profile is a validation plan, not a
production one.
"""

from __future__ import annotations

import os
import ipaddress
import pwd
import resource
import socket
from pathlib import Path
from functools import lru_cache

from .plan.contracts import (
    SCHEMA_VERSION,
    ControlLimits,
    ExposureMode,
    ReadinessLimits,
    ScaleEnvelope,
    SiteProfile,
)

AURORA_SITE_ID = "alcf-aurora"


def local_account_name() -> str:
    """Return the passwd-backed account name as a safe path component."""

    try:
        account = pwd.getpwuid(os.getuid()).pw_name
    except (KeyError, OSError) as exc:
        raise RuntimeError("cannot derive the local account name from the passwd database") from exc
    if not account or account in {".", ".."} or Path(account).name != account:
        raise RuntimeError("the local account name is not a safe path component")
    return account


def default_model_storage_path() -> str:
    """Derive an account-local Aurora path without embedding a developer name."""

    project_root = Path(os.environ.get("EXASERVE_PROJECT_ROOT", "/lus/flare/projects/AuroraGPT"))
    if not project_root.is_absolute():
        raise RuntimeError("EXASERVE_PROJECT_ROOT must be an absolute path")
    try:
        account = local_account_name()
    except RuntimeError as exc:
        raise RuntimeError(
            "cannot derive the Aurora account model path; set EXASERVE_MODEL_STORAGE_PATH"
        ) from exc
    return str(project_root / account / "models")


# Values formerly exported with ``${NAME:-default}`` by launch_cluster.sh.
# That made an inherited login shell an undocumented second configuration
# source.  They are now exact SiteProfile content: drift changes the site hash,
# and activation overwrites ambient values before any Ray process is created.
_AURORA_PREPARED_ENVIRONMENT = (
    ("CCL_PROCESS_LAUNCHER", "hydra"),
    ("EXASERVE_RAY_INTERNAL_STARTUP_LIMIT", "8"),
    ("MKL_NUM_THREADS", "1"),
    ("NUMEXPR_NUM_THREADS", "1"),
    ("OMP_NUM_THREADS", "1"),
    ("OPENBLAS_NUM_THREADS", "1"),
    ("PYTHONUNBUFFERED", "1"),
    ("RAYON_NUM_THREADS", "1"),
    ("RAY_SERVE_MAX_DEPLOYMENT_CONSTRUCTOR_RETRY_COUNT", "200"),
    ("RAY_SERVE_QUEUE_LENGTH_RESPONSE_DEADLINE_S", "300.0"),
    ("RAY_SERVE_THROUGHPUT_OPTIMIZED", "1"),
    ("RAY_core_worker_num_server_call_thread", "1"),
    ("RAY_enable_metrics_collection", "0"),
    ("RAY_gcs_rpc_server_connect_timeout_s", "30"),
    ("RAY_gcs_rpc_server_reconnect_timeout_s", "120"),
    ("RAY_gcs_server_num_threads", "8"),
    ("RAY_gcs_server_request_timeout_seconds", "60"),
    ("RAY_num_grpc_internal_threads", "1"),
    ("RAY_num_server_call_thread", "4"),
    ("RAY_raylet_client_connect_timeout_milliseconds", "30000"),
    ("RAY_raylet_client_num_connect_attempts", "20"),
    ("RAY_task_events_report_interval_ms", "0"),
    ("RAY_worker_num_grpc_internal_threads", "1"),
    ("RAY_worker_register_timeout_seconds", "120"),
    ("TOKENIZERS_PARALLELISM", "false"),
    ("VECLIB_MAXIMUM_THREADS", "1"),
    ("ZE_AFFINITY_MASK", ""),
    ("ZE_FLAT_DEVICE_HIERARCHY", "FLAT"),
    # Ray must leave device selection to ZE_AFFINITY_MASK on Aurora.  The
    # selector itself is explicitly removed below; it is never introduced.
    ("RAY_EXPERIMENTAL_NOSET_ONEAPI_DEVICE_SELECTOR", "1"),
)


@lru_cache(maxsize=4)
def default_site_profile(site_id: str = "") -> SiteProfile:
    """The site profile for this deployment host.

    Values come from the environment where the site genuinely varies, and are
    fixed here where they are properties of the machine.
    """
    site_id = site_id or os.environ.get("EXASERVE_SITE_ID", AURORA_SITE_ID)
    from .compat.profile import default_profile

    max_nodes = int(os.environ.get("EXASERVE_SITE_MAX_NODES", "64"))
    gpus_per_node = int(os.environ.get("EXASERVE_SITE_GPUS_PER_NODE", "12"))
    model_storage_path = os.environ.get("EXASERVE_MODEL_STORAGE_PATH")
    if not model_storage_path:
        model_storage_path = default_model_storage_path()
    profile = default_profile("xpu")
    # Only this exact dimension has candidate-bound one-/two-node evidence.
    # Other implemented capabilities compile into explicit validation-only
    # envelopes; listing them here would overstate an evidence-backed maximum
    # for chat, streaming, direct exposure, or an alternate gateway.
    envelopes = [
        ScaleEnvelope(
            schema_version=SCHEMA_VERSION,
            envelope_id="aurora-xpu-vllm-haproxy-completion-non_streaming-candidate64",
            site_id=site_id,
            scheduler_type="pbs",
            vendor="xpu",
            accelerator="pvc",
            engine="vllm",
            compatibility_profile_ref=profile.profile_id,
            gateway_kind="haproxy",
            exposure_mode=ExposureMode.PROXIED_INTERNAL.value,
            request_mode="completion",
            streaming_mode="non_streaming",
            min_nodes=1,
            supported_max_nodes=min(2, max_nodes),
            qualification_target_nodes=max_nodes,
            qualification_target_approved=False,
            max_replicas_per_model=max_nodes * gpus_per_node,
            max_total_replicas=max_nodes * gpus_per_node,
            max_models=64,
            validation_tier="candidate-64-unapproved",
            evidence_refs=("doc/hardening/decisions/ADR-000-production-envelope.md",),
        )
    ]
    return SiteProfile(
        schema_version=SCHEMA_VERSION,
        site_id=site_id,
        max_nodes=max_nodes,
        gpus_per_node=gpus_per_node,
        cpus_per_node=int(os.environ.get("EXASERVE_SITE_CPUS_PER_NODE", "64")),
        # This default is an Aurora release profile, not a registry of code
        # paths that happen to exist. Slurm, CUDA/ROCm, and SGLang require a
        # separately named SiteProfile with their own exact compatibility and
        # qualification evidence.
        scheduler_types=("pbs",),
        gateway_kinds=("haproxy", "litellm", "nginx", "envoy", "pingora"),
        vendors=("xpu",),
        engines=("vllm",),
        model_storage_path=model_storage_path,
        local_stage_path=os.environ.get("EXASERVE_LOCAL_STAGE_PATH", "/tmp/hf_home"),
        control=ControlLimits(),  # evidence_backed=False by construction
        readiness=ReadinessLimits(),  # likewise awaits WP12 measurements
        launcher_capabilities=("mpi", "pbs", "ray_serve.run_many"),
        filesystem_semantics=(("shared", "lustre"), ("local_stage", "node_local")),
        accelerator_inventory=("pvc",),
        network_boundary="trusted_allocation",
        environment_profile_ref=profile.profile_id,
        prepared_environment=_AURORA_PREPARED_ENVIRONMENT,
        environment_unset=("ONEAPI_DEVICE_SELECTOR",),
        stack_size_kb=8192,
        scale_envelopes=tuple(envelopes),
    ).finalize()


def apply_site_profile_environment(
    profile: SiteProfile, *, environment: dict[str, str] | None = None
) -> dict[str, str]:
    """Apply one verified SiteProfile's exact environment and stack limit.

    The returned mapping is the environment that children should inherit.
    When ``environment`` is omitted this process is prepared in-place.  Stack
    preparation is process-wide and therefore only performed for that normal
    in-place mode.
    """
    target = os.environ if environment is None else environment
    for key in profile.environment_unset:
        target.pop(key, None)
    for key, value in profile.prepared_environment:
        target[key] = value
    if environment is None:
        wanted = profile.stack_size_kb * 1024
        soft, hard = resource.getrlimit(resource.RLIMIT_STACK)
        if hard != resource.RLIM_INFINITY and wanted > hard:
            raise RuntimeError(
                f"SiteProfile stack limit {profile.stack_size_kb} KiB exceeds "
                f"the process hard limit {hard // 1024} KiB"
            )
        resource.setrlimit(resource.RLIMIT_STACK, (wanted, hard))
    return target


def resolve_allocation_node_address(node: str, *, site_id: str) -> str:
    """Resolve one bound allocation node to a non-loopback IPv4 address.

    Address selection is site setup derived from the immutable allocation
    binding.  Ambient ``EXASERVE_HEAD_IP``/``RAY_HEAD_IP`` values are not an
    authority and cannot silently redirect a generation to another cluster.
    """
    if not isinstance(node, str) or not node.strip():
        raise RuntimeError("allocation binding contains an empty node name")
    node = node.strip()
    candidates = [node]
    if site_id == AURORA_SITE_ID:
        short = node.split(".", 1)[0]
        hsn = f"{short}.hsn.cm.aurora.alcf.anl.gov"
        candidates = [hsn, node]
    errors = []
    for candidate in dict.fromkeys(candidates):
        try:
            infos = socket.getaddrinfo(
                candidate, None, family=socket.AF_INET, type=socket.SOCK_STREAM
            )
        except OSError as exc:
            errors.append(f"{candidate}: {exc}")
            continue
        addresses = sorted({item[4][0] for item in infos})
        for address in addresses:
            parsed = ipaddress.ip_address(address)
            if not (parsed.is_loopback or parsed.is_unspecified or parsed.is_multicast):
                return address
        errors.append(f"{candidate}: no allocation-reachable IPv4 address")
    raise RuntimeError(
        f"could not resolve bound head node {node!r} for site {site_id!r}: " + "; ".join(errors)
    )


def prepare_runtime_site(plan, path: str | None = None) -> SiteProfile:
    """Verify the runtime SiteProfile against ``plan`` and prepare this process."""
    from .plan.io import load_site_profile

    resolved_path = path or os.environ.get("EXASERVE_SITE_PROFILE_PATH", "")
    if not resolved_path:
        raise RuntimeError("EXASERVE_SITE_PROFILE_PATH is required at runtime")
    profile = load_site_profile(resolved_path)
    if (
        profile.site_id != plan.site_profile_id
        or profile.site_profile_hash != plan.site_profile_hash
    ):
        raise RuntimeError("runtime SiteProfile does not belong to DeploymentPlan")
    if profile.environment_profile_ref != plan.compatibility_profile_hash:
        raise RuntimeError(
            "runtime SiteProfile compatibility profile does not match DeploymentPlan"
        )
    apply_site_profile_environment(profile)
    return profile


def is_production_qualified(profile: SiteProfile) -> tuple[bool, str]:
    """Return whether the site-wide control contract has measured evidence."""
    if not profile.control.evidence_backed or not profile.readiness.evidence_backed:
        return False, (
            f"SiteProfile {profile.site_id} carries unmeasured control/readiness "
            "limits; "
            "measure them at the required tiers (S01/WP12) before treating a "
            "plan compiled against it as production-qualified"
        )
    return True, "site profile is evidence-backed"


def require_execution_qualification(plan, profile: SiteProfile) -> bool:
    """Fail closed before executing a production-mode deployment.

    Compilation remains usable for inspecting candidate plans and for tests of
    the immutable contracts.  Crossing the submission/launcher boundary is a
    stronger operation: a non-validation plan must be backed by measured site
    limits, an explicitly approved scale envelope, and durable qualification
    evidence.  Validation-mode plans are allowed through this boundary but are
    deliberately returned as *not* production-qualified.

    Returns ``True`` only for a qualified production execution and ``False``
    for an explicit validation execution.  Otherwise it raises before any job
    or child process can be created.
    """
    if (
        plan.site_profile_id != profile.site_id
        or plan.site_profile_hash != profile.site_profile_hash
    ):
        raise RuntimeError("DeploymentPlan and SiteProfile identities do not match")
    if plan.validation_mode:
        return False
    qualified, reason = is_production_qualified(profile)
    if not qualified:
        raise RuntimeError(f"production execution is not qualified: {reason}")
    envelope = plan.scale_envelope
    if envelope.validation_mode:
        raise RuntimeError("production plan is bound to a validation-mode scale envelope")
    if envelope not in profile.scale_envelopes:
        raise RuntimeError(
            f"production scale envelope {envelope.envelope_id!r} is not declared by "
            f"SiteProfile {profile.site_id!r}"
        )
    if not envelope.qualification_target_approved:
        raise RuntimeError(
            f"production scale envelope {envelope.envelope_id!r} has no durable "
            "product-owner approval"
        )
    if not envelope.evidence_refs:
        raise RuntimeError(
            f"production scale envelope {envelope.envelope_id!r} has no qualification evidence"
        )
    if plan.num_nodes > envelope.supported_max_nodes:
        # The plan contract already enforces this.  Keep the execution boundary
        # independently fail-closed in case a future loader changes that rule.
        raise RuntimeError(
            f"production plan requests {plan.num_nodes} nodes above evidence-backed "
            f"maximum {envelope.supported_max_nodes}"
        )
    return True
