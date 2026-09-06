"""Engine-process self-attestation (plan WP3.13/WP4, audit IMP-B04, EN-01).

The vLLM ``EngineCore`` runs in a spawned subprocess that ExaServe does not
import into. Until now it was attested by its owning replica — an honest but
weak evidence class: the owner can prove the executable and the prepared
environment, never that a patch took effect *inside* the engine.

This module closes that gap without giving the engine a Ray connection. The
generated ``sitecustomize`` shim (EN-01) already runs at engine-interpreter
startup; it now also writes a receipt describing what THIS process actually
received, and the owning replica forwards those receipts to the control plane.

The distinction that keeps it honest: the shim reports whether the patch set
was *delivered* to this process at all.

- PP deployments install the patch import, so the shim proves each required
  patch with the same in-process sentinels the replica uses.
- Non-PP deployments resolve the gated vLLM/PP patches as ``NOT_REQUIRED``;
  EN-01 remains required and is proven by this code running in the engine.

Version strings are passed in from the replica so the shim does not import Ray
inside an engine process just to read ``__version__``.
"""

from __future__ import annotations

import glob
import hashlib
import os
import sys
import threading
import time
import errno
import math
from dataclasses import dataclass
from typing import Optional

RECEIPT_DIR_ENV = "EXASERVE_ENGINE_RECEIPT_DIR"
IMPORT_PATCHES_ENV = "EXASERVE_ENGINE_SHIM_PATCHES"
ENGINE_KIND_ENV = "EXASERVE_ENGINE_SHIM_KIND"
ENGINE_MODEL_ENV = "EXASERVE_RECEIPT_MODEL_ID_ENGINE"
ENGINE_DEVICE_IDS_ENV = "EXASERVE_RECEIPT_DEVICE_IDS_ENGINE"
ENGINE_REPLICA_INDEX_ENV = "EXASERVE_RECEIPT_REPLICA_INDEX"
ENGINE_WORKER_RANK_ENV = "EXASERVE_ENGINE_WORKER_GLOBAL_RANK"
ENGINE_WORKER_KIND_ENV = "EXASERVE_ENGINE_WORKER_KIND"

_ENGINE_IDENTITY_LOCK = threading.Lock()
_ENGINE_IDENTITY_CACHE: dict[tuple[str, ...], dict] = {}

# Written verbatim as ``sitecustomize.py`` in the immutable compatibility
# overlay.  Source staging publishes that overlay at the same node-local path
# on every rank.  Keeping the engine bootstrap there is essential for Ray PP
# workers: a shim created later in the replica's node-local ``/tmp`` does not
# exist on a remote actor's node even if Ray faithfully copies ``PYTHONPATH``.
#
# The non-engine branch preserves the ordinary generated-overlay bootstrap.
# The engine branch stays import-light and fails closed before a vLLM/Ray patch
# target can be imported.  Attestation failures after that point withhold READY
# and remain diagnosable without crashing an otherwise inspectable engine.
_SHIM_SOURCE = '''\
"""Generated ExaServe compatibility and EN-01 bootstrap. Do not edit."""
import os
import sys

_engine_kind = os.environ.get("EXASERVE_ENGINE_SHIM_KIND", "")
if _engine_kind:
    # This file runs only in the spawned interpreter. Bind its role here so
    # preparing a child never mutates the replica parent's overlay role.
    os.environ["EXASERVE_COMPAT_ROLE"] = "engine_bootstrap"
    try:
        # Verify the exact profile, dependency versions, upstream source bytes,
        # and patch artifacts before importing any Ray/vLLM patch target.
        from exaserve.compat.engine_shim import verify_engine_bootstrap
        verify_engine_bootstrap(_engine_kind)
    except Exception as exc:
        print("[ExaServe EngineShim] compatibility verification failed: %s: %s" % (type(exc).__name__, exc), file=sys.stderr, flush=True)
        os._exit(78)

    try:
        from exaserve.compat.engine_shim import start_engine_attestation

        _exaserve_attestation_watcher = start_engine_attestation(
            patches_imported=True, engine_kind=_engine_kind
        )
    except Exception as exc:
        print("[ExaServe EngineShim] attestation start failed: %s: %s" % (type(exc).__name__, exc), file=sys.stderr, flush=True)
else:
    from exaserve.compat.generated_overlay import install_from_environment
    install_from_environment()
'''


_MANAGED_ENV_KEYS = frozenset(
    {
        "PYTHONPATH",
        RECEIPT_DIR_ENV,
        IMPORT_PATCHES_ENV,
        ENGINE_KIND_ENV,
        ENGINE_MODEL_ENV,
        ENGINE_DEVICE_IDS_ENV,
        ENGINE_REPLICA_INDEX_ENV,
        ENGINE_WORKER_RANK_ENV,
        ENGINE_WORKER_KIND_ENV,
        "EXASERVE_DEPLOYMENT_ID",
        "EXASERVE_GENERATION",
        "EXASERVE_VENDOR",
        "EXASERVE_COMPAT_PROFILE_ID",
        "EXASERVE_COMPAT_MANIFEST_HASH",
        "EXASERVE_COMPAT_ROLE",
        "EXASERVE_QUALIFIED_PYTHON",
        "EXASERVE_QUALIFIED_PYTHON_SHA256",
        "EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH",
        "EXASERVE_COMPAT_SOURCES_NODE_PROFILE",
        "EXASERVE_COMPAT_SOURCES_NODE_MANIFEST",
    }
)


def _ensure_local_receipt_directory(directory: str) -> None:
    from ..model_staging import ensure_node_local_directory
    from ..plan.runtime_environment import LOCAL_STATE_ROOT_ENV, require_contained_local_path

    state_root = os.environ.get(LOCAL_STATE_ROOT_ENV, "")
    if not state_root:
        raise OSError("engine receipt directory has no local state root")
    try:
        require_contained_local_path(
            directory,
            state_root,
            name="engine receipt directory",
            require_exists=False,
        )
        ensure_node_local_directory(
            directory,
            mode=0o700,
            enforce_mode=True,
        )
        require_contained_local_path(
            directory,
            state_root,
            name="engine receipt directory",
            require_exists=True,
        )
    except (RuntimeError, ValueError) as exc:
        raise OSError(f"engine receipt directory is unsafe: {exc}") from exc


def verify_engine_bootstrap(engine_kind: str) -> None:
    """Fail closed before a spawned engine's first Ray/vLLM import."""
    if engine_kind != "vllm":
        raise RuntimeError(f"unsupported engine shim kind {engine_kind!r}")
    from ..plan.runtime_environment import (
        LOCAL_RUNTIME_ROOT_ENV,
        LOCAL_STATE_ROOT_ENV,
        require_contained_local_path,
        validate_vllm_rpc_base_path,
    )

    runtime_root = os.environ.get(LOCAL_RUNTIME_ROOT_ENV, "")
    if not runtime_root:
        raise RuntimeError(f"{LOCAL_RUNTIME_ROOT_ENV} is required for engine bootstrap")
    require_contained_local_path(
        __file__,
        os.path.join(runtime_root, "python"),
        name="engine ExaServe module",
        require_exists=True,
    )
    validate_vllm_rpc_base_path(
        os.environ.get("VLLM_RPC_BASE_PATH", ""),
        os.environ.get(LOCAL_STATE_ROOT_ENV, ""),
    )
    from .activator import CompatibilityActivator
    from .producers import manifest_hash
    from .profile import default_profile

    profile = default_profile(os.environ.get("EXASERVE_VENDOR", "xpu"))
    expected_profile = os.environ.get("EXASERVE_COMPAT_PROFILE_ID", "")
    expected_manifest = os.environ.get("EXASERVE_COMPAT_MANIFEST_HASH", "")
    if profile.profile_id != expected_profile:
        raise RuntimeError(
            "engine compatibility profile identity does not match the DeploymentPlan"
        )
    if manifest_hash(profile) != expected_manifest:
        raise RuntimeError(
            "engine compatibility manifest identity does not match the DeploymentPlan"
        )
    # The bootstrap role has no semantic patch of its own. Verify the exact
    # environment first, then install the complete, role-aware import
    # interceptor without importing any Ray/vLLM target eagerly. Ray requires
    # a fresh Python worker to register within its worker-start deadline;
    # importing the whole engine patch surface from sitecustomize made that
    # registration depend on multi-gigabyte framework initialization before
    # Ray had assigned the worker's runtime identity and accelerator context.
    #
    # The finder filters against the live role at each natural target import.
    # The attestation watcher still resolves the actual process role later and
    # withholds the receipt until every required in-process postcondition is
    # demonstrably true, so lazy delivery does not weaken fail-closed proof.
    CompatibilityActivator(profile=profile).activate("engine_bootstrap", apply_fn=lambda: None)
    from .generated_overlay import ROOT_ENV, install as install_generated_overlay

    root = os.environ.get(ROOT_ENV, "")
    if not root:
        raise RuntimeError(f"{ROOT_ENV} is required for engine bootstrap")
    install_generated_overlay(profile, root, role="engine_bootstrap")


def environment_snapshot() -> dict[str, Optional[str]]:
    """Capture every variable preparation may overwrite in its parent process."""
    keys = _MANAGED_ENV_KEYS | {key for key in os.environ if key.startswith("EXASERVE_VERSION_")}
    return {key: os.environ.get(key) for key in keys}


def restore_environment(snapshot: dict[str, Optional[str]]) -> None:
    """Undo shim injection after the engine subprocess has inherited it."""
    current_versions = {key for key in os.environ if key.startswith("EXASERVE_VERSION_")}
    for key in _MANAGED_ENV_KEYS | current_versions | set(snapshot):
        value = snapshot.get(key)
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def shim_source() -> str:
    return _SHIM_SOURCE


def prepare_environment(
    receipt_dir: str,
    *,
    import_patches: bool,
    deployment_id: str,
    generation: int,
    vendor: str,
    engine_kind: str = "vllm",
    versions: Optional[dict] = None,
) -> None:
    """Prepare an engine child's environment against the staged bootstrap.

    The compatibility root is content-inventoried and broadcast before Ray
    starts.  This boundary verifies that exact bootstrap rather than creating a
    second mutable, head-only shim.  Filesystem and contract errors propagate
    with their causes so the engine constructor can fail closed and explain
    why; callers restore the parent environment in ``finally``.
    """
    from .generated_overlay import ROOT_ENV
    from ..plan.runtime_environment import (
        LOCAL_RUNTIME_ROOT_ENV,
        LOCAL_STATE_ROOT_ENV,
        require_contained_local_path,
    )

    if engine_kind != "vllm":
        raise ValueError(f"unsupported engine shim kind {engine_kind!r}")
    root = os.environ.get(ROOT_ENV, "")
    if not root:
        raise RuntimeError(f"{ROOT_ENV} is required for engine bootstrap")
    root = os.path.abspath(root)
    runtime_root = os.environ.get(LOCAL_RUNTIME_ROOT_ENV, "")
    state_root = os.environ.get(LOCAL_STATE_ROOT_ENV, "")
    if not runtime_root or not state_root:
        raise RuntimeError("engine preparation requires the published local runtime/state roots")
    require_contained_local_path(
        root,
        runtime_root,
        name="engine compatibility overlay",
        require_exists=True,
    )
    require_contained_local_path(
        receipt_dir,
        state_root,
        name="engine receipt directory",
        require_exists=False,
    )
    if os.environ.get("PYTHONNOUSERSITE") != "1":
        raise RuntimeError("engine preparation requires PYTHONNOUSERSITE=1")
    if os.environ.get("PYTHONSAFEPATH") != "1":
        raise RuntimeError("engine preparation requires PYTHONSAFEPATH=1")
    python_paths = [
        os.path.abspath(item) for item in os.environ.get("PYTHONPATH", "").split(os.pathsep) if item
    ]
    if root not in python_paths:
        raise RuntimeError("compatibility bootstrap root is absent from PYTHONPATH")
    shim_path = os.path.join(root, "sitecustomize.py")
    from ..state.atomic import regular_file_reader

    with regular_file_reader(shim_path) as fh:
        observed_source = fh.read()
    if observed_source != _SHIM_SOURCE:
        raise RuntimeError("staged compatibility bootstrap source does not match EN-01")

    _ensure_local_receipt_directory(receipt_dir)
    os.environ[RECEIPT_DIR_ENV] = receipt_dir
    os.environ[IMPORT_PATCHES_ENV] = "1" if import_patches else "0"
    os.environ[ENGINE_KIND_ENV] = engine_kind
    # Worker-only identity must be established by the pinned Ray or
    # multiprocessing bootstrap adapter, never inherited accidentally by
    # EngineCore from the replica's ambient environment.
    os.environ.pop(ENGINE_WORKER_RANK_ENV, None)
    os.environ.pop(ENGINE_WORKER_KIND_ENV, None)
    os.environ["EXASERVE_DEPLOYMENT_ID"] = str(deployment_id)
    os.environ["EXASERVE_GENERATION"] = str(generation)
    os.environ["EXASERVE_VENDOR"] = str(vendor)
    for key, value in (versions or {}).items():
        os.environ[f"EXASERVE_VERSION_{key.upper()}"] = str(value)


def _ray_actor_identity() -> tuple[object, str] | None:
    """Return a live Ray actor context without accepting driver processes."""
    try:
        import ray

        if not ray.is_initialized():
            return None
        context = ray.get_runtime_context()
        actor_id = context.get_actor_id()
        actor_text = actor_id.hex() if hasattr(actor_id, "hex") else str(actor_id)
        if not actor_text or set(actor_text) == {"0"}:
            return None
        return context, actor_text
    except (ImportError, AttributeError, RuntimeError, TypeError, ValueError):
        return None


def _engine_process_role(engine_kind: str | None) -> str | None:
    """Classify only the vLLM processes that own planned engine slots."""
    if engine_kind in (None, ""):
        return "engine_core"  # direct producer/test calls identify a core
    if engine_kind == "vllm":
        worker_kind = os.environ.get(ENGINE_WORKER_KIND_ENV)
        if "vllm.v1.worker.gpu_model_runner" in sys.modules and (
            (worker_kind == "ray" and _ray_actor_identity() is not None)
            or (worker_kind == "multiproc" and os.environ.get(ENGINE_WORKER_RANK_ENV))
        ):
            return "engine_worker"
        # Import presence is insufficient: the spawn resource tracker and
        # worker children can import the same module. vLLM pins this process
        # name when constructing its EngineCore process; wait until the child
        # bootstrap has installed that identity.
        import multiprocessing

        if (
            "vllm.v1.engine.core" in sys.modules
            and multiprocessing.current_process().name.startswith("EngineCore_DP")
        ):
            return "engine_core"
    return None


def _engine_process_ready(engine_kind: str | None) -> bool:
    """Exclude multiprocessing helpers that merely inherited sitecustomize.

    Python's resource tracker also imports ``sitecustomize``. It is not an
    EngineCore and must never supersede the real engine's exact receipt slot.
    The watcher evaluates this before importing vLLM itself, then retries while
    the target process completes its normal imports.
    """
    return _engine_process_role(engine_kind) is not None


def _resolve_engine_receipt_identity(engine_kind: str | None) -> dict | None:
    role = _engine_process_role(engine_kind)
    if role is None:
        return None
    if role == "engine_core":
        requirement_id = os.environ.get("EXASERVE_RECEIPT_REQUIREMENT_ID_ENGINE", "")
        component_id = os.environ.get("EXASERVE_RECEIPT_COMPONENT_ID_ENGINE", "")
        owner_rank_raw = os.environ.get("EXASERVE_RECEIPT_RANK")
        if not requirement_id or not component_id or owner_rank_raw is None:
            return None
        try:
            owner_rank = int(owner_rank_raw)
        except ValueError:
            return None
        if owner_rank < 0:
            return None
        return {
            "role": role,
            "requirement_id": requirement_id,
            "component_id": component_id,
            "owner_rank": owner_rank,
            "actor_id": None,
        }

    model_id = os.environ.get(ENGINE_MODEL_ENV, "")
    plan_path = os.environ.get("EXASERVE_PLAN_PATH", "")
    binding_path = os.environ.get("EXASERVE_ALLOCATION_BINDING_PATH", "")
    if not model_id or not plan_path or not binding_path:
        return None
    from ..plan.runtime_environment import (
        LOCAL_RUNTIME_ROOT_ENV,
        require_contained_local_path,
    )

    runtime_root = os.environ.get(LOCAL_RUNTIME_ROOT_ENV, "")
    if not runtime_root:
        return None
    require_contained_local_path(
        plan_path,
        runtime_root,
        name="engine DeploymentPlan",
        require_exists=True,
    )
    require_contained_local_path(
        binding_path,
        runtime_root,
        name="engine AllocationBinding",
        require_exists=True,
    )
    worker_kind = os.environ.get(ENGINE_WORKER_KIND_ENV)
    actor = _ray_actor_identity() if worker_kind == "ray" else None
    actor_id = None
    if actor is not None:
        context, actor_id = actor
        accelerator_ids = context.get_accelerator_ids()
        if not isinstance(accelerator_ids, dict):
            return None
        device_ids = []
        for values in accelerator_ids.values():
            if isinstance(values, (list, tuple)):
                device_ids.extend(int(value) for value in values)
        if len(set(device_ids)) != 1:
            return None
    elif worker_kind != "multiproc":
        return None

    # The pinned vLLM bootstrap supplies the stable logical world rank for
    # both Ray and multiprocessing workers.  A Ray-selected physical GPU id is
    # merely an observed resource and may change when an actor is retried; it
    # must never choose the canonical evidence slot.
    try:
        worker_rank = int(os.environ.get(ENGINE_WORKER_RANK_ENV, ""))
        replica_index = int(os.environ.get(ENGINE_REPLICA_INDEX_ENV, ""))
    except ValueError:
        return None
    if worker_rank < 0 or replica_index < 0:
        return None

    import socket

    from ..plan.io import (
        load_allocation_binding,
        load_deployment_plan,
        rank_for_node,
        resolve_engine_worker_receipt_requirement,
    )

    plan = load_deployment_plan(plan_path)
    binding = load_allocation_binding(binding_path)
    owner_rank = rank_for_node(binding, socket.gethostname())
    requirement_id = resolve_engine_worker_receipt_requirement(
        plan=plan,
        model_id=model_id,
        replica_index=replica_index,
        worker_rank=worker_rank,
        owner_rank=owner_rank,
    )
    requirement = next(
        item for item in plan.receipt_requirements if item.receipt_requirement_id == requirement_id
    )
    return {
        "role": role,
        "requirement_id": requirement_id,
        "component_id": requirement.component_slot,
        "owner_rank": owner_rank,
        "actor_id": actor_id,
    }


def _engine_receipt_identity(engine_kind: str | None) -> dict | None:
    """Resolve immutable plan/binding placement at most once per process.

    The watcher polls changing import postconditions. Plan, binding, actor and
    logical worker identity do not change inside one engine process, so
    reopening their artifacts on every 100 ms pass only amplifies storage.
    ``None`` is deliberately not cached because the runtime may not have bound
    the actor/worker identity yet.
    """

    cache_key = (
        engine_kind or "direct",
        str(os.getpid()),
        *(
            os.environ.get(key, "")
            for key in (
                "EXASERVE_DEPLOYMENT_ID",
                "EXASERVE_GENERATION",
                "EXASERVE_PLAN_HASH",
                "EXASERVE_ALLOCATION_BINDING_HASH",
                "EXASERVE_RECEIPT_REQUIREMENT_ID_ENGINE",
                "EXASERVE_RECEIPT_COMPONENT_ID_ENGINE",
                "EXASERVE_RECEIPT_RANK",
                ENGINE_MODEL_ENV,
                ENGINE_REPLICA_INDEX_ENV,
                ENGINE_WORKER_RANK_ENV,
                ENGINE_WORKER_KIND_ENV,
            )
        ),
    )
    with _ENGINE_IDENTITY_LOCK:
        cached = _ENGINE_IDENTITY_CACHE.get(cache_key)
        if cached is not None:
            return dict(cached)
    resolved = _resolve_engine_receipt_identity(engine_kind)
    if resolved is not None:
        with _ENGINE_IDENTITY_LOCK:
            existing = _ENGINE_IDENTITY_CACHE.setdefault(cache_key, dict(resolved))
        return dict(existing)
    return None


def _build_engine_receipt(*, patches_imported: bool, engine_kind: str | None = None):
    from .activator import _postcondition_sitecustomize
    from .producers import attest_self

    identity = _engine_receipt_identity(engine_kind)
    if identity is None:
        return None

    def _postcondition(patch_id: str):
        if patch_id == "EN-01":
            return True
        if not patches_imported:
            return None
        return _postcondition_sitecustomize(patch_id)

    import sys

    return attest_self(
        requirement_id=identity["requirement_id"],
        role=identity["role"],
        component_id=identity["component_id"],
        owner_scope="RANK",
        owner_rank=identity["owner_rank"],
        actor_id=identity["actor_id"],
        argv=list(sys.argv),
        postcondition=_postcondition,
    )


def _required_proof_failures(receipt) -> tuple[str, ...]:
    from .producers import required_patch_ids
    from .receipt_v2 import PatchStatus

    failures = []
    for patch_id in required_patch_ids(receipt.role):
        result = receipt.patch_results.get(patch_id)
        if result is None:
            failures.append(f"{patch_id}=MISSING")
        elif result.status != PatchStatus.APPLIED.value or not result.postcondition_passed:
            failures.append(
                f"{patch_id}={result.status}/postcondition={result.postcondition_passed}"
            )
    return tuple(failures)


def _required_proof_complete(receipt) -> bool:
    return not _required_proof_failures(receipt)


def _receipt_delivery_target(receipt) -> str:
    """Resolve the socket owned by the receipt's authenticated rank.

    A PP worker can inherit the coordinator actor's environment even though
    Ray schedules it on another allocation node.  The ambient socket is safe
    only when its ambient rank agrees with the receipt's independently
    resolved allocation rank.  Otherwise derive the rank-scoped node-local
    path and never submit the receipt through another rank's session.
    """
    from .local_ingress import SOCKET_ENV, socket_path_for

    owner_rank = receipt.owner_rank
    if isinstance(owner_rank, bool) or not isinstance(owner_rank, int) or owner_rank < 0:
        raise RuntimeError("engine receipt does not carry a valid rank owner")
    configured = os.environ.get(SOCKET_ENV, "")
    try:
        ambient_rank = int(os.environ.get("EXASERVE_RECEIPT_RANK", ""))
    except ValueError:
        ambient_rank = -1
    if configured and ambient_rank == owner_rank:
        return configured
    return socket_path_for(
        receipt.deployment_id,
        receipt.generation,
        owner_rank=owner_rank,
    )


def _write_and_deliver(receipt) -> tuple[str, bool]:
    from ..state.atomic import atomic_create_or_verify_json
    from .local_ingress import deliver_receipt_checked

    directory = os.environ[RECEIPT_DIR_ENV]
    # ``install`` creates this directory on the replica's node.  Ray PP
    # workers run on other nodes with node-local /tmp, so each producer must
    # materialize its own diagnostic directory before publication.
    _ensure_local_receipt_directory(directory)
    path = os.path.join(directory, f"engine_{os.getpid()}_{receipt.receipt_hash}.json")
    payload = receipt.to_dict()
    deliver_receipt_checked(payload, path=_receipt_delivery_target(receipt))
    # This file is the owning replica's local confirmation that the engine's
    # self-receipt crossed the authoritative hop.  Publishing it before the
    # ACK let a failed delivery masquerade as accepted evidence.
    atomic_create_or_verify_json(path, payload)
    return path, True


def _record_attestation_error(exc: Exception, attempts: int) -> None:
    """Publish one bounded diagnostic record and a centrally visible log.

    The receipt directory is node-local.  Ray forwards worker stderr to the
    deployment log, so the bounded stderr record keeps remote PP failures
    observable even when the worker never managed to reach its local ingress.
    Callers invoke this function only at exponentially spaced attempts and at
    the final deadline, preventing a persistent proof failure from flooding
    logs.
    """
    print(
        "[ExaServe EngineShim] attestation pending "
        f"after {attempts} attempt(s): {type(exc).__name__}: {str(exc)[:1024]}",
        file=sys.stderr,
        flush=True,
    )
    directory = os.environ.get(RECEIPT_DIR_ENV)
    if not directory:
        return
    try:
        from ..state.atomic import atomic_write_json

        _ensure_local_receipt_directory(directory)
        atomic_write_json(
            os.path.join(directory, f"engine_error_{os.getpid()}.json"),
            {
                "schema_version": 1,
                "pid": os.getpid(),
                "attempts": attempts,
                "error_type": type(exc).__name__,
                "error": str(exc)[:1024],
                "observed_at": time.time(),
            },
        )
    except OSError as diagnostic_exc:
        print(
            f"[ExaServe EngineShim] could not publish attestation error: {diagnostic_exc}",
            file=sys.stderr,
            flush=True,
        )


@dataclass(frozen=True)
class EngineAttestationWatcher:
    """Cancellable owner for the engine process's bounded attestation loop."""

    _stop_requested: threading.Event
    _thread: threading.Thread

    def stop(self, timeout_s: float = 5.0) -> bool:
        if not math.isfinite(timeout_s) or timeout_s < 0:
            raise ValueError("attestation stop timeout must be finite and non-negative")
        self._stop_requested.set()
        self._thread.join(timeout_s)
        return not self._thread.is_alive()


def start_engine_attestation(
    *,
    patches_imported: bool,
    engine_kind: str | None = None,
    timeout_s: float = 600.0,
    poll_s: float = 0.1,
) -> EngineAttestationWatcher:
    """Publish only after every required in-engine postcondition passes."""

    if not math.isfinite(timeout_s) or timeout_s <= 0:
        raise ValueError("attestation timeout must be finite and positive")
    if not math.isfinite(poll_s) or poll_s <= 0:
        raise ValueError("attestation poll interval must be finite and positive")
    stop_requested = threading.Event()

    def _watch() -> None:
        deadline = time.monotonic() + timeout_s
        attempts = 0
        identified_engine = False
        while not stop_requested.is_set() and time.monotonic() < deadline:
            try:
                receipt = _build_engine_receipt(
                    patches_imported=patches_imported, engine_kind=engine_kind
                )
                if receipt is not None:
                    identified_engine = True
                    failures = _required_proof_failures(receipt)
                    if not failures:
                        _, delivered = _write_and_deliver(receipt)
                        if delivered:
                            return
                    else:
                        attempts += 1
                        if attempts & (attempts - 1) == 0:
                            _record_attestation_error(
                                RuntimeError(
                                    "engine compatibility proof incomplete: " + ", ".join(failures)
                                ),
                                attempts,
                            )
            except Exception as exc:
                # This is an engine-startup hook.  Missing proof must withhold
                # the receipt and block READY, not crash an otherwise
                # diagnosable engine before its imports settle.
                attempts += 1
                if attempts & (attempts - 1) == 0:
                    _record_attestation_error(exc, attempts)
            stop_requested.wait(poll_s)
        # sitecustomize is inherited by multiprocessing helpers such as the
        # resource tracker.  They never own a planned engine slot, so their
        # bounded classifier must end silently instead of publishing a false
        # engine failure.  An identified engine still records exact proof or
        # delivery failure, while a missing receipt remains a readiness blocker.
        if not stop_requested.is_set() and identified_engine:
            _record_attestation_error(
                RuntimeError("engine attestation deadline expired without authoritative delivery"),
                attempts,
            )

    thread = threading.Thread(
        target=_watch,
        name="exaserve-engine-attestation",
        daemon=True,
    )
    thread.start()
    return EngineAttestationWatcher(stop_requested, thread)


def latest_error(receipt_dir: str) -> dict | None:
    """Return the newest bounded engine-attestation diagnostic, if valid."""
    from ..state.atomic import strict_json_load_path

    newest: tuple[float, dict] | None = None
    for path in glob.glob(os.path.join(receipt_dir, "engine_error_[0-9]*.json")):
        try:
            payload = strict_json_load_path(path)
            if (
                not isinstance(payload, dict)
                or payload.get("schema_version") != 1
                or not isinstance(payload.get("error_type"), str)
                or not isinstance(payload.get("error"), str)
                or not isinstance(payload.get("observed_at"), (int, float))
            ):
                continue
            observed_at = float(payload["observed_at"])
            if not math.isfinite(observed_at):
                continue
        except (OSError, ValueError, TypeError):
            continue
        if newest is None or observed_at > newest[0]:
            newest = (observed_at, payload)
    return None if newest is None else newest[1]


def write_engine_receipt(*, patches_imported: bool) -> Optional[str]:
    """Called INSIDE the engine process by the shim. Never raises."""
    directory = os.environ.get(RECEIPT_DIR_ENV)
    if not directory:
        return None
    try:
        receipt = _build_engine_receipt(patches_imported=patches_imported)
        if receipt is None:
            return None
        path, _ = _write_and_deliver(receipt)
        return path
    except Exception as exc:
        _record_attestation_error(exc, 1)
        return None


def collect(
    receipt_dir: str,
    *,
    requirement_id: str | None = None,
    component_id: str | None = None,
    timeout_s: float = 0.0,
    poll_s: float = 0.25,
    consume: bool = False,
) -> list[dict]:
    """Replica-side: read receipts written by engine processes.

    ``timeout_s`` bounds a short wait for the first receipt, because the engine
    writes it during startup. Absence leaves the exact engine slot missing;
    no owner assertion is substituted.
    """
    from ..state.atomic import strict_json_load_path

    deadline = time.monotonic() + max(0.0, timeout_s)
    warned: set[str] = set()
    while True:
        found: list[dict] = []
        accepted_paths: list[str] = []
        for path in sorted(glob.glob(os.path.join(receipt_dir, "engine_[0-9]*.json"))):
            try:
                data = strict_json_load_path(path)
            except (OSError, ValueError) as exc:
                if path not in warned:
                    warned.add(path)
                    print(
                        f"[ExaServe EngineShim] ignored invalid receipt {path}: "
                        f"{type(exc).__name__}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
                continue
            if isinstance(data, dict):
                try:
                    from .receipt_v2 import receipt_from_dict

                    receipt = receipt_from_dict(data)
                    identity_matches = (
                        requirement_id is None or receipt.receipt_requirement_id == requirement_id
                    ) and (component_id is None or receipt.component_id == component_id)
                    if identity_matches and receipt.receipt_hash == receipt.compute_hash():
                        found.append(data)
                        accepted_paths.append(path)
                except (KeyError, TypeError, ValueError) as exc:
                    if path not in warned:
                        warned.add(path)
                        print(
                            f"[ExaServe EngineShim] ignored invalid receipt {path}: "
                            f"{type(exc).__name__}: {exc}",
                            file=sys.stderr,
                            flush=True,
                        )
                    continue
        if found and consume:
            for path in accepted_paths:
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    print(
                        f"[ExaServe EngineShim] could not consume receipt {path}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
            try:
                os.rmdir(receipt_dir)
            except FileNotFoundError:
                pass
            except OSError as exc:
                # Other diagnostics or an in-flight atomic temporary may still
                # belong to this exact engine; the generation-scoped directory
                # remains bounded and is never accepted by another generation.
                if exc.errno not in {errno.ENOTEMPTY, errno.EEXIST}:
                    print(
                        f"[ExaServe EngineShim] could not remove receipt directory "
                        f"{receipt_dir}: {exc}",
                        file=sys.stderr,
                        flush=True,
                    )
        if found or time.monotonic() >= deadline:
            return found
        time.sleep(poll_s)


def receipt_dir_for(deployment_id: str, generation: int, component_id: str = "shared") -> str:
    """Return a path isolated by generation and exact logical engine slot.

    Hashing caller-controlled identities prevents path traversal and keeps Unix
    socket/filesystem component lengths bounded. A prior implementation used
    one deployment-wide directory, making every replica scan sibling receipts.
    """
    if isinstance(generation, bool) or not isinstance(generation, int) or generation < 0:
        raise ValueError("generation must be a non-negative integer")
    if not isinstance(deployment_id, str) or not deployment_id:
        raise ValueError("deployment_id must be a non-empty string")
    if not isinstance(component_id, str) or not component_id:
        raise ValueError("component_id must be a non-empty string")
    identity = f"{deployment_id}\0{generation}\0{component_id}".encode()
    suffix = hashlib.sha256(identity).hexdigest()[:24]
    state_root = os.environ.get("EXASERVE_LOCAL_STATE_ROOT", "")
    if state_root:
        return os.path.join(state_root, "engine_receipts", suffix)
    return os.path.join("/tmp", f"exaserve_engine_receipt_{suffix}")
