"""EN-01: spawned EngineCore produces an exact schema-v2 self receipt."""

from __future__ import annotations

import json
import os
import socket
import sys
import time

import pytest

from exaserve.compat import engine_shim
from exaserve.compat.profile import (
    MULTIPROC_WORKER_PATCH_GATE,
    PP_PATCH_GATE,
    RAY_WORKER_PATCH_GATE,
)


@pytest.fixture(autouse=True)
def _restore_shim_environment(tmp_path, monkeypatch):
    snapshot = engine_shim.environment_snapshot()
    # Engine processes now require the distribution transaction's node-local
    # contract.  pytest's tmp_path models one node-local filesystem here.
    monkeypatch.setenv("EXASERVE_LOCAL_RUNTIME_ROOT", str(tmp_path))
    monkeypatch.setenv("EXASERVE_LOCAL_STATE_ROOT", str(tmp_path))
    monkeypatch.setenv("EXASERVE_SHARED_ROOTS", "/home:/lus/flare")
    monkeypatch.setenv("EXASERVE_SITE_PROFILE_HASH", "2" * 64)
    monkeypatch.setenv("EXASERVE_QUALIFIED_PYTHON", sys.executable)
    from exaserve.compat.producers import file_hash

    monkeypatch.setenv("EXASERVE_QUALIFIED_PYTHON_SHA256", file_hash(sys.executable))
    monkeypatch.setenv("EXASERVE_QUALIFIED_PYTHON_SITE_PROFILE_HASH", "2" * 64)
    monkeypatch.setenv("EXASERVE_COMPAT_PROFILE_ID", "a" * 64)
    monkeypatch.setenv("EXASERVE_COMPAT_MANIFEST_HASH", "b" * 64)
    monkeypatch.setenv("EXASERVE_COMPAT_SOURCES_NODE_PROFILE", "a" * 64)
    monkeypatch.setenv("EXASERVE_COMPAT_SOURCES_NODE_MANIFEST", "b" * 64)
    monkeypatch.setenv("PYTHONNOUSERSITE", "1")
    monkeypatch.setenv("PYTHONSAFEPATH", "1")
    monkeypatch.setenv("PYTHONPATH", str(tmp_path))
    for key in (
        "HOME",
        "TMPDIR",
        "TMP",
        "TEMP",
        "XDG_CACHE_HOME",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_RUNTIME_DIR",
        "IPYTHONDIR",
        "JUPYTER_CONFIG_DIR",
        "NUMBA_CACHE_DIR",
        "TORCH_EXTENSIONS_DIR",
        "MPLCONFIGDIR",
        "HF_HOME",
        "HF_HUB_CACHE",
        "HUGGINGFACE_HUB_CACHE",
        "TRANSFORMERS_CACHE",
        "TORCH_HOME",
        "TRITON_CACHE_DIR",
        "VLLM_CACHE_ROOT",
        "RAY_TMPDIR",
    ):
        monkeypatch.setenv(key, str(tmp_path))
    yield
    engine_shim.restore_environment(snapshot)


def _identity(monkeypatch) -> None:
    monkeypatch.setenv("EXASERVE_PLAN_HASH", "1" * 64)
    monkeypatch.setenv("EXASERVE_SITE_PROFILE_HASH", "2" * 64)
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_HASH", "3" * 64)
    monkeypatch.setenv("EXASERVE_RECEIPT_REQUIREMENT_ID_ENGINE", "engine/slot0/core")
    monkeypatch.setenv("EXASERVE_RECEIPT_COMPONENT_ID_ENGINE", "engine-slot0/core")
    monkeypatch.setenv("EXASERVE_RECEIPT_RANK", "0")


def _start_receipt_ingress(tmp_path, monkeypatch):
    from exaserve.compat.local_ingress import LocalReceiptIngress, socket_path_for

    path = socket_path_for("engine-attestation-test", 0, root=str(tmp_path))
    ingress = LocalReceiptIngress(path)
    assert ingress.start()
    monkeypatch.setenv("EXASERVE_RECEIPT_SOCKET", path)
    return ingress


def _stage_bootstrap(tmp_path, monkeypatch):
    """Model the compatibility root source staging publishes on every rank."""
    from exaserve.compat.generated_overlay import ROOT_ENV

    root = tmp_path / "compat-root"
    root.mkdir(exist_ok=True)
    (root / "sitecustomize.py").write_text(engine_shim.shim_source())
    existing = os.environ.get("PYTHONPATH", "")
    monkeypatch.setenv(
        "PYTHONPATH",
        str(root) + (os.pathsep + existing if existing else ""),
    )
    monkeypatch.setenv(ROOT_ENV, str(root))
    return root


def test_prepare_uses_the_staged_cross_node_bootstrap(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/pre/existing")
    bootstrap_root = _stage_bootstrap(tmp_path, monkeypatch)
    receipts = tmp_path / "receipts"
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=True,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
        versions={"ray": "2.53.0"},
    )
    source = (bootstrap_root / "sitecustomize.py").read_text()
    assert "verify_engine_bootstrap" in source
    assert "import exaserve._sitecustomize" not in source
    assert "start_engine_attestation" in source
    assert os.environ["PYTHONPATH"].split(os.pathsep)[0] == str(bootstrap_root)
    assert "/pre/existing" in os.environ["PYTHONPATH"]
    assert os.environ["EXASERVE_ENGINE_SHIM_PATCHES"] == "1"


def test_prepare_never_creates_receipts_through_intermediate_symlink(tmp_path, monkeypatch):
    _stage_bootstrap(tmp_path, monkeypatch)
    state = tmp_path / "state"
    shared = tmp_path / "shared-like"
    state.mkdir()
    shared.mkdir()
    (state / "escape").symlink_to(shared, target_is_directory=True)
    monkeypatch.setenv("EXASERVE_LOCAL_STATE_ROOT", str(state))

    with pytest.raises((OSError, RuntimeError), match="shared|escape|symlink"):
        engine_shim.prepare_environment(
            str(state / "escape" / "receipts"),
            import_patches=True,
            deployment_id="d1",
            generation=7,
            vendor="xpu",
        )

    assert not (shared / "receipts").exists()


def test_prepare_rejects_a_tampered_staged_bootstrap(tmp_path, monkeypatch):
    bootstrap_root = _stage_bootstrap(tmp_path, monkeypatch)
    (bootstrap_root / "sitecustomize.py").write_text("# drifted\n")
    with pytest.raises(RuntimeError, match="does not match EN-01"):
        engine_shim.prepare_environment(
            str(tmp_path / "receipts"),
            import_patches=True,
            deployment_id="d1",
            generation=7,
            vendor="xpu",
        )


def test_engine_bootstrap_installs_overlay_without_eager_target_import(monkeypatch):
    from types import SimpleNamespace

    from exaserve.compat import activator, generated_overlay, producers, profile as profile_module
    from exaserve.plan import runtime_environment

    selected = SimpleNamespace(profile_id="a" * 64)
    observed = {}

    class StubActivator:
        def __init__(self, *, profile):
            observed["activator_profile"] = profile

        def activate(self, role, *, apply_fn):
            observed["activation_role"] = role
            apply_fn()

    def install(profile, root, *, role):
        observed["install"] = (profile, root, role)

    monkeypatch.setattr(profile_module, "default_profile", lambda vendor: selected)
    monkeypatch.setattr(producers, "manifest_hash", lambda profile: "b" * 64)
    monkeypatch.setattr(activator, "CompatibilityActivator", StubActivator)
    monkeypatch.setattr(generated_overlay, "install", install)
    monkeypatch.setattr(runtime_environment, "require_contained_local_path", lambda *a, **k: "")
    monkeypatch.setattr(
        generated_overlay,
        "activate_patch_ids",
        lambda *args, **kwargs: pytest.fail("engine bootstrap imported patch targets eagerly"),
    )
    monkeypatch.setenv("EXASERVE_VENDOR", "xpu")
    monkeypatch.setenv("EXASERVE_COMPAT_PROFILE_ID", selected.profile_id)
    monkeypatch.setenv("EXASERVE_COMPAT_MANIFEST_HASH", "b" * 64)
    monkeypatch.setenv(generated_overlay.ROOT_ENV, "/verified/overlay")

    engine_shim.verify_engine_bootstrap("vllm")

    assert observed == {
        "activator_profile": selected,
        "activation_role": "engine_bootstrap",
        "install": (selected, "/verified/overlay", "engine_bootstrap"),
    }


def test_parent_environment_can_be_restored_after_the_engine_spawn(tmp_path, monkeypatch):
    monkeypatch.setenv("PYTHONPATH", "/before")
    _stage_bootstrap(tmp_path, monkeypatch)
    snapshot = engine_shim.environment_snapshot()
    engine_shim.prepare_environment(
        str(tmp_path / "receipts"),
        import_patches=False,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
        versions={"ray": "2.53.0"},
    )
    engine_shim.restore_environment(snapshot)
    assert os.environ["PYTHONPATH"] == snapshot["PYTHONPATH"]
    assert "EXASERVE_ENGINE_RECEIPT_DIR" not in os.environ
    assert "EXASERVE_VERSION_RAY" not in os.environ


def test_generated_watcher_rejects_a_non_engine_multiprocessing_helper(monkeypatch):
    import multiprocessing

    monkeypatch.delitem(sys.modules, "vllm.v1.engine.core", raising=False)
    assert engine_shim._engine_process_ready("vllm") is False
    fake = type(sys)("vllm.v1.engine.core")
    monkeypatch.setitem(sys.modules, "vllm.v1.engine.core", fake)
    assert engine_shim._engine_process_ready("vllm") is False
    monkeypatch.setattr(multiprocessing.current_process(), "name", "EngineCore_DP0")
    assert engine_shim._engine_process_ready("vllm") is True


def test_multiproc_bootstrap_exports_exact_rank_only_during_spawn(monkeypatch):
    pytest.importorskip("vllm", reason="optional backend plugin is absent from clean wheel gate")
    from exaserve import _sitecustomize
    from vllm.v1.executor import multiproc_executor

    observed = {}

    def original(*args, **kwargs):
        observed["kwargs"] = kwargs
        observed["kind"] = os.environ.get(engine_shim.ENGINE_WORKER_KIND_ENV)
        observed["rank"] = os.environ.get(engine_shim.ENGINE_WORKER_RANK_ENV)
        return "done"

    worker_cls = multiproc_executor.WorkerProc
    original_worker_main = worker_cls.worker_main
    monkeypatch.setattr(worker_cls, "make_worker_process", staticmethod(original))
    monkeypatch.setenv(engine_shim.ENGINE_WORKER_KIND_ENV, "parent-kind")
    monkeypatch.delenv(engine_shim.ENGINE_WORKER_RANK_ENV, raising=False)

    _sitecustomize._patch_vllm_multiproc_worker_identity()
    assert worker_cls.make_worker_process(rank=3, local_rank=1) == "done"
    assert worker_cls.worker_main is original_worker_main
    assert observed == {
        "kwargs": {"rank": 3, "local_rank": 1},
        "kind": "multiproc",
        "rank": "3",
    }
    assert os.environ[engine_shim.ENGINE_WORKER_KIND_ENV] == "parent-kind"
    assert engine_shim.ENGINE_WORKER_RANK_ENV not in os.environ


def test_worker_receipt_identity_uses_logical_rank_not_physical_gpu(tmp_path, monkeypatch):
    from exaserve.plan.compiler import compile_deployment_plan
    from exaserve.plan.contracts import build_allocation_binding
    from exaserve.plan.io import write_allocation_binding, write_deployment_plan
    from exaserve.site import default_site_profile

    plan = compile_deployment_plan(
        {
            "num_nodes": 1,
            "num_gpus_per_node": 12,
            "validation_mode": True,
            "models": [
                {
                    "model_id": "org/model",
                    "tensor_parallel_size": 2,
                    "max_model_len": 128,
                    "size": 8,
                }
            ],
        },
        site=default_site_profile(),
        deployment_id="d1",
    )
    binding = build_allocation_binding(
        plan=plan,
        generation=7,
        scheduler_allocation_id="job1",
        nodes=[socket.gethostname()],
    )
    plan_path = tmp_path / "deployment.plan.json"
    binding_path = tmp_path / "allocation_binding.json"
    write_deployment_plan(str(plan_path), plan)
    write_allocation_binding(str(binding_path), binding)
    monkeypatch.setenv("EXASERVE_PLAN_PATH", str(plan_path))
    monkeypatch.setenv("EXASERVE_ALLOCATION_BINDING_PATH", str(binding_path))
    monkeypatch.setenv(engine_shim.ENGINE_MODEL_ENV, "org/model")
    monkeypatch.setenv(engine_shim.ENGINE_REPLICA_INDEX_ENV, "0")
    monkeypatch.setenv(engine_shim.ENGINE_WORKER_KIND_ENV, "multiproc")
    monkeypatch.setenv(engine_shim.ENGINE_WORKER_RANK_ENV, "1")
    # These physical ids are deliberately reversed and neither value is the
    # logical plan's worker ordinal. They remain resource observations only.
    monkeypatch.setenv(engine_shim.ENGINE_DEVICE_IDS_ENV, "11,10")
    fake_runner = type(sys)("vllm.v1.worker.gpu_model_runner")
    monkeypatch.setitem(sys.modules, "vllm.v1.worker.gpu_model_runner", fake_runner)

    identity = engine_shim._engine_receipt_identity("vllm")

    assert identity is not None
    assert identity["requirement_id"] == ("model/org--model/replica/0/engine/worker/stage0/device1")
    assert identity["owner_rank"] == 0


def test_non_pp_engine_writes_exact_v2_receipt(tmp_path, monkeypatch):
    _identity(monkeypatch)
    ingress = _start_receipt_ingress(tmp_path, monkeypatch)
    for gate in (PP_PATCH_GATE, RAY_WORKER_PATCH_GATE, MULTIPROC_WORKER_PATCH_GATE):
        monkeypatch.delenv(gate, raising=False)
    receipts = tmp_path / "receipts"
    _stage_bootstrap(tmp_path, monkeypatch)
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=False,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
    )
    monkeypatch.setattr(
        "exaserve.compat.activator._postcondition_sitecustomize", lambda patch_id: True
    )
    try:
        path = engine_shim.write_engine_receipt(patches_imported=True)
    finally:
        ingress.stop()
    assert path is not None
    data = json.loads(open(path, encoding="utf-8").read())
    assert data["schema_version"] == 2
    assert data["role"] == "engine_core"
    assert data["attestation_type"] == "SELF"
    assert data["receipt_requirement_id"] == "engine/slot0/core"
    assert data["patch_results"]["EN-01"]["status"] == "APPLIED"
    gated = {key: value for key, value in data["patch_results"].items() if key != "EN-01"}
    assert all(value["status"] == "NOT_REQUIRED" for value in gated.values())


def test_engine_core_patch_requirements_follow_the_selected_executor(monkeypatch):
    from exaserve.compat.profile import default_profile

    profile = default_profile("xpu")
    for gate in (PP_PATCH_GATE, RAY_WORKER_PATCH_GATE, MULTIPROC_WORKER_PATCH_GATE):
        monkeypatch.setenv(gate, "0")
    assert profile.required_patch_ids("engine_core") == ("EN-01",)

    monkeypatch.setenv(RAY_WORKER_PATCH_GATE, "1")
    assert set(profile.required_patch_ids("engine_core")) == {"EN-01", "EW-01", "EW-03"}

    monkeypatch.setenv(RAY_WORKER_PATCH_GATE, "0")
    monkeypatch.setenv(MULTIPROC_WORKER_PATCH_GATE, "1")
    assert set(profile.required_patch_ids("engine_core")) == {"EN-01", "EW-02"}


def test_engine_delivery_uses_configured_socket_only_for_its_own_rank(monkeypatch):
    from types import SimpleNamespace

    from exaserve.compat.local_ingress import socket_path_for

    monkeypatch.setenv("EXASERVE_RECEIPT_SOCKET", "/tmp/coordinator-receipts.sock")
    monkeypatch.setenv("EXASERVE_RECEIPT_RANK", "0")
    coordinator = SimpleNamespace(deployment_id="d1", generation=7, owner_rank=0)
    remote_worker = SimpleNamespace(deployment_id="d1", generation=7, owner_rank=1)

    assert engine_shim._receipt_delivery_target(coordinator) == ("/tmp/coordinator-receipts.sock")
    assert engine_shim._receipt_delivery_target(remote_worker) == socket_path_for(
        "d1", 7, owner_rank=1
    )


def test_bound_serve_actor_rebinds_rank_local_receipt_socket(monkeypatch):
    from exaserve.actor_runtime import build_actor_runtime_env
    from exaserve.compat.local_ingress import socket_path_for

    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "four-node-test")
    monkeypatch.setenv("EXASERVE_GENERATION", "17")
    monkeypatch.setenv(
        "EXASERVE_RECEIPT_SOCKET",
        socket_path_for("four-node-test", 17, owner_rank=0),
    )

    env_vars = build_actor_runtime_env(receipt_owner_rank=2)["env_vars"]

    assert env_vars["EXASERVE_RECEIPT_RANK"] == "2"
    assert env_vars["EXASERVE_RECEIPT_SOCKET"] == socket_path_for(
        "four-node-test", 17, owner_rank=2
    )
    assert "EXASERVE_SHARED_ROOTS" not in env_vars


def test_native_serve_actor_does_not_inherit_the_head_receipt_socket(monkeypatch):
    from exaserve.actor_runtime import build_actor_runtime_env

    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "four-node-test")
    monkeypatch.setenv("EXASERVE_GENERATION", "17")
    monkeypatch.setenv("EXASERVE_RECEIPT_RANK", "0")
    monkeypatch.setenv("EXASERVE_RECEIPT_SOCKET", "/tmp/rank-zero.sock")

    env_vars = build_actor_runtime_env(dynamic_receipt_owner=True)["env_vars"]

    assert "EXASERVE_RECEIPT_RANK" not in env_vars
    assert "EXASERVE_RECEIPT_SOCKET" not in env_vars


def test_dynamic_and_declared_receipt_ownership_are_mutually_exclusive(monkeypatch):
    from exaserve.actor_runtime import build_actor_runtime_env

    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "four-node-test")
    monkeypatch.setenv("EXASERVE_GENERATION", "17")
    with pytest.raises(ValueError, match="cannot also declare"):
        build_actor_runtime_env(receipt_owner_rank=0, dynamic_receipt_owner=True)


@pytest.mark.parametrize(
    "field",
    [
        "EXASERVE_DEPLOYMENT_ID",
        "EXASERVE_GENERATION",
        "EXASERVE_PLAN_HASH",
        "EXASERVE_ALLOCATION_BINDING_HASH",
        "EXASERVE_COMPAT_ROLE",
        "EXASERVE_RECEIPT_RANK",
        "EXASERVE_RECEIPT_SOCKET",
        "EXASERVE_SHARED_ROOTS",
    ],
)
def test_actor_extras_cannot_override_canonical_identity(monkeypatch, field):
    from exaserve.actor_runtime import build_actor_runtime_env

    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "four-node-test")
    monkeypatch.setenv("EXASERVE_GENERATION", "17")
    with pytest.raises(ValueError, match="cannot override"):
        build_actor_runtime_env({field: "forged"}, receipt_owner_rank=1)


@pytest.mark.parametrize("extra", [{"OK": 1}, {"": "value"}, [("OK", "value")]])
def test_actor_extras_require_a_string_mapping(monkeypatch, extra):
    from exaserve.actor_runtime import build_actor_runtime_env

    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "four-node-test")
    monkeypatch.setenv("EXASERVE_GENERATION", "17")
    with pytest.raises(TypeError, match="map of non-empty strings"):
        build_actor_runtime_env(extra, receipt_owner_rank=1)


def test_actor_extra_cannot_hide_shared_path_under_nonstandard_name(monkeypatch):
    from exaserve.actor_runtime import build_actor_runtime_env

    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "four-node-test")
    monkeypatch.setenv("EXASERVE_GENERATION", "17")
    with pytest.raises(RuntimeError, match="ODD_SETTING.*shared storage"):
        build_actor_runtime_env(
            {"ODD_SETTING": "/home/user/hidden-model"},
            receipt_owner_rank=1,
        )


@pytest.mark.parametrize("owner_rank", [True, -1, "2"])
def test_bound_serve_actor_rejects_invalid_receipt_owner_rank(monkeypatch, owner_rank):
    from exaserve.actor_runtime import build_actor_runtime_env

    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", "four-node-test")
    monkeypatch.setenv("EXASERVE_GENERATION", "17")
    with pytest.raises(ValueError, match="receipt_owner_rank"):
        build_actor_runtime_env(receipt_owner_rank=owner_rank)


def test_engine_delivery_materializes_node_local_diagnostic_directory(tmp_path, monkeypatch):
    _identity(monkeypatch)
    ingress = _start_receipt_ingress(tmp_path, monkeypatch)
    receipts = tmp_path / "remote-node-receipts"
    _stage_bootstrap(tmp_path, monkeypatch)
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=False,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
    )
    receipts.rmdir()
    monkeypatch.setattr(
        "exaserve.compat.activator._postcondition_sitecustomize", lambda patch_id: True
    )
    try:
        path = engine_shim.write_engine_receipt(patches_imported=True)
    finally:
        ingress.stop()
    assert path is not None
    assert receipts.is_dir()
    assert os.stat(receipts).st_mode & 0o777 == 0o700


def test_pp_half_patch_is_failed_and_not_published_as_complete(tmp_path, monkeypatch):
    _identity(monkeypatch)
    ingress = _start_receipt_ingress(tmp_path, monkeypatch)
    monkeypatch.setenv(PP_PATCH_GATE, "1")
    monkeypatch.setenv(RAY_WORKER_PATCH_GATE, "1")
    monkeypatch.setenv(MULTIPROC_WORKER_PATCH_GATE, "0")
    receipts = tmp_path / "receipts"
    _stage_bootstrap(tmp_path, monkeypatch)
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=True,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
    )
    monkeypatch.setattr(
        "exaserve.compat.activator._postcondition_sitecustomize", lambda patch_id: False
    )
    try:
        path = engine_shim.write_engine_receipt(patches_imported=True)
    finally:
        ingress.stop()
    data = json.loads(open(path, encoding="utf-8").read())
    from exaserve.compat.profile import default_profile

    required = set(default_profile("xpu").required_patch_ids("engine_core")) - {"EN-01"}
    failed = [data["patch_results"][key] for key in required]
    assert failed and all(value["status"] == "FAILED" for value in failed)
    assert data["patch_results"]["EW-02"]["status"] == "NOT_REQUIRED"


def test_watcher_waits_until_postconditions_pass(tmp_path, monkeypatch):
    _identity(monkeypatch)
    ingress = _start_receipt_ingress(tmp_path, monkeypatch)
    monkeypatch.setenv(PP_PATCH_GATE, "1")
    monkeypatch.setenv(RAY_WORKER_PATCH_GATE, "1")
    monkeypatch.setenv(MULTIPROC_WORKER_PATCH_GATE, "0")
    receipts = tmp_path / "receipts"
    _stage_bootstrap(tmp_path, monkeypatch)
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=True,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
    )
    ready = {"value": False}
    monkeypatch.setattr(
        "exaserve.compat.activator._postcondition_sitecustomize", lambda patch_id: ready["value"]
    )
    watcher = engine_shim.start_engine_attestation(
        patches_imported=True, timeout_s=1.0, poll_s=0.01
    )
    time.sleep(0.05)
    assert engine_shim.collect(str(receipts)) == []
    ready["value"] = True
    deadline = time.monotonic() + 2
    try:
        while time.monotonic() < deadline and not engine_shim.collect(str(receipts)):
            time.sleep(0.02)
        assert len(engine_shim.collect(str(receipts))) == 1
        assert watcher.stop()
    finally:
        ingress.stop()


def test_failed_engine_delivery_cannot_publish_a_success_receipt(tmp_path, monkeypatch):
    _identity(monkeypatch)
    receipts = tmp_path / "receipts"
    _stage_bootstrap(tmp_path, monkeypatch)
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=False,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
    )
    from exaserve.compat.local_ingress import socket_path_for

    monkeypatch.setenv(
        "EXASERVE_RECEIPT_SOCKET",
        socket_path_for("dead-engine-ingress-test", 0, root=str(tmp_path)),
    )
    monkeypatch.setattr(
        "exaserve.compat.activator._postcondition_sitecustomize", lambda patch_id: True
    )
    assert engine_shim.write_engine_receipt(patches_imported=True) is None
    assert engine_shim.collect(str(receipts)) == []
    errors = list(receipts.glob("engine_error_*.json"))
    assert len(errors) == 1
    error = json.loads(errors[0].read_text())
    assert error["error_type"] == "LocalDeliveryError"
    assert "transport failed" in error["error"]


def test_watcher_records_and_logs_the_exact_incomplete_patch_set(tmp_path, monkeypatch, capsys):
    _identity(monkeypatch)
    monkeypatch.setenv(PP_PATCH_GATE, "0")
    monkeypatch.setenv(RAY_WORKER_PATCH_GATE, "1")
    monkeypatch.setenv(MULTIPROC_WORKER_PATCH_GATE, "0")
    receipts = tmp_path / "receipts"
    _stage_bootstrap(tmp_path, monkeypatch)
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=True,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
    )
    monkeypatch.setattr(
        "exaserve.compat.activator._postcondition_sitecustomize", lambda patch_id: None
    )
    watcher = engine_shim.start_engine_attestation(
        patches_imported=True, timeout_s=0.2, poll_s=0.01
    )
    try:
        deadline = time.monotonic() + 1
        diagnostic = None
        while time.monotonic() < deadline and diagnostic is None:
            diagnostic = engine_shim.latest_error(str(receipts))
            time.sleep(0.01)
        assert diagnostic is not None
        assert "EW-01=NOT_REQUIRED/postcondition=False" in diagnostic["error"]
        assert "EW-03=NOT_REQUIRED/postcondition=False" in diagnostic["error"]
        assert "[ExaServe EngineShim] attestation pending" in capsys.readouterr().err
    finally:
        assert watcher.stop()


def test_watcher_silently_expires_for_a_process_without_an_engine_slot(
    tmp_path, monkeypatch, capsys
):
    receipts = tmp_path / "receipts"
    receipts.mkdir()
    monkeypatch.setenv(engine_shim.RECEIPT_DIR_ENV, str(receipts))
    monkeypatch.setattr(engine_shim, "_build_engine_receipt", lambda **_kwargs: None)

    watcher = engine_shim.start_engine_attestation(
        patches_imported=True, timeout_s=0.05, poll_s=0.005
    )
    time.sleep(0.1)

    assert watcher.stop()
    assert engine_shim.latest_error(str(receipts)) is None
    assert "attestation pending" not in capsys.readouterr().err


@pytest.mark.parametrize(
    ("timeout_s", "poll_s"),
    [(0.0, 0.1), (float("nan"), 0.1), (1.0, 0.0), (1.0, float("inf"))],
)
def test_watcher_rejects_unbounded_timing(timeout_s, poll_s):
    with pytest.raises(ValueError, match="finite and positive"):
        engine_shim.start_engine_attestation(
            patches_imported=True, timeout_s=timeout_s, poll_s=poll_s
        )


def test_collect_ignores_malformed_receipts(tmp_path, monkeypatch):
    _identity(monkeypatch)
    ingress = _start_receipt_ingress(tmp_path, monkeypatch)
    receipts = tmp_path / "receipts"
    _stage_bootstrap(tmp_path, monkeypatch)
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=False,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
    )
    try:
        engine_shim.write_engine_receipt(patches_imported=False)
    finally:
        ingress.stop()
    (receipts / "engine_99999.json").write_text(json.dumps({"role": "engine_core"}))
    assert len(engine_shim.collect(str(receipts))) == 1


def test_collect_never_substitutes_a_sibling_engine_receipt(tmp_path, monkeypatch):
    _identity(monkeypatch)
    ingress = _start_receipt_ingress(tmp_path, monkeypatch)
    receipts = tmp_path / "receipts"
    _stage_bootstrap(tmp_path, monkeypatch)
    engine_shim.prepare_environment(
        str(receipts),
        import_patches=False,
        deployment_id="d1",
        generation=7,
        vendor="xpu",
    )
    try:
        engine_shim.write_engine_receipt(patches_imported=False)
    finally:
        ingress.stop()
    assert engine_shim.collect(str(receipts), requirement_id="engine/other") == []
    assert (
        len(
            engine_shim.collect(
                str(receipts),
                requirement_id="engine/slot0/core",
                component_id="engine-slot0/core",
                consume=True,
            )
        )
        == 1
    )
    assert engine_shim.collect(str(receipts)) == []


def test_collect_returns_empty_without_waiting_forever(tmp_path):
    start = time.monotonic()
    assert engine_shim.collect(str(tmp_path / "nothing"), timeout_s=0.3) == []
    assert time.monotonic() - start < 5.0


def test_receipt_dir_is_generation_scoped():
    assert engine_shim.receipt_dir_for("d1", 1) != engine_shim.receipt_dir_for("d1", 2)


def test_receipt_dir_is_component_scoped_and_path_safe():
    first = engine_shim.receipt_dir_for("../../hostile", 1, "engine/a")
    second = engine_shim.receipt_dir_for("../../hostile", 1, "engine/b")
    assert first != second
    assert first.startswith(os.environ["EXASERVE_LOCAL_STATE_ROOT"] + os.sep)
    assert ".." not in first and "hostile" not in first


def test_the_shim_never_raises_without_a_receipt_dir(monkeypatch):
    monkeypatch.delenv(engine_shim.RECEIPT_DIR_ENV, raising=False)
    assert engine_shim.write_engine_receipt(patches_imported=True) is None
