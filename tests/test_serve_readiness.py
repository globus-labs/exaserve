"""IMP-B02/B04: the readiness AUTHORITY, exercised without a Ray cluster.

These tests pin the properties the stdout marker never had: readiness is
revocable, a canary failure blocks it, receipts are required per role, and a
patch that was never requested does not fail a correct configuration.
"""

from __future__ import annotations

import json
import time

import pytest

from exaserve.compat.activator import CompatibilityActivator, _postcondition_sitecustomize
from exaserve.compat.profile import default_profile
from exaserve.control import serve_readiness as sr


# -- compatibility semantics ------------------------------------------------


def test_gated_off_patch_is_not_required(monkeypatch):
    monkeypatch.delenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", raising=False)
    profile = default_profile()
    assert profile.required_patch_ids("replica") == ()
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    assert "SC-01" in profile.required_patch_ids("replica")


def test_postcondition_is_not_applicable_when_target_absent(monkeypatch):
    """A module that was never imported yields None, not a false 'applied'."""
    import sys

    monkeypatch.delitem(sys.modules, "vllm.config.vllm", raising=False)
    assert _postcondition_sitecustomize("SC-01") is None


def test_required_patch_cannot_be_downgraded_to_not_applicable(monkeypatch):
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    activator = CompatibilityActivator(deployment_id="d1", generation=7)
    from exaserve.compat.activator import ActivationError

    with pytest.raises(ActivationError, match="no in-process post-condition"):
        activator.activate(
            "replica",
            apply_fn=lambda: None,
            verify_environment=False,
            postcondition=lambda pid: None,
        )


def test_a_half_patched_process_is_fatal(monkeypatch):
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    activator = CompatibilityActivator(deployment_id="d1", generation=7)
    from exaserve.compat.activator import ActivationError

    with pytest.raises(ActivationError, match="did not take effect"):
        activator.activate(
            "replica",
            apply_fn=lambda: None,
            verify_environment=False,
            postcondition=lambda pid: False,
        )


# -- canary + snapshot ------------------------------------------------------


def test_http_canary_rejects_a_response_without_a_completion(monkeypatch):
    class _Resp:
        def read(self):
            return json.dumps({"choices": []}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(
        sr.urllib.request,
        "build_opener",
        lambda *a: type("O", (), {"open": lambda s, *a, **k: _Resp()})(),
    )
    ok, detail = sr.http_canary("http://x/v1/completions")
    assert not ok and "no completion" in detail


def test_health_endpoint_alone_cannot_satisfy_the_canary(monkeypatch):
    class _Resp:
        def read(self):
            return json.dumps({"status": "healthy"}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(
        sr.urllib.request,
        "build_opener",
        lambda *a: type("O", (), {"open": lambda s, *a, **k: _Resp()})(),
    )
    ok, _ = sr.http_canary("http://x/health")
    assert not ok


def test_the_observation_producer_has_no_shared_file_writer():
    assert not hasattr(sr, "write_snapshot")
    assert not hasattr(sr, "write_evidence")
    assert "deployment_evidence.json" not in open(sr.__file__, encoding="utf-8").read()


def test_event_observer_stop_is_idempotent_and_retains_the_first_failure():
    import threading

    class BrokenClient:
        calls = 0

        def stop(self):
            self.calls += 1
            raise RuntimeError("client teardown broke")

    observer = object.__new__(sr.ServeEventObserver)
    observer._lock = threading.RLock()
    observer._stop_lock = threading.Lock()
    observer._stop_result = None
    observer._stop_requested = False
    observer._failure = None
    observer._client = BrokenClient()
    observer._loop = None
    observer._thread = None

    assert observer.stop() is False
    assert observer.stop() is False
    assert observer._failure == "long-poll client stop failed: RuntimeError: client teardown broke"
    assert observer._client is None


def test_sentinel_survives_a_staticmethod_wrapper(monkeypatch):
    """SC-11 marks a FUNCTION that the class stores as `staticmethod(fn)`.

    Reading `vars(cls)` raw finds the staticmethod object, whose attribute
    lookup does not forward to the wrapped function — which reported a
    correctly patched replica as half-patched and blocked readiness on a
    healthy cluster.
    """
    import sys
    import types

    module = types.ModuleType("ray._private.accelerators.intel_gpu")

    def visible_ids():
        return []

    visible_ids._exaserve_generic_selector_patch = True

    class Manager:
        pass

    Manager.get_current_process_visible_accelerator_ids = staticmethod(visible_ids)
    module.IntelGPUAcceleratorManager = Manager
    monkeypatch.setitem(sys.modules, "ray._private.accelerators.intel_gpu", module)
    assert _postcondition_sitecustomize("SC-11") is True

    class Unpatched:
        pass

    module.IntelGPUAcceleratorManager = Unpatched
    assert _postcondition_sitecustomize("SC-11") is False


def test_activator_normalizes_only_scheduler_fallback_not_canonical_id(monkeypatch):
    """The compiled deployment identity is exact; raw PBS fallback is scoped."""
    from exaserve.compat.activator import CompatibilityActivator

    for var in ("EXASERVE_DEPLOYMENT_ID", "EXASERVE_SCALING_TRACE_TOKEN", "EXASERVE_JOBID"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("PBS_JOBID", "8736431.aurora-pbs-0001.hostmgmt.example")
    assert CompatibilityActivator().deployment_id == "8736431"

    canonical = "fq-final37-supervisor-watchdog-2n-20260809.with-semantic-suffix"
    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID", canonical)
    assert CompatibilityActivator().deployment_id == canonical


def test_discovery_is_bootstrap_only_and_never_polls_status(monkeypatch):
    """Repeated reads use local bootstrap state, never ``serve.status()``."""
    calls = {"details": 0}

    def _details():
        calls["details"] += 1
        return {
            "applications": {
                "app": {
                    "route_prefix": "/",
                    "status": "RUNNING",
                    "deployments": {
                        "d": {"target_num_replicas": 4, "replicas": [{"state": "RUNNING"}] * 4}
                    },
                }
            }
        }

    monkeypatch.setattr(sr, "_serve_details", _details)
    sr.reset_discovery_cache()
    first = sr.discover_applications(use_cache=False)
    assert first["app"]["target"] == 4 and calls["details"] == 1

    second = sr.discover_applications()
    assert calls["details"] == 1  # no second expensive call
    assert second["app"]["target"] == 4  # target remembered
    assert second["app"]["running"] == 4  # bootstrap state is not re-polled


def test_event_stream_replica_or_route_loss_revokes_readiness():
    import threading
    from types import SimpleNamespace

    key = SimpleNamespace(app_name="app")
    # SimpleNamespace is unhashable, while the real DeploymentID is frozen.
    key = ("app", "d")
    observer = object.__new__(sr.ServeEventObserver)
    observer.identity = {
        "deployment_id": "d1",
        "generation": 1,
        "deployment_plan_hash": "a" * 64,
        "site_profile_hash": "b" * 64,
        "allocation_binding_hash": "c" * 64,
    }
    observer._lock = threading.RLock()
    observer._apps = {"app": {"route_prefix": "/"}}
    observer._deployments = {key: ("app", 2)}
    observer._running = {key: 2}
    observer._available = {key: True}
    observer._route_apps = {"app"}
    observer._nodes = []
    observer._event_count = 0
    observer._failure = None
    observer._client = None
    observer._stop_requested = False
    assert sr.applications_at_target(observer.snapshot())

    observer._deployment_update(
        key, SimpleNamespace(running_replicas=[object()], is_available=True)
    )
    assert not sr.applications_at_target(observer.snapshot())
    observer._route_update({})
    assert observer.snapshot()["applications"]["app"]["status"] == "MISSING_ROUTE"


def test_route_less_proxy_anchor_is_running_without_a_route_table_entry():
    import threading

    key = ("_exaserve_proxy_anchor_r1", "ProxyAnchor-rank-1")
    observer = object.__new__(sr.ServeEventObserver)
    observer.identity = {
        "deployment_id": "d1",
        "generation": 1,
        "deployment_plan_hash": "a" * 64,
        "site_profile_hash": "b" * 64,
        "allocation_binding_hash": "c" * 64,
    }
    observer._lock = threading.RLock()
    observer._apps = {"_exaserve_proxy_anchor_r1": {"route_prefix": None}}
    observer._deployments = {key: ("_exaserve_proxy_anchor_r1", 1)}
    observer._running = {key: 1}
    observer._available = {key: True}
    observer._route_apps = set()
    observer._nodes = []
    observer._event_count = 0
    observer._failure = None
    observer._client = None
    observer._stop_requested = False

    snapshot = observer.snapshot()
    anchor = snapshot["applications"]["_exaserve_proxy_anchor_r1"]
    assert anchor == {
        "running": 1,
        "target": 1,
        "route_prefix": None,
        "status": "RUNNING",
    }
    assert sr.applications_at_target(snapshot)


def test_snapshot_reconciliation_visits_deployments_once_at_high_cardinality():
    import threading

    class CountingDict(dict):
        item_iterations = 0

        def items(self):
            self.item_iterations += 1
            return super().items()

    size = 2048
    observer = object.__new__(sr.ServeEventObserver)
    observer.identity = {
        "deployment_id": "d1",
        "generation": 1,
        "deployment_plan_hash": "a" * 64,
        "site_profile_hash": "b" * 64,
        "allocation_binding_hash": "c" * 64,
    }
    observer._lock = threading.RLock()
    observer._apps = {f"app-{index}": {"route_prefix": None} for index in range(size)}
    observer._deployments = CountingDict(
        {(index, "deployment"): (f"app-{index}", 1) for index in range(size)}
    )
    observer._running = {key: 1 for key in observer._deployments}
    observer._available = {key: True for key in observer._deployments}
    observer._route_apps = set()
    observer._nodes = []
    observer._failure = None
    observer._client = None
    observer._stop_requested = False
    started = time.perf_counter()
    snapshot = observer.snapshot()
    assert time.perf_counter() - started < 1.0
    assert len(snapshot["applications"]) == size
    assert observer._deployments.item_iterations == 1


def test_unplanned_route_event_fails_the_observer_closed():
    import threading

    observer = object.__new__(sr.ServeEventObserver)
    observer._lock = threading.RLock()
    observer._apps = {"planned": {"route_prefix": "/"}}
    observer._route_apps = {"planned"}
    observer._failure = None
    observer._event_count = 0
    route_key = type("RouteKey", (), {"app_name": "unplanned"})()
    observer._route_update({route_key: object()})
    assert "unplanned Serve route event" in observer._failure


def _stub_cluster(monkeypatch, *, running: int, target: int = 2, canary_ok: bool):
    """Minimal stand-in for a live Ray/Serve cluster."""
    import sys
    import types

    ray = types.ModuleType("ray")
    ray.nodes = lambda: [{"NodeID": "n0", "Alive": True}]
    ray.serve = type(
        "S",
        (),
        {
            "status": staticmethod(
                lambda: {
                    "proxies": {"n0": "HEALTHY"},
                    "applications": {
                        "app": {
                            "status": "RUNNING",
                            "deployments": {"d": {"replica_states": {"RUNNING": running}}},
                        }
                    },
                }
            )
        },
    )
    monkeypatch.setitem(sys.modules, "ray", ray)
    monkeypatch.setitem(sys.modules, "ray.serve", ray.serve)
    sr.reset_discovery_cache()
    monkeypatch.setattr(
        sr,
        "_serve_details",
        lambda: {
            "applications": {
                "app": {
                    "route_prefix": "/",
                    "status": "RUNNING",
                    "deployments": {
                        "d": {
                            "target_num_replicas": target,
                            "replicas": [{"state": "RUNNING"}] * running,
                        }
                    },
                }
            }
        },
    )
    monkeypatch.setattr(
        sr, "http_canary", lambda *a, **k: (canary_ok, "" if canary_ok else "refused")
    )

    class _Store:
        profile = default_profile()

        def add(self, *_):
            return True, "ok"

        def satisfied(self):
            return True, "all required roles attested"

        def count(self):
            return 1

        def externally_attested_roles(self):
            return []

    # These exercise the readiness predicate, not the private-API surface;
    # without a real Ray the surface check would fail first and mask them.
    import exaserve.compat.private_api as _pa

    monkeypatch.setattr(_pa, "verify", lambda strict=True: {"stubbed": True})

    class _Observer:
        def snapshot(self):
            return {
                "payload_version": 2,
                "kind": "SERVE_APPLICATION_SNAPSHOT",
                "deployment_id": "d1",
                "generation": 1,
                "deployment_plan_hash": "a" * 64,
                "site_profile_hash": "b" * 64,
                "allocation_binding_hash": "c" * 64,
                "applications": {
                    "app": {
                        "running": running,
                        "target": target,
                        "route_prefix": "/",
                        "status": "RUNNING",
                    }
                },
                "nodes": [],
                "proxies": [],
                "observed_at": 1.0,
            }

    return _Observer()


def _identity_kwargs(published):
    return {
        "deployment_id": "d1",
        "generation": 1,
        "deployment_plan_hash": "a" * 64,
        "site_profile_hash": "b" * 64,
        "allocation_binding_hash": "c" * 64,
        "publish": lambda payload: published.append(payload) or True,
    }


def test_the_child_publishes_evidence_and_decides_nothing(monkeypatch):
    """WP13/IMP-B02: this process is a witness, not the readiness authority.

    It could see neither the gateway nor the other ranks' sessions, so the
    in-child gate decided READY against an endpoint no client uses. The
    replacement reports what this process genuinely observes and returns it;
    `PlanReadiness` in the composition root decides.
    """
    observer = _stub_cluster(monkeypatch, running=2, target=2, canary_ok=True)
    published = []
    evidence = sr.observe_deployment(
        **_identity_kwargs(published),
        timeout_s=5.0,
        observer=observer,
        poll_s=0.01,
        log=lambda *_: None,
    )
    assert sr.applications_at_target(evidence)
    assert "ready" not in evidence  # not this process's word to say
    assert published == [evidence]
    assert evidence["kind"] == "SERVE_APPLICATION_SNAPSHOT"


def test_a_shortfall_times_out_after_publishing_nonready_evidence(monkeypatch):
    observer = _stub_cluster(monkeypatch, running=1, target=2, canary_ok=True)
    published = []
    with pytest.raises(TimeoutError, match="without every application at target"):
        sr.observe_deployment(
            **_identity_kwargs(published),
            timeout_s=0.02,
            observer=observer,
            poll_s=0.01,
            log=lambda *_: None,
        )
    evidence = published[-1]
    assert not sr.applications_at_target(evidence)
    app = next(iter(evidence["applications"].values()))
    assert (app["running"], app["target"]) == (1, 2)


def test_publishing_evidence_does_not_create_a_shared_file(monkeypatch, tmp_path):
    observer = _stub_cluster(monkeypatch, running=2, target=2, canary_ok=True)
    published = []
    sr.observe_deployment(
        **_identity_kwargs(published),
        timeout_s=5.0,
        observer=observer,
        poll_s=0.01,
        log=lambda *_: None,
    )
    assert list(tmp_path.iterdir()) == []


def test_ipc_rejection_is_fatal_not_a_file_fallback(monkeypatch):
    observer = _stub_cluster(monkeypatch, running=2, target=2, canary_ok=True)
    kwargs = _identity_kwargs([])
    kwargs["publish"] = lambda _payload: False
    with pytest.raises(RuntimeError, match="IPC rejected"):
        sr.observe_deployment(
            **kwargs, timeout_s=5.0, observer=observer, poll_s=0.01, log=lambda *_: None
        )


def test_there_is_no_degraded_readiness_escape_hatch():
    """The deleted gate honoured EXASERVE_ALLOW_DEGRADED_READINESS.

    An env var that turns a fail-closed predicate into a warning is the thing
    the audit objected to; nothing on the decision path reads it now.
    """
    import inspect

    from exaserve.control import plan_readiness

    for module in (sr, plan_readiness):
        assert "ALLOW_DEGRADED_READINESS" not in inspect.getsource(module)
