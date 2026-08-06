"""IMP-B02/B04: the readiness AUTHORITY, exercised without a Ray cluster.

These tests pin the properties the stdout marker never had: readiness is
revocable, a canary failure blocks it, receipts are required per role, and a
patch that was never requested does not fail a correct configuration.
"""

from __future__ import annotations

import json

import pytest

from exaserve.compat.activator import CompatibilityActivator, _postcondition_sitecustomize
from exaserve.compat.profile import default_profile
from exaserve.compat.receipt import ReceiptStore, build_receipt
from exaserve.control import serve_readiness as sr
from exaserve.control.readiness import ReadinessCoordinator


def _coord(**kw):
    plan = sr.build_plan(
        deployment_id="d1", generation=7, plan_hash="h",
        node_ids=kw.get("nodes", ["n0"]),
        expected_replicas=kw.get("replicas", {"app": 2}),
        routes=kw.get("routes", ["app"]))
    return ReadinessCoordinator(plan, receipts=kw.get("receipts"))


def _feed(coord, *, running: int, app_status="RUNNING", proxy_healthy=True,
          nodes=("n0",)):
    """Stand in for collect_observations without importing ray."""
    from exaserve.control.contracts import ComponentState

    for node in nodes:
        for cid in (f"node@{node}", f"proxy@{node}"):
            coord.observe(sr._observation(
                coord.plan, component_id=cid, instance_id="i", node_id=node,
                role="proxy",
                state=(ComponentState.RUNNING.value
                       if proxy_healthy or cid.startswith("node@")
                       else ComponentState.FAILED.value)))
    coord.set_replicas("app", [f"app#{i}" for i in range(running)])
    coord.set_route_health("app", app_status == "RUNNING")


def test_ready_requires_replicas_route_and_canary():
    coord = _coord()
    _feed(coord, running=2)
    assert not coord.is_ready()          # canary has not answered yet
    coord.set_canary("app", True)
    assert coord.is_ready()


def test_readiness_is_revocable_when_a_replica_dies():
    coord = _coord()
    _feed(coord, running=2)
    coord.set_canary("app", True)
    assert coord.is_ready()
    _feed(coord, running=1)              # one replica vanished
    snap = coord.evaluate()
    assert not snap.ready
    assert any("1/2 replicas" in b for b in snap.blockers)


def test_unhealthy_proxy_blocks_readiness():
    coord = _coord()
    _feed(coord, running=2, proxy_healthy=False)
    coord.set_canary("app", True)
    assert any("proxy@n0" in b for b in coord.blockers())


def test_missing_node_blocks_readiness():
    coord = _coord(nodes=["n0", "n1"])
    _feed(coord, running=2, nodes=("n0",))
    coord.set_canary("app", True)
    assert any("nodes not reporting" in b for b in coord.blockers())


def test_receipts_gate_readiness_by_role():
    profile = default_profile()
    store = ReceiptStore(profile, "d1", 7)
    coord = _coord(receipts=store)
    _feed(coord, running=2)
    coord.set_canary("app", True)
    assert not coord.is_ready()
    assert any("compatibility receipt" in b for b in coord.blockers())

    for role in profile.required_roles:
        ok, why = store.add(build_receipt(
            profile=profile, role=role, deployment_id="d1", generation=7,
            patch_results={p: True for p in profile.required_patch_ids(role)}))
        assert ok, why
    assert coord.is_ready()


def test_receipt_from_a_stale_generation_is_rejected():
    profile = default_profile()
    store = ReceiptStore(profile, "d1", 7)
    ok, why = store.add(build_receipt(
        profile=profile, role="replica", deployment_id="d1", generation=6,
        patch_results={}))
    assert not ok and "stale generation" in why


# -- compatibility semantics ------------------------------------------------

def test_gated_off_patch_is_not_required(monkeypatch):
    monkeypatch.delenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", raising=False)
    profile = default_profile()
    assert profile.required_patch_ids("replica") == ()
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    assert "SC-01" in profile.required_patch_ids("replica")


def test_postcondition_is_not_applicable_when_target_absent():
    """A module that was never imported yields None, not a false 'applied'."""
    assert _postcondition_sitecustomize("SC-01") is None


def test_not_applicable_patches_do_not_claim_to_be_applied(monkeypatch):
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    activator = CompatibilityActivator(deployment_id="d1", generation=7)
    receipt = activator.activate("replica", apply_fn=lambda: None,
                                 verify_environment=False,
                                 postcondition=lambda pid: None)
    assert receipt.patch_results == {}
    assert "SC-01" in receipt.not_applicable
    ok, why = receipt.is_complete_for(activator.profile.required_patch_ids("replica"))
    assert ok, why


def test_a_half_patched_process_is_fatal(monkeypatch):
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    activator = CompatibilityActivator(deployment_id="d1", generation=7)
    from exaserve.compat.activator import ActivationError

    with pytest.raises(ActivationError, match="did not take effect"):
        activator.activate("replica", apply_fn=lambda: None,
                           verify_environment=False,
                           postcondition=lambda pid: False)


# -- canary + snapshot ------------------------------------------------------

def test_http_canary_rejects_a_response_without_a_completion(monkeypatch):
    class _Resp:
        def read(self):
            return json.dumps({"choices": []}).encode()

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    monkeypatch.setattr(sr.urllib.request, "build_opener",
                        lambda *a: type("O", (), {"open": lambda s, *a, **k: _Resp()})())
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

    monkeypatch.setattr(sr.urllib.request, "build_opener",
                        lambda *a: type("O", (), {"open": lambda s, *a, **k: _Resp()})())
    ok, _ = sr.http_canary("http://x/health")
    assert not ok


def test_snapshot_is_written_atomically_and_is_machine_readable(tmp_path):
    coord = _coord()
    _feed(coord, running=2)
    coord.set_canary("app", True)
    path = sr.write_snapshot(coord.evaluate(), str(tmp_path),
                             extra={"deployment_id": "d1"})
    data = json.loads(open(path).read())
    assert data["ready"] is True and data["generation"] == 7
    assert data["deployment_id"] == "d1"
    assert not list(tmp_path.glob("*.tmp.*"))


def test_await_ready_returns_the_last_snapshot_on_timeout(monkeypatch):
    coord = _coord()
    monkeypatch.setattr(sr, "collect_observations", lambda *a, **k: {})
    snap = sr.await_ready(coord, node_id="n0", instance_id="i",
                          canary=lambda: (False, "down"), routes=["app"],
                          timeout_s=0.0, poll_s=0.0, log=lambda *_: None)
    assert not snap.ready and snap.blockers


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


def test_route_sampling_is_recorded_not_implied():
    routes = [f"app_r{i}" for i in range(24)]
    sampled = sr.sample_routes(routes, 5)
    assert routes[0] in sampled and routes[-1] in sampled
    assert 5 <= len(sampled) <= 7      # first/last may already be in the stride
    plan = sr.build_plan(deployment_id="d", generation=1, plan_hash="h",
                         node_ids=["n0"], expected_replicas={r: 1 for r in routes},
                         routes=routes, canary_routes=sampled)
    # Only the SAMPLED routes must answer; the rest are not claimed.
    assert plan.routes_to_canary() == sampled


def test_external_attestation_is_a_distinct_and_weaker_evidence_class():
    """An unmodified daemon cannot prove a sentinel from inside itself.

    It must declare the required patches as not-provable-here and carry the
    owner's probe — and it must NOT be able to pass by simply omitting them.
    """
    from exaserve.compat.activator import CompatibilityActivator

    profile = default_profile()
    store = ReceiptStore(profile, "d1", 7)
    activator = CompatibilityActivator(deployment_id="d1", generation=7)

    receipt = activator.attest_external("engine", executable="/usr/bin/python3",
                                        version_probe="vllm 0.15.0")
    ok, why = store.add(receipt)
    assert ok, why
    assert "engine" in store.externally_attested_roles()

    # No evidence at all must be rejected.
    from dataclasses import replace

    bare = replace(receipt, versions={}, not_applicable=())
    ok, why = ReceiptStore(profile, "d1", 7).add(bare)
    assert not ok and "evidence" in why


def test_external_attestation_cannot_claim_in_process_proof(monkeypatch):
    """A 'supervisor' receipt asserting applied patches is still judged as
    external evidence, so it cannot masquerade as self-attestation."""
    monkeypatch.setenv("EXASERVE_VLLM_PATCH_PP_LAYER_FILTER", "1")
    profile = default_profile()
    forged = build_receipt(
        profile=profile, role="engine", deployment_id="d1", generation=7,
        patch_results={p: True for p in profile.required_patch_ids("engine")},
        attestation="supervisor", versions={"executable": "/bin/python"})
    ok, why = ReceiptStore(profile, "d1", 7).add(forged)
    assert not ok and "does not account for" in why


def test_redelivered_receipts_do_not_inflate_the_count():
    """The channel is drained every poll and drains are non-destructive."""
    profile = default_profile()
    store = ReceiptStore(profile, "d1", 7)
    receipt = build_receipt(profile=profile, role="replica", deployment_id="d1",
                            generation=7, patch_results={})
    assert store.add(receipt) == (True, "ok")
    assert store.add(receipt) == (True, "duplicate")
    assert store.count() == 1


def test_activator_normalizes_a_raw_scheduler_job_id(monkeypatch):
    """A raw PBS_JOBID must not produce a receipt the head rejects.

    The head scopes the deployment id (`split('.')[0][:40]`); an activator that
    read the raw env value built receipts under a different id and every one
    was rejected as "wrong deployment".
    """
    from exaserve.compat.activator import CompatibilityActivator

    for var in ("EXASERVE_DEPLOYMENT_ID", "EXASERVE_SCALING_TRACE_TOKEN",
                "EXASERVE_JOBID"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("PBS_JOBID", "8736431.aurora-pbs-0001.hostmgmt.example")
    assert CompatibilityActivator().deployment_id == "8736431"

    monkeypatch.setenv("EXASERVE_DEPLOYMENT_ID",
                       "8736431.aurora-pbs-0001.hostmgmt.example")
    assert CompatibilityActivator().deployment_id == "8736431"


def test_discovery_caches_targets_but_not_running_counts(monkeypatch):
    """The gate must not become an O(replicas) fleet-wide poller (KI-A4).

    Declared targets are static within a generation, so the expensive
    per-replica detail call happens once; running counts come from the light
    status overview on every pass, and a vanished app revokes readiness.
    """
    calls = {"details": 0}

    def _details():
        calls["details"] += 1
        return {"applications": {"app": {
            "route_prefix": "/", "status": "RUNNING",
            "deployments": {"d": {"target_num_replicas": 4,
                                  "replicas": [{"state": "RUNNING"}] * 4}}}}}

    monkeypatch.setattr(sr, "_serve_details", _details)
    sr.reset_discovery_cache()
    first = sr.discover_applications(use_cache=False)
    assert first["app"]["target"] == 4 and calls["details"] == 1

    class _Serve:
        @staticmethod
        def status():
            return {"applications": {"app": {
                "status": "RUNNING",
                "deployments": {"d": {"replica_states": {"RUNNING": 2}}}}}}

    import sys
    import types

    module = types.ModuleType("ray")
    module.serve = _Serve
    monkeypatch.setitem(sys.modules, "ray", module)
    second = sr.discover_applications()
    assert calls["details"] == 1          # no second expensive call
    assert second["app"]["target"] == 4   # target remembered
    assert second["app"]["running"] == 2  # running refreshed


def test_a_vanished_application_revokes_readiness(monkeypatch):
    sr.reset_discovery_cache()
    monkeypatch.setattr(sr, "_serve_details", lambda: {"applications": {"app": {
        "route_prefix": "/", "status": "RUNNING",
        "deployments": {"d": {"target_num_replicas": 2,
                              "replicas": [{"state": "RUNNING"}] * 2}}}}})
    sr.discover_applications(use_cache=False)

    import sys
    import types

    module = types.ModuleType("ray")
    module.serve = type("S", (), {"status": staticmethod(lambda: {"applications": {}})})
    monkeypatch.setitem(sys.modules, "ray", module)
    gone = sr.discover_applications()
    assert gone["app"]["running"] == 0 and gone["app"]["status"] == "MISSING"


def _stub_cluster(monkeypatch, *, running: int, target: int = 2, canary_ok: bool):
    """Minimal stand-in for a live Ray/Serve cluster."""
    import sys
    import types

    ray = types.ModuleType("ray")
    ray.nodes = lambda: [{"NodeID": "n0", "Alive": True}]
    ray.serve = type("S", (), {"status": staticmethod(lambda: {
        "proxies": {"n0": "HEALTHY"},
        "applications": {"app": {"status": "RUNNING", "deployments": {
            "d": {"replica_states": {"RUNNING": running}}}}}})})
    monkeypatch.setitem(sys.modules, "ray", ray)
    monkeypatch.setitem(sys.modules, "ray.serve", ray.serve)
    sr.reset_discovery_cache()
    monkeypatch.setattr(sr, "_serve_details", lambda: {"applications": {"app": {
        "route_prefix": "/", "status": "RUNNING", "deployments": {
            "d": {"target_num_replicas": target,
                  "replicas": [{"state": "RUNNING"}] * running}}}}})
    monkeypatch.setattr(sr, "http_canary",
                        lambda *a, **k: (canary_ok, "" if canary_ok else "refused"))

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

    monkeypatch.setattr(sr, "_build_receipt_store",
                        lambda *a, **k: (_Store(), ["replica"]))


def test_gate_raises_when_the_predicate_fails(monkeypatch, tmp_path):
    """Fail-closed: an unsatisfied predicate must abort the deploy, not warn."""
    _stub_cluster(monkeypatch, running=1, target=2, canary_ok=True)
    monkeypatch.delenv("EXASERVE_ALLOW_DEGRADED_READINESS", raising=False)
    with pytest.raises(RuntimeError, match="refusing to declare"):
        sr.enforce_readiness(deployment_id="d1", generation=1, plan_hash="h",
                             base_url="http://x:8000", snapshot_dir=str(tmp_path),
                             timeout_s=0.0, log=lambda *_: None)
    # The verdict is still recorded, with the blocker named.
    data = json.loads((tmp_path / "readiness.json").read_text())
    assert data["ready"] is False
    assert any("1/2 replicas" in b for b in data["blockers"])


def test_gate_returns_ready_when_everything_holds(monkeypatch, tmp_path):
    _stub_cluster(monkeypatch, running=2, target=2, canary_ok=True)
    snap = sr.enforce_readiness(deployment_id="d1", generation=1, plan_hash="h",
                                base_url="http://x:8000", snapshot_dir=str(tmp_path),
                                timeout_s=5.0, log=lambda *_: None)
    assert snap.ready


def test_a_dead_canary_alone_blocks_the_gate(monkeypatch, tmp_path):
    """Replicas can all be RUNNING while the served route answers nothing."""
    _stub_cluster(monkeypatch, running=2, target=2, canary_ok=False)
    monkeypatch.delenv("EXASERVE_ALLOW_DEGRADED_READINESS", raising=False)
    with pytest.raises(RuntimeError, match="refusing to declare"):
        sr.enforce_readiness(deployment_id="d1", generation=1, plan_hash="h",
                             base_url="http://x:8000", snapshot_dir=str(tmp_path),
                             timeout_s=0.0, log=lambda *_: None)
    data = json.loads((tmp_path / "readiness.json").read_text())
    assert any(b.startswith("canary ") for b in data["blockers"])


def test_degraded_escape_starts_anyway_but_records_the_blockers(monkeypatch, tmp_path):
    _stub_cluster(monkeypatch, running=1, target=2, canary_ok=True)
    monkeypatch.setenv("EXASERVE_ALLOW_DEGRADED_READINESS", "1")
    snap = sr.enforce_readiness(deployment_id="d1", generation=1, plan_hash="h",
                                base_url="http://x:8000", snapshot_dir=str(tmp_path),
                                timeout_s=0.0, log=lambda *_: None)
    assert not snap.ready and snap.blockers
