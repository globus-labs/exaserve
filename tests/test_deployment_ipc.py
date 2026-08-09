"""WP4/WP5: isolated deployment child -> authenticated app evidence."""

from __future__ import annotations

import os
import time
from types import SimpleNamespace

import pytest

from exaserve.control.deployment_ipc import (
    DeploymentIPCError,
    DeploymentObservationIngress,
    deliver_snapshot,
    socket_path_for,
    validate_snapshot,
)


def test_deployment_socket_path_is_generation_scoped_and_path_safe(tmp_path):
    path = socket_path_for("../../escape", 7, root=str(tmp_path))
    assert path != socket_path_for("../../escape", 8, root=str(tmp_path))
    assert "escape" not in path
    assert len(os.fsencode(path)) <= 103
    assert os.path.isabs(path)


def _payload(**overrides):
    payload = {
        "payload_version": 2,
        "kind": "SERVE_APPLICATION_SNAPSHOT",
        "deployment_id": "d1",
        "generation": 7,
        "deployment_plan_hash": "a" * 64,
        "site_profile_hash": "b" * 64,
        "allocation_binding_hash": "c" * 64,
        "applications": {
            "app": {
                "running": 2,
                "target": 2,
                "route_prefix": "/",
                "status": "RUNNING",
            }
        },
        "nodes": [
            {
                "node_id": "ray-node-0",
                "node_name": "n0",
                "node_address": "10.0.0.1",
                "alive": True,
                "cpu": 64.0,
                "gpu": 12.0,
            }
        ],
        "proxies": [{"node_id": "ray-node-0", "status": "HEALTHY"}],
        "observed_at": time.time(),
    }
    payload.update(overrides)
    return payload


def _validate(payload):
    return validate_snapshot(
        payload,
        deployment_id="d1",
        generation=7,
        deployment_plan_hash="a" * 64,
        site_profile_hash="b" * 64,
        allocation_binding_hash="c" * 64,
    )


def test_snapshot_contract_is_exact_and_identity_bound():
    assert _validate(_payload())["applications"]["app"]["target"] == 2
    with pytest.raises(DeploymentIPCError, match="generation mismatch"):
        _validate(_payload(generation=6))
    bad = _payload()
    bad["extra"] = True
    with pytest.raises(DeploymentIPCError, match="fields"):
        _validate(bad)


def test_snapshot_accepts_only_an_explicit_null_for_a_route_less_application():
    payload = _payload()
    payload["applications"]["app"]["route_prefix"] = None
    assert _validate(payload)["applications"]["app"]["route_prefix"] is None

    payload["applications"]["app"]["route_prefix"] = ""
    with pytest.raises(DeploymentIPCError, match="route is invalid"):
        _validate(payload)


@pytest.mark.parametrize(
    ("field", "value"),
    [("payload_version", True), ("generation", True), ("deployment_id", 1)],
)
def test_snapshot_identity_rejects_coercible_types(field, value):
    with pytest.raises(DeploymentIPCError):
        _validate(_payload(**{field: value}))


def test_ingress_attributes_the_kernel_peer_pid(tmp_path):
    path = socket_path_for("peer-pid-test", 0, root=str(tmp_path))
    ingress = DeploymentObservationIngress(path)
    assert ingress.start()
    try:
        payload = _payload()
        assert deliver_snapshot(payload, path=path)
        assert ingress.drain_with_peer() == [(os.getpid(), payload)]
    finally:
        ingress.stop()


def test_second_ingress_cannot_unlink_a_live_owners_socket(tmp_path):
    path = socket_path_for("owner-lock-test", 0, root=str(tmp_path))
    first = DeploymentObservationIngress(path, log=lambda _message: None)
    second = DeploymentObservationIngress(path, log=lambda _message: None)
    assert first.start()
    try:
        assert not second.start()
        payload = _payload()
        assert deliver_snapshot(payload, path=path)
        assert first.drain_with_peer() == [(os.getpid(), payload)]
    finally:
        assert second.stop()
        assert first.stop()


def test_outer_observer_accepts_only_the_owned_child():
    from exaserve.control.deployment_observer import DeploymentObserver

    plan = SimpleNamespace(
        deployment_id="d1", deployment_plan_hash="a" * 64, site_profile_hash="b" * 64
    )
    binding = SimpleNamespace(generation=7, allocation_binding_hash="c" * 64)

    class _Ingress:
        def __init__(self):
            self.items = [(os.getpid(), _payload())]

        def drain_with_peer(self):
            items, self.items = self.items, []
            return items

    observer = DeploymentObserver(
        ingress=_Ingress(),
        owned_pid=lambda: os.getpid(),
        plan=plan,
        binding=binding,
        poll_s=0.01,
        log=lambda _message: None,
    )
    observer.start()
    deadline = time.monotonic() + 1
    snapshot = None
    while snapshot is None and time.monotonic() < deadline:
        snapshot = observer.current(max_age_s=1)
        time.sleep(0.005)
    assert observer.stop()
    assert observer.failure() is None
    assert snapshot["applications"]["app"]["status"] == "RUNNING"

    snapshot["applications"]["app"]["status"] = "CORRUPTED"
    snapshot["nodes"][0]["node_id"] = "CORRUPTED"
    retained = observer.current(max_age_s=1)
    assert retained["applications"]["app"]["status"] == "RUNNING"
    assert retained["nodes"][0]["node_id"] == "ray-node-0"


def test_outer_observer_timing_contract_rejects_coercion():
    from exaserve.control.deployment_observer import DeploymentObserver

    class _Ingress:
        def drain_with_peer(self):
            return []

    with pytest.raises(ValueError, match="poll interval"):
        DeploymentObserver(
            ingress=_Ingress(),
            owned_pid=lambda: None,
            plan=object(),
            binding=object(),
            poll_s="0.2",  # type: ignore[arg-type]
        )


def test_owned_child_protocol_violation_becomes_outer_failure():
    from exaserve.control.deployment_observer import DeploymentObserver

    plan = SimpleNamespace(
        deployment_id="d1", deployment_plan_hash="a" * 64, site_profile_hash="b" * 64
    )
    binding = SimpleNamespace(generation=7, allocation_binding_hash="c" * 64)

    class _Ingress:
        items = [(os.getpid(), _payload(generation=6))]

        def drain_with_peer(self):
            items, self.items = self.items, []
            return items

    observer = DeploymentObserver(
        ingress=_Ingress(),
        owned_pid=lambda: os.getpid(),
        plan=plan,
        binding=binding,
        poll_s=0.01,
        log=lambda _message: None,
    )
    observer.start()
    deadline = time.monotonic() + 1
    while observer.failure() is None and time.monotonic() < deadline:
        time.sleep(0.005)
    assert observer.stop()
    assert "generation mismatch" in observer.failure()
