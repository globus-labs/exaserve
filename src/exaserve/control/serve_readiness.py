"""Serve application observation producer (plan WP5, audit IMP-B02).

This turns the current cluster into observations. It was once the *authority*
-- an in-child gate that decided READY against this node's internal Serve port,
from a process that can see neither the gateway nor the other ranks' sessions.
WP13 removed that: `observe_deployment` publishes typed evidence, and the
allocation-head ``ReadinessCoordinator`` in the composition root decides.

Why this exists: readiness used to be a line of stdout. `CLUSTER FULLY READY`
was printed after a bring-up sequence, and every consumer (driver, eval
backends, smoke scripts) grepped for that text. That made readiness
*unfalsifiable* — the marker could not be revoked when a replica died 200ms
later, and nothing checked that the externally-routed path actually answered.
Here READY is a predicate over observed identities, so it is revocable and each
failure names its blocker.

Expected replica counts come from each application's ``target_num_replicas``
(the applied deployment config), never from what happens to be running — so a
replica that dies drops the count below target and revokes readiness.

Evidence is delivered through the one versioned deployment-child IPC boundary
directly to the outer supervisor. Kernel peer credentials bind each message to
the exact child it owns. No shared file, rank-zero relay, or log line is an
input to readiness.
"""

from __future__ import annotations

import json
import asyncio
import math
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from typing import Callable

from .deployment_ipc import KIND, PAYLOAD_VERSION


def _state_name(value) -> str:
    for candidate in (getattr(value, "value", None), getattr(value, "name", None), value):
        if isinstance(candidate, str) and candidate:
            return candidate
    raise RuntimeError(f"Serve state is not a nonempty string: {value!r}")


def _mapping(value, *, label: str) -> Mapping:
    if not isinstance(value, Mapping):
        raise RuntimeError(f"{label} must be an object")
    return value


def _sequence(value, *, label: str) -> Sequence:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise RuntimeError(f"{label} must be a sequence")
    return value


def _nonnegative_count(value, *, label: str) -> int:
    if type(value) is not int or value < 0:
        raise RuntimeError(f"{label} must be a nonnegative integer")
    return value


def _get(obj, key, default=None):
    """Read a field from a mapping OR a model.

    ``ServeControllerClient.get_serve_details()`` returns a plain dict, while
    ``serve.status()`` returns dataclasses. Attribute-only access silently fell
    through to the weaker overview path, where target == running — so a dead
    replica could not lower the count below target and readiness never revoked.
    """
    if isinstance(obj, Mapping):
        value = obj.get(key, default)
    else:
        value = getattr(obj, key, default)
    return default if value is None else value


def _route_prefix(obj, *, label: str) -> str | None:
    """Read a Serve route while preserving an intentional route-less app."""
    value = (
        obj.get("route_prefix") if isinstance(obj, Mapping) else getattr(obj, "route_prefix", None)
    )
    if value is not None and (
        not isinstance(value, str) or not value.startswith("/") or len(value) > 1024
    ):
        raise RuntimeError(f"{label} has an invalid route")
    return value


def _serve_details():
    """ServeInstanceDetails from the compatibility-profile-pinned adapter."""
    try:
        from ray.serve._private.api import _get_global_client
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("verified Ray profile does not expose Serve details") from exc
    try:
        return _get_global_client().get_serve_details()
    except (AttributeError, RuntimeError, TypeError, ValueError) as exc:
        raise RuntimeError(
            f"Serve details observation failed: {type(exc).__name__}: {exc}"
        ) from exc


def _running_from_overview(dep_status) -> int:
    # _get, not getattr: the overview is dataclasses in production but a plain
    # mapping in other paths, and attribute access on a mapping silently
    # returns nothing — reporting zero running replicas for a healthy app.
    states = _mapping(_get(dep_status, "replica_states", {}), label="replica_states")
    return sum(
        _nonnegative_count(count, label=f"replica state {_state_name(state)!r} count")
        for state, count in states.items()
        if _state_name(state).upper() == "RUNNING"
    )


# Declared targets and route prefixes come from the applied config and do not
# change within a generation, so the EXPENSIVE per-replica detail call is made
# once and cached. Polling it every pass would make the readiness gate an
# O(replicas) fleet-wide poller — exactly the cost profile KI-A4 warns about.
_STATIC_CACHE: dict[str, dict] = {}


def reset_discovery_cache() -> None:
    _STATIC_CACHE.clear()


def discover_applications(use_cache: bool = True) -> dict[str, dict]:
    """{app_name: {"route_prefix", "target", "running", "status"}} from live Serve.

    ``target`` is the *declared* replica count of the applied config. Volatile
    counts are refreshed by :class:`ServeEventObserver`, never by repeated
    ``serve.status()`` calls.
    """
    if use_cache and _STATIC_CACHE:
        return {name: dict(info) for name, info in _STATIC_CACHE.items()}
    apps: dict[str, dict] = {}
    details = _serve_details()
    applications = _mapping(_get(details, "applications", {}), label="Serve applications")
    for name, app in applications.items():
        if not isinstance(name, str) or not name:
            raise RuntimeError("Serve application name must be a nonempty string")
        target = running = 0
        deployments = _mapping(_get(app, "deployments", {}), label=f"{name} deployments")
        for dep in deployments.values():
            target += _nonnegative_count(
                _get(dep, "target_num_replicas", 0), label=f"{name} target replicas"
            )
            replicas = _sequence(_get(dep, "replicas", []), label=f"{name} replicas")
            running += sum(
                1 for r in replicas if _state_name(_get(r, "state", "")).upper() == "RUNNING"
            )
        route = _route_prefix(app, label=f"Serve application {name!r}")
        apps[name] = {
            "route_prefix": route,
            "target": target,
            "running": running,
            "status": _state_name(_get(app, "status", "")).upper(),
        }
    if apps:
        _STATIC_CACHE.clear()
        _STATIC_CACHE.update({k: dict(v) for k, v in apps.items()})
    return apps


class ServeEventObserver:
    """One initial reconciliation plus Serve's push-on-change event stream.

    Ray 2.53 has no public Serve subscription API.  The compatibility profile
    therefore pins the controller's ``LongPollClient`` surface.  It broadcasts
    only changed route tables and deployment replica sets, so an ordinary
    heartbeat is O(applications) local serialization and performs no fleet-wide
    Ray/Serve RPC.  ``ray.nodes()`` and ``get_serve_details()`` run exactly once
    during the race-safe bootstrap.
    """

    def __init__(
        self,
        *,
        deployment_id: str,
        generation: int,
        deployment_plan_hash: str,
        site_profile_hash: str,
        allocation_binding_hash: str,
    ) -> None:
        self.identity = {
            "deployment_id": deployment_id,
            "generation": generation,
            "deployment_plan_hash": deployment_plan_hash,
            "site_profile_hash": site_profile_hash,
            "allocation_binding_hash": allocation_binding_hash,
        }
        self._lock = threading.RLock()
        self._apps: dict[str, dict] = {}
        self._deployments: dict[object, tuple[str, int]] = {}
        self._running: dict[object, int] = {}
        self._available: dict[object, bool] = {}
        self._route_apps: set[str] = set()
        self._nodes: list[dict] = []
        self._event_count = 0
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._client = None
        self._started = threading.Event()
        self._start_lock = threading.Lock()
        self._start_attempted = False
        self._stop_lock = threading.Lock()
        self._stop_result: bool | None = None
        self._stop_requested = False
        self._failure: str | None = None

    @property
    def event_count(self) -> int:
        with self._lock:
            return self._event_count

    def _bootstrap(self) -> tuple[object, dict]:
        from ..compat.private_api import require

        get_client = require("serve_controller_client")
        deployment_id_type = require("serve_deployment_id")
        details = _serve_details()
        applications = _mapping(
            _get(details, "applications", {}), label="Serve bootstrap applications"
        )
        apps: dict[str, dict] = {}
        deployments: dict[object, tuple[str, int]] = {}
        running: dict[object, int] = {}
        available: dict[object, bool] = {}
        route_apps: set[str] = set()
        for app_name, app in applications.items():
            if not isinstance(app_name, str) or not app_name:
                raise RuntimeError("Serve application name must be a nonempty string")
            route = _route_prefix(app, label=f"Serve application {app_name!r}")
            target_total = 0
            running_total = 0
            app_deployments = _mapping(
                _get(app, "deployments", {}), label=f"{app_name} deployments"
            )
            for deployment_name, dep in app_deployments.items():
                if not isinstance(deployment_name, str) or not deployment_name:
                    raise RuntimeError("Serve deployment name must be a nonempty string")
                target = _nonnegative_count(
                    _get(dep, "target_num_replicas", 0),
                    label=f"{app_name}/{deployment_name} target replicas",
                )
                replicas = _sequence(
                    _get(dep, "replicas", []), label=f"{app_name}/{deployment_name} replicas"
                )
                current = sum(
                    1
                    for replica in replicas
                    if _state_name(_get(replica, "state", "")).upper() == "RUNNING"
                )
                key = deployment_id_type(name=deployment_name, app_name=app_name)
                deployments[key] = (app_name, target)
                running[key] = current
                available[key] = _state_name(_get(dep, "status", "HEALTHY")).upper() not in {
                    "UNHEALTHY",
                    "DEPLOY_FAILED",
                }
                target_total += target
                running_total += current
            status = _state_name(_get(app, "status", "")).upper()
            apps[app_name] = {
                "route_prefix": route,
                "target": target_total,
                "running": running_total,
                "status": status,
            }
            if route:
                route_apps.add(app_name)

        import ray

        from .ray_cluster_probe import normalize_nodes

        nodes = normalize_nodes(ray.nodes())
        with self._lock:
            self._apps = apps
            self._deployments = deployments
            self._running = running
            self._available = available
            self._route_apps = route_apps
            self._nodes = sorted(nodes, key=lambda item: item["node_id"])
        client = get_client()
        return client._controller, deployments  # pinned by private-api profile

    def _deployment_update(self, key: object, update) -> None:
        with self._lock:
            if key not in self._deployments:
                self._failure = f"unplanned Serve deployment event: {key!r}"
                return
            try:
                replicas = _sequence(
                    _get(update, "running_replicas", ()), label="running_replicas update"
                )
                available = _get(update, "is_available", False)
                if type(available) is not bool:
                    raise RuntimeError("Serve deployment is_available update must be a boolean")
            except RuntimeError as exc:
                self._failure = str(exc)
                return
            self._running[key] = len(replicas)
            self._available[key] = available
            self._event_count += 1

    def _route_update(self, routes) -> None:
        try:
            route_mapping = _mapping(routes, label="Serve route update")
            apps = set()
            for key in route_mapping:
                app_name = getattr(key, "app_name", "")
                if not isinstance(app_name, str) or not app_name:
                    raise RuntimeError("Serve route app_name must be a nonempty string")
                apps.add(app_name)
        except RuntimeError as exc:
            with self._lock:
                self._failure = str(exc)
            return
        with self._lock:
            unexpected = sorted(apps - set(self._apps))
            if unexpected:
                self._failure = f"unplanned Serve route event: {unexpected[:8]}"
                return
            self._route_apps = apps
            self._event_count += 1

    def start(self) -> "ServeEventObserver":
        with self._start_lock:
            if self._start_attempted:
                raise RuntimeError("Serve event observer start may be attempted only once")
            self._start_attempted = True
        controller, deployments = self._bootstrap()
        from ..compat.private_api import require

        long_poll_client = require("serve_long_poll_client")
        namespace = require("serve_long_poll_namespace")
        listeners = {
            (namespace.DEPLOYMENT_TARGETS, key): (
                lambda update, key=key: self._deployment_update(key, update)
            )
            for key in deployments
        }
        listeners[namespace.ROUTE_TABLE] = self._route_update

        def _run() -> None:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            try:
                self._client = long_poll_client(controller, listeners, call_in_event_loop=loop)
                self._started.set()
                loop.run_forever()
            except BaseException as exc:  # thread boundary; observed fail-closed
                with self._lock:
                    self._failure = f"Serve event observer failed: {type(exc).__name__}: {exc}"
                self._started.set()
            finally:
                loop.close()

        self._thread = threading.Thread(target=_run, daemon=True, name="exaserve-serve-events")
        self._thread.start()
        if not self._started.wait(10.0) or self._failure:
            startup_failure = self._failure or "Serve event observer did not start"
            self.stop(timeout_s=5.0)
            raise RuntimeError(self._failure or startup_failure)
        return self

    def stop(self, timeout_s: float = 5.0) -> bool:
        if (
            isinstance(timeout_s, bool)
            or not isinstance(timeout_s, (int, float))
            or not math.isfinite(float(timeout_s))
            or timeout_s < 0
        ):
            raise ValueError("Serve event observer stop timeout must be finite and non-negative")
        with self._stop_lock:
            if self._stop_result is not None:
                return self._stop_result
            self._stop_requested = True
            errors = []
            if self._client is not None:
                try:
                    self._client.stop()
                except BaseException as exc:
                    errors.append(f"long-poll client stop failed: {type(exc).__name__}: {exc}")
                self._client = None
            if self._loop is not None:
                try:
                    self._loop.call_soon_threadsafe(self._loop.stop)
                except RuntimeError as exc:
                    errors.append(f"event loop stop failed: {exc}")
            if self._thread is not None:
                self._thread.join(float(timeout_s))
                if self._thread.is_alive():
                    errors.append(f"event observer thread survived {float(timeout_s):g}s")
                else:
                    self._thread = None
                    self._loop = None
            if errors:
                with self._lock:
                    self._failure = self._failure or "; ".join(errors)
            self._stop_result = not errors
            return self._stop_result

    def snapshot(self) -> dict:
        with self._lock:
            if self._failure:
                raise RuntimeError(self._failure)
            if (
                self._client is not None
                and not self._client.is_running
                and not self._stop_requested
            ):
                raise RuntimeError("Serve controller event stream stopped unexpectedly")
            aggregates: dict[str, dict[str, int | bool]] = {
                app_name: {"running": 0, "target": 0, "available": True, "keys": 0}
                for app_name in self._apps
            }
            for key, (owner, target) in self._deployments.items():
                aggregate = aggregates.get(owner)
                if aggregate is None:
                    raise RuntimeError(f"deployment projection has unknown owner {owner!r}")
                aggregate["running"] = int(aggregate["running"]) + self._running.get(key, 0)
                aggregate["target"] = int(aggregate["target"]) + target
                aggregate["available"] = bool(aggregate["available"]) and self._available.get(
                    key, False
                )
                aggregate["keys"] = int(aggregate["keys"]) + 1
            apps = {}
            for app_name, static in self._apps.items():
                aggregate = aggregates[app_name]
                running = int(aggregate["running"])
                target = int(aggregate["target"])
                available = bool(aggregate["available"])
                has_deployments = int(aggregate["keys"]) > 0
                route_ok = static["route_prefix"] is None or app_name in self._route_apps
                status = (
                    "RUNNING"
                    if has_deployments and available and route_ok and running >= target > 0
                    else "UNHEALTHY"
                    if has_deployments and not available
                    else "MISSING_ROUTE"
                    if has_deployments and not route_ok
                    else "DEPLOYING"
                )
                apps[app_name] = {
                    "running": running,
                    "target": target,
                    "route_prefix": static["route_prefix"],
                    "status": status,
                }
            return {
                "payload_version": PAYLOAD_VERSION,
                "kind": KIND,
                **self.identity,
                "applications": apps,
                "nodes": [dict(item) for item in self._nodes],
                # Per-node proxy health is published by NodeSupervisors.  A
                # static controller snapshot must not masquerade as a lease.
                "proxies": [],
                "observed_at": time.time(),
            }


def http_canary(url: str, *, timeout_s: float = 60.0, max_tokens: int = 4) -> tuple[bool, str]:
    """One real completion through the EXTERNAL route (plan WP5.9).

    A /health probe is not a canary: it proves the proxy is up, not that the
    model answers. Proxy env vars are bypassed — on Aurora the site proxy would
    otherwise intercept in-cluster traffic and report a false failure.
    """
    body = json.dumps({"prompt": "The capital of France is", "max_tokens": max_tokens}).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout_s) as resp:
            from ..state.atomic import strict_json_loads

            payload = strict_json_loads(resp.read().decode())
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return False, f"{type(exc).__name__}: {exc}"
    if not isinstance(payload, dict):
        return False, f"completion payload is not an object: {str(payload)[:200]}"
    choices = payload.get("choices")
    if (
        not isinstance(choices, list)
        or not choices
        or not isinstance(choices[0], dict)
        or not isinstance(choices[0].get("text"), str)
    ):
        return False, f"no completion in response: {str(payload)[:200]}"
    return True, choices[0]["text"][:80]


def sample_deployment(
    *,
    deployment_id: str,
    generation: int,
    deployment_plan_hash: str,
    site_profile_hash: str,
    allocation_binding_hash: str,
    observer: ServeEventObserver | None = None,
) -> dict:
    """Render one heartbeat from locally retained event state.

    A caller that does not supply an observer gets a one-shot observer solely
    for compatibility with diagnostics/tests. Production owns one observer for
    the generation and passes it to every call.
    """
    owned = observer is None
    observer = (
        observer
        or ServeEventObserver(
            deployment_id=deployment_id,
            generation=generation,
            deployment_plan_hash=deployment_plan_hash,
            site_profile_hash=site_profile_hash,
            allocation_binding_hash=allocation_binding_hash,
        ).start()
    )
    try:
        return observer.snapshot()
    finally:
        if owned:
            observer.stop()


def applications_at_target(payload: dict) -> bool:
    apps = payload["applications"]
    return bool(apps) and all(
        info["target"] > 0
        and info["running"] >= info["target"]
        and info["status"].upper() == "RUNNING"
        for info in apps.values()
    )


def observe_deployment(
    *,
    deployment_id: str,
    generation: int,
    deployment_plan_hash: str,
    site_profile_hash: str,
    allocation_binding_hash: str,
    publish: Callable[[dict], bool],
    observer: ServeEventObserver | None = None,
    timeout_s: float = 1800.0,
    poll_s: float = 5.0,
    log=print,
) -> dict:
    """Report what THIS process can see. Decide nothing (§3.2.1 Q3, IMP-B02).

    The deployment child used to be the readiness authority: it drained
    receipts, canaried its own internal Serve port, and printed the READY
    marker. That decided readiness against an endpoint no client uses, from a
    process that cannot see the gateway or the other ranks' sessions.

    The composition root owns that decision now. What survives here is the one
    thing this process is genuinely the best witness for -- whether its own
    Serve applications reached their replica targets. Every sample must cross
    the narrow, acknowledged IPC boundary; an unpublishable fact is not usable
    evidence and fails the child rather than falling back to a file.
    """
    deadline = time.monotonic() + timeout_s
    last_report = 0.0
    payload: dict = {}
    owned_observer = observer is None
    observer = (
        observer
        or ServeEventObserver(
            deployment_id=deployment_id,
            generation=generation,
            deployment_plan_hash=deployment_plan_hash,
            site_profile_hash=site_profile_hash,
            allocation_binding_hash=allocation_binding_hash,
        ).start()
    )
    try:
        while True:
            payload = observer.snapshot()
            if not publish(payload):
                raise RuntimeError(
                    "outer-supervisor deployment observation IPC rejected the sample"
                )
            running_all = applications_at_target(payload)
            if running_all:
                log(
                    f"[Deployment] evidence: "
                    f"{len(payload['applications'])} application(s) at target"
                )
                return payload
            now = time.monotonic()
            if now - last_report > 30.0:
                progress = {
                    k: "{}/{}".format(v["running"], v["target"])
                    for k, v in payload["applications"].items()
                }
                log(f"[Deployment] not at target yet: {progress}")
                last_report = now
            if now > deadline:
                raise TimeoutError(
                    "deployment observation deadline reached without every application at target"
                )
            time.sleep(poll_s)
    finally:
        if owned_observer:
            observer.stop()
