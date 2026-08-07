"""Live readiness authority for the Serve deployment (plan WP5, audit IMP-B02).

This turns the current cluster into observations. It was once the *authority*
-- an in-child gate that decided READY against this node's internal Serve port,
from a process that can see neither the gateway nor the other ranks' sessions.
WP13 removed that: `observe_deployment` publishes EVIDENCE, and
`control/plan_readiness.py` in the composition root decides.

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

The evidence is written to ``deployment_evidence.json`` so consumers read a
structured, generation-tagged fact instead of parsing logs.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Callable, Iterable, Optional

from .contracts import SCHEMA_VERSION, ComponentObservation, ComponentState, OwnerScope
from .readiness import ReadinessCoordinator, ReadinessPlan, ReadinessSnapshot

# EVIDENCE, not a decision. `readiness.json` is the composition root's single
# READY record; this file is what the deployment child observed about its own
# applications. They were the same filename until the root took ownership of
# the decision, at which point one process's evidence would have overwritten
# the other's verdict in the same directory.
EVIDENCE_FILENAME = "deployment_evidence.json"

_SEQ: dict[str, int] = {}


def _observation(plan, *, component_id: str, instance_id: str, node_id: str,
                 state: str, role: str, **extra) -> ComponentObservation:
    """Build a well-formed observation (the §3.1 field list is mandatory)."""
    seq = _SEQ[component_id] = _SEQ.get(component_id, 0) + 1
    return ComponentObservation(
        schema_version=SCHEMA_VERSION, deployment_id=plan.deployment_id,
        plan_hash=plan.plan_hash, generation=plan.generation,
        component_id=component_id, instance_id=instance_id, sequence=seq,
        owner_scope=OwnerScope.GLOBAL.value, role=role, node_id=node_id,
        state=state, observed_at=time.time(), **extra)


def _state_name(value) -> str:
    return str(getattr(value, "value", None) or getattr(value, "name", None) or value)


def _get(obj, key, default=None):
    """Read a field from a mapping OR a model.

    ``ServeControllerClient.get_serve_details()`` returns a plain dict, while
    ``serve.status()`` returns dataclasses. Attribute-only access silently fell
    through to the weaker overview path, where target == running — so a dead
    replica could not lower the count below target and readiness never revoked.
    """
    if isinstance(obj, dict):
        value = obj.get(key, default)
    else:
        value = getattr(obj, key, default)
    return default if value is None else value


def _serve_details():
    """ServeInstanceDetails, or None when this Ray build does not expose it."""
    try:
        from ray.serve._private.api import _get_global_client

        return _get_global_client().get_serve_details()
    except Exception:
        return None


def _running_from_overview(dep_status) -> int:
    # _get, not getattr: the overview is dataclasses in production but a plain
    # mapping in other paths, and attribute access on a mapping silently
    # returns nothing — reporting zero running replicas for a healthy app.
    states = _get(dep_status, "replica_states", {}) or {}
    return sum(int(c) for s, c in states.items() if _state_name(s).upper() == "RUNNING")


# Declared targets and route prefixes come from the applied config and do not
# change within a generation, so the EXPENSIVE per-replica detail call is made
# once and cached. Polling it every pass would make the readiness gate an
# O(replicas) fleet-wide poller — exactly the cost profile KI-A4 warns about.
_STATIC_CACHE: dict[str, dict] = {}


def reset_discovery_cache() -> None:
    _STATIC_CACHE.clear()


def discover_applications(use_cache: bool = True) -> dict[str, dict]:
    """{app_name: {"route_prefix", "target", "running", "status"}} from live Serve.

    ``target`` is the *declared* replica count of the applied config; ``running``
    is always read fresh from the light status overview.
    """
    if use_cache and _STATIC_CACHE:
        return _merge_running(_STATIC_CACHE)
    apps: dict[str, dict] = {}
    details = _serve_details()
    if details is not None:
        for name, app in _get(details, "applications", {}).items():
            target = running = 0
            for dep in _get(app, "deployments", {}).values():
                target += int(_get(dep, "target_num_replicas", 0) or 0)
                running += sum(
                    1 for r in _get(dep, "replicas", [])
                    if _state_name(_get(r, "state", "")).upper() == "RUNNING")
            apps[str(name)] = {
                "route_prefix": _get(app, "route_prefix", "/") or "/",
                "target": target, "running": running,
                "status": _state_name(_get(app, "status", "")).upper(),
            }
        if apps:
            _STATIC_CACHE.clear()
            _STATIC_CACHE.update({k: dict(v) for k, v in apps.items()})
            return apps
    # Fallback: status overview has no target counts; treat running as target
    # so the predicate still requires app RUNNING + healthy proxies + canary.
    from ray import serve

    for name, app in _get(serve.status(), "applications", {}).items():
        running = sum(_running_from_overview(d)
                      for d in _get(app, "deployments", {}).values())
        apps[str(name)] = {"route_prefix": "/", "target": running,
                           "running": running, "degraded_source": True,
                           "status": _state_name(_get(app, "status", "")).upper()}
    return apps


def _merge_running(static: dict[str, dict]) -> dict[str, dict]:
    """Refresh only the volatile fields (running count, app status)."""
    from ray import serve

    overview = _get(serve.status(), "applications", {})
    merged: dict[str, dict] = {}
    for name, info in static.items():
        app = overview.get(name)
        entry = dict(info)
        if app is not None:
            entry["running"] = sum(_running_from_overview(d)
                                   for d in _get(app, "deployments", {}).values())
            entry["status"] = _state_name(_get(app, "status", "")).upper()
        else:
            # The application vanished from the overview: report it as gone so
            # readiness is revoked rather than kept alive by a cached target.
            entry["running"] = 0
            entry["status"] = "MISSING"
        merged[name] = entry
    return merged


def collect_observations(coord: ReadinessCoordinator, *, node_id: str,
                         instance_id: str) -> dict:
    """Feed live Ray/Serve state into the coordinator. Returns a debug summary.

    Every fact here is observed from the cluster, never from a log line.
    """
    import ray
    from ray import serve

    plan = coord.plan
    summary: dict = {"nodes": 0, "apps": {}, "proxies": {}}

    alive = [n for n in ray.nodes() if n.get("Alive")]
    summary["nodes"] = len(alive)
    for node in alive:
        coord.observe(_observation(
            plan, component_id=f"node@{node['NodeID']}", instance_id=instance_id,
            node_id=str(node["NodeID"]), state=ComponentState.RUNNING.value,
            role="node"))

    for proxy_node_id, proxy in _get(serve.status(), "proxies", {}).items():
        name = _state_name(_get(proxy, "status", proxy)).upper()
        healthy = "HEALTHY" in name and "UN" not in name
        summary["proxies"][str(proxy_node_id)] = name
        coord.observe(_observation(
            plan, component_id=f"proxy@{proxy_node_id}", instance_id=instance_id,
            node_id=str(proxy_node_id), role="proxy",
            state=(ComponentState.RUNNING.value if healthy
                   else ComponentState.FAILED.value)))

    for app_name, info in discover_applications().items():
        summary["apps"][app_name] = info
        # Absolute set: a replica that vanished revokes readiness.
        coord.set_replicas(app_name,
                           (f"{app_name}#{i}" for i in range(int(info["running"]))))
        coord.set_route_health(app_name, info["status"] == "RUNNING")
    return summary


def http_canary(url: str, *, timeout_s: float = 60.0,
                max_tokens: int = 4) -> tuple[bool, str]:
    """One real completion through the EXTERNAL route (plan WP5.9).

    A /health probe is not a canary: it proves the proxy is up, not that the
    model answers. Proxy env vars are bypassed — on Aurora the site proxy would
    otherwise intercept in-cluster traffic and report a false failure.
    """
    body = json.dumps({"prompt": "The capital of France is",
                       "max_tokens": max_tokens}).encode()
    req = urllib.request.Request(
        url, data=body, headers={"Content-Type": "application/json"})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout_s) as resp:
            payload = json.loads(resp.read().decode())
    except (urllib.error.URLError, OSError, ValueError) as exc:
        return False, f"{type(exc).__name__}: {exc}"
    choices = payload.get("choices") or []
    if not choices or "text" not in choices[0]:
        return False, f"no completion in response: {str(payload)[:200]}"
    return True, str(choices[0]["text"])[:80]


def write_snapshot(snapshot: ReadinessSnapshot, directory: str,
                   extra: Optional[dict] = None) -> Optional[str]:
    """Persist the authoritative snapshot. Best-effort: never fails the deploy."""
    if not directory:
        return None
    try:
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, EVIDENCE_FILENAME)
        payload = snapshot.to_dict()
        if extra:
            payload.update(extra)
        tmp = f"{path}.tmp.{os.getpid()}"
        with open(tmp, "w") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        return path
    except OSError:
        return None


def sample_routes(routes: Iterable[str], limit: int) -> tuple[str, ...]:
    """Deterministic even sample, always including the first and last route."""
    routes = list(routes)
    if limit <= 0 or len(routes) <= limit:
        return tuple(routes)
    step = (len(routes) - 1) / (limit - 1) if limit > 1 else 1
    picked = {routes[min(len(routes) - 1, round(i * step))] for i in range(limit)}
    picked.add(routes[0])
    picked.add(routes[-1])
    return tuple(sorted(picked))


def build_plan(*, deployment_id: str, generation: int, plan_hash: str,
               node_ids: Iterable[str], expected_replicas: dict[str, int],
               routes: Iterable[str], canary_routes: Iterable[str] = (),
               required_receipt_roles: Iterable[str] = ()) -> ReadinessPlan:
    node_ids = tuple(str(n) for n in node_ids)
    return ReadinessPlan(
        deployment_id=deployment_id, generation=generation, plan_hash=plan_hash,
        expected_nodes=node_ids,
        expected_components=tuple(f"proxy@{n}" for n in node_ids),
        expected_replicas=dict(expected_replicas),
        expected_routes=tuple(routes),
        canary_routes=tuple(canary_routes),
        required_receipt_roles=tuple(required_receipt_roles))


def await_ready(coord: ReadinessCoordinator, *, node_id: str, instance_id: str,
                canary: Callable[[], tuple[bool, str]],
                routes: Iterable[str] = (),
                on_poll: Optional[Callable[[], None]] = None,
                timeout_s: float = 600.0, poll_s: float = 5.0,
                log: Callable[[str], None] = print) -> ReadinessSnapshot:
    """Poll live state until the predicate holds or the deadline expires.

    Observations refresh on EVERY pass, so a replica that dies between passes
    revokes readiness instead of latching it.
    """
    routes = tuple(routes)
    deadline = time.monotonic() + timeout_s
    last_report = 0.0
    while True:
        collect_observations(coord, node_id=node_id, instance_id=instance_id)
        if on_poll is not None:
            on_poll()
        # Canary only once the rest of the predicate holds, so we do not probe
        # the engine while replicas are still starting.
        pending = [b for b in coord.blockers() if not b.startswith("canary ")]
        if not pending and routes:
            ok, detail = canary()
            if not ok:
                log(f"[Readiness] canary not answering yet: {detail}")
        snapshot = coord.evaluate()
        if snapshot.ready:
            return snapshot
        now = time.monotonic()
        if now - last_report > 30.0:
            log(f"[Readiness] blocked on: {list(snapshot.blockers)[:4]}")
            last_report = now
        if now > deadline:
            return snapshot
        time.sleep(poll_s)


def observe_deployment(*, deployment_id: str, generation: int,
                       snapshot_dir: str = "", timeout_s: float = 1800.0,
                       poll_s: float = 5.0, log=print) -> dict:
    """Report what THIS process can see. Decide nothing (§3.2.1 Q3, IMP-B02).

    The deployment child used to be the readiness authority: it drained
    receipts, canaried its own internal Serve port, and printed the READY
    marker. That decided readiness against an endpoint no client uses, from a
    process that cannot see the gateway or the other ranks' sessions.

    The composition root owns that decision now. What survives here is the one
    thing this process is genuinely the best witness for -- whether its own
    Serve applications reached their replica targets -- written to a file whose
    name says it is evidence.
    """
    import ray

    deadline = time.monotonic() + timeout_s
    last_report = 0.0
    payload: dict = {}
    while True:
        reset_discovery_cache()
        apps = discover_applications(use_cache=False)
        per_app = {}
        running_all = bool(apps)
        for name, info in apps.items():
            target = max(int(info.get("target", 0) or 0), 0)
            running = int(info.get("running", 0) or 0)
            per_app[name] = {"running": running, "target": target,
                             "route_prefix": info.get("route_prefix", "/"),
                             "status": info.get("status", "")}
            if target <= 0 or running < target:
                running_all = False
        payload = {
            "schema_version": SCHEMA_VERSION,
            "deployment_id": deployment_id,
            "generation": generation,
            "applications": per_app,
            "applications_running": running_all,
            "node_ids": [str(n["NodeID"]) for n in ray.nodes() if n.get("Alive")],
            "observed_at": time.time(),
            "evidence_only": True,
        }
        write_evidence(payload, snapshot_dir)
        if running_all:
            log(f"[Deployment] evidence: {len(per_app)} application(s) at target")
            return payload
        now = time.monotonic()
        if now - last_report > 30.0:
            progress = {k: "{}/{}".format(v["running"], v["target"])
                        for k, v in per_app.items()}
            log(f"[Deployment] not at target yet: {progress}")
            last_report = now
        if now > deadline:
            log("[Deployment] evidence deadline reached without reaching target")
            return payload
        time.sleep(poll_s)


def write_evidence(payload: dict, directory: str) -> Optional[str]:
    """Persist the evidence file. Best-effort: the root blocks if it is absent."""
    if not directory:
        return None
    try:
        os.makedirs(directory, exist_ok=True)
        path = os.path.join(directory, EVIDENCE_FILENAME)
        tmp = f"{path}.tmp.{os.getpid()}"
        with open(tmp, "w") as fh:
            json.dump(payload, fh, indent=2, sort_keys=True)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        return path
    except OSError:
        return None
