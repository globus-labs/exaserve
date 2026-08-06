"""Live readiness authority for the Serve deployment (plan WP5, audit IMP-B02).

This is the *production* binding of :mod:`exaserve.control.readiness`. It turns
the current cluster into observations, runs a real inference canary through the
external route, and returns one authoritative snapshot.

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

The snapshot is written to ``readiness.json`` so consumers read a structured,
generation-tagged fact instead of parsing logs.
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

READINESS_FILENAME = "readiness.json"

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
    states = getattr(dep_status, "replica_states", None) or {}
    return sum(int(c) for s, c in states.items() if _state_name(s).upper() == "RUNNING")


def discover_applications() -> dict[str, dict]:
    """{app_name: {"route_prefix", "target", "running", "status"}} from live Serve.

    ``target`` is the *declared* replica count of the applied config.
    """
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
        path = os.path.join(directory, READINESS_FILENAME)
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


def _build_receipt_store(deployment_id: str, generation: int, log) -> tuple:
    """Head-side receipt store + the roles this topology can attest.

    Returns ``(store, required_roles)``. ``supervisor`` is required only when a
    supervisor actually stamped the environment — the legacy `exec bash` path
    has no supervisor to attest, and demanding a receipt nobody can issue would
    fail closed for a reason unrelated to compatibility.
    """
    import sys

    from ..compat.activator import CompatibilityActivator
    from ..compat.collector import collector_name as _collector_name
    from ..compat.collector import drain_receipts, receipt_from_dict

    activator = CompatibilityActivator(deployment_id=deployment_id,
                                       generation=generation)
    profile = activator.profile
    stamped = os.environ.get("EXASERVE_COMPAT_PROFILE_ID", "")
    roles = ["ray_head", "ray_worker", "replica", "engine"]
    if stamped:
        roles.insert(0, "supervisor")
        if stamped != profile.profile_id:
            log(f"[Readiness] WARNING: supervisor profile {stamped[:12]} != head "
                f"profile {profile.profile_id[:12]}")
    override = os.environ.get("EXASERVE_REQUIRED_RECEIPT_ROLES", "")
    if override:
        roles = [r.strip() for r in override.split(",") if r.strip()]

    from dataclasses import replace as _replace

    scoped = _replace(profile, required_roles=tuple(roles))
    object.__setattr__(scoped, "profile_id", profile.profile_id)

    from ..compat.receipt import ReceiptStore

    store = ReceiptStore(scoped, deployment_id, generation)

    # Ray daemons are unmodified external processes started with a prepared
    # environment: the head attests them (plan §3.1), it does not pretend they
    # self-report. The supervisor receipt is re-derived from its env stamp.
    ray_version = ""
    try:
        import ray as _ray

        ray_version = _ray.__version__
    except Exception:
        pass
    for role in ("supervisor", "ray_head", "ray_worker"):
        if role in roles:
            ok, why = store.add(activator.attest_external(
                role, executable=sys.executable,
                version_probe=f"ray {ray_version}"))
            if not ok:
                log(f"[Readiness] receipt for {role} rejected: {why}")

    drained = drain_receipts()
    if not drained:
        log("[Readiness] no receipts on the channel yet "
            f"(collector={_collector_name()})")
    for payload in drained:
        receipt = receipt_from_dict(payload)
        if receipt is None:
            continue
        ok, why = store.add(receipt)
        if not ok:
            log(f"[Readiness] receipt from {payload.get('role')} rejected: {why}")
    return store, roles


def enforce_readiness(*, deployment_id: str, generation: int, plan_hash: str,
                      base_url: str, snapshot_dir: str = "",
                      timeout_s: float = 600.0,
                      log=print) -> ReadinessSnapshot:
    """THE readiness gate. Raises unless the predicate holds (fail-closed).

    Replaces "we got through bring-up, so print CLUSTER FULLY READY". The
    marker is still printed for backward compatibility, but only *after* this
    returns ready, so the text can no longer outrun the fact.
    """
    import ray

    node_ids = [str(n["NodeID"]) for n in ray.nodes() if n.get("Alive")]
    apps = discover_applications()
    if not apps:
        raise RuntimeError("[Readiness] no Serve applications are deployed; "
                           "refusing to declare readiness.")
    expected_replicas = {name: max(int(i["target"]), 1) for name, i in apps.items()}
    routes = tuple(apps)

    store, roles = _build_receipt_store(deployment_id, generation, log)
    # Probe every route when there are few; sample deterministically when a
    # shard-aware deployment exposes hundreds. The sample is RECORDED, so the
    # snapshot never implies routes answered that were not probed.
    limit = int(os.environ.get("EXASERVE_CANARY_ROUTE_LIMIT", "8") or 8)
    canary_routes = sample_routes(sorted(apps), limit)
    plan = build_plan(
        deployment_id=deployment_id, generation=generation, plan_hash=plan_hash,
        node_ids=node_ids, expected_replicas=expected_replicas, routes=routes,
        canary_routes=canary_routes, required_receipt_roles=tuple(roles))
    coord = ReadinessCoordinator(plan, receipts=store)

    def _url_for(app: str) -> str:
        prefix = str(apps[app].get("route_prefix") or "/").rstrip("/")
        return f"{base_url.rstrip('/')}{prefix}/v1/completions"

    canary_url = _url_for(canary_routes[0]) if canary_routes else ""
    if len(canary_routes) < len(apps):
        log(f"[Readiness] canary SAMPLES {len(canary_routes)}/{len(apps)} routes "
            f"(EXASERVE_CANARY_ROUTE_LIMIT={limit}): {list(canary_routes)}")
    log(f"[Readiness] gate: {len(node_ids)} nodes, apps={expected_replicas}, "
        f"roles={roles}, canary={canary_url}")

    def _canary_all() -> tuple[bool, str]:
        """Probe each sampled route; every one must answer."""
        details = []
        all_ok = True
        for app in canary_routes:
            ok, detail = http_canary(_url_for(app))
            coord.set_canary(app, ok)
            all_ok = all_ok and ok
            if not ok:
                details.append(f"{app}: {detail}")
        return all_ok, "; ".join(details) if details else "all sampled routes answered"

    def _refresh_receipts() -> None:
        """Receipts arrive as replicas finish starting, so re-drain each pass."""
        from ..compat.collector import drain_receipts as _drain
        from ..compat.collector import receipt_from_dict as _parse

        for payload in _drain():
            receipt = _parse(payload)
            if receipt is not None:
                store.add(receipt)

    snapshot = await_ready(
        coord, node_id=(node_ids[0] if node_ids else "head"),
        instance_id=f"gen{generation}", routes=canary_routes,
        on_poll=_refresh_receipts, canary=_canary_all,
        timeout_s=timeout_s, log=log)

    path = write_snapshot(snapshot, snapshot_dir, extra={
        "deployment_id": deployment_id, "canary_url": canary_url,
        "canary_routes": list(canary_routes), "routes_total": len(apps),
        "required_roles": list(roles), "receipts": store.count(),
        "externally_attested_roles": store.externally_attested_roles(),
        "expected_replicas": expected_replicas,
        "degraded_discovery": any(a.get("degraded_source") for a in apps.values()),
    })
    if path:
        log(f"[Readiness] snapshot -> {path}")
    if not snapshot.ready:
        if os.environ.get("EXASERVE_ALLOW_DEGRADED_READINESS") == "1":
            log(f"[Readiness] WARNING: degraded start, blockers={list(snapshot.blockers)}")
            return snapshot
        raise RuntimeError(
            "[Readiness] refusing to declare the cluster ready; blockers: "
            f"{list(snapshot.blockers)}. Set EXASERVE_ALLOW_DEGRADED_READINESS=1 "
            "to start anyway.")
    log(f"[Readiness] READY — {list(snapshot.satisfied)}")
    return snapshot


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
