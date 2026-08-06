#!/usr/bin/env python3
"""S02 early-compute spike (plan §4.2): readiness authority via PUBLIC APIs.

Run on the head node of a 2-node lease AFTER a 2-node Ray cluster is up
(see spike_s02_launch.sh). Answers, with evidence, the ADR-002 ladder
question "are public APIs sufficient?" for each readiness conjunct at small
scale:

  Q1 exact node membership + resources ......... ray.nodes()/cluster_resources
  Q2 app/deployment/replica states .............. serve.status() (public)
  Q3 per-node proxy health ...................... serve.status().proxies
  Q4 route liveness on EVERY node ............... HTTP canary per node proxy
  Q5 replica death reflected (no silent READY) .. ray.util.state + kill + status
  Q6 stale/prior-generation confusion ........... redeploy + status identity

Exit 0 iff all probes produced usable public evidence; the JSON verdict
records per-question sufficiency for ADR-002.
"""

from __future__ import annotations

import json
import socket
import time
import urllib.request

VERDICT: dict[str, object] = {"host": socket.gethostname(), "questions": {}}


def q(name: str, ok: bool, detail: str) -> None:
    VERDICT["questions"][name] = {"ok": bool(ok), "detail": detail}
    print(f"[S02] {name}: {'OK' if ok else 'INSUFFICIENT'} — {detail}", flush=True)


# Canaries must go node-to-node directly: env_aurora exports http_proxy for
# internet access, and a proxied urllib turns every canary into a corporate-
# proxy 502 (observed in run 2; same class as the eval harness's known
# proxy-env-strip gotcha).
_DIRECT = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def http_ok(url: str, timeout: float = 10.0) -> tuple[bool, str]:
    try:
        with _DIRECT.open(url, timeout=timeout) as resp:
            return resp.status == 200, f"{resp.status} {resp.read()[:60]!r}"
    except Exception as exc:  # evidence, not control flow
        return False, repr(exc)


def main() -> int:
    import ray
    from ray import serve

    ray.init(address="auto", namespace="s02")
    t0 = time.monotonic()

    # ---- Q1: membership/resources (public) -------------------------------
    nodes = [n for n in ray.nodes() if n.get("Alive")]
    hosts = sorted(n.get("NodeManagerHostname", "?") for n in nodes)
    res = ray.cluster_resources()
    q("Q1_membership", len(nodes) == 2,
      f"alive={hosts} cpus={res.get('CPU')} (public ray.nodes/cluster_resources)")

    # ---- deploy a tiny 2-replica app (public API) -------------------------
    @serve.deployment(num_replicas=2, ray_actor_options={"num_cpus": 0.1})
    class Echo:
        async def __call__(self, request) -> str:
            return "ok"

    serve.start(proxy_location="EveryNode", http_options={"host": "0.0.0.0", "port": 8000})
    serve.run(Echo.bind(), name="s02app", route_prefix="/s02")
    deploy_s = time.monotonic() - t0

    # ---- Q2: application/deployment/replica states ------------------------
    status = serve.status()
    app = status.applications.get("s02app")
    replica_states: dict[str, int] = {}
    if app:
        for dname, dstatus in app.deployments.items():
            for state, count in dstatus.replica_states.items():
                replica_states[str(state)] = replica_states.get(str(state), 0) + count
    q("Q2_app_replica_states", bool(app) and replica_states.get("RUNNING", 0) == 2,
      f"app={getattr(app, 'status', None)} replicas={replica_states} deploy_s={deploy_s:.1f}")

    # ---- Q3: per-node proxy health (poll to deadline; record the gap) -----
    # First S02 run proved the point-in-time hazard: at app=RUNNING the
    # worker proxy was still STARTING and both routes 502'd. Readiness must
    # therefore conjoin proxy health + external canaries, each with its own
    # deadline. Here we measure time-to-true for both.
    t_q3 = None
    for _ in range(60):
        proxies = {str(k): str(v) for k, v in serve.status().proxies.items()}
        if len(proxies) == 2 and all("HEALTHY" in v for v in proxies.values()):
            t_q3 = round(time.monotonic() - t0, 1)
            break
        time.sleep(2)
    q("Q3_proxy_health", t_q3 is not None,
      f"proxies={proxies} healthy_at_s={t_q3} after app RUNNING at {deploy_s:.1f}s "
      "(public serve.status().proxies; NOT true at app-RUNNING instant)")

    # ---- Q4: per-node route canary (external HTTP, every node, polled) ----
    canaries: dict[str, object] = {}
    t_q4 = None
    for _ in range(60):
        canaries = {h: http_ok(f"http://{h}:8000/s02") for h in hosts}
        if all(ok for ok, _ in canaries.values()):
            t_q4 = round(time.monotonic() - t0, 1)
            break
        time.sleep(2)
    q("Q4_route_canary_every_node", t_q4 is not None,
      f"routes_200_at_s={t_q4}; last={ {h: d for h, (o, d) in canaries.items()} } "
      "(both nodes 502'd at app-RUNNING instant in run 1)")

    # ---- Q5: replica death is visible; READY is not silently retained -----
    # Run 1 fact: ray.util.state needs the dashboard API server (:8265),
    # which is absent on this stack -> that ladder rung is NOT dependable
    # here. Use an OS-level kill of a replica worker process instead (also
    # more faithful to real failure).
    import subprocess
    pids: list[str] = []
    for host in hosts:
        cmd = ["pgrep", "-f", "ServeReplica.*Echo"]
        if host != socket.gethostname():
            cmd = ["ssh", host] + cmd
        out = subprocess.run(cmd, capture_output=True, text=True)
        found = [p for p in out.stdout.split() if p.strip().isdigit()]
        if found:
            pids = [(host, found[0])]
            break
    detail5 = f"replica worker pids found={pids} (pgrep; dashboard state API unavailable)"
    ok5 = False
    if pids:
        host, pid = pids[0]
        kill_cmd = ["kill", "-9", pid]
        if host != socket.gethostname():
            kill_cmd = ["ssh", host] + kill_cmd
        subprocess.run(kill_cmd, capture_output=True)
        # Poll public status for the dip below target replicas. Run 3 fact:
        # controller logged ActorDiedError ~6s after SIGKILL, but public
        # status held RUNNING:2 for the full 30s window — measure the real
        # detection latency (default health check: 10s period x 3 misses).
        t_kill = time.monotonic()
        saw_dip = False
        for _ in range(120):
            time.sleep(1)
            app = serve.status().applications.get("s02app")
            counts = {}
            for dstatus in app.deployments.values():
                for state, count in dstatus.replica_states.items():
                    counts[str(state)] = counts.get(str(state), 0) + count
            app_running = "RUNNING" in str(app.status)  # enum str is qualified
            if counts.get("RUNNING", 0) < 2 or not app_running:
                saw_dip = True
                detail5 += (f"; dip observed after {time.monotonic() - t_kill:.0f}s: "
                            f"app={app.status} counts={counts}")
                break
        ok5 = saw_dip
        if not saw_dip:
            detail5 += "; NO dip in public status within 120s of SIGKILL"
    q("Q5_replica_death_visible", ok5, detail5)

    # ---- Q6: redeploy generation identity ---------------------------------
    serve.run(Echo.options(num_replicas=1).bind(), name="s02app", route_prefix="/s02")
    app = serve.status().applications.get("s02app")
    # Public status has last_deployed_time_s — usable as a generation proxy?
    gen_field = getattr(app, "last_deployed_time_s", None)
    q("Q6_generation_identity", gen_field is not None,
      f"last_deployed_time_s={gen_field} (public; NOT a plan-hash-scoped "
      "generation — ExaServe must own generation identity itself)")

    serve.shutdown()
    all_ok = all(v["ok"] for v in VERDICT["questions"].values())
    VERDICT["pass"] = all_ok
    VERDICT["elapsed_s"] = round(time.monotonic() - t0, 1)
    print(json.dumps(VERDICT, indent=2, default=str))
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
