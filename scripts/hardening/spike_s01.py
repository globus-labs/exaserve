#!/usr/bin/env python3
"""S01 early-compute spike (plan §4.2): distributed ownership, authenticated
control channel, MPI exit contract, and node-local watchdog — on real
mpiexec/PALS across 1-2 nodes. No Ray, no engines: the long-lived child is a
placeholder process, because S01's verdict is about process/transport
topology, not serving.

Head mode (run on the lease's head node, inside the allocation):
    python spike_s01.py --mode head --ranks 2 --scenario clean|child-death|sup-death|conn-drop

The head binds the listener, exports the redacted environment (secret via env
only), launches `mpiexec -n N -ppn 1 python spike_s01.py --mode rank`, and
prints a JSON verdict. Exit 0 iff the scenario's invariants held.

Scenario invariants (AC-SUP-01 / AC-CTL-01 early slices):
  clean       all ranks register+observe+goodbye; launcher exit 0.
  child-death rank 1 SIGKILLs its child: typed FAILED observation with the
              first cause arrives; rank exits 21; launcher exit nonzero.
  sup-death   rank 1 supervisor os._exit(22) without goodbye: head sees the
              session drop; launcher exit nonzero.
  conn-drop   rank 1 drops the control connection but keeps running: its
              watchdog kills the local child and exits 24 within the grace
              deadline; launcher exit nonzero.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import socket
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "src"))

from exaserve.control.contracts import SCHEMA_VERSION, ComponentObservation  # noqa: E402
from exaserve.control.transport import (  # noqa: E402
    ControlListener,
    NodeChannel,
    new_deployment_secret,
)

ENV_PREFIX = "EXASERVE_SPIKE_"
GRACE_S = 5.0
CHILD_EXIT_RANK = 21
SUP_DEATH_CODE = 22
WATCHDOG_EXIT = 24


def _rank_from_env() -> int:
    for var in ("PALS_RANKID", "PMI_RANK", "PMIX_RANK", "SLURM_PROCID"):
        value = os.environ.get(var)
        if value is not None:
            return int(value)
    return 0


def _obs(rank: int, seq: int, state: str, reason: str | None = None,
         detail: str | None = None) -> ComponentObservation:
    return ComponentObservation(
        schema_version=SCHEMA_VERSION,
        deployment_id=os.environ[ENV_PREFIX + "DEP"],
        plan_hash=os.environ[ENV_PREFIX + "PLAN"],
        generation=int(os.environ[ENV_PREFIX + "GEN"]),
        component_id=f"placeholder-child-{rank}",
        instance_id="i0", sequence=seq, owner_scope="RANK", owner_rank=rank,
        role="placeholder-child", node_id=socket.gethostname(), state=state,
        observed_at=time.time(), reason_code=reason, detail=detail)


# --------------------------- rank (NodeSupervisor stand-in) ----------------

async def rank_main(scenario: str, duration: float, act_rank: int) -> int:
    rank = _rank_from_env()
    chan = NodeChannel(
        host=os.environ[ENV_PREFIX + "HOST"], port=int(os.environ[ENV_PREFIX + "PORT"]),
        secret=bytes.fromhex(os.environ[ENV_PREFIX + "SECRET"]),
        deployment_id=os.environ[ENV_PREFIX + "DEP"],
        plan_hash=os.environ[ENV_PREFIX + "PLAN"],
        generation=int(os.environ[ENV_PREFIX + "GEN"]),
        rank=rank, node_id=socket.gethostname())
    await chan.connect_and_register()

    if os.environ.get(ENV_PREFIX + "CHILD_SELFREAP") == "1":
        # Local (non-MPI) smokes: child exits when its parent dies so no
        # orphan outlives the harness. On compute this stays OFF so the
        # launcher's process-tree cleanup is actually measured.
        child_code = ("import os,time\np=os.getppid()\n"
                      "while os.getppid()==p: time.sleep(0.5)")
    else:
        child_code = "import time\nwhile True: time.sleep(0.5)"
    child = await asyncio.create_subprocess_exec(sys.executable, "-c", child_code)
    seq = 1
    await chan.send_observation(_obs(rank, seq, "RUNNING")); seq += 1

    acting = rank == act_rank and scenario != "clean"
    deadline = time.monotonic() + duration
    act_at = time.monotonic() + 3.0

    async def watchdog() -> None:
        await chan.wait_disconnected()
        # Lost control lease: bounded local cleanup, then nonzero exit.
        child.kill()
        await asyncio.wait_for(child.wait(), GRACE_S)
        os._exit(WATCHDOG_EXIT)

    watchdog_task = asyncio.create_task(watchdog())

    while time.monotonic() < deadline:
        if child.returncode is not None:
            # Unexpected long-lived child exit is fatal (plan WP4.7).
            await chan.send_observation(_obs(
                rank, seq, "FAILED", reason="CHILD_EXIT",
                detail=f"placeholder child exited rc={child.returncode}"))
            await chan.close()
            return CHILD_EXIT_RANK
        if acting and time.monotonic() >= act_at:
            acting = False
            if scenario == "child-death":
                child.kill()  # next loop iteration reports FAILED
            elif scenario == "sup-death":
                os._exit(SUP_DEATH_CODE)
            elif scenario == "conn-drop":
                chan.drop_connection()
                # watchdog fires via wait_disconnected -> exit 24
        try:
            await chan.send_heartbeat()
        except Exception:
            pass  # connection loss is handled by the watchdog
        await asyncio.sleep(0.5)

    watchdog_task.cancel()
    child.terminate()
    await child.wait()
    await chan.close()
    return 0


# --------------------------------- head ------------------------------------

async def head_main(ranks: int, scenario: str, duration: float, act_rank: int) -> int:
    received: list[tuple[int, ComponentObservation]] = []
    changes: list[tuple[int, bool, float]] = []
    secret = new_deployment_secret()
    dep = dict(deployment_id=f"spike-{int(time.time())}", plan_hash="s01", generation=1)

    listener = ControlListener(
        **dep, expected_ranks=ranks, secret=secret,
        on_observation=lambda r, o: received.append((r, o)),
        on_session_change=lambda r, up: changes.append((r, up, time.monotonic())))
    await listener.start()

    env = dict(os.environ)
    env.update({
        ENV_PREFIX + "HOST": socket.gethostname(),
        ENV_PREFIX + "PORT": str(listener.port),
        ENV_PREFIX + "SECRET": secret.hex(),  # env-only; never logged (plan §3.2)
        ENV_PREFIX + "DEP": dep["deployment_id"],
        ENV_PREFIX + "PLAN": dep["plan_hash"],
        ENV_PREFIX + "GEN": str(dep["generation"]),
    })
    launcher = os.environ.get("EXASERVE_MPILAUNCH", f"mpiexec -n {ranks} -ppn 1").split()
    cmd = launcher + [sys.executable, os.path.abspath(__file__),
                      "--mode", "rank", "--scenario", scenario,
                      "--duration", str(duration), "--act-rank", str(act_rank)]
    outdir = os.environ.get(ENV_PREFIX + "OUTDIR", ".")
    os.makedirs(outdir, exist_ok=True)
    rank_log = open(os.path.join(outdir, f"ranks_{scenario}.log"), "wb")
    print(f"[head] listener {socket.gethostname()}:{listener.port}; launching: {' '.join(cmd)}")
    t0 = time.monotonic()
    # Rank output goes to a file, never our pipe: an orphaned grandchild
    # holding stdout must not be able to wedge the harness.
    proc = await asyncio.create_subprocess_exec(
        *cmd, env=env, stdout=rank_log, stderr=rank_log)

    registered = await listener.wait_all_registered(60)
    reg_t = time.monotonic() - t0
    try:
        launcher_rc = await asyncio.wait_for(proc.wait(), duration + 60)
    except asyncio.TimeoutError:
        proc.kill()
        launcher_rc = -9
    total_t = time.monotonic() - t0
    await listener.stop()

    failed_obs = [(r, o) for r, o in received if o.state == "FAILED"]
    drops = [c for c in changes if not c[1]]
    verdict: dict[str, object] = {
        "scenario": scenario, "ranks": ranks,
        "all_registered": registered, "registration_s": round(reg_t, 2),
        "observations": len(received), "failed_observations":
            [(r, o.reason_code, o.detail) for r, o in failed_obs],
        "session_drops": [c[0] for c in drops],
        "launcher_exit": launcher_rc, "total_s": round(total_t, 2),
        "audit": [f"{a.reason}:{a.detail}" for a in listener.audit],
    }

    ok = registered
    if scenario == "clean":
        ok &= launcher_rc == 0 and not failed_obs
    elif scenario == "child-death":
        ok &= launcher_rc != 0 and any(o.reason_code == "CHILD_EXIT" for _, o in failed_obs)
    elif scenario == "sup-death":
        ok &= launcher_rc != 0 and act_rank in [c[0] for c in drops]
    elif scenario == "conn-drop":
        ok &= launcher_rc != 0 and act_rank in [c[0] for c in drops]
    verdict["pass"] = bool(ok)
    print(json.dumps(verdict, indent=2))
    return 0 if ok else 1


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["head", "rank"], default="head")
    parser.add_argument("--ranks", type=int, default=2)
    parser.add_argument("--scenario", default="clean",
                        choices=["clean", "child-death", "sup-death", "conn-drop"])
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--act-rank", type=int, default=1,
                        help="rank that performs the failure injection")
    args = parser.parse_args()
    if args.mode == "rank":
        raise SystemExit(asyncio.run(rank_main(args.scenario, args.duration, args.act_rank)))
    raise SystemExit(
        asyncio.run(head_main(args.ranks, args.scenario, args.duration, args.act_rank)))


if __name__ == "__main__":
    main()
