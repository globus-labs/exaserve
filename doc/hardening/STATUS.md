# ExaServe Production-Hardening — Status

**Updated:** 2026-08-06, after the implementation audit
(`doc/PRODUCTION_HARDENING_IMPLEMENTATION_AUDIT.md`) of commit `e73f3eb`.
Companion to `MIGRATION_LOG.md` (chronological) and `FINDINGS.yaml`
(authoritative ledger, 82 records).

## Headline — read this first

**NOT production ready. The target architecture is not yet wired into the
production path.**

An earlier version of this file claimed "34 of 35 audit findings fixed" and
that no open item was a production blocker. **That claim was wrong** and has
been withdrawn. The work in `e73f3eb` is genuine and useful, but it is
P00/P01-class foundations plus incremental repairs to the *legacy* path — not
the WP4/WP5/WP13 cutover that owns the blocker invariants.

Current ledger (YAML-parsed, not regex-counted — the previous count was also
wrong):

| Status | All records (82) | Audit findings (35) |
|---|---|---|
| FIXED | 30 | 15 |
| IN_PROGRESS | 28 | 20 |
| OPEN | 22 | 0 |
| OUT_OF_PRODUCTION_SCOPE | 2 | 0 |

(Counts are YAML-parsed from `FINDINGS.yaml`, not regex-counted. Pass 3 moved
**PR-008 to FIXED** on the on-hardware evidence below, and pass 4 moved
**PR-009 to FIXED** (per-rank ownership) and **PR-001 to FIXED** (two
independent nonzero-producing failure signals). **KI-D1 stays
IN_PROGRESS**: the mechanism is closed and validated at 2 and 16 nodes, but the
original false-ready symptom was observed at **256n** and has not been re-run
there — the ledger and `KNOWN_ISSUES.md` agree on that wording deliberately.)

A record is `FIXED` only when its invariant holds **on the path a production
deployment actually takes**. Anything owned by the un-cut-over architecture is
`IN_PROGRESS` with the narrow completed slice named in its `evidence` field.

## What is genuinely complete (narrow, verified)

- Source config copied before runtime head-IP mutation (PR-003 slice).
- Derived model-identity collision checks (PR-007).
- AST-restricted matrix expressions (PR-016), with escape-attempt tests.
- Required-model default placement failure (PR-023).
- HAProxy admin lockdown + `haproxy -c` / `nginx -t` preflight (PR-024 slice).
- ClientLab `faults` default + argv-vector SSH (PR-030).
- Spec enum/bound validation (PR-020); benchmark-gateway marking (PR-025).
- Scheduler/eval job-body shell quoting **after** the IMP-B09 fix below.
- Atomic single-writer publication helpers; lifecycle enums/transition tables.

## Defects found by the audit and now fixed (this pass)

Each has a regression test in `tests/test_audit_regressions.py` (17 tests):

| ID | Defect | Fix |
|---|---|---|
| IMP-B09 | **Self-inflicted shell injection**: my PR-015 "fix" pasted `shlex.quote` output *inside* double quotes, where single quotes lose their power — `$(cmd)` in a path executed | assign to a shell var (single-quoted), reference as `"$var"`; regression test renders `$()`/backtick payloads |
| IMP-B05 | completion marker trusted by existence; deleting weights still read "complete"; tokenizer-only download wrote a full-model marker | marker inventory verified against disk (name+size); `kind` recorded; tokenizer-only never certifies a model |
| IMP-B07 | lease takeover unfenced (stale holder's `release()` deleted the successor's live lease); two stealers could both win; status CAS had an ABA hole; a record could initialize directly as READY | per-acquisition fencing token + `holds_lease()`/`renew()`; O_EXCL arbitration for takeover; `expected_revision` CAS; initial states restricted to PLANNED |
| IMP-B06 | an authenticated rank could forge another rank's or a **GLOBAL** observation; malformed observations were skipped not fail-closed; `all_registered` stayed true after disconnect; unbounded listener state | observation identity bound to the authenticated session (scope/rank/node); fail-closed termination; registration cleared on disconnect; bounded dedup/audit; per-component sequence enforcement; heartbeat recorded as a lease timestamp |
| IMP-H01 | `plan_hash` included `source_path` (same intent → different identity); frozen plan held caller-owned mutable dicts (content changed, hash didn't); `nan` accepted; `reservation_topology: false` bypassed node-agreement; top-level `envelope` ignored | hash covers semantic intent only; deep-freeze of nested options; non-finite rejected; reservation topology must be a non-empty string; `envelope` block honored |
| IMP-H04 | list/scalar bodies escaped as `AttributeError`/`TypeError` (500 not 400); `"false"` was truthy for `stream`/`ignore_eos`/HAProxy options | `require_object_body()` + typed model check + `strict_flag()` wired into both handlers and HAProxy options |
| IMP-B08 | partial replay / missing rank shards / failed required stats still wrote `succeeded` | incomplete runs are written as `partial` with reasons; `_is_completed` treats partial as needing human triage, not resubmission |
| IMP-H04b | `serve_url` looked for `proxy_out/proxy_port` beside the **source** config while the launcher writes it beside the run-scoped runtime config | search run-log tree (newest first), then legacy location |
| IMP-H07 | randomized CI job used `pytest -p randomly` without declaring `pytest-randomly` | added to `[dev]` extra |
| IMP-B10 | ledger recorded labels, not demonstrated closure; record count itself was wrong | 23 audit findings reopened as IN_PROGRESS; all 82 records now carry required plan §8 fields; counts YAML-parsed |

**Suite at that pass: 123 passed / 0 failed.**


## Pass 3 (2026-08-06): the architecture reaches the production path

The blocker in the previous section was that the target architecture existed
but nothing on a real deployment's path used it. That is no longer true for
readiness, supervision, and compatibility.

### What a deployment now does that it did not before

1. **Readiness is a predicate, not a print.** `server.py` calls
   `control.serve_readiness.enforce_readiness()` before the
   `CLUSTER FULLY READY` marker. It requires exact node membership, a healthy
   proxy per node, `target_num_replicas` running per application, app status
   RUNNING, a **real completion through the external route**, and a
   compatibility receipt from every required role. Unsatisfied → the process
   fails closed with each blocker named.
2. **Readiness is revocable.** Observations refresh every poll and replica sets
   are absolute, so a replica that dies lowers the count below target and
   readiness goes back off. The old marker could only ever latch.
3. **Consumers read a fact, not text.** The gate writes `readiness.json`;
   `eval/lib/backends/base.py` consumes it, and a snapshot that says *not
   ready* **overrides** the stdout marker. The marker path survives only as a
   logged fallback for pre-gate backends.
4. **The launch is supervised.** `cli.launch_cluster` hands off to
   `RuntimeSupervisor` (signals installed before any child, own process group,
   unexpected exit-0 is fatal, first cause preserved, typed exit code).
5. **Compatibility is attested per role.** Replicas self-attest with
   sentinel-proved patches; Ray daemons and the engine core are attested by
   their owner. Receipts flow over a named Ray actor and gate READY.
6. **Staging is generation-isolated.** `/tmp/exaserve_src.<generation>` with an
   atomic symlink publish, so a run cannot import a previous run's deleted
   modules.

### Evidence

- Unit/contract suite: **182 passed / 0 failed** (`tests/`, `eval/tests/`,
  `clientlab/tests/`); ruff correctness gate (E9,F63,F7,F82,F401,F841) clean on
  all touched modules.
- On-hardware 2-node runs on Aurora drove every fix below; each failure was
  found by running the gate on a real cluster, not by inspection.

### Defects this pass found in its OWN new code

| Defect | Why it mattered |
|---|---|
| Atomic staging publish could not replace a pre-existing real directory | first live run failed staging outright (`mv: cannot overwrite directory`) |
| `ComponentObservation` built without its mandatory §3.1 fields | the live collector would have raised `TypeError` on first use; caught by a unit test before it ran |
| `get_serve_details()` returns a **dict**, not a model | attribute-only access silently fell back to a path where target == running, so a dead replica could not revoke readiness |
| SC-11 sentinel hidden behind a `staticmethod` wrapper | a correctly patched replica was judged half-patched; **all 24 replicas** refused to publish and the gate blamed the wrong subsystem |
| receipt-channel identity depended on ambient env; collector name normalized differently from the head | receipts were unroutable in principle whenever `PBS_JOBID` was the id source |
| external attestation had no evidence class | an unmodified daemon can never prove an in-process sentinel, so `engine` could never satisfy a profile that demanded one |
| one canary result was applied to **every** route | claimed routes had answered that were never probed |

The recurring lesson, and the one worth carrying forward: **a fail-closed check
is only as strong as the evidence it can actually see.** Three separate times
this pass, a check that could not observe a correct state reported a healthy
cluster as broken and named the wrong cause. Fail-closed is right; blind is
not.

## Pass 4 (2026-08-06): ownership becomes structural

- **The S01 topology is real.** `RuntimeSupervisor` owns exactly one
  `RankLauncher` (one mpiexec/srun); each rank runs a `NodeSupervisor` owning
  that node's children. Because the head holds exactly one PID, there is no
  path by which it *could* signal a remote one — the invariant is structural,
  not a rule. `NodeSupervisor` enforces the same from the other side: it
  refuses to adopt a component that already has a process.
- **`launch_cluster.sh` is a site adapter.** Environment and preflight stay in
  the shell; the run is handed to `exaserve.supervisor_main`, which decides the
  launch, owns it, decides terminal state, and produces the exit code.
- **`DeploymentManager`** gives the lifecycle the five WP4.1 operations with
  typed exceptions, a state machine that refuses illegal transitions, and a
  revocable READY.
- **Engine self-attestation (EN-01) closed** — the engine writes its own
  receipt from inside the engine process.

Verified on 2 Aurora nodes through the full new stack: `[supervisor] owning 1
rank launcher over 2 node(s)`, `[Deployment] state=READY`, and
gate_ready / marker_never_precedes_gate / receipts / canary / tree_reaped /
engine_self_attested **all PASS**; supervisor exits 143 with the tree reaped
(17 named processes → 0). Suite: **219 passed / 0 failed**.

### Closed in this pass

- **`server.py` is a callable entry point.** The `__main__` block became
  `main()`, which also fixed a latent scope bug: as a block, `for app in
  built_apps:` silently rebound the module-level FastAPI `app` at import time.
- **The control channel's second failure signal is wired.** The head binds an
  ephemeral port before rank launch and hands ranks the address and secret;
  ranks register and report their own lifecycle. A fatal rank observation ends
  the run immediately rather than waiting for the launch to unwind, and a lost
  lease counts as that rank's failure. Verified across nodes on Aurora
  (`Rank 0`/`Rank 1 registered on the control channel`, rank 1 remote over HSN).

### What still remains (honest scope)

- **Scale evidence.** The gate is validated at 2 and 16 nodes; KI-D1's original
  256n symptom has not been re-run at that scale (held at the user's request).
- **`cli.server()` still re-execs** rather than calling `main()` in-process:
  compatibility patches must be applied before Ray/vLLM import and the calling
  interpreter may already have imported them. This is the WP4.6 fallback,
  taken knowingly rather than by omission.
- **Command dispatch over the channel** (head → rank commands, snapshots on
  reconnect) is not implemented; ranks currently publish and the head consumes.
- Legacy switches (`EXASERVE_READINESS_GATE=0`, `EXASERVE_USE_SUPERVISOR=0`,
  `EXASERVE_PYTHON_RANK_LAUNCH=0`, `EXASERVE_ALLOW_DEGRADED_READINESS=1`) are
  present by design and are removed at the WP13 cutover.

### On-hardware verdict (2 nodes, Aurora, 2026-08-06)

`scripts/hardening/run_supervisor_smoke.sh`, 8B direct mode, 24 replicas
across 2 nodes, driven through the packaged CLI (`exaserve.cli.launch_cluster`
→ `RuntimeSupervisor` → `launch_cluster.sh`):

| Check | Result |
|---|---|
| `gate_ready` — the predicate, not the marker | **PASS** |
| `marker_never_precedes_gate` | **PASS** |
| `receipts` from all required roles | **PASS** |
| `canary` — real completion via the external route | **PASS** (`" Paris, located in the north-central part"`) |
| `tree_reaped` on SIGTERM | **PASS** (process group 4→0, named ExaServe/Ray processes 17→0) |
| supervisor exit code | 143 (typed SIGTERM) |

Snapshot recorded by the gate:

```
satisfied: membership: 2 nodes | components: 2 healthy |
           model default: 24/24 replicas | routes: 1 healthy |
           canaries: 1/1 routes answered | receipts: all required roles attested
```

### Weak-scaling through the gate

| Nodes | Source of READY | Aggregate RPS | Per-node RPS | Errors | p50 / p99 |
|---|---|---|---|---|---|
| 2 (new path) | snapshot | 43.3 | **21.66** | 0 | 1.459 / 1.607 s |
| 16 (new path) | snapshot | 345.3 | **21.58** | 0 | 1.466 / 1.576 s |
| 16 (baseline, pre-gate) | marker | 344.4 | 21.53 | 0 | 1.465 / 1.593 s |
| 64 (baseline, pre-gate) | marker | 1373.9 | 21.47 | 0 | 1.469 / 1.585 s |

Per-node RPS is flat at 21.5–21.7 across 2→64 nodes and 16-node throughput
through the gate matches its pre-gate baseline within noise (345.3 vs 344.4,
zero errors both). The readiness gate, the per-role receipt collection, and
generation-isolated staging cost nothing measurable in throughput or latency.
Readiness also arrives no later than the marker did (16n: 240s via snapshot vs
260s via marker), so the gate is not a bring-up tax.

At 16 nodes the gate accounted for 192/192 replicas, 16 healthy proxies, an
answered canary, and receipts from all four roles required on that path
(`supervisor` is correctly not demanded when nothing stamped the environment).
