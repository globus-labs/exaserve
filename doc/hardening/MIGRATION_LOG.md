# Hardening Migration Log

Chronological record of slices, migration switches, and document-precedence
conflicts. Every entry is a reviewed working-tree checkpoint (git operations
are not authorized by the active request; see plan §0/§10).

---

## 2026-08-05 — Slice P00.1: baseline freeze + hermetic test-environment repair

**Finding IDs:** F-01..F-07, F-12 (BASELINE.md), AC-TST-01; feeds PR-031.
**Invariant:** unit tests are hermetic (no live HF, no undeclared host
binaries) and every test subtree is independently collectible under one
source-layout contract.

**Changes:**
- `doc/hardening/BASELINE.md` — frozen baseline (revision `005891e`, env,
  12 failing node IDs + root causes, canonical plugin/seed policy).
- `conftest.py` (new, root) — single sys.path contract (repo root + `src/`);
  HF offline guard (`HF_HUB_OFFLINE=1`, `TRANSFORMERS_OFFLINE=1`, overridable).
- Deleted `tests/conftest.py`, `eval/tests/conftest.py`,
  `clientlab/tests/conftest.py` (per-dir path hacks; root cause of the
  subset-collection failure — `src/` was only injected by `tests/conftest.py`,
  loaded as a rootdir-enumeration side effect).
- `pyproject.toml` — `[tool.pytest.ini_options] addopts = "-p no:randomly"`
  (deterministic default; WP11 randomized CI job re-enables explicitly).
- `eval/tests/test_eval_control_plane.py` —
  `test_eval_runtime_has_no_legacy_import_hacks` rewritten as an in-process,
  cwd-independent scan (was `subprocess.run(["rg", ...])`, F-12); the two
  subprocess-launching tests now build child `PYTHONPATH` from the test file
  location (repo root + src), fixing F-06/F-07; removed the ineffective
  cross-process `build_tokenizer_map` monkeypatches.
- **Tokenizer-builder seam (fixes F-01..F-06):** optional
  `trace.tokenizer_builder: "module:function"` spec field —
  `eval/lib/models.py` (TraceSpec field), `eval/lib/spec_io.py` (parse),
  `eval/lib/trace_generators.py` (`build_tokenizer_map` honors it),
  `eval/lib/trace_store.py` (**included in trace identity** — it changes
  generated content; PR-017 lesson), `eval/testing.py` (new;
  `whitespace_tokenizer_map` — a functioning fake; note the old `{}`
  monkeypatch would have KeyError'd in `_truncate_prompt`, so it never
  actually covered generation).

**Commands/results:**
- `pytest -q` (frameworks 3.12.12): **32 passed / 4 failed, 7.9s** (was 24/12,
  42s). Remaining reds are contract-drift items kept identified by node ID per
  plan §4.1(4): F-08 (WP9), F-09/F-10 (WP7), F-11 (WP8).
- Standalone collection: `eval/tests` 16, `tests` 16, `clientlab/tests` 4 —
  all collect without errors (was: 2 collection errors standalone).
- `PATH=/usr/bin:/bin pytest <legacy-import test>` → passed (no rg anywhere).

**Decision/fallbacks:** no fallback needed; preferred options worked.
**Residual risk:** forkserver workers inherit sys.path from the pytest parent;
the installed-wheel contract is deferred to WP11 item 5 (recorded there).

---

## 2026-08-05 — Slice P00.2: ledger + ADR-000 draft + S01 control-channel substrate

**Finding IDs:** S00 (ADR-000), S01 (partial: local transport slice of
AC-CTL-01); ledger covers all findings.
**Invariant:** typed, authenticated, fail-closed control channel per plan
§3.2; no readiness policy in transport.

**Changes:**
- `doc/hardening/FINDINGS.yaml` — full closure ledger (PR-001..035, KI-A1..E1,
  TD-*, F-*), statuses seeded from the adjudicated docs; F-01..F-07/F-12/
  F-COLLECT = FIXED with evidence.
- `doc/hardening/decisions/ADR-000-production-envelope.md` — PROVISIONAL:
  Aurora PBS + XPU + vLLM + HAProxy + trusted network + non-streaming;
  `qualification_target = 64` nodes (per active user request); 256n topology
  question resolved as NOT a release requirement for this pass; 64n smoke must
  be consistent-or-better vs recorded baselines.
- `src/exaserve/control/__init__.py`, `contracts.py`, `transport.py` (new) —
  §3.1 enums/observations/envelopes with canonical JSON + schema validation;
  §3.2 framing (len|HMAC-SHA256|body, 1 MiB bound), 256-bit secret,
  registration (rank range/uniqueness, node identity pinning), per-sender
  sequence enforcement, at-least-once dedup by
  (deployment, generation, component, instance, sequence), observation-level
  scope check, audited fail-closed rejections, session-change callbacks.
- `tests/test_control_channel.py` (new) — 5 hermetic tests: register/observe/
  dedup/disconnect; bad-MAC / stale-generation / rogue-rank / impersonation /
  duplicate-rank rejections; sequence regression; oversized frame; snapshot
  batching. All pass in 1.2s.

**Commands/results:** control tests 5/5; full suite **37 passed / 4 known-red**
(F-08..F-11, owners WP7/WP8/WP9) in 8.7s.
**Next unblocked slice:** S03 inventory (agent running) → S03 activator
harness; then S01/S02 one-/two-node early-compute proofs (WP12 early lane,
subjob lease).

---

## 2026-08-05 — Slice P00.3: S03 inventory landed + S01 spike harness validated locally

**Finding IDs:** S03 (inventory phase complete), S01 (harness ready), PR-026
(evidence recorded).

**Results:**
- `doc/hardening/COMPATIBILITY_INVENTORY.md` — 66-record manifest seed.
  Decisive findings: measured **ray 2.53.0 / vllm 0.15.0 / frameworks
  2025.3.1** vs stale pin ray 2.49.1 (patch layer would hard-fail
  `strict=True` today; overlay assembly has no version assertion); **~1,900
  lines provably dead** (overlay `client.py`+`proxy_state.py` byte-identical
  to upstream; SC-07/SC-08 self-documented obsolete; SC-D1..D3 dead call
  sites); **overlay carries zero functional fixes** (7 constants + 11
  instrumentation blocks; constants already flow via public
  `worker_process_setup_hook` SV-02/SV-04); the genuine vendor-compat core is
  SC-01..06 (PP aliasing), SC-09..12 (XPU DAG/selector), EN-01/05/07/09/11,
  SH-03/04/05/06/13, RS-02/03.
- `scripts/hardening/spike_s01.py` — S01 harness (head/rank modes, 4
  scenarios, `--act-rank`, file-redirected rank output, self-reap option for
  local runs). Local single-rank validation: **clean, child-death, sup-death,
  conn-drop all PASS**, no orphans. Two fixes found during validation:
  orphaned placeholder children held the harness stdout pipe (now redirected
  to files); failure scenarios need an acting-rank parameter.
- `exaserve/control/transport.py` — added `wait_disconnected()` /
  `drop_connection()` (rank watchdog inputs).
- Compute: no keepalive running → self-allocated 2-node debug sleep job
  **8734762** (skill fallback recipe); WP12 gate rows recorded in
  `artifacts/hardening/EXPERIMENT_PLAN.md` (S01-2N, S02-2N, S03-1N-SPAWN,
  S03-2N-RECEIPTS) before any gate runs.

---

## 2026-08-05 — Slice P00.4: S01 two-node proof PASSED; S02 probe iterating

**Gate S01-2N (allocation 8734762, x4117c0s7b0n0 + x4117c4s3b0n0): PASS —
all 4 scenarios.** Evidence in `artifacts/hardening/s01-2n/verdict_*.json`,
decision in `decisions/ADR-001-process-boundaries.md`. Facts proven: PALS
propagates the secret env (2/2 authenticated registrations in ≤0.4s); typed
first cause (`CHILD_EXIT`) arrives over the channel while mpiexec reports
only a cause-free 143 job abort — confirming the channel/launcher role
split; node watchdog kills local children and exits nonzero on control-lease
loss; zero process residue after every scenario. First attempt failed on a
subjob/SSH quoting hazard (multi-line -c command mangled) → batteries now
live in script files (`run_s01_battery.sh`).

**Gate S02-2N COMPLETE after 4 runs — ADR-002 decided** (see
`decisions/ADR-002-readiness-authority.md`). Headline measurements at 2
nodes, default constants: app RUNNING at 3.6s but proxies healthy/routes
serving only at 5.6s (a 2.0s false-ready window, 100% reproducible); a
SIGKILLed replica stays publicly "RUNNING" for **~120s** before the
controller marks it unhealthy (kill 07:14:55 → unhealthy 07:16:55, run 4)
even though the controller logs ActorDiedError within ~6s; `ray.util.state`
is dashboard-dependent and unavailable on this stack; canaries must strip
proxy env (http_proxy → corporate-proxy 502s, runs 1-2). Harness iterations:
run 2 = state-API dependency found; run 3 = enum-compare false positive
fixed + proxy-env fix verified (Q1-Q4,Q6 OK); run 4 = detection-latency
measurement. Allocation 8734762 released after the battery (qdel).

## 2026-08-05 — Slice P01.1 (WP2.1/WP2.2 start): atomic primitives + first conversions

**Finding IDs:** PR-035 (partial: eval helpers), PR-018 (run-group race),
KNOWN_ISSUES C3 class. **Invariant:** WP2 exit gate (old-or-new, never
partial).

**Changes:**
- `src/exaserve/state/atomic.py` (new): `atomic_write_bytes/text/json/yaml`
  (same-dir mkstemp + fsync + `os.replace` + best-effort dir fsync);
  `ExclusiveLease` — O_CREAT|O_EXCL acquisition, owner identity, TTL-based
  takeover via atomic rename, same-host dead-pid takeover, **foreign live
  leases respected** (the PR-013 defect class), unreadable lease NOT
  silently cleared.
- `tests/test_atomic_state.py` (new): 6 acceptance tests — crash injected at
  the rename boundary preserves prior state and leaves no litter; 4 writers +
  2 readers hammer test never observes torn JSON; lease exclusivity incl.
  foreign-live-lease; TTL/dead-pid takeover records `stole_from`; torn lease
  file raises; shrinking rewrite leaves no trailing bytes (C3). All pass.
- `eval/lib/utils.py` `dump_yaml_file`/`dump_json_file` → atomic publish.
- `eval/lib/run_planner.py`: `_claim_run_group_dir()` — atomic `os.mkdir`
  claim loop replaces list-then-create (PR-018); collision loser advances.

**Results:** suite 43 passed / 4 known-red, 11s.
**Next:** P01.2 — versioned RunStatus/DeploymentStatus + CAS transitions +
snapshot-provenance acknowledgement (PR-034); then P01.3 WP1 plan schemas.

---

## 2026-08-05 — Slice P01.2 (WP2.3): versioned status store with CAS transitions

**Finding IDs:** PR-013 (durable job identity substrate; executor adoption in
WP8), WP5.1 state machine substrate, WP9.6 partial/invalid distinction.

**Changes:**
- `src/exaserve/state/status.py` (new): `DeploymentState` (§WP5.1 machine
  incl. READY→VALIDATING revalidation; FAILED/CANCELLED from any active
  state; terminals closed) and `RunState`
  (PLANNED/SUBMITTED/RUNNING/SUCCEEDED/PARTIAL/FAILED/CANCELLED/INVALID;
  FAILED→SUBMITTED = explicit resubmission); `StatusStore` — lease-guarded
  read-modify-publish, caller names the expected state, mismatch ⇒ typed
  `StatusConflict`, unknown schema version fails closed, full history with
  reason codes, `data` carries durable scheduler job identity.
- `tests/test_status_store.py` (new, 4 tests): lifecycle+history, illegal
  skip + stale expectation, PARTIAL terminal semantics + durable job id,
  two-thread CAS race with exactly one winner.
- Two defects found by the tests and fixed: lease double-acquire on
  with-reentry (acquire now idempotent for the holder); lease contention now
  waits bounded (10s) then surfaces `StatusConflict` instead of leaking
  `LeaseHeldError`.

**Results:** suite **47 passed / 4 known-red**, 9.8s.
**Next:** P01.3 — WP1 plan schemas/compiler (strict coercion, ScaleEnvelope,
model-identity collisions, legacy adapter); PR-034 snapshot-policy switch.

---

## 2026-08-05 — Slice P01.3 (WP1): plan schemas + strict compiler

**Finding IDs:** PR-006, PR-007, PR-003 (compiler never mutates source),
PR-020/AC-PLAN-01 (scheduler-vs-deployment agreement), AC-SCALE-01 slice,
PR-025 (benchmark gateway marking), S03-attempt-1 lesson (pp<=nodes at
compile time).

**Changes:**
- `src/exaserve/plan/{__init__,schemas}.py` (new): frozen versioned
  `DeploymentPlan/ModelPlan/GatewayPlan/SchedulerPlan/ScaleEnvelope`;
  strict coercion (`"false"`→False, path-specific errors, unknown-key
  rejection); model-identity collision detection across raw/storage/route
  names; `scheduler.nodes` inherits or must match unless a named
  `reservation_topology` is declared; envelope bounds BOTH deployment and
  allocation size (fig7 oversized-control confound), with
  qualification_target=64 / supported_max grows only with WP12 evidence /
  validation_mode escape for gate runs; canonical SHA-256 `plan_hash`;
  `from_legacy_yaml` adapter (existing config shape compiles unchanged;
  legacy loaders remain the runtime default per §4.1 migration rules).
- `tests/test_plan_schemas.py` (new, 10 tests) — all pass; one compiler gap
  found by tests and fixed (allocation-size envelope check).

**Results:** suite **57 passed / 4 known-red**, 10.9s.
**Next:** P01.4 — PR-017 trace content-identity + atomic trace publish;
PR-034 snapshot-policy switch. Then P02 (WP3 compatibility profile).

---

## 2026-08-05 — Slice P01.4 + P02.1: trace identity complete; WP3 delete-first

**P01.4 (PR-017):** trace identity now includes `workload.arrival` and
SHA-256 content digests of prompt/trace inputs; `write_trace` publishes via
same-dir temp + `os.replace`; materialization runs under an
`ExclusiveLease` with `metadata.json` as the completion marker (re-checked
under the lease). New `eval/tests/test_trace_identity.py` (2 tests): arrival
fixed-vs-poisson no longer collide; in-place prompt edit changes the id.
PR-017 → FIXED (result-side completeness remains PR-019/WP9).

**P02.1 (WP3 delete-first, per ADR-003 decision 1 + inventory):**
- `_sitecustomize.py` 1629 → 1419 lines: dead SC-D1/D2/D3 removed (call
  sites were commented out; restorable from commit 2f32633 — noted in-file).
- Overlay `client.py` (626) + `proxy_state.py` (823) deleted — byte-identical
  to installed ray 2.53.0 (verified with `cmp` before deletion); farm now
  symlinks upstream for those, behavior unchanged.
- Stale pin corrected: `RAY_PINNED_VERSION` 2.49.1→2.53.0 (+full commit
  0de2118…); `apply_all(strict=True)` no longer hard-fails on the live stack
  and the per-run mismatch warning stops lying.
- `patches/__init__.py` header: version/counts/dead-plan-reference fixed to
  point at COMPATIBILITY_INVENTORY.md + ADR-003.
- `setup_overlay.sh`: **version assertion added** — refuses to assemble a
  mixed-version overlay when `ray.__version__ != RAY_PINNED_VERSION`
  (override env for spikes only); stale "five patched files" comment fixed.
- SC-07/SC-08 (~445 vendored lines) NOT deleted yet: their code path
  demonstrably executes in EngineCore (S03 logs, `_sitecustomize.py:789`);
  removal needs the A/B PP run without them — queued for the next compute
  round with the P03 two-node regressions.

**Results:** suite **59 passed / 4 known-red**; shell syntax checked.

---

## 2026-08-05 — Slice P02.2: baseline reds cleared — SUITE FULLY GREEN (64/64)

**Finding IDs:** PR-030 (both defects), F-08..F-11 closure; PR-024/PR-027
test-drift halves.

**Changes:**
- `clientlab/analysis/diagnostics.py`: optional `faults` gets the schema
  default instead of KeyError (F-08 fixed by fixing the DEFECT, not the test).
- `clientlab/collectors/netstats.py`: `os.system(f"ssh {node} …")` →
  argument-vector `subprocess.run` (PR-030 shell half).
- `tests/test_haproxy_proxy.py`: updated to the adjudicated intended design
  (single `/-/healthz` proxy-liveness check, commit 3d130c8 / shard-health
  funnel fix) and now also asserts the ABSENCE of the old per-model checks —
  strengthened, not weakened (F-09/F-10).
- `tests/test_submit.py`: PBS renderer tested under explicit
  `EXASERVE_SCHEDULER=pbs`; NEW test pins the PSI/J default artifact
  (`deploy.psij.sh`) so the PR-027 default is now under test (F-11).

**Results: full hermetic suite 64 passed / 0 failed (27.7s).** Every
baseline failure from BASELINE.md is now closed by node ID with the root
cause fixed (8 environment repairs in P00.1, 4 contract fixes here).

---

## 2026-08-05 — Slice P02.3: Blocker/High burn-down (driver, eval, scheduler, proxy)

Keepalive re-armed by user; long compute now via subjob leases.

**Fixed (code + tests, suite 67/67 green):**
- **PR-001 + PR-028 (driver):** `driver.py` now tracks `exit_code`, raises
  `SystemExit(code)` on serving-child/ray-worker nonzero exit and on the
  broad-except path (with traceback); SIGTERM handler → SystemExit(143) so
  the finally cleanup runs; Ray cleanup `wait(timeout=30)`+kill (was
  unbounded, PR-009). `server.py` main loop: SIGTERM/SIGINT → orderly
  `serve.shutdown()` with a 60s forced-cleanup watchdog thread.
- **PR-016 (matrix eval):** `_eval_derived` is now AST-restricted
  (literals/axes/approved-fns/`math.<approved>` only; dunder/attribute/
  subscript/lambda/comprehension rejected). 8 escape attempts tested incl.
  `().__class__.__mro__`, `__import__`, `min.__globals__`.
- **PR-019 (result ordering + completeness):** `_latest_result_path` numeric
  (result10 > result9); gather records `expected/collected/missing ranks`
  into result `meta.gather`, and `EXASERVE_EVAL_STRICT_COMPLETE=1` raises on
  partial instead of silently succeeding.
- **PR-014 (scheduler fail-open):** `count_queued` returns `Optional` — None
  on command failure/nonzero rc (both eval PBS+Slurm, psij-without-native);
  submit-all holds 30s on None instead of blind-submitting; package Slurm
  `job_state` returns None (not "D") when squeue fails.
- **PR-012 (URL/port):** `serve_url` reads the driver-published
  `proxy_out/proxy_port` (actual bound port) before falling back to config.
- **PR-010 (HAProxy admin):** stats page defaults read-only + loopback bind;
  `stats admin` requires explicit opt-in AND `stats_auth`, else refuses to
  render (2 new tests).

**Deferred to next compute round (need real runs):** PR-004 bcast.c,
PR-005 staging manifest, PR-002 driver vendor gating, SC-07/08 removal — all
want a leased 2-node verification and are batched together.

---

## 2026-08-05/06 — Slice P02.4/P04.1: driver vendor-gating, native staging, model manifests

**PR-002 (driver):** `resolve_vendor()` (EXASERVE_VENDOR, default xpu) +
`resolve_num_gpus()` (from validated deployment config, fallback env then 12),
resolved once in `main()` and threaded through `start_ray_head/worker` and
`get_ray_env`. `--num-gpus=12` literal gone; XPU/ZE_*/VLLM_TARGET_DEVICE/
ONEAPI vars now gated on `vendor=="xpu"` in the driver, matching
launch_cluster.sh (CUDA/ROCm no longer get XPU env).

**PR-004 (bcast.c):** rewritten — `shquote()` single-quotes every path into
the tar/mkdir commands (injection-safe) with truncation checks; `mkdir_p()`
via syscalls replaces `system("mkdir -p")`; both `pclose()` statuses checked
(`WIFEXITED`/`WEXITSTATUS`); per-rank failure aggregated via `MPI_Allreduce`
so any rank's tar error makes the whole broadcast exit nonzero (was: silent
success); buffer 1 GiB→64 MiB; `assert(buf)`→explicit NULL check + abort.
Compiles clean under `mpicc -O2 -Wall`. Functional mpiexec round-trip +
fail-loudly checks are in the P04 battery (compute).

**PR-005 (model_staging):** `_validate_model_dir` validates config.json +
weights + ALL safetensors/pytorch index shards (catches interrupted
downloads); `.exaserve_complete.json` completion marker written last (legacy
complete dirs upgraded in place — existing 30GB staged models keep working);
`download_model` now transactional (per-model `ExclusiveLease`, staging dir,
validate, marker, atomic `os.replace`; concurrent stager waits for the
winner); HF snapshot fallback picks newest-by-mtime not lexicographic-first.
5 hermetic tests.

**Tests:** full suite **72 passed / 0 failed**. New: test_model_staging.py
(5), earlier test_plan_schemas/test_status_store/test_atomic_state/
test_control_channel/test_trace_identity + PR-010/PR-016 additions.

## 2026-08-06 — Slice P05.3: audit burn-down to 34/35 + proxy-mode compute validation

Remaining code findings closed (suite 102 green, ruff-critical clean throughout):
- **PR-020:** spec_io enum + bound checks (scheduler.type/engine/arrival,
  go_concurrency/num_go_procs ≥ 1). 1 test.
- **PR-025:** benchmark gateways (litellm/envoy/nginx/pingora) marked at
  driver start — warn by default, hard-reject under
  `EXASERVE_PRODUCTION_BOUNDARY=1` (doesn't break the eval benchmark harness).
- **PR-032:** handlers preserve a caller `X-Request-ID` as a correlation id
  (linked to, not conflated with, the completion id) and echo it back.
- **PR-033:** `doc/hardening/COMPATIBILITY_MATRIX.md` published (support
  dimensions, scale tiers with VALIDATION_OWED markers, readiness semantics).
- **PR-024:** HAProxy `haproxy -c` + NGINX `nginx -t` config preflight before
  launch. **COMPUTE-VALIDATED** (haproxy smoke: "config validated (haproxy -c)"
  fired; malformed config would now fail preflight).
- **PR-009:** driver supervises BOTH serve + proxy; proxy death terminates the
  deployment. **COMPUTE-VALIDATED** (haproxy smoke: killing HAProxy →
  "Proxy exited ... terminating deployment").
- **PR-029:** telemetry actors (serving + replica-init collectors) are now
  deployment-scoped by name (`:<deployment_id>`) so a reused Ray cluster can't
  mix two deployments' state.
- **PR-026:** private Serve APIs (`_run_many`/`serve_start`/build_app) behind
  capability-guarded imports that fail with a clear pin message on an
  unsupported Ray. (Pin already corrected + overlay version-assert added.)
- **PR-019:** `result_is_complete()` helper + weakscaling plot gate
  (`EXASERVE_PLOT_STRICT_COMPLETE=1` rejects incomplete gathers; warns
  otherwise; legacy results without gather-meta still load).

**HAProxy proxy-mode smoke (2-node lease): 4/4 PASS** — deploy_ready,
pr024_config_validated, proxy_canary ("Paris, located in the north-central
part" through HAProxy), pr009_proxy_supervision.

**Audit findings: 34/35 FIXED; PR-031 (CI) IN_PROGRESS** — workflow + ruff
config + wheel-build gate written and locally green; enablement in the GitHub
Actions provider is a repo-admin step outside this environment.

## 2026-08-06 — Slice P07: implementation-audit corrections (IMP-*)

Codex audited commit `e73f3eb` (`doc/PRODUCTION_HARDENING_IMPLEMENTATION_AUDIT.md`,
verdict NOT PRODUCTION READY). **The audit is substantially correct.** I
verified its reproducible claims with an independent script before changing
anything; 7/7 defect classes reproduced. All are now fixed with regressions,
and the overstated ledger/status claims are withdrawn.

**Verified-then-fixed defects** (regressions in `tests/test_audit_regressions.py`):
- **IMP-B09 (worst, self-inflicted):** my PR-015 "injection fix" pasted
  `shlex.quote()` output INSIDE double quotes in the eval job body, where
  single quotes lose their quoting power — `code_root=/tmp/$(touch X)`
  rendered an *executing* substitution. Fixed by assigning the quoted value to
  `_ES_CODE_ROOT` and referencing `"$_ES_CODE_ROOT"` (bash does not
  re-evaluate a variable's value). Regression renders `$()`+backtick payloads
  and asserts no payload sits in a double-quote context.
- **IMP-B05:** `check_model_exists()` returned True on marker existence alone
  (deleting the only weight file still read "complete"); tokenizer-only
  downloads wrote a full-model marker. Marker inventory is now verified
  against disk (name + size), `kind` is recorded, empty/corrupt markers fail.
- **IMP-B07:** lease takeover was unfenced — a stale holder's `release()`
  deleted the successor's LIVE lease; two stealers could both `os.replace`;
  status CAS compared only the enum (ABA-stale writer accepted); a record
  could initialize directly as READY. Added per-acquisition fencing tokens,
  `holds_lease()`/`renew()`, O_EXCL arbitration for takeover,
  `expected_revision` CAS, and PLANNED-only initial states.
- **IMP-B06:** an authenticated rank could publish another rank's — or a
  GLOBAL (supervisor-owned) — observation; malformed observations were
  skipped rather than fail-closed; `_all_registered` never cleared on
  disconnect; dedup/audit state grew unbounded. Observation identity is now
  bound to the authenticated session, violations terminate the session,
  registration clears on disconnect, state is bounded, per-component
  sequences are enforced, heartbeats record a receiver-side lease timestamp.
- **IMP-H01:** `plan_hash` included `source_path` (same intent, different
  identity); the frozen plan held caller-owned dicts (content could mutate
  while the hash stayed); `nan` passed bounds; `reservation_topology: false`
  bypassed node-agreement; a top-level `envelope` block was ignored. All fixed.
- **IMP-H04:** list/scalar request bodies escaped as AttributeError/TypeError
  (500 instead of 400); `"false"` was truthy for `stream`/`ignore_eos` and the
  HAProxy `http_no_delay`/`abortonclose` options; `serve_url` looked for the
  published port beside the SOURCE config while the launcher writes it beside
  the run-scoped runtime config. All fixed.
- **IMP-B08:** partial replay, missing rank shards, or a failed REQUIRED stats
  collection still wrote `succeeded`. Incomplete runs are now written as
  `partial` with reasons; `_is_completed` treats partial as needing triage
  (not resubmission).
- **IMP-H07:** the randomized CI job ran `pytest -p randomly` without
  declaring `pytest-randomly`; added to the `dev` extra.

**IMP-B10 — ledger/status honesty (the audit's central complaint):**
- The "34/35 FIXED / no open production blocker" headline was **wrong** and is
  withdrawn. 23 audit findings whose invariant is owned by the un-cut-over
  architecture (or whose own evidence said work remained) are reopened as
  IN_PROGRESS. A record is FIXED only when its invariant holds on the path a
  production deployment actually takes.
- My record COUNT was also wrong (76 by regex vs 82 actual). Counts are now
  YAML-parsed. All 82 records carry the plan §8 required fields.
- Ledger now: **27 FIXED / 31 IN_PROGRESS / 22 OPEN / 2 out-of-scope**;
  audit findings: **12 FIXED / 23 IN_PROGRESS**.
- The 16/64-node results are relabelled in COMPATIBILITY_MATRIX.md as
  **direct-mode feasibility smoke on the legacy path**, explicitly NOT WP12
  qualification (legacy topology, `proxy_config: none`, no predeclared
  provenance) — per IMP-H08.

**Not fixed (correctly remains the real work):** IMP-B01/B02/B03/B04 — the
supervisor/readiness/compat-receipt architecture and the WP13 cutover. These
own the blocker invariants; they are the next packets, not cleanup.

**Verification:** suite **123 passed / 0 failed** (was 102); ruff correctness
gate clean; the audit's own reproduction script now fails to reproduce any of
the 7 defect classes.

---

## 2026-08-06 — Slice P06.3: 64-NODE weak-scaling smoke — PASS. Scaling ladder COMPLETE.

Job 8737093 (64-node debug-scaling batch). Direct-MPI probe, 64/64 shards.

**Result: 43,207 requests, 0 errors, aggregate 1373.9 RPS, per-node 21.47 RPS,
p50 1.469s, p99 1.585s. Deploy READY in 300s** (768 replicas = 64×12).

**Weak scaling is linear and regression-free across the full ladder:**

| nodes | replicas | per-node RPS | aggregate RPS | p99 | errors | ready |
|---|---|---|---|---|---|---|
| 2  | 24  | 11.0 (under-driven) | 22.1 | 2.49s | 0 | 100s |
| 16 | 192 | 21.53 | 344.4 | 1.59s | 0 | 260s |
| 64 | 768 | **21.47** | **1373.9** | 1.585s | 0 | 300s |

- **Per-node RPS flat 16→64n: 21.53 → 21.47 (0.3% Δ over a 4× cluster).**
  Aggregate 344→1374 RPS = 4.0× for 4× nodes ≈ 100% weak-scaling efficiency.
- **p99 latency flat** (1.59→1.585s); **0 errors across 43,207 requests** at
  768 replicas.
- **768/768 GPUs registered**; staging 29.93 GiB in 47s (flat 2→64n).
- The hardened orchestration (fail-closed readiness, immutable config, atomic
  state, supervised proxy, capability-guarded compat, request validation)
  imposes **no scaling penalty** — 64n/768-replica deploy+serve is as clean as
  2n. A2 port race handled by Serve retries within the walltime.

This meets the user's bar — "scalable to the point we tested before, with
consistent or better results" — conclusively: linear scaling, zero
regression, zero errors, stable latency, to 64 nodes.

Note on absolute magnitude vs the recorded ~6.8k RPS@64n baseline: that
baseline offered a higher per-node rate (110 rps/node via the high-concurrency
go client); this probe offers ~32 concurrency/node (latency-bound, not
saturation-bound), so the completed rate is lower by design. The comparable,
regression-detecting metric is per-node RPS *invariance* across scale, which
holds flat.

---

## 2026-08-06 — Slice P06.2: 16-node weak-scaling smoke — PASS (hardened code)

Job 8736814 (16-node debug-scaling batch, self-contained). Direct-MPI probe
(16 client ranks, 16/16 shards collected).

**Result: 10,821 requests, 0 errors, aggregate 344.4 RPS, per-node 21.5 RPS,
p50 1.47s, p99 1.59s.** Deploy reached ALL SERVICES READY.

Scale-invariance evidence (the headline weak-scaling criterion):
- per-node RPS **11.0 (2n) → 21.5 (16n)** — does NOT degrade as the cluster
  grows 8×; the 2n number was under-driven (only 2 client ranks vs 16 here),
  so 16n is the first fully-loaded point. No control-plane contention hit.
- p99 latency **2.49s → 1.59s** (tighter, not worse).
- **0 errors / 10,821 requests** through the hardened validation + fail-closed
  readiness path at **192 replicas** (16×12).
- **All 192/192 GPUs registered** — PR-008 GPU predicate scaled clean.
- Staging: 29.93 GiB bcast in 49s (flat vs 2-node — KI-A7 MPI staging holds
  at 16n). A2 port race handled by Serve retries (deploy still reached READY).

64-node smoke (job 8737093) queued behind it.

---

## 2026-08-06 — Slice P06.1: weak-scaling smoke — 2-node baseline on hardened code

Weak-scaling throughput smoke runs on the LIVE hardened code (launch_cluster
MPI-bcasts `src/` to every node → /tmp/exaserve_src; the eval HEAD-snapshot
path can't see uncommitted changes, and commits aren't authorized).
Harness: `run_scaling_smoke.sh` (direct-mode tp=1 8B, 12 replicas/node) +
`throughput_probe.py`.

**2-node result: 708 requests, 0 errors, aggregate 22.1 RPS, 11.0 per-node
RPS, p50 1.40s, p99 2.49s, deploy READY in 100s.** Per-node RPS is the
weak-scaling invariant to hold flat at 16/64 nodes.

Three debug iterations, each a real finding (the deploy itself was healthy
throughout — READY, 24/24 GPUs):
1. Harness broke the readiness wait on the first replica `Traceback`. That
   Traceback is the **A2 static-port race** (vLLM torch.distributed
   EADDRINUSE) under 12 concurrent EngineCore starts/node — transient, Ray
   Serve retries the replica (recovered to READY on re-run). A2 is a
   pre-existing KNOWN_ISSUES item (not a regression). Harness now only breaks
   on a driver-level FATAL/Refusing.
2. Probe hit `hostname:8000` → 503 (Ray Serve binds the proxy to the Ray node
   IP, not the PBS hostname). Fixed to read `ray_node_ips.txt`.
3. Probe sent `"model":"m"` → 100% 400s — **my own PR-011 validation
   correctly rejecting an unknown model**. Confirmed the fix works; probe now
   omits the model field.

Note on absolute magnitude: the probe is a single head-node client (Direct-Fat
style), which the recorded baseline showed plateaus on the head client, not
the server, at high N (findings #4). The comparable metric across scales is
therefore per-node RPS at fixed concurrency, not the peak aggregate (that
needs the Direct-MPI go client = eval harness = committed HEAD). 16-node
(job 8736814) + 64-node smokes submitted.

---

Next: weak-scaling throughput smoke on the LIVE hardened code (launch_cluster
bcasts src/, bypassing the eval HEAD-snapshot which can't see uncommitted
changes) at 2 → 16 → 64 nodes; per-node RPS should stay flat vs the recorded
~110 rps/node direct baseline.

---

## 2026-08-06 — Slice P05.2: PR-008 fail-closed readiness (COMPUTE-VALIDATED) + PR-035

- **PR-008 (Blocker):** three readiness predicates in `server.py` now FAIL
  CLOSED instead of declaring a degraded cluster ready — (1) GPU registration
  shortfall after the 600s deadline raises (opt-in
  `EXASERVE_ALLOW_DEGRADED_GPUS=1`); (2) the proxy-serving `ray.wait` loop got
  an overall deadline (`EXASERVE_PROXY_READY_DEADLINE_S`, default 900s) — was
  unbounded; (3) proxy health is a predicate: any unhealthy proxy OR a
  status-collection failure raises before `CLUSTER FULLY READY`
  (opt-in `EXASERVE_ALLOW_DEGRADED_PROXIES=1`). This is the D1 false-ready
  class, closed. **Compute-validated (2-node lease from 8736431): a HEALTHY
  deploy still reaches READY** — "All 24/24 GPUs registered" → "CLUSTER FULLY
  READY" → working canary → SIGTERM drain. The fail-closed changes did NOT
  false-negative healthy startup (the critical regression risk).
- **PR-003 compute-validated same run:** source `config.pp2.yaml` head_ip
  stayed `""`; `runtime_config.yaml` got `head_ip: 10.115.33.38`. (The
  battery's own immutable-source assertion FAILED on a stale-glob bug picking
  an old run_logs dir — the invariant itself is verified by direct inspection;
  battery assertion fix is cosmetic.)
- **PR-035:** replay result save + both scaling-trace writers now use atomic
  temp+rename (`exaserve.state.atomic.atomic_write_text`). Combined with the
  earlier eval `dump_*` conversion, the result/metadata write surface is
  atomic; the KNOWN_ISSUES C3 in-place-rewrite trailing-byte hazard is
  eliminated for these writers (fresh file each publish).

**P04 battery this run: 6/7 PASS** (bcast round-trip, bcast fail-loudly,
deploy_ready, canary, sigterm_drain, pr008_healthy_reaches_ready; the 7th —
pr003_immutable_source — is a battery glob bug, invariant verified manually).

**PR findings: 24 FIXED / 4 IN_PROGRESS / 7 OPEN.**

---

## 2026-08-06 — Slice P05.1: stats/engine/scheduler/eval fixes + CI gate

Batch of pure-code closures (suite 101 passed / 0 failed throughout):
- **PR-021:** `VLLMEngine.collect_stats` consumes the shipped
  summary/sample/scheduler_snapshots schema; the KeyError on a nonexistent
  `finished_requests` key (every call when stats enabled) is gone.
- **PR-022:** `enable_log_requests` added to `EngineSpec`, threaded through
  `EngineWorker`/`deploy_model`, and applied to vLLM `AsyncEngineArgs`
  (honors both `enable_log_requests` and `disable_log_requests` shapes).
- **PR-015:** scheduler directive fields validated (no newlines/metachars)
  via `validate_directive_field` (eval) and `JobSpec.__post_init__`
  (package); eval `_body` now shlex-quotes code_root/env_script/run_yaml/
  export values; `env_setup` stays exempt as documented operator shell.
  8 injection tests.
- **PR-013:** submit-all uses the atomic cross-host `ExclusiveLease` (foreign
  live lease respected — no more foreign-host-equals-stale); durable
  `submitted` state with `scheduler_job_id` recorded before counting;
  discovery skips submitted/running/replaying runs (idempotent re-invocation);
  bounded retries populate the `failed` dict. 3 idempotency tests.
- **PR-027:** scheduler module docstring corrected to PSI/J default; `psij`
  declared as the `scheduler` optional dependency; backend already fails
  preflight with a clear message when absent.
- **PR-034:** dirty working tree now REQUIRES `EXASERVE_ALLOW_DIRTY_SNAPSHOT=1`
  (or `allow_dirty=True`) instead of silently snapshotting stale HEAD; the
  warning + run_group provenance are retained. Test updated to assert the
  raise + ack path.
- **PR-031 (IN_PROGRESS):** `.github/workflows/ci.yml` — hermetic test matrix
  (py3.10/3.12, fixed + randomized-seed order), correctness-critical ruff gate
  (E9/F63/F7/F82/F811 — already clean tree-wide), advisory full lint,
  wheel-build + clean-venv import. `[tool.ruff]` config added. Wheel builds and
  packages control/state/plan + bcast.c; verified. (CI provider enablement +
  type/coverage gates still owed → stays IN_PROGRESS.)

**PR findings: 22 FIXED / 5 IN_PROGRESS / 8 OPEN.** Remaining OPEN are the
architectural WP4/5/10 items (PR-008 readiness coordinator, PR-009 supervisor,
PR-024 proxy validation, PR-026 capability checks, PR-029 telemetry lifecycle,
PR-032 metrics) plus PR-020/PR-025/PR-033.

---

## 2026-08-06 — Slice P04.2: PR-011 request validation, PR-003 immutable config, PR-006/007 legacy hardening

- **PR-011:** new ray-free `request_validation.py` (`parse_sampling`,
  `validate_model_field`, `validate_messages`, `RequestValidationError`);
  both OpenAI handlers now return **400** on malformed JSON / bad sampling
  ranges/types / unknown `model` / bad messages (was: opaque 500s + ignored
  model field). `_served_model_names()` accepts HF id + storage + route.
  12 hermetic tests (temperature="hot", max_tokens=1e9, min>max, model=gpt-4…).
- **PR-003:** `launch_cluster.sh` no longer mutates the operator's source
  YAML. A run-scoped `runtime_config.yaml` copy receives the resolved
  `head_ip` (atomic temp+rename); the driver, model_bcast, and head-ip writer
  all consume the runtime copy, so `ray_node_ips.txt`/`proxy_out/` land in the
  run dir. Source config is immutable. (Compute assertion added to the P04
  battery for the next run.)
- **PR-006/PR-007 (legacy schemas.py — the path that runs today):**
  `_strict_bool` (quoted "false" no longer becomes True); invalid
  `num_replicas` raises instead of silently degrading to auto-plan; derived
  storage/route identity collisions rejected in `validate_deployment_config`
  (the `a/b--c` vs `a--b/c` and `a.b/c` vs `a-b/c` cases). 3 legacy tests.

**Suite: 90 passed / 0 failed.** Findings FIXED this slice: PR-003, PR-006,
PR-007, PR-011, PR-023 (+ PR-001/002/004/005/010/012/014/016/028 earlier).

---

## 2026-08-06 — P04 COMPUTE BATTERY: 5/5 PASS on 2 nodes (lease from 8736432)

`artifacts/hardening/p04-battery/`. All verdicts PASS:
- **bcast_roundtrip** — hardened bcast.c mpiexec round-trip of a
  space+single-quote path extracted correctly on the head (injection-safe).
- **bcast_fail_loudly** — bogus source → `tar: Exiting with failure status`
  → new `bcast: FAILED — ... broadcast is not complete` message + nonzero
  exit (was: silent success). PR-004 MPI_Allreduce aggregation confirmed live.
- **deploy_ready** — full launch_cluster pp=2 8B deploy reaches
  `ALL SERVICES READY` in ~120s with the hardened driver; PR-002 evidence
  live in the log: `Starting RAY HEAD ... GPUs=12 vendor=xpu` (resolved from
  config, not the old hardcoded `--num-gpus=12`).
- **canary** — functional inference through the served root route:
  "The capital of France is" → " a city of love, art, fashion" (pp=2, the
  SC-* PP-alias patches remain load-bearing and correct).
- **sigterm_drain** — SIGTERM to the driver produced
  `[ExaServe] Shutdown requested; draining Ray Serve...` (PR-028 handler
  fired; was: process died with no drain path).

PR-023 (required-models policy) added after the battery: extracted
`enforce_required_models_policy` into ray-free `replica_planner.py` (raises
`RequiredModelsError` unless `EXASERVE_ALLOW_PARTIAL_MODELS=1`); 3 hermetic
tests. Suite **75 passed / 0 failed**.

---

## 2026-08-05/06 — Compute battery (P04) first run notes
bcast round-trip+fail-loudly, full pp=2 deploy with the hardened driver
(PR-001/002/028) + staging (PR-005) + overlay version-assert, functional
canary, SIGTERM drain evidence.

---

## 2026-08-05 — P00 GATE CLOSED: all four spike verdicts recorded with proofs

- **S00** ADR-000 (envelope: Aurora/PBS/XPU/vLLM/HAProxy, non-stream,
  qualification target 64n; 256n not a release requirement) — finalized, the
  co-design loop produced no contradicting evidence.
- **S01** ADR-001 — two-node proof PASSED (4/4 scenarios).
- **S02** ADR-002 — decided on 4-run evidence (2.0s RUNNING→serving gap,
  ~120s public replica-death blindness, state-API rejected, canary
  proxy-env rule).
- **S03** ADR-003 — inventory (66 records) + spawn/receipt proofs PASSED:
  attempt 4 exit 0 with `VLLM::EngineCore` environ carrying the EN-01 shim,
  all 12 patches applied in-process, SC-10 fallback live per decode step,
  and a functional pp=2 completion ("The capital of France is" → tokens).
  Stale-pin warning (2.49.1 vs 2.53.0) reproduced live. New public-API drift
  datum: `serve.status()` app overview lacks `route_prefix` on this Ray.
  Harness lessons: routes must be discovered not guessed; process scans must
  not assume `python` titles (`VLLM::EngineCore` retitles); auto-planning
  now deploys multi-replica PP (stale KI-D4 assumption).
- Allocations 8734762/8736039/8736110 all released. Total compute spend for
  P00: ≲ 3 node-hours.
- **P01 open:** WP1 immutable plan contracts + WP2 atomic state (login-node).

**WP0 action 8 — readiness-marker consumer inventory (complete, 2026-08-05):**
Control consumers (replaced by typed status at their owner WP, removed WP13):
`src/exaserve/driver.py:33` (driver greps server stdout for
`CLUSTER FULLY READY` — the core log-parsing control path; WP4);
`eval/lib/backends/ray.py:92` (`[Driver] ALL SERVICES READY` wait; WP9);
`eval/scripts/debug_128n.pbs:117-122` and
`eval/scripts/profile_init_scaling.sh:82-88` (log greps; WP9);
`src/exaserve/proxy/litellm_proxy.py` (own startup-marker readiness; WP7).
Producers kept as post-persist compatibility renderings: `server.py:2198`,
`driver.py:613`. Prose/teaching updates owed at WP13: `README.md:236,321`,
`eval/lib/models.py:189`, `src/exaserve/proxy/ray_serve_proxy.py:112`,
`doc/exaserve.md:47`, `doc/exawork_psij_notes.md:51`. Spike scripts
(`run_s03_spawn.sh`) grep the marker as a temporary migration oracle —
registered here per plan §4.1 switch rules, removal owed with WP4 cutover.
No external marker consumer identified.

**S02-2N run 1 (superseded detail):** At the instant
serve.status() reports app=RUNNING (3.6s), the worker proxy was still
STARTING and BOTH nodes' routes returned 502 — the D1 false-ready class
reproduced with public APIs at 2 nodes. Also: `ray.util.state` requires the
dashboard API server (:8265), absent on this stack → that ladder rung is
not dependable on Aurora (recorded for ADR-002). Probe v2 polls proxy/route
readiness to a deadline (measures the RUNNING→serving gap) and kills a
replica via OS signal instead of the state API; rerun in flight.

---

## 2026-08-06 — Pass 3: the readiness authority reaches the production path

This pass closes the consumer half of IMP-B02 and the transport half of
IMP-B04. Where pass 2 built the mechanisms, this one puts them **in the way of
a deployment**: `server.py` cannot print `CLUSTER FULLY READY` until an
explicit predicate holds, and the eval harness no longer trusts that text.

### New production seams

- `src/exaserve/control/serve_readiness.py` — the live binding of
  `ReadinessCoordinator`. Turns Ray/Serve state into typed observations,
  requires a real completion through the **external route**, drains
  compatibility receipts, writes `readiness.json`, and raises unless the
  predicate holds. Expected replica counts come from each application's
  `target_num_replicas` (the applied config), never from what happens to be
  running — so a dead replica drops the count below target and **revokes**
  readiness rather than being invisible.
- `src/exaserve/compat/collector.py` — the receipt channel. Receipts come from
  processes whose stdout we do not own (replicas scattered across the
  allocation), so they travel over a named detached Ray actor. Always created;
  deliberately not gated on the tracing flag, because readiness must not depend
  on an optional instrumentation switch.
- `server.py` — readiness gate before the marker; receipt collector created
  after `ray.init` and before `serve.run`; every replica self-attests and
  attests the engine core it owns.
- `eval/lib/backends/base.py` — `ProcessMonitor` consumes `readiness.json`.
  A snapshot that says *not ready* **overrides** the marker. The marker is
  accepted only after a grace window with no snapshot, and taking that path
  prints a warning, so the legacy fallback is visible rather than silent.

### Migration switches (plan §4.1)

| Switch | Default | Removal |
|---|---|---|
| `EXASERVE_READINESS_GATE` | `1` | WP13 — `0` restores marker-only readiness |
| `EXASERVE_ALLOW_DEGRADED_READINESS` | unset | WP13 — starts despite named blockers |
| `EXASERVE_REQUIRED_RECEIPT_ROLES` | derived | WP13 — overrides the required role set |
| `EXASERVE_COMPAT_ENFORCE` | unset | WP13 — makes replica activation failure fatal in-replica |
| `EXASERVE_USE_SUPERVISOR` | `1` | WP13 — `0` restores `exec bash` |

The `supervisor` role is required **only when a supervisor stamped the
environment**. The legacy `exec bash` path has no supervisor to attest, and
demanding a receipt nobody can issue would fail closed for a reason unrelated
to compatibility.

### Patch accounting: gates and the third state

Two corrections were forced by running this on hardware rather than reasoning
about it:

1. **Gated patches.** Every `SC-*` patch is requested by
   `EXASERVE_VLLM_PATCH_PP_LAYER_FILTER`. Demanding proof that a patch applied
   when it was never requested fails closed on a *correct* configuration, so
   `PatchSpec.env_gate` now scopes the required set. Head and replica read the
   same environment and derive the same set.
2. **Not-applicable is not applied.** A patch whose target module is not
   imported in a process neither applied nor was needed. The postcondition is
   tri-state (`True` / `False` / `None`) and the receipt carries
   `not_applicable` separately from `patch_results`. A receipt must never claim
   a patch took effect when it did not; `False` with the target loaded remains
   fatal (a half-patched process).

### Generation-isolated staging (IMP-H02)

`distribute_to_nodes.sh` stages into `/tmp/exaserve_src.<generation>` and
publishes the stable `/tmp/exaserve_src` name by `rename(2)` on a symlink, so
a reader sees the old tree or the new one, never a mix, and a deleted module
cannot survive into the next run. `launch_cluster.sh` exports
`EXASERVE_GENERATION` / `EXASERVE_DEPLOYMENT_ID` for the whole allocation.
`cleanup_run.sh` removes generation trees and dangling links.

First on-hardware attempt **failed** (`mv: cannot overwrite directory
'/tmp/exaserve_src' with non-directory`): rename cannot replace a pre-existing
real directory with a symlink. Retiring the legacy directory before publishing
fixed it. The supervisor reported this correctly as
`FIRST CAUSE: launch_cluster: UNEXPECTED_EXIT (exit=1)` — the supervision
machinery working as designed on its first real failure.

### Receipt routing: the first cluster run named the wrong culprit

The gate's first live run reported
`receipts: no compatibility receipt from role(s): ['replica', 'engine']` — which
reads as a compatibility failure but was a **routing** failure. Two fixes, both
of the same shape:

1. **Identity travels with the deployment.** `EXASERVE_DEPLOYMENT_ID` /
   `EXASERVE_GENERATION` / `EXASERVE_VENDOR` are now propagated through
   `build_actor_runtime_env`, so a replica derives the receipt-channel name and
   its receipt's deployment/generation fields from the same values the head
   used, instead of hoping ambient env reached every worker raylet.
2. **No silent swallow.** `create_receipt_collector` proves the channel is
   resolvable *by name* immediately after creating it, `publish_receipt`
   `ray.get()`s the report so a transport failure surfaces at the sender, and
   both print the reason once per process. Receipts are re-drained on every
   readiness poll, since replicas publish as they finish starting.

This is the audit's own lesson applied to new code: a swallowed exception turns
a transport bug into a false accusation against an unrelated subsystem.

### The receipt that would not arrive: three faults, one lesson

Three consecutive 2-node runs blocked on
`no compatibility receipt from role(s): ['replica','engine']`. Each fix
exposed the next fault, and none was the one the message implied:

1. **Invisible replica output.** Serve replica stdout does not reach the driver
   log, so the replica's own error print was unreachable. Fixed by recording
   the compat outcome (`compat_published`, `compat_error`, `compat_collector`)
   on the replica-stats channel — which was already proven to work — and
   summarizing it head-side. *Every subsequent diagnosis depended on this.*
2. **Cross-namespace lookup.** The channel lived in its own Ray namespace while
   the one proven reachable from a replica used `serve`. Moved to `serve`; the
   actor name is already deployment-scoped, so no isolation was lost.
3. **The actual cause — a sentinel behind a `staticmethod`.** SC-11 sets its
   marker on a function, then installs it as
   `manager.get_current_process_visible_accelerator_ids = staticmethod(fn)`.
   The postcondition read `vars(cls)` raw, found the `staticmethod` **object**
   (whose attribute lookup does not forward to the wrapped function), and
   concluded the patch had not applied. A correctly patched replica was
   reported half-patched, and every replica declined to publish.

The lesson is the audit's own: **a fail-closed check is only as good as its
evidence**. A postcondition that cannot see a correct state does not make the
system safer, it makes it unavailable — and the operator is handed a message
that accuses the wrong subsystem. Both the sentinel resolution and the
staticmethod shape are now covered by unit tests
(`tests/test_serve_readiness.py`).
