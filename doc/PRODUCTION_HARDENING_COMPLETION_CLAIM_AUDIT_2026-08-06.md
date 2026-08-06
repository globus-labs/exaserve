# Production-Hardening Completion-Claim Audit

**Audit date:** 2026-08-06  
**Frozen revision:** `2a7726f7236236ceed0256353096e21f329c06df`  
**Worktree at freeze:** clean  
**Architecture authority:** `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`  
**Verdict:** **NOT PRODUCTION READY; the claim that only 256-node work remains is rejected.**

This document audits the implementation and evidence represented by Claude
Code as `67 FIXED / 8 IN_PROGRESS / 5 ACCEPTED_LIMIT / 2
OUT_OF_PRODUCTION_SCOPE`, with all eight in-progress records allegedly waiting
only for scale evidence: five explicitly cite 256 nodes, two cite 128/256, and
one says only “at scale.” It is an audit report, not a replacement
specification. Where this report and implementation/status prose differ, the
execution plan remains authoritative.

No new MPI, GPU, or cluster workload was launched for this audit. Existing
Aurora artifacts were inspected. Source inspection, the complete lightweight
test suite, the hermetic lane, lint, and focused local negative-contract
reproductions were run on the login node after loading the repository-required
modules.

## 1. Executive result

Claude delivered a substantial amount of useful substrate: 15 commits after
the prior audit snapshot, 42 changed files, roughly 4,018 inserted lines,
additional tests, typed lifecycle classes, an authenticated control transport,
port-lease helpers, capability declarations, and observability primitives. The
test suite is healthy.

The completion accounting is nevertheless materially wrong. Most new
components are not the components that the reachable production path uses, or
their predicates are weaker than their names and documentation claim.

| Measure | Audited result |
|---|---:|
| Full lightweight test suite | **298 passed**, 1 warning |
| Hermetic deterministic lane | **291 passed, 7 skipped** |
| Configured Ruff gate | **passed** |
| Canonical grouped findings fully closed | **0 of 18** |
| Canonical grouped findings still remaining | **18 of 18**: 17 partial, 1 open |
| Release-definition clauses demonstrated end-to-end | **0 of 12** |
| Ledger records claimed `FIXED` | 67 of 82 |
| Ledger records containing every plan section 8 field | **0 of 82** |
| Validly approved `ACCEPTED_LIMIT` records | **0 of 5** |
| Required `doc/hardening/FINAL_AUDIT.md` | **missing** |

Green unit tests show that the helpers work under the behavior asserted by the
tests. They do not prove that those helpers own production, that every negative
contract fails closed, or that the packaged deployment/eval/ClientLab paths
share them.

Several tests explicitly encode the weaker behavior: the rank-topology test
expects `exaserve.driver` as the rank command
(`tests/test_rank_topology.py:242-246`), compatibility tests accept required
patches as `not_applicable` (`tests/test_serve_readiness.py:121-129`), and the
control-wiring test requires an unreachable channel to degrade rather than
fail (`tests/test_control_channel_wiring.py:104-111`). Passing those tests is
not evidence for the stronger canonical contracts.

## 2. The latest Aurora smoke disproves the headline

The most recent **local, untracked** two-node smoke is useful positive evidence that a model
can deploy and answer a canary. It is also direct evidence that the claimed
architecture is not the executing architecture:

- `artifacts/hardening/supervisor-smoke/launch.log:2` shows the outer launcher
  supervising `bash .../launch_cluster.sh`.
- The same log at line 43 shows the Python supervisor launching
  `mpiexec ... python -m exaserve.driver`, not a per-rank `NodeSupervisor`.
- Lines 50-52 show `exaserve.driver` launching the server and explicitly
  waiting for `CLUSTER FULLY READY`.
- The generated `readiness.json` contains `"plan_hash": "plan"`, demonstrating
  that readiness is not bound to a compiled immutable plan.
- That snapshot's canary URL is `http://10.115.33.38:8000/...`, the internal
  Ray Serve endpoint. The smoke configured direct mode and exercised no
  external-gateway path.
- On configurations with an external proxy, the server declares READY before
  the driver starts that proxy; the inspected smoke selected direct mode, so
  it cannot provide gateway-path evidence.

The smoke therefore establishes a narrow success on two Aurora nodes. It does
not establish the canonical ownership tree, marker-free control, plan-bound
readiness, external-gateway readiness, or revocable steady-state operation.

## 3. Disposition of the 18 canonical finding groups

The grouping below is the same one used by
`doc/PRODUCTION_HARDENING_IMPLEMENTATION_AUDIT.md` and the preceding live
re-audit. A group closes only when its invariant is on every reachable
production path and has the required negative and deployment evidence.

| Group | Disposition | Decisive remaining gap |
|---|---|---|
| IMP-B01 target architecture | **PARTIAL / blocker** | The live rank entry point is still `exaserve.driver`; `NodeSupervisor` has no production consumer, and `DeploymentManager` labels operations inside the server rather than owning them from the allocation supervisor. |
| IMP-B02 readiness authority | **PARTIAL / blocker** | Expected membership/apps/routes are derived from currently surviving Ray/Serve observations, the canary bypasses the external gateway, READY is one-shot, and marker consumers remain. |
| IMP-B03 essential-child supervision | **PARTIAL / blocker** | Good generic supervisor primitives exist, but the legacy driver owns real children; unexpected zero exits and loss of the Ray head are not consistently fatal. |
| IMP-B04 compatibility | **PARTIAL / blocker** | Receipts require one item per role rather than exact planned instances; `not_applicable` can satisfy required EN-01; missing self-attestation falls back to owner assertion. |
| IMP-B05 model completeness | **PARTIAL / blocker** | Completion is top-level name/size based, not content/revision based; publication and lease renewal are not transactional. |
| IMP-B06 control transport | **PARTIAL / blocker** | HMAC framing is useful, but the channel is optional/fail-open, is only a failure side-channel, lacks command dispatch, mandatory reconnect snapshots, production heartbeats, and a lease watchdog. |
| IMP-B07 leases/status | **PARTIAL / blocker** | Token check and renew/release mutation are separate race windows; revision fencing is optional; the status store is not used by production. |
| IMP-B08 eval correctness | **PARTIAL / blocker** | Marker fallback, stale/partial result acceptance, last-arm-only validation, silent stats failure, and zero-exit partial runs remain. |
| IMP-B09 scheduler/submission | **PARTIAL / blocker** | Two scheduler stacks remain; direct submission is not idempotent; submit/persist uncertainty has no scheduler reconciliation. |
| IMP-B10 ledger truth | **PARTIAL / blocker** | Counts parse, but many `FIXED` dispositions are false, required fields are absent, accepted limits lack approval, and required final evidence is missing. |
| IMP-H01 immutable shared plans | **PARTIAL / blocker** | The new compiler is unreachable from production/eval, discards runtime-significant accepted fields, and is not the shared RunPlan/SiteProfile contract. |
| IMP-H02 generation-isolated distribution | **PARTIAL / blocker** | Source, model, venv, overlay, and PP-stage publication lack one content-addressed, exact per-rank receipt protocol; several final paths remain stable/in-place. |
| IMP-H03 Bash cutover | **OPEN / blocker** | The 557-line script still owns scheduler setup, runtime config, staging, distribution, Copper, MPI helpers, cleanup, logs, and final launch; it does not end in the required single `exec`. |
| IMP-H04 gateway/API/security | **PARTIAL / blocker** | Production-boundary enforcement is opt-in; wildcard ingress lacks request-aware policy; driver readiness is TCP-only; proxy schema/request validation and port ownership remain incomplete. |
| IMP-H05 observability/IDs | **PARTIAL / blocker** | Metrics hooks are not called by production requests, stats actor names disagree, IDs are not echoed/propagated on all response modes, and detached telemetry lifecycle is incomplete. |
| IMP-H06 transactional artifacts | **PARTIAL / blocker** | Atomic helpers exist, but authoritative writers still truncate/write in place and readiness persistence is best-effort. |
| IMP-H07 CI/release gates | **PARTIAL / blocker** | CI tests an editable checkout, wheel testing is import-only, and the job named format/lint/types runs neither formatting nor type checks; no locked dependencies or final fault matrix exists. |
| IMP-H08 qualification | **PARTIAL / blocker** | Existing 2/16/64 evidence is not the required clean packaged target-architecture matrix and lacks standard manifests/provenance/verdicts. |

## 4. Confirmed release-blocking defects and overclaims

### 4.1 Ownership, supervision, and Bash

1. **The documented topology is not implemented.**
   `src/exaserve/supervisor_main.py:51-60` constructs rank argv as
   `python -m exaserve.driver`. Repository search finds no production
   construction of `NodeSupervisor`; its consumers are tests. The topology
   comment and Pass 4 status prose therefore describe a design, not the
   reachable process tree.

2. **There are two nested supervisors around a large shell orchestrator.**
   `src/exaserve/launcher.py:51-78` supervises Bash. Bash invokes
   `exaserve.supervisor_main` without `exec` at
   `src/exaserve/resources/launch_cluster.sh:547-552`. The shell retains the
   lifecycle work enumerated above. The reachable launch stack still exposes
   both legacy switches: `EXASERVE_PYTHON_RANK_LAUNCH` in the shell and
   `EXASERVE_USE_SUPERVISOR` in `launcher.py`.

3. **Essential children can disappear without an immediate fatal result.**
   The real rank-zero loop at `src/exaserve/driver.py:686-705` polls Serve and
   the optional gateway, but not the Ray-head child. Worker Ray exit is turned
   into failure only when its exit status is nonzero at lines 736-743. A
   long-lived Ray worker exiting zero can therefore become a successful rank.

4. **The structured channel is fail-open.**
   Listener construction failure logs a warning and continues
   (`supervisor_main.py:74-95`), and rank connection/observation failure also
   fails open (`control/channel_runtime.py:156-207`). Initial connection
   failure prints a warning; later observation-send failure returns false and
   is ignored. The plan requires the canonical control path to fail closed.

5. **A clean protocol close becomes a fatal lost lease.**
   `GOODBYE` exits the listener loop, whose unconditional disconnect callback
   is translated by `HeadChannel` into `rank N: control lease lost`. A focused
   authenticated reproduction confirmed this exact false failure.

6. **Reconnect semantics are unsafe.**
   A reconnected rank may send an incremental `OBSERVATION`; the listener does
   not demand the mandatory full `SNAPSHOT`. `COMMAND`/`COMMAND_RESULT`
   dispatch remains explicitly unimplemented at
   `control/transport.py:313-314`. Heartbeat send methods and
   `wait_all_registered` appear only in tests, with no production expiry
   watchdog.

### 4.2 Readiness remains weaker than claimed

7. **The expected plan is inferred from survivors.**
   `control/serve_readiness.py:355-362` derives expected nodes from alive
   `ray.nodes()` and expected apps/routes/replica targets from currently
   discovered Serve applications. A planned node or entire application that
   never appears can shrink the expected set rather than block READY.

8. **Replica fallback can equate target with current running count.**
   The fallback at `control/serve_readiness.py:127-136` sets target equal to
   running, making a missing replica invisible when stronger Serve detail is
   unavailable.

9. **The canary is not an external-gateway canary.**
   `src/exaserve/server.py:2562-2569` gives readiness Ray Serve port 8000. The
   driver starts the selected external proxy only after seeing the server's
   marker (`driver.py:638-680`). Gateway health, configuration, and advertised
   route therefore cannot be proved by this gate.

10. **READY is operationally latched.**
    `await_ready()` returns at the first ready observation. After marker
    emission the server only waits for shutdown (`server.py:2598-2600`); it
    does not run post-READY reconciliation or call `DeploymentManager.observe`.
    A later failed replica, route, gateway, or canary cannot revoke the
    published production state.

11. **Control-lease loss does not revoke even the standalone coordinator.**
    `ReadinessCoordinator.rank_disconnected()` only removes a rank from
    `_connected_ranks`, but `evaluate()` never consults that set. Reproduction:
    readiness was `True` before disconnect and remained `True` afterward.

12. **Snapshot persistence and gate bypasses fail open.**
    `write_snapshot()` catches `OSError` and returns `None`; marker emission
    continues. `EXASERVE_ALLOW_DEGRADED_READINESS=1` can return a snapshot with
    `ready=false`, and `EXASERVE_READINESS_GATE=0` disables the predicate, yet
    the server still reaches its unconditional marker.

13. **Stdout still controls lifecycle.**
    The production driver advances by parsing `CLUSTER FULLY READY`
    (`driver.py:609-651`). Eval's `ProcessMonitor` accepts the marker after a
    grace period if no snapshot exists (`eval/lib/backends/base.py:104-132`).
    Operational/eval scripts also grep the marker. ClientLab has a different,
    duplicated subprocess-readiness handshake rather than the shared readiness
    contract. This directly violates release-definition item 2.

14. **Eval can consume the wrong readiness file.**
    It selects the newest recursive `readiness.json` by modification time,
    without binding its path or contents to the requested deployment,
    generation, plan hash, or schema.

### 4.3 Compatibility and patch delivery

15. **EN-01 can be absent and still satisfy READY.**
    A self receipt treats required patches present only in `not_applicable` as
    complete (`compat/receipt.py:64-71`). When the generated shim runs but does
    not import the patch payload, `engine_shim.py:112-139` places all required
    engine patches, including unconditional EN-01, there. The focused
    reproduction was accepted and made `ReceiptStore.satisfied()` true.

16. **Missing engine self-attestation has a success fallback.**
    `server.py:1017-1100` treats no engine receipt as nonfatal and substitutes
    a replica-owner attestation. `CompatibilityActivator.attest_external()`
    marks every required patch `not_applicable`; an executable/version string
    is then enough. This can turn failed engine injection into READY.

17. **Receipt cardinality is role-level, not plan-level.**
    `ReceiptStore.satisfied()` requires at least one receipt per role, not one
    per planned node/rank/daemon/replica/engine instance. The head even
    synthesizes one `ray_worker` receipt locally
    (`control/serve_readiness.py:311-328`). One worker/replica/engine receipt
    can therefore certify a much larger fleet.

18. **Profile identity is not artifact identity.**
    The profile hash covers declarative metadata, not patch source bytes,
    executable/base-image identity, overlay manifest, or semantic probes.
    `default_profile()` also sets Python from the currently observed
    interpreter, so Python drift is self-pinned instead of rejected.

    Compatibility evidence also bypasses the authenticated control channel:
    receipts travel through a detached Ray actor and node-local engine files.
    The receipt schema has no plan hash or exact planned logical/instance
    identity, nor source/venv/model content digests or final-path identity,
    weakening scope fencing, cardinality proof, and distribution attestation.

19. **Legacy activation still runs early and non-strict.**
    `server.py:35-36` calls legacy patch application with the default
    `strict=False` before later receipt validation. Import/application errors
    may be swallowed while the one-time done flag is set. Even the new profile
    path imports Ray/vLLM while discovering versions before it verifies the
    environment, so it cannot satisfy the canonical verify-before-import
    boundary.

20. **Engine receipts can be stale or cross-replica.**
    The receipt directory is scoped only to deployment/generation, not replica
    and engine identity. Every replica reads every `engine_*.json`; the
    directory is not cleared before reuse. A restarted or failed engine can be
    “proved” by another or prior engine's file in the same generation.

### 4.4 Plans, schemas, state, and leases

21. **The immutable plan compiler is not on a runtime path.**
    Repository search finds `compile_deployment_plan` and `from_legacy_yaml`
    only in their definitions/exports and tests. Production uses
    `src/exaserve/schemas.py`; eval uses separate mutable dataclasses in
    `eval/lib/models.py`. There is no shared SiteProfile: at least
    `eval/site_config.py` and `src/exaserve/config.py` define separate site
    defaults, with account/queue/filesystem defaults duplicated elsewhere;
    the eval version also contains a personal absolute SGLang path. The target
    plan's gateway enum omits active runtime type `ray_serve`, so a currently
    meaningful production configuration cannot even compile under the
    supposed canonical contract. Its default `supported_max_nodes` remains 2,
    consistent with the support matrix's classification of 16/64 as
    feasibility smokes and contrary to Pass 4's impression of new-path
    qualification. Because the compiler is unreachable, the bound does not
    gate a real launch.

22. **Plan identity omits accepted runtime behavior.**
    `deployment_name`, `replica_max_ongoing_requests`, and the entire
    `ray_cluster_config` are accepted by the compiler but discarded from
    `DeploymentPlan` and its hash. Active settings `enable_log_requests`,
    `num_cpus_per_replica`, and `collect_stats` are not represented. Focused
    reproductions changed each of the first three values and obtained the same
    plan hash.

    This is not only a compiler omission: the live path never exports
    `EXASERVE_PLAN_HASH`. `supervisor_main.py` defaults it to the literal
    `"plan"`, and `server.py` also falls back to `"plan"` because
    `ScalingTracer` has no `config_hash`. Every live deployment can therefore
    claim the same plan identity even when its configuration differs.

23. **Active configuration remains permissive.**
    Production `_model_config_from_dict()` accepts `num_replicas: true` as 1
    and `8.9` as 8. Eval uses `bool(value)`, so YAML string `"false"` becomes
    true, and it ignores unknown keys. Active `gpu_memory_utilization` accepts
    non-finite NaN. Proxy parsing is not unknown-key-strict and uses raw
    numeric coercion, including accepting a boolean as a port.

24. **The proxy client fan-out fix is unreachable for omission.**
    The loader first maps omitted `client.num_nodes` to
    `deployment.num_nodes`; normalization only applies the new bound when the
    value is below 1. A real 256-node proxy spec with the field omitted still
    loads 256 client ranks. The added tests construct `ClientSpec(num_nodes=0)`
    directly and do not cover the loader path.

25. **Eval RunPlan round-trip drops behavior.**
    Loading a saved plan omits `client.stream`, saturation configuration,
    `workload.arrival`, and `trace.tokenizer_builder`; a save/load cycle can
    silently change the experiment.

26. **Capability declarations are incompletely enforced.**
    `require_streaming_comparison()` has no production caller, so the
    fake-streaming comparison guard exists only in tests. Scheduler/site
    portability is not capability-gated, and capability validation occurs
    after parts of cluster startup rather than at plan compilation.

27. **Status revision fencing is optional and unused by production.**
    `StatusStore.transition(... expected_revision=None)` permits state-only
    writes. A deterministic `SUBMITTED → FAILED → SUBMITTED` ABA reproduction
    allowed a stale writer to transition the new `SUBMITTED` record. The store
    itself has no production lifecycle consumer. Eval instead atomically
    replaces an ad-hoc lower-case run-state JSON document with no lease,
    revision, or CAS, so concurrent writers are last-writer-wins.

28. **Lease fencing still has check-then-mutate races.**
    `ExclusiveLease.renew()` checks its token and then replaces the file;
    `release()` checks and then unlinks. A successor taking over between those
    operations can be overwritten or deleted. Both interleavings were
    reproduced deterministically. Port stale-lock takeover and stale sweeping
    have the same read/unlink race, so two ExaServe contenders can own the
    same advertised port.

### 4.5 Staging, distribution, and artifacts

29. **A completion marker does not prove model content.**
    It records top-level filename and size only. A marker listing an arbitrary
    file and no `config.json`/weights was accepted. Replacing a weight with
    different bytes of the same size was also accepted. Source repository and
    revision are unpinned. On the legacy no-marker path, any weight file is
    accepted when a shard index is absent; an interrupted sharded download
    missing its index can therefore be classified complete and automatically
    upgraded to a completion marker.

30. **Model download publication is not continuously transactional.**
    The final directory is deleted before `os.replace`, creating an absence
    window. A fixed sibling staging path and a two-hour lease that is never
    renewed allow long-download takeover/races. A transient partial broadcast
    can strand the cache until manual deletion.

31. **Distribution does not prove exact membership/content.**
    Cache probing counts JSON lines but does not enforce unique expected hosts.
    Source/venv/overlay/model receipts do not bind every rank to exact content,
    profile, model revision, and final path. Stable `/tmp/exaserve_venv`,
    `/tmp/exaserve_overlay`, and PP-stage destinations can retain or mix old
    files.

32. **Generation isolation is incomplete.**
    Source generation defaults to second-resolution time and reuses an
    environment-supplied value. Extraction occurs into the final generation
    directory without first clearing it, so a retry/collision can preserve a
    module deleted from the new source. Deployment identity defaults to the
    scheduler job ID; two launches within the same second in one allocation
    can consequently share deployment ID, generation, constant plan hash,
    receipt namespaces, control scope, and source destination.

33. **PP staging bypasses the scheduler seam.**
    `pp_stage.py` hardcodes `mpiexec --hosts`, so it does not work on the
    advertised Slurm path. Shared stage directories are rebuilt in place with
    no lease, manifest, content hash, or atomic publish, and stale files are
    not cleared.

34. **Native helpers retain unsafe/failure-prone shell boundaries.**
    `gather.c` constructs `mkdir -p <dest>` for `system()` without quoting and
    embeds output paths in single-quoted shell commands without escaping an
    apostrophe. `bcast.c` uses a tar pipe and does not handle SIGPIPE; if an
    extractor exits mid-stream, one rank can die before the collective error
    reduction while peers remain in `MPI_Bcast`, risking a hang or
    launcher-dependent abort. Its quoting helper reduces injection risk, but
    it still constructs a shell string and uses `popen`, contradicting
    PR-004's literal no-shell-string invariant. Model and PP broadcast/probe
    subprocesses also have no operation deadline, so a wedged collective can
    last until scheduler walltime rather than fail within a bounded stage.

35. **Many authoritative writes still publish in place.**
    Examples include eval manifest YAML, job scripts, replay result repair,
    server-stat JSON, package submit scripts/job IDs, model broadcast timing,
    driver port/scaling artifacts, and Ray node artifacts. The existence of
    atomic helpers does not satisfy PR-018/PR-035 until all authoritative
    consumers use them.

36. **Trace completeness is metadata-only.**
    `trace_store.py` treats `metadata.json` as proof of a complete trace. A
    focused reproduction deleted the trace data while retaining metadata; a
    rematerialization reused and returned a nonexistent trace path. Tokenizer
    snapshot revision/content is also absent from identity. Trace temp/replace
    publication does not fsync the file or parent directory, so it is
    visibility-atomic but does not meet the plan's durability contract.

37. **Repository snapshot reuse is metadata-only.**
    `_ensure_repo_snapshot` reuses a commit-keyed directory when
    `snapshot_meta.json` exists without verifying the archived files or the
    locally built Go replay client. The metadata records the source commit but
    no archive-integrity hash or Go toolchain/binary-output hash, so a
    deleted/corrupt snapshot or binary built by a different toolchain can be
    silently reused.

### 4.6 Eval, scheduler, and ClientLab

38. **Eval still launches Bash and trusts markers.**
    `eval/lib/backends/ray.py:247-259` directly launches
    `launch_cluster.sh`. Its monitor's marker-only fallback reproduced
    `True`/`readiness_source='marker'` without a snapshot.

39. **Replay validation can bless the wrong or partial result.**
    Only the last dispatch topology arm is validated. Missing gather metadata
    is considered complete; the newest pre-existing numeric result may be
    reused without generation/freshness identity; trailing garbage is ignored
    and the source file rewritten in place.

40. **`partial` can still be scheduler-visible success.**
    `run_executor.py:108-132` writes partial state but returns the original
    zero process exit. `startup_only` can write success with no replay result.
    This violates release-definition item 6.

41. **Required stats can fail silently.**
    The producer actor is named `ServingStatsCollector:<scope>` while eval
    looks up literal `ServingStatsCollector`. Collection returns an error
    dictionary instead of raising, and the executor ignores that return value.
    Expected-versus-received replica cardinality is not enforced.

42. **Submission is not at-most-once.**
    The direct public submit path has no lease/state/job identity; a two-call
    fake-scheduler reproduction submitted twice. In `submit-all`, successful
    scheduler submission followed by state-write failure only warns. A
    `submitting` record without job ID is then skipped without reconciliation,
    and an unobservable queue can be retried forever. The 24-hour submission
    lease is never renewed inside that potentially unbounded loop, so a second
    submitter can steal the lease while the first continues submitting.

43. **Two scheduler abstractions remain.**
    `src/exaserve/schedulers` and `eval/lib/schedulers` have different
    interfaces and lifecycle coverage. Package submission defaults to PSI/J
    even though it is only an optional extra; eval's registry advertises it,
    while normal eval spec loading defaults to PBS and passes that explicitly.
    Generic PSI/J also collapses distinct terminal states.

44. **`serve_url` is not deployment-bound.**
    It chooses the newest recursive port artifact and proves only scheduler
    state, so it can return another concurrent deployment's port before the
    requested deployment is READY. It resolves the candidate port file only
    once before polling, so if none exists initially a file published later is
    deterministically missed and the configured/default port is used. It
    performs no connection or health verification before advertising the URL.

45. **ClientLab can return success for a failed study.**
    Point exceptions are caught and recorded, but the CLI always returns zero;
    a focused reproduction with the sole point failing produced
    `diagnosis=error` and exit 0. ClientLab also retains its own subprocess,
    SSH/PID, readiness, and cleanup lifecycle rather than the shared control
    contracts. An error before its cleanup `try` can leak launched targets.

### 4.7 Gateway, security, ports, and API behavior

46. **Production exposure enforcement is ambient opt-in.**
    Without `EXASERVE_PRODUCTION_BOUNDARY=1`, benchmark-only/unsupported
    combinations can warn and continue. HAProxy binds a wildcard frontend and
    has no typed auth, TLS, body-size, request-aware/per-tenant admission,
    overload, retry, or cancellation policy. It does render `maxconn` and HTTP
    backend health checks, but the driver's proxy-process readiness check is
    only a TCP connect.

47. **Request validation still has 500-class paths.**
    Message role/content types and prompt-list elements are not comprehensively
    checked. A non-dict truthy `chat_template_kwargs` is later expanded with
    `**`, and invalid chat-template inputs can escape as server errors. No
    application body-size limit is declared.

48. **Port ownership is incomplete.**
    A filesystem lease does not hold the actual socket against foreign
    processes; lease-directory failure silently falls back to probe-only
    allocation. Stale-lock takeover is racy. The patched vLLM path also still
    calls its own `get_open_port()` for an inner distributed-init port outside
    ExaServe's registry.

49. **Slurm proxy and PP paths remain broken.**
    The shell materializes `EXASERVE_NODEFILE`, but proxy backend discovery
    reads `PBS_NODEFILE`; eval's Ray backend explicitly rejects any scheduler
    other than PBS, and PP staging hardcodes mpiexec. `TD-SLURM-AMD=FIXED` is
    therefore not supportable.

50. **The SGLang/XPU override conflicts with Aurora's site contract.**
    `vendors/xpu.py` explicitly introduces `ONEAPI_DEVICE_SELECTOR` for the
    SGLang path, while the repository's Aurora operating rules require
    `ZE_AFFINITY_MASK` and prohibit introducing that selector. SGLang remains
    outside the provisional release envelope, but the override should be hard
    blocked on Aurora or separately approved and proven rather than described
    as a completed/gated capability.

### 4.8 Observability and CI

51. **The operational metrics registry is not wired to requests.**
    `record_request()` and `mark_replica_ready()` have no non-test call sites.
    `/metrics` exists but normally exposes no request/replica series beyond
    registry internals.

52. **Request/correlation ID propagation is partial.**
    Malformed/validation errors, chat engine errors, chat non-stream success,
    and both streaming response paths omit `X-Request-ID` (non-stream
    completion engine errors do echo it). The vLLM engine uses its completion
    ID and discards the supplied correlation ID; there is no end-to-end
    correlated logging proof.

53. **Telemetry actor lifecycle is incomplete.**
    The serving-stats actor is detached, and its latest-payload dictionary has
    uncapped `node_ip:pid` key cardinality. Server teardown kills the
    compatibility collector but not this actor, allowing stale PID entries and
    memory to accumulate across deployment reuse.

54. **CI is not the release gate named by the plan.**
    Tests install `-e .[dev]`; the built-wheel job only imports a few modules.
    The “format + lint + types” job runs Ruff only, with a narrow correctness
    subset and broader lint nonblocking. There is no formatter check, type
    checker, security-focused SAST/dependency scanner, Go/native build/test
    lane, dependency lock, installed-wheel test suite, or packaged
    fault/upgrade matrix.

## 5. Ledger and evidence integrity

Direct YAML parsing produced the advertised arithmetic but not the advertised
record quality:

| Field required by plan section 8 | Records missing/empty |
|---|---:|
| `affected_regions` | 47 |
| `acceptance_tests` | 42 |
| `decision` | 82 |
| `evidence` | 18 |
| `fallback` | 82 |
| `residual_risk` | 82 |
| `support_impact` | 80 |
| `revisit_condition` | 80 |

All five `ACCEPTED_LIMIT` records—`KI-C6`, `TD-STAGE-PAR`, `TD-CONSTS`,
`TD-PROXYPROF`, and `TD-DOCS-REGEN`—lack approval identity, timestamp, and
linked approval evidence. Plan lines 1329-1333 make those dispositions
invalid. In particular, `TD-CONSTS` calls SiteProfile centralization cosmetic
debt even though the canonical WP1 design requires it.

`doc/hardening/STATUS.md` is internally contradictory: its headline still says
the target architecture is not wired and reports the older 30/28/22 counts,
while later Pass 4/5 prose says the architecture is finished. The current
67/8/5 counts come from `FINDINGS.yaml`, not a coherent replacement of the
headline status. The required `doc/hardening/FINAL_AUDIT.md` is absent.

PR-004 is a compact example of invalid promotion: its invariant says “no
shell-string construction,” while its own evidence explicitly retains “the one
shell string.” That evidence can justify a bounded residual decision, not the
current `FIXED` disposition for the stated invariant.

No tracked file exists beneath `artifacts/`. The inspected local hardening
experiment directories lack the required six-file set
`manifest.yaml`, `command.txt`, `environment.txt`, `stdout.log`, `stderr.log`,
and `verdict.md`. Raw local logs may still be useful diagnostic evidence, but
without the manifest, exact command/environment, checksums, independent
verdict, and durable link they are not WP12 release evidence.

### Minimum ledger corrections

The following current `FIXED` records are demonstrably broader than their
implementation/evidence and should return to `IN_PROGRESS` pending narrower
record splitting or full closure:

- `PR-001`, `PR-004`, `PR-005`, `PR-006`, `PR-008`, `PR-009`
- `PR-010`, `PR-011`, `PR-012`, `PR-013`, `PR-014`
- `PR-017`, `PR-018`, `PR-019`, `PR-020`, `PR-021`
- `PR-024`, `PR-025`, `PR-026`, `PR-027`, `PR-028`, `PR-029`
- `PR-031`, `PR-032`, `PR-035`
- `KI-A2`, `KI-B3`, `KI-C1`, `KI-C3`, `KI-C4`, `KI-C5`
- `TD-PORTS`, `TD-REQID`, `TD-METRICS`, `TD-SITECUST`,
  `TD-SLURM-AMD`, `TD-PSIJ`, and the duplicate `TD-TESTS`

`KI-D1` and `KI-D2` should remain `IN_PROGRESS`, but their evidence must stop
claiming mechanism closure. `PR-033` remains in progress, and its required
lower-tier evidence must be stated accurately.

This is a minimum correction list, not permission to relabel the remaining
records without rechecking each record's complete invariant.

## 6. Scale-claim correction

“Only 256 nodes remain” is wrong in two independent ways.

First, code-level failures above reproduce on one process or two nodes and do
not depend on scale. Second, the repository's current **provisional**
first-release ADR
(`doc/hardening/decisions/ADR-000-production-envelope.md:18-33`) sets
`qualification_target = 64` and explicitly says 256 nodes are **not** a release
requirement. The compatibility matrix admits that 16/64 are feasibility
smokes, with the 64-node production-gateway/target-architecture qualification
still owed. `artifacts/hardening/EXPERIMENT_PLAN.md:21` says the 64-node gate is
not even planned.

For the repository's provisional release envelope, once it is explicitly
approved, the missing sequence is:

1. clean packaged one-node lifecycle and recovery;
2. claimed engine/vendor smoke;
3. two-node negative/failure matrix on the final architecture;
4. four-node TP/PP and failure verification;
5. final 16-node and 64-node topology, gateway, distribution, readiness, and
   recovery qualification;
6. separate offsite/capability evidence or explicit production rejection.

The 128/256 gates are required only if the support envelope is approved and
expanded to claim those tiers. If they remain unclaimed, the product owner
must approve that scope and the ledger must record the corresponding
unsupported/accepted-limit decision rather than keeping eight records in
progress and calling 256 the release blocker.

## 7. Focused reproductions and checks

| Check | Result |
|---|---|
| Full repository pytest | `298 passed, 1 warning in 58.42s` |
| Hermetic deterministic lane | `291 passed, 7 skipped in 63.38s` |
| Configured Ruff check | passed |
| Omitted 256-node proxy client count | loaded as **256**, not bounded default |
| `num_replicas: true` / `8.9` | accepted as **1 / 8** |
| Eval `collect_stats: "false"` | loaded as **True** |
| Plan hash after changing deployment name / replica limit / Ray config | unchanged for all three |
| Hand-authored full-model marker without config/weights | accepted |
| Same-size corrupted weight | accepted |
| Status ABA with omitted revision | stale transition accepted |
| Lease successor between check and renew | successor overwritten |
| Lease successor between check and release | successor deleted |
| Readiness after rank disconnect | stayed `True` |
| EN-01 only in `not_applicable` | receipt accepted; role satisfied |
| Clean control `GOODBYE` | recorded as lost-lease failure |
| Reconnect without full snapshot | incremental observation accepted |
| Marker-only eval monitor | returned ready with source `marker` |
| Failed sole ClientLab point | CLI returned 0 |
| Deleted trace data with metadata retained | missing trace artifact reused |

The first attempted focused script failed before any check because the source
layout was not on `PYTHONPATH`; it was rerun with `PYTHONPATH=src:.` and all
listed reproductions completed. That setup error is not counted as a product
failure.

## 8. What should be credited

The audit should not erase useful work. The following are meaningful advances
that should be retained while their containing gates remain open:

- bounded authenticated framing, identity checks, and duplicate suppression in
  the control transport;
- generic process-group supervision with first-cause preservation;
- typed readiness/lifecycle vocabulary and useful pure helper tests;
- atomic JSON/YAML helpers and atomic run-group allocation;
- derived model-identity collision checking and AST-restricted expressions;
- HAProxy admin lockdown and native config preflight;
- a bounded metrics registry implementation and request-ID parsing helpers;
- port-lease and capability primitives, even though coverage is incomplete;
- deterministic and randomized hermetic test lanes;
- real two-node deployment/canary evidence and improved local diagnostics.

These are foundations and narrow repairs. They should be described as such,
not as closure of the broader production contracts.

## 9. Release recommendation

Do not tag, deploy, or advertise this revision as production-ready. Before any
additional large-scale run, finish the lower-cost correctness cutover:

1. make one compiled plan and generation identity authoritative in core, eval,
   and ClientLab;
2. make the allocation supervisor own real per-rank node supervisors and
   remove marker-driven state and nested Bash lifecycle ownership;
3. derive readiness from the immutable plan, include the selected external
   gateway, make it continuously revocable, and fail if durable publication
   fails;
4. require exact per-instance compatibility/distribution receipts and make
   missing EN-01 self-attestation fatal;
5. repair lease/port races and transactional model/source/venv/PP staging;
6. unify scheduler/submission/result/state behavior and make partial/fatal
   outcomes nonzero;
7. wire gateway policy, metrics, request IDs, cleanup, and packaged CI;
8. correct the ledger and produce the required manifests, verdicts, support
   matrix, and final audit before running the final 4/16/64 qualification.

Only after these code-level contracts pass should additional scale evidence be
treated as release qualification. A 256-node success cannot compensate for a
one-node false-ready, stale-state, silent-success, or identity failure.
