# Paper Experiment Preview — 2026-08-11 through 2026-08-13

Status: **in progress**. This ledger is updated only from sealed run artifacts.
It is not a final reproduction claim.

## Request and evidence rules

The campaign reuses the SC26 paper specifications, changing only the run-group
name and `client.num_runs` to `2`, as requested. For every cell, run 0 is the
warmup and run 1 is the reported measurement.

A **complete-success** cell requires all of the following:

1. the durable state is `SUCCEEDED`;
2. the result manifest has `complete: true` and no incomplete reasons;
3. every replay scheduled and completed the same number of requests;
4. every replay reports zero request errors; and
5. the immutable source, plan, site-profile, and allocation identities remain
   available in the run bundle.

The paper also intentionally measures overload and failure rates. Such a cell
must not be relabelled `SUCCEEDED` by the production control plane. It is a
**faithful measured-partial** only when the deployment reached canonical READY,
every request and rank shard is accounted for, all expected evidence entries
are sealed, the manifest's only incomplete reasons are the explicitly counted
request failures, and those failures are compared with the paper. Unexpected
errors remain rejected, not normalized away.

The requested order is low node count first, then 64 nodes. The 128- and
256-node cells remain last. A partial report is due immediately after a valid
64-node result, and again after valid 128- and 256-node results.

## Scope inventory

The materialized core inventory contains 42 specifications and 124 cells:

| Family | Specs | Cells | Notes |
|---|---:|---:|---|
| Proxy comparison | 20 | 60 | direct, HAProxy, Envoy, LiteLLM, and Ray Serve; stream/non-stream |
| Offered-traffic and trace workloads | 18 | 36 | 1 and 64 nodes; stream/non-stream |
| 405B pipeline-parallel scale | 4 | 28 | includes the late 128- and 256-node cells |

The accepted tables currently contain 47 rows but only 46 unique cells: the
64-node Envoy non-streaming cell appears once as historical measured-partial
evidence and once as the accepted zero-error replacement. Therefore 78 of the
124 inventory cells still require an accepted, rejected, externally blocked,
or explicit not-run disposition. The older pending list understated this work
by naming only twelve high-node proxy cells.

The twelve Ray Serve proxy cells use the explicit `RAY_SERVE_HEAD_ONLY`
validation exposure. They are supported as the paper's native, centralized Ray
Serve baseline, but cannot support a production-exposure claim. The contract
requires one head proxy, the full canonical replica set, and `PARTIAL` rather
than `SUCCEEDED` when the paper's overload trace produces counted proxy errors.
The paper's separate SGLang figure cannot currently be reproduced on this site:
the active site profile supports vLLM only and SGLang is not installed. No
vLLM result will be relabelled as SGLang evidence.

## Complete-success results

All rows below use two replays and report the measured replay (run 1).

| Experiment | Nodes | Stream | Requests | Errors | RPS | p50 (s) | p99 (s) | Provisional comparison |
|---|---:|:---:|---:|---:|---:|---:|---:|---|
| Proxy / HAProxy | 1 | yes | 6,600/6,600 | 0 | 101.745 | 4.230 | 6.572 | +6.7% vs historical mean; within prior spread |
| Proxy / HAProxy | 1 | no | 6,600/6,600 | 0 | 107.124 | 1.831 | 1.948 | valid; close to direct non-streaming baseline |
| Proxy / direct | 1 | yes | 6,600/6,600 | 0 | 102.908 | 3.645 | 5.354 | +3.6% vs historical mean; within prior spread |
| Proxy / direct | 1 | no | 6,600/6,600 | 0 | 107.151 | 1.775 | 1.887 | valid current baseline; historical comparison pending |
| Proxy / Ray Serve HeadOnly | 1 | yes | 6,600/6,600 | 0 | 102.315 | 3.157 | 6.648 | valid native-Ray baseline |
| Proxy / Ray Serve HeadOnly | 1 | no | 6,600/6,600 | 0 | 107.063 | 1.851 | 1.943 | valid native-Ray baseline |
| Proxy / Envoy | 1 | yes | 6,600/6,600 | 0 | 97.058 | 6.957 | 14.182 | route/target repair verified; tail exceeds HAProxy |
| Proxy / Envoy | 1 | no | 6,600/6,600 | 0 | 107.107 | 1.850 | 1.908 | route/target repair verified |
| Proxy / LiteLLM | 1 | yes | 6,600/6,600 | 0 | 85.645 | 12.511 | 25.960 | relaxed startup/request budgets and offline tokenizer verified |
| Proxy / LiteLLM | 1 | no | 6,600/6,600 | 0 | 107.036 | 1.851 | 2.340 | relaxed LiteLLM path verified |
| Proxy / Envoy | 4 | yes | 26,400/26,400 | 0 | 414.485 | 3.621 | 5.462 | final snapshot; 1.046x replica imbalance and clean teardown |
| Proxy / Envoy | 4 | no | 26,400/26,400 | 0 | 428.470 | 1.837 | 1.909 | final snapshot; 1.067x replica imbalance and clean teardown |
| Proxy / LiteLLM | 4 | no | 26,400/26,400 | 0 | 426.940 | 1.868 | 2.513 | `deb3f56`; two current-snapshot passes succeeded at 428.031/426.940 RPS; exact READY and clean 4/4 teardown |
| Proxy / Ray Serve HeadOnly | 4 | no | 26,400/26,400 | 0 | 285.182 | 13.866 | 45.857 | `deb3f56`; one native head proxy, no anchors/gateway, exact READY and clean 4/4 teardown |
| Proxy / Envoy | 16 | no | 105,600/105,600 | 0 | 1,712.636 | 1.850 | 1.916 | `4628bbf` snapshot; exact 208-app/192-replica READY and clean 16/16 teardown |
| Proxy / Envoy | 64 | no | 422,400/422,400 | 0 | 6,782.935 | 1.894 | 3.773 | `3ba15df`; warmup was also zero-error at 6,787.959 RPS; Envoy idle-pool fix, exact READY, complete manifest, and clean 64/64 teardown verified |
| OAT 8B baseline | 1 | yes | 5,880/5,880 | 0 | 95.346 | 1.896 | 2.209 | throughput essentially unchanged; p99 improved |
| OAT 8B baseline | 1 | no | 5,880/5,880 | 0 | 95.507 | 1.790 | 1.930 | valid current baseline; historical comparison pending |
| OAT 8B 2K/2K | 1 | yes | 216/216 | 0 | 1.664 | 70.171 | 82.732 | RPS -2.4% vs historical mean; tail is slower |
| OAT 8B 2K/2K | 1 | no | 216/216 | 0 | 1.637 | 70.163 | 80.966 | valid; stream/non-stream agree closely |
| OAT 8B 4K/2K (`4kx4k` spec name) | 1 | yes | 108/108 | 0 | 0.852 | 63.376 | 80.299 | RPS -5.9%; p99 about 21% slower; investigate variance |
| OAT 8B 4K/2K (`4kx4k` spec name) | 1 | no | 108/108 | 0 | 0.878 | 65.151 | 76.955 | valid; tail remains slower than historical stream baseline |
| OAT 120B | 1 | yes | 540/540 | 0 | 8.533 | 3.652 | 3.743 | consistent with the paper's 9 RPS offered-load cell |
| OAT 120B | 1 | no | 540/540 | 0 | 8.541 | 3.586 | 3.667 | stream/non-stream agree closely |
| OAT 8B Code | 1 | yes | 324/324 | 0 | 4.510 | 12.101 | 13.399 | agrees with paper ledger's rounded 5 RPS |
| OAT 8B Code | 1 | no | 324/324 | 0 | 4.513 | 11.995 | 13.229 | stream/non-stream RPS differ by 0.06% |
| OAT 8B BurstGPT | 1 | yes | 1,120/1,120 | 0 | 19.243 | 0.125 | 0.274 | RPS -0.08% vs prior result; p99 +9.0% |
| OAT 8B BurstGPT | 1 | no | 1,120/1,120 | 0 | 19.243 | 0.124 | 0.265 | stream/non-stream agree closely |
| OAT 8B Summary | 1 | yes | 648/648 | 0 | 9.803 | 6.710 | 8.169 | agrees with paper ledger's rounded 10 RPS after a clean-node retry |
| OAT 8B Summary | 1 | no | 648/648 | 0 | 9.838 | 6.724 | 8.041 | stream/non-stream RPS differ by 0.36% |
| OAT 8B Poisson | 1 | yes | 5,880/5,880 | 0 | 94.946 | 1.917 | 2.345 | +3.2% vs paper ledger's rounded 92 RPS |
| OAT 8B Poisson | 1 | no | 5,880/5,880 | 0 | 95.343 | 1.811 | 1.933 | stream/non-stream RPS differ by 0.42% |

The current evidence supports “still runnable” for these 32 complete-success
cells. It supports
“numbers stay the same” for throughput in the cells with historical evidence,
subject to a tail-latency warning for the long-context 4K/4K cell. Two replays
are a preview, not a confidence interval; the final report must not overstate
statistical equivalence.

## Faithful measured-partial results

| Experiment | Nodes | Stream | Requests | Errors | Success | RPS | p50 (s) | p99 (s) | Comparison |
|---|---:|:---:|---:|---:|---:|---:|---:|---:|---|
| OAT 8B Chat | 1 | yes | 648/648 | 3 | 99.54% | 8.987 | 7.240 | 20.495 | Matches paper's rounded 9 RPS, 0.5% errors, and 0.99 attainment |
| OAT 8B Chat | 1 | no | 648/648 | 3 | 99.54% | 9.119 | 7.192 | 19.665 | Same three deterministic trace records; close stream/non-stream agreement |
| Proxy / Ray Serve HeadOnly | 4 | yes | 26,400/26,400 | 1,684 | 93.62% | 125.634 | 95.212 | 164.546 | `deb3f56`; pass 0 had 1,357 errors; exact one-proxy READY, complete 4-rank gather, clean teardown |
| Proxy / LiteLLM | 4 | yes | 26,400/26,400 | 1,927 | 92.70% | 146.352 | 43.132 | 88.685 | `deb3f56`; pass 0 had 1,519 errors; full READY, bounded recovery, complete 4-rank gather, clean teardown |
| Proxy / LiteLLM | 16 | no | 105,600/105,600 | 8,652 | 91.81% | 787.910 | 20.171 | 51.610 | `4628bbf`; pass 0 had 8,882 resets; all requests accounted; clean 16/16 teardown |
| Proxy / Envoy | 16 | yes | 105,600/105,600 | 1 | 100.00% | 1,647.013 | 3.471 | 5.354 | `d0772d4`; pass 0 had zero errors; exact 418/418 readiness receipts and clean 16/16 teardown |
| Proxy / LiteLLM | 16 | yes | 105,600/105,600 | 54,127 | 48.74% | 340.251 | 129.178 | 245.995 | `d0772d4`; pass 0 had 52,130 errors; 300-second request budget allowed exact accounting and clean 16/16 teardown |
| Proxy / Ray Serve HeadOnly | 16 | no | 105,600/105,600 | 47,844 | 54.70% | 369.105 | 111.826 | 216.526 | `d0772d4`; pass 0 had 46,999 errors; exact one-proxy/192-replica READY and clean 16/16 teardown |
| Proxy / Ray Serve HeadOnly | 16 | yes | 105,600/105,600 | 73,007 | 30.86% | 312.737 | 158.880 | 276.375 | `aba3730`; pass 0 had 72,685 errors; bounded reconnect and readiness recovery completed; clean 16/16 teardown |
| Proxy / Envoy | 64 | no | 422,400/422,400 | 1 | 100.00% | 6,800.589 | 1.861 | 2.140 | `c7b63f7`; pass 0 was 422,400/422,400 with zero errors at 6,846.853 RPS; the measured error was one transient HTTP 503 connection termination; exact 1,666/1,666 readiness receipts and clean 64/64 teardown |
| Proxy / Envoy | 64 | yes | 422,400/422,400 | 157,063 | 62.82% | 4,633.058 | 18.184 | 27.695 | `3ba15df`; pass 0 was error-free at 4,931.039 RPS; measured saturation revoked and recovered readiness, all requests/rank shards were accounted, and teardown was clean 64/64 |
| Proxy / LiteLLM | 64 | no | 422,400/422,400 | 43,684 | 89.66% | 826.039 | 73.704 | 155.673 | `3ba15df`; pass 0 had 36,586 errors at 862.389 scheduled RPS; relaxed budgets allowed exact READY, full accounting, bounded readiness recovery, and clean 64/64 teardown |
| Proxy / LiteLLM | 64 | yes | 422,400/422,400 | 254,855 | 39.67% | 442.582 | 183.372 | 443.728 | `bf27004`; pass 0 had 284,163 errors at 511.522 scheduled RPS; the 3,600-second recovery floor allowed both saturated passes to finish with exact accounting, repeated fail-closed READY recovery, and clean 64/64 teardown |
| Proxy / Ray Serve HeadOnly | 64 | no | 422,400/422,400 | 292,594 | 30.73% | 585.733 | 164.084 | 494.869 | `ccccb82`; pass 0 had 295,458 errors at 562.719 scheduled RPS; exact one-proxy READY, repeated bounded recovery, six sealed artifacts, and clean 64/64 teardown |
| Proxy / Ray Serve HeadOnly | 64 | yes | 422,400/422,400 | 359,269 | 14.95% | 515.604 | 218.105 | 469.517 | `ccccb82`; pass 0 had 351,832 errors at 493.270 scheduled RPS; exact one-proxy READY, repeated bounded recovery, six sealed artifacts, and clean 64/64 teardown |

For the two OAT 8B Chat rows, both replays in both modes fail the same three
dataset records. Their declared
input/output lengths are 5,191/197, 5,649/165, and 20,438/680 against the
specification's 4,096-token model limit. This is not a new random regression:
the paper ledger records the chat cell at rounded 9 RPS and 0.5% errors, and the
paper summary records 0.99 attainment. The hardened executor correctly seals
all six expected evidence entries but marks the manifest incomplete and the run
`PARTIAL`; treating it as `SUCCEEDED` would violate the result contract.

This preview exposed a production API defect: oversized non-stream requests
returned HTTP 500, while streaming requests started HTTP 200 and then
terminated the SSE body with an unexpected EOF. The `release-v0.4.0` source
candidate now performs live-tokenizer context preflight and returns a typed,
correlated HTTP 400 before streaming headers. The sealed historical rows remain
unchanged, and the same three trace records must still be counted as rejected
requests when the affected cells are rerun.

## Rejected attempts and defects exposed by the preview

Rejected attempts remain on disk and are not counted above.

1. The first 64-node HAProxy streaming attempt failed before deployment. Every
   rank authenticated successfully, but the pre-START heartbeat used the
   remainder of a two-second polling interval (about 0.5 seconds) instead of
   the plan's 30-second control lease. A rank disconnect therefore failed the
   campaign prematurely. The fix anchors pre-START and maintenance heartbeats
   to the last acknowledged control message and checks round-trip success.
2. A 4-node HAProxy non-streaming check exposed a vLLM V1 rendezvous collision.
   ExaServe reserved and supplied a unique `master_port`, but vLLM 0.15's
   `UniprocExecutor` ignored it and independently selected the same ephemeral
   port in two actors on one node. The fix supplies vLLM's supported `VLLM_PORT`
   input while retaining the port lease through engine construction.
3. Candidate `b7c4f6b` passed its complete login-node gate with 1,234 Python
   tests passed and 19 skipped. The current immutable candidate `4628bbf`
   adds authoritative fixed-delivery-set shutdown accounting and an eval-side
   cleanup-evidence gate. Candidate `d0772d4` additionally retains bounded
   receipt batches across permitted control reconnects. Its complete Python
   gate passes 1,240 tests with 19 skipped; its full Python lint gate, Go tests,
   and focused reconnect tests are also clean. Candidate `aba3730` adds the
   eval-to-canonical control/readiness override path. Candidate `4da2749`
   additionally seals snapshots against runtime writes and suppresses false
   attestation failures from processes that never identify as an engine. Its
   complete gate passes 1,244 Python tests with 19 skipped, repository-wide
   Ruff, and all Go-client tests. Candidate `590b9dc` restores the plan-owned
   clean-stage transaction; its complete gate passes 1,247 Python tests with
   19 skipped, repository-wide Ruff, and all Go-client tests. Candidate
   `c0e4b3f` additionally rejects parent-traversing model roots; its complete
   gate passes 1,252 Python tests with 19 skipped, repository-wide Ruff, and
   all Go-client tests. Candidate `58040d4` preserves typed lease errors across
   multiprocessing worker boundaries. Candidate `97ab2a7` separates Aurora's
   256-node physical validation ceiling from the unapproved candidate-64
   release target. Its complete gate passes 1,254 Python tests with 19 skipped,
   repository-wide Ruff, and all Go-client tests. Candidate `43779c5` makes
   private source transactions compatible with sealed read-only snapshots.
   Candidate `d837c54` additionally contains transport write resets inside the
   reconnectable rank-session boundary. Candidate `9c6dfec` batches only the
   disposable live binding projection while retaining an immutable event per
   accepted transition. Its complete gate passes 1,257 Python tests with 19
   skipped, repository-wide Ruff, 131 focused control/reconnect tests, and all
   Go-client tests. Current 16-node rows are materialized from
   `4628bbf`, `d0772d4`, or `aba3730` as named; accepted 4-node rows remain valid
   `b7c4f6b` evidence.
4. Some early retries failed on an unhealthy node or before a complete result
   marker was produced. Clean retries were required; partial payloads were not
   promoted to results.
5. The first 1-node summary streaming attempt reached READY and completed its
   warmup, then one XPU engine process reported an Aurora GPU page fault during
   the measured repetition. The durable outcome is preserved as `PARTIAL`
   (648/648 completed, 25 errors) and is not counted. An otherwise identical
   clean-node retry completed both repetitions with zero errors and sealed
   `SUCCEEDED` at 9.803 RPS.
6. The first 1-node Envoy attempts returned HTTP 404 because the renderer did
   not select a canonical replica route or rewrite `/v1/...`. The repaired
   generated route now reaches the exact canonical targets; both final 1-node
   modes completed 6,600/6,600 requests with zero errors. The failed attempts
   remain rejected rather than being merged with the successful evidence.
7. Interrupting those known-broken readiness waits shut the deployments down
   cleanly but allowed `KeyboardInterrupt` to escape the executor, leaving the
   durable status at `RUNNING`. Cancellation must terminate in an explicit
   non-success state and seal its reason; the abandoned state is not accepted
   as campaign evidence.
8. The first 1-node LiteLLM attempts expired under a generic 30-second startup
   deadline. The final plan compiles a 120-second startup floor, 300-second
   gateway request timeout, 360-second recovery interval, offline accounting
   tokenizer, supported Uvicorn workers, and 120-second keepalive. Both final
   1-node modes completed 6,600/6,600 requests per pass with zero errors.
9. A native 4-node HeadOnly diagnostic on candidate `52251dd` reached exact
   READY (48/48 replicas, one healthy proxy, 105/105 receipts) and completed
   both 26,400-request passes. It recorded 2,005 and 1,574 transport errors and
   about 110 successful requests/s, with a 17.88x replica load imbalance. This
   is consistent with the paper's intended centralized-baseline collapse and
   remains measured-partial, not a failed launch or a production result.
10. A temporary `86447bb` ablation disabled Ray's default local-node/AZ routing.
    It reduced replica imbalance to 1.06x but still produced 1,380 and 2,091
    single-listener transport errors. That change is rejected from the paper
    baseline because it tunes native default behavior; it also exposed a real
    teardown race in which a fast rank's post-DRAIN GOODBYE could precede the
    head's sequential authorization. Candidate `b7c4f6b` fixes that ordering
    atomically. Final 4-node HeadOnly, Envoy, and LiteLLM runs all proved exact
    DRAIN 4/4, GOODBYE 4/4, clean shutdown reports, and terminal publication.
11. Final-snapshot 4-node execution confirms that the prior generic 30-second
    deadline was invalid for LiteLLM. Both LiteLLM modes reached exact READY
    only after the 48-replica deployment took 67--69 seconds and the gateway
    completed its own startup and canary. The compiled 120-second startup
    floor, 300-second request timeout, and 360-second recovery interval allowed
    the streaming trial to drain requests for up to 178 seconds rather than
    manufacturing early client timeouts.
12. The first 16-node Envoy non-streaming attempt completed both 105,600-request
    passes with zero request errors and all 16 ranks exited zero, but cleanup
    reported only 14 DRAIN and GOODBYE acknowledgements. The control listener
    had atomically authorized every acknowledgement; the head subsequently
    re-queried mutable connection state and skipped ranks that disconnected
    quickly. This attempt is rejected. Candidate `4628bbf` freezes the exact
    successful delivery set, waits for those command results concurrently, and
    makes the eval accept a result only after validating a same-generation,
    clean terminal shutdown report. The identical retry produced 16/16 DRAIN,
    16/16 GOODBYE, `STOPPED`, and a complete `SUCCEEDED` manifest.
13. The 16-node LiteLLM non-streaming cell proves that relaxed startup is
    necessary and sufficient for launch: Ray Serve deployment took about 74
    seconds before the external gateway could start, after which the declared
    endpoint reached exact READY. Both replays accounted for 105,600 requests,
    and shutdown was clean at 16/16 ranks. It is nevertheless measured-partial:
    passes 0 and 1 recorded 8,882 and 8,652 client-side `connection reset by
    peer` failures. The owned eight-worker gateway stayed alive, and its
    64-KiB diagnostic tail retained healthy HTTP 200 traffic but dropped about
    14.8 MiB of earlier access output. The 134.5x replica request imbalance and
    reset rate are under investigation as a LiteLLM head-gateway saturation or
    routing limitation; neither errors nor a tuned replacement topology will
    be hidden inside the paper baseline.
14. A first 16-node Envoy streaming attempt on `4628bbf` completed the replay
    but exposed a shutdown command-delivery race: all 16 ranks acknowledged
    DRAIN while only eight GOODBYE receipts were retained. Candidate `ccd683f`
    reserved a bounded part of the shared shutdown deadline for rank exit, but
    its retry exposed a separate reconnect defect before READY. A receipt
    forwarder could drain a local batch, lose the authenticated control
    connection transiently, reject the first submission, and discard the
    remainder. Candidate `d0772d4` retains the unchanged bounded batch across
    reconnects and treats rejection as terminal only when the control channel
    publishes an authoritative failure. The identical retry reached exact
    READY (418/418 receipts), accounted for both 105,600-request passes, and
    completed 16/16 DRAIN and GOODBYE. Its measured pass has one request error,
    so the durable state remains `PARTIAL` rather than being overclaimed.
15. The 16-node LiteLLM streaming cell confirms that 30 seconds is not a viable
    lifecycle or request deadline. Ray Serve deployment alone took about 74
    seconds before LiteLLM startup and endpoint canary; under the paper load,
    the two replay arms took 302.7 and 310.4 seconds to account for every
    request. Readiness correctly moved from READY to VALIDATING when live
    canaries timed out under saturation and re-persisted READY after recovery.
    The measured arm recorded 54,127 request errors and 51,473 successes. Across
    both arms, all 106,257 errors are attributable to the centralized ingress:
    88,150 connection-establishment timeouts and 18,107 connection resets, with
    no unclassified request failures. All six evidence entries were sealed and
    teardown proved 16/16 DRAIN,
    16/16 GOODBYE, and zero rank exit codes. This is a faithful overload result,
    not a launch failure and not a complete-success cell.
16. The first final-snapshot 16-node HeadOnly streaming attempt reached all 192
    replicas in about 65 seconds, then eight rank control sessions entered
    recovery during the synchronized actor-startup burst. The generic
    60-second reconnect test default expired before their replacement snapshots
    could be accepted, so the attempt failed closed before READY and is
    rejected. The canonical deployment compiler already supported an explicit
    `deployment.control.reconnect_grace_s`, but eval's YAML frontend rejected
    the entire `deployment.control` field. Candidate `aba3730` closes that
    schema-convergence defect, also exposes canonical readiness overrides, and
    keeps canonical compiler validation authoritative. The paper HeadOnly spec
    now hashes a bounded 300-second reconnect grace into its run identity. Its
    retry reproduced the burst, recovered all 16 rank sessions and 417/417
    receipt slots, then reached exact READY without degraded evidence.
17. The accepted 16-node HeadOnly streaming retry completed both 105,600-request
    arms in 307.7 and 337.7 seconds. READY was revoked while the centralized
    proxy was saturated and re-persisted after both bounded recoveries. Passes
    0 and 1 recorded 72,685 and 73,007 request errors: respectively 63,597 and
    63,211 connection timeouts plus 9,088 and 9,796 connection resets. All four
    client shards per pass were sealed, server statistics covered 192/192
    replicas, and teardown proved 16/16 DRAIN, 16/16 GOODBYE, and a clean
    `STOPPED` report. This is the intended centralized-baseline bottleneck, not
    a production exposure result.
18. Importing the CLI from an immutable snapshot without
    `PYTHONDONTWRITEBYTECODE=1` can write `__pycache__` into that snapshot before
    artifact validation runs, causing a self-inflicted snapshot-hash mismatch.
    The rendered scheduler job already exported the guard and the accepted run
    used it. Candidate `4da2749` additionally publishes every snapshot file and
    directory without write permission and validates that seal on reuse, so a
    direct `run execute` import cannot mutate its own source artifact.
19. During the long HeadOnly replay, every Serve actor emitted an
    `engine attestation deadline expired` diagnostic after 600 seconds with
    zero attempts. Canonical READY and the sealed receipt manifest prove the
    192 actual engine-core receipts were present; these were non-engine actor
    watchers that could never produce engine evidence. The messages did not
    alter lifecycle state, but starting those irrelevant watchers was noisy and
    wasteful. Candidate `4da2749` now lets an unidentified helper's bounded
    classifier expire silently while preserving exact diagnostics for any
    process that did identify as a planned engine slot.
20. Audit before the 64-node checkpoint confirmed that every paper
    validation/full spec still compiled `runtime.clean_stage=true`, but the
    shell-to-Python cutover had left that policy without a runtime consumer.
    Candidate `590b9dc` restores it inside the owned model-staging step. One
    bounded MPI/srun transaction removes only the exact plan-declared model
    publications plus matching abandoned candidate/quarantine paths, requires
    an identity-checked receipt from every bound rank, and fails the staging
    step on any error. It deliberately does not resurrect the legacy script's
    broad deletion of Ray sessions, port leases, source trees, or unrelated
    `/tmp` state.
21. A final safety review found that “absolute” paths containing a lexical
    `..` component could escape their apparent staging root. Candidate
    `c0e4b3f` rejects parent traversal in both `SiteProfile` and
    `DeploymentPlan` validation and repeats the check at the destructive
    clean-stage boundary. The `c0e4b3f` 64-node bundles were retired without
    execution after later profile fixes changed their canonical identity.
22. The six two-pass 128/256 scale specifications validate with the original
    paper queue/walltime matrix, but their first materialization attempt was
    stopped before any run bundle was published. Generating the 128/256 weak-
    scaling traces expanded the two planner workers to roughly 2.7 GiB and two
    CPU cores each, which is beyond the login-node preparation allowance. The
    retained partial root contains only run-group metadata, not an executable
    run. High-tier trace and bundle materialization was therefore moved into a
    validated one-node `subjob`; this is an execution-placement correction, not
    accepted experiment evidence.
23. That compute-side retry encountered the two leases left by the killed
    login-node trace workers. The lease itself behaved correctly, but
    `LeaseHeldError` required `(path, owner)` to construct and could not be
    unpickled by the materializer's parent result thread. Candidate `58040d4`
    preserves both constructor fields across multiprocessing. The two exact
    stale leases were reclaimed only after their owner PIDs were verified dead;
    partial trace data remained subject to normal transactional replacement.
24. Once the exception crossed the boundary correctly, canonical compilation
    rejected 128 nodes because the default Aurora `SiteProfile.max_nodes` mixed
    a physical validation ceiling with the proposed production ceiling of 64.
    Candidate `97ab2a7` sets the physical/campaign validation ceiling to 256
    while leaving the declared production envelope at
    `qualification_target_nodes=64`, unapproved. Checked-in 128/256 HAProxy
    specs now explicitly declare validation mode. Plans above 64 resolve to
    `alcf-aurora-unqualified-generic`; none is production evidence.
25. The final compute-side materialization sealed all twelve high-tier bundles:
    six at 128 nodes with `debug-scaling`/one hour and six at 256 nodes with
    `prod`/2.5 hours. Every bundle has two replays and uses immutable, read-only
    snapshot `97ab2a7f0e6b67a9f921af9e85c39336ff698282-v2c`. LiteLLM retains
    its 120-second gateway-start floor, 300-second gateway timeout, and
    360-second recovery deadline; HeadOnly retains its explicit 300-second
    reconnect grace. These bundles are prepared but deliberately unsubmitted
    until the 64-node checkpoint is reported.
26. The first 64-node execution of `97ab2a7` failed before source distribution.
    The immutable snapshot was correctly published read-only, but the private
    source transaction preserved those directory modes and then attempted to
    create its generated compatibility overlay inside the read-only copy. Its
    cleanup path consequently could not remove that copy either. Candidate
    `43779c5` adds owner permissions only to directories in the unique private
    copy; it does not mutate the immutable snapshot or broaden copied file
    permissions. A regression test constructs, extends, and removes a genuinely
    read-only release tree.
27. The first `43779c5` 64-node Envoy non-streaming bundle passed source
    distribution, produced 64/64 clean-stage receipts, broadcast a fresh
    32.13-GB model publication to every rank with `cache_reused=false`, and
    established all 64 supervisors. During the synchronized 768-engine startup
    burst, however, the generic 60-second reconnect grace expired while
    `serve.run_many` was registering applications. The failed generation is
    rejected. All twelve proxy-preview specifications now explicitly hash a
    300-second control reconnect grace, separate from readiness, gateway-health,
    and canary deadlines.
28. The second `43779c5` attempt proved the longer grace was necessary but then
    exposed a different transport defect. A socket reset raised by
    `StreamWriter.drain()` on one heartbeat response escaped the rank-session
    handler and was incorrectly promoted to listener-wide `CONTROL_FAILURE`.
    This contradicted the contract, under which EOF/reset establishes rank loss
    and allows a same-identity replacement session until grace expiry. Candidate
    `d837c54` contains only connection/OSError write failures at that boundary;
    callback, schema, and identity failures remain fatal.
29. The `d837c54` retry contained the write reset correctly, but rank 21 still
    could not reconnect before the explicit 300-second grace ended. At that
    point the durable component-binding store had emitted roughly 4,000
    one-transition event files and had also rewritten its growing `current.json`
    materialization once per event. The immutable events are the authoritative
    history, so candidate `9c6dfec` retains every event but batches the disposable
    current projection in groups of 64 and forces an exact flush before READY
    and during shutdown. This removes scale-dependent Lustre rewrites from the
    listener's hot path without moving control decisions to another thread or
    weakening receipt durability.
30. The next 64-node retry exposed an upstream Ray 2.53 defect after exactly
    300 seconds: each Serve proxy timeout completed the `asyncio` destination
    future directly, and the later source completion made `_chain_future`
    assert once per proxy. Candidate `a631e22` added an exact-version generated
    `proxy_state.py` overlay that returns a separate timeout-result future and
    permits the wrapped source future to complete normally. Its first scale
    retry still failed because the overlay was delivered only to the
    `deployment` role, while Ray's ServeController inherits the `ray_head`
    environment and launches the ProxyActors. Candidate `c7b63f7` assigns the
    same hash-verified overlay to both roles. A one-node real Ray/Serve smoke
    proved activation in the driver, ServeController, and ProxyActor; the next
    64-node deployment crossed the 300-second boundary without the assertion.
    No installed Ray file is modified.
31. The `c7b63f7` 64-node Envoy non-streaming cell reached canonical READY with
    832 applications, 768/768 replicas, 64/64 healthy proxies, a healthy Envoy
    endpoint and 1,666/1,666 exact receipts. Both 422,400-request arms were
    fully accounted. Pass 0 had zero errors; pass 1 had one HTTP 503
    `upstream connect error or disconnect/reset before headers` at 3.345
    seconds, with no matching proxy, replica, or control failure. The run is
    therefore faithfully `PARTIAL`, not relabelled success. Teardown proved
    64/64 DRAIN, 64/64 GOODBYE, and zero rank exit codes. A diagnostic
    accidentally stopped only the eval driver for several minutes before
    READY; the serving actors continued to reach `RUNNING` and replay began
    only after the driver resumed, so request metrics remain usable but the
    recorded cold bring-up duration is excluded.
32. HeadOnly did not need to be restored. The authoritative hardening plan and
    the checked-in paper preview specs already define
    `RAY_SERVE_HEAD_ONLY`: one root-route native Ray proxy, the full replica
    set, and no ExaServe anchor applications. It is valid paper-baseline
    evidence and intentionally is not a production-exposure claim. LiteLLM
    likewise no longer uses a generic 30-second budget: its compiled contract
    retains a 120-second startup floor, 300-second gateway request timeout,
    360-second recovery deadline, and 120-second keepalive.
33. A 64-node LiteLLM non-streaming diagnostic on `c7b63f7` reached exact
    canonical READY before replay: 64 rank sessions, 64 exact Ray nodes, 832
    applications, 64 healthy Serve proxies, 768/768 replicas, 1,666/1,666
    receipts, a healthy LiteLLM gateway, and a successful advertised-endpoint
    canary. Under the paper load, the centralized gateway saturated and live
    readiness correctly revoked. The replay client carried a 3,600-second
    request timeout; this was not a hidden 30-second abort. The diagnostic was
    operator-stopped and is rejected as result evidence, but it exposed a real
    teardown bug: the multiprocess LiteLLM worker group received only a fixed
    five-second cleanup slice and could outlive its owner.
34. Candidate `deb3f56` budgets gateway cleanup proportionally within the
    existing global cleanup deadline, capped at 30 seconds. It does not change
    routing, overload semantics, result classification, or the paper topology.
    The complete local gate passes 1,260 Python tests with 19 skipped,
    repository-wide Ruff, and all Go tests. Current four-node LiteLLM
    non-streaming then sealed `SUCCEEDED`: both 26,400-request passes completed
    with zero errors at 428.031 and 426.940 RPS, followed by 4/4 DRAIN, 4/4
    GOODBYE, four zero rank exits, and no residual serving processes.
35. The `deb3f56` four-node LiteLLM streaming retry reached the same exact READY
    contract and reproduced the documented centralized-proxy collapse without
    hanging. Passes 0 and 1 accounted for every request, recorded 1,519 and
    1,927 errors, and drained for roughly 91--120 seconds. Readiness revoked on
    60-second live-canary timeouts and recovered inside the separate 360-second
    recovery budget. The durable state is correctly `PARTIAL`, with complete
    four-rank gathering and clean ordered teardown.
36. The `deb3f56` four-node HeadOnly non-streaming retry proves that HeadOnly is
    implemented as the contract requires: `gateway.kind=none`, one native Ray
    Serve application and head proxy, 48 replicas, no proxy anchors, and
    105/105 exact receipts. Both 26,400-request passes completed with zero
    errors at 283.680 and 285.182 RPS. Its much worse 13.866-second measured
    p50 and 45.857-second p99 demonstrate why it remains a validation-only
    centralized paper baseline rather than production exposure. A subsequent
    streaming run reached exact READY and completed its first pass, but the
    214.74-second pass plus 75-second cooldown could not safely fit a second
    pass and teardown inside the remaining PBS walltime. It was deliberately
    interrupted during cooldown, recorded `CANCELLED`, completed 4/4 DRAIN and
    GOODBYE with no residual processes, and is not accepted as two-pass result
    evidence. Fresh immutable run group `run1` then reached the same exact
    READY contract and completed both 26,400-request streaming passes. Passes 0
    and 1 recorded 1,357 and 1,684 errors at 118.759 and 125.634 RPS; every
    request was accounted, readiness recovered after saturation, all four
    result ranks were gathered, and teardown proved 4/4 DRAIN, 4/4 GOODBYE,
    four zero rank exits, and no residual serving processes. The durable
    `PARTIAL` state is the faithful result of the intentionally centralized
    one-proxy baseline, not a launch or cleanup failure.
37. A fresh 64-node Envoy streaming qualification exposed two independent
    scale-only control-path defects after the earlier paper cells had already
    passed. On the first attempt, synchronous durable receipt writes on Lustre
    starved heartbeat and reconnect handling. Candidate `4ce2446` moved those
    writes to one bounded off-loop writer while retaining exact, locked rank
    snapshots. The retry then reached exact READY with 64 Ray nodes, 832 Serve
    applications, 64 healthy proxies, 768/768 replicas, and 1,666/1,666 slots.
    Both 422,400-request passes were fully accounted: pass 0 recorded 149,956
    Envoy upstream-connect-timeout HTTP 503s at 5,525.510 scheduled RPS and
    pass 1 recorded 85,147 at 5,309.507 scheduled RPS. Readiness revoked under
    overload and recovered after each pass. All 64 ranks acknowledged DRAIN
    and GOODBYE and exited zero, but the attempt is rejected because no durable
    `shutdown_report.json` was published.
38. The missing report was caused by queuing one durable revocation per healthy
    receipt during expected GOODBYE, consuming the global shutdown watchdog.
    Candidate `910cd54` preserves issued receipts on an expected, acknowledged
    disconnect, still revokes them on an unexpected disconnect, and reports an
    unclean bounded-writer timeout without performing an unbounded join. A
    two-node Envoy teardown gate then reached exact READY, completed both
    13,200-request passes without errors, published a complete result manifest
    and a clean `STOPPED` shutdown report, received 2/2 DRAIN and GOODBYE, and
    left no residual processes. Its outer executor still waited the full
    120-second grace interval on an already-exited zombie group leader.
39. Candidate `793a780` reaps an exited backend leader while polling the process
    group, eliminating that false grace wait. The complete current-candidate
    gate passes 1,265 Python tests with 19 skipped, repository-wide Ruff, and
    all Go tests. This is the serving-code source for the two-node qualification
    bundles; its source snapshot hash is
    `3c371a986ea6cd9d1f2d5fbcec8da0377a624b4f3acaadd4a5a176b48a2f9398`.
40. The first current-candidate LiteLLM teardown gate deliberately sourced the
    site LiteLLM environment in the parent shell, as the site helper suggests.
    That activated a NumPy 2.4 environment incompatible with the framework
    vLLM/Numba runtime, which requires NumPy 2.2 or older, so model actors
    failed before READY. The attempt was stopped immediately and completed
    bounded 2/2 teardown with no residuals. The feasible isolation is to keep
    the Ray parent and model actors in `env_aurora` and launch only the gateway
    from the plan-bound `/home/wenyiw/venv/litellm/bin/litellm` executable.
    This retains one authoritative Python control plane and prevents gateway
    packages from contaminating the model runtime.
41. The isolated LiteLLM retry on `793a780` used an explicit 180-second gateway
    startup deadline, 420-second recovery deadline, 300-second request timeout,
    120-second keepalive, offline tokenizer mode, and 3,600-second client
    request timeout. Ray/vLLM deployment alone took about 68 seconds, directly
    disproving a generic 30-second startup budget. The run reached exact READY
    with two Ray nodes, 26 Serve applications, 24/24 replicas, 54/54 slots, a
    healthy LiteLLM gateway, and a successful model canary. Its two
    13,200-request passes had zero errors at 209.898 and 214.094 RPS. The run
    sealed `SUCCEEDED` with a complete manifest, clean shutdown report, 2/2
    DRAIN and GOODBYE, zero rank exits, and no residual processes. The
    contract's separate 30-second cap remains appropriate only for the
    multiprocess gateway's share of the overall cleanup watchdog.
42. A current-candidate HeadOnly gate confirms no code needed to be restored.
    The plan compiles to `RAY_SERVE_HEAD_ONLY` with `gateway.kind=none`, one
    native root application, one healthy head proxy, all 24 replicas, no
    anchors or external gateway, and 53/53 exact slots. Both 13,200-request
    passes completed with zero errors at 213.569 and 213.601 RPS. The manifest
    and shutdown report are complete and clean, both ranks acknowledged DRAIN
    and GOODBYE and exited zero, and both nodes were residual-free. This remains
    the contract-required validation and paper-comparison topology, not a claim
    that one centralized head proxy is suitable production exposure.
43. The qualification also exposed documentation drift: `AGENTS.md` and the
    execution plan said to activate the LiteLLM virtualenv for a mixed
    Ray/vLLM plus LiteLLM run, while the compiler, generated PBS job, and
    `plan_adapter` correctly require an `env_aurora` parent and a separately
    bound LiteLLM executable. Candidate `e6b5ba8` makes that isolation rule
    authoritative in both documents. No serving code changed. All six 64-node
    variants were resealed from this final candidate with source snapshot hash
    `564db0df8b8ee949b1a8ce795d54bfa2784bf936cc98d101adbd6a4d83016c0a`.
44. The first `e6b5ba8` 64-node Envoy non-streaming execution reached exact
    READY and completed its first 422,400-request replay with zero errors. Its
    stdout then entered the required 75-second cooldown, but delayed buffered
    application logs made the controller appear stalled. The operator
    interrupted the run before the second replay. The immutable run remains
    correctly `CANCELLED`; ordered teardown still proved 64/64 DRAIN, 64/64
    GOODBYE, every rank exiting zero, and no residual serving processes. It is
    rejected as result evidence, and the mistaken diagnosis is retained here
    so transient Ray log silence is not used as a future failure predicate.
45. Fresh immutable `run1` on the same `e6b5ba8` source reached canonical READY
    with 64 exact Ray nodes, 832 applications, 64 healthy Serve proxies,
    768/768 model replicas, 1,666/1,666 exact receipt slots, healthy Envoy, and
    a successful advertised-endpoint canary. Bring-up took 696.55 seconds.
    Both 422,400-request replays completed without error: warmup delivered
    6,849.017 RPS with 1.864-second p50 and 2.167-second p99; the measured replay
    delivered 6,813.228 RPS with 1.880-second p50 and 4.696-second p99. The run
    sealed `SUCCEEDED`, a complete six-entry manifest, 64/64 DRAIN and GOODBYE,
    64 zero rank exits, and a residual-free allocation.
46. The accepted run also measured a Ray-internal scale hazard rather than a
    model, node, or gateway failure. All 768 replicas were healthy in about 81
    seconds, but Ray's single Serve controller was still serializing hundreds
    of application state transitions when all 64 per-node proxy health checks
    expired together at the configured 300 seconds. The synchronized timeout
    callbacks delayed application publication until a later bulk catch-up; no
    proxy was actually unhealthy. Candidate `4327fe9` makes the internal proxy
    watchdog at least the complete canonical `initial_deadline_s` (3,600
    seconds by default). ExaServe's authenticated node-local proxy probes,
    observation freshness, canaries, revocable READY, and recovery deadline
    continue to own prompt failure detection, so this prevents dependency
    preemption without weakening the production readiness predicate. The gate
    passes 1,238 Python tests with 19 skipped, repository-wide Ruff, and all Go
    tests.
47. All six post-fix 64-node variants are materialized from clean commit
    `4327fe9`, source snapshot hash
    `c4cda1f71ab675e9437a0f938af032e57e009d5e6905b5efe16090294f2cbfb5`.
    Every plan resolves `initial_deadline_s=3600` and the Ray-internal proxy
    health timeout to the same 3,600-second bound. LiteLLM retains the explicit
    180-second bind deadline, 420-second recovery deadline, 300-second gateway
    request timeout, 120-second keepalive, offline tokenizer mode, and
    3,600-second replay request deadline. HeadOnly remains the exact one-native-
    proxy validation topology required by the paper contract. A planner retry
    that omitted the Go module failed before creating any run identity; the
    successful immutable materializations were performed with both framework
    and Go modules loaded.
48. The first `4327fe9` Envoy non-streaming identity reached exact canonical
    READY after 733.47 seconds and completed both 422,400-request passes. Pass
    0 was error-free at 6,753.877 RPS. Pass 1 delivered 6,803.514 scheduled RPS
    but recorded 13 identical Envoy HTTP 503 connection-termination responses,
    all 3.05--3.92 seconds into the pass and distributed across all four client
    ranks. This is the signature of upstream connections last used early in
    pass 0 being closed by Ray Serve's 90-second idle timeout while remaining
    reusable in Envoy's pool across the 75-second cooldown. Shutdown then
    stopped ingress and Serve but exhausted the former 120-second whole-run
    cleanup watchdog after only 55/64 DRAIN and GOODBYE acknowledgements; all
    64 allocated nodes were independently verified residual-free. The identity
    remains correctly `FAILED` and is rejected. Candidate `3ba15df` makes
    Envoy's hash-bound upstream idle timeout 60 seconds, so Envoy retires its
    side before Ray, and raises the single outer cleanup watchdog to 300
    seconds without weakening the all-rank predicate. Its complete gate passes
    1,240 Python tests with 19 skipped, repository-wide Ruff, Envoy 1.32.3
    configuration validation, and all Go tests. All six replacement 64-node
    bundles share source snapshot hash
    `e78d6ba67d5da84bae37f053dc643f35d8879c473b91828812669b16973325b9`,
    resolve the Ray proxy watchdog to 3,600 seconds, and resolve cleanup to 300
    seconds. LiteLLM still has explicit 180-second startup, 300-second request,
    420-second recovery, 120-second keepalive, and 3,600-second client budgets;
    HeadOnly remains the unchanged contract-required native baseline.
49. Both replacement `3ba15df` Envoy cells are now sealed from one validated
    64-node PBS allocation. Non-streaming reached exact READY in 704.82 seconds
    and completed both 422,400-request passes without error at 6,787.959 and
    6,782.935 RPS. Measured p50/p99 were 1.894/3.773 seconds. This directly
    closes the stale-upstream defect: pass 2 had no early connection-
    termination 503s. Streaming independently reached exact READY in 689.58
    seconds. Its warmup was error-free at 4,931.039 RPS; the measured pass
    accounted for all 422,400 requests, with 157,063 explicit errors, 4,633.058
    scheduled RPS, and 2,910.326 successful RPS. Readiness revoked under the
    overload and recovered after each pass. The durable states are correctly
    `SUCCEEDED` and `PARTIAL`, respectively; both produced all six expected
    result entries, clean `STOPPED` shutdown reports, 64/64 DRAIN and GOODBYE,
    64 zero rank exits, and residual-free allocations. This also closes the
    former 55/64 cleanup truncation under the 300-second outer watchdog.
50. The replacement `3ba15df` LiteLLM non-streaming cell proves that the
    isolated gateway is runnable at 64 nodes when its deadlines reflect the
    actual dependency behavior. The mixed parent remained in `env_aurora`; the
    plan launched the hash-bound
    `/home/wenyiw/venv/litellm/bin/litellm` executable with a 180-second bind
    deadline, 300-second gateway request timeout, 420-second recovery deadline,
    120-second keepalive, offline tokenizer mode, and 3,600-second replay
    request budget. Exact canonical READY followed 696.55 seconds of backend
    bring-up and proved 64 Ray nodes, 832 applications, 64 healthy proxies,
    1,666/1,666 receipts, 768/768 replicas, a healthy LiteLLM gateway, and a
    successful advertised-endpoint canary. Both 422,400-request passes were
    fully accounted: pass 0 completed in 489.80 seconds with 36,586 errors at
    862.389 scheduled and 787.693 successful RPS; pass 1 completed in 511.36
    seconds with 43,684 errors at 826.039 scheduled and 740.611 successful RPS,
    with 73.704-second p50 and 155.673-second p99. READY revoked during gateway
    saturation and recovered after each pass. The identity is correctly
    `PARTIAL`, with only the explicit request-error reasons in its six-entry
    manifest. Shutdown is clean and `STOPPED`: 64/64 DRAIN, 64/64 GOODBYE, all
    rank exits zero, the gateway and deployment return zero, the cleanup
    deadline is not exhausted, and an independent all-node probe found 64
    clean nodes and zero residual ExaServe, Ray, LiteLLM, or vLLM processes.
    Thus the former 30-second policy was invalid, but relaxing it does not make
    centralized LiteLLM capable of sustaining the paper's 64-node offered load.
51. The first `3ba15df` LiteLLM streaming identity is rejected and records the
    next measured boundary rather than a performance row. It reached the same
    exact 64-node READY after 706.49 seconds of backend bring-up, and the owned
    gateway remained alive and accepted TCP under load. The first replay pass
    fully dispatched and drained its client partitions in 608.55--641.94
    seconds, but continuous end-to-end canary failure exhausted the explicitly
    configured 420-second recovery deadline before the pass finished. The
    composition correctly failed closed with `READINESS_RECOVERY_EXPIRED`; its
    forced Serve drain then reported six still-active replica records before
    all 64 supervisors acknowledged DRAIN and GOODBYE and exited zero. The
    executor nevertheless left the replay subprocess in its 75-second
    inter-pass cooldown after the backend had exited. Continuing would only
    have measured a dead endpoint, so the executor was cancelled; durable run
    state is `CANCELLED`, no result was accepted, the shutdown report is clean
    with no exhausted outer deadline, and an independent probe found all 64
    nodes residual-free. Candidate work following this identity makes
    LiteLLM's recovery floor at least the resolved initial-readiness horizon
    (3,600 seconds on Aurora), still finite and plan-bound, and makes the
    executor terminate the owned replay process group promptly when its backend
    exits. This replaces the empirically insufficient 420-second override; it
    does not weaken immediate READY revocation or qualify LiteLLM for
    production exposure.
52. The replacement `bf27004` LiteLLM streaming identity confirms both the
    feasibility of the relaxed timeout policy and the centralized gateway's
    performance limit. PBS job `8751396` used one validated 64-node
    `debug-scaling` allocation and the hash-bound plan
    `1536ddc753ae52bfd61aea5af57d925b23c6460e076ad917a6f0699674a0d021`.
    Clean staging produced 64/64 receipts, and exact READY followed a
    732.14-second backend bring-up: 64 exact Ray nodes, 832 applications, 64
    healthy Serve proxies, 1,666/1,666 receipts, 768/768 replicas, a healthy
    LiteLLM gateway, and a successful advertised-endpoint canary. Ray Serve's
    application-status publication paused transiently at replicas 320--323 and
    later released already-healthy states in a burst; a read-only Ray resource
    check during the pause found 64 active nodes, all 768 GPUs allocated, no
    pending resource demand, and no recent Ray failures. This is retained as a
    control-plane observation, not counted as replay time. Both streaming
    passes then scheduled and completed exactly 422,400 requests. Pass 0 took
    825.77 seconds with 138,237 successes and 284,163 errors at 511.522
    scheduled and 167.403 successful RPS. Pass 1 took 954.40 seconds with
    167,545 successes and 254,855 errors at 442.582 scheduled and 175.550
    successful RPS, with 183.372-second p50 and 443.728-second p99. READY
    revoked immediately during each saturation interval and recovered after
    each pass without approaching the finite 3,600-second recovery deadline.
    The six expected result artifacts are present and hash-sealed; the manifest
    and durable run state are correctly `PARTIAL` only because of the explicit
    request-error counts. Shutdown is clean and `STOPPED`: deployment,
    gateway, and rank launcher return zero, the cleanup deadline is not
    exhausted, 64/64 ranks acknowledge DRAIN and GOODBYE, every rank exits
    zero, and an independent probe finds 64 clean nodes with no residual
    ExaServe, Ray, LiteLLM, vLLM, or Envoy processes. Therefore LiteLLM is able
    to run with plan-bound relaxed timeouts, but the centralized gateway is not
    capable of sustaining the paper's 64-node streaming offered load and must
    not be described as production-qualified at this scale.
53. The first 64-node HeadOnly non-streaming identity on `3ba15df` is rejected
    because its plan-owned recovery bound was empirically too short. It reached
    exact READY with 64/64 sessions, 64 exact Ray nodes, one exact native Serve
    application, one healthy head proxy, 1,665/1,665 receipts, 768/768 replicas,
    and a successful advertised-endpoint canary. Under the paper's fixed load,
    that single proxy reset a connection and entered `STARTING`; READY was
    revoked immediately as required. The fully dispatched first replay pass
    needed 488.7 seconds to drain, but the former six-canary-window recovery
    floor expired at 360 seconds and forced terminal
    `READINESS_RECOVERY_EXPIRED` before replay completion. The old identity was
    cancelled before its invalid second pass could target the stopped backend,
    so no performance row is accepted. Its shutdown report is clean and
    `STOPPED`, all 64 ranks acknowledged DRAIN and GOODBYE and exited zero, and
    an independent probe found 64/64 nodes residual-free. Candidate `ccccb82`
    raises only the validation-only HeadOnly recovery floor to the same finite,
    immutable initial-readiness horizon (3,600 seconds on Aurora), retains
    immediate READY revocation, and adds a regression test. The full gate is
    green with 1,269 passed and 19 skipped, plus clean Ruff and Go tests. Both
    HeadOnly replacements were rematerialized from this committed source;
    their non-streaming and streaming plan hashes are respectively
    `2b30e4f9f1ac4d7180702d3848155a554c6cb01fb7f2f8670e40eb5954463536`
    and `929ce696119857cb3c29af333bfa0a6feef00b8af170c3ba64c87c40346a01d8`.
54. The replacement `ccccb82` HeadOnly non-streaming identity validates both
    the native-Ray baseline and its corrected recovery contract. PBS job
    `8751591` ran in one validated 64-node `debug-scaling` allocation after a
    64/64 clean residual preflight. Exact READY followed an 81.73-second
    backend bring-up: 64 planned sessions, 64 exact Ray nodes, one exact native
    Serve application, one healthy head proxy, 1,665/1,665 receipts, 768/768
    replicas, and a successful advertised-endpoint canary. Both passes
    scheduled and completed exactly 422,400 requests. Pass 0 took 750.64
    seconds with 126,942 successes and 295,458 errors at 562.719 scheduled and
    169.112 successful RPS. Pass 1 took 721.15 seconds with 129,806 successes
    and 292,594 errors at 585.733 scheduled and 179.999 successful RPS, with a
    164.084-second p50 and 494.869-second p99. Saturation revoked READY and put
    the one proxy in `STARTING` during each pass; validation recovered and
    READY was atomically re-persisted after each pass without approaching the
    finite 3,600-second deadline. The old 360-second boundary would have
    terminated both valid drains, so the measured correction is necessary.
    The six expected artifacts are hash-sealed under manifest
    `7219a8813e1e0298ca31672ce4360314bbd6239e6bc6e285f397bc9124e2dbd7`;
    durable state is correctly `PARTIAL` only for the explicit error counts.
    Shutdown is clean and `STOPPED`: deployment and rank launcher return zero,
    the deadline is not exhausted, 64/64 ranks acknowledge DRAIN and GOODBYE,
    every rank exits zero, and an independent postflight probe finds 64/64
    nodes residual-free. HeadOnly is therefore runnable and contract-consistent
    as the paper's centralized validation baseline, but its 30.73% measured
    success rate at 64 nodes confirms that it is not a production exposure.
55. The replacement `ccccb82` HeadOnly streaming identity completes the
    six-cell 64-node checkpoint and independently confirms the corrected
    recovery contract. PBS job `8751657` ran in one validated 64-node
    `debug-scaling` allocation after a 64/64 clean residual preflight. Exact
    READY followed a 139.05-second backend bring-up: 64 planned sessions, 64
    exact Ray nodes, one exact native Serve application, one healthy head
    proxy, 1,665/1,665 receipts, 768/768 replicas, and a successful
    advertised-endpoint completion canary. Both passes scheduled and completed
    exactly 422,400 requests. Pass 0 took 856.33 seconds with 70,568 successes
    and 351,832 errors at 493.270 scheduled and 82.408 successful RPS. Pass 1
    took 819.23 seconds with 63,131 successes and 359,269 errors at 515.604
    scheduled and 77.061 successful RPS, with a 218.105-second p50 and
    469.517-second p99. The dispatchers needed 240--329 seconds to drain after
    dispatch. Saturation revoked READY and put the single native proxy in
    `STARTING` during each pass; validation recovered and READY was atomically
    re-persisted after each drain. This is the repeatable throughput limit of
    the contract-required centralized baseline, not a topology or launcher
    failure. The six expected artifacts are hash-sealed under manifest
    `b3bfe2cf4c3c1942e7eda90af15fdfdf41a7c4ce0489b670596086f8be586d8f`;
    durable state is correctly `PARTIAL` only for the explicit request-error
    counts. Shutdown is clean and `STOPPED`: all 64 ranks acknowledge DRAIN and
    GOODBYE and record exit zero, the expected harness SIGTERM leaves the rank
    launcher at 143, the cleanup deadline is not exhausted, and the shutdown
    report contains no errors. Two bounded optional post-stop diagnostics
    timed out, but the authoritative terminal report reconciles them and an
    independent one-process-per-node probe finds all 64 nodes residual-free.
    HeadOnly streaming is therefore runnable and contract-consistent as
    validation evidence, but its 14.95% measured success rate makes it
    unsuitable as a production exposure at the 64-node paper load.

## Paper-equivalence caveats

Accepted rows through 16 nodes were executed before clean-stage ownership was
restored and reused a verified node-local model cache. Their serving throughput
and latency remain comparable because staging precedes replay, but their
cold-start, broadcast, and bring-up timings are not paper-equivalent and remain
excluded. Candidate `9c6dfec` now consumes the hashed `runtime.clean_stage`
policy in the Python-owned staging transaction. The first `c7b63f7` 64-node
cell proved a complete per-rank cleanup receipt set and `cache_reused=false`,
but its cold bring-up timing is excluded because the eval driver was paused by
a diagnostic before READY. Replay timing was not started until after resume.
The later `4ce2446` 64-node attempt is excluded in full because its shutdown
evidence was incomplete even though replay accounting and rank teardown were
observed. Candidates `910cd54` and `793a780` repair and locally qualify that
evidence path. The first accepted current 64-node result uses the
documentation-consistent `e6b5ba8` immutable snapshot, whose serving code is
identical to `793a780`. The rejected `4327fe9` retry additionally moved Ray's
dependency-internal proxy watchdog outside the canonical initial-readiness
window and exposed the independent Envoy idle-pool and scale-cleanup defects.
The accepted Envoy cells and accepted LiteLLM non-streaming cell use
`3ba15df`, which retains that watchdog fix and adds the two hash-bound
corrections described in finding 48. The accepted replacement LiteLLM streaming
cell uses successor `bf27004`, whose only serving-policy changes are the
measured 3,600-second bounded recovery floor and executor/backend-liveness
coupling described in finding 51. The rejected first HeadOnly 64-node attempt
also uses `3ba15df`; both replacement HeadOnly cells use successor `ccccb82`
and the measured HeadOnly-only recovery correction described in finding 53.
Results from the source snapshots remain separately identified rather than
being represented as one binary-equivalent matrix.

The preview also materialized some early streaming OAT bundles under
`.../data/experiments/<run-group>` and later bundles under
`.../data/experiments/runs/<run-group>`. The immutable bundles are intact, but
the final index must normalize both roots rather than implying a single storage
root.

Ray's event/metrics exporter is unavailable on some allocated nodes and emits
warnings. The serving path, canonical readiness evidence, replay results, and
server-stat artifacts remain available; these warnings have not been treated
as request failures.

## Pending order

1. Preserve all accepted and rejected evidence already recorded. In particular,
   do not tune the native HeadOnly topology away from the paper baseline or
   represent the multi-snapshot 64-node preview as one release artifact.
2. Freeze one clean source candidate and rematerialize every remaining cell;
   the new exposure and timing contracts intentionally change plan identity.
3. Complete the twelve missing direct/HAProxy proxy cells at 4, 16, and 64
   nodes, then complete all eighteen 64-node offered-traffic cells.
4. Run the four 405B specifications at 4, 8, 16, 32, and 64 nodes before moving
   upward. If the checkpoint or allocation is unavailable, record a durable
   external blocker rather than silently omitting the cell.
5. Report the complete 64-node inventory checkpoint.
6. Run and report all ten 128-node proxy cells plus the four 128-node 405B
   cells. The old `97ab2a7` bundles remain historical preparation artifacts and
   cannot qualify the release candidate.
7. Run and report all ten 256-node proxy cells plus the four 256-node 405B
   cells last.
8. Treat LiteLLM streaming as `buffered_response`: throughput and request-error
   evidence remain valid, but it cannot enter incremental-SSE TTFT/TBT or
   streaming-correctness comparisons. Non-streaming timing remains E2E only.
9. Reconcile all 124 cells into accepted, measured-partial, rejected, retired,
   externally blocked, or not-run-with-reason; no unresolved cell may disappear
   from the report.
