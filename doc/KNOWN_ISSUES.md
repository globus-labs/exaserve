# Known issues and empirical failure log

**Reconciled:** 2026-08-09 against final35.

**Role:** historical/empirical evidence. `hardening/FINDINGS.yaml` is the
authoritative disposition ledger; this file cannot close or waive a release
gate.

The final35 candidate is qualified at one and two Aurora nodes. Historical
128/256-node observations below remain useful diagnostic evidence, but they do
not qualify the new architecture or establish a support envelope.

## Current unresolved scale/scope observations

### A1. Envoy 256-node streaming deployment collapse — unqualified dimension

Three historical Envoy streaming attempts reported zero request success after
an EngineCore port collision, unhealthy replicas, a Serve controller failure,
a false-ready marker, and later connection refusal. Envoy itself had initially
been healthy. Final35 fixes port ownership, false readiness, process ownership,
and failure evidence for the qualified HAProxy path, but Envoy/streaming/256
nodes is not an approved release dimension and has not been reproduced with the
new control plane. Ledger: `KI-A1`, `IN_PROGRESS`.

### A3. Ray GCS/controller pressure at large N — boundary evidence missing

Historical 256-node runs showed roughly quadratic proxy/actor-handle work and a
single-controller bottleneck. ExaServe's final readiness processing is indexed,
bounded, and does not fleet-poll, but it cannot remove or claim to solve
upstream Ray behavior. Measure the exact final35 candidate at the approved
boundary tier before setting a larger supported maximum. Refs:
`findings/gcs_contention_quantitative.md` and
`findings/proxyactor_death_cascade_256n.md`. Ledger: `KI-A3`, `IN_PROGRESS`.

### A7 / D2. Residual source-import and MPI activation cost — scale proof missing

Final35 transactionally inventories and stages one source tree, generated
overlay, and bootstrap, then requires a receipt from every planned rank. The
two-node gates prove both ranks activated identical bytes. Residual imports
from the shared environment and native broadcast behavior still need
measurement at the approved scale boundary. Ledgers: `KI-A7`, `KI-D2`, and
`TD-COPPER`, `IN_PROGRESS`.

### B2. Centralized 256-node streaming congestion and rare HAProxy death

Historical bounded-client streaming commonly completed requests but suffered a
packet/retransmission storm (up to about 6.75 million retransmits and about
195,000 established connections). A rarer run ended in total HAProxy death and
connection refusal; its causal signal was not captured. Do not conflate the
common degraded network regime with the rare process death. Streaming is not a
final35 production claim. Refs:
`eval/specs/sc26workshop/FINDINGS_haproxy_256n.md`. Ledger: `KI-B2`,
`IN_PROGRESS`.

## Resolved mechanisms in final35

### A2. Static port races — resolved

Ports are generation-owned through descriptor/lease contracts. An address
collision fails before the component counts toward readiness. The final35
two-node null gate deliberately holds the HAProxy port and proves
fail-before-launch behavior.

### A4 / D1. False or proxy-only readiness — resolved

No log line, open socket, proxy health response, one canary, or prior-generation
file can make a deployment READY. The canonical predicate requires exact
sessions, resources, component/replica/engine receipts, owned gateway state,
and a typed completion through the advertised endpoint; it is continuously
re-evaluated. The two-node negative gate withholds a worker proxy and remains
non-ready. Scale support is tracked separately by `PR-033` rather than by
keeping the correctness defect open.

### A5. Tokenizer/Rayon oversubscription guard — resolved

The bounded tokenizer environment is a typed SiteProfile/compatibility
capability and is applied to the managed processes that need it. It is no longer
an ambient shell workaround.

### A6. Scaling-trace ownership and load — resolved

Telemetry is deployment/generation scoped, bounded, freshness checked, and
owned by the composition lifecycle. Cleanup runs through the same bounded
shutdown path. Dead per-replica file-writing behavior is not reachable.

### B1 / E1. Corrected non-streaming HAProxy result — preserved, not generalized

The historical corrected result is about 27.1k requests/s at 256 nodes with
about 0.04% errors and an explicitly bounded client topology. It replaced the
confounded result in which one client per node hammered a single proxy. This is
regression context, not final35 scale qualification and not evidence about
streaming. Refs: `eval/specs/sc26workshop/FINDINGS_haproxy_256n.md` and
`findings/weakscaling_short_v3_progress.md`.

### B3. LiteLLM fake streaming — resolved by capability refusal

LiteLLM's buffered end-burst cannot enter a real-streaming TBT comparison. The
capability contract rejects that measurement instead of presenting degenerate
near-zero TBT as good streaming behavior.

### C1. Server statistics silent/partial success — resolved

The active producer/consumer uses one strict typed contract. Requested stats
must cover the planned producers; missing, malformed, or partial required stats
fail the run. The incompatible legacy pull API is not the production path.

### C2. Snapshot provenance — resolved

Run semantics and provenance are separate immutable identities. Dirty-tree
materialization requires explicit acknowledgement and records what was used;
submission cannot silently borrow current working-tree state.

### C3. Trailing bytes on structured rewrites — resolved

Structured state and results use same-directory temporary files, fsync where
required, and atomic replacement. Readers are strict and do not hide trailing
garbage with `raw_decode`.

### C4. Distributed result partials — resolved

Shard collection remains fault-isolating rather than reverting to the hanging
root collective, but exact rank/result identities and terminal completeness are
mandatory. A missing or duplicated shard cannot produce success.

### C5. Client topology footgun — resolved

The immutable run plan represents deployment and client topology explicitly.
Every dispatch arm validates before submission; an omitted, targeted, or
derived node-count mismatch follows the canonical plan rules rather than a
hidden spec convention.

### C6. TTFT/TBT interpretation — resolved

Analysis and figures report TTFT and TBT attainment separately. Non-streaming
latency is labeled as a coarse full-response estimate and cannot masquerade as
per-token streaming timing.

### D3. Competing proxy instrumentation owner — resolved

The retired profiling hook and full-file overlay path are absent from the
reachable production code. Instrumentation has one documented owner and atomic
activation/receipt semantics.

### D4. Pipeline-parallel topology/private API — resolved for selected profile

PP topology is a plan capability with explicit node-pinned stage ownership.
Final35 proves one real PP=2 replica across two physical hosts and exact
EngineCore/worker receipts. Unsupported multi-replica/non-shard combinations
fail plan capability checks rather than proceeding with ambiguous placement.

## Current release blockers

- one additional four-node final35 attempt needs explicit authorization and a
  predeclared candidate-bound gate;
- ADR-000's proposed 64-node ceiling needs a product-owner decision; and
- if 64 is selected, exact-candidate 4/16/64 qualification remains required.

See `hardening/STATUS.md`, `hardening/FINAL_AUDIT.md`, and
`hardening/COMPATIBILITY_MATRIX.md` for the current release verdict.
