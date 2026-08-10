# ExaServe backlog and research TODOs

**Reconciled:** 2026-08-09 against final43.

**Role:** non-normative backlog. The execution plan defines architecture and
`hardening/FINDINGS.yaml` defines disposition; this file cannot waive a gate.

## Release-gating actions

1. Product owner: authorize one additional four-node final43 attempt.
2. Product owner: approve the proposed 64-node first-release ceiling or select
   a different ceiling in ADR-000.
3. After those decisions, predeclare and run only the matching final43 ladder.
   A 64-node choice requires 4, 16, and 64 nodes with the same immutable wheel.
4. Update ADR-000, the compatibility matrix, ledger, and release verdict from
   those exact receipts. Do not substitute legacy or earlier-candidate runs.

## Canonical work still open

These are the eight `IN_PROGRESS` ledger records; all depend on scale/scope
evidence rather than an unresolved two-node code defect:

| ID | Required next proof |
|---|---|
| `PR-033` | Owner-approved envelope and exact-candidate qualification at its boundary. |
| `KI-A1` | Only if Envoy/streaming/256 nodes enters scope: reproduce with owned failure evidence. |
| `KI-A3` | Measure final43 Ray/GCS/controller behavior at the approved boundary. |
| `KI-A7` | Measure residual shared-environment imports at the approved scale. |
| `KI-B2` | Only if streaming enters scope: capture the rare HAProxy death's real cause. |
| `KI-D2` | Prove native distribution and every activation receipt at the approved boundary. |
| `TD-COPPER` | Quantify residual shared-filesystem import traffic at that boundary. |
| `IMP-B16` | Freeze the already-implemented plan family against the approved/measured envelope. |

## External platform qualification

`TD-SLURM-AMD` is `EXTERNAL_BLOCKER`. The scheduler/vendor abstractions and
failure contracts are unit-tested, but production support requires a native
Slurm allocation with a pinned CUDA or ROCm stack. When such a system is
available, qualify device isolation, dependency profile, HAProxy availability,
multi-node launch, PP placement, process failure, readiness, and cleanup. Until
then the selected SiteProfile rejects the combination.

SGLang is explicitly gated out of the selected Aurora profile. Adding it is a
new compatibility/site profile with its own clean package, real-engine,
failure, and scale evidence; the existence of `SGLangEngine` is not a support
claim.

## Optional work outside the current production release

- `TD-CACHE`: proxy-layer request caching/deduplication.
- `TD-STAGE-PAR`: concurrent multi-model download. Transactional serial staging
  remains correct; revisit only if startup SLO data makes parallelism necessary.
- `TD-CPP-CLIENT`: paper-only C++ client experiment. The Go replay contract is
  the production benchmark/client path.

## Research and measurement ideas

- Measure raw latency including a LiteLLM hop, clearly labeled as
  validation-only and not real streaming.
- Isolate proxy overhead with matched request/client topology.
- Compare error rate before and after explicit retry policies.
- Explore distributed/sharded ingress if an approved envelope exceeds the
  measured single-head HAProxy boundary.
- Evaluate a sharded control-plane alternative only if the approved scale and
  final43 measurements show Ray's upstream controller is the limiting factor.
- Update SC26 paper framing and figures separately from the release claims.

## Completed hardening items retained for context

The following former TODOs are closed in final43 and should not be reopened by
stale prose:

- hermetic/package CI, typing, linting, security, and failure-path tests;
- strict immutable plans shared by serving, eval, ClientLab, and schedulers;
- OS-owned/leased ports and deliberate collision failure;
- exact null-compute/tokenizer identity and capability labeling;
- loud, narrow chat-template fallback behavior;
- transport/completion request-ID correlation and bounded `/metrics`;
- centralized SiteProfile constants;
- exact-hash generated compatibility delivery with no installed-file edits;
- explicit PP topology/capability gating and a real two-host PP=2 proof;
- one scheduler abstraction and declared PSI/J dependency-when-selected;
- removal of the legacy Bash lifecycle/readiness path; and
- documented, path-independent, atomic reference-card regeneration. The
  ignored template and two figure prerequisites are listed in `doc/exaserve.md`.

For current release state, see `hardening/STATUS.md` and
`hardening/FINAL_AUDIT.md`.
