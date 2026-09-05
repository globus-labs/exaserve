# ExaServe production-hardening status

> **2026-09-04 supersession:** the non-head shared-filesystem cutover changes
> runtime, harness, model-manifest, and result identities. The final43 and
> release-v0.4.0-rc2 evidence below remains historical but cannot qualify the
> current tree. Code remediation and the still-open live gates are tracked in
> `doc/SHARED_FILESYSTEM_FANOUT_AUDIT_20260904.md`; do not update old artifact
> hashes to make them match new code.

**Status date:** 2026-08-30

**Verdict:** `SUCCESSOR_SOURCE_READY_SCALE_PENDING` — the
`release-v0.4.0` source candidate incorporates the complete preview repair
series and the final release-consolidation fixes. Final43 remains the last
packaged artifact qualified at one and two Aurora nodes. This is not yet an
approved general production release and does not claim support above that
measured boundary.

## Successor source candidate

The branch descends from validated preview candidate `ccccb82` and adds:

- exact live-tokenizer context admission with pre-header HTTP 400 errors;
- plan-derived incremental, buffered, and non-streaming timing semantics;
- fail-closed first-release exposure configuration;
- monotonic, boot-fenced READY leases;
- mixed-scheduler `submit-all` refusal;
- the hash-bound 4/16/64 scale qualification/adjudication lane; and
- the complete preview evidence ledger and updated architecture call graph.

Because these changes alter result and deployment-plan identity, every release
qualification bundle must be rematerialized from the final clean wheel. The
multi-snapshot preview remains diagnostic and paper evidence only.

### Successor source gates

| Gate | Result |
|---|---|
| Full source suite in validated one-node PBS job `8792581` | PASS — 1,323 passed, 19 skipped |
| Focused release-contract integration | PASS — 200 passed |
| Ruff and compileall | PASS |
| Findings ledger | PASS — 93 FIXED / 8 IN_PROGRESS / 1 EXTERNAL_BLOCKER / 3 OUT_OF_PRODUCTION_SCOPE |
| Scale harness/adjudicator regression | PASS — 30 passed, 9 skipped |
| Go formatting, vet, and tests | PASS |
| Clean installed-wheel gate in validated PBS job `8792605` | PASS — 1,302 passed, 31 skipped; resources present; typed-core mypy clean |

The owner-approved hardware scale ladder remains a release gate; package
success does not predeclare that result.

The installed package was built from source commit
`a920d7e3f2e2909f636b5c71b26d33c6cdd05837`:

- wheel: `artifacts/hardening/release-v0.4.0-rc2/exaserve-0.4.0-py3-none-any.whl`,
  SHA-256 `75bab1c97948d29237000d5fa087f3ed1f1d18e5ef3233fe2c759b4faf0e32f8`;
- sdist: `artifacts/hardening/release-v0.4.0-rc2/exaserve-0.4.0.tar.gz`,
  SHA-256 `8e8b0a6af44ad75bc48004dba48b66fc382e451d8e44449b55ec36dd8cd6bcb3`;
- package receipt:
  `artifacts/hardening/release-v0.4.0-packaged-gate-20260830-a4/receipt.json`,
  SHA-256 `dc28e9f38e8ff6e2af9aa06b3b6feb4931c869217369d2a61d03f38e3c9a5fad`.

## Last packaged qualified candidate

| Identity | Value |
|---|---|
| Release | `artifacts/hardening/release-20260809-final43` |
| Wheel SHA-256 | `1041be53eb5b5875d198d5ee6c6664718b4085775dcba99107873dd3d1fcdff2` |
| sdist SHA-256 | `94d9b10a0f47603a3c847ec55cdd74f154d42b08bb9124fef1d730da4defcee6` |
| Artifact-manifest SHA-256 | `0c8fcf3025fb3dff9b6ea4aa22c325ddef33bf5ddc06068379fee04456033cc8` |
| Site-profile hash | `4814429547fd4397014819a0f8b5c6ec8f7d77c889eaf844d27935b39a0a6e26` |
| Compatibility-profile hash | `c17e684fe485261a9cfa82248bd24a9209b66a7c66bae8b889b24ca878d335d3` |
| Compatibility-manifest hash | `cd85123822f4b936216282ed43346223a4b68f1a7cb152a85715a36fdab24259` |
| Candidate review | `artifacts/hardening/final43-candidate-review.json` (`763dfdf04e72ef6b42a35277d362c275e64dcd66b477457e87c5235045638c6d`) |

The wheel was built from the sdist in an isolated environment. Installed Ray
and vLLM files are never edited; the selected compatibility mechanism is the
hash-verified, role-filtered generated overlay described in ADR-003.

## Last packaged candidate gates

| Gate | Result | Durable evidence |
|---|---|---|
| Final working-tree source suite | PASS — 1224 passed | `artifacts/hardening/final43-final-source-gate-20260809-a1/pytest.log` |
| Clean installed-package gate | PASS — 1215 passed, 9 skipped; four-module typed contract core mypy clean | `artifacts/hardening/final43-packaged-gate-20260809-a1/` |
| Final static/ledger gate | PASS — Ruff, security, compileall, Go, ledger, adjudicator | `artifacts/hardening/final43-final-static-gate-20260809-a1/` |
| 1-node null engine | PASS — READY, typed canary, drain, gateway death, cleanup | `artifacts/hardening/final43-null-1n-20260809-a1/qualification/result.json` |
| 1-node real vLLM/XPU | PASS — real EngineCore receipt, canary, drain, gateway death, cleanup | `artifacts/hardening/final43-real-1n-20260809-a1/qualification/result.json` |
| 2-node null engine | PASS — exact membership/receipts, worker death, port collision, partial-start non-readiness, cleanup | `artifacts/hardening/final43-null-2n-20260809-a1/qualification/result.json` |
| 2-node real vLLM/XPU PP=2 | PASS — one replica spans two physical hosts with core and both worker receipts | `artifacts/hardening/final43-real-2n-20260809-a1/qualification/result.json` |
| HAProxy no-delay on | PASS — bounded real-engine workload, diagnostics, cleanup | `artifacts/hardening/final43-proxy-nodelay-on-1n-20260809-a1/qualification/result.json` |
| HAProxy no-delay off | PASS — paired bounded real-engine workload, diagnostics, cleanup | `artifacts/hardening/final43-proxy-nodelay-off-1n-20260809-a1/qualification/result.json` |
| Supervisor/watchdog faults | PASS — exact head Ray child and worker supervisor deaths, one first cause, zero survivors | `artifacts/hardening/final43-supervisor-watchdog-v3q2-2n-20260809-a1/qualification/result.json` |

The package gate ran from a clean installed wheel inside a one-node lease. All
hardware runs used `subjob`, the pinned Aurora environment, immutable
predeclared campaign rows, and the same wheel/site/compatibility identities.

## Architecture exercised by the candidate

- One Python composition root owns staging, rank launch, deployment, gateway,
  readiness, terminal publication, and cleanup.
- Ray daemons, PALS/MPI, HAProxy, native staging, and one isolated deployment
  child remain subprocesses because they are genuine operating-system fault
  boundaries. They use argv vectors, exact process identity, deadlines, typed
  receipts, and owned process groups; no thread parses logs for control state.
- `ray.scripts.scripts.cli.main` runs inside the supervised `exaserve.ray_start`
  daemon boundary. Ray's informational CLI output is presentation only.
- Canonical READY is a typed, generation-bound, continuously revocable
  predicate. `CLUSTER READY` and other stdout text cannot advance state.
- Compatibility has one exact-version delivery path. No installed dependency
  replacement, version-control mutation, or two-tier monkey-patch fallback is
  present on the selected path.
- Scheduler-rendered shell contains only site setup and the final Python entry
  point; no Bash script owns deployment lifecycle, readiness, or cleanup.

## Ledger

`FINDINGS.yaml` validates against the exact canonical schema:

| Disposition | Count | Meaning |
|---|---:|---|
| `FIXED` | 93 | Candidate-bound tests and/or final43 receipts close the invariant at the qualified boundary. |
| `IN_PROGRESS` | 8 | Scale/scope approval or measurements are missing; none is a hidden two-node code defect. |
| `EXTERNAL_BLOCKER` | 1 | Native Slurm plus CUDA/ROCm needs unavailable offsite hardware. |
| `OUT_OF_PRODUCTION_SCOPE` | 3 | Optional caching, concurrent download optimization, and paper-only C++ client. |

The eight open records are `PR-033`, `KI-A1`, `KI-A3`, `KI-A7`, `KI-B2`,
`KI-D2`, `TD-COPPER`, and `IMP-B16`.

## Support boundary and residuals

The evidence-derived maximum is two Aurora nodes for the exact PBS / Intel PVC
XPU / Ray 2.53.0 / vLLM 0.15.0+xpu / HAProxy / non-streaming completion /
trusted-allocation profile. SGLang, streaming, public exposure, alternate
production gateways, and native Slurm/CUDA/ROCm are unqualified or rejected.

All final43 hardware campaigns deliberately used `validation_mode=true`
because the selected SiteProfile still has unmeasured control/readiness limits
and no approved release envelope. They nevertheless used HAProxy with
`PROXIED_INTERNAL`, the same serving/readiness/receipt/cleanup paths, and the
same immutable wheel. The runtime differences are limited to permitting an
explicit allocation identity when no native scheduler ID is present and
recording `production_qualified=false` in gateway evidence. The
`require_execution_qualification(...) -> True` branch therefore remains a
pending hardware exercise when WP12 measurements and product approval create
an evidence-backed production SiteProfile.

Injected head/worker loss is classified promptly by the owning control plane,
but the pinned Ray driver may continue emitting failed GCS/task notifications
until its configured 120-second reconnect timeout. Cleanup remained within the
declared gate and left zero exact-generation survivors. This bounded latency is
a dependency residual, not an instant-recovery claim.

Optional Ray metrics-exporter warnings were observed on Aurora. Metrics export
is not a readiness conjunct; owned status, canary, cleanup, and ExaServe metrics
contracts passed.

## Remaining external decisions

1. A product owner must approve a release ceiling (the current ADR proposal is
   64 nodes) or select another envelope.
2. Any envelope above two nodes requires authorization and a new immutable,
   predeclared release-v0.4.0 ladder. For a 64-node ceiling the outstanding cells are
   4, 16, and 64 nodes.
3. Offsite Slurm/CUDA/ROCm support requires an appropriate native allocation.

Historical or earlier-candidate runs cannot satisfy these cells. Do not
relabel their receipts as release-v0.4.0 evidence; build a new candidate review
from the final wheel and exact ladder.
