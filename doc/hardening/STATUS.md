# ExaServe production-hardening status

**Status date:** 2026-08-09

**Verdict:** `TECHNICAL_PASS_SCOPE_PENDING` — every locally resolvable
production-hardening gate passes for the exact final42 artifact at one and two
Aurora nodes. This is not an approved general production release and does not
claim support above the measured boundary.

## Exact candidate

| Identity | Value |
|---|---|
| Release | `artifacts/hardening/release-20260809-final42` |
| Wheel SHA-256 | `5346c7ab858b056448702b207b76350ac2ee134a65fa45ea67779039d41362e3` |
| sdist SHA-256 | `af1f9d75a6a6168806ef128df55bbfb1f4f29db84e4594146c6f0fa35d64eb7b` |
| Artifact-manifest SHA-256 | `0bc6f132a167bd5e0e8df2d6651b68217cee0d27a6359358bb5bec09a1d987e6` |
| Site-profile hash | `4814429547fd4397014819a0f8b5c6ec8f7d77c889eaf844d27935b39a0a6e26` |
| Compatibility-profile hash | `c17e684fe485261a9cfa82248bd24a9209b66a7c66bae8b889b24ca878d335d3` |
| Compatibility-manifest hash | `cd85123822f4b936216282ed43346223a4b68f1a7cb152a85715a36fdab24259` |
| Candidate review | `artifacts/hardening/final42-candidate-review.json` |

The wheel was built from the sdist in an isolated environment. Installed Ray
and vLLM files are never edited; the selected compatibility mechanism is the
hash-verified, role-filtered generated overlay described in ADR-003.

## Passed gates

| Gate | Result | Durable evidence |
|---|---|---|
| Final working-tree source suite | PASS — 1216 passed | `artifacts/hardening/final42-final-source-gate-20260809-a3/pytest.log` |
| Clean installed-package gate | PASS — 1207 passed, 9 skipped; mypy clean | `artifacts/hardening/final42-packaged-gate-20260809-a4/` |
| Final static/ledger gate | PASS — Ruff, security, compileall, Go, ledger, adjudicator | `artifacts/hardening/final42-final-static-gate-20260809-a2/` |
| 1-node null engine | PASS — READY, typed canary, drain, gateway death, cleanup | `artifacts/hardening/final42-null-1n-20260809-a1/qualification/result.json` |
| 1-node real vLLM/XPU | PASS — real EngineCore receipt, canary, drain, gateway death, cleanup | `artifacts/hardening/final42-real-1n-20260809-a1/qualification/result.json` |
| 2-node null engine | PASS — exact membership/receipts, worker death, port collision, partial-start non-readiness, cleanup | `artifacts/hardening/final42-null-2n-20260809-a1/qualification/result.json` |
| 2-node real vLLM/XPU PP=2 | PASS — one replica spans two physical hosts with core and both worker receipts | `artifacts/hardening/final42-real-2n-20260809-a1/qualification/result.json` |
| HAProxy no-delay on | PASS — bounded real-engine workload, diagnostics, cleanup | `artifacts/hardening/final42-proxy-nodelay-on-1n-20260809-a1/qualification/result.json` |
| HAProxy no-delay off | PASS — paired bounded real-engine workload, diagnostics, cleanup | `artifacts/hardening/final42-proxy-nodelay-off-1n-20260809-a1/qualification/result.json` |
| Supervisor/watchdog faults | PASS — exact head Ray child and worker supervisor deaths, one first cause, zero survivors | `artifacts/hardening/final42-supervisor-watchdog-v3q2-2n-20260809-a1/qualification/result.json` |

The package gate ran from a clean installed wheel inside a one-node lease. All
hardware runs used `subjob`, the pinned Aurora environment, immutable
predeclared campaign rows, and the same wheel/site/compatibility identities.

## Architecture now reached in production

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
| `FIXED` | 80 | Candidate-bound tests and/or final42 receipts close the invariant at the qualified boundary. |
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
   predeclared final42 ladder. For a 64-node ceiling the outstanding cells are
   4, 16, and 64 nodes.
3. Offsite Slurm/CUDA/ROCm support requires an appropriate native allocation.

Historical or earlier-candidate runs cannot satisfy these cells. Do not rebuild,
relabel, or broaden final42 without creating a new candidate review.
