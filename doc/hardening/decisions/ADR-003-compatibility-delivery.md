# ADR-003: Compatibility delivery across process lifecycles

**Status:** PROVISIONAL INVENTORY; P00/S03 REOPENED on 2026-08-07. The 2026-08-05
inventory and spawn observations are valuable reachability evidence, but the
required per-patch elegant-first fallback ladder and full two-node v2 receipt
proof have not passed. Evidence:
`doc/hardening/COMPATIBILITY_INVENTORY.md` (66-record manifest seed),
`artifacts/hardening/s03-1n/` (attempt logs, shim/proc/canary captures).
Receipt cardinality/transport was reconciled with plan §3.2.1 on 2026-08-07;
the existing role-only/Ray-collector implementation is historical partial
mechanism, not proof that the target transport is complete.

## Historical reach facts (measured; not fallback-ladder verdicts)

| Role | Profile reach today | Evidence |
|---|---|---|
| Driver | `apply_all()` at import (SV-06) | AuroraPatch lines pid=driver (130024) |
| Serve replica (EngineWorker) | sitecustomize via `PYTHONPATH` (SV-05) | AuroraPatch lines from replica pid |
| Spawned EngineCore (pp>1) | **EN-01 generated shim — PROVEN**: `VLLM::EngineCore` environ contains `/tmp/exaserve_pp_shim`; all 12 patches applied in-process; SC-10 fallback observed live per decode step | attempt-4 shim_reach + patch lines pid=135376 |
| multiprocessing resource tracker | same shim (collateral, harmless) | pid=135375 lines |
| Ray worker daemons / RayWorkerWrapper | shell env + `ray_env` var copy; patches applied on both nodes | attempt-2 worker-node EngineCore pids 15026x |
| Serve controller/proxies | constants via public `worker_process_setup_hook` (SV-02/04) | runtime_env creation log with hook name |
| Functional proof | pp=2 8B generation through the served route returns tokens | attempt-4 canary.json |

Also proven live: the stale pin fires a real warning
(`pinned=2.49.1 runtime=2.53.0 — Patches may break`), i.e. the current system
runs on an unverified version combination every day (AC-COMP-01 driver).

## Candidate disposition map (must not freeze before the ladder proof)

1. **Delete-first candidates:** remove OV-DEAD-1/2 and SC-D1/D2/D3 only after a
   packaged static/reachability test proves they have no supported call path;
   remove SC-07/SC-08 only after that proof plus a PP regression. Deletion is
   preferred to carrying an unnecessary patch, but “dead” is an evidence claim,
   not permission to skip verification.
2. **Constants are rung-1 candidates:** evaluate the 7 timeout/health constants
   through SV-01/SV-02/SV-04's supported public setup hook and make them
   evidence-derived profile capabilities rather than global source mutations.
   Their semantic/receipt proof is still owed; raised detection windows are
   load-shedding for large-scale storms, not defaults.
3. **Instrumentation is unresolved per entry:** first use/drop it in favor of
   upstream gauges where they satisfy the requirement. For each remaining OV
   block, try an immutable patched wheel/environment before a generated
   exact-hash overlay. The overlay is selectable only with recorded wheel-rung
   failure; the current symlink farm is never the target.
4. **The current vendor-compat runtime patches are feasibility evidence, not
   the selected target rung:** SC-01..05, SC-06, SC-09..12, EN-01,
   EN-05/07/09/11, RS-02/03, and SH-03/04/05/06/13 each follow the canonical
   order: public/upstream behavior; immutable exact-version patched
   wheel/environment; generated exact-hash overlay; only then a narrow guarded
   runtime adapter. The generated EN-01 shim proves spawned-process reach, but
   does not by itself prove that earlier rungs are infeasible or classify the
   shim as the production choice.
5. **Receipts:** profile ID = SHA-256 over the canonical manifest +
   base-environment identity. Every exact planned managed instance self-reports.
   Rank-owned receipts use the authenticated §3.2 channel; GLOBAL receipts use
   only the in-process outer `RuntimeSupervisor` ingress and the same strict
   validator/global writer. A rank may not submit a GLOBAL receipt. An owning
   supervisor may attest only one individually identified unmodified external
   daemon for which the manifest requires no in-process patch. Per-rank batching
   is transport only; the head reconciles exact requirement/instance identities
   as specified by plan §3.2.1. Pin corrected to ray 2.53.0/vllm 0.15.0;
   version assertion added to overlay assembly; a missing, mismatched,
   role-only, or stale receipt fails READY.
6. **Mandatory per-patch ladder gate (blocks the S03 verdict/P00 technical
   pass):** record the rung-1 semantic result; timebox the immutable
   patched-wheel/environment attempt to at most three days; if and only if it
   has a reproduced failure, test the generated exact-hash overlay; if and only
   if that also has a reproduced failure, test the narrow runtime adapter.
   Record failure evidence and rejection reason for every skipped/infeasible
   rung. A successful earlier rung wins even if the current runtime patch is
   easier. Existing runtime patches may remain only on the explicitly legacy
   path during migration; they are not a frozen production verdict.

## Reopened S03 proof owed before P00 technical pass

- Normalize the inventory so every required patch/role has its rung attempts,
  exact version/source/artifact hashes, semantic postcondition, delivery timing,
  and selected-or-rejected verdict with evidence.
- Run the one-node spawned EngineCore proof using the actually selected profile,
  then the two-node proof with exact v2 receipts from the outer supervisor, Ray
  head/workers, every affected Serve actor/replica, and spawned engines.
- Prove fail-closed behavior for a missing/mismatched/stale receipt and targeted
  `NOT_REQUIRED`, route rank receipts over the authenticated channel, and inject
  GLOBAL receipts only through the local outer-supervisor validator.

Until these pass, the ADR is an inventory plus candidate map, not a closed S03
decision or permission to skip to runtime monkey patches.

## Rejected

- Editing installed packages in place (never; enforced by test in WP3).
- Keeping `builtins.__import__` overrides (highest blast radius; MetaPathFinder
  replacement specified).
- `ray.util.state`-based attestation (dashboard absent on this stack; see
  ADR-002).

## Revisit

First close the per-patch ladder and cross-role receipt proof above. Thereafter,
any Python/Ray/vLLM/frameworks change alters the profile/base identity and
forces re-verification from rung 1; activation fails closed until then.
