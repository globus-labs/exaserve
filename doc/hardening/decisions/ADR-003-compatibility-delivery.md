# ADR-003: Compatibility delivery across process lifecycles

**Status:** DECIDED (S03 inventory 2026-08-05 + spawn/receipt proofs, gates
S03 attempts 2-4 on allocations 8736039/8736110). Evidence:
`doc/hardening/COMPATIBILITY_INVENTORY.md` (66-record manifest seed),
`artifacts/hardening/s03-1n/` (attempt logs, shim/proc/canary captures).

## Proven facts (role-reach matrix, measured)

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

## Decision (WP3 target, per inventory ladder verdicts)

1. **Delete-first:** OV-DEAD-1/2 (1,449 byte-identical lines), SC-D1/D2/D3
   (dead call sites) — immediate; SC-07/SC-08 (~445 obsolete vendored lines)
   after a PP regression run (now cheap: the S03 config is the regression).
2. **Constants stop being an overlay:** the 7 timeout/health constants flow
   only through SV-01/SV-02/SV-04 (public setup hook — already the effective
   path) and become scale-gated profile capabilities per ADR-002 (the raised
   detection windows are load-shedding for ≥64n storms, not defaults).
3. **Instrumentation (11 OV blocks) leaves the overlay:** opt-in generated
   overlay (ladder 3) built content-addressed from verified upstream hashes,
   or dropped where upstream gauges exist (OV-controller-01). The symlink
   farm (SH-19/21-25) retires with it.
4. **The vendor-compat core stays as guarded runtime patches (ladder 4) under
   one `CompatibilityProfile`:** SC-01..05 (PP aliasing; upstream-PR
   candidates), SC-06 (rewritten as a `MetaPathFinder`, not a
   `builtins.__import__` override), SC-09..12 (XPU DAG/selector), EN-01
   (generated shim — the proven spawn-delivery mechanism, kept), EN-05/07/09/
   11, RS-02/03, SH-03/04/05/06/13.
5. **Receipts:** profile ID = SHA-256 over the canonical manifest +
   base-environment identity; roles self-report through the §3.2 channel
   (transport already built and proven in S01); external daemons attested by
   their owning supervisor. Pin corrected to ray 2.53.0/vllm 0.15.0; version
   assertion added to overlay assembly; missing/mismatched receipt fails
   READY.
6. **Timeboxed probe owed in WP3 (not blocking P00):** immutable
   patched-wheel/environment attempt (ladder 2) for the vendor-compat core,
   ≤3 days; on failure the guarded-runtime-patch + generated-shim design
   above stands as the selected implementation.

## Rejected

- Editing installed packages in place (never; enforced by test in WP3).
- Keeping `builtins.__import__` overrides (highest blast radius; MetaPathFinder
  replacement specified).
- `ray.util.state`-based attestation (dashboard absent on this stack; see
  ADR-002).

## Revisit

Frameworks upgrade (any of python/ray/vllm changes) → profile hash changes →
activation fails closed until the manifest is re-verified; that is the
designed behavior, not an incident.
