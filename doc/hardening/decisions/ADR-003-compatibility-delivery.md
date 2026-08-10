# ADR-003: Compatibility delivery across process lifecycles

**Status:** SELECTED AND PROVEN FOR FINAL43 AT ONE AND TWO NODES (2026-08-09).
The exact candidate's four-node attempt awaits explicit authorization; scale
qualification and the ADR-000 envelope decision remain separate gates.

## Decision

ExaServe uses one immutable `CompatibilityProfile` and one selected delivery
architecture:

- public configuration/environment controls for behavior the dependencies
  already expose;
- a generated, exact-hash, role-filtered source overlay for the remaining
  pinned Ray/vLLM Python modules; and
- the generated EN-01 `sitecustomize` bootstrap only for spawned vLLM
  EngineCore/worker interpreters that have no supported pre-import hook.

Installed framework files are never edited. The old full-package symlink farm,
Ray Serve overlay, global import override, import-side-effect patch sweep, and
runtime-adapter fallback are removed from the production path.

## Mandatory ladder result

The ladder was evaluated in canonical order.

| Rung | Result | Evidence / disposition |
|---|---|---|
| 1. Public/upstream API | PARTIAL | Public Ray/Serve status, deployment health settings, supported environment controls, and actor `runtime_env` are used where they satisfy the requirement. The remaining entries target pinned private vLLM PP/XPU execution symbols, Ray accelerator internals, the unexposed Serve startup constant, Raylet argv construction, or a spawned-interpreter bootstrap for which the measured public signatures expose no equivalent hook. |
| 2. Immutable patched wheel/environment | REJECTED AFTER FEASIBILITY ATTEMPT | The installed vLLM distribution records an exact build-wheel URI and hash, but `/input/frameworks/26.26.0/wheelhouse/vllm-0.15.0+xpu-py3-none-any.whl` is absent at runtime. Ray has no `direct_url` provenance. Repacking installed files would capture an unrecorded build environment and Ray native payload rather than produce a source-reproducible release artifact. The probe records 3,014 vLLM files/39.6 MB and 4,829 Ray files/207 MB, including 11 Ray native files. This is unsuitable as ExaServe-owned dependency build output. |
| 3. Generated exact-hash overlay | PASS / SELECTED | Twelve complete target modules are generated from source bytes whose distribution, version, path, and SHA-256 match the profile. Every output has its own hash and patch-ID set. A role-filtered meta-path finder loads only modules required by the live process role. Real pinned-stack imports and semantic sentinels passed for deployment, Ray head, replica, EngineCore, and engine worker, including a Ray-head-to-replica role transition before target import. |
| 4. Narrow runtime adapter | NOT SELECTED | The earlier `_sitecustomize` import sweep proved functional reach, but an earlier rung succeeded. Importing `_sitecustomize` is now definition-only and cannot mutate a process. |
| 5. Unsupported | NOT NEEDED for the selected Aurora/vLLM profile | Unsupported engines, vendors, gateways, and scale dimensions remain rejected by the compiler/support matrix rather than borrowing this profile. |

Wheel/environment and process-boundary evidence:
`artifacts/hardening/architecture-feasibility-20260809-r2/result.json`, SHA-256
`6916762f1f091c991f9d1869aed484c3a98cf35baae0ed1f48ce66ebb0801a30`.

Selected overlay evidence:
`artifacts/hardening/compatibility-overlay-20260809-r5/result.json`, SHA-256
`64c1fdec86a29e9c04cdc1659c398e81b316124bf4ffb2d5ef8d7568e4df320e`.
That run recorded profile
`375cec6a84e36545febe4022331a694c01d977d2e6d30e684b509862b1fd0912`,
compatibility manifest
`6b5a7ffe2cff553d2fdbbccd9b70c807bc3e4cacca04b515d72737d88fdf30fb`,
and generated-overlay manifest
`19fd07ab6e0d91e9d9a0660edb56c91abc8ef4762fafd197b870744cd6031580`.
The transition observation started with `ray_head` at site bootstrap, rebound
the same interpreter to `replica` before any affected target import, and proved
all nine replica sentinels from the generated modules. This models Ray applying
an actor `runtime_env` after spawning a generic worker interpreter.
Those identities are feasibility evidence and are superseded by any later
source change; final qualification records the frozen candidate identities.

Final43 freezes the selected mechanism in wheel
`1041be53eb5b5875d198d5ee6c6664718b4085775dcba99107873dd3d1fcdff2`
with compatibility profile
`c17e684fe485261a9cfa82248bd24a9209b66a7c66bae8b889b24ca878d335d3`,
compatibility manifest
`cd85123822f4b936216282ed43346223a4b68f1a7cb152a85715a36fdab24259`,
and site profile
`4814429547fd4397014819a0f8b5c6ec8f7d77c889eaf844d27935b39a0a6e26`.
The clean installed-package gate is
`artifacts/hardening/final43-packaged-gate-20260809-a1/`. Real vLLM/XPU
qualification passed at one node in
`artifacts/hardening/final43-real-1n-20260809-a1/qualification/result.json`
and at two nodes/PP=2 in
`artifacts/hardening/final43-real-2n-20260809-a1/qualification/result.json`.
The latter contains one EngineCore receipt and two engine-worker receipts from
the two planned physical hosts. These receipts, not the earlier feasibility
hashes, are the release-candidate proof.

## Selected manifest

| IDs | Target / capability | Required roles | Delivery |
|---|---|---|---|
| SC-01..SC-05 | vLLM PP layer lookup, KV binding, forward/attention context, backend lookup | replica and affected engine roles | generated exact-hash modules |
| SC-09..SC-10 | XPU Ray channel selection and uncompiled PP fallback | replica, EngineCore | generated exact-hash `ray_executor.py` |
| SC-11..SC-12 | Ray XPU visibility and accelerator-device mapping | replica, engine worker | generated exact-hash Ray modules |
| EW-01..EW-03 | pre-interpreter worker environment and stable logical worker identity | EngineCore | generated exact-hash vLLM executor modules |
| RS-01 | Serve startup proxy timeout with no supported Ray 2.53 control | deployment | generated exact-hash constants module |
| RS-02 | Raylet startup/prestart fanout flags absent from the public CLI | Ray head/worker | generated exact-hash services module; plan values supplied through verified environment |
| EN-01 | spawned EngineCore/worker pre-import verification and self-attestation | engine core/worker | generated shim |

SC-06 is deleted: the generated module loads SC-05 at the target module's own
import boundary, so a second meta-path patch-on-import adapter is unnecessary.
Dead SC-D1/D2/D3, obsolete SC-07/08, the instrumentation-only OV files, and
the old setup-overlay shell path remain deleted.

## Materialization and activation contract

1. Source staging copies a clean ExaServe package and materializes the overlay
   at
   `/tmp/exaserve_src/exaserve/_compat_runtime/<compatibility_profile_hash>`.
   The complete staged tree, including overlay modules, manifest, and bootstrap,
   is content-inventoried and broadcast transactionally.
2. The plan-derived environment prepends that compatibility root and the
   staged source root to `PYTHONPATH`, and binds `EXASERVE_COMPAT_ROLE` before
   each managed interpreter starts.
3. Generated `sitecustomize` verifies the exact Python/Ray/vLLM versions, all
   pinned base source hashes, patch implementation hashes, delivery-code hashes,
   profile identity, and overlay manifest before installing the finder.
4. The finder resolves `EXASERVE_COMPAT_ROLE` at each affected target import.
   This permits a generic Ray worker to receive an actor role after interpreter
   startup, but only before an affected target is loaded. Reinstalling the same
   profile/root after a role transition is idempotent. A selected target already
   loaded from its base distribution, a different root, or a different profile
   fails closed.
5. Each generated target module invokes only its manifest-declared helper and
   establishes a semantic sentinel during import. `CompatibilityActivator`
   imports the required targets and fails if any sentinel is absent.
6. The EN-01 child shim binds `engine_bootstrap` inside the child interpreter,
   verifies the profile before target imports, activates the union needed by
   EngineCore/worker lifecycles, and later publishes the actual resolved role's
   exact self receipt. Preparing that child does not change the replica
   parent's role.
7. READY requires exact receipt-slot equality. Receipts include dependency,
   base-source, patch-artifact, and delivery-artifact identities; a missing,
   stale, mismatched, role-only, or failed semantic result blocks READY.

## Failure behavior

- Missing or wrong base wheels, source drift, target import before overlay
  install, generated-file tampering, incomplete role binding, mismatched
  profile/manifest, missing semantic sentinel, or receipt transport failure is
  fatal before the affected process can count toward readiness.
- The materializer writes only into a fresh transaction-owned staging tree.
  A partial tree is removed and never published.
- No compatibility exception is swallowed to keep serving, and no supervisor
  claims an in-process patch for another interpreter.

## Revisit condition

Every Python, Ray, vLLM, framework, patch-helper, or delivery-code change
changes the profile and forces the ladder and hardware proofs to run again.
If an upstream public hook appears, it replaces the corresponding overlay
entry. A reproducible vendor-supplied exact patched wheel may replace rung 3
only after its provenance and multi-process reach pass the same receipt gates.
