# Missing Paper Scale Campaign — 2026-08-31

Status: **in progress; last updated 2026-09-06**. This document is an evidence
ledger, not a completed paper reproduction claim. A number appears in an
accepted table only after the immutable run bundle, terminal state, result
manifest, readiness evidence, and shutdown evidence all pass the repository's
fail-closed acceptance checks.

## Requested scope

This campaign fills the two paper series that were not present as complete,
current-infrastructure evidence:

1. HAProxy null-compute startup scaling at 32, 64, 128, and 256 nodes, with two
   independently materialized deployment lifecycles per node count.
2. Llama-3.1-405B-Instruct HAProxy non-streaming weak scaling at 4, 8, 16, 32,
   64, 128, and 256 nodes, using TP=8, PP=2, one replica per two nodes, and two
   client replays per deployment.

The PP2 protocol treats replay 0 as warmup and replay 1 as the reported sample.
It therefore supplies two executions of the workload, but only one reported
measurement per deployment; it must not be described as two independent
measurement lifecycles.

The authoritative specifications are:

- `eval/specs/sc26workshop/full/nullcompute_haproxy_scale_to256_v040.yaml`
- `eval/specs/sc26workshop/full/pp405b_pp2_haproxy_nostream_v040.yaml`

## 2026-09-05 corrected-snapshot attempt

The first post-fan-out snapshot, commit `4d204ce` with source snapshot hash
`bbbd13550a0c42d9cc8e740c97dd50a588140e4a44d4679aafb58e936fe93802`,
was exercised at the two smallest campaign gates:

- PP2 `run5/n4`, PBS `8807213`, and
- null-compute `run8/n32`, PBS `8807214`.

Both jobs are rejected negative evidence. Native source broadcast succeeded on
every allocated rank (4/4 and 32/32 respectively), but the verifier then failed
the source stage on every rank because it used the exact attempt directory as
`HOME`, TMP and XDG scratch while successful publication incorrectly required
that same directory to be empty. Neither job reached Ray startup or READY;
neither produced a result manifest or paper measurement. Both published clean
failure shutdown evidence.

Commit `bf7eafd` moves candidate cleanup to the outer owner after the finite
verifier MPI/PMIx process has exited, performs one exact collective cleanup,
and preserves the verifier error as the first cause if cleanup also fails. It
also explicitly discards Lmod/module bookkeeping from the closed child
environment; a one-node live lifecycle exposed that otherwise-unused
`__LMOD_REF_COUNT_PATH` retained login-home entries. The complete compute-node
suite passes with this correction, but a newly materialized snapshot and new
run identities are required before resubmission. `run5` and `run8` remain
immutable; unattempted `run9` is operationally superseded because it binds the
same defective snapshot and must not be submitted.

### Second low-node attempt

Commit `6816440`, source snapshot
`7298aefce67c5b1a7a820dc0f20e5135abc6a967a3d33ea3edc6239bd4d2aa6b`,
was exercised as PP2 `run6/n4` (PBS `8807651`) and null-compute
`run10/n32` (PBS `8807652`).

Null-compute `run10/n32` is accepted as the first corrected-snapshot n32
trial. Its complete seven-entry ResultManifest hash is
`ae4415f0d11234ad55fba4cc62a3569d75a8f3f6049101c1eee573df591cb863`.
It proved 32/32 ranks, 384/384 replicas, 64 Serve applications and 450/450
exact receipt slots; READY-after-trace-start was 29.369265 seconds,
`serve.run_many` was 10.5004 seconds and canonical deployment was 19.8270
seconds. All ranks acknowledged DRAIN and GOODBYE and terminal publication was
clean `STOPPED`.

The independently materialized `run11/n32`, PBS `8807874`, is accepted as the
second corrected-snapshot n32 trial. Its complete seven-entry ResultManifest
hash is
`d9c4361b1e7d08f00e520533be7581a4516494cda4e853c132e46e17119b7609`.
It independently proved the same exact 32/32-rank, 384/384-replica,
64-application and 450/450-receipt topology. READY-after-trace-start was
28.840942 seconds, `serve.run_many` was 10.5998 seconds and canonical
deployment was 20.0267 seconds. All 32 ranks acknowledged DRAIN and GOODBYE;
the deployment, HAProxy and rank launcher were reaped without cleanup errors,
and terminal publication was clean `STOPPED`. The corrected-snapshot n32 pair
is therefore 29.369265 and 28.840942 seconds, or **29.105103 +/- 0.373581
seconds** (mean +/- sample standard deviation). It is a separate homogeneous
pair and must not be combined silently with the preserved pre-hardening n32
pair below.

PP2 `run6/n4` is rejected negative evidence. Its source capsule and all four
per-rank Python/compatibility proofs completed correctly, but `model_bcast`
never launched: the staging-step list had captured its environment before
source activation installed `EXASERVE_QUALIFIED_PYTHON_SHA256` and the other
distributed proof values. Commit `1e5eb1b` defers the model staging environment
until execution after source activation and adds the previously missing
sequential handoff regression. `run6` remains immutable and must not be
resubmitted; PP2 requires a newly materialized run group from the follow-on
snapshot.

### Third PP low-node attempt and constructor diagnosis

The follow-on PP2 `run7/n4`, PBS `8807877`, used source snapshot
`270e59c1516b39f82310afa5ed9faf2a044baeb8d4be3a8d7b4b79332214db92`
and is rejected negative evidence. The source capsule completed on 4/4 ranks
in 4.3 seconds. Head-rooted shard-aware model distribution then sent stage 0
only to ranks 0 and 2 and stage 1 only to ranks 1 and 3; all four immutable
publication receipts passed. Model broadcast took 2,767.9 seconds. Ray reached
the exact four-node/48-GPU membership, and all four per-node proxy anchors were
RUNNING.

The run failed while constructing the two PP `EngineWorker` applications.
Each application started three actor processes, but neither emitted a
`VLLMEngine`/EngineCore/EngineShim startup record. Application replica 0 then
entered `DEPLOY_FAILED`; the durable outer first cause is `deployment:
UNEXPECTED_EXIT (exit=1)`. The run never reached canonical READY, gateway
launch or either replay. Its `results/` directory is empty and it contributes
no paper measurement. Failure cleanup is nevertheless complete evidence:
DRAIN and GOODBYE were received from 4/4 ranks, the deployment exited 1, the
rank launcher was terminated with the expected 143, and the shutdown report
records `clean=true`, no errors and no exhausted deadline. Stable immutable
model caches were preserved; no attempt candidate required removal.

The retained Ray application message collapsed the actual actor-start failure
to `Failed to update the deployments`, while Ray kept the detailed deployment
message only in the now-gone node-local controller logs. The absence of a
retained `VLLMEngine` stdout line is not phase evidence: the later successful
null canary also retained Ray/IPEX worker output but not its ordinary
`EngineWorker` constructor prints. The immutable artifact therefore cannot
distinguish pre-constructor bootstrap, canonical binding, compatibility
activation, tree validation or backend creation. Commit `b5a43f6` did not
invent a retroactive cause. It (1) applies PP-gated SC-11 to the inherited
`ray_head` and `ray_worker` roles before Ray's early Intel accelerator import,
(2) stops overriding Ray's node-owned `RAY_TMPDIR` through actor `runtime_env`,
and (3) adds bounded public `serve.status()` deployment diagnostics plus named
constructor phases so a future failure preserves its actual cause. It also
adds the eval-owned two-node TP8 x PP2 null-compute startup canary; the canary
exercises the real PP placement/compatibility lifecycle without distributing
model bytes and always owns DRAIN/GOODBYE cleanup. These changes are committed
but were not yet live-qualified by `run7`. The subsequent qualification and
fresh PP identity are recorded below. `run7/n4` is immutable and must not be
rerun.

### Two-node PP compatibility qualification and fourth PP attempt

The first canary materialization, `pp_compatibility_canary_2node/run0`, is a
rejected immutable partial record. Its one-second, 0.1-request/s/node trace
rounded to zero requests, and the fail-closed materializer stopped before
creating the `n2` cell. The retained run group contains only its `meta/spec.yaml`
and `meta/run_group.json`: it has no RunPlan, PBS submission, compute execution,
READY evidence, result manifest or measurement. It must not be repaired or
submitted under the same identity.

The corrected nonempty-trace identity, canary `run1/n2`, ran as PBS `8808125`
from commit `58d5524` and source snapshot
`a8ed2c2d7165e5547cb96e8a20076537832b28301759b3b066e38160059f2670`.
It is accepted **only as live compatibility/lifecycle qualification evidence**,
not as a paper data point. Source distribution and verification passed on 2/2
ranks. The real Ray default-worker process activated the replica role for one
TP8 x PP2 null-compute replica spanning both planned ranks, while the null path
correctly skipped model distribution and EngineCore creation. Canonical READY
proved 2/2 sessions, two exact Ray nodes, three exact Serve applications, two
healthy Serve proxies, 7/7 receipt slots, one of one model replica, a healthy
HAProxy gateway and a successful model canary. READY-after-trace-start was
43.500215 seconds.

Canary `run1/n2` published a complete seven-entry ResultManifest with hash
`9da71807c8102ff691108f022cd7de95ae8a717d84b9d5e53bc9715921f77aa6`.
The deployment exited 0; all 2/2 ranks acknowledged DRAIN and GOODBYE; terminal
state was `STOPPED`; and the shutdown report records `clean=true`, no errors
and no exhausted deadline. This closes the narrow live PP
default-worker-to-replica compatibility gate, but it does not exercise 405B
model staging, vLLM/EngineCore initialization, replay, or throughput.

The full PP2 paper campaign was then materialized as `run8` from the same
`a8ed2c2d...59f2670` source snapshot. Its n4/n8/n16/n32/n64/n128/n256 cells
are immutable distinct plans. The n4 gate ran as PBS `8808137` in the
capacity queue and is rejected negative evidence. Source capsule distribution
and verification passed on 4/4 ranks in 4.5 seconds. Head-owned source-model
verification took 974.94 seconds. Shard-aware distribution then sent the
408,748,312,778-byte stage-0 tree only to ranks 0 and 2 in 344.84 seconds and
the 407,607,560,984-byte stage-1 tree only to ranks 1 and 3 in 270.74 seconds.
Every target published its exact immutable 102-file receipt; the model entry
took 1,873.7709 seconds and the complete model-broadcast step took 2,849.9674
seconds (2,850.7 seconds at the composition-step boundary). Ray subsequently
proved the exact four-node/48-GPU cluster and all four proxy-anchor
applications reached RUNNING.

Both single-replica PP applications then failed deterministically. Each
application exhausted exactly three actor-start attempts before entering
`DEPLOY_FAILED`; the six observed actor PIDs were distinct. The retained public
status recorded both failed applications and deployments, but the then-current
serializer kept only the first 192 characters of each Ray deployment message.
That prefix ended inside Ray's fixed retry/traceback preamble, before the causal
tail. Consequently `run8/n4` does **not** prove that failure occurred before
`EngineWorker.__init__`, compatibility activation, `validate_node_local_tree`,
or `VLLMEngine.create`. Missing ordinary vLLM stdout is likewise not proof of
any one boundary.

`run8/n4` never reached canonical READY, HAProxy gateway launch or replay, and
its `results/` directory is empty. Cleanup nevertheless completed: all 4/4
ranks acknowledged DRAIN and GOODBYE, the deployment exited 1, the rank
launcher was reaped with its expected shutdown code 143, and the shutdown
report records `clean=true`, no errors and no exhausted deadline. PBS also
terminated with exit 1. The unsubmitted `run8` n8/n16/n32/n64/n128/n256 plans
bind the same source snapshot and causal-evidence defect; they are
operationally superseded and must not be submitted or reused. No `run8` cell
is a paper result.

Commit `5401cbd` corrects the diagnostic loss without changing deployment
semantics: it strips complete ANSI sequences and retains a bounded message
head plus the causal traceback tail, where the named `EngineWorker` phase and
root exception appear. Commit `ec48442` adds the validation-only
`pp8b_real_engine_canary_2node` specification: two nodes, one TP8 x PP2 real
8B replica and startup-only lifecycle. It is the inexpensive full-path probe
for engine-core binding, immutable tree validation, vLLM/EngineCore creation,
all 16 Ray engine workers, receipts and cleanup. The fix and specification are
committed. Its first execution and the diagnostic-coverage defect that it
exposed are recorded below; neither constitutes paper evidence.

### Real 8B PP diagnostic canary and single-replica coverage gap

The real-engine canary was materialized as
`pp8b_real_engine_canary_2node/run0/n2` from commit `ec48442`, source snapshot
`8f668a112c46974e332f1ac8e62b068a4b0193c878ade7a0fd87d9769e6c93d8`,
and submitted as PBS `8808210` in the debug queue. It is rejected diagnostic
evidence, not a paper workload result.

All finite staging gates passed. Source-capsule distribution and verification
completed on 2/2 ranks in 4.2 seconds. Head-owned source-model verification
took 31.27 seconds. Shard-aware transfer sent the 9,987,887,987-byte,
14-file stage-0 tree only to rank 0 in 7.01 seconds and the
11,095,244,439-byte, 15-file stage-1 tree only to rank 1 in 5.34 seconds. Both
targets published exact immutable receipts; their verification times were
15.529840 and 17.159565 seconds, respectively. The model entry took 48.7920
seconds, the sealed model-broadcast result records 81.2804 seconds total, and
the composition step completed in 82.0 seconds. This reproducer therefore
reached the real-engine branch with roughly 21.1 GB of stage payload rather
than rebroadcasting the approximately 816 GB 405B pair.

The one PP application then started three distinct actors (PIDs 106098,
106211 and 106326) and entered `DEPLOY_FAILED`. The run did not reach canonical
READY, gateway launch or replay; `results/` is empty. It preserves no supported
claim about whether the actor failed in canonical binding, compatibility
activation, tree validation or backend creation. Cleanup was complete: 2/2
ranks acknowledged DRAIN and GOODBYE, the deployment exited 1, the rank
launcher was reaped with expected code 143, and the shutdown report records
`clean=true`, no errors and no exhausted deadline. PBS exited 1.

The canary also proves a separate diagnostic-coverage defect. Commit
`5401cbd` made the multi-application `serve.run_many` failure wrapper preserve
the bounded causal traceback tail, but `deploy_from_canonical_binding` sends
`n_rep == 1` through a direct, unwrapped `serve.run` branch. Although Ray's
public `serve.run` delegates internally to its own multi-run implementation,
the ExaServe wrapper and `serialize_public_serve_status` never executed. The
retained traceback goes directly from `deploy_from_canonical_binding` into
Ray's `serve.run` and contains neither `serve_status=` nor a causal-tail
snapshot. This is a proven coverage bug, not evidence about the underlying
actor failure. Commit `c5dd69a` routes both `serve.run` branches and both
`serve.run_many` branches through the same bounded public-status diagnostic.
Fresh canary `run1/n2` was materialized from that commit and submitted as PBS
`8808227`. It is now terminal and rejected as described below. Rejected
`run0/n2` remains immutable and must not be rerun.

Canary `run1/n2`, source snapshot
`1b8179fe26189895c03bd1a49ff2d8d965a4c2bc51ca6467e8016c94cd44e272`,
proved that the common Serve wrapper works and localized the actor failure to
`EngineWorker` phase `backend_create`. After three actor-start attempts, the
retained final failure is a Pydantic `ValidationError` while constructing vLLM
`ModelConfig`: model architecture `LlamaForCausalLM` “failed to be inspected.”
This is the exact retained outer failure, but it does not reveal why vLLM's
registry inspection failed. The run never reached READY and its `results/`
directory is empty. All 2/2 ranks acknowledged DRAIN and GOODBYE; the
deployment exited 1, the rank launcher was reaped with expected code 143, and
the shutdown report records `clean=true`, no errors and no exhausted deadline.

Commit `f201923` then attached a bounded handler to the exact vLLM model-registry
logger around `AsyncEngineArgs` construction. Fresh canary `run2/n2`, source
snapshot
`fa856a1e8f7e65166a0867b896aec884dbe77d09d010c8119bb31e29dd92ef0c`,
ran as PBS `8808252` and is also rejected. It reproduced three actor-start
attempts and retained the same final `backend_create`/`ModelConfig`
architecture-inspection failure. The registry hook demonstrably fired, but
its logger record was rendered into text and then passed through nested
head/tail truncation. The retained tail contains only Transformers' nonfatal
deprecation warning for `TRANSFORMERS_CACHE`; it contains neither the registry
subprocess return code nor its causal exception/signal. This is another
diagnostic representation failure, not evidence that the warning caused the
actor failure. `run2/n2` has no READY or result artifact. Its deployment
exited 1 and its shutdown was again clean with 2/2 DRAIN and GOODBYE, expected
rank-launcher code 143, no cleanup errors and no exhausted deadline.

Commit `a970d38` replaced that nested text with a versioned structured
diagnostic carrying the registry exception, its immediate subprocess cause,
return code/signal, and separately bounded stderr head and tail. Fresh canary
`run3/n2`, PBS `8808281`, used source snapshot
`9198863dea1eb1b4bdcfa0691d631151d96c5f18cd32a146a37ebe1bd55ec8a7`.
Its exact model-broadcast result was again healthy (80.7057 seconds total;
9,987,887,987 bytes to rank 0 and 11,095,244,439 bytes to rank 1), after which
the replica exhausted three actor starts in `backend_create`.

This time the retained cause is decisive: vLLM's
`vllm.model_executor.models.registry` subprocess died with
`CalledProcessError`, `returncode=-11`, `signal=SIGSEGV` while inspecting
`LlamaForCausalLM`. Its stderr head begins in IPEX/XPU operator registration;
the tail contains only a nonfatal Transformers cache deprecation. The failure
therefore occurs in the fresh-cache native registry helper, before EngineCore
or model-weight loading, and is not caused by model size or the warning at the
stderr tail. `run3/n2` has no READY/result artifact and exited 1 after 4:31;
cleanup was clean with 2/2 DRAIN and GOODBYE, expected rank-launcher code 143,
no errors and no exhausted deadline.

The accepted historical PP `eabf59c/run3` environment exposes the likely hidden
dependency that these cold-cache canaries uncovered. At that revision the
actor runtime did not project `HOME`, `XDG_CACHE_HOME` or `VLLM_CACHE_ROOT`, so
Ray actors inherited `/home/wenyiw`; retained vLLM output explicitly names
`/home/wenyiw/.config/vllm`. vLLM defaults its model-info cache to
`~/.cache/vllm`, and the current
`vllm-model_executor-models-llama-LlamaForCausalLM.json` there predates the
accepted run by more than five months. Its recorded module hash
`a9013536a88eca40c49613d301e4d8ae` still matches the installed Llama module,
and the 754-byte file's current SHA-256 is
`28e56efcb7a06784a7a6ddaf45c25e022799bdf7f12b235b36c19b033d96609d`.
This is strong evidence that historical success used a shared-home cache hit,
whereas production hardening correctly creates a fresh generation-local
`VLLM_CACHE_ROOT` and therefore exercises vLLM's registry subprocess on a
miss. The historical manifest did not bind those cache bytes, so this remains
a documented dependency inference rather than retroactive immutable proof.
Restoring ambient `/home` cache access is not an admissible fix: it would
reintroduce worker Lustre fan-out and unversioned mutable runtime input. The
next diagnostic must preserve the registry exception type, subprocess return
code/signal and bounded stderr as separate structured fields before choosing
between a hash-bound distributed cache seed and a correctly isolated
cache-miss subprocess.

## Current accepted results

### Corrected-snapshot HAProxy null-compute startup

| Nodes | Exact replicas | READY trials (s) | READY mean +/- sample std (s) | `serve.run_many` trials (s) | Canonical deploy trials (s) | Status |
|---:|---:|---:|---:|---:|---:|---|
| 32 | 384 | 29.369, 28.841 | 29.105 +/- 0.374 | 10.500, 10.600 | 19.827, 20.027 | accepted corrected pair |
| 64 | 768 | pending | pending | pending | pending | not run from corrected snapshot |
| 128 | 1,536 | pending | pending | pending | pending | not run from corrected snapshot |
| 256 | 3,072 | pending | pending | pending | pending | not run from corrected snapshot |

| Trial | PBS job | Result-manifest SHA-256 |
|---|---|---|
| run10/n32 | 8807652 | `ae4415f0d11234ad55fba4cc62a3569d75a8f3f6049101c1eee573df591cb863` |
| run11/n32 | 8807874 | `d9c4361b1e7d08f00e520533be7581a4516494cda4e853c132e46e17119b7609` |

Both rows use source snapshot
`7298aefce67c5b1a7a820dc0f20e5135abc6a967a3d33ea3edc6239bd4d2aa6b`.
No corrected-snapshot number exists yet above 32 nodes.

### Preserved pre-hardening HAProxy null-compute startup

READY time is measured from the start of the sealed startup trace to the
canonical composition-root READY transition. It is not inferred from a log
string or proxy liveness check.

| Nodes | Exact replicas | READY trials (s) | READY mean ± sample std (s) | `serve.run_many` trials (s) | Canonical deploy trials (s) | Status |
|---:|---:|---:|---:|---:|---:|---|
| 32 | 384 | 57.237, 67.018 | 62.127 ± 6.916 | 7.242, 7.362 | 11.622, 11.843 | accepted |
| 64 | 768 | 86.021, 83.724 | 84.872 ± 1.624 | 20.286, 20.009 | 25.805, 25.416 | accepted |
| 128 | 1,536 | 238.008, 137.568 | 187.788 ± 71.022 | 103.956, 102.644 | 111.648, 110.328 | accepted |
| 256 | 3,072 | 582.279, no result | pending (two accepted trials required) | 553.095, no result | 565.570, no result | trial A accepted; `run7` trial B rejected |

Every accepted 32-node trial has exactly 64 Serve applications and 450 receipt
requirements. Every accepted 64-node trial has exactly 128 Serve applications
and 898 receipt requirements. Every accepted 128-node trial has exactly 256
Serve applications, 1,536 replica measurements, and 1,794 receipt requirements.
The accepted 256-node trial A has exactly 512 Serve applications, 3,072 replica
measurements, and 3,586 receipt requirements. All seven accepted manifests are
complete and all ranks acknowledged DRAIN and GOODBYE before a clean `STOPPED`
terminal publication.

The two 128-node deploy phases agree within 1.32 seconds, while their canonical
READY times differ by 100.44 seconds. The difference is post-deploy distributed
receipt convergence: trial A took roughly 125 seconds after its child trace,
versus roughly 26 seconds in trial B. Both waited for and sealed the complete
1,794/1,794 set; neither value is discarded as an outlier.

| Trial | PBS job | Result-manifest SHA-256 |
|---|---|---|
| run6/n32 | 8793058 | `11fdff87cff014a8fae3df643db92a0998af1fb7f420b60b15895db7139c8e16` |
| run7/n32 | 8793071 | `6b1f76f5273e85575aaa7ed1a67962aa0f2053f2f1354853d15d5d96510e551c` |
| run6/n64 | 8793087 | `3320cff6e7d3e4301d5a453ad32ce2255ec8fc835f2a62e7a1edb4b3ccc98f6f` |
| run7/n64 | 8793098 | `6dce8249dbb42473d9c485ea08f59caffec664ffdf9a536fbdab6f8e7bd5bb9a` |
| run6/n128 | 8798655 | `5b5c4fdff5c14e172d7236069ea14959775a5b4f147a999cf6c24923b45f2adf` |
| run7/n128 | 8798839 | `d4f2250283fb1162902e85bfd5f3d4c9a43e762fa791d54d9463cf00e31350b0` |
| run6/n256 | 8799035 | `c2b54ceb793e1b14b11012c10a6a714c403b8c666a82f550f3c927c1e024b3d5` |

These trials share source snapshot
`a6e383094c6320e83fa0f290298ca0510be5f21a2ffcb358868d8eb5aa8a82e1`
from commit `4d009c1` and use collision-resistant deployment identities.

The accepted n256 trial A started after 13:14:55 of eligible queue time and
completed as PBS job `8799035` (exit 0, walltime 16:54). Its canonical READY
time is 582.279 seconds, including a 553.095-second `serve.run_many` phase and a
565.570-second canonical deploy phase. The complete manifest binds all seven
expected artifacts; teardown published clean `STOPPED` after DRAIN and GOODBYE
from all 256 ranks. Trial B remains required before a mean or sample standard
deviation is reported.

### HAProxy non-streaming TP8 x PP2

The table reports replay 1. Replay 0 also completed with zero errors in every
accepted cell.

| Nodes | PP replicas | Requests | Errors | Successful RPS | p50 (s) | p99 (s) | Status |
|---:|---:|---:|---:|---:|---:|---:|---|
| 4 | 2 | 96/96 | 0 | 0.719703 | 14.950 | 17.611 | accepted |
| 8 | 4 | 192/192 | 0 | 1.437971 | 14.579 | 17.412 | accepted |
| 16 | 8 | 384/384 | 0 | 2.741455 | 14.890 | 24.384 | accepted |
| 32 | 16 | 768/768 | 0 | 5.339415 | 15.106 | 29.495 | accepted |
| 64 | 32 | 1,536/1,536 | 0 | 10.750777 | 14.785 | 24.624 | accepted |
| 128 | 64 | 3,072/3,072 | 0 | 20.739459 | 15.071 | 31.018 | accepted |
| 256 | 128 | pending | pending | pending | pending | pending | not run |

The accepted low-node cells are immutable `run3` artifacts from source snapshot
`160036b8367213da1d317ca9270de8c5016a471048842daea2ed1d8fe9b64b71`
at commit `eabf59c`. The 32-node and larger cells are pinned to
`run4`, snapshot
`a6e383094c6320e83fa0f290298ca0510be5f21a2ffcb358868d8eb5aa8a82e1`
at commit `4d009c1`. This split is a reviewed compatibility waiver, not an
unnoticed mix: the grouped-application behavior is admitted only for dense
TP1/PP1 `null_compute` plans. Its shared HAProxy route renderer retains `_r` as
the default and therefore preserves the PP2 route/configuration behavior; the
remaining change replaces lossy materialized deployment IDs. The PP2 model,
placement, gateway, replay, and offered-load semantics are unchanged. The
figure consumer must pin both exact source hashes and reject any third
snapshot. `run3/n128` must never be selected because its legacy truncated
deployment ID aliases `run3/n16`; all `run4` identities are bounded and
collision resistant.

The accepted n32 cell is PBS job `8793104` (exit 0, walltime 47:24) with
result-manifest hash
`211bd3088e9a8dea7e48280ebed2efd1444763bb71402a7279eb714710d84fbb`.
Both 768-request replays completed without errors. Canonical READY contained 48
Serve applications, 16 PP2 replicas, and 354/354 exact receipt slots; teardown
published a clean `STOPPED` report after DRAIN and GOODBYE from all 32 ranks.

The accepted n64 cell is PBS job `8798327` (exit 0, walltime 47:29) with
result-manifest hash
`ae7e1bf0cf57ed755095f7269a661548ab624fabbd8cefdcb6b3deff9dcedc13`.
Its warmup replay completed 1,536/1,536 requests with zero errors at 9.287877
RPS; the reported replay completed 1,536/1,536 with zero errors at 10.750777
RPS. Canonical READY contained 96 Serve applications, 32 PP2 replicas, and
706/706 exact receipt slots. Teardown published clean `STOPPED` after DRAIN and
GOODBYE from all 64 ranks. Reported throughput is 2.013x the accepted n32
throughput for a 2x node/replica increase.

The accepted n128 cell is PBS job `8798933` (exit 0, walltime 47:12) with
result-manifest hash
`0672c06711b531577965660f58640c7e3346567d137544be181e44a99afd5aa0`.
Its warmup replay completed 3,072/3,072 requests with zero errors at 21.083153
RPS; the reported replay completed 3,072/3,072 with zero errors at 20.739459
RPS. Canonical READY contained 192 Serve applications, 64 PP2 replicas, and
1,410/1,410 exact receipt slots. Teardown published clean `STOPPED` after DRAIN
and GOODBYE from all 128 ranks. Reported throughput is 1.929x the accepted n64
throughput for a 2x node/replica increase.

## Defects and feasibility decisions exposed by the campaign

1. **Unsafe Hugging Face link staging.** The first PP2 attempt could not stage a
   model containing Hugging Face cache symlinks. Staging now resolves only
   contained, validated targets and preserves the immutable input contract.
2. **Registration deadline began too early.** PP2 staging can move roughly
   380 GiB before the engine process exists. The registration clock now begins
   immediately before the owned process is started, rather than before model
   staging.
3. **Startup evidence was not schema-stable.** A valid early null-compute run
   reached exact READY and then failed evidence capture because an assumed Ray
   status timestamp was absent. Startup evidence now has an explicit v2 schema,
   identity/hash edges, same-boot monotonic READY anchors, and legacy-v1 read
   compatibility.
4. **PP2 had no usable KV cache on one Aurora tile.** At 0.90 memory utilization,
   a repeatable low-memory tile was 751.81 MiB short after the 405B weights and
   runtime footprint. The PP2-only setting is 0.95, yielding about 2,524 MiB of
   cache against 1,008 MiB required by `max_num_seqs=8`. This preserves model,
   parallelism, and offered load while adding bounded headroom.
5. **One Serve application per null replica did not scale.** The original
   128-node layout attempted 1,536 model applications and stalled at 557 before
   the one-hour queue limit. The admitted dense null topology now uses one
   node-pinned model application per rank with 12 native actors that still bind
   and attest the exact 12 canonical GPU slots. This reduces the total from
   13N to 2N Serve applications without reducing logical replicas or receipts.
   Uneven, multi-model, partial-device, non-null, TP>1, and PP>1 plans retain
   the exact per-replica layout.
6. **Materialized deployment IDs could alias.** Fixed 40-character truncation
   made long n16 and n128 identities equal. New bundles use `bounded_hash_v2`,
   retain a readable run/variant tail, and append a structured 12-hex digest.
   Historical bundles remain loadable under explicit legacy semantics.
7. **Scheduler feasibility is tight for PP2.** Aurora permits at most one hour
   in `debug-scaling` for 32, 64, and 128 nodes. The accepted 16-node job used
   about 48 minutes, largely because of cold model staging. A timeout at larger
   scale will be classified as a walltime/queue feasibility result; the paper
   workload will not be silently shortened to manufacture a number.
8. **The historical PP2 figure selection had drifted.** The original figure
   chose the numerically newest `run*/n*/result0.json` at render time. A later
   hardening edit pinned both streaming baselines to `run0`, which silently
   reduced the direct curve to four node counts and the HAProxy curve to one.
   The generator now names one immutable result per active serving/Ray node
   count, binds its result, legacy run plan, and producing PBS stdout by
   SHA-256, validates the recorded allocation subset, exact disjoint stage-xname
   sets, and exact disjoint replica-IP pairs/indices, and requires the
   4/8/16/32/64/128/256 ladder. The Figure 7 x-axis
   is therefore **active serving/Ray nodes**, not necessarily the containing PBS
   allocation: direct n64 and HAProxy n32/n64 used explicit 256-node `allocfix`
   subsets, while HAProxy n128 job `8686262` used the explicit 256-to-128
   `prod256` subset. All other selected legacy points used exact-size
   allocations.

   This is limited historical provenance, not current qualification. It proves
   the selected result bytes, legacy `run.yaml`, producing stdout, active-node
   cardinality, the independent stage-xname cardinality/disjointness claim, and
   the independent replica-IP index/cardinality/disjointness claim. The
   historical stdout does not record an xname-to-IP mapping, so it cannot prove
   that each staged xname is the same machine as a particular replica IP. It
   also does **not** provide a saved submission wrapper, a content hash for the
   historical source tree named by that plan, a current ResultManifest/
   RunProvenance chain, or current terminal/cleanup qualification. Figure 7 is
   consequently a deliberate mixed-contract overlay: historical streaming
   points use source revisions `d997b696`, `933a9d23`, and `2a447fb9`, GPU
   utilization 0.90, and
   HAProxy `maxconn=50000`; the new HAProxy non-stream curve uses the reviewed
   `run3`/`run4` current snapshots, GPU utilization 0.95, and `maxconn=8000`.
   Those differences must remain explicit in the paper rather than being
   described as one homogeneous rerun.

   The corrected post-allocation reruns are explicitly `result1.json`: direct
   n64 is job `8726613` at 10.299334 successful RPS (89.476% efficiency),
   HAProxy n32 is job `8726612` at 4.682119 RPS (81.353%), and HAProxy n64 is
   job `8702358` at 8.665885 RPS (75.286%). Historical HAProxy streaming error
   rates over reported (non-warmup) iterations are exactly 0% through n32,
   29/7,680 = 0.377604% at n64, 60/15,360 = 0.390625% at n128, and 77/30,720 =
   0.250651% at n256; all historical direct points are 0%. These errors reduce
   successful throughput, but Figure 7 does not display an error-fraction
   series. The existing paper prose and rendered
   `eval/plot/output/sc26_full/iter13/fig7_pp405b.pdf`, its adjacent caption
   text, and `doc/figures/fig7_pp405b.png` remain stale until the final
   current-infrastructure n128/n256 cells exist and the figure is deliberately
   rerendered. The corrected generator reads the reviewed selections from
   `eval/plot/pp405b_legacy_evidence.yaml`; fixing that generator does not by
   itself update any checked-in or copied paper artifact.
9. **The 256-node pre-START control path is load-sensitive.** Null-compute
   `run7/n256` (PBS `8800730`) used the same sealed plan and control limits as
   accepted `run6/n256`, but source distribution took 43.9 seconds instead of
   9.8 seconds and durable supervisor registration took 69.9 seconds for only
   238 ranks instead of 21.8 seconds for all 256. The first 75 established
   ranks then exhausted their 30-second pre-START heartbeat lease while the
   plan still allowed 300 seconds for registration. Rank 17 exited first and
   PALS terminated its peers. Code inspection identifies a concrete starvation
   path: the listener synchronously stages a snapshot under the receipt-ledger
   lock while the durable writer can hold that same lock across a Lustre event
   file and directory `fsync`. A deterministic slow-store/concurrent-snapshot
   test is required before selecting the fix; merely retrying could cherry-pick
   a favorable filesystem interval. The subsequent repository-wide audit in
   `doc/SHARED_FILESYSTEM_FANOUT_AUDIT_20260904.md` found several other active
   worker/shared-storage paths, so this lock race is credible but is not claimed
   as the sole cause. The eval adapter also misreported the
   already-terminal deployment as a readiness timeout because cleanup was still
   in progress. Both availability and terminal-cause propagation must be fixed
   and qualified before another 256-node attempt.

## Rejected evidence that must not enter the paper

- Per-replica null-compute `run2`/`run3` 32- and 64-node results predate the
  admitted grouped topology and final identity scheme.
- Grouped null-compute `run4`/`run5` 32-node diagnostics predate the final
  collision-resistant deployment identity.
- The per-replica `run2/n128` attempt stalled at 557/1,536 applications and was
  cancelled before walltime; it has no paper result.
- PP2 `run2/n16` deterministically failed KV-cache initialization at 0.90 and
  has no accepted result.
- Null-compute `run7/n256`, PBS job `8800730`, failed before Ray startup. Its
  exact 256-node allocation and source staging were valid, but only 238/256
  supervisor bindings were ever journaled, 75 established ranks reported
  pre-START heartbeat timeouts, and `results/` is empty. Its clean shutdown
  report proves bounded failure cleanup only: it has no DRAIN, GOODBYE,
  canonical READY, startup metric, or result manifest and contributes no paper
  number.
- PP2 `run7/n4`, PBS job `8807877`, completed exact source and shard-aware
  model distribution and formed the exact four-node Ray cluster, but both
  `EngineWorker` applications exhausted three actor-start attempts. It never
  reached READY or replay and `results/` is empty. Its clean 4/4
  DRAIN/GOODBYE shutdown is failure evidence only. The retained application
  error omitted Ray's detailed constructor message; absence of ordinary actor
  stdout cannot narrow the failure to a pre-vLLM or compatibility phase.
- PP2 `run8/n4`, PBS job `8808137`, again completed exact source/model
  distribution and four-node Ray formation before both PP applications failed
  after three actor attempts each. Its status serializer retained the Ray
  traceback prefix but truncated the causal tail. It has no READY, replay or
  result artifact and exited 1, although 4/4 DRAIN/GOODBYE cleanup was clean.
  All larger unsubmitted `run8` cells are operationally superseded with it.
- The real 8B PP canary `run0/n2`, PBS job `8808210`, completed exact source
  and approximately 21.1 GB shard-aware staging before its one application
  exhausted three actor-start attempts. It has no READY or result artifact and
  exited 1 with clean 2/2 DRAIN/GOODBYE. Its single-replica `serve.run` branch
  bypassed the new causal-tail serializer, so it diagnoses a coverage defect
  but does not establish the actor's root-cause phase.
- Real 8B PP canary `run1/n2`, PBS job `8808227`, proved the shared Serve
  wrapper and retained exact phase `backend_create` plus vLLM `ModelConfig`'s
  `LlamaForCausalLM` architecture-inspection failure. The swallowed registry
  cause is absent; it has no READY/result and clean 2/2 DRAIN/GOODBYE only.
- Real 8B PP canary `run2/n2`, PBS job `8808252`, proved the registry logging
  hook ran, but nested truncation retained only a nonfatal
  `TRANSFORMERS_CACHE` warning instead of subprocess exit/cause. It likewise
  has no READY/result and ended with clean 2/2 DRAIN/GOODBYE.
- Real 8B PP canary `run3/n2`, PBS job `8808281`, retained the structured
  native cause: vLLM's cold `LlamaForCausalLM` registry helper died with
  `returncode=-11`/`SIGSEGV` during `backend_create`. It has no READY/result
  and ended with clean 2/2 DRAIN/GOODBYE.
- PP2 `run2` 4- and 8-node successes use the superseded 0.90 setting; the final
  low-node values come from `run3` at 0.95.
- Three historical streaming PP2 cells were produced while their nominal
  32/64-node deployment was attached to a 256-node Ray cluster and must never
  enter the figure: direct `run3/n64` job `8669337`, HAProxy `run5/n32` job
  `8660044`, and HAProxy `run5/n64` job `8669336`. Their pre-fix result bytes are
  retained only as `result0.json.preallocfix_20260726T014708Z.bak` evidence; the
  strict ledger cannot select a `*.bak` filename.

## Remaining execution order

PBS job `8799035`, the exact `run6/n256` null-compute trial A bundle, completed
successfully and is accepted above. PBS job `8800730`, the independently
materialized `run7/n256` trial B bundle, failed before Ray startup and is
rejected above. The failed bundle is immutable negative evidence and must not
be reset, resubmitted, or silently replaced in the paper consumer.

The corrected-snapshot null-compute n32 pair is now complete as `run10/n32`
and `run11/n32`. PP2 `run7/n4`, PBS `8807877`, is the newest immutable failure
and must likewise never be reset or resubmitted. The two-node PP canary
`run1/n2` is accepted qualification evidence only. PP2 `run8/n4`, PBS
`8808137`, is also rejected and its larger siblings are superseded without
submission. Commit `5401cbd` contains the causal-tail diagnostic correction;
commit `ec48442` contains the real-engine 8B PP canary. Its immutable
`run0/n2`, PBS `8808210`, reproduced the actor-start failure but also proved
that the single-replica `serve.run` branch bypasses that correction. Commit
`c5dd69a` closes that coverage gap; rejected `run1/n2`, PBS `8808227`, then
localized the failure to `backend_create`/vLLM model-registry inspection.
Rejected `f201923` canary `run2/n2`, PBS `8808252`, fired its registry logger
hook but lost subprocess exit/cause to nested text truncation. Rejected
`a970d38` canary `run3/n2`, PBS `8808281`, then proved the cold registry helper
dies with SIGSEGV. All four 8B canary identities are immutable negative
evidence.

1. Ship reviewed, exact-version vLLM model-info seeds inside the immutable
   source capsule, bind their seed and target-module SHA-256 identities into
   the compatibility profile, and install/verify them on every node's local
   `VLLM_CACHE_ROOT` before Ray starts. Prove a cold local cache never launches
   the SIGSEGV registry subprocess, then materialize a fresh two-node 8B
   real-engine PP canary. Do not restore ambient shared-home model-info caches
   or reuse rejected `run0`/`run1`/`run2`/`run3` identities.
2. Only after that diagnosis, materialize a fresh 405B PP run group and run its
   n4 cell. The n4 workload must complete both replays before its n256 cell is
   submitted; no rejected `run7`/`run8` identity may be reset or reused.
3. For the current largest-scale bring-up objective, use the already-proven
   `run10`/`run11` null snapshot to run its two independently materialized n256
   lifecycles, and jump from the accepted fresh PP n4 gate directly to PP n256.
   Do not replay every intermediate scale merely as a launch prerequisite.
   The corrected n64/n128 null pairs and PP n8/n16/n32/n64/n128 cells remain
   required later for a homogeneous final curve; until they exist, preserve
   prior measurements separately and label any combined output explicitly as
   a mixed-campaign comparison.
4. Render the strict null startup table and full PP2 figure. Both consumers must
   reject missing, partial, malformed, or provenance-mismatched cells.
5. Replace all `pending` rows in this ledger with sealed evidence or an explicit
   externally blocked disposition, then run the complete release gate.
