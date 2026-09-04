# Shared-Filesystem Fan-Out Audit — 2026-09-04

Status: **RELEASE BLOCKER**

This audit was opened after the failed 256-node null-compute trial
`run7/n256` (PBS `8800730`) exposed load-sensitive startup behavior. It audits
the current branch at commit `6052979` against the product-owner requirement
that shared project/home storage have one allocation-head reader and that
runtime content be delivered to node-local storage through MPI/native
collectives.

No new compute job was run for this audit. Evidence comes from static call-path
inspection, the sealed `run6/n256` and `run7/n256` artifacts, the generated PBS
job, and current mount inspection on the Aurora login node.

## 1. Required invariant

After allocation entry:

1. Only explicitly head-owned processes running on the allocation-head node may
   open project artifacts on `/home`, `/lus/flare`, or any other
   SiteProfile-declared shared filesystem. Each input has one designated shared
   reader; this may be an MPI global-rank-0 staging helper on the same head node.
2. Source, canonical plans, allocation/site bindings, compatibility artifacts,
   model/tokenizer files, evaluation traces, executables, scripts, and any
   non-site runtime environment must be delivered to content-addressed
   node-local paths through one supervised MPI/native distribution transaction.
3. Workers publish small completion/status data through MPI gather or the
   authenticated live control channel. They never create or poll rank-named
   files on shared storage.
4. Large result or diagnostic payloads remain node-local until a bounded MPI
   reduction or chunked gather transfers them to one head-owned writer.
5. A node-local path must be proven local by SiteProfile policy and runtime
   filesystem identity. An absolute path, a `/tmp` spelling, or a top-level
   real directory is not sufficient when descendants can escape through
   symlinks.
6. Runtime descendants must have a closed path/environment allowlist. They may
   use node-local storage, procfs/devices, and explicitly qualified immutable
   site-local bootstrap files. They must not inherit a fallback project
   `PYTHONPATH`, user site-packages, shared `HOME` caches, or shared run paths.
7. Lifecycle observations and compatibility receipts may continue to use the
   authenticated TCP control channel. “Use MPI for files” does not replace the
   independent typed control protocol.

“Head reads once” is used below to mean **one shared-filesystem reader**. An
integrity check and a later streaming pass on that same head may still be
necessary unless the content hash is computed while streaming from a trusted
manifest.

## 2. Verdict

The implementation does **not** satisfy the invariant. The violation is broad,
not isolated to the receipt-ledger lock.

Confirmed fan-out includes:

- every rank reading shared plan and SiteProfile artifacts;
- every Serve replica and many engine workers reopening a shared plan and
  allocation binding;
- every source/model staging participant writing its own durable shared result;
- PP stage subsets deliberately selecting non-head shared-storage readers;
- multi-node replay ranks repeatedly hashing the entire shared trace, reading
  shared trace shards and executables, and writing shared result shards;
- every cleanly stopped rank writing a shared diagnostics archive and manifest;
- every normal managed framework interpreter loading a legacy user-site
  customization from `/home`; and
- no complete venv/runtime/eval/native-tool capsule being staged at all.

The release and the remaining 256-node paper jobs must remain blocked until
these paths are removed or explicitly classified as a qualified immutable
site-local bootstrap exception.

## 3. Critical findings

### SF-P0-01 — Canonical runtime artifacts fan out through Lustre

The outer head passes the shared `deployment.plan.json` path to every rank in
`src/exaserve/launcher.py:371-372`. `CompositionRoot.launch_ranks()` also
exports the shared allocation-binding and run-log paths in
`src/exaserve/composition.py:653-692`.

Each rank then independently:

- opens and parses `DeploymentPlan` in `src/exaserve/rank_main.py:346-367`; and
- opens and parses `SiteProfile` through
  `src/exaserve/site.py:249-267`.

The same shared plan/binding paths are deliberately inherited by Ray actors in
`src/exaserve/actor_runtime.py:23-68`. Every Serve `EngineWorker` reopens both
files in `src/exaserve/server.py:773-850`, and Ray/multiprocessing vLLM workers
reopen them again in `src/exaserve/compat/engine_shim.py:305-367`.

At null-compute n256, the plan is 2,385,887 bytes and the allocation binding is
19,098 bytes. The 3,072 Serve replicas therefore request about 7.3 GB of plan
data and 59 MB of binding data from the same shared artifacts, excluding rank,
Ray, metadata, and retry reads. These reads are unnecessary: the plan and
binding are immutable before rank launch.

Required correction: include `DeploymentPlan`, `SiteProfile`, and
`AllocationBinding` in the MPI-distributed runtime capsule, export only their
node-local paths, and pass compact already-validated placement/receipt
projections to actors. No actor should resolve a shared path.

### SF-P0-02 — Source staging reboots from shared code and uses N shared receipts

The useful data path in `bcast.c` is correct: communicator rank 0 alone opens
the archive source (`src/exaserve/resources/bcast.c:173-205,238-261`) and every
rank extracts the MPI-broadcast bytes locally (`:211-224,263-274`). The Python
transaction surrounding it breaks the invariant:

- `CompositionRoot.default_staging_steps()` retains inherited shared
  `PYTHONPATH` entries (`src/exaserve/composition.py:520-559`).
- After broadcasting the candidate, source staging launches
  `sys.executable -m exaserve.source_staging` on every rank without switching
  that command to the staged local package
  (`src/exaserve/source_staging.py:480-510`).
- Every verifier creates its own result JSON under the shared run directory
  (`src/exaserve/source_staging.py:504-515,604-615`).
- The generic result helper explicitly describes and implements independent
  shared rank files (`src/exaserve/staging_results.py:1-5,34-72`).
- Each `atomic_create_json` performs a temporary create, file `fsync`, link,
  unlink, and directory `fsync` (`src/exaserve/state/atomic.py:175-213`).

The failed n256 attempt contains exactly 256 source-stage rank-result files.
Its native transfer took 2.95 seconds, while the complete source stage took
43.9 seconds. This timing does not isolate one syscall, but it demonstrates
that most of the stage was outside the actual MPI payload transfer.

Even the head-only preparation is unnecessarily placed on Lustre:
`source_staging.stage()` constructs `.source-input.*`, copies the package, and
rehashes that copy beneath the shared run directory
(`src/exaserve/source_staging.py:465-473`). Capsule assembly should occur on
head-local storage before the designated head reader streams it.

The `bcast` executable is also built under the shared run directory and passed
as the application executable to all ranks
(`src/exaserve/model_bcast.py:360-405` and
`src/exaserve/source_staging.py:480-484`). There is no PALS executable-transfer
request or attestation, so the bootstrap itself still depends on a shared-path
`exec`.

Required correction: a separately qualified site-local bootstrap/verifier must
validate the freshly staged package before any code from that candidate is
executed. Verification and gathering may be integrated into the same MPI helper
as broadcast or use a second, separately supervised verifier collective; the
first helper has exited, so no communicator persists between the current
commands. Only global rank 0 publishes one aggregate manifest. Resolve the
bootstrap separately as described in section 7; a payload must never certify
itself.

### SF-P0-03 — Model and PP staging still use worker-side shared storage

All of these stages create one shared JSON file per rank:

- cache probe: `src/exaserve/model_bcast.py:446-502`;
- clean-stage cleanup: `src/exaserve/model_bcast.py:595-678`;
- model candidate publication: `src/exaserve/model_bcast.py:879-958`;
- source publication: `src/exaserve/source_staging.py:504-515,604-615`; and
- PP publication: `src/exaserve/pp_stage.py:212-269,307-330`.

PP staging has an additional direct-read violation. It divides nodes into one
communicator per PP stage (`src/exaserve/pp_stage.py:48-81,145-193`) and runs a
separate broadcast over each subset. Only communicator rank 0 reads that
stage's source, but stage 1's communicator rank 0 is an allocation worker, not
the allocation head. The source is a shared stage tree under
`<model_storage_path>/_pp_stage` and contains symlinks back to shared model
files (`src/exaserve/model_bcast.py:1039-1050` and
`src/exaserve/shard_prune.py:203-263`). The file header explicitly calls these
non-head nodes “seed” readers.

Single-replica PP has a separate unsupported path. Shard-aware staging is
restricted away from a single replica, and checked-in calibration/smoke specs
set `local_stage_path` equal to a Lustre model path with the stated intent that
vLLM read weights directly from shared storage. More generally, the compiler
only requires an absolute local-stage path
(`src/exaserve/plan/contracts.py:1152-1153` and
`src/exaserve/plan/compiler.py:1285-1294`); it neither classifies the filesystem
nor rejects a shared destination.

The current 405B flat path is itself a top-level symlink, which the present
`verify_and_publish_model()` top-level `lstat` check rejects before vLLM starts.
Those checked-in specs therefore encode a prohibited direct-Lustre intent but
may fail before exercising it today. The underlying contract defect remains:
an actual shared directory is accepted as `local_stage_path` and can be reused
and served directly.

Required correction: one allocation-wide distribution communicator must keep
global rank 0 as the sole shared reader. Each file/chunk carries a recipient
rank set, allowing PP stage-specific payloads to be sent to only the ranks that
need them. Cache/probe/publish outcomes must be MPI-gathered. Plans that cannot
prove a node-local destination must fail compilation.

### SF-P0-04 — A “local” model cache can escape back to Lustre

Model inventory permits symlink entries and follows them while hashing
(`src/exaserve/model_staging.py:107-155`). Publication verifies that the top
candidate is a real directory but does not require every descendant to resolve
inside the node-local root on the same filesystem
(`src/exaserve/model_bcast.py:800-865`). A `/tmp` model directory containing
weight symlinks to `/lus/flare` can therefore pass the completeness contract and
be reused by every engine.

The replica constructor also retains `local_model_path or model_id`
(`src/exaserve/server.py:883-886`). If the staged mapping is lost in a future
path, this fallback can send Hugging Face/vLLM back to ambient shared caches or
the network instead of failing closed.

Required correction: all published model descendants must be regular local
files, or resolve within the approved node-local root with the same filesystem
identity. Remove the model-ID fallback for a staged production plan, force
offline model loading, and redirect all Hugging Face/vLLM cache roots locally.
Key cache targets by the immutable model manifest/revision hash rather than
only model ID, and distribute only to gathered recipient ranks whose local
cache is missing or invalid.

### SF-P0-05 — The runtime environment is not staged or closed

Source staging copies only the `exaserve` package
(`src/exaserve/source_staging.py:395-422`). It does not stage:

- the interpreter or a relocatable venv;
- Ray/vLLM dependency content;
- `eval`;
- the Go replay binary;
- native staging tools; or
- gateway/client helper binaries.

`staged_pythonpath()` prepends local source but deliberately retains inherited
entries (`src/exaserve/plan/runtime_environment.py:13-20`). The generated PBS
job starts from a shared source snapshot and never sets `PYTHONNOUSERSITE`
(`src/exaserve/schedulers/base.py:333-364`). Rank, Ray, actor, and engine
environments are descendants of that environment.

The rank launcher also supplies no node-local `cwd`: `CompositionRoot` creates
`RankLauncher` without one (`src/exaserve/composition.py:653-692`), so the MPI
application inherits the allocation-head process's shared snapshot working
directory. The same environment exports the shared `EXASERVE_RUN_LOG_DIR` and
`EXASERVE_RUN_LOG_ROOT` to every rank. Even code that normally uses absolute
local paths therefore retains shared relative-path and logging fallbacks.

Current mount inspection reports both `/home` and `/lus/flare` as Lustre. The
framework Python has user-site enabled. Before source staging, normal framework
interpreters automatically import both:

```text
/home/wenyiw/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/sitecustomize.py
/home/wenyiw/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/usercustomize.py
```

The first file globally replaces `builtins.__import__` and mutates Ray Serve
timeouts. The second also installs an import hook and always emits a local
instrumentation file. After local source staging, the generated local
`sitecustomize.py` is first on `PYTHONPATH`, but the shared `usercustomize.py`
still loads because user site remains enabled. Thus every normal managed
framework interpreter retains a live undeclared shared-home import hook, while
pre-staging/source-verifier interpreters can load both legacy files. This
contradicts ADR-003's statement that the old global import override is absent
and that the generated overlay is the sole selected policy.

The current UAN mounts `/opt/aurora/.../frameworks` as read-only `squashfs`, not
Lustre. That may be a legitimate site-local bootstrap exception, but the same
mount and backing-store behavior must be verified on compute nodes rather than
assumed. Project/user environments under `/home` are not acceptable worker
dependencies.

Required correction: set `PYTHONNOUSERSITE=1` before the first Python process,
construct `PYTHONPATH` from a closed local allowlist, and redirect `HOME`,
`TMPDIR`, every library cache root, and worker working directories to verified
generation-scoped local directories. Prebuild and qualify a relocatable runtime
capsule during release/materialization; do not repackage an active venv inside
each allocation. The allocation head verifies and streams that capsule, or uses
an explicitly qualified site-local runtime. Remove shared run/log variables
from descendants. Fail if any managed module is loaded from a
SiteProfile-declared shared root.

### SF-P0-06 — Multi-node replay is deliberately Lustre-based

The replay path is the largest confirmed amplification:

1. `_run_replay_client()` launches every client rank with a shared snapshot as
   `cwd` and `PYTHONPATH`, a shared runtime-manifest path, and an inherited
   Python executable (`eval/lib/run_executor.py:578-680`).
2. Every rank loads the shared eval manifest before MPI initialization
   (`eval/replay_client.py:32-45` and
   `eval/lib/replay_engine.py:1707-1772`).
3. `EvalManifest.deployment_plan` calls the uncached `run_plan` property, which
   hashes the complete trace (`eval/lib/manifest.py:324-435`). The current
   replay call path invokes this at least four times per rank before shard
   consumption: manifest validation, the initial port lookup, the repeated port
   lookup inside URL validation, and URL validation's separate plan lookup.
4. For the source comment's 790 MB example, n256 therefore performs at least
   1,024 full scans, roughly 809 GB of shared reads, before consuming a shard.
5. Root writes N trace shard files next to the shared trace and fsyncs them
   (`eval/lib/replay_engine.py:1334-1437`). Every rank validates every shard path
   and lists that shared directory before reading its own shard
   (`:1263-1331,1853-1868`), creating at least 65,536 shared metadata probes at
   n256.
6. Every ordinary non-saturation replay rank locates, stats, and executes the
   Go binary from the shared source snapshot
   (`:581-597,708-815,1806-1811`). Saturation mode still resolves it on all
   ranks but executes its current single-root path only on root.
7. Result gathering explicitly uses N concurrent shared-file creates and root
   polling (`:75-239,1947-1965`). The source comment says the MDS “handles this
   fine”; the new invariant and the absence of a bounded shared-filesystem
   service guarantee make that an unacceptable production contract.

The generic direct-mode fallback also lets every replay rank read
`PBS_NODEFILE` before MPI initialization
(`eval/lib/replay_engine.py:1191-1205`). The canonical executor usually passes
resolved base URLs and avoids that branch, but the fallback must still become a
root-only topology read followed by MPI broadcast.

Required correction: initialize the replay communicator first. Global rank 0
alone loads and validates the runtime plan and trace, broadcasts a compact
validated replay contract, and streams bounded trace partitions to rank-local
files. Stage the Go binary locally. Use MPI reductions for summary mode and
supervised nonblocking/chunked point-to-point transfer to one root writer for
raw results. A blocking `Gatherv` alone is neither bounded nor fault-tolerant;
the protocol needs an absolute deadline, rank-completeness evidence, and a
bounded abort path. The existing communicator makes nested `mpiexec`
unnecessary.

### SF-P0-07 — No invariant or acceptance test prevents regression

`SiteProfile.filesystem_semantics` is recorded but is not used to classify or
enforce runtime paths. `DeploymentPlan` accepts any absolute
`local_stage_path`. `/tmp` is trusted by spelling without realpath/statfs
verification. Worker environments may retain shared paths.

Tests currently verify the safety and identity of rank-file/shard protocols
rather than forbidding those protocols on a shared runtime path:

- `tests/test_source_staging.py:230-260` exercises shared rank-result creation;
- `eval/tests/test_process_supervision.py:437-481` requires shared replay
  shards; and
- `eval/tests/test_process_supervision.py:485-500` requires shared trace shard
  materialization.

The fixtures use `tmp_path`; they do not themselves require a shared mount. They
nonetheless freeze a production design whose call sites place those files on
Lustre. `AC-DIST-01` proves native command construction and receipt
completeness, but it does not observe file opens or reject worker shared paths.
Existing `KI-A7` and `TD-COPPER` classify the problem as missing residual
measurements. The code now proves definite violations, so those findings
understate the blocker.

Required correction: make the invariant a canonical plan/ADR rule, enforce it
in path compilation and child-environment construction, and add syscall-level
qualification that fails on a non-head open beneath any declared shared root.

## 4. High-severity amplification outside direct worker reads

### SF-P1-01 — Head-only receipt persistence is still O(receipts) metadata I/O

Compatibility receipts correctly travel from workers over node-local sockets
and the authenticated control channel. The head then creates one durable event
file per binding (`src/exaserve/state/bindings.py:98-130,150-185`). Each event
incurs file and directory durability operations. The n256 null trial created
3,586 event files and periodically rewrote a growing `current.json` projection.

This is a single writer, so it does not violate the worker-open rule. It is
still not scale-efficient and contributed a shared-I/O dependency to the
control plane.

There is also a concrete event-loop blocking path:

1. `_on_snapshot()` awaits the current durable tail, then synchronously calls
   `_decode_snapshot_receipts()` (`src/exaserve/control/channel_runtime.py:555-611`).
2. That call enters `stage_rank_snapshot()` under the receipt-ledger lock
   (`src/exaserve/compat/receipt_v2.py:623-651`).
3. The durable worker can simultaneously hold the same lock while
   `commit_rank_snapshot()` calls `binding_store.bind_receipt()` and performs
   Lustre durability (`src/exaserve/compat/receipt_v2.py:674-707`).

Thus one listener coroutine can block its event-loop thread on a lock held
across filesystem I/O, preventing unrelated heartbeat handlers from running.
The dedicated writer reduces exposure but does not close the race between
capturing the old durable tail and submitting the next mutation.

`run6/n256` committed 256 initial supervisor bindings in about 21.8 seconds;
`run7/n256` took about 69.9 seconds to journal only 238, and the first 75
established ranks then reported pre-START heartbeat timeouts. This correlation
and the static lock path make the mechanism credible, but without a targeted
slow-store trace it must not be called the sole proven cause.

Required correction: no event-loop operation may acquire a lock that can be
held across I/O. Serialize the whole validate/stage/commit transaction off-loop,
group-commit receipt events, and publish a compact projection/hash at defined
barriers rather than one fsync-heavy file per receipt.

### SF-P1-02 — READY refresh rewrites a multi-megabyte record

At n256, `deployment_status.json` is about 4.3 MB because it embeds thousands
of capability entries and receipt hashes. Every READY validation refresh loads
and atomically rewrites that projection
(`src/exaserve/status_api.py:524-589`). Long PP/replay runs repeat this on the
validation cadence.

Required correction: persist immutable detailed evidence once, and refresh a
small lease/status record that references its authenticated manifest hash.

### SF-P1-03 — Engine attestation can multiply artifact reads thousands of times

Every compatibility self-receipt hashes the entire resolved executable
(`src/exaserve/compat/producers.py:66-98,252-298`). The frameworks Python is
31,455,456 bytes. This is about 8.1 GB of aggregate executable hashing for 256
NodeSupervisors and about 96.6 GB for 3,072 replicas, even before engine-worker
receipts. If compute-node `/opt` is qualified site-local squashfs, this is
CPU/page-cache amplification rather than a shared-reader violation; if its
backing is shared, it becomes another P0 violation. Either way, repeatedly
hashing an already content-addressed runtime per process is unsuitable at this
scale.

The engine attestation watcher polls every 0.1 seconds for up to 600 seconds
(`src/exaserve/compat/engine_shim.py:525-589`). Each identified-but-not-yet-
proved Ray or multiprocessing worker attempt rebuilds identity by reopening
plan/binding and rebuilds a full self-receipt, including the executable hash
(`:305-416`). The EngineCore identity branch receives its exact IDs directly
and does not perform those plan/binding loads. A single delayed affected worker
can theoretically perform about 6,000 iterations.

Current compatibility startup adds another large multiplier. Generated
`sitecustomize` verifies installed target sources and `install()` reads them
again in every managed interpreter
(`src/exaserve/compat/generated_overlay.py:348-386,554-576`). Ray prestarts up
to eight generic Python workers per node
(`src/exaserve/control/ray_runtime.py:125-160`), or 2,048 interpreters at n256.
Each can load shared `usercustomize`; replica construction then invokes
`CompatibilityActivator` once at `src/exaserve/server.py:878-880` and again at
`:1047-1053`.

Required correction: bind receipt identity to the already-verified node-local
runtime capsule, memoize immutable identity once per process/node, and make the
poll loop check only changing in-memory postconditions.

### SF-P1-04 — Per-rank shutdown diagnostics write directly to shared storage

Every cleanly stopped rank launches a diagnostics subprocess with the shared
run directory (`src/exaserve/rank_main.py:619-645`). Each process can create a
64 MB archive plus a manifest directly under `run_dir/per_node`
(`src/exaserve/state/diagnostics.py:129-283`). The successful n256 null trial
created 256 archives and 256 manifests within the shutdown interval.

Required correction: collect archives locally, then use one bounded post-run
MPI gather or retain only small control-channel summaries. Only the head may
publish shared diagnostic artifacts.

### SF-P1-05 — ClientLab and scale harnesses repeat shared-path launch patterns

ClientLab's PBS synthetic-target path launches every remote target from the
shared repository, gives each rank a shared config template, and executes a
shared C++ binary (`clientlab/runner/runtime.py:2138-2203`). Several hardening
harnesses SSH to allocated workers and invoke helper scripts by their shared
repository paths.

These are validation/tool paths rather than the canonical serving runtime, but
they cannot credibly qualify the new invariant while violating it themselves.
They must use the same runtime capsule or be explicitly excluded from
production-scale evidence.

## 5. Paths that are already sound or locally scoped

The following pieces should be preserved:

- normal non-PP `bcast.c` payload bytes are read by MPI communicator rank 0 and
  broadcast into local destinations;
- source candidates publish beneath `/tmp/exaserve_src` transactionally;
- current paper PP models use `/tmp/pp405b_shard` as their intended runtime
  destination;
- Ray runtime/session state is redirected beneath a generation-scoped `/tmp`
  root;
- process-ownership receipts and local receipt ingress sockets live beneath
  private `/tmp` roots;
- compatibility receipts and lifecycle observations travel over local IPC plus
  the authenticated TCP control channel, not shared files;
- deployment, gateway, scheduler, global status, and final result publication
  are allocation-head responsibilities; and
- final replay JSON publication is root-only.

Two caveats remain even for these paths:

1. Non-PP broadcast assumes MPI communicator rank 0 is the allocation-binding
   rank 0; it does not attest the expected root hostname.
2. A `/tmp` destination is not safe until its mount identity and all descendant
   paths are verified as node-local and non-escaping.

## 6. Specification and evidence corrections required

The authoritative production plan already contains parts of the desired rule:

- WP3 prefers one immutable environment staged to every node and used by all
  process roles;
- WP3 says processes must not race to write independent shared-filesystem
  receipt files; and
- WP6 requires unified transactional model/environment/native-tool staging.

Those requirements are not implemented. Before this audit, the product-owner
rule was also stricter than the plan because the plan did not state a universal
non-head shared-open prohibition. This audit adds that normative prohibition to
the canonical plan; implementation and acceptance evidence remain outstanding.

The following evidence claims must be reopened or narrowed:

- `KI-A7` and `TD-COPPER`: this is no longer only missing 128/256-node
  measurement; there are known active violations.
- ADR-003 and `KI-D3`: a legacy global user-site import hook is currently live
  in every Aurora interpreter, so “removed/unreachable” is false for the actual
  execution environment.
- `AC-DIST-01`: receipt completeness is insufficient without a negative
  non-head shared-open proof.
- any paper/release claim that treats a successful retry as proof the shared
  filesystem path scales. A favorable filesystem interval is not an
  architectural qualification.

## 7. Required target architecture

Implement one allocation-wide `DistributionTransaction` owned by the outer
supervisor:

1. **Bootstrap.** Establish one executable that PALS can start without worker
   shared-project access. First test native PALS executable staging. If absent,
   use a hash-pinned, qualified site-local bootstrap (for example the current
   read-only site image). Do not silently keep the shared `bcast` executable as
   an exception.
2. **Runtime capsule.** Release/materialization prebuilds and qualifies one
   content-addressed relocatable runtime containing ExaServe and eval code,
   compatibility content, Go/native helpers, and non-site dependencies. The
   allocation head verifies and streams it; it does not package the active
   environment afresh. A small run-specific capsule/overlay adds the exact
   plan/site/binding. MPI distributes these to a generation-scoped local
   candidate, and the qualified bootstrap—not code from the unverified
   candidate—verifies it before atomic publication.
   ADR-003's provenance rejection still applies: blindly copying or packing an
   active vendor installation is forbidden. The base must be a reproducible
   vendor-proven artifact/image, or the immutable site-local image must be
   qualified directly; only reproducibly owned additions are staged.
3. **Root identity.** The collective proves that MPI global rank 0 maps to
   allocation-binding rank 0 before it opens shared source content.
4. **Recipient-aware models.** Every model file/chunk carries an exact recipient
   rank set. Global rank 0 streams each shared file once and sends PP-stage
   payloads only to the ranks that need them. This also supports single-replica
   PP without direct Lustre loading.
5. **MPI receipts.** Cache, cleanup, extraction, hash, and publication outcomes
   are gathered within a supervised collective or nonblocking point-to-point
   protocol with an absolute deadline and bounded abort. Only rank 0 writes one
   authenticated aggregate result.
6. **Closed runtime.** Rank and actor paths point only into the local capsule;
   shared `PYTHONPATH`, SiteProfile, plan, binding, run directory, user site,
   and home/cache fallbacks are removed.
7. **Replay distribution.** Replay initializes MPI first. Root validates once,
   broadcasts the compact contract, streams rank partitions locally, and
   gathers results through reductions or supervised nonblocking/chunked
   transfers with an absolute deadline and bounded abort.
8. **Head persistence.** Detailed receipts are group-committed or stored in one
   compact append journal. READY status references immutable manifests rather
   than rewriting the entire evidence set.

## 8. Mandatory acceptance gates

No release or new 256-node paper run should proceed until all gates pass:

1. A static launch test rejects any non-head argv, cwd, `PYTHONPATH`, plan,
   binding, config, trace, model, result, diagnostics, or executable path below
   a declared shared root.
2. A clean-interpreter test sets `PYTHONNOUSERSITE=1` and proves neither
   external `sitecustomize` nor `usercustomize` is loaded; only the staged,
   hash-bound compatibility bootstrap may run.
3. A path-containment test rejects shared `local_stage_path` values and all
   descendant symlink/mount escapes.
4. A deterministic slow-store/concurrent-snapshot test proves the control event
   loop continues acknowledging heartbeats while persistence is blocked.
5. A two-node syscall trace begins at managed-process creation and proves that
   non-head ranks and all descendants open no path under `/home`, `/lus/flare`,
   or other declared shared roots. If tracing begins after a bootstrap, separate
   evidence must first prove the complete executable/loader/bootstrap chain is
   immutable and site-local.
6. A multi-node distribution fault test injects one extraction/hash failure and
   proves MPI aggregate failure, no shared rank files, and no partial local
   publication.
7. A PP test proves global allocation rank 0 is the sole shared model reader for
   every PP stage.
8. A replay test proves one root trace read, no shared worker shard/result
   files, bounded MPI transfer, and exact result completeness.
9. A scale telemetry gate records per-rank shared-open counts and requires zero
   for non-head processes. The tracing mechanism itself must not create a
   filesystem storm.
10. Only after these pass should the paper campaigns resume. Because this is a
    material runtime/control/storage change, every node count used in a
    homogeneous curve must be rerun from the same corrected snapshot. Otherwise
    the output must be explicitly labeled a mixed-campaign comparison rather
    than a homogeneous scaling curve. PP n256 remains last.

## 9. Immediate disposition

- Do not resubmit failed `run7/n256`; it is immutable negative evidence.
- Do not submit PP n256 on the current runtime.
- Do not pair accepted `run6/n256` with a fixed-code retry as two homogeneous
  measurements. A runtime/storage change also prevents corrected n256 results
  from being silently appended to the existing n32/n64/n128 curve. Either rerun
  every plotted node count from one corrected snapshot (twice where the paper
  requires two lifecycles) or label the result as a mixed-campaign comparison.
- Reassess the full PP curve as well: disabling the undeclared user-site patch
  layer and changing runtime/model staging can affect engine behavior, so an old
  point is not automatically homogeneous with a corrected n256 point.
- Preserve the successful `run6` evidence, but do not interpret success during
  one favorable storage interval as proof of production scalability.
