# ExaServe Production-Readiness Audit

> **Document role:** Corrected finding and evidence register. This audit
> describes observed defects and risk; its remediation prose is not the worker
> specification. Implementation architecture, order, acceptance gates, and
> completion are governed by `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`.

**Audit date:** 2026-08-04  
**Audited branch:** `feature/slurm-amd-support`  
**Audit type:** Static code and architecture review plus lightweight unit-test execution  
**Runtime experiments:** None. No PBS allocation, GPU workload, deployment, or evaluation was launched.  
**Worktree changes from audit:** Documentation only; no runtime code changes.
**Verification revision:** Cross-checked against
`doc/PRODUCTION_READINESS_CLAUDE_AUDIT.md` on the same commit. Valid citation
and arithmetic corrections are incorporated below; disputed claims are
adjudicated with evidence in that document.

## Executive summary

ExaServe is a capable HPC research and benchmarking system, but it is not yet
ready to operate as an unattended production inference service. The most serious
risks are not cosmetic code-style concerns: several paths can report success
after a deployment failure, accept incomplete model data, declare a degraded
cluster ready, duplicate scheduler jobs, or produce incomplete/stale evaluation
results without making the run fail.

The current code is best understood as three related products with different
readiness levels:

1. `src/exaserve`: an experimental HPC inference launcher and serving runtime.
2. `eval`: a research evaluation control plane and replay harness.
3. `clientlab`: a client-side load and diagnostic laboratory.

These surfaces share concepts but do not yet share one strict configuration
model, scheduler abstraction, artifact lifecycle, or failure contract. That
fragmentation is one of the main architectural sources of future glitches.

### Release recommendation

Do not label the current branch production-ready. Before a production release,
at minimum complete findings PR-001 through PR-010, establish a green hermetic
CI gate, pin the Ray/vLLM compatibility matrix, and validate the intended
vendor/scheduler combinations in controlled cluster runs.

## Severity definitions

- **Blocker:** Can make a failed or unsafe deployment look successful, serve bad
  state, or violate a fundamental security/integrity expectation.
- **High:** Likely to create duplicate work, incorrect resource placement,
  misleading results, outages, or serious operational ambiguity.
- **Medium:** A reliability, maintainability, portability, or observability gap
  that should be resolved before broad deployment.
- **Low / debt:** Does not immediately break a deployment, but makes future
  changes fragile or unnecessarily difficult to reason about.

## Detailed findings

### PR-001 — Fatal errors are converted into successful exits

**Severity:** Blocker  
**Regions:** `src/exaserve/driver.py:616-645`, `src/exaserve/driver.py:638-640`,
`src/exaserve/resources/launch_cluster.sh:514-515`

The driver logs a nonzero ExaServe child return code but does not propagate it.
It also ignores the Ray worker return code. Most importantly, its broad
`except Exception` handler prints the exception and returns normally. Python
therefore exits with status zero after many fatal startup failures.

Because `launch_cluster.sh` invokes the driver under MPI/srun, the scheduler can
record a successful job when the driver converts a model-deployment or serving
failure into a zero exit. The launcher itself has `set -e` and would propagate a
genuine nonzero launcher/driver status; the defect is that the driver returns
zero on these failure paths. Downstream automation therefore cannot reliably
distinguish success from failure.

**Recommended change:** Give every supervised child an explicit failure
contract; propagate its return code or raise `SystemExit(nonzero)`. Preserve the
original exception after cleanup. Add tests that execute the CLI around injected
child failures and assert a nonzero process result.

### PR-002 — Ray startup ignores configured accelerator count and vendor

**Severity:** Blocker for AMD/Slurm claims; High on Aurora  
**Regions:** `src/exaserve/driver.py:146-180`, `src/exaserve/driver.py:225-271`

Both Ray head and worker commands advertise `--num-gpus=12`, ignoring
`DeploymentConfig.num_gpus_per_node`. `get_ray_env()` also installs Aurora/XPU
variables such as `VLLM_TARGET_DEVICE=xpu` unconditionally. The shell launcher
contains vendor gating, but the Python driver overrides that effort when it
creates Ray subprocess environments.

On an 8-GPU AMD node this can advertise phantom resources and allow Ray to place
actors that cannot acquire real devices. On other non-XPU sites it can select the
wrong vLLM target.

**Recommended change:** Resolve the vendor and accelerator count once, before
Ray startup, from validated deployment/site configuration. Pass them into both
Ray start commands and vendor-specific environment builders. Verify the Ray
resource view against physical devices before deployment.

### PR-003 — The launcher mutates the operator's source configuration

**Severity:** High  
**Regions:** `src/exaserve/resources/launch_cluster.sh:98-114`,
`src/exaserve/resources/launch_cluster.sh:145-160`,
`src/exaserve/resources/launch_cluster.sh:292-298`,
`src/exaserve/server.py:1870-1881`, `src/exaserve/driver.py:377-423`

The launcher writes the resolved Ray head IP directly into the original YAML.
It copies the YAML to the run log directory before performing that mutation, so
the audit copy is not the configuration actually used. Server and proxy
artifacts such as `ray_node_ips.txt`, proxy configs, and proxy-port files are
also written beside the configuration.

Two jobs using the same YAML can overwrite each other's head IP and artifacts.
A read-only config cannot be launched. A crash during YAML rewrite can leave a
truncated source config.

**Recommended change:** Treat input configuration as immutable. Create a unique
per-run directory, copy the validated config there, write runtime-resolved fields
to a separate runtime descriptor, and publish all files through atomic
temp-write-plus-rename.

### PR-004 — MPI model broadcast can hide archive/extraction failures

**Severity:** Blocker  
**Regions:** `src/exaserve/resources/bcast.c:17`,
`src/exaserve/resources/bcast.c:74-90`, `src/exaserve/resources/bcast.c:94-145`

The native broadcaster constructs `tar` and `mkdir` shell commands by inserting
paths without shell quoting. It does not validate command truncation and ignores
the return values from `system()` and both `pclose()` calls. A tar producer error,
extractor error, disk-full event, or malformed path can therefore be reported as
a successful broadcast.

Each MPI rank also allocates a fixed 1 GiB buffer. At large node counts this is a
large aggregate memory cost for a streaming copy operation.

**Recommended change:** Avoid the shell entirely or use a library/archive child
created with argument-vector APIs. Check all child statuses with
`WIFEXITED`/`WEXITSTATUS`, aggregate failures across ranks, and return nonzero.
Use a bounded streaming buffer such as 16-64 MiB.

### PR-005 — Model-cache completeness checks are too shallow

**Severity:** Blocker  
**Regions:** `src/exaserve/model_staging.py:29-83`,
`src/exaserve/model_staging.py:144-187`,
`src/exaserve/model_staging.py:211-231`

A directory is considered complete if it contains `config.json` and any one
top-level `.bin`, `.safetensors`, or `.pt` file. The code does not validate all
weight shards referenced by an index, tokenizer/configuration files, sizes,
checksums, or an explicit completion marker.

Hugging Face downloads write directly to the final model directory without a
per-model lock, temporary directory, pinned revision, or atomic rename. An
interrupted or concurrent download can leave partial data. The next launch then
either misclassifies the directory as complete or refuses to repair it
automatically.

When a Hugging Face ref is unavailable, `_resolve_hf_cache_snapshot()` selects
the lexicographically first snapshot rather than a requested or newest revision.

**Recommended change:** Pin model revisions; download under a locked temporary
directory; validate the Hugging Face index and required assets; optionally hash
or size-check files; write a completion manifest last; atomically rename into
the cache; and define a safe repair/quarantine workflow.

### PR-006 — Configuration parsing silently changes user intent

**Severity:** High  
**Regions:** `src/exaserve/schemas.py:89-131`,
`src/exaserve/schemas.py:134-197`, `eval/lib/models.py:66-166`,
`eval/lib/spec_io.py:137-286`

Manual parsing uses `bool(value)`, so strings such as `"false"` become `True`.
Invalid `num_replicas` values are converted to `None`, silently changing an
explicit request into automatic replica planning. Unknown keys are ignored,
making misspellings difficult to detect.

The current validation omits or incompletely checks several important values:

- GPU-memory utilization bounds.
- Positive model length, size, CPU, sequence, and worker counts.
- Proxy type, port ranges, and options shape.
- Scheduler and engine enumerations.
- Arrival/generation/sampling modes.
- Scheduler-node consistency with deployment-node count.
- Local model/storage path policies.

**Recommended change:** Replace the hand-written coercion layer with one strict,
versioned schema shared by serving, eval, and ClientLab. Forbid unknown fields by
default, provide explicit migrations, and report all validation errors before
allocation or staging.

### PR-007 — Distinct model IDs can collide in routes and cache directories

**Severity:** High  
**Regions:** `src/exaserve/model_paths.py:5-17`,
`src/exaserve/schemas.py:152-154`

Model IDs are checked for raw-string uniqueness, but their derived storage and
route identifiers are not checked. Examples confirmed during the audit:

- `a/b--c` and `a--b/c` both map to storage name `a--b--c`.
- `a.b/c` and `a-b/c` both map to route name `a-b--c`.

This can overwrite caches, collide Ray deployment names, or route a request to
the wrong model.

**Recommended change:** Use a reversible encoding or append a stable digest to
normalized names. Validate uniqueness across every derived identifier before
staging or deployment.

### PR-008 — Readiness is fail-open and can hang indefinitely

**Severity:** Blocker  
**Regions:** `src/exaserve/server.py:1819-1863`,
`src/exaserve/server.py:2090-2107`, `src/exaserve/server.py:2143-2163`,
`src/exaserve/server.py:2193-2205`, `doc/KNOWN_ISSUES.md` D1

After ten minutes, the server proceeds with fewer GPUs than requested. One
proxy-readiness path loops without an overall deadline. Nonhealthy proxy states
and status-collection exceptions are warnings, after which the server prints
`CLUSTER FULLY READY`.

The project failure log confirms a real 256-node incident where readiness was
declared despite a crashed controller and unhealthy service.

**Recommended change:** Define a readiness contract containing expected nodes,
expected GPUs, planned replicas, running replicas, healthy Ray proxies, and
healthy external proxies. Fail closed by default with a configurable explicit
degraded-mode policy. Apply global deadlines and return structured failure
details.

### PR-009 — External proxy processes are not continuously supervised

**Severity:** High  
**Regions:** `src/exaserve/driver.py:599-616`,
`src/exaserve/driver.py:647-667`, `src/exaserve/proxy/*_proxy.py`

The driver starts the selected proxy and then waits only for the ExaServe server
process. If HAProxy, LiteLLM, Envoy, NGINX, or Pingora exits after readiness, the
job can remain alive and continue to look healthy to scheduler-level monitoring.
Some proxy implementations retain log file handles without a clearly paired
close. Ray termination uses an unbounded `wait()` after `terminate()`, so cleanup
itself can hang.

**Recommended change:** Introduce one process supervisor that monitors every
essential child, terminates the deployment when an essential service exits,
captures its reason, and performs bounded graceful shutdown followed by forced
cleanup.

### PR-010 — Public service and administrative endpoints lack a production security boundary

**Severity:** Blocker when reachable outside a trusted allocation network  
**Regions:** `src/exaserve/server.py:966-1079`,
`src/exaserve/server.py:1803-1814`,
`src/exaserve/proxy/haproxy_proxy.py:168-177`,
`src/exaserve/proxy/litellm_proxy.py:55-119`,
`src/exaserve/engines/sglang.py:58-75`

Ray Serve binds to `0.0.0.0`. The OpenAI-compatible endpoints do not implement
authentication, authorization, TLS, request-size policy, rate limits, tenant
isolation, or an auditable identity boundary. HAProxy exposes `stats admin if
TRUE` on `*:9999` without authentication. LiteLLM authentication is optional and
defaults to no master key, while generated config files may contain plaintext
secrets under default filesystem permissions.

SGLang loads tokenizer/model code with `trust_remote_code=True`, making model
revision trust a code-execution decision.

**Recommended change:** Document whether ExaServe is internal-only or a public
service. For anything beyond a trusted allocation network, require a hardened
gateway or implement TLS/authentication, bind administrative endpoints to
loopback or a management network, disable HAProxy admin by default, store secrets
outside YAML with restrictive permissions, and pin/allowlist model revisions.

### PR-011 — Request validation and OpenAI API behavior are incomplete

**Severity:** High for public serving; Medium internally  
**Regions:** `src/exaserve/server.py:1002-1079`,
`src/exaserve/engines/vllm.py:234-275`

Handlers parse raw JSON dictionaries and directly call `float()`/`int()`.
Malformed values can become generic 500 errors. Negative or excessive token
limits and invalid temperatures are not rejected centrally. Message/prompt
types, requested model names, and many protocol fields are not validated. The
body's `model` value is effectively ignored by a model-specific deployment.

Streaming error and cancellation behavior is incomplete: generator failures are
not consistently encoded into structured SSE errors, and vLLM request abortion
on client disconnect is not explicit in this layer.

**Recommended change:** Use versioned request/response models, return consistent
4xx errors, enforce resource limits before generation, validate the requested
model, propagate request IDs, and test disconnect/cancellation and streaming
failure behavior.

### PR-012 — Port allocation is race-prone and reported URLs can be wrong

**Severity:** High  
**Regions:** `src/exaserve/server.py:278-291`,
`src/exaserve/engines/sglang.py:74-76`,
`src/exaserve/proxy/litellm_proxy.py:127-164`,
`src/exaserve/submit.py:183-224`, `doc/KNOWN_ISSUES.md` A2

Several components probe a port by binding and closing it, then later ask the
real process to bind the same port. This is a time-of-check/time-of-use race.
Other engine ports are derived from static bases plus device IDs. The project's
failure log documents real `EADDRINUSE` incidents at scale.

LiteLLM may fall back to a dynamically selected port, but `serve_url()` reports
the configured port and never reads the actual published proxy-port file. It
also treats scheduler state `R` as sufficient even though application readiness
may occur much later. Terminal job states are not failed immediately when
waiting.

**Recommended change:** Let the serving process bind port zero or use a lease
held until handoff. Publish one atomic service descriptor containing actual
host, port, readiness state, run ID, and model routes. Make URL discovery consume
that descriptor rather than scheduler state plus source configuration.

### PR-013 — `submit-all` can submit the same run more than once

**Severity:** High  
**Regions:** `eval/lib/run_executor.py:421-480`,
`eval/lib/run_executor.py:502-578`, `eval/lib/run_executor.py:587-619`

Lock acquisition checks for a file and then writes it normally, so it is not
atomic. A lock from another hostname is treated as stale even though the runs
live on a shared filesystem. Successful scheduler submission is not followed by
durable recording of the job ID or a `submitted` state. Pending-run discovery
skips only runs already marked succeeded.

A second invocation can therefore duplicate queued jobs. Submission failures
are classified as retryable forever; the `failed` collection is never populated
by this loop, making its permanent-failure branch ineffective.

**Recommended change:** Use an atomic cross-host lease appropriate for the
shared filesystem, persist state transitions and scheduler IDs transactionally,
make submission idempotent, reconcile state with the scheduler, and use bounded,
classified retries.

### PR-014 — Scheduler queries fail open

**Severity:** High  
**Regions:** `eval/lib/schedulers/pbs.py:63-90`,
`eval/lib/schedulers/slurm.py:74-102`,
`src/exaserve/schedulers/slurm.py:61-84`

Eval scheduler-count exceptions return an empty map, which looks like zero jobs
and permits more submissions. Package Slurm status ignores the `squeue` return
code; empty output is interpreted as completed even when the scheduler command
failed or the job ID never existed.

**Recommended change:** Distinguish command failure, unknown job, queued,
running, and terminal state. Fail closed for submission throttling and report a
specific scheduler-observability error.

### PR-015 — Job-script rendering trusts raw configuration as shell syntax

**Severity:** High with untrusted configuration; Medium in operator-only use  
**Regions:** `src/exaserve/schedulers/base.py:75-92`,
`src/exaserve/schedulers/pbs.py:18-32`,
`src/exaserve/schedulers/slurm.py:33-52`,
`eval/lib/schedulers/base.py:65-79`, `eval/lib/schedulers/pbs.py:42-61`,
`eval/lib/schedulers/slurm.py:49-72`

Job names, accounts, queues, walltimes, log paths, exports, and code roots are
interpolated into scheduler directives or shell without consistent validation
and quoting. Newlines or shell metacharacters in configuration can inject job
directives or commands. `EXASERVE_ENV_SETUP` is intentionally raw shell and must
therefore be treated as privileged code, not data.

**Recommended change:** Validate scheduler fields against strict allowlists,
shell-quote all data fields, reject newlines, and clearly separate trusted
operator-provided script fragments from ordinary configuration.

### PR-016 — Eval matrix expressions use unsafe Python `eval`

**Severity:** High with shared/untrusted specs; Medium in a trusted repo  
**Region:** `eval/lib/matrix.py:26-40`

Derived matrix fields are evaluated with Python `eval()`. Removing builtins does
not create a safe sandbox because Python object traversal can recover powerful
runtime objects. A YAML experiment spec is therefore executable code across an
unclear trust boundary.

**Recommended change:** Parse expressions into an AST and allow only numeric
literals, declared axis names, explicitly supported operators, and approved
functions—or replace expressions with declarative transformations.

### PR-017 — Trace cache identity is incomplete and artifact writes race

**Severity:** High for scientific correctness  
**Regions:** `eval/lib/trace_store.py:29-93`,
`eval/lib/trace_generators.py:42-68`,
`eval/lib/trace_generators.py:182-198`, `eval/lib/utils.py:85-98`

Trace identity omits workload arrival mode even though arrival mode changes the
generated timestamps. It includes input file paths but not their contents,
hashes, or revisions. Editing a prompt/trace file in place can reuse stale
cached output.

Trace generation uses an exists-only cache and writes the final file without a
lock or atomic rename. Parallel materializers can write the same artifact, and
metadata can describe different content from the trace file.

**Recommended change:** Include every content-affecting field and input-content
digest in the identity. Generate under an exclusive artifact lease, fsync where
appropriate, write metadata and trace to temporary files, and atomically publish
a final completion manifest.

### PR-018 — Run-group and metadata creation are not transactional

**Severity:** High for concurrent automation  
**Regions:** `eval/lib/run_planner.py:119-124`,
`eval/lib/run_planner.py:260-282`, `eval/lib/utils.py:85-98`

The next run-group ID is computed by listing existing directories and then
creating the next one without exclusive creation. Concurrent materializers can
select the same `runN`. General JSON/YAML helpers write directly to final files,
so interruption can leave truncated state or readers can observe mid-write
content.

**Recommended change:** Allocate run IDs atomically, use immutable run IDs or
UUID-backed directories, and centralize atomic structured-file writes.

### PR-019 — Partial distributed results can be accepted as successful

**Severity:** High for evaluation correctness  
**Regions:** `eval/lib/replay_engine.py:120-159`,
`eval/lib/run_executor.py:271-358`

Shard gathering logs a warning and drops missing ranks after timeout. Result
validation reports partial request failure but returns success as long as some
successful work exists. This can produce apparently valid throughput and
latency aggregates from only part of the requested client fleet.

The latest-result selector sorts filenames lexicographically, so
`result9.json` sorts after `result10.json` and can be validated as the newest
result.

**Recommended change:** Record expected and collected rank sets in results;
mark incomplete runs non-successful unless an explicit policy permits partials;
use numeric result ordering; and make plots reject incomplete input by default.

### PR-020 — Evaluation validation does not fully establish a runnable plan

**Severity:** Medium-High  
**Regions:** `eval/lib/spec_io.py:118-199,207-286`,
`eval/lib/matrix.py:65-80`, `eval/lib/backends/ray.py:90-155`,
`eval/lib/run_executor.py:271-279`

Validation does not consistently verify scheduler/engine/mode enums, client
process and concurrency values, saturation settings, agreement between
`scheduler.nodes` and `deployment.num_nodes`, or placement capacity. The
existing direct local/paired check compares `client.num_nodes` with
`deployment.num_nodes`; it does not compare scheduler allocation size with the
deployment. Omitted scheduler size defaults to deployment size, and ordinary
matrix variants synchronize the two unless scheduler size is explicitly
targeted or derived. Those conveniences do not reject an explicit mismatch in
a non-matrix spec or an explicitly targeted/derived variant; allocation and
deployment then consume the two values independently. Dispatch-topology
execution validates only the last arm from which it finds a result, allowing
earlier arms to escape semantic validation if their process exited zero.

**Recommended change:** Validate every fully resolved variant after defaults and
matrix derivation, compute resource feasibility, require allocation/deployment
agreement unless a typed reservation policy explicitly permits divergence,
check every dispatch arm, and emit one immutable launch plan before submission.

### PR-021 — Stats collection has split contracts and best-effort failure semantics

**Severity:** Medium-High when stats are requested
**Regions:** `src/exaserve/server.py:779-865`,
`src/exaserve/server.py:979-984`,
`src/exaserve/engines/vllm.py:279-297`,
`eval/lib/run_executor.py:96-103`, `eval/lib/server_stats.py:41-109`,
`doc/KNOWN_ISSUES.md` C1

`CollectingStatLogger.to_dict()` returns `summary`, `sample`, and
`scheduler_snapshots`. `VLLMEngine.collect_stats()` immediately reads a
nonexistent `finished_requests` key and raises `KeyError`. That legacy
`EngineWorker.collect_stats()` path has no repository caller, so it is latent
debt rather than proof that the current collector always fails.

The active path is push-based: replicas publish bounded summaries to a named
actor and `run_executor` invokes `collect_server_stats()` before teardown.
Repository evidence records a successful 12/12-replica `server_stats.json` run,
so the old Known Issue's categorical silent-no-op claim is superseded. However,
the executor catches collection exceptions and can still complete a run without
the requested stats artifact, leaving required-versus-best-effort semantics
undefined.

**Recommended change:** Define and type one stats schema, test producer/consumer
compatibility, and delete or repair the dead pull API. If the resolved plan
requires stats, missing or incomplete output must make the run non-successful;
otherwise publish an explicit telemetry-degraded status rather than silently
treating the artifact as complete.

### PR-022 — `enable_log_requests` is parsed but not propagated

**Severity:** Medium  
**Regions:** `src/exaserve/schemas.py:18-48`,
`src/exaserve/engines/base.py:30-43`,
`src/exaserve/server.py:913-924`,
`src/exaserve/engines/vllm.py:124-147`

The deployment schema includes `enable_log_requests`, but `EngineSpec` does not.
The worker therefore cannot forward the configured value to vLLM. vLLM engine
construction contains a compatibility fallback but does not implement the
operator's setting.

**Recommended change:** Generate engine arguments from a typed, tested mapping
and add a test asserting every public engine setting reaches the backend.

### PR-023 — Auto planning can silently omit configured models

**Severity:** High for multi-model service correctness  
**Regions:** `src/exaserve/server.py:1521-1548`, replica-planner modules/tests

When automatic placement cannot fit a model, the deployment prints that it is
skipping the model and can continue with the remaining set. A production
configuration naming multiple models usually implies that all are required;
quietly serving a subset is surprising and can be mistaken for successful
deployment.

**Recommended change:** Add an explicit required/optional policy per model.
Default to failing the deployment if any declared model cannot be placed.

### PR-024 — Proxy configuration generation is insufficiently validated

**Severity:** Medium-High  
**Regions:** `src/exaserve/proxy/haproxy_proxy.py:374-432`,
`src/exaserve/proxy/nginx_proxy.py:103-145`, related proxy generators

Backend identifiers, model route prefixes, hostnames, balancing methods, and
numeric options are interpolated into HAProxy/NGINX configuration with partial
sanitization. Some invalid values can break configuration or inject directives
if configuration is not fully trusted. Generated configurations are not
consistently checked with native validation commands such as `haproxy -c` or
`nginx -t` before readiness.

Most proxy health checks establish process/TCP liveness, not end-to-end routing
and successful backend inference. The HAProxy tests currently expect old model
route checks while the implementation intentionally checks `/-/healthz`, showing
test/design drift that should be resolved explicitly rather than accidentally.

**Recommended change:** Strictly validate all proxy inputs, run the proxy's
configuration validator before launch, and define separate liveness, readiness,
and end-to-end canary checks.

### PR-025 — Pingora is a benchmark component and proxy capabilities are not normalized

**Severity:** Medium  
**Regions:** `src/exaserve/proxy/pingora_proxy.py`,
`scripts/pingora_lb/`, other proxy backends

The custom Pingora implementation describes itself as a minimal benchmark load
balancer, rejects multi-model configurations, silently falls back for an
unknown load-balancing method, ignores its requested thread count, and uses a
TCP-only readiness check. It lacks the policy surface expected from a
production API gateway. The other proxy backends are not all benchmark-only,
but they vary materially in authentication, retry, streaming, body-limit,
health, and observability semantics without one enforced capability contract.

**Recommended change:** Separate production-supported gateways from benchmark
backends in configuration and documentation. Give each supported gateway a
capability matrix and conformance tests.

### PR-026 — Private dependency monkeypatching is too broad for loose version ranges

**Severity:** High  
**Regions:** `src/exaserve/_sitecustomize.py`,
`src/exaserve/patches/ray_serve_overlay/ray/serve/_private/`,
`src/exaserve/server.py:1782-2053`, `pyproject.toml:16-24`

The runtime uses a 1,629-line import-time monkeypatch plus approximately 10,900
vendored lines from Ray Serve private internals (approximately 12,600 lines
combined). `server.py` additionally imports private Serve constants,
`_run_many`, `serve_start`, deploy utilities, generated protobufs, private
deployment fields, private client/controller methods, and a hardcoded controller
actor name. `ray_start.py` imports `ray._private` services. At the same time,
package dependencies use open lower bounds (`ray[serve]>=2.49`) and unpinned
vLLM/LiteLLM.

A normal dependency upgrade can alter private classes, protobufs, constants, or
constructor contracts without a clear compatibility failure. Import-time global
patching also makes behavior depend on import order.

**Recommended change:** Pin and test an explicit compatibility matrix, refuse to
start on unsupported versions, minimize the overlay, add upstream patch links,
and move compatibility behavior behind narrow adapters rather than global
`sitecustomize` mutation.

### PR-027 — Scheduler architecture and package defaults have drifted

**Severity:** Medium-High  
**Regions:** `src/exaserve/schedulers/`, `eval/lib/schedulers/`,
`src/exaserve/schedulers/__init__.py:1-48`,
`src/exaserve/submit.py:1-22`, `pyproject.toml:11-28`

There are two scheduler abstraction stacks with different APIs and error
semantics. The scheduler package's module documentation says PBS is the default,
while `get_scheduler()` defaults to PSI/J. `submit.py` still describes and labels
the workflow as PBS. PSI/J is not declared as a core or optional package
dependency, so the advertised default is not self-contained outside the site
environment.

The evaluation Ray adapter still rejects Slurm even though Slurm scheduler code
exists. The package Slurm backend labels itself untested.

**Recommended change:** Consolidate scheduler responsibilities, select one
documented default that is installed by the corresponding package extra, and
add backend contract tests plus real site validation.

### PR-028 — Graceful lifecycle and signal handling are incomplete

**Severity:** Medium-High  
**Regions:** `src/exaserve/server.py:2202-2206`,
`src/exaserve/driver.py:647-667`, proxy stop methods

The server loops forever and only handles `KeyboardInterrupt`. Scheduler
termination is commonly delivered as `SIGTERM`, which may bypass application
draining and explicit Ray Serve shutdown. Driver cleanup can abruptly terminate
the server while requests are active, and Ray cleanup has an unbounded wait.

**Recommended change:** Install explicit `SIGTERM`/`SIGINT` handling, transition
readiness to false, stop accepting requests, drain with a deadline, shut down
Serve/Ray/proxies in order, and always impose a final forced-cleanup deadline.

### PR-029 — Detached actors and telemetry lifecycle are insufficiently isolated

**Severity:** Medium  
**Regions:** serving/scaling stats actor creation in
`src/exaserve/scaling_trace.py` and `src/exaserve/server.py`

Some telemetry actors use detached lifetime and fixed names/namespaces. The
replica-init collector can collide with a survivor and is not killed on its
error path; the serving collector uses `get_if_exists=True`, has no
deployment-scoped reset/eviction, and can retain stale entries in a reused Ray
cluster. The serving path already bounds push frequency and sample size and
keeps only the latest payload per replica key, but thousands of replicas still
fan into one cluster-wide actor without backpressure or guaranteed cleanup. The
code calls it a head actor, but supplies no node-affinity or scheduling strategy
that guarantees head-node placement.

**Recommended change:** Namespace telemetry by immutable run/deployment ID,
reject stale generations, intentionally declare placement, define bounded or
acknowledged reporting plus drop/error metrics, clear state at deployment start,
and clean up actors in every shutdown/error path.

### PR-030 — ClientLab currently has a confirmed configuration-path regression

**Severity:** Medium  
**Regions:** `clientlab/analysis/diagnostics.py:55-82`,
`clientlab/tests/test_spec_and_analysis.py:36-60`

`summarize_point()` indexes `run_config["faults"]` even when the otherwise valid
configuration omits `faults`. The unit test fails with `KeyError`.

ClientLab's remote netstats cleanup also uses `os.system()` with an interpolated
node value at `clientlab/collectors/netstats.py:39-40`. Most other SSH paths use
argument vectors and quoting and should be the consistent pattern.

**Recommended change:** Apply defaults through the shared schema before analysis
and replace the remaining shell command with a validated argument-vector call.

### PR-031 — Test coverage and repository quality gates are not release-ready

**Severity:** Blocker as a release-process issue  
**Regions:** `tests/`, `eval/tests/`, `clientlab/tests/`, `pyproject.toml`,
`eval/tests/test_eval_control_plane.py:496-511`, test `conftest.py` files,
`doc/TODO.md:26-31`

The dated lightweight baseline at commit `005891e` collected 36 tests and
finished with **25 passed, 11 failed** in an environment with a real `rg`
executable on `PATH`. An independent recheck reproduced that result twice under
frameworks Python 3.12.12 / pytest 8.3.5, including once with pytest random
reordering disabled. A controlled environment without an executable `rg`
finished with **24 passed, 12 failed**; the additional failure is
`test_eval_runtime_has_no_legacy_import_hacks`, which invokes `rg` as an
undeclared host binary. Both counts are valid environment-specific observations,
not a canonical portable baseline. Failure categories were:

- Six eval materialization/control-plane tests attempted network access to the
  nonexistent Hugging Face ID `test/model`; the parent-process tokenizer
  monkeypatch does not reach forkserver workers, so the suite is not hermetic.
- One eval plotting subprocess failed to import `exaserve` because its test
  environment prepends the repository root rather than `src/`.
- ClientLab failed with the `faults` `KeyError` described in PR-030.
- The serve-submission test expects a PBS artifact while the implementation now
  defaults to PSI/J.
- Two HAProxy tests expect old model-route health checks while production code
  uses `/-/healthz`.
- Conditionally, the legacy-import scan raises `FileNotFoundError` when ripgrep
  is unavailable. It should use a Python filesystem/text scan instead of a host
  command.

The eval subtree also is not independently collectible from a source-layout
checkout unless ExaServe is installed or `src/` is added to `PYTHONPATH`;
full-suite collection can hide this because `tests/conftest.py` mutates the
process import path while `eval/tests/conftest.py` adds only the repository root.
The audit environment's auto-loaded, unpinned `pytest-randomly` plugin changes test order; the
reproducibility gap is the undeclared plugin/seed, not randomized testing itself.

No `.github` CI workflow, dependency lock, pre-commit configuration, linter/type
gate, coverage gate, or security/dependency audit configuration was found. Core
areas such as schema rejection behavior, model-cache integrity, native staging
failures, proxy lifecycle, scheduler idempotency, readiness, and exit semantics
have little or no direct coverage. The former `doc/TODO.md` claim that there were
zero tests was corrected during audit adjudication, illustrating why the final
closure pass must recheck current documentation rather than preserve stale
current-tense claims.

**Recommended change:** Make tests offline and deterministic; replace the
ripgrep subprocess with an in-process Python scan; define one explicit
source-layout/package-install contract for full, targeted, and child-process
tests; pin the canonical pytest plugin set and record randomized seeds; repair
the remaining failures; and add CI, typing, linting, security, packaging,
coverage, and failure-path integration gates before any production label.

### PR-032 — Observability is research-oriented rather than operational

**Severity:** Medium-High  
**Regions:** serving stats/tracing code, proxy logs, `doc/TODO.md:47-65`

Ray's proxy code contains dependency-level metrics and request-context support,
and ExaServe exposes `/health` and `/stats` handlers whose responses are local to
the selected replica behind a load-balanced deployment route; they are not
independently addressable per-replica operational endpoints. The driver sets
`RAY_ENABLE_METRICS_COLLECTION=0` in the Ray-start child environment, and
ExaServe does not expose one stable, aggregate operational contract for request
rate, latency, errors, queueing, replica health, resource saturation,
child-process health, staging integrity, and readiness. EngineWorker does not
read an incoming transport correlation header and creates a new OpenAI-format
completion/engine UUID. That application ID is semantically distinct from a
transport trace ID, but the two are not linked across gateway, Serve, and engine
boundaries.

**Recommended change:** Define structured logs, OpenMetrics/Prometheus metrics,
and one transport correlation ID accepted or generated at ingress, echoed and
linked to the distinct completion ID through proxies, Serve, engines, and logs.
Add alertable aggregate health/status, log rotation/retention, and a deployment
status endpoint independent of experiment tracing.

### PR-033 — Known scale limits remain architectural constraints

**Severity:** High for exascale production; Medium for small deployments  
**Regions:** `doc/KNOWN_ISSUES.md` sections A and B, supporting `findings/`
documents

Current empirical limitations include static-port collisions,
GCS/ServeController contention and proxy-startup cliffs, and centralized
no-coalescing streaming delivery/network concentration. The repository's
512-node evidence places the current single-head Ray/GCS architecture beyond
its demonstrated envelope.

Two older Known Issues must not be treated as current limits. Application source
is now broadcast once and executed from `/tmp/exaserve_src`, with optional
node-local venv staging, so the former application-source Lustre import stampede
has a code-level remedy that now needs scale revalidation. Likewise, newer
256-node evidence shows non-streaming HAProxy at about 27.1k RPS after correcting
the client-topology harness bug; the unqualified “single HAProxy throughput
ceiling” was falsified. The remaining measured constraint is the centralized
streaming path with per-token no-delay behavior, not a general HAProxy or
non-streaming ceiling.

**Recommended change:** State a tested production envelope by node count,
replica count, models, request mode, and proxy topology. For larger deployments,
consider sharded control planes and distributed ingress rather than treating one
Ray GCS/ServeController and one head-node gateway as indefinitely scalable.

### PR-034 — Committed-HEAD snapshot behavior is safe but surprising

**Severity:** Medium usability/correctness risk  
**Regions:** `eval/lib/run_planner.py:162-182`,
`doc/KNOWN_ISSUES.md` C2

Eval materialization snapshots committed `HEAD` and excludes dirty working-tree
changes. It prints a warning, but users can still believe they evaluated the code
currently visible in their editor. The behavior supports reproducibility, yet
the interface makes stale-code execution easy.

**Recommended change:** Require an explicit `--allow-dirty-head-snapshot`
acknowledgement when the tree is dirty, record the diff hash, or offer an
explicit patch-inclusive snapshot mode with clear provenance.

### PR-035 — Several result and metadata files are written non-atomically

**Severity:** Medium  
**Regions:** `eval/lib/replay_engine.py:1337-1338`,
`eval/lib/utils.py:85-98`, `src/exaserve/scaling_trace.py`,
`clientlab/utils.py`, report writers

Many JSON/YAML/report writers open the final path with `"w"`. A crash, walltime
termination, or concurrent reader can observe truncated content. The repository
already documents filesystem-specific trailing-byte behavior, making a shared
atomic-write utility particularly important.

**Recommended change:** Centralize atomic structured writes with temp files,
flush/fsync policy, rename, optional checksums, and completion markers for
multi-file artifacts.

## Code regions that are not production-elegant

The following are maintainability hotspots even where no immediate defect has
yet been observed:

1. **Broad catch-and-print exception handling.** Failure semantics are implicit,
   and important exceptions are downgraded to warnings or successful exit.
2. **Large multi-responsibility orchestration functions.** `server.py` combines
   compatibility patching, Ray startup, resource polling, model resolution,
   placement, deployment, readiness, telemetry, and lifecycle handling.
3. **Import-time global monkeypatching.** Behavior depends on module import order
   and private dependency structures.
4. **Duplicated infrastructure.** Serving and eval maintain separate scheduler,
   configuration, state, and artifact concepts.
5. **Manual dictionary coercion.** It creates silent defaults and type surprises
   instead of explicit validation errors.
6. **Shell construction for structured operations.** Archive, scheduler, and SSH
   operations are harder to quote, validate, and test than argument-vector APIs.
7. **Scattered hardcoded site/resource constants.** GPU counts, ports, timeouts,
   and Aurora behavior leak into otherwise portable layers.
8. **Mutable source configuration and adjacent artifacts.** Inputs, resolved
   runtime state, credentials, logs, and results do not have clean ownership
   boundaries.
9. **Inconsistent health semantics.** TCP liveness, proxy liveness, model
   readiness, and full request success are sometimes treated interchangeably.
10. **Documentation and implementation drift.** Defaults, health-check strategy,
    test counts, scheduler support, and limitations disagree across files.

## Recommended remediation program

### Phase 0 — Make failure truthful

1. Propagate all fatal child and driver errors to scheduler-visible nonzero exit.
2. Implement fail-closed, deadline-bounded cluster readiness.
3. Supervise all essential child processes throughout deployment lifetime.
4. Make model staging transactional and integrity checked.
5. Establish a documented security boundary and disable unsafe admin defaults.

### Phase 1 — Make launches deterministic and idempotent

1. Introduce a shared strict, versioned configuration schema.
2. Make input configs immutable and isolate every run's resolved artifacts.
3. Make scheduler submission and run state transactional/idempotent.
4. Fix vendor-specific Ray resources and complete Slurm/AMD validation.
5. Replace race-prone port discovery with owned leases or bind-to-zero.

### Phase 2 — Protect measurement correctness

1. Correct trace identity and make trace/result publishing atomic.
2. Fail or explicitly mark incomplete distributed gathers.
3. Validate every dispatch arm and resource plan.
4. Repair the stats schema and dead configuration fields.
5. Make working-tree/snapshot provenance impossible to overlook.

### Phase 3 — Establish a sustainable production baseline

1. Pin and test the Ray/vLLM/LiteLLM compatibility matrix.
2. Consolidate scheduler/configuration/artifact abstractions.
3. Add hermetic CI, integration tests, type/lint/security gates, and coverage.
4. Define operational metrics, tracing, dashboards, and alerts.
5. Publish tested support envelopes and separate benchmark-only backends from
   production-supported paths.

## Suggested architecture discussion topics

These decisions should be made before implementing many of the fixes above:

1. **Product boundary:** Is ExaServe an internal HPC allocation-scoped serving
   tool, a long-running multi-tenant service, an evaluation framework, or all
   three as separately packaged products?
2. **Control-plane ownership:** Should Ray Serve remain the global deployment
   control plane at 128-1,000+ nodes, or should ExaServe introduce sharded
   clusters/control planes?
3. **Ingress topology:** Is a single head-node proxy acceptable, or should
   production ingress be distributed across nodes with service discovery?
4. **Failure policy:** Must every declared node/model/replica be healthy, or is
   degraded service a supported state? Who decides and how is it reported?
5. **Artifact model:** What is the immutable run/deployment identity, and which
   files constitute its source config, resolved config, status, logs, and
   results?
6. **Configuration ownership:** Can serving, eval, and ClientLab share one schema
   and plan compiler, with separate execution front ends?
7. **Scheduler layer:** Is PSI/J the intended portability foundation, or should
   native PBS/Slurm adapters remain first-class?
8. **Model supply chain:** How are revisions pinned, verified, approved, staged,
   quarantined, and garbage-collected?
9. **Security model:** Is the network trusted? Where do TLS, authentication,
   authorization, quotas, and secret management live?
10. **Compatibility strategy:** Should private Ray/vLLM modifications be pinned
    overlays, narrow adapters, maintained forks, or upstream contributions?
11. **Production support envelope:** Which vendors, schedulers, engines, proxy
    backends, node counts, and request modes will be release-blocking supported
    combinations?
12. **Evaluation truth policy:** Should any partial request/rank/model result be
    accepted, and how should incomplete data flow into plots and publications?

## Validation record

The audit ran the following lightweight test command after loading the project
framework environment:

```bash
python -m pytest -q
```

Observed results at commit `005891e` under frameworks Python 3.12.12 and pytest
8.3.5:

| Test environment | Collected | Passed | Failed |
|---|---:|---:|---:|
| Real `rg` executable on `PATH` | 36 | 25 | 11 |
| No `rg` executable on `PATH` | 36 | 24 | 12 |

The same 25/11 result was reproduced twice during cross-verification in an
environment containing a real ripgrep executable. Claude's 24/12 result was
also reproduced under a controlled `PATH` without such an executable. The
additional failure is
`eval/tests/test_eval_control_plane.py::test_eval_runtime_has_no_legacy_import_hacks`:
the test calls `subprocess.run(["rg", ...])` and raises `FileNotFoundError` when
only a shell-level shim, rather than an executable, is available. Disabling
`pytest-randomly` does not change that discriminator. Consequently neither count
is a universal baseline; the repository has an undeclared ripgrep dependency
and must track failures by node ID, root cause, environment receipt, and seed.

No deployment, evaluation, distributed job, GPU workload, or native MPI staging
operation was executed as part of this audit. Findings concerning large-scale
runtime behavior are based on static code paths and the repository's own
documented empirical failure log; they should be validated with targeted
interactive-cluster tests only after the corresponding unit-level fixes are in
place.
