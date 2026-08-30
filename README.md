# ExaServe

ExaServe is an allocation-scoped Ray Serve control plane for OpenAI-compatible
LLM inference on ALCF Aurora. The current production candidate is deliberately
narrow: Aurora PBS, Intel PVC/XPU, vLLM, HAProxy, non-streaming requests, and a
trusted allocation-internal network. Slurm, CUDA/ROCm, SGLang, streaming, direct
Serve exposure, and alternate gateways are rejected or validation-only unless a
separately qualified `SiteProfile` says otherwise.

The active successor source is `release/v0.4.0`. Final43 remains the last
packaged one/two-node hardware-qualified artifact until that branch passes a
clean wheel gate and the owner-approved scale ladder; preview results are not
silently promoted into release evidence.

Qualification is immutable-candidate and dimension specific. The proposed
64-node target is not a support claim: it needs product-owner scope approval
and the predeclared exact-candidate ladder. See
[`doc/hardening/STATUS.md`](doc/hardening/STATUS.md) for the current artifact,
evidence-backed maximum, and unresolved gates.

## Architecture

One Python composition root owns a deployment generation from allocation bind
through bounded teardown:

```text
PBS job
  -> exaserve.launcher (one composition root)
       -> immutable DeploymentPlan + SiteProfile + AllocationBinding
       -> finite model/source staging
       -> one supervised rank launcher
            -> one NodeSupervisor per planned node
                 -> Ray daemon and rank-local receipt ingress
       -> rank-zero deployment child (Ray Serve applications)
       -> one head-owned HAProxy
       -> generation-bound readiness predicate and status publication
```

Subprocesses remain only at real OS/process boundaries: PBS commands, MPI,
Ray daemons, the isolated Serve deployment child, native staging, and HAProxy.
They use argument vectors, finite deadlines, process-group ownership, and
structured handshakes/status. No shell script or stdout marker owns lifecycle or
readiness.

[`doc/design/call_graph.md`](doc/design/call_graph.md) walks the same path
function by function, worked against the canonical two-node HAProxy example.

## Install on Aurora

```bash
module load frameworks
python3 -m pip install --user .

SCRIPTS_DIR="$(python3 -c 'import sysconfig; print(sysconfig.get_path("scripts", "posix_user"))')"
export PATH="$SCRIPTS_DIR:$PATH"
```

Ray, vLLM, MPI, and the XPU stack come from Aurora's `frameworks` module. The
wheel supplies the Python control plane plus the source for its small native MPI
broadcast helper. HAProxy must be available on `PATH`; the repository includes
`scripts/build_haproxy.sh` as an operator setup helper.

## Candidate inspection and eventual submission

> A candidate is not a production release until the status document names its
> exact wheel and every required approval and qualification gate. Before that
> point, normal production submission fails closed; only authorized,
> predeclared work may set `validation_mode: true`.

The current default SiteProfile intentionally has no approved, evidence-backed
production envelope, so submitting the canonical HAProxy example currently
fails with `production execution is not qualified`. That is a release gate, not
a setup error. After `doc/hardening/STATUS.md` records the required measurements
and approval, copy the canonical HAProxy example and change the shared model
path:

```bash
cp examples/config.haproxy.yaml my_config.yaml
$EDITOR my_config.yaml

exaserve-serve-submit my_config.yaml \
  --project-account YOUR_PROJECT \
  --wait
```

The command prints the exact scheduler job ID and, only after the matching
generation publishes canonical `READY`, its compiled advertised endpoint. It
does not infer readiness from PBS `RUNNING`, a listening proxy, a log line, or a
newest file.

For plans admitted by the current SiteProfile, the topology policy defaults to
Aurora's `capacity` queue and `01:00:00` through sixteen nodes and to
`debug-scaling` from 17 through the 64-node candidate ceiling. Normal
production plans above the evidence-backed two-node maximum are rejected; the
default profile rejects every plan above 64 before scheduler rendering. Queue
and walltime can be overridden by CLI arguments or
`EXASERVE_DEFAULT_QUEUE` / `EXASERVE_DEFAULT_WALLTIME` only within an approved
or explicitly authorized validation envelope.

The submission protocol is idempotent. A durable `SUBMITTING` intent prevents a
blind retry when scheduler acceptance is ambiguous; use `--new-generation`
only when deliberately creating a new deployment generation.

Useful commands:

```bash
# For an authorized validation or qualified production config, render without submitting.
exaserve-serve-submit my_config.yaml --dry-run --log-dir ./submit-preview

# Resolve an already submitted job; add --wait to poll canonical status.
exaserve-serve-url JOB_ID --wait

# Inspect why a known run directory is or is not ready.
exaserve-status show --run-dir /path/to/run --json
```

End the allocation with the scheduler (`qdel JOB_ID`). The composition root
handles drain, child/process-group cleanup, leases/listeners, and terminal
status publication.

## Canonical deployment YAML

There is one flat input language. Nested `model_deployment_config`,
`proxy_config`, and mutable Ray-runtime manifests were removed rather than kept
as parallel compatibility schemas.

```yaml
num_nodes: 2
num_gpus_per_node: 12
node_cpus: 64
ray_port: 6379
model_storage_path: /lus/flare/projects/YOUR_PROJECT/YOUR_USER/models
local_stage_path: /tmp/hf_home
deployment_name: exaserve_serve
replica_max_ongoing_requests: 32
engine: vllm
vendor: xpu

models:
  - model_id: meta-llama/Meta-Llama-3-8B-Instruct
    tensor_parallel_size: 1
    pipeline_parallel_size: 1
    max_model_len: 4096
    size: 8
    gpu_memory_utilization: 0.90
    enforce_eager: true
    max_num_seqs: 64
    num_cpus_per_replica: 4

exposure:
  mode: PROXIED_INTERNAL
  network_boundary: trusted_allocation
  request_body_limit_bytes: 16777216

gateway:
  kind: haproxy
  port: 4001
  backend_port: 8000
  executable_ref: PATH:haproxy
  worker_count: 1
  options:
    balance: leastconn
    maxconn: 8000
```

`maxconn: 8000` is intentional. ExaServe computes HAProxy's descriptor demand
as `2 * maxconn + backend_count + 256`, verifies the hard `RLIMIT_NOFILE`, and
raises the soft limit when allowed. Larger copied values such as 50,000 or
100,000 cannot run under Aurora's observed 16,384 hard limit and fail before
launch.

See [`examples/config.reference.yaml`](examples/config.reference.yaml) for the
complete schema. [`examples/config.direct.yaml`](examples/config.direct.yaml)
is explicitly `DIRECT_VALIDATION`, and `config.litellm.yaml` is a
validation-only alternate-gateway example. NGINX, Envoy, and Pingora adapters
exist for future qualification/benchmark work, but no shipped example or
production exposure claim currently advertises them.

## Readiness and compatibility

`READY` is a continuously evaluated projection over the exact plan and current
generation. It requires:

- every planned node session and supervised component;
- exact Ray membership and resource totals;
- every planned Serve application/replica/route;
- engine-level readiness and exact compatibility receipts;
- the owned gateway process/listener; and
- a real canary through the compiled advertised endpoint.

Loss after `READY` revokes readiness and drives the declared terminal policy.
Compatibility is activated before Ray/vLLM imports and attested in each actual
process role; there is no full-file Ray overlay or source-tree replacement.

## Eval and ClientLab

The eval control plane compiles an immutable `RunPlan` containing the exact
`DeploymentPlan`; its runtime manifest references the plan by path and SHA-256
instead of copying serving fields.

```bash
python3 -m eval.cli spec validate eval/specs/smoke_haproxy_1node.yaml
python3 -m eval.cli run materialize eval/specs/smoke_haproxy_1node.yaml
python3 -m eval.cli run submit-all smoke_haproxy_1node
```

ClientLab uses `python3 -m clientlab`. Synthetic studies are explicitly
diagnostic-only. A real ExaServe study must bind one RunPlan, trace, generation,
and deployment-status directory; it cannot accept a raw endpoint or maintain a
private readiness/scheduler control plane. The retired ad-hoc runners are
explained in [`clientlab/scripts/README.md`](clientlab/scripts/README.md).

## Testing and release gates

Lightweight checks on the login node require Aurora's project toolchain:

```bash
module load frameworks
module load go
python3 -m pytest -q
ruff check .
ruff format --check .
(cd eval/go_client && go test ./...)
```

The release gate also builds an sdist and wheel, installs the wheel into a clean
Python environment outside the source tree, runs deterministic and randomized
hermetic suites, validates `doc/hardening/FINDINGS.yaml`, strictly compiles the
native MPI helper, and performs the required tests inside validated Aurora
compute sessions. Hardware/scale evidence that has not run on the final code is
reported as pending, never inferred from unit tests or historical artifacts.

The authoritative architecture and implementation contract is
[`doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`](doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md).
Historical audits, `KNOWN_ISSUES`, and TODO material are context only and cannot
override it.
