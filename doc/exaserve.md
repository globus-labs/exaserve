# ExaServe reference card — hardened architecture

This page is the machine-readable companion to `ExaServe_Reference_Card.docx`.
The authoritative architecture and acceptance contract is
[`PRODUCTION_HARDENING_EXECUTION_PLAN.md`](PRODUCTION_HARDENING_EXECUTION_PLAN.md);
the live release verdict is [`hardening/STATUS.md`](hardening/STATUS.md).

## Current support boundary

The only production candidate is:

- ALCF Aurora, native PBS;
- Intel PVC/XPU, 12 accelerator tiles per node;
- vLLM under the exact compatibility profile shipped with the current Aurora
  frameworks environment;
- HAProxy on the allocation head;
- non-streaming OpenAI-compatible requests; and
- trusted allocation-internal network exposure.

The evidence-backed maximum is two nodes. The proposed 64-node ladder remains a
candidate pending explicit product-owner scope approval and final-architecture
4/16/64-node evidence. Historical 128/256-node results are useful regression
context but do not qualify this architecture.

Slurm/PSI-J, CUDA/ROCm, SGLang, streaming, public exposure, direct Serve
exposure, and LiteLLM/NGINX/Envoy/Pingora are unsupported or validation-only.
The presence of an implementation interface is not a support claim.

## Run

```bash
module load frameworks
python3 -m pip install --user .

cp examples/config.haproxy.yaml my_config.yaml
# Edit model_storage_path.
exaserve-serve-submit my_config.yaml --project-account YOUR_PROJECT --wait
```

The submit command prints the exact job ID. It prints an endpoint only after the
matching deployment generation publishes canonical `READY`; PBS `RUNNING`, an
open proxy socket, or stdout text is insufficient.

```bash
exaserve-serve-url JOB_ID --wait
exaserve-status show --run-dir /path/to/run --json
qdel JOB_ID
```

## Canonical deployment input

```yaml
num_nodes: 2
num_gpus_per_node: 12
node_cpus: 64
ray_port: 6379
model_storage_path: /lus/flare/projects/PROJECT/USER/models
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

There is no nested `model_deployment_config`, `proxy_config`, or mutable Ray
runtime schema. `maxconn: 8000` fits Aurora's observed descriptor ceiling;
ExaServe calculates and checks the exact required `RLIMIT_NOFILE` before
HAProxy launch.

## What the control plane proves

One Python composition root owns the complete generation. It persists immutable
plan/site/allocation identities, runs finite staging, supervises all process
groups, hosts the authenticated rank-control sessions, owns the deployment
child and HAProxy, evaluates readiness, and performs bounded cleanup.

`READY` requires the exact planned node sessions, Ray membership/resources,
Serve applications and replicas, engine/process compatibility receipts, route
health, the owned gateway, and a real canary through the compiled advertised
endpoint. Evidence is generation-bound and freshness-limited. Loss after READY
revokes readiness and triggers the declared failure/teardown policy.

Subprocesses exist only at genuine OS boundaries and use argv vectors,
deadlines, process-group ownership, and structured handshakes. No shell wrapper
or output parser decides lifecycle or readiness. Compatibility uses targeted,
version-pinned activation with attestation; the old full-file overlay/replacement
path was removed.

## Eval and ClientLab

```bash
python3 -m eval.cli spec validate eval/specs/smoke_haproxy_1node.yaml
python3 -m eval.cli run materialize eval/specs/smoke_haproxy_1node.yaml
python3 -m eval.cli run submit-all smoke_haproxy_1node

python3 -m clientlab plan client_microbench
python3 -m clientlab smoke client_microbench
```

Eval embeds the exact DeploymentPlan inside one immutable RunPlan and requires a
complete result manifest. ClientLab synthetic results are diagnostic-only; a
real deployment study must bind its RunPlan, trace, generation, and status
directory. Neither subsystem owns a private scheduler, serving lifecycle, or
readiness protocol.

## Historical measurements

The SC26 research campaign includes measurements through 256 Aurora nodes,
including HAProxy/non-streaming and pipeline-parallel workloads. Those artifacts
were produced before the final hardened control plane and remain in `findings/`
for scientific context. They may be used as regression baselines, not as current
release evidence. A support statement requires the final code, exact packaged
artifact, approved envelope, canonical status/receipt chain, and required tier
to pass together.

## Regenerating the Word reference card

`doc/ExaServe_Reference_Card.docx` is generated output. The repository does not
redistribute the Academy template or the paper figures, so a clean clone needs
these ignored inputs restored first:

```text
tmp/ref_card/Academy_Framework_Reference_Card.docx
doc/figures/fig1_proxy_scaling.png
doc/figures/fig7_pp405b.png
```

The two figures come from the paper-analysis output; the template must be
obtained from its licensed project source. From any working directory, run:

```bash
python3 /path/to/exaserve/tools/make_refcard_docx.py
```

The generator resolves all paths relative to its repository, verifies the PNG
inputs, and atomically replaces the generated DOCX. Edit the generator and
this Markdown companion rather than editing generated Word XML by hand.
