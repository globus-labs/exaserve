# AURORA RAYSERVER

## Scaling LLM Inference on HPC — Setup Reference Card

Ray Serve + vLLM inference at HPC scale • Code: <https://github.com/wenyiwang-us/aurora_rayserver> • Reference system: ALCF Aurora (Intel PVC XPU, 12 tiles/node)

`aurora_rayserver` turns a PBS allocation of N nodes into a single OpenAI-compatible inference endpoint: it launches a Ray cluster over the allocation, stages model weights to node-local storage with an MPI broadcast, deploys one vLLM (or SGLang) replica per GPU tile as Ray Serve applications, and optionally fronts them with a head-node proxy (HAProxy, LiteLLM, and others). A companion declarative benchmarking harness (`eval/`) drives replay workloads against the deployment and has validated linear weak scaling to **256 nodes / 3,072 XPU tiles (18.1k req/s streaming)** and multi-node pipeline-parallel serving of **Llama-3.1-405B (TP8 × PP2)**.

The launch pattern (PBS + MPI weight broadcast + one Ray Serve app per accelerator + pluggable ingress) is portable to other schedulers and accelerators; the Aurora-specific parts are isolated in the `frameworks` environment module and a small patch set (`src/aurora_rayserver/patches/`).

---

## 1 · Install

The package targets the Python that ships in Aurora's `frameworks` module (Python 3.12 with vLLM, Ray, MPI, and oneAPI already provided — none of those come from pip).

```bash
module load frameworks
git clone https://github.com/wenyiwang-us/aurora_rayserver
cd aurora_rayserver
python3 -m pip install --user .
```

Console scripts land in a frameworks-versioned user base, **not** `~/.local/bin`. Add it to `PATH` with the dynamic form so it survives frameworks version bumps:

```bash
SCRIPTS_DIR="$(python3 -c 'import sysconfig; print(sysconfig.get_path("scripts", "posix_user"))')"
export PATH="$SCRIPTS_DIR:$PATH"
```

Verify the install:

```bash
which aurora-serve-submit   # ~/.local/aurora/frameworks/<ver>/bin/aurora-serve-submit
python3 -m pytest tests/    # packaging + submit + driver unit tests
```

One-time extras — only needed for the feature that uses them:

| Component | Needed for | Setup |
|---|---|---|
| HAProxy | `proxy_config.type: haproxy` | `bash scripts/build_haproxy.sh` (installs `~/.local/haproxy-<ver>`, symlinks `~/bin/haproxy`) |
| LiteLLM | `proxy_config.type: litellm` | Own venv (deps conflict with Ray/vLLM): `python3 -m venv ~/litellm_venv && ~/litellm_venv/bin/pip install 'litellm[proxy]'` |
| Go load generator | benchmarking with `eval/` | `module load go && bash eval/go_client/build.sh` → `eval/go_client/bin/go_dispatch` |

`proxy_config.type: direct` (clients hit each node's Ray Serve HTTP on port 8000) needs none of these.

Full install notes, including isolated `--target` installs: [README.md](../README.md#install).

## 2 · Configure a Deployment

A deployment is one YAML file with three sections: `ray_cluster_config`, `model_deployment_config`, `proxy_config`. Start from a template in [examples/](../examples/):

| Template | Use |
|---|---|
| [config.direct.yaml](../examples/config.direct.yaml) | No proxy; each node serves port 8000. Best raw-scaling baseline. |
| [config.haproxy.yaml](../examples/config.haproxy.yaml) | HAProxy on the head node load-balances all nodes on port 4001. Best single-endpoint baseline. |
| [config.litellm.yaml](../examples/config.litellm.yaml) | LiteLLM head-node gateway: auth, rate limits, usage tracking. |
| [config.reference.yaml](../examples/config.reference.yaml) | Every schema field with defaults and commentary. Copy fields, don't run as-is. |

Minimal example (the only fields that typically vary per user are `model_storage_path` — your Lustre model dir — and `local_stage_path` — the node-local staging dir):

```yaml
ray_cluster_config:
  head_ip: ""              # filled in at runtime
  port: 6379
  node_cpus: 64

model_deployment_config:
  num_nodes: 2
  model_storage_path: /lus/flare/projects/<PROJECT>/<user>/models
  local_stage_path: /tmp/hf_home
  num_gpus_per_node: 12
  replica_max_ongoing_requests: 128
  model_configs:
    - model_id: meta-llama/Meta-Llama-3-8B-Instruct
      tensor_parallel_size: 1
      pipeline_parallel_size: 1
      max_model_len: 4096
      size: 8                # billions of params, drives replica planning
      gpu_memory_utilization: 0.90
      enforce_eager: true
      max_num_seqs: 64

proxy_config:
  type: haproxy              # haproxy | litellm | none
  port: 4001                 # client-facing port
  backend_port: 8000         # Ray Serve HTTP port on each node
```

## 3 · Launch and Verify

**Batch (recommended)** — from a login node, no allocation needed:

```bash
aurora-serve-submit my_config.yaml \
    --project-account YOUR_PROJECT --queue debug-scaling --walltime 01:00:00 --wait
# 8470123.aurora-pbs-0001...       <- PBS job id
# http://x4310c1s0b0n0:4001        <- service URL (printed by --wait)
```

Defaults without flags: project `AuroraGPT`, queue `debug-scaling`, walltime `01:00:00`, filesystems `home:flare` (env vars `AURORA_PROJECT_ACCOUNT`, `AURORA_DEFAULT_QUEUE`, `AURORA_DEFAULT_WALLTIME`, `AURORA_DEFAULT_FILESYSTEMS`). `--dry-run` writes the generated PBS script without submitting — the starting point for custom PBS pipelines.

**Interactive** — inside `qsub -I`:

```bash
qsub -I -l select=2,walltime=01:00:00 -A YOUR_PROJECT -q debug-scaling -l filesystems=home:flare
module load frameworks
aurora-launch-cluster my_config.yaml
# wait for:  [Driver] ALL SERVICES READY
```

**Verify** — the endpoint speaks the OpenAI API:

```bash
curl -sS -X POST "http://<head_node>:4001/v1/chat/completions" \
     -H 'Content-Type: application/json' \
     -d '{"model":"meta-llama/Meta-Llama-3-8B-Instruct",
          "messages":[{"role":"user","content":"hello"}],"max_tokens":8}'
```

Tear down with `qdel <jobid>` (the job otherwise runs to walltime). Resolve a URL later with `aurora-serve-url <jobid>`.

## 4 · Scale It — Model Tiers, Dispatch Paths, Knobs

**Model → parallelism mapping** (validated configurations):

| Model | TP × PP | Footprint | Notes |
|---|---|---|---|
| Llama-3-8B | 1 × 1 | 1 tile (12 replicas/node) | primary weak-scaling workhorse, 1–256 nodes |
| gpt-oss-120B | 1 × 1 | 1 node | MXFP4 |
| Llama-3.1-405B | 8 × 2 | 2 nodes/replica | shard-aware PP: each pipeline stage stages only its own weight shard node-locally (380 GiB fits tmpfs) |

**Dispatch path** — the single most important scaling decision:

- `direct` (no proxy): clients spread requests across every node's port 8000 themselves. **Scales linearly — use this for any large-scale deployment or benchmark.**
- Head-node proxy (`haproxy` etc.): one convenient endpoint, but a single head-node process caps streaming throughput from ~128 nodes (network saturation on the head, not CPU). Fine at small/medium scale or for non-streaming.

**Launch-time environment knobs** (read by `launch_cluster.sh` / the server):

| Env var | Effect |
|---|---|
| `AURORA_ENGINE=sglang` | Use SGLang instead of vLLM as the inference engine |
| `AURORA_PP_SHARD_AWARE=1` | Shard-aware multi-node PP: per-stage weight staging + node-pinned per-replica deploys (required for 405B-class models) |
| `AURORA_PP_UMBRELLA=1` | Add a root-route umbrella ingress over the per-replica PP routes so any proxy/client sees one `/v1` endpoint |
| `AURORA_NULL_COMPUTE=1` | Skip the engine entirely and simulate latency — control-plane/routing stress tests |
| `AURORA_CLEAN_STAGE=1` | Wipe node-local staged weights first, so bring-up is timed cold |

**Known scale cliffs** (details in [KNOWN_ISSUES.md](KNOWN_ISSUES.md)): Ray/vLLM need small site patches at 256+ nodes (HTTP proxy timeouts, XPU PP layer filter) — applied automatically by the launcher via `aurora_rayserver.apply_all()`; thread-pool exhaustion on many-core nodes requires the clamps the launcher already sets (`RAYON_NUM_THREADS=1`, `TOKENIZERS_PARALLELISM=false`).

## 5 · Benchmark — the Eval Harness

The `eval/` harness turns one declarative spec (workload × model × node-count matrix) into materialized traces, per-cell PBS jobs, and a multi-node MPI replay client that drives a compiled Go load generator against the deployment.

Setup (once): `cp eval/site_config_local.example.py eval/site_config_local.py` and edit the paths; build the Go client (§1).

```bash
python -m eval.cli spec validate eval/specs/<spec>.yaml   # parse + validate
python -m eval.cli run materialize <spec>                 # traces + run bundles + PBS jobs
python -m eval.cli run submit-all <spec>                  # qsub all cells (respects queue limits)
```

Spec anatomy (condensed — full schema in [eval/DESIGN.md](../eval/DESIGN.md)):

```yaml
name: my_weak_scaling
matrix:                      # sweep axes; each cell = one PBS job
  axes:
    - name: num_nodes
      values: [1, 4, 16, 64]
      targets: [deployment.num_nodes, client.num_nodes, scheduler.nodes]
trace: {kind: weak_scaling}
workload: {duration: 60.0, input_len: 64, output_len: 64, rate_per_node: 110.0}
deployment:
  models: [{model_id: meta-llama/Meta-Llama-3-8B-Instruct, tensor_parallel_size: 1, ...}]
client:  {num_runs: 4, dest: direct, stream: true, num_go_procs: 8, num_go_workers: 4}
backend: {default: ray, args: {ray: {proxy: {type: none}}}}
scheduler: {type: pbs}
```

Protocol: run 0 of every cell is warm-up and is dropped; remaining runs are data. Results land under `<experiments_root>/runs/<spec>/<cell>/runN/results/result*.json` with per-request TTFT/TBT/E2E and aggregate throughput; score with `python -m eval.plot.goodput -e <spec> --preset paper`.

## 6 · Examples of It Running

Worked, copy-paste examples with expected output — quickstart service, single-node benchmark, 256-node weak-scaling sweep, and 405B pipeline-parallel — are in **[doc/examples.md](examples.md)**. Headline validated results:

| Experiment | Spec | Result |
|---|---|---|
| 8B weak scaling, direct dispatch, streaming | `eval/specs/sc26workshop/full/proxycmp_direct*` | linear 1→256 nodes; **18.1k req/s at 256 nodes** |
| Same, via one head-node HAProxy | `.../full/proxycmp_haproxy*` | plateaus ~4.7k req/s from 128 nodes |
| 405B TP8×PP2 demo | `.../smokes/pp405b_verify_2node` | 30/30 streaming requests on 2 nodes |
| 405B TP8×PP2 weak scaling | `.../full/pp405b_pp2_scale_direct` | 2→128 replicas (4→256 nodes), zero errors |
| Workload robustness (chat/code/summary/long-context, Poisson, BurstGPT) | `.../full/oat_8b_*`, `oat_120b` | per-workload SLO attainment at N=1 and N=64 |

The full experiment suite and status matrix: [eval/specs/sc26workshop/README.md](../eval/specs/sc26workshop/README.md).

## 7 · Everyday Commands

| Command | What it does |
|---|---|
| `aurora-serve-submit <cfg> --wait` | Submit a serving job from a login node; print job id + service URL |
| `aurora-serve-submit <cfg> --dry-run` | Write the generated PBS script without submitting |
| `aurora-launch-cluster <cfg>` | Foreground launch inside an interactive allocation |
| `aurora-serve-url <jobid>` | Resolve a running job's service URL |
| `aurora-model-bcast --config <cfg> --num-nodes N` | Pre-stage model weights to node-local storage without starting Ray |
| `qdel <jobid>` | Tear down a deployment |
| `python -m eval.cli spec list` | List available benchmark specs |
| `python -m eval.cli run materialize / submit-all <spec>` | Generate and submit a benchmark sweep |
| `python -m eval.plot.goodput -e <spec> --preset paper` | Score a finished sweep against latency SLOs |

## Learn More

- [README.md](../README.md) — full install, configuration, and console-script reference
- [doc/examples.md](examples.md) — worked running examples with expected output
- [eval/DESIGN.md](../eval/DESIGN.md) — benchmark harness architecture and complete spec schema
- [KNOWN_ISSUES.md](KNOWN_ISSUES.md) — scale cliffs and their fixes
- [findings/](../findings/) — profiling and scaling analyses (launch stages, GCS contention, proxy limits)

*Contact: Wenyi Wang (UChicago/ANL) • Globus Labs — Kyle Chard, Ian Foster*
