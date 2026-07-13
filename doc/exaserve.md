---
name: exaserve
description: Framework for scaling OpenAI-compatible LLM inference across HPC compute nodes — Ray Serve + vLLM deployments over PBS allocations with MPI weight staging, HAProxy/LiteLLM front ends, multi-node pipeline parallelism, and a declarative scaling-benchmark harness
package: aurora-rayserver
install: module load frameworks && pip install --user .
language: python
python_requires: ">=3.10"
docs: https://github.com/wenyiwang-us/ExaServe/blob/main/README.md
source: https://github.com/wenyiwang-us/ExaServe
examples: https://github.com/wenyiwang-us/ExaServe/tree/main/examples
benchmarks: https://github.com/wenyiwang-us/ExaServe/tree/main/eval/specs/refcard
reference_system: ALCF Aurora (PBS, Intel PVC XPU, 12 tiles/node)
---

# ExaServe Reference Card

ExaServe (distributed as the `aurora-rayserver` package) turns a PBS allocation of N HPC nodes into a single OpenAI-compatible LLM inference endpoint: it launches a Ray cluster over the allocation, stages model weights to node-local storage with an MPI broadcast, deploys vLLM (or SGLang) replicas as Ray Serve applications — one per accelerator tile for single-tile models, or spanning tiles and nodes via tensor/pipeline parallelism for larger ones — and fronts them with a head-node proxy such as HAProxy. Validated on ALCF Aurora at up to 256 nodes / 3,072 XPU tiles: 27.1k non-streaming requests/s with Llama-3-8B (one replica per tile) through a single HAProxy front end — 96% weak-scaling efficiency (27.1k of 28.2k offered, 0% errors) — and multi-node pipeline-parallel serving of Llama-3.1-405B (TP8 × PP2).

## Install

Installs into the Python provided by Aurora's `frameworks` module, which already ships Ray, vLLM, MPI, and the oneAPI toolchain; pip adds only this package.

```bash
module load frameworks
git clone https://github.com/wenyiwang-us/ExaServe && cd ExaServe
python3 -m pip install --user .

# console scripts land in a frameworks-versioned bin dir; add it to PATH:
export PATH="$(python3 -c 'import sysconfig; print(sysconfig.get_path("scripts", "posix_user"))'):$PATH"
which aurora-serve-submit   # verify
```

One-time extras, only for the feature that uses them:

| Component | Needed for | Setup |
|---|---|---|
| HAProxy | `proxy_config.type: haproxy` | `bash scripts/build_haproxy.sh` |
| LiteLLM | `proxy_config.type: litellm` | own venv: `python3 -m venv ~/litellm_venv && ~/litellm_venv/bin/pip install 'litellm[proxy]'` |
| Go load generator | benchmarking (`eval/`) | `module load go && bash eval/go_client/build.sh` |

## Core commands

| Command | Purpose |
|---|---|
| `aurora-serve-submit <cfg> --wait` | Submit a serving job from a login node; prints PBS job id + service URL |
| `aurora-serve-submit <cfg> --dry-run` | Write the generated PBS script without submitting (custom PBS pipelines) |
| `aurora-launch-cluster <cfg>` | Foreground launch inside an interactive allocation; ready when it prints `[Driver] ALL SERVICES READY` |
| `aurora-serve-url <jobid>` | Resolve a running job's service URL |
| `aurora-model-bcast --config <cfg> --num-nodes N` | Pre-stage weights node-locally without starting Ray |
| `qdel <jobid>` | Tear down a deployment |
| `python -m eval.cli run materialize <spec>` | Benchmark spec → traces + per-cell PBS jobs |
| `python -m eval.cli run submit-all <spec>` | Submit all cells of a benchmark sweep |
| `python -m eval.plot.goodput -e <spec> --preset paper` | Score a finished sweep against latency SLOs |

## Minimal runnable example

One YAML file describes a deployment (`ray_cluster_config`, `model_deployment_config`, `proxy_config`). Only `model_storage_path` (your model dir on the shared file system, e.g. Lustre) and `local_stage_path` (node-local staging dir) typically vary per user.

```yaml
# my_config.yaml — 2 nodes, 24 Llama-3-8B replicas, HAProxy endpoint
ray_cluster_config:
  head_ip: ""              # filled in at runtime
  port: 6379
  node_cpus: 64
model_deployment_config:
  num_nodes: 2
  model_storage_path: /lus/flare/projects/<PROJECT>/<user>/models
  local_stage_path: /tmp/hf_home
  num_gpus_per_node: 12
  model_configs:
    - model_id: meta-llama/Meta-Llama-3-8B-Instruct
      tensor_parallel_size: 1
      pipeline_parallel_size: 1
      max_model_len: 4096
      size: 8                # billions of params; drives replica planning
      gpu_memory_utilization: 0.90
      enforce_eager: true
      max_num_seqs: 64
proxy_config:
  type: haproxy              # haproxy | litellm | none
  port: 4001                 # client-facing port
  backend_port: 8000         # Ray Serve HTTP port on each node
```

```bash
aurora-serve-submit my_config.yaml --project-account YOUR_PROJECT --wait
# 8470123.aurora-pbs-0001...
# http://x4310c1s0b0n0:4001

curl -sS -X POST "http://x4310c1s0b0n0:4001/v1/chat/completions" \
     -H 'Content-Type: application/json' \
     -d '{"model":"meta-llama/Meta-Llama-3-8B-Instruct",
          "messages":[{"role":"user","content":"hello"}],"max_tokens":8}'
# {"id":"chatcmpl-...","choices":[{"message":{"content":"Hi!"...}}],...}

qdel 8470123
```

Ready-to-customize templates: [examples/](https://github.com/wenyiwang-us/ExaServe/tree/main/examples) — `config.haproxy.yaml`, `config.litellm.yaml`, `config.reference.yaml` (every field, annotated).

## Deployment recipes

### Front-end proxy at scale

All client traffic enters through the head-node proxy (HAProxy recommended). Measured behavior at scale (Llama-3-8B, 64-in/64-out, 110 QPS/node offered, up to 256 nodes / 3,072 single-tile replicas):

- **Non-streaming completions** scale nearly linearly through a single HAProxy: 27.1k requests/s at 256 nodes, 0.0% errors; p99 end-to-end latency stays near ~2 s at every scale.
- **Streaming (SSE)** is harder on a centralized front end: the per-token delivery path saturates the head node's network. HAProxy still completes essentially every request but slowly — throughput plateaus at ~4.7k requests/s from 128 nodes, with p50 5.7 s / p99 17 s end-to-end at 256 nodes, so SLO attainment is effectively zero. Other centralized proxies fare worse: Envoy degrades to 3.0k requests/s at 44% success (256 nodes), the Ray Serve single-ProxyActor front end falls from 100% success at 1 node to 15% at 16 and 3% at 256, and LiteLLM's uvicorn front end falls to 6.5% success by 64 nodes. Budget streaming capacity per proxy, and prefer non-streaming at extreme scale.

For benchmarking only, the harness's client can bypass the proxy and dispatch to per-node endpoints (`client.dest: direct` in a spec) to isolate front-end overhead from backend capacity — the backends themselves stream at 19.4k requests/s with 100% success at 256 nodes when the proxy is bypassed. This mode exposes one endpoint per node and is not a deployment path.

### Multi-node pipeline parallelism (405B-class models)

Llama-3.1-405B spans two nodes per replica: TP=8 within a node × PP=2 across a node pair. Shard-aware staging (`AURORA_PP_SHARD_AWARE=1`) gives each pipeline stage only its own weight shard (~380 GiB, fits node-local tmpfs) and pins each replica's deployment to its nodes. The benchmark harness derives this env var automatically from the spec (PP>1 with multiple replicas). Measured at a fixed per-replica offered rate (0.4 req/s per replica), streaming: aggregate successful throughput grows from 0.7 query/s at 2 replicas to 31.0 query/s at 128 replicas (4 → 256 nodes) — 67% weak-scaling efficiency vs the 4-node base, sublinear rather than linear; through a single HAProxy the service tracks the proxy-bypass diagnostic up to 64 nodes (9.3 query/s, 80%) before the head-node streaming ceiling appears. A non-streaming 405B configuration has not been measured.

```yaml
model_configs:
  - model_id: meta-llama/Llama-3.1-405B-Instruct
    tensor_parallel_size: 8
    pipeline_parallel_size: 2
    size: 405
    max_num_seqs: 8
```

### Workload robustness (one-axis-at-a-time)

Holding the 8B baseline fixed and varying one axis at a time, with each row's offered rate pinned at 90% of its single-node saturation. Values are SLO attainment — the fraction of requests meeting TTFT ≤ 2 s and P99 TBT ≤ 250 ms — at N=1 and N=64 nodes with HAProxy:

| Perturbation | attain (N=1) | attain (N=64) | Δ |
|---|---|---|---|
| (baseline) 8B, 64/64, fixed-interval | 0.91 | 0.43 | −0.48 |
| Workload: ShareGPT 2K/2K | 1.00 | 0.90 | −0.10 |
| Workload: ShareGPT 4K/4K | 1.00 | 1.00 | 0.00 |
| Workload: Code (HumanEval) | 1.00 | 1.00 | 0.00 |
| Workload: Chat (ShareGPT natural) | 0.99 | 0.99 | 0.00 |
| Workload: Summarization | 1.00 | 1.00 | 0.00 |
| Model: 120B (TP=8, rate 9) | 0.98 | 0.97 | −0.01 |
| Arrival: Poisson | 0.89 | 0.35 | −0.54 |
| Arrival: BurstGPT trace † | 0.46 | 0.46 | 0.00 |

† BurstGPT replays a fixed total arrival rate not scaled by N, so its N=64 cell is not a weak-scaling stress; reported for completeness.

Long-context and moderate-rate workloads (2K/2K, 4K/4K, code, chat, summarization) and the 120B model hold attainment at 64 nodes within a point of their single-node value. The two rows that degrade — the short/high-rate baseline and Poisson arrivals at the same mean — lose their latency margin at the saturation knee once 64 nodes share the streaming path: the failure is load-shape specific, not model- or workload-specific.

### Steady-state serving vs startup time

All numbers above are steady-state serving, measured after the cluster reports ready. Cluster bring-up does **not** scale the same way — this is Ray's key limitation at HPC scale. Of the four bring-up phases, three are roughly flat in node count: MPI model staging (~50 s for 8B; per-replica weight load ~33 s), Ray cluster start (~45 s), and first-request warm-up (~8 s). The fourth, Ray Serve's `serve.run`, grows superlinearly (measured, HAProxy, 12 replicas/node):

| Nodes | Replicas | deploy_apps (s) | wait_proxies (s) | serve.run (s) | Total (s) |
|---|---|---|---|---|---|
| 64 | 768 | 115 | 38 | 154 | 171 |
| 128 | 1,536 | 156 | 300 | 456 | 470 |
| 256 | 3,072 | 155 | 1,612 | 1,767 | 1,857 |

The growth is concentrated in `wait_proxies` (38 s → 1,612 s, a 42× increase over a 4× node increase): on every deployment broadcast, every Ray Serve proxy resolves every replica handle against the Ray GCS — `R·N²` work that reaches 1.38 M `GetActorInfo` calls and 1,587 s of per-proxy GCS time at 256 nodes. A 512-node bring-up fails outright. Practical consequences: budget PBS walltime as bring-up + serving window (405B weight loading adds substantially more), and reuse a running cluster across runs where possible instead of re-deploying per experiment.

### Launch-time environment knobs

| Env var | Effect |
|---|---|
| `AURORA_ENGINE=sglang` | SGLang instead of vLLM as inference engine |
| `AURORA_PP_SHARD_AWARE=1` | Shard-aware multi-node PP staging + node-pinned per-replica deploys |
| `AURORA_PP_UMBRELLA=1` | Single root-route ingress over the per-replica PP routes |
| `AURORA_NULL_COMPUTE=1` | Skip the engine, simulate latency — control-plane/routing stress tests |
| `AURORA_CLEAN_STAGE=1` | Wipe node-local staged weights first (cold-start timing) |

Scale cliffs and fixes (Ray/vLLM patches at 256+ nodes, thread-pool clamps — applied automatically by the launcher): [doc/KNOWN_ISSUES.md](KNOWN_ISSUES.md).

## Benchmarking harness

Declarative spec (workload × model × node-count matrix) → materialized traces + per-cell PBS jobs → multi-node MPI replay client driving a Go load generator. Run 0 of every cell is warm-up and dropped; per-request TTFT/TBT/E2E land in `<experiments_root>/runs/<spec>/<cell>/runN/results/result*.json`.

```bash
cp eval/site_config_local.example.py eval/site_config_local.py   # edit paths; once
python -m eval.cli spec validate eval/specs/refcard/refcard_smoke_1node.yaml
python -m eval.cli run materialize refcard_smoke_1node
python -m eval.cli run submit-all refcard_smoke_1node
python -m eval.plot.goodput -e refcard_smoke_1node --preset paper
```

Full spec schema: [eval/DESIGN.md](https://github.com/wenyiwang-us/ExaServe/blob/main/eval/DESIGN.md).

## Serving LLM agents and OpenAI-compatible clients

The deployment is a standard OpenAI-compatible endpoint — any client (LangChain `ChatOpenAI`, Academy LLM agents, `litellm`, `openai` SDK) works by pointing the standard variables at it. No auth by default; use the LiteLLM front end for keys/rate limits.

```bash
export OPENAI_BASE_URL=http://<head_node>:4001/v1
export OPENAI_API_KEY=EMPTY
```

## Runnable examples (do not copy; clone and run)

| Example | What | Location |
|---|---|---|
| `examples/config.haproxy.yaml` | 2-node quickstart: 24 replicas behind one HAProxy endpoint | [examples/](https://github.com/wenyiwang-us/ExaServe/tree/main/examples) |
| `refcard_smoke_1node` | 1-node benchmark smoke: deploy → replay → TTFT/TBT → SLO score | [eval/specs/refcard/](https://github.com/wenyiwang-us/ExaServe/tree/main/eval/specs/refcard) |
| `refcard_weakscaling_haproxy` | 8B weak scaling behind HAProxy, 1→64 nodes (extend to 256): 27.1k req/s non-streaming at 256n; streaming plateaus ~4.7k on the head-node network | [eval/specs/refcard/](https://github.com/wenyiwang-us/ExaServe/tree/main/eval/specs/refcard) |
| `refcard_pp405b_2node` | 405B TP8×PP2 demo on one 2-node replica: 30/30 streaming | [eval/specs/refcard/](https://github.com/wenyiwang-us/ExaServe/tree/main/eval/specs/refcard) |
| `refcard_pp405b_scale` | 405B weak scaling 2→128 replicas (4→256 nodes) at fixed per-replica load: 0.7 → 31.0 query/s (streaming, 67% efficiency) | [eval/specs/refcard/](https://github.com/wenyiwang-us/ExaServe/tree/main/eval/specs/refcard) |

Deeper profiling and scaling analyses: [findings/](https://github.com/wenyiwang-us/ExaServe/tree/main/findings).

## Citation

The system and its 1–256-node evaluation are described in an SC26 workshop paper (in preparation):

```bibtex
@misc{wang2026exaserve,
    title = {ExaServe: Deploying and Measuring Large-Scale
             Ray Serve for LLM Inference on Aurora System},
    author = {Wenyi Wang and Shu Shi and Yadu Nand Babuji and
              Ian Foster and Kyle Chard},
    note = {SC26 workshop paper, in preparation},
    year = {2026}
}
```
