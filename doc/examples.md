# Aurora RayServer — Running Examples

Companion to the [reference card](reference_card.md). Four worked examples, smallest first. All assume the install and `PATH` setup from the card's §1, run from the repo root on an Aurora login node.

---

## Example 1 — 2-node OpenAI-compatible service (~10 min + queue)

The quickstart: serve Llama-3-8B on 2 nodes (24 replicas) behind HAProxy.

```bash
# One-time: build HAProxy (not on Aurora by default)
bash scripts/build_haproxy.sh

# Point the example config at your Lustre model dir
cp examples/config.haproxy.yaml my_config.yaml
$EDITOR my_config.yaml            # edit model_storage_path (and local_stage_path if desired)

# Submit and wait for the URL
aurora-serve-submit my_config.yaml --project-account YOUR_PROJECT --wait
# 8470123.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov
# http://x4310c1s0b0n0:4001
```

Query it from the login node (any host on the HSN works):

```bash
curl -sS -X POST "http://x4310c1s0b0n0:4001/v1/chat/completions" \
     -H 'Content-Type: application/json' \
     -d '{"model":"meta-llama/Meta-Llama-3-8B-Instruct",
          "messages":[{"role":"user","content":"hello"}],"max_tokens":8}'
```

Expected: an OpenAI-style JSON response,
`{"id":"chatcmpl-...","choices":[{"message":{"role":"assistant","content":"Hi!"}}],...}`.
Tear down with `qdel 8470123`.

If a model isn't already cached under `model_storage_path`, the first boot downloads it from HuggingFace — set the ALCF web-proxy env vars first (see [README.md](../README.md#required-environment)).

## Example 2 — single-node benchmark smoke (~15 min + queue)

Validates the whole measurement pipeline — deploy, replay via the Go load generator, per-request TTFT/TBT capture, SLO scoring — on one node.

```bash
# One-time eval-harness setup
module load go && bash eval/go_client/build.sh
cp eval/site_config_local.example.py eval/site_config_local.py
$EDITOR eval/site_config_local.py      # your Lustre data/model paths

# Materialize + submit the smoke spec (1 node, streaming, HAProxy)
python -m eval.cli spec validate eval/specs/sc26workshop/smokes/smoke_slo_stream_1node.yaml
python -m eval.cli run materialize smoke_slo_stream_1node
python -m eval.cli run submit-all smoke_slo_stream_1node
```

When the PBS job finishes, results are under
`<user_data_root>/experiments/runs/sc26workshop/smokes/smoke_slo_stream_1node/run0/results/result*.json`
— per-request records plus aggregates under the `"overall"` key. Score against the latency SLO:

```bash
python -m eval.plot.goodput -e smoke_slo_stream_1node --preset paper
```

## Example 3 — weak-scaling sweep to 256 nodes

The paper's proxy-comparison series: Llama-3-8B, one replica per XPU tile (12/node), 110 req/s offered per node, 64-in/64-out streaming, `dest=direct` (clients spread over every node's port 8000 — no head-node proxy).

Spec: [eval/specs/sc26workshop/full/proxycmp_direct.yaml](../eval/specs/sc26workshop/full/proxycmp_direct.yaml) (N ∈ {1,4,16,64}) and its `_scale` extension (N ∈ {128,256}).

```bash
python -m eval.cli run materialize proxycmp_direct
python -m eval.cli run submit-all proxycmp_direct     # one PBS job per node count
```

Each cell submits to the queue that fits its size (derived in the spec: small N → `capacity`, 64–128 → `debug-scaling`, 256 → `prod`). Run 0 of each cell is warm-up and dropped automatically.

**Measured result** (data runs, streaming): throughput scales linearly from 1 to 256 nodes, reaching **18.1k req/s at 256 nodes / 3,072 tiles at ~100% weak-scaling efficiency**. The identical workload through a single head-node HAProxy ([proxycmp_haproxy.yaml](../eval/specs/sc26workshop/full/proxycmp_haproxy.yaml)) plateaus at ~4.7k req/s from 128 nodes — the head node's network saturates on the streaming token path. This is why the reference card recommends `direct` dispatch at scale. Full result matrix: [eval/specs/sc26workshop/README.md](../eval/specs/sc26workshop/README.md).

## Example 4 — Llama-3.1-405B, multi-node pipeline parallel

405B spans two nodes per replica (TP=8 within a node × PP=2 across a node pair). Shard-aware staging gives each pipeline stage only its own weight shard (~380 GiB, fits node-local tmpfs), and each replica is deployed node-pinned at its own route. The harness derives `AURORA_PP_SHARD_AWARE=1` automatically from the spec (PP>1 with multiple replicas) — no hand-set environment needed.

Smallest demonstration — one 2-node replica, 30 streaming requests:

```bash
python -m eval.cli run materialize pp405b_verify_2node
python -m eval.cli run submit-all pp405b_verify_2node
```

Expected: 30/30 streaming completions. Spec: [eval/specs/sc26workshop/smokes/pp405b_verify_2node.yaml](../eval/specs/sc26workshop/smokes/pp405b_verify_2node.yaml).

Weak scaling — replica count grows with the allocation (`num_replicas = num_nodes / 2`), clients hit each replica's stage-0 node directly:

```bash
python -m eval.cli run materialize pp405b_pp2_scale_direct   # N ∈ {4,8,16,32,64,128,256}
python -m eval.cli run submit-all pp405b_pp2_scale_direct
```

**Measured result**: scales from 2 to 128 replicas (4 → 256 nodes) with zero errors. Spec (heavily annotated, including queue-routing rationale): [eval/specs/sc26workshop/full/pp405b_pp2_scale_direct.yaml](../eval/specs/sc26workshop/full/pp405b_pp2_scale_direct.yaml). For fronting shard-aware PP with a single endpoint instead, set `AURORA_PP_UMBRELLA=1` (root-route umbrella ingress) or use the HAProxy path in [pp405b_pp2_scale.yaml](../eval/specs/sc26workshop/full/pp405b_pp2_scale.yaml).

---

## Where the numbers live

- Suite status and result tables: [eval/specs/sc26workshop/README.md](../eval/specs/sc26workshop/README.md)
- Figure generation: `python -m eval.plot.sc26_full_figures` → `eval/plot/output/sc26_full/`
- Profiling / scaling analyses behind the design choices: [findings/](../findings/)
