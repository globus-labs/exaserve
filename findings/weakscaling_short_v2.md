# Weak-Scaling Short v2 (64in/64out, Llama-3-8B)

## Server Characterization (2026-04-09)

Re-characterized 1-node throughput with relaxed plateau_ratio (0.75 vs old 0.95).
The old 0.95 threshold incorrectly declared saturation at 25 RPS — the ~8-10%
achieved/target gap is client measurement overhead, not server saturation.

| Config (mns / mor) | Saturation (RPS) | @100 RPS achieved | p50 @100 |
|---------------------|-----------------|-------------------|----------|
| mns64, mor64        | 109             | 89.4              | 204ms    |
| mns64, mor256       | **118**         | 89.5              | 83ms     |
| mns256, mor64       | 115             | 90.1              | 193ms    |
| mns256, mor256      | 115             | 82.8              | 306ms    |

**Winner:** mns64, mor256 (118 RPS). Larger max_num_seqs doesn't help at 64-token
sequences. Higher replica_max_ongoing_requests (256) improves queueing headroom.

Spec: `eval/specs/server_char_llama8b_short_v2.yaml`
Data: `/lus/flare/projects/AuroraGPT/wenyiw/data/experiments/runs/server_char_llama8b_short_v2/run1/`

## Weak-Scaling v2 Specs

Based on characterization, target rate_per_node = 110 RPS (~93% of saturation).
Duration: 30s, num_runs: 3.

### Run protocol
- **Run 0 is warmup** — discard from analysis.
- **Runs 1-2 are measured** — report mean and variance of these only.
- No explicit warmup_rps/warmup_duration_s needed.

### Deployment config
- max_num_seqs: 64
- replica_max_ongoing_requests: 256
- 12 replicas/node (auto), TP=1, PP=1

### Client config
- HAProxy: dest=proxy, num_go_procs=8, num_go_workers=4
- Direct: dest=direct, num_go_procs=8, num_go_workers=4

### Specs
- `eval/specs/weakscaling_haproxy_short_v2.yaml`
- `eval/specs/weakscaling_direct_short_v2.yaml`
