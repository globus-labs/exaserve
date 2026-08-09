# Historical benchmark results

This directory retains analysis helpers and historical measurements produced by
the pre-hardening benchmark harness. It is not a deployment or experiment entry
point and is outside the production release surface.

The old `bench_client.py`, `bench_proxy.py`, stub launchers, and shell
orchestrators were removed because they generated the retired eval manifest,
owned process groups independently of the canonical supervisor, and could no
longer produce trustworthy current results.

Use ClientLab for client, proxy, synthetic-target, saturation, and internode
studies:

```bash
python3 -m clientlab plan clientlab/specs/client_microbench.yaml
python3 -m clientlab run clientlab/specs/client_microbench.yaml --local
python3 -m clientlab report <study-directory>
```

Use `python3 -m eval.cli` for serving experiments. Follow `AGENTS.md` for every
nontrivial or multi-node run. Existing files such as `stats.md` and analysis
scripts describe old result formats only and do not qualify the hardened
architecture.
