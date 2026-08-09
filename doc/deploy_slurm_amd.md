# Slurm and CUDA/ROCm qualification guide

Status: unsupported by the default ExaServe release profile. This is a
qualification checklist, not a deployment recipe.

The repository contains Slurm, CUDA, and ROCm adapters, but the authoritative
Aurora `SiteProfile` accepts only PBS + XPU + vLLM. Setting environment variables
does not enable an unsupported combination: compilation and submission reject
it before launch.

To qualify a new platform, create a separately named, hash-bearing
`SiteProfile` that records:

- exact scheduler and launcher behavior;
- accelerator inventory and vendor;
- model/shared/local storage semantics;
- environment preparation and forbidden variables;
- compatibility profile and patch manifest;
- network/exposure boundary;
- measured control/readiness limits;
- per-protocol and per-gateway scale envelopes.

Then provide, in order:

1. hermetic unit tests for strict plan validation and job rendering;
2. real scheduler submit, observe, cancel, and ambiguous-submit reconciliation;
3. one-node engine startup, request, receipt, failure, and cleanup evidence;
4. two-node allocation binding, rank failure, gateway, and streaming evidence;
5. approved scale-ladder evidence for every advertised envelope;
6. clean wheel installation and execution on the target site.

Do not copy Aurora's XPU compatibility profile into another site or mark its
historical tests as evidence. Do not add the platform to a release support
matrix until the new profile is reviewed and its measured limits are marked
evidence-backed.

The implemented seams are described in
[`design/scheduler_abstraction.md`](design/scheduler_abstraction.md) and
[`design/vendor_site_abstraction.md`](design/vendor_site_abstraction.md).
