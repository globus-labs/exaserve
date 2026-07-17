# ExaServe generic-HPC refactor — design index

ExaServe is being generalized from an Aurora/Intel-XPU/PBS-specific stack into a
portable HPC serving platform: **Ray + engine-of-choice + proxy-of-choice**, with
pluggable schedulers and vendors. This directory holds the design; the table below
is the source of truth for **what is built vs. proposed**.

## Roadmap & status

| Step | Scope | Doc | Status |
|---|---|---|---|
| (1) | Rename `aurora_rayserver` → `exaserve` (package, CLI, env vars, brand) | — (commit `6a2faa9`) | **Done & validated on real compute** (1-node all-features, 2-node 24-replica proxy/internode; 405B PP re-smoke in progress) |
| (2) | Pluggable **engine** interface (+ proxy polish) | [pluggable_interfaces.md](pluggable_interfaces.md) | **Partially built**: `engines/` ABC + `NullEngine` + registry shipped; `server.py` extraction (VLLM/SGLang engines behind one `EngineWorker`) **not started** |
| (3) | Pluggable **scheduler** (PBS → Slurm) | [scheduler_abstraction.md](scheduler_abstraction.md) | **Built** (`feature/slurm-amd-support`): package `SchedulerBackend` PBS+Slurm + srun/nodefile runtime seam. PBS validated, Slurm untested (offsite). Eval harness still PBS-only. |
| (4) | Pluggable **vendor** (XPU → CUDA/ROCm) + **site** config | [vendor_site_abstraction.md](vendor_site_abstraction.md) | **Built** (`feature/slurm-amd-support`): `VendorBackend` XPU/CUDA/ROCm, engines delegate device isolation. XPU validated, CUDA/ROCm untested (offsite). `SiteConfig` object still deferred (env-var based). See [deploy_slurm_amd.md](../deploy_slurm_amd.md). |
| (5) | Update `doc/exaserve.md`, README, paper to the generic framing | — | Not started (do last, once the above land) |

## The common pattern

All four axes converge on one shape, already proven by the proxy layer
(`proxy/base.py::ProxyBackend` + `proxy/get_proxy()`):

> an **ABC** + a lazy **registry** + a single **host/caller** that touches only the
> interface, selected by an `EXASERVE_*` env var or the site config.

- Proxy: `ProxyBackend` / `get_proxy` — **already clean** (the template).
- Engine: `EngineBackend` / `get_engine` — interface shipped, host wiring pending.
- Scheduler: `SchedulerBackend` / `get_scheduler` — proposed.
- Vendor: `VendorBackend` / `get_vendor` — proposed.
- Site: `SiteConfig` composes the above (picks vendor + scheduler, supplies facts).

## Validation gate (applies to every step)

Any refactor of the serving/launch path must re-pass the smoke set the rename was
validated against, with numbers matching baselines:
`nullcompute_smoke_1node`, `refcard_smoke_1node` (≈28.6 rps, 0 err),
`smoke_slo_stream_{proxy,direct,rayserve}`, `serverstats_ttft_{off,on}delay_1node`,
`serverstats_ttft_ondelay_2node` (24 replicas, ≈39 rps, 0 err), and the 405B PP
runs (`pp405b_pp2_proxyfix` 4-node, `pp405b_verify_2node`).

## What stays site/vendor-specific (not portable, by design)

Aurora `subjob`/`keepalive` dev tooling, `env_aurora`/`/opt/aurora`/frameworks
module, the Intel-XPU `EXASERVE_XPU_*` workarounds, and the PVC/Triton-SYCL
quirks — these are inputs to the abstractions, consumed via `SiteConfig` /
`VendorBackend`, never hardcoded in the serving core.

## Offsite / unprovable here

Slurm end-to-end and NVIDIA/AMD end-to-end **cannot** be validated on Aurora
(PBS + XPU only). Those backends can be unit-tested for command/env shape here;
real validation is owed on a Slurm system and CUDA/ROCm hardware respectively, and
must be flagged unproven until then.
