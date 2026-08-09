# Hardened architecture notes

These notes describe the implemented architecture after the production-hardening
cutover. The normative architecture and acceptance gates remain
[`../PRODUCTION_HARDENING_EXECUTION_PLAN.md`](../PRODUCTION_HARDENING_EXECUTION_PLAN.md).
If a design note conflicts with that plan, the plan wins.

## Implemented boundaries

| Concern | Canonical owner | Selection authority |
|---|---|---|
| Deployment topology | `exaserve.plan.compiler` | immutable `DeploymentPlan` |
| Site capabilities | `exaserve.site` | hash-bearing `SiteProfile` |
| Allocation membership | scheduler plus `AllocationBinding` | verified runtime artifact |
| Scheduler jobs | `exaserve.schedulers.SchedulerBackend` | `SchedulerPlan`, constrained by `SiteProfile` |
| Accelerator behavior | `exaserve.vendors.VendorBackend` | `DeploymentPlan.vendor` |
| Inference engine | `exaserve.engines.EngineBackend` | `DeploymentPlan.engine` |
| Gateway rendering | `exaserve.proxy.ProxyBackend` | `DeploymentPlan.gateway` |
| Process lifecycle | `RuntimeSupervisor` / `NodeSupervisor` | typed observations and deadlines |
| Readiness | `ReadinessCoordinator` | generation-bound status, receipts, and canaries |
| Eval and ClientLab | shared plan/status APIs | exact plan and run hashes |

Runtime environment variables used by native dependencies are outputs of
`runtime_environment(plan)`. They are not public configuration switches.

## Release support versus implemented adapters

The default release profile accepts native PBS, Intel XPU, vLLM, and HAProxy
for production-mode configuration. Direct exposure and other gateway adapters
require explicit validation mode. Slurm, PSI/J, CUDA, ROCm, and SGLang code is
retained for future qualification but the Aurora profile rejects it before
submission or launch. Implemented does not mean production-qualified.

The currently evidence-backed node maximum and the larger unapproved candidate
target are recorded in `SiteProfile.scale_envelopes` and the hardening status
documents; historical runs do not automatically qualify the new architecture.

## Related notes

- [`pluggable_interfaces.md`](pluggable_interfaces.md): engine and gateway seams
- [`scheduler_abstraction.md`](scheduler_abstraction.md): shared scheduler boundary
- [`vendor_site_abstraction.md`](vendor_site_abstraction.md): site and accelerator ownership
- [`../deploy_slurm_amd.md`](../deploy_slurm_amd.md): unsupported-platform qualification guide
- [`../../eval/DESIGN.md`](../../eval/DESIGN.md): eval control plane
