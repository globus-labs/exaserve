# Vendor and site ownership

Status: implemented. Aurora/PVC is the only release-qualified site/vendor
family.

## Separate concepts

A vendor backend describes accelerator mechanics: device visibility, engine
device names, and bounded vendor-specific workarounds. A `SiteProfile` describes
facility facts and support claims: schedulers, accelerator inventory, nodes and
CPUs, paths, launcher capabilities, network boundary, prepared environment,
control/readiness limits, and scale envelopes.

The two identities are deliberately independent. A site may use the same vendor
as another site while requiring different scheduler, filesystem, fabric, or
compatibility behavior.

## Immutable authority

The compiler verifies `DeploymentPlan.vendor` and `DeploymentPlan.engine`
against the selected `SiteProfile`. The profile hash and compatibility hashes
participate in the plan identity. Runtime setup then:

1. loads and verifies the exact `SiteProfile` artifact;
2. removes forbidden environment names such as `ONEAPI_DEVICE_SELECTOR`;
3. applies the profile's prepared environment and stack limit;
4. projects plan runtime fields into native-child environment values;
5. passes the plan-derived vendor into each `EngineSpec`.

Ambient `EXASERVE_VENDOR` is therefore not a supported way to change a compiled
deployment. It remains only as an internal compatibility-tool fallback.

## Current implementations

- XPU: used by the Aurora profile and enforces `ZE_AFFINITY_MASK` ownership.
- CUDA: adapter present, no qualified site profile.
- ROCm: adapter present, no qualified site profile.

CUDA/ROCm code paths must not be advertised or launched through the Aurora
profile. Qualification requires a new profile and compatibility manifest,
real accelerator smoke tests, exact worker receipts, lifecycle failure tests,
and scale-envelope evidence.

## Evidence-backed limits

Default control/readiness constants are structurally valid candidate values,
not measurements. `ControlLimits.evidence_backed` and
`ReadinessLimits.evidence_backed` remain false until the required hardware
evidence is recorded. Code availability cannot turn those flags into a
production claim.
