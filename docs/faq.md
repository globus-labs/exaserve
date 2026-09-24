# Frequently asked questions

## What is ExaServe?

ExaServe coordinates allocation-scoped Ray Serve deployments for
OpenAI-compatible LLM inference. It compiles immutable plans, supervises
runtime processes, and exposes generation-bound readiness to serving and
evaluation clients. Start with [the overview](../README.md) and
[documentation index](index.md).

## Which Python versions are supported?

The portable package requires Python 3.10 or newer, with a portable CI matrix
targeting Python 3.10 and 3.12. That metadata is not a claim that every newer
Python version has been qualified. The pinned Aurora serving compatibility
profile uses Python 3.12.12, Ray 2.53.0, and vLLM 0.15.0+xpu. See
[dependency profiles](../requirements/README.md).

## Should I install all optional dependencies?

No. `make install-dev` creates the portable development/test environment. The
module-provided Aurora Ray/vLLM/XPU stack is a separate environment. Do not
replace it with `uv sync --all-extras` or a generic `server` extra installation.
An isolated LiteLLM gateway does not replace the site-provided Ray/vLLM
environment required by the ExaServe parent. Standalone LiteLLM diagnostics
and the serving parent use separate environments. See
[Aurora setup](getting_started.md#aurora-development-and-serving).

## Can I run the full test suite on an Aurora login node?

No. Use a validated compute session for full tests, package/build gates,
benchmarks, evaluations, MPI, and GPU work. Only brief, clearly lightweight
checks are suitable for a login node, after loading `frameworks` and `go` when
needed. A command named `smoke` or an option named `--local` is not sufficient
evidence that it is safe for a shared login node. [AGENTS.md](../AGENTS.md)
governs the execution location.

## How do I obtain Aurora compute resources?

Reuse an existing valid session or request a scheduler-created interactive
PBS allocation using current site-approved instructions and only the nodes
needed. Validate the job ID, readable nodefile, current-host membership,
expected node count, interactive session provenance, and remaining walltime
before running work. Plain SSH does not establish a valid allocation. See
[Aurora setup](getting_started.md#aurora-development-and-serving) for toolchain,
runtime, and preflight guidance; personal helper scripts are not required.

Canonical batch campaigns use `python3 -m eval.cli run materialize`, `submit`,
and `submit-all`; their workloads execute in native PBS batch allocations,
not fabricated interactive sessions. See [usage](usage.md).

## Why does submission say production execution is not qualified?

The configuration is subject to the selected `SiteProfile` and exact-candidate
qualification gates. Installation, portable tests, an implemented adapter, or a
historical hardware run do not satisfy those gates. Read
[hardening status](../doc/hardening/STATUS.md), including supersession notices.
Only authorized, predeclared validation may use `validation_mode: true`; it is
not a general workaround for an admission failure.

## Does a node ceiling or an implemented backend establish platform support?

No. Physical/validation limits, proposed scale targets, measured historical
results, and a production-qualified envelope are different things. Qualification
is tied to the exact artifact, site/runtime profile, topology, exposure, and
workload dimensions. A different platform or backend requires its own evidence
and admission profile. See the
[site/vendor design](../doc/design/vendor_site_abstraction.md) and
[unsupported-platform qualification guide](../doc/deploy_slurm_amd.md).

## Why is my scheduler job running but ExaServe is not READY?

The scheduler allocates resources; it does not attest model or gateway
readiness. Use `exaserve-status show --run-dir /path/to/run --json` and inspect
the exact generation's blockers and errors. `READY` requires the compiled
membership, component/compatibility evidence, and endpoint canary, and can be
revoked when evidence becomes stale or a component fails. See [usage](usage.md).

## Can I retry a submission or edit a materialized plan?

Repeated submission of the same completed intent attaches to the recorded job.
Investigate ambiguous scheduler acceptance before retrying. Use
`--new-generation` only when intentionally requesting a new generation.
Generated plans and run bundles are immutable; change the source configuration
and materialize a new identity instead of editing artifacts in place. Preserve
old results and logs.

## Why does eval submit-all require a run group?

The explicit `--run-group`, such as `run0`, prevents submission from silently
selecting a different materialized run identity. Use the actual group in the
bundle path, not an inferred `latest`. [Usage](usage.md) shows a preview command.

## Are synthetic ClientLab results serving qualification?

No. Synthetic studies are diagnostic. Real ExaServe studies must bind the
RunPlan, trace, generation, and deployment status directory and consume the
shared readiness API. See [ClientLab](../clientlab/README.md).

## Where should I report problems or contribute?

Read [CONTRIBUTING.md](../CONTRIBUTING.md), including its policy on AI-assisted
contributions, and search the
[issue tracker](https://github.com/globus-labs/exaserve/issues). Include
reproduction steps, package/commit identity, Python and runtime versions, and
redacted errors. Follow [SECURITY.md](../SECURITY.md) for security reports;
do not publish credentials or sensitive deployment data.

## What is the license?

ExaServe is distributed under the [Apache License 2.0](../LICENSE). See
[NOTICE](../NOTICE) for project attribution information. Dependencies remain
subject to their own licenses.

## Why are there both docs/ and doc/ directories?

`docs/` is the public getting-started, usage, and API entry point. The existing
`doc/` directory retains architecture, hardening decisions, audits, and evidence
context. The [execution plan](../doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md) is
the sole architecture and implementation specification, while
[AGENTS.md](../AGENTS.md) controls operational safety. The two directories are
linked rather than competing specifications.
