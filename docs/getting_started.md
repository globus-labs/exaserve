# Getting started

Choose the environment for the work you intend to do. The portable development
environment is not an installation recipe for Aurora's serving stack.

## Portable development

Use a Linux workstation or a suitable CI runner with Git, Make, Python, and
`uv==0.10.1` (install it with `python3 -m pip install uv==0.10.1`).
Package metadata requires Python 3.10 or newer; the portable CI matrix targets
Python 3.10 and 3.12. Select one of those versions for reproducible development.
This does not claim qualification on Python 3.13 or 3.14, nor does portable CI
qualify any GPU runtime.

The committed uv lock resolves Linux Python 3.10–3.13; the existing optional
vLLM dependency excludes Python 3.14. Use a CI interpreter version above for
development rather than assuming every version allowed by package metadata is
covered by the lock or tested by CI.

```bash
git clone https://github.com/globus-labs/exaserve.git
cd exaserve
make install-dev
```

`make install-dev` synchronizes the portable development and test dependency
groups from [uv.lock](../uv.lock). The dependency declarations and tool settings
live in [pyproject.toml](../pyproject.toml). Ray, vLLM, MPI, and accelerator
drivers are not required for the portable control-plane test environment.

From the repository root:

```bash
make lint
make format-check
make type-check
make test
make test-cov
make build
```

These targets provide lint/format checks, the selected typed-core check, tests,
coverage, and distribution builds. The typed-core check is not a claim that the
entire repository is statically checked. Use locked tool invocations for
individual checks, for example:

```bash
uv run --locked ruff check src/exaserve/config.py
uv run --locked exaserve-status --help
```

Read [CONTRIBUTING.md](../CONTRIBUTING.md) before changing dependencies, runtime
contracts, or generated compatibility files. Do not regenerate the lockfile as
a routine response to a failing test; first establish whether the interpreter
and dependency profile are correct.

## Aurora development and serving

On Aurora, follow [AGENTS.md](../AGENTS.md) before executing code. Login nodes
are for editing, inspection, configuration preparation, submission, and brief
lightweight checks. Full test suites, package/build gates, benchmarks,
evaluations, MPI, and GPU work belong in an allocated compute session.

For a brief permitted login-node Python or Go check, first load the project
toolchain:

```bash
module load frameworks
module load go
```

Do not use the old system Python. Loading modules does not make a login node
appropriate for a full test suite or experiment.

### Obtain and validate a session

Reuse a valid existing session when possible. Otherwise, lease only the nodes
needed from the user's existing keepalive/debug allocation:

```bash
subjob 1
```

The keepalive allocation is user-managed; do not start or manage it as part of
this workflow. If no source is available to `subjob`, use `~/script/srundbg`
for one node or `~/script/srundsc N` for an interactive PBS fallback. Do not
invent scheduler parameters or replace the lease with a plain SSH shell.

A valid session requires all of the following:

- `PBS_JOBID` is set.
- `PBS_NODEFILE` exists and is readable.
- The current hostname matches a host in that nodefile.
- `AURORA_SUBJOB=1`, or the shell is the verified interactive PBS compute shell.

Check the leased node count against the intended workload, and inspect
`subjob status` for lease time remaining. Account for the underlying PBS job
deadline as well as the lease TTL before starting work.

### Initialize the runtime inside that session

```bash
source ~/script/env_aurora
unset ONEAPI_DEVICE_SELECTOR
cd /path/to/exaserve
command -v python3
command -v mpiexec
command -v haproxy
```

Replace `/path/to/exaserve` with the checkout path. Verify the active
environment, input files, model/checkpoint access, executables, and writable
output locations before proceeding. Use `ZE_AFFINITY_MASK` for Aurora GPU
visibility; do not introduce `ONEAPI_DEVICE_SELECTOR`.

Aurora's module-provided compatibility profile records Python 3.12.12,
Ray 2.53.0, and vLLM 0.15.0+xpu. See the
[site dependency record](../requirements/aurora-frameworks-2025.3.1.lock) and
[compatibility matrix](../doc/hardening/COMPATIBILITY_MATRIX.md). These are
site-specific distributions, not a generic PyPI installation recipe. Their
presence alone does not satisfy current-candidate release gates.

Do not run `uv sync --all-extras`, install the `server` extra over the module
stack, or use the portable `.venv` as the Aurora Ray/vLLM parent environment.
Keep the portable development environment separate. After an approved package
is built, install that exact artifact into the intended site environment
without replacing its module-provided dependencies, following the applicable
package/qualification procedure.

Use `source ~/script/env_aurora` for ExaServe runs even when they use an isolated
LiteLLM gateway. `source ~/script/env_litellm` is only for standalone LiteLLM
diagnostics that do not import or run Ray/vLLM in the parent process.

### Before the first run

Read [usage](usage.md) and [hardening status](../doc/hardening/STATUS.md).
Production admission is fail-closed; `production execution is not qualified`
is a qualification gate, not a request to bypass checks. Only predeclared,
authorized validation work may enable `validation_mode`.

Use unique output directories, keep prior artifacts, and actively monitor
stdout/stderr and canonical deployment status. Stop to diagnose an early
failure instead of waiting for a broken process to exit on its own.
