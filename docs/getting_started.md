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

This section is the operator setup guide, not a requirement to install a
maintainer's personal scripts. Use the site's scheduler and module system.
Portable development dependencies and the serving runtime are separate.

### Obtain compute resources

Login nodes are for editing, inspection, configuration preparation, submission
and brief lightweight checks. Full suites, package/build gates, benchmarks,
evaluations, MPI and GPU work belong on allocated compute nodes.

For ad-hoc work, reuse an authorized allocation or request an interactive PBS
session using the site's published instructions and your project's account,
queue, node count and walltime. ExaServe does not supply or require a personal
lease manager. Do not guess allocation settings or enter a compute node through
plain SSH and assume that creates a valid scheduler session.

Before using an interactive session, verify:

- `PBS_JOBID` is set and `PBS_NODEFILE` is readable.
- The current hostname belongs to the nodefile and the node count is correct.
- The shell was entered through the scheduler's interactive workflow
  (`PBS_ENVIRONMENT=PBS_INTERACTIVE` for native PBS), not fabricated exports.
- The remaining allocation walltime is sufficient; inspect
  `qstat -f "$PBS_JOBID"`. If your site offers an approved lease service, check
  its deadline too and preserve its real scheduler state.

For normal serving/evaluation, the [canonical submission commands](usage.md)
create native PBS batch jobs; a separate interactive allocation is not required
to submit them. Those jobs execute in their own allocation. Do not set
interactive/lease markers in a batch job to bypass checks. SSH from an allocated
head is limited to nodes actually assigned to that session.

### Prepare the runtime

Load Aurora's site toolchain in every fresh session. Brief permitted login-node
Python/Go checks also need these modules rather than the old system Python:

```bash
module load frameworks
module load go
unset ONEAPI_DEVICE_SELECTOR
```

The serving profile records Python 3.12.12, Ray 2.53.0 and vLLM 0.15.0+xpu.
Compare the loaded module with the
[site dependency record](../requirements/aurora-frameworks-2025.3.1.lock) and
[compatibility matrix](../doc/hardening/COMPATIBILITY_MATRIX.md). The unversioned
module name can change; a version mismatch requires an approved runtime update,
not bypassing compatibility checks. These are site-built distributions, not
a public-PyPI installation recipe.

Use the qualified module-provided interpreter. Canonical launch requires its
executable to be non-shared and inside a declared read-only site root
(`/opt/aurora` in the current profile). A home-directory virtualenv can fail this
check even if its Python symlinks to the module interpreter. This is stricter
than merely being able to import Ray.

The operator must provision the exact approved ExaServe artifact and dependencies
for that interpreter and its batch bootstrap. Do not modify the shared module
installation yourself. The [README preflight](../README.md#install-on-aurora)
checks imports, not installation or qualification. The setup must provide:

- ExaServe and its control-plane dependencies to that Python with
  `PYTHONNOUSERSITE=1` and `PYTHONSAFEPATH=1`. A `pip install --user` alone is
  insufficient. Regular eval supplies its immutable source snapshot explicitly;
  this is distinct from installed-wheel serving.
- The compatible Ray/vLLM/XPU/MPI libraries, MPI compiler/launcher and required
  shared libraries.
- HAProxy on the batch runtime's `PATH`, or the configured gateway executable.
  The repo's [build helper](../scripts/build_haproxy.sh) requires an audited
  `HAPROXY_SOURCE_SHA256`; compile on compute, not a login node.
- Readable model inputs and writable run/scratch paths with sufficient space.
- Site-approved proxy/network settings when downloads require them, without
  routing allocation-internal serving traffic through an external proxy.

Do not install the `server` extra over the site stack or use
`uv sync --all-extras` to prepare it. Keep the portable `.venv` separate.
An isolated LiteLLM gateway uses its own configured Python; the parent stays
in the Ray/vLLM runtime. Use `ZE_AFFINITY_MASK` for Aurora device visibility,
never `ONEAPI_DEVICE_SELECTOR`.

Before launching, check the selected interpreter and tools without starting
Ray or importing the GPU stack:

```bash
python3 --version
PYTHONNOUSERSITE=1 PYTHONSAFEPATH=1 python3 -c 'import sys, exaserve; print(sys.executable); print(exaserve.__file__)'
command -v mpiexec
command -v mpicc
command -v haproxy
```

Check the printed paths against the intended interpreter and artifact; an
inherited checkout `PYTHONPATH` must not masquerade as an installed-wheel check.
Successful command lookup is a preflight check, not hardware qualification.

### Configure evaluation environment preparation

For ordinary Ray evaluation jobs, explicitly select your own setup file instead
of relying on a home-directory default. Save a site-reviewed shell file at an
absolute path readable by the job head. Its job is only environment preparation;
it must not launch daemons, implement readiness/cleanup, or stage data per node.
For example, adapting the paths to your installation:

```bash
# Contents of your operator-owned environment.sh; sourced by the job head.
module load frameworks || return 1
module load go || return 1
unset ONEAPI_DEVICE_SELECTOR
export PATH="/absolute/path/to/haproxy/bin:$PATH"
```

Keep the module-provided `python3`; do not prepend a home virtualenv's Python.
Select the setup file before materializing the run:

```bash
export EXASERVE_ENV_SCRIPT_AURORA=/absolute/path/to/environment.sh
export EXASERVE_PROJECT_ROOT=/lus/flare/projects/YOUR_PROJECT
python3 -m eval.site_config get env_script_aurora
```

Set model/data/output paths in your
[local site configuration](../eval/site_config_local.example.py).
`EXASERVE_SITE_CONFIG_LOCAL` can select another override file.
A per-spec `backend.args.ray.launch.env_script` takes precedence over the
environment variable, which takes precedence over local `SITE_OVERRIDES`;
use an absolute path in the spec too. The field names are existing API names,
not required filenames. If using the validation-only LiteLLM gateway, configure
`EXASERVE_LITELLM_PYTHON_PATH` (or its per-spec Python override) separately.

### Current bootstrap limitations

Environment preparation is not yet uniform across all entry points:

- **Serving CLI:** captures the submitting interpreter's `sys.executable`.
  Its job bootstrap still checks `$HOME/script/env_aurora` and otherwise loads
  `frameworks`; no custom setup-file CLI option exists. That private file is
  optional, but if present it is executed. The eval environment override above
  does not affect this entry point. Ensure the recorded interpreter, required
  libraries and gateway are available after that bootstrap; inspect the rendered
  job with `--dry-run` before submission. Loading a different module in the job
  does not change the recorded interpreter.
- **Allocation campaigns:** currently hard-code a private environment-script
  path, spool location and account assumptions in
  [the controller](../eval/lib/allocation_campaign.py). They do not honor the
  ordinary eval override. Treat that path as operator-specific, not a portable
  deployment recipe. Making it configurable must bind the setup to the immutable
  campaign identity and re-run the WP12 proof; do not edit a generated campaign
  or fabricate private helper files to make it pass.
- **Historical hardening helpers and the mock eval backend:** some retain
  maintainer-local paths. They are not the general installation interface.

These limitations are recorded, not fixed by changing documentation. Use the
ordinary explicit-config Ray evaluation path where applicable; do not advertise
the allocation controller as portable until its bootstrap is corrected and
qualified.

### Before the first run

Read [usage](usage.md) and [hardening status](../doc/hardening/STATUS.md).
Production admission is fail-closed; `production execution is not qualified`
is a qualification gate, not a request to bypass checks. Only predeclared,
authorized validation work may enable `validation_mode`.

Verify the working directory, configs, inputs, environment, output paths and
walltime. Use unique output directories and preserve prior artifacts. Monitor
stdout/stderr and canonical status; diagnose early errors and safely stop
clearly broken work. Report the exact command, session and evidence paths;
distinguish scheduler walltime expiry from application failure. Cleanup belongs
to the canonical executor, not a second shell lifecycle.
