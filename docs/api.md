# API reference

This is a manual reference to the public planning and status boundaries. The
[execution plan](../doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md) remains the
architecture contract. Private helpers and process-supervisor internals are not
alternate extension points.

## Immutable plans

The package exports `DeploymentPlan`, `RunPlan`, `SiteProfile`,
`AllocationBinding`, `compile_deployment_plan`, and `compile_run_plan` from
`exaserve`. The complete canonical plan namespace is
[`exaserve.plan`](../src/exaserve/plan/__init__.py); contracts are defined in
[`exaserve.plan.contracts`](../src/exaserve/plan/contracts.py).

| Object | Responsibility |
| --- | --- |
| `DeploymentPlan` | Compiled topology, models, exposure, gateway, runtime policy, and support-envelope identity |
| `RunPlan` | Exact deployment plus workload, trace, client, scheduler, backend, and artifact policy |
| `SiteProfile` | Hash-bearing site capabilities and qualified execution limits |
| `AllocationBinding` | Verified allocation membership and runtime identity |
| `PlanError` | Invalid or inconsistent configuration, contract, or plan artifact |

The [compiler](../src/exaserve/plan/compiler.py) accepts human-authored mappings:

```python
def compile_deployment_plan(
    raw,
    *,
    site=None,
    deployment_id="deployment",
    compatibility_profile_hash="",
    manifest_hash="",
    envelope=None,
) -> DeploymentPlan: ...

def compile_run_plan(
    raw,
    *,
    site=None,
    run_id,
    deployment_id,
    scheduler=None,
    workload=None,
    trace=None,
    client=None,
    backend=None,
    artifacts=None,
) -> RunPlan: ...
```

`raw` is a mapping; `site` is an optional `SiteProfile`. The compiler validates
the configuration and resolves it into a finalized semantic plan. Compilation
does not launch a deployment or constitute hardware qualification. Prefer the
canonical CLI for operator submissions; compiling a plan is not a substitute
for its admission, allocation, and runtime checks.

[`exaserve.plan.io`](../src/exaserve/plan/io.py) provides paired
`load_deployment_plan` / `write_deployment_plan`, `load_run_plan` /
`write_run_plan`, and `load_site_profile` / `write_site_profile` functions.
Loaders accept a path, rehydrate typed data, and verify content hashes.
Writers take `(path, object)` and reject replacement by different immutable
content. Do not edit a generated plan JSON file to change a deployment.

For example, inspecting an existing artifact requires no Ray import or launch:

```python
from exaserve.plan.io import load_deployment_plan

plan = load_deployment_plan("/path/to/deployment.plan.json")
print(plan.deployment_id)
print(plan.deployment_plan_hash)
print(plan.num_nodes)
```

## Shared deployment status

Consumers use [`exaserve.status_api`](../src/exaserve/status_api.py), not process
inspection, private readiness files, or log parsing.

| Interface | Result |
| --- | --- |
| `read_deployment_status(run_dir, *, clock=None)` | A typed `DeploymentStatus`, or `None` if no record has been published |
| `require_ready_status(run_dir, *, expected_generation=None, expected_plan_hash="", clock=None)` | The current READY status, with optional exact-identity checks |
| `require_ready_endpoint(run_dir, *, expected_generation=None, expected_plan_hash="", clock=None)` | The compiled advertised endpoint from that validated READY status |

`DeploymentStatus` exposes identity fields, state/revision, reason/detail,
readiness evidence, model/capability maps, and an advertised endpoint. Its
`ready` property also checks the current readiness lease; a persisted state
string of `READY` alone is insufficient. Its `terminal` property covers
`FAILED`, `STOPPED`, and `CANCELLED`.

Supply the expected generation and plan hash from the intended submission:

```python
from exaserve.plan.io import load_deployment_plan
from exaserve.status_api import require_ready_endpoint


def endpoint_for_run(run_dir: str, generation: int, plan_path: str) -> str:
    plan = load_deployment_plan(plan_path)
    return require_ready_endpoint(
        run_dir,
        expected_generation=generation,
        expected_plan_hash=plan.deployment_plan_hash,
    )
```

`DeploymentNotReady` reports missing, stale, mismatched, or non-ready state.
`InvalidDeploymentStatus` reports invalid status artifacts; underlying I/O and
validation failures may also propagate. Do not recover from these failures by
guessing a URL. `DeploymentStatusPublisher` belongs to the composition root;
clients must not publish or repair status themselves.

## Inference HTTP surface

The serving application in [`server.py`](../src/exaserve/server.py) defines the
following paths relative to its model route:

| Method and path | Purpose |
| --- | --- |
| `GET /v1/models` | List the model served by the selected application |
| `POST /v1/completions` | Generate a completion from a prompt |
| `POST /v1/chat/completions` | Generate a response from chat messages |
| `GET /health` | Application-local health response, not canonical deployment READY |
| `GET /metrics` | ExaServe-owned operational metrics |
| `GET /stats` | Replica/backend diagnostics |

Use the canonical advertised endpoint, not a direct Serve port. Single-model
HAProxy deployments accept the root API route. Multi-model requests need the
compiled model route prefix from the plan/status model map; HAProxy does not
route by the JSON `model` field. Do not construct internal replica-specific
routes yourself.

These paths describe implemented interfaces, not blanket OpenAI API parity or
qualification of every request option. The selected site/scale envelope and
workload policy govern admission; the default production candidate is
non-streaming and allocation-internal. Streaming and other validation-only
dimensions require their own authorized qualification work. See
[the request example](usage.md#send-a-non-streaming-request) and
[`request_validation.py`](../src/exaserve/request_validation.py) for input checks.

## Installed command reference

Entry points are declared in [pyproject.toml](../pyproject.toml) and dispatch
through [`exaserve.cli`](../src/exaserve/cli.py).

| Command | Purpose and boundary |
| --- | --- |
| `exaserve-serve-submit CONFIG` | Validate, materialize, and idempotently submit one deployment intent; `--dry-run` avoids scheduler submission |
| `exaserve-serve-url JOB_ID_OR_FILE` | Resolve only the matching READY generation's advertised endpoint; `--wait` enables bounded polling |
| `exaserve-status show --run-dir DIR` | Inspect canonical status; `--json` emits the typed record as JSON |
| `exaserve-status wait --run-dir DIR --generation N --plan-hash SHA256` | Wait for an exact generation and plan; supports `--binding-hash`, `--timeout`, `--poll`, and `--json` |
| `exaserve-launch-cluster PLAN_OR_CONFIG` | Enter the canonical runtime composition root inside an approved allocation |
| `exaserve-driver PLAN_OR_CONFIG` | Deprecated one-way alias for the canonical launcher |
| `exaserve-model-bcast` | Native model-staging boundary; not an alternative serving lifecycle |
| `exaserve-ray-start` | Supervised Ray-daemon boundary; not an independent deployment entry point |

`exaserve-status` returns `0` for current READY, `2` for absent/non-ready or
terminal status, `3` for wait timeout, and `4` for invalid or mismatched status.
Argument parsing errors also use the parser's error exit code. `wait` requires
both generation and plan hash, and positive timeout/poll values. See
[`status_cli.py`](../src/exaserve/status_cli.py) for the exact contract.

For submission options, use `exaserve-serve-submit --help`; for status options,
use `exaserve-status --help`. Runtime boundary commands must not be invoked
outside their required allocation and supervision context.

## Repository-local control planes

`python3 -m eval.cli` exposes `spec`, `trace`, `run`, and `allocation` commands;
[`eval/cli.py`](../eval/cli.py) defines the parser. `run submit-all` requires an
explicit `--run-group`. See [usage](usage.md) and [eval design](../eval/DESIGN.md).

`python3 -m clientlab` exposes `plan`, `run`, `report`, `compare`, and `smoke`;
[`clientlab/cli.py`](../clientlab/cli.py) defines the parser. See
[ClientLab](../clientlab/README.md) for study specifications and artifacts.
Neither source-tree module is included in the installed ExaServe wheel.
