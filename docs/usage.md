# Usage

Run examples from the repository root. Commands that start serving, replay,
benchmarks, or full tests on HPC require the
[environment and compute-session setup](getting_started.md#aurora-development-and-serving).
The canonical submission commands may prepare and
submit native PBS jobs from a login node; execution remains in the allocated
batch session, not on the login node.

## Configure a deployment

Start from [the HAProxy example](../examples/config.haproxy.yaml) and use
[the reference configuration](../examples/config.reference.yaml) for available
fields. Use a new local filename and update the model storage path, account,
topology, and resource choices to match the intended allocation.

The deployment input is a flat YAML mapping with model, gateway, and exposure
sections. Its compiler produces an immutable `DeploymentPlan`; runtime code
does not consume a second mutable configuration schema. The default production
candidate is the Aurora PBS / Intel XPU / vLLM / HAProxy,
trusted-allocation, non-streaming path. An implemented adapter or accepted
validation setting is not a production support claim.

Before submitting, review [the current status](../doc/hardening/STATUS.md),
including its supersession notices. The default profile has unresolved
production qualification gates. A dry run also validates admission and can
reject an unqualified configuration. Do not add `validation_mode: true` merely
to make an example run; validation needs explicit authorization and a declared
scope.

## Inspect and submit an admitted configuration

For an authorized validation configuration or a separately qualified production
configuration, render a submission into a new output location without calling
the scheduler:

```bash
exaserve-serve-submit my_config.yaml \
  --project-account YOUR_PROJECT \
  --log-dir ./submit-preview \
  --dry-run
```

`--dry-run` can create local plan/submission artifacts; it means no scheduler
submission, not no filesystem writes. Choose a fresh output directory.

Submit only after the relevant approval, environment, and input checks:

```bash
exaserve-serve-submit my_config.yaml \
  --project-account YOUR_PROJECT \
  --log-dir ./deployment-run \
  --wait
```

Queue and walltime defaults come from the compiled topology and site policy.
CLI options and the `EXASERVE_DEFAULT_QUEUE` / `EXASERVE_DEFAULT_WALLTIME`
environment variables do not override the support envelope or cluster rules.
Use the account authorized for the project.

Submission uses a durable intent identity. Repeating the same submitted intent
attaches to its recorded job. An ambiguous scheduler response is not permission
to submit a duplicate job. `--new-generation` deliberately creates a new
generation and is not a generic retry flag.

## Resolve readiness and diagnose a run

```bash
exaserve-serve-url JOB_ID --wait
exaserve-status show --run-dir /path/to/run --json
```

Use the exact job ID and run directory recorded by the submission. To wait for
a specific identity, supply its generation and compiled plan hash:

```bash
exaserve-status wait \
  --run-dir /path/to/run \
  --generation GENERATION \
  --plan-hash PLAN_SHA256 \
  --timeout 1800 \
  --json
```

Replace the uppercase placeholders with values from that submission; the
generation is an integer. `--binding-hash` can additionally constrain the
allocation binding. The CLI exit codes are documented in [the API reference](api.md).

Canonical `READY` binds the current generation, exact plan/allocation,
supervised components, Ray membership, Serve applications, compatibility
receipts, gateway, and endpoint canary. It is continuously revocable. PBS
`RUNNING`, a listening port, or a successful `/health` request alone is not
sufficient. Never choose an endpoint by grepping logs or selecting the newest
artifact directory.

Keep monitoring the run after readiness. Capture the exact failing command and
relevant errors, preserve outputs, and diagnose before retrying. For teardown,
operate on the exact scheduler job you intend to end; the composition root owns
bounded process cleanup and terminal status publication.

## Send a non-streaming request

For an admitted single-model HAProxy deployment, send requests from within the
authorized allocation network after canonical READY. Use the model ID from
that deployment and the endpoint returned for its exact job:

```bash
EXASERVE_ENDPOINT="$(exaserve-serve-url JOB_ID --wait)"
curl --fail --show-error --silent --max-time 120 \
  "${EXASERVE_ENDPOINT%/}/v1/completions" \
  -H 'Content-Type: application/json' \
  -d '{"model":"meta-llama/Meta-Llama-3-8B-Instruct","prompt":"Hello","max_tokens":16,"stream":false}'
```

Replace `JOB_ID` and the model ID with the recorded values. Only continue if
endpoint resolution succeeds. This example assumes the single-model root
route; multi-model HAProxy deployments require the compiled model route prefix.
HAProxy does not select a model by inspecting the JSON `model` field. See
[the HTTP surface](api.md#inference-http-surface) for routing and support limits.

## Evaluation runs

Eval and ClientLab are source-tree tools; the ExaServe wheel packages
`src/exaserve`, not the top-level `eval` or `clientlab` directories. Invoke them
from a checkout with the appropriate dependencies and site configuration.

Inspect an example specification without launching it:

```bash
python3 -m eval.cli spec validate eval/specs/smoke_haproxy_1node.yaml
```

The example is explicitly validation-only. First configure an
[operator-owned environment file](getting_started.md#configure-evaluation-environment-preparation).
Before materializing or running it,
review its model paths, site settings, resource requirements, output roots, and
authorization. See [site override examples](../eval/site_config_local.example.py)
and [the eval design](../eval/DESIGN.md).

Materialize an immutable bundle in the approved environment. Materialization
can generate traces and stage source artifacts, so nontrivial materialization
belongs on compute, even though small configuration-only validation may run on
the login node.

```bash
python3 -m eval.cli run materialize eval/specs/smoke_haproxy_1node.yaml
```

The command prints exact `run.yaml` paths. Select one explicitly for a scheduler
preview:

```bash
python3 -m eval.cli run submit /path/to/bundle/run.yaml --dry-run
```

Remove `--dry-run` only when ready to submit that authorized run. To submit a
bounded, idempotent group of already materialized runs:

```bash
python3 -m eval.cli run submit-all smoke_haproxy_1node --run-group run0 --dry-run
```

Replace `run0` with the exact materialized group. The CLI requires
`--run-group`; an implicit `latest` group is intentionally unsupported. Review
the preview before removing `--dry-run`. The generated scheduler job invokes
the canonical executor; do not reconstruct deployment lifecycle in a shell
wrapper.

The separate `eval.cli allocation` controller can pack logical child runs into
a finite physical allocation campaign. It currently has
[operator-specific bootstrap limitations](getting_started.md#current-bootstrap-limitations)
and does not honor the ordinary eval environment override. Its WP12 isolation proof and
qualification requirements still apply. Report physical allocation nodes and
logical child nodes separately; this is not a keepalive or fabricated lease.

## ClientLab studies

ClientLab measures the Go replay client with versioned study specifications:

```bash
python3 -m clientlab plan client_microbench
python3 -m clientlab report /path/to/completed-study
```

Planning and reporting may create artifacts. Keep prior study outputs and
select new destinations where applicable. Nontrivial `run` or `smoke` work
requires compute resources on Aurora, regardless of the name of a preset.
`--local` selects execution in the current environment; it does not authorize
execution on a login node.

Synthetic-target results are diagnostic-only. An ExaServe target must bind an
immutable RunPlan, trace, generation, and deployment status directory; a raw
endpoint is not sufficient. Read [ClientLab](../clientlab/README.md) and the
[retired-runner guidance](../clientlab/scripts/README.md) before a study.
