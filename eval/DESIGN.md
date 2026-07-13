# Eval Control Plane — Design Document

## Motivation

The original eval path consisted of three tightly-coupled scripts that mixed
configuration, generation, and execution logic in a single imperative flow:

| Old script | Responsibility |
|---|---|
| `exp_configs.py` | Hardcoded experiment parameters as Python dataclasses; built `ExpConfig` objects imperatively with nested loops for weak-scaling sweeps. |
| `exp_generator.py` | Generated trace files + PBS job scripts by calling into `exp_configs.py` and rendering shell templates. |
| `submit_all.py` | Walked a directory tree of generated experiments, called `qsub` on each. |
| `trace_generator.py` | Standalone script for generating trace JSONL files, duplicating logic from `exp_generator.py`. |
| `replay_client.py` | ~1200-line monolith handling trace loading, Go process orchestration, MPI fanout, and result aggregation. |

Problems with this design:

1. **No single source of truth** — experiment parameters were scattered across
   Python dataclass constructors, shell template variables, and YAML configs,
   making it hard to know which values were actually used.
2. **Duplicated logic** — model dict parsing, trace generation, and config
   construction were copy-pasted between `exp_configs.py`, `exp_generator.py`,
   and `trace_generator.py`.
3. **No reproducibility** — traces were regenerated on every run even when
   the workload parameters hadn't changed.
4. **Untestable** — the scripts relied on module-level constants from
   `site_config` and PBS environment variables, making unit testing difficult
   without a live HPC environment.
5. **Rigid sweep model** — adding a new sweep axis (e.g., varying proxy
   workers) required editing Python code rather than declaring it in config.

## Architecture After Refactor

```
eval/
├── cli.py                     # Unified CLI entry point (python -m eval.cli)
├── specs/                     # Declarative experiment definitions (YAML)
│   ├── weak_scaling.yaml
│   ├── null_compute_litellm.yaml
│   └── ...
├── lib/                       # Shared library (the "control plane")
│   ├── models.py              # Pure data: all dataclasses, no behavior
│   ├── catalog.py             # Spec discovery: find_spec_path, list_spec_names
│   ├── spec_io.py             # Load + validate + normalize spec YAML
│   ├── matrix.py              # Cartesian product expansion of sweep axes
│   ├── trace_store.py         # Content-addressed trace caching layer
│   ├── trace_generators.py    # Trace row generation (weak_scaling, azure_trace)
│   ├── run_planner.py         # Materialize spec -> run bundles on disk
│   ├── run_executor.py        # Execute a materialized run (backend lifecycle)
│   ├── utils.py               # YAML/JSON I/O, hashing, dotted-path access
│   ├── backends/              # Pluggable backend adapters
│   │   ├── base.py            # Abstract BackendAdapter + ProcessMonitor
│   │   ├── ray.py             # Real Ray Serve backend
│   │   └── mock.py            # No-op backend for testing
│   └── schedulers/
│       └── pbs.py             # PBS job rendering + queue defaults
├── templates/
│   └── job.pbs.tmpl           # PBS job template (now just a reference; rendered in code)
├── replay_client.py           # Replay engine (Go process orchestration + MPI)
├── exp_configs.py             # Legacy shim (DeprecationWarning)
├── exp_generator.py           # Legacy shim (DeprecationWarning)
├── submit_all.py              # Legacy shim (DeprecationWarning)
└── trace_generator.py         # Legacy shim (DeprecationWarning)
```

## Pipeline Phases

The eval pipeline is split into four distinct phases that can run independently:

### Phase 1: Spec Loading (`spec_io.py`)

```
spec YAML file  ──→  load_experiment_spec()  ──→  ExperimentSpec dataclass
```

- Parses the YAML into typed dataclass fields.
- Fills site-specific defaults (model storage paths, prompt datasets) from
  `get_site_config()` lazily — no module-level constants.
- Validates structural constraints (required fields, value ranges, supported
  enum values for trace kind, backend, client dest).
- Normalizes derived fields (e.g., `client.num_nodes` defaults to
  `deployment.num_nodes` when not explicitly set).

### Phase 2: Matrix Expansion (`matrix.py`)

```
ExperimentSpec  ──→  expand_matrix()  ──→  list[VariantSpec]
```

- Computes the Cartesian product of all matrix axes.
- For each point in the product, deep-copies the spec and injects axis values
  into the targeted dotted paths (e.g., `deployment.num_nodes`).
- Auto-syncs `client.num_nodes` and `scheduler.nodes` to `deployment.num_nodes`
  unless those fields are themselves targeted by a matrix axis.
- Generates a human-readable variant name from the axis label templates.

Example spec matrix:
```yaml
matrix:
  name_template: "{num_nodes}_nodes"
  axes:
    - name: num_nodes
      values: [1, 2, 4, 8]
      targets: [deployment.num_nodes, client.num_nodes, scheduler.nodes]
```
This produces 4 variants: `1_nodes`, `2_nodes`, `4_nodes`, `8_nodes`.

### Phase 3: Materialization (`run_planner.py` + `trace_store.py`)

```
list[VariantSpec]  ──→  materialize_run_bundles()  ──→  list[RunPlan]
                                                          (+ files on disk)
```

For each variant:

1. **Trace generation** — `trace_store.py` computes a stable hash of the
   "trace identity" (workload params + deployment topology + trace kind) and
   checks if a cached trace exists. If not, it delegates to
   `trace_generators.py` to generate rows and writes them to the
   content-addressed store. Two variants that differ only in scheduler settings
   share the same cached trace.

2. **Run bundle creation** — creates a directory tree:
   ```
   runs/<spec_name>/<run_id>/
   ├── run.yaml               # Self-contained RunPlan snapshot
   ├── job/job.pbs            # PBS job script
   ├── meta/spec_snapshot.yaml
   ├── runtime/<backend>_runtime.yaml  # Backend-specific manifest
   ├── logs/
   ├── results/
   └── state/status.json
   ```

3. **Backend validation + manifest** — asks the backend adapter to validate
   the RunPlan and build a runtime manifest. For the `ray` backend, this
   translates the eval-layer RunPlan into the serving-layer `ExpConfig` YAML
   that `src/exaserve/resources/launch_cluster.sh` and `replay_client.py` understand.

4. **PBS job rendering** — renders a PBS job script that sources the env
   script and runs `python -m eval.cli run execute <run.yaml>`.

5. **State tracking** — writes `status.json` with `"materialized"` status.

### Phase 4: Execution (`run_executor.py`)

```
run.yaml  ──→  execute_run()  ──→  exit code
```

Called inside a PBS job (or locally for mock backend):

1. Loads the RunPlan from `run.yaml`.
2. Delegates to the backend adapter's lifecycle:
   - `launch()` → starts the serving cluster.
   - `wait_ready()` → blocks until the readiness marker appears in stdout.
   - `discover_targets()` → returns base URLs for the replay client.
3. Spawns `replay_client.py` against the discovered endpoints.
   - For multi-node client fanout (`client.dest == "direct"`), wraps the
     command in `mpiexec`.
4. Writes state transitions: `running` → `replaying` → `succeeded`/`failed`.
5. Stops the backend in a `finally` block.

## Backend Adapter Interface

```python
class BackendAdapter(ABC):
    def validate(run_plan) -> None
    def build_runtime_manifest(run_plan) -> str
    def runtime_env(run_plan) -> RuntimeEnvSpec
    def launch(run_ctx) -> LaunchedBackend
    def wait_ready(run_ctx, launched) -> None
    def discover_targets(run_ctx, launched) -> list[str]
    def stop(run_ctx, launched) -> None
```

The `ray` adapter bridges two config worlds: it translates the eval-layer
`RunPlan` into the serving-layer `ExpConfig` (defined in `src/schemas.py`)
which is what the existing `src/exaserve/resources/launch_cluster.sh`, `src/driver.py`,
and `replay_client.py` understand. This avoids rewriting the serving
infrastructure while giving the eval layer its own clean data model.

The `mock` adapter allows the full materialize→execute pipeline to be tested
without a real cluster — it immediately signals ready and returns a dummy URL.

## Trace Store Design

Trace artifacts are stored in a content-addressed layout:

```
traces/<sha256_prefix>/
├── trace.jsonl       # The generated trace (metadata header + request rows)
└── metadata.json     # Identity hash, row count, variant info
```

The identity hash is computed from the subset of spec fields that affect trace
content:
- `trace.kind`, `trace.input_prompt_path`, `trace.input_trace_path`
- `workload.duration`, `workload.input_len`, `workload.output_len`,
  `workload.rate_per_node`, `workload.speedup`, `workload.sampling_strategy`,
  `workload.seed`, `workload.modes`
- `deployment.num_nodes`, `deployment.model_storage_path`, `deployment.models[*]`

Fields like `scheduler.*`, `client.*`, and `backend.*` are excluded because
they don't affect trace content. This means a weak-scaling sweep with 11 node
counts but identical workload parameters generates only **one** trace per
unique (num_nodes, rate_per_node) pair, not 11 redundant copies.

## Spec YAML Schema

```yaml
name: <experiment_name>          # Required. Used in directory names and PBS job names.

matrix:                          # Optional. Omit for single-variant experiments.
  name_template: "{axis1}_{axis2}"
  axes:
    - name: <axis_name>
      values: [v1, v2, ...]
      targets: [dotted.path.to.field, ...]
      label_template: "{value}_suffix"

trace:
  kind: weak_scaling | azure_trace
  input_prompt_path: <path>      # Optional. Defaults from site_config.
  input_trace_path: <path>       # Required for azure_trace.

workload:
  duration: <seconds>            # Required.
  input_len: 2048
  output_len: 512
  rate_per_node: 80.0
  speedup: 1.0
  sampling_strategy: peak | sparse | random
  generation_mode: deterministic | natural
  seed: 42
  modes: {chat: 1, completion: 0}

deployment:
  num_nodes: <int>
  models:
    - model_id: <hf_model_id>
      tensor_parallel_size: 1
      pipeline_parallel_size: 1
      max_model_len: 4096
      size: 8                    # Used for weighted model assignment in azure traces.
      num_replicas: null         # null = auto-scale
      gpu_memory_utilization: 0.90
  model_storage_path: <path>     # Optional. Defaults from site_config.
  local_stage_path: <path>       # Optional. Defaults from site_config.
  replica_max_ongoing_requests: 64

client:
  num_runs: 3
  dest: proxy | direct
  num_go_procs: 16
  num_go_workers: 2
  go_concurrency: 40
  warmup_rps: 0
  warmup_duration_s: 0.0
  include_tp: false
  early_stop: 0.0               # Fraction of total requests (0 = no early stop).
  sum_only: false

backend:
  default: ray | mock
  args:
    ray:
      launch:
        null_compute: false
        env_script: <path>       # Optional override.
      proxy:
        type: litellm | haproxy | none
        port: 4001
        num_workers: 1

scheduler:
  type: pbs
  nodes: <int>                   # Defaults to deployment.num_nodes.
  queue: ""                      # Auto-selected by node count if empty.
  walltime: ""                   # Auto-selected by node count if empty.
  project: AuroraGPT
  filesystems: home:flare
```

## Legacy Compatibility

The old entry points (`exp_generator.py`, `submit_all.py`, `trace_generator.py`)
are preserved as thin shims that emit `DeprecationWarning` and delegate to the
new library. `exp_configs.py` retains backward-compatible constants and a
`WeakScalingExpParams` dataclass (deprecated) plus an `EXPERIMENT_REGISTRY` dict
that maps spec names to paths for any remaining callers.

The new canonical workflow:
```bash
# Materialize (generates traces + run bundles + PBS jobs)
python -m eval.cli run materialize weak_scaling

# Submit to PBS
python -m eval.cli run submit runs/weak_scaling/<run_id>/

# Or execute directly (for testing)
python -m eval.cli run execute runs/weak_scaling/<run_id>/run.yaml --dry-run
```

## Testing

`tests/test_eval_control_plane.py` validates the full pipeline using the mock
backend:
- Spec loading and matrix expansion
- Trace artifact caching and reuse (same identity → same trace_id)
- Run bundle materialization (directory tree, job.pbs, run.yaml, state.json)
- RunPlan round-trip (materialize → load_run_plan)
- Dry-run execution
- CLI subprocess validation (`eval.cli spec validate`, `eval.cli run submit --dry-run`)
- Absence of legacy import hacks (`sys.path.insert`, `from exp_configs import *`)
  in the runtime code paths
