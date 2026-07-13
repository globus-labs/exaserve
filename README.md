# ExaServe

Ray Serve helpers, launch scripts, and scale patches for scaling OpenAI-compatible
LLM inference on HPC clusters: a pluggable inference engine (vLLM/SGLang, selected
by `EXASERVE_ENGINE`) behind one endpoint, with a pluggable front-end proxy
(HAProxy/LiteLLM), over batch-scheduler allocations. Currently targets the ALCF
Aurora HPC cluster (PBS, Intel XPU); Slurm and other schedulers/vendors are on the
roadmap (see [`doc/design/`](doc/design/)). Distributed as the `exaserve` package
(`import exaserve`).

## Install

The package targets Aurora's frameworks Python (3.12).

```bash
module load frameworks
python3 -m pip install --user .
```

Aurora's `frameworks` module pins `pip install --user` to a frameworks-
versioned user base (so packages installed against one frameworks
version don't shadow another). Concretely, the console scripts land
in `~/.local/aurora/frameworks/<version>/bin/` — not `~/.local/bin/`.
`pip install --user` will print a warning showing the exact directory:

```
WARNING: The scripts exaserve-driver, ... are installed in
'/home/<your-user>/.local/aurora/frameworks/2025.3.1/bin'
```

Add that directory to `PATH`. Use the dynamic form so it keeps working
when Aurora bumps the frameworks version:

```bash
module load frameworks
SCRIPTS_DIR="$(python3 -c 'import sysconfig; print(sysconfig.get_path("scripts", "posix_user"))')"
export PATH="$SCRIPTS_DIR:$PATH"

# Persist across logins (after `module load frameworks` so the right
# scripts dir is picked up):
cat >> ~/.bashrc <<'BASH'
if module is-loaded frameworks 2>/dev/null; then
    SCRIPTS_DIR="$(python3 -c 'import sysconfig; print(sysconfig.get_path("scripts", "posix_user"))')"
    case ":$PATH:" in *":$SCRIPTS_DIR:"*) ;; *) export PATH="$SCRIPTS_DIR:$PATH" ;; esac
fi
BASH
```

Verify with:

```bash
which exaserve-serve-submit
# /home/<your-user>/.local/aurora/frameworks/2025.3.1/bin/exaserve-serve-submit
```

For an isolated install, use `--target` and prepend that location to
`PYTHONPATH` and `PATH`:

```bash
python3 -m pip install --target ~/exaserve_install --no-deps .
export PATH="$HOME/exaserve_install/bin:$PATH"
export PYTHONPATH="$HOME/exaserve_install:$PYTHONPATH"
```

`ray`, `vllm`, MPI, and PBS come from the Aurora frameworks module rather
than from `pip install`. The optional extras under
`[project.optional-dependencies]` (`server`, `proxy`, `dev`) exist to
document the intended dependency groups but are not a substitute for the
frameworks module.

**HAProxy and LiteLLM need separate one-time setup before you can run a
deployment that uses them:**

- **HAProxy** is not on Aurora out of the box and is not in the frameworks
  module. If your config has `proxy_config.type: haproxy`, build it with
  `bash scripts/build_haproxy.sh` (see [One-time setup](#one-time-setup)).
- **LiteLLM** has dependency versions that conflict with Ray + vLLM, so it
  needs its own venv. If your config has `proxy_config.type: litellm`,
  follow the venv recipe in [One-time setup](#one-time-setup) and point
  `proxy_config.python_path` at it.

If you only use `proxy_config.type: direct` (no proxy; clients hit each
node's Ray Serve HTTP on port 8000), you don't need either of these.

## Quickstart

End-to-end from a clean Aurora login node, using the bundled HAProxy
example. Run these on a login node — `exaserve-serve-submit` does not
need an allocation, only `qsub`.

```bash
# 1. Install the package against Aurora's Python.
module load frameworks
python3 -m pip install --user .

# 2. Point a copy of the example at your Lustre model dir. Open
#    my_config.yaml and change `model_storage_path:` to a Lustre path
#    you can write to. That's typically the only field you need to
#    edit; everything else has reasonable defaults.
cp examples/config.haproxy.yaml my_config.yaml
$EDITOR my_config.yaml   # or vi / nano

# 3. Submit. --wait blocks until PBS reports the job is running and
#    then prints the service URL. Without --wait, you only get the
#    job id back; use exaserve-serve-url <jobid> later to resolve it.
exaserve-serve-submit my_config.yaml \
    --project-account YOUR_PROJECT \
    --queue debug-scaling \
    --walltime 01:00:00 \
    --wait
# 8470123.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov
# http://x4310c1s0b0n0:4001

# 4. Hit the service from the same login node (or anywhere on the HSN
#    that can reach the head node).
URL="http://x4310c1s0b0n0:4001"   # printed by step 3
curl -sS -X POST "$URL/v1/chat/completions" \
     -H 'Content-Type: application/json' \
     -d '{
       "model": "meta-llama/Meta-Llama-3-8B-Instruct",
       "messages": [{"role":"user","content":"hello"}],
       "max_tokens": 8
     }'
# {"id":"chatcmpl-...","choices":[{"message":{"role":"assistant","content":"Hi!"}}], ...}

# 5. Tear it down when you're done. The job runs until walltime
#    expires; qdel ends it early.
qdel 8470123
```

Defaults if you don't pass the flags / set the env vars:
project=`AuroraGPT`, queue=`debug-scaling`, walltime=`01:00:00`,
filesystems=`home:flare`. See [examples/config.reference.yaml](examples/config.reference.yaml)
for the full set of submit-time env vars.

## One-time setup

### HAProxy (only for `proxy_config.type: haproxy`)

Aurora doesn't ship a recent `haproxy`, and the Aurora frameworks module
doesn't include one. Build it once from source:

```bash
bash scripts/build_haproxy.sh           # installs to ~/.local/haproxy-<version>,
                                        # symlinks ~/bin/haproxy
```

The launcher prepends `$HOME/bin` to `PATH` so the symlinked binary is
picked up automatically inside the PBS job. The default version is HAProxy
`3.1.6`; pass a different version as the script's first arg if you need
one.

If you want to skip this step, use `proxy_config.type: direct` (clients
hit each node's Ray Serve HTTP directly on port 8000) or
`proxy_config.type: litellm` (one-time setup below).

### LiteLLM (only for `proxy_config.type: litellm`)

LiteLLM pins dependency versions that conflict with Ray and vLLM, so it
needs its own venv. Create one and point `proxy_config.python_path` at
the venv's `python3`. The release branch shipped a
`scripts/setup_litellm_venv.sh` helper; reproduce it manually if you need
LiteLLM in v0.1.0:

```bash
module load frameworks
python3 -m venv ~/litellm_venv
source ~/litellm_venv/bin/activate
pip install 'litellm[proxy]'
deactivate
```

Then in your config:

```yaml
proxy_config:
  type: litellm
  python_path: /home/<your-user>/litellm_venv/bin/python3
  options:
    master_key: "sk-yourtoken"
```

## Required environment

A login shell with `module load frameworks` is enough to import the package
and call `exaserve-serve-submit`. To launch or run a cluster you also need:

- **Inside a PBS allocation** (interactive: `qsub -I ...`; batch: a `.pbs`
  script). `exaserve-launch-cluster` reads `$PBS_NODEFILE` and refuses to run
  outside an allocation.
- **`module load frameworks`** — provides Aurora's Python 3.12, vLLM, Ray,
  MPI (`mpiexec`, `mpicc`), `oneAPI` SDK, `level_zero` device selectors.
  The `exaserve-launch-cluster` PBS script generated by `exaserve-serve-submit`
  loads this for you if `$HOME/script/env_aurora` is missing.
- **`mpicc` on `PATH`** — required only at first boot, to compile the
  bundled `bcast.c` MPI helper. `module load frameworks` provides it.
- **Optional ALCF web proxy** — set these only if your job needs outbound
  HTTPS (e.g. downloading a model from HuggingFace at first boot). Models
  already cached under `model_storage_path` don't need internet:
  ```bash
  export HTTP_PROXY=http://proxy.alcf.anl.gov:3128
  export HTTPS_PROXY=http://proxy.alcf.anl.gov:3128
  export http_proxy=$HTTP_PROXY
  export https_proxy=$HTTPS_PROXY
  export no_proxy="*.alcf.anl.gov,127.0.0.1,localhost"
  ```
- **Project-specific paths** in your YAML config (model storage on Lustre,
  the per-node `local_stage_path` to copy weights into, etc.). See
  *Configuration* below.

You do **not** need `module load go`, a Go toolchain, or any research-side
helpers (`eval/`, `clientlab/`, `benchmarks/`). Those live in the source
tree but never enter the wheel.

## Console scripts

The wheel installs seven `aurora-*` console scripts. They split into three
groups: high-level user entry points, internal subprocesses, and a couple
of utilities.

### High-level (what users run)

#### `exaserve-launch-cluster <config.yaml>`

Foreground launcher. Brings up Ray + inference-engine replicas (vLLM or SGLang)
+ (optionally) HAProxy on whatever nodes are in `$PBS_NODEFILE`, blocks until
shutdown, then finalizes logs. Run this **inside an interactive PBS allocation**:

```bash
qsub -I -l select=2,walltime=01:00:00 -A AuroraGPT -q debug-scaling -l filesystems=home:flare
# … inside the interactive shell …
module load frameworks
exaserve-launch-cluster path/to/config.yaml
```

Once it prints `[Driver] ALL SERVICES READY`, point your client at
`http://<head_node>:<proxy_port>/v1/chat/completions` (the proxy port is
whatever you set in the config; default 4001 with HAProxy).

#### `exaserve-serve-submit <config.yaml>`

Non-interactive batch submission. Reads `num_nodes` from the config,
generates a one-shot PBS script that calls `exaserve-launch-cluster`,
`qsub`s it, prints the job id, and writes the id to `<config>.jobid`:

```bash
$ exaserve-serve-submit path/to/config.yaml
8470004.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov
```

Site values come from env vars with defaults; override either way:

| Env var | Default | CLI flag |
|---|---|---|
| `EXASERVE_PROJECT_ACCOUNT`    | `AuroraGPT`     | `--project-account` |
| `EXASERVE_DEFAULT_QUEUE`      | `debug-scaling` | `--queue`           |
| `EXASERVE_DEFAULT_WALLTIME`   | `01:00:00`      | `--walltime`        |
| `EXASERVE_DEFAULT_FILESYSTEMS`| `home:flare`    | (no flag — env var) |

`--wait` makes the command poll until the job is running and then print
the service URL on a second line:

```bash
$ exaserve-serve-submit path/to/config.yaml --wait
8470004.aurora-pbs-0001.hostmgmt.cm.aurora.alcf.anl.gov
http://x4310c1s0b0n0:4001
```

`--dry-run` writes the `.pbs` to the log dir without `qsub`-ing — useful
for inspecting what would be submitted.

#### `exaserve-serve-url <jobid_or_jobid_file>`

Resolves a PBS job id to a service URL. Polls `qstat -f` for `exec_host`,
parses the head node, and prints `http://<head>:<proxy_port>`:

```bash
$ exaserve-serve-url 8470004
http://x4310c1s0b0n0:4001

$ exaserve-serve-url path/to/config.yaml.jobid --wait --config path/to/config.yaml
http://x4310c1s0b0n0:4001
```

`--wait` keeps polling while the job is queued; without it, the command
fails immediately if the job isn't running yet. `--port` overrides the
proxy port; otherwise it reads `proxy_config.port` from `--config` (4001
if neither is provided).

### Utilities

#### `exaserve-model-bcast --config <config.yaml> --num-nodes <N>`

Standalone MPI model-staging helper. Reads the model list from a config,
pre-builds the bundled `bcast.c` helper into a writable dir, then
broadcasts model directories from `model_storage_path` (Lustre) to
`local_stage_path` (`/tmp` on each node) with a one-source-many-sinks
MPI broadcast. Run this on the head of an allocation if you want to
prime the local cache without spinning up Ray:

```bash
exaserve-model-bcast --config my_config.yaml --num-nodes 4
```

The build directory for the C helper is `$EXASERVE_BCAST_BUILD_DIR` if set,
else `$EXASERVE_RUN_LOG_DIR/bcast_build`, else `$PWD/.exaserve_build/bcast`.

### Internal (don't normally run by hand)

#### `exaserve-driver --config <runtime_yaml>`

Per-rank entry point invoked by `mpiexec` inside `exaserve-launch-cluster`.
Rank 0 starts the Ray head, spawns `exaserve-server` as a subprocess,
optionally starts the proxy, and waits. Other ranks start a Ray worker
and block. You normally don't run this directly.

#### `exaserve-server --config <runtime_yaml>`

Hosts the FastAPI app on the Ray head. Spawned as a subprocess by
`exaserve-driver`; its readiness marker is what `exaserve-launch-cluster`
keys off when announcing `ALL SERVICES READY`.

#### `exaserve-ray-start [head|worker] [...]`

Thin wrapper around `ray start` with Aurora-specific defaults, used by
`exaserve-driver` to bring up the Ray cluster on each node.

## Configuration

The `examples/` directory has four ready-to-customize deployment templates:

| File | Use |
|---|---|
| [examples/config.direct.yaml](examples/config.direct.yaml)       | No proxy. Each node serves its own Ray Serve HTTP on port 8000; clients pick a node themselves. Best for benchmarking the raw Ray Serve path. |
| [examples/config.haproxy.yaml](examples/config.haproxy.yaml)     | HAProxy on the head load-balances across every Ray Serve HTTP proxy. Pure L7 routing on port 4001. Best performance baseline. |
| [examples/config.litellm.yaml](examples/config.litellm.yaml)     | LiteLLM on the head exposes an OpenAI-compatible API with auth, rate limiting, usage tracking. Needs a separate LiteLLM venv (its deps conflict with Ray/vLLM). |
| [examples/config.reference.yaml](examples/config.reference.yaml) | Comprehensive list of every field the schema accepts, with defaults and per-knob commentary. Not meant to run as-is — copy the fields you need. |

To run one of the templates, edit `model_storage_path` to point at your own
Lustre dir, then:

```bash
exaserve-serve-submit examples/config.haproxy.yaml --wait
```

**Custom PBS scripting.** If you need to integrate the launcher into your own
PBS pipeline (chained jobs, reservation queues, custom prologue, dependency
chains), use `exaserve-serve-submit --dry-run` to get a starting template:

```bash
exaserve-serve-submit examples/config.haproxy.yaml --dry-run --log-dir ./my-pbs
cat ./my-pbs/config.haproxy.pbs    # edit this, then qsub it directly
```

That way your custom script starts from exactly the PBS template the
package itself generates, instead of hand-syncing a separate example.

The deployment YAML has three top-level sections:

```yaml
ray_cluster_config:
  head_ip: ""        # left empty; the launcher fills it in from $HOSTNAME at runtime
  port: 6379
  node_cpus: 64

model_deployment_config:
  num_nodes: 2
  model_storage_path: /lus/flare/projects/AuroraGPT/<your-user>/models
  local_stage_path: /tmp/hf_home
  deployment_name: my_serve
  replica_max_ongoing_requests: 128
  num_gpus_per_node: 12
  model_configs:
    - model_id: meta-llama/Meta-Llama-3-8B-Instruct
      tensor_parallel_size: 1
      pipeline_parallel_size: 1
      max_model_len: 4096
      size: 8                   # billions of params, used for replica planning
      gpu_memory_utilization: 0.90
      enforce_eager: true
      max_num_seqs: 64

proxy_config:
  type: haproxy                 # haproxy | litellm | none
  port: 4001                    # client-facing port
  backend_port: 8000             # Ray Serve HTTP port on each node
  num_workers: 1
  options:
    balance: leastconn
    maxconn: 50000
```

`model_storage_path` and `local_stage_path` are the only fields that
typically vary per user. The rest can usually be reused as-is.

## Patches

Aurora's Ray and vLLM need a few site-specific patches at scale (HTTP
proxy timeouts at 256+ nodes, vLLM PP layer-filter on Aurora XPU, Ray
oneAPI accelerator context). They live in `exaserve.patches`
and are applied by `apply_all()`. The launcher and `exaserve-driver` call
it at the right moment in the lifecycle. If you embed the package in
your own code, call it explicitly **before** `import vllm` or
`import ray.serve`:

```python
import exaserve
exaserve.apply_all()
import vllm   # safe to import now
```

The patches docstring (`exaserve/patches/__init__.py`) explains
why activation has to be explicit (Ray instantiates `ray.serve._private`
classes during package import; monkey-patching after the fact is too late).

## Testing

```bash
module load frameworks
python3 -m pytest tests/
```

Three unit tests come with the wheel:

- `tests/test_packaging.py` — verifies the four resource files
  (`launch_cluster.sh`, `distribute_to_nodes.sh`, `bcast.c`,
  `bcast.Makefile`) ship in the wheel and that bcast sources can
  materialize to a writable build dir.
- `tests/test_submit.py` — `exaserve-serve-submit --dry-run` produces a
  PBS script with the expected env-aurora source, `EXASERVE_*`
  exports, direct `bash` invocation, and no console-script PATH dependency.
- `tests/test_driver.py`, `test_haproxy_proxy.py`, `test_replica_planner.py`
  — driver/proxy/planner unit tests carried over from the release branch.

End-to-end Aurora verification (running a real cluster against a real
config) is in `plan/production_packaging.md`, §9 verification matrix.
