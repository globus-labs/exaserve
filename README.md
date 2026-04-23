# aurora_rayserver

Launch a Ray cluster, deploy Ray Serve with vLLM, and (optionally) put
HAProxy or LiteLLM in front — on Aurora. One config file, one `qsub`.

## Table of contents

1. [What you get](#what-you-get)
2. [One-time setup](#one-time-setup)
   - [Environment](#environment)
   - [HAProxy (only for `type: haproxy`)](#haproxy-only-for-type-haproxy)
   - [LiteLLM (only for `type: litellm`)](#litellm-only-for-type-litellm)
3. [Quick start](#quick-start)
4. [Config reference](#config-reference)
5. [What the launcher modifies outside the repo](#what-the-launcher-modifies-outside-the-repo)
6. [Repo layout](#repo-layout)
7. [Troubleshooting](#troubleshooting)

## What you get

Pick a proxy mode up front; the launcher handles the rest.

| mode      | entry point                                | best for                                    |
|-----------|--------------------------------------------|---------------------------------------------|
| `direct`  | `http://<node>:8000/v1/...`                | raw Ray Serve throughput, single-node tests |
| `haproxy` | `http://<head>:4001/v1/...`                | L7 load balancing with no auth overhead     |
| `litellm` | `http://<head>:4001/v1/...` + bearer token | API keys, rate limits, usage tracking       |

### direct

```text
  client
    |  (client picks which node; no LB)
    v
  node[i]:8000  --->  Ray Serve HTTP proxy  --->  vLLM replicas on that node
                      (every node has its own :8000, all equivalent)
```

### haproxy

```text
  client
    |
    v
  head:4001  HAProxy  (L7, balance=leastconn)
    |
    +--->  head :8000   --->  Ray Serve  --->  vLLM replicas
    +--->  node1:8000   --->  Ray Serve  --->  vLLM replicas
    +--->  node2:8000   --->  Ray Serve  --->  vLLM replicas
    +--->  ...
```

### litellm

```text
  client  -- Authorization: Bearer <master_key> -->
    |
    v
  head:4001  LiteLLM (uvicorn: auth + rate limit + usage tracking)
    |  (router picks backend per routing_strategy)
    +--->  head :8000   --->  Ray Serve  --->  vLLM replicas
    +--->  node1:8000   --->  Ray Serve  --->  vLLM replicas
    +--->  node2:8000   --->  Ray Serve  --->  vLLM replicas
    +--->  ...
```

## One-time setup

### Environment

Loaded automatically by `examples/launch.pbs`:
```bash
source scripts/env_aurora.sh   # module load frameworks + go, ALCF proxy vars
```
The `frameworks` module provides Python, Ray, vLLM, FastAPI, and PyYAML.
You do not pip-install the core stack.

### HAProxy (only for `type: haproxy`)

Aurora doesn't ship a recent haproxy; build one:
```bash
bash scripts/build_haproxy.sh           # installs to ~/bin/haproxy
```
The launcher prepends `$HOME/bin` to `PATH` so this is picked up automatically.

### LiteLLM (only for `type: litellm`)

LiteLLM needs its own venv (conflicts with vLLM's pinned deps):
```bash
bash scripts/setup_litellm_venv.sh      # creates $HOME/litellm_venv
```
Then set `proxy_config.python_path` in your config to the venv's python3.

## Quick start

```bash
# 1. Clone and enter the repo on an Aurora login node.
cd ~/aurora_rayserver

# 2. Copy one of the example configs and edit model_storage_path + num_nodes.
cp examples/config.direct.yaml my_config.yaml
$EDITOR my_config.yaml

# 3. Submit. All PBS knobs live on the qsub command line; launch.pbs stays
#    parameter-free so you can reuse it across queues.
qsub -l select=1 \
     -l walltime=01:00:00 \
     -l filesystems=home:flare \
     -q debug \
     -A YOUR_PROJECT \
     -N ray-serve \
     -o $HOME/tmp/ray-serve.stdout \
     -e $HOME/tmp/ray-serve.stderr \
     -v CONFIG=my_config.yaml \
     examples/launch.pbs
```

The cluster stays up for the PBS walltime. Find the head node from
`qstat -xf <job>` and send requests to it.

## Config reference

All three modes share `ray_cluster_config` and `model_deployment_config`;
they differ only in `proxy_config`. See:

- [examples/config.direct.yaml](examples/config.direct.yaml) — no proxy
- [examples/config.haproxy.yaml](examples/config.haproxy.yaml) — HAProxy on head
- [examples/config.litellm.yaml](examples/config.litellm.yaml) — LiteLLM on head
- [examples/config.reference.yaml](examples/config.reference.yaml) — every tunable knob with schema defaults + inline docs

Authoritative schema is [src/schemas.py](src/schemas.py).

Key things to set per run:
- `model_deployment_config.num_nodes` — must match `qsub -l select=N`
- `model_deployment_config.model_storage_path` — where weights live on Lustre
- `model_deployment_config.model_configs[].model_id` — any HF model id
- `ray_cluster_config.head_ip` — leave empty; auto-populated by the launcher

## What the launcher modifies outside the repo

To make Ray Serve survive at 128+ nodes, `scripts/launch_cluster.sh` writes
an import-hook `sitecustomize.py` into your **frameworks Python user
site-packages** (e.g. `~/.local/aurora/frameworks/2025.3.1/lib/python3.12/site-packages/sitecustomize.py`).
Python auto-loads this at every interpreter startup, so every `python3` run
under the frameworks module afterwards sees it — not just during the job.

The hook rewrites Ray Serve's hardcoded health-check constants when the
modules are imported (because several submodules copy the values in with
`from .constants import …`, a plain attribute-set in one place isn't
enough):

| constant | default | patched |
|---|---|---|
| `HTTP_PROXY_TIMEOUT` | 60 | 3600 |
| `PROXY_HEALTH_CHECK_TIMEOUT_S` | 10 | 300 |
| `PROXY_HEALTH_CHECK_UNHEALTHY_THRESHOLD` | 3 | 100 |
| `DEFAULT_HEALTH_CHECK_TIMEOUT_S` | 30 | 600 |
| `DEFAULT_HEALTH_CHECK_PERIOD_S` | 10 | 120 |
| `REPLICA_HEALTH_CHECK_UNHEALTHY_THRESHOLD` | 3 | 100 |

The file **persists after the job exits**. To revert to stock Ray Serve
timeouts, delete it:
```bash
rm "$(python3 -m site --user-site)/sitecustomize.py"
```

(This is distinct from [src/sitecustomize.py](src/sitecustomize.py), which
lives on `PYTHONPATH` and carries vLLM PP / XPU patches for this project.)

## Repo layout

```
examples/   example configs and PBS script (what users edit)
scripts/    launch_cluster.sh + env/build helpers
src/        Ray Serve driver, vLLM deployment, proxy backends
tools/      MPI broadcast helper (built on first run)
tests/      unit tests
```

## Troubleshooting

- **`[System] ERROR: haproxy: command not found`** — run `scripts/build_haproxy.sh`.
- **`[LiteLLMProxy] ... no such file`** — `proxy_config.python_path` must
  point at an existing venv with LiteLLM installed.
- **Job hangs before `ALL SERVICES READY`** — check `run_logs/<stamp>/launch.log`.
  Most common: models not present at `model_storage_path` and HuggingFace
  download stalled behind the ALCF proxy.
- **Port 4001 in use** — the proxy falls back to an OS-assigned port;
  check `run_logs/<stamp>/proxy_out/proxy_port`.
