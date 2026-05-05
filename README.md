# aurora-rayserver

Ray Serve helpers, Aurora-specific launch scripts, and scale patches for vLLM
deployments on the ALCF Aurora HPC cluster.

## Install

Use Aurora's frameworks Python, not the system Python on login nodes:

```bash
module load frameworks
python3 -m pip install .
```

The package installs console scripts including:

- `aurora-launch-cluster`
- `aurora-driver`
- `aurora-server`
- `aurora-model-bcast`
- `aurora-ray-start`
- `aurora-serve-submit`
- `aurora-serve-url`

`ray`, `vllm`, MPI, PBS, HAProxy, and LiteLLM are expected to come from the
Aurora runtime environment or site-managed virtual environments. Optional
Python extras document the intended dependency groups but are not a substitute
for Aurora's frameworks module.

## Running

Do not run deployments directly on a login node. Start or reuse an interactive
PBS allocation first, source the Aurora runtime environment, then launch:

```bash
source ~/script/env_aurora
aurora-launch-cluster path/to/config.yaml
```

For ad-hoc batch submission:

```bash
aurora-serve-submit path/to/config.yaml --wait
```

The submit helper writes a PBS script, submits it with `qsub`, prints the job
id, and with `--wait` also prints the service URL once PBS reports the job is
running.
