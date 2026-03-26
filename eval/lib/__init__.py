"""Shared library modules for the eval control plane.

This package implements the spec-driven eval pipeline that replaced the
old imperative approach (exp_configs.py + exp_generator.py + submit_all.py).

Key design principles:
  - Declarative: experiments are defined entirely in YAML spec files (eval/specs/).
  - Reproducible: trace artifacts are content-addressed and cached by identity hash.
  - Pluggable: backends (ray, mock) and schedulers (pbs) are behind adapter interfaces.
  - Phased: the pipeline is split into load -> expand -> materialize -> execute,
    where each phase can run independently (e.g., materialize offline, execute on PBS).

Public API (re-exported here for convenience):
  - find_spec_path / list_spec_names: discover spec YAML files under eval/specs/.
  - load_experiment_spec: parse and validate a spec YAML into an ExperimentSpec.
  - materialize_run_bundles / materialize_traces: write trace + run artifacts to disk.
  - load_run_plan / execute_run / submit_run: reload and execute a materialized run.
"""

from .catalog import find_spec_path, list_spec_names
from .run_executor import execute_run, submit_run
from .run_planner import load_run_plan, materialize_run_bundles, materialize_traces
from .spec_io import load_experiment_spec
