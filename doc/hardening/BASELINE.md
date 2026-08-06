# WP0 Baseline (frozen before hardening changes)

**Recorded:** 2026-08-05
**Revision:** `005891e` ("Stage audit docs, ready for review and implementation."), branch `feature/slurm-amd-support`
**Dirty worktree at freeze (pre-existing, user/Codex-owned, preserved):**
`AGENTS.md`, `doc/KNOWN_ISSUES.md`, `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`,
`doc/PRODUCTION_READINESS_AUDIT.md`, `doc/TODO.md`; untracked
`doc/PRODUCTION_READINESS_CLAUDE_AUDIT.md`, `doc/PLAN_FEASIBILITY_CLAUDE.md`.
Hardening work adds files under `doc/hardening/` and `artifacts/hardening/` and
edits code/tests as logged in `doc/hardening/MIGRATION_LOG.md`. No git
commit/branch/push is performed (not authorized by the active request).

## Environment (login node, Aurora)

- Python: `/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/bin/python` (3.12.12)
- `PYTHONUSERBASE=/home/wenyiw/.local/aurora/frameworks/2025.3.1`
  (contains `sitecustomize.py`/`usercustomize.py` `_aurora_import` hooks)
- pytest 8.3.5; plugins: `pytest-randomly 4.0.1` (auto-loaded; shuffles order,
  prints seed in header)
- `exaserve` is NOT pip-installed; source layout only (`src/`)
- Host tools: `rg` exists only as a user-profile **shell function**; no ripgrep
  binary on `PATH` (subprocess `rg` ⇒ `FileNotFoundError`)
- `PYTHONPATH`: unset in the recording shell

## Test baseline (exact command and results)

```
cd /home/wenyiw/exaserve && PYTHONUSERBASE=... python -m pytest -q -p no:cacheprovider
```

36 collected. **24 passed / 12 failed in this environment** (3 consecutive
reproductions on 2026-08-04/05, incl. once with `-p no:randomly`; durations
41.98s / 41.00s / 31.98s). In an environment with a real `rg` binary the same
commit yields 25/11 (Codex, twice) — the delta is failure F-12 below.

Failed node IDs and root causes:

| # | Node ID | Root cause class |
|---|---|---|
| F-01 | `eval/tests/test_eval_control_plane.py::test_run_bundle_materialization_and_mock_execute` | Live HF access to nonexistent `test/model`: parent-process tokenizer monkeypatch does not reach forkserver workers |
| F-02 | `eval/tests/test_eval_control_plane.py::test_trace_artifacts_are_reused` | same as F-01 |
| F-03 | `eval/tests/test_eval_control_plane.py::test_materialize_twice_creates_incrementing_run_groups_and_reuses_snapshot` | same as F-01 |
| F-04 | `eval/tests/test_eval_control_plane.py::test_dirty_repo_warning_and_snapshot_excludes_uncommitted_content` | same as F-01 |
| F-05 | `eval/tests/test_eval_control_plane.py::test_ray_adapter_always_uses_exaserve_env` | same as F-01 |
| F-06 | `eval/tests/test_eval_control_plane.py::test_cli_validate_and_submit_all_latest_dry_run` | same as F-01 |
| F-07 | `eval/tests/test_eval_control_plane.py::test_plot_scripts_resolve_latest_run_group` | Plotting subprocess prepends repo root, not `src/`; child cannot import `exaserve` |
| F-08 | `clientlab/tests/test_spec_and_analysis.py::test_summarize_point_prefers_queue_bound_when_queue_fraction_is_high` | `run_config["faults"]` unconditional index (PR-030); owner WP9 |
| F-09 | `tests/test_haproxy_proxy.py::test_haproxy_multi_model_routes_by_path_prefix` | Stale test: asserts pre-3d130c8 per-model `/health` checks; code emits `/-/healthz` (PR-024); owner WP7/WP11 |
| F-10 | `tests/test_haproxy_proxy.py::test_haproxy_single_model_uses_root_health_check` | same as F-09 |
| F-11 | `tests/test_submit.py::test_submit_serve_dry_run_renders_self_contained_pbs` | Stale test: expects PBS artifact; default backend is PSI/J (PR-027); owner WP8 |
| F-12 | `eval/tests/test_eval_control_plane.py::test_eval_runtime_has_no_legacy_import_hacks` | Undeclared host binary: `subprocess.run(["rg", ...])`; env-dependent (AC-TST-01); owner WP0 |

Additional collection defect (not a failing node in full runs):
`pytest eval/tests/...` standalone cannot import `exaserve` — `src/` enters
`sys.path` only via `tests/conftest.py`, which the full run loads as a side
effect of enumerating the rootdir. Any subset invocation without `tests/`
fails collection. Owner WP0 (AC-TST-01 collection clause).

## Canonical test policy (WP0 decision, refined by WP11)

- Default/local and fixed-order CI: `pytest-randomly` disabled via
  `[tool.pytest.ini_options] addopts = "-p no:randomly"` — deterministic order.
- Randomized job (WP11): run explicitly with `-o addopts="" `; pytest-randomly
  prints a replayable seed header; record it in the job log.
- No test may invoke an undeclared host executable; source-layout path setup
  lives in exactly one root `conftest.py`.

## Scheduler/site profile at freeze

Aurora, PBS; package scheduler default = PSI/J (`get_scheduler()`), eval
scheduler = PBS/Slurm EvalScheduler stack; site queues: capacity 1–16 nodes,
debug-scaling 2–256 @ ≤1 h, prod ≥256. Compute sessions per `AGENTS.md`
(subjob lease preferred; `srundbg`/`srundsc N` fallback).
