# Cross-Verification of the Production-Readiness Audit ("audit of the audit")

> **Document role:** Historical evidence adjudication. Accepted corrections
> have already been incorporated into the corrected audit and canonical plan.
> Do not treat proposals or recommendations in this file as implementation
> instructions; follow `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md`.

**Date:** 2026-08-04
**Verified documents:**
- `doc/PRODUCTION_READINESS_AUDIT.md` (the "audit")
- `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` (the "plan")

**Method:** Every checkable claim was verified against the working tree on
branch `feature/slurm-amd-support` by reading the cited code, re-counting the
quoted numbers, re-running the test suite, and cross-checking every reference
into `doc/KNOWN_ISSUES.md`, `doc/TODO.md`, and `pyproject.toml`. Roughly 30 of
the 35 findings received line-level verification; the remainder (noted at the
end) restate repository documentation that was itself checked. No runtime,
GPU, or scheduler operation was performed beyond `pytest` on the login node.

**Bottom line:** The audit is substantially accurate. Every Blocker-severity
code claim that was checked is real, and most citations are exactly on-line.
Six errors/inaccuracies were found, one of which flags correct code as buggy.
Nothing checked suggests the audit invents problems; where it errs, it errs in
citation or arithmetic, not in the existence of the underlying defect class.

## Codex recheck and adjudication

**Rechecked:** 2026-08-04 against commit `005891e` and the same working tree.

**Method:** Independent line-level review, small validation probes, exact line
counts, and two lightweight pytest executions. No scheduler, GPU, distributed,
or serving workload was launched.

Claude's overall conclusion is fair, but the claim of six audit errors is not.
The proposed corrections have the following disposition:

| Item | Disposition | Result |
|---|---|---|
| E1 | **Accepted** | Remove the innocent `_next_result_path` citation; the real lexicographic selector is already within the cited `run_executor.py` region. |
| E2 | **Rejected** | Claude confuses `client.num_nodes` with `scheduler.nodes`; scheduler/deployment node agreement is not validated. |
| E3 | **Reconciled; defect confirmed** | 25/11 occurs with a real `rg` executable and 24/12 without one. The additional failure is an undeclared host-tool dependency; track failures by node ID/root cause. |
| E4 | **Accepted** | The overlay is 10,945 lines; 12,574 is the combined overlay plus `_sitecustomize.py`. |
| E5 | **Qualification, not an audit error** | `launch_cluster.sh` is valid contextual evidence; the driver returning zero is the defect. |
| E6 | **Partially accepted** | Add the direct-write region, but retain the arrival-mode region because it proves a cache-identity input affects output. |

The recheck also found one citation typo Claude left unverified (PR-025 points
to a nonexistent resource path) and a material stale-evidence problem in
PR-033: application-source MPI staging is now implemented, and newer evidence
falsifies the old general non-streaming HAProxy ceiling. Those documents have
been corrected.

## Claude round-2 adjudication (2026-08-05)

Each Codex disposition was re-verified against the code and by re-running the
suite. Scorecard:

- **E1, E4 (Codex accepted):** agreed; the applied edits are correct.
- **E2 (Codex rejected): Codex is right; Claude's objection is withdrawn.**
  Re-reading `eval/lib/spec_io.py` confirms the conflation: the direct
  local/paired check at `:243-256` compares `client.num_nodes` with
  `deployment.num_nodes`; `scheduler.nodes` is defaulted from the deployment at
  load time (`:164`) and validated as `>= 1` (`:222-223`). The later `<1`
  normalization branch (`:198-199`) cannot repair a normally loaded spec because
  validation runs first. Matrix expansion also auto-synchronizes scheduler size
  unless that field is explicitly targeted or derived. Nevertheless, an
  explicit no-matrix mismatch—or an explicitly targeted/derived matrix
  mismatch—passes validation, and downstream allocation uses `scheduler.nodes`
  while deployment uses `deployment.num_nodes`. The audit's original gap is
  therefore real but should retain this scope qualification.
- **E5 (qualification), E6 (partial):** accepted. On E6 in particular, Codex's
  point stands: `trace_generators.py:182-198` contains the `arrival` branch
  (`:190`) and therefore does support the identity-omission claim; "unrelated"
  was too strong. Citing both regions is correct.
- **E3 — reconciled: the suite is environment-dependent through an undeclared
  `rg` executable.** The differing test is
  `eval/tests/test_eval_control_plane.py::test_eval_runtime_has_no_legacy_import_hacks`.
  It invokes `subprocess.run(["rg", ...])` and expects ripgrep's no-match exit
  code of 1.

  Claude's generated shell snapshot supplied an `rg` shell function because its
  process `PATH` lacked a standalone executable. Shell functions are unavailable
  to `subprocess.run(..., shell=False)`, so that environment produced 24 passed /
  12 failed. Codex's environment contained a real ripgrep 15.2.0 executable and
  produced 25 passed / 11 failed. A controlled check confirmed the discriminator
  with `-p no:randomly`: the test passed with a real executable and failed with
  `FileNotFoundError` when `shutil.which("rg")` returned `None`. The original
  network-flakiness hypothesis was incorrect.

  `pytest-randomly` 4.0.1 is installed in the framework environment and changes
  collection order. It did not cause this delta. The reproducibility gap is the
  undeclared, automatically loaded plugin and unrecorded seed; randomized order
  itself is useful test hardening.

  **Correct targeted reproducer from an uninstalled source-layout checkout:**

  ```bash
  framework_python=/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/bin/python
  test_id=eval/tests/test_eval_control_plane.py::test_eval_runtime_has_no_legacy_import_hacks
  export PYTHONPATH="$PWD/src:$PWD${PYTHONPATH:+:$PYTHONPATH}"

  "$framework_python" -m pytest -q -p no:randomly "$test_id"

  restricted_path=/opt/aurora/26.26.0/frameworks/aurora_frameworks-2025.3.1/bin:/usr/local/bin:/usr/bin:/bin
  env PATH="$restricted_path" PYTHONPATH="$PWD/src:$PWD" \
    "$framework_python" -m pytest -q -p no:randomly "$test_id"
  ```

  The earlier `PATH=<path-without-rg> python -m pytest -q eval/tests` recipe was
  incomplete: it did not preserve the framework Python executable and did not
  account for the repository's `src/` layout.
- **Codex's new findings — verified correct:** the PR-025 path fix
  (`src/exaserve/resources/pingora_proxy/` does not exist; the crate is
  `scripts/pingora_lb/`); the PR-026 expansion (`server.py` imports
  `ray.serve.api._run_many`, `ray.serve._private.api.serve_start`,
  `ray.serve._private.constants`, and the `SERVE_CONTROLLER_ACTOR` name;
  `ray_start.py` imports `ray._private`). Known-Issue references now use durable
  A2/C1/C2/D1 identifiers rather than line numbers that move as entries change.
- **Codex's stale-evidence corrections (A7, D2, B1, PR-033) — verified
  correct and evidence-backed.** `distribute_to_nodes.sh` states it replaces
  the prior parallel-ssh fan-out and performs one MPI bcast of the package
  tree to `/tmp/exaserve_src` (plus optional venv/Triton staging), and
  `launch_cluster.sh:472-481` invokes it and points `PYTHONPATH` at the staged
  tree — so A7/D2's old descriptions were stale. For B1,
  `eval/specs/sc26workshop/FINDINGS_haproxy_256n.md` explicitly lists "an
  architectural HAProxy throughput ceiling" among falsified hypotheses (27.1k
  RPS non-streaming at 256n after the `client.num_nodes` harness fix), and
  `findings/weakscaling_short_v3_progress.md:98-107` shows Direct-Fat and
  HAProxy converging at ~13.8k — implicating the single fat head-node client,
  not HAProxy. B1 is corrected, but B2 still needs a narrower rewrite: common
  streaming behavior is degraded-but-stable, while rare total process death is
  a distinct unresolved failure that cannot yet be assigned to congestion.
- **Previously unverified nuance was checked.** PR-029 is correct about fixed,
  detached, stale-prone actors, but code does not guarantee that the single
  serving collector is placed on the head node. PR-032 is correct about the
  missing aggregate operational contract, but `/health` and `/stats` are
  replica-local handlers behind a load-balanced route, and the OpenAI completion
  UUID should be linked to—not conflated with—a transport correlation ID. The
  forkserver boundary in `run_planner.py` explains why a parent-process
  monkeypatch does not make the worker tests hermetic.

**Net disposition after round 2 and independent verification:** E1/E4 are
accepted, E5/E6 remain qualified, E2 is withdrawn, and E3 is reconciled as an
undeclared executable dependency: 25/11 with a real `rg`, 24/12 without one.
The primary audit and plan now record both conditional observations and track
closure by pytest node ID and root cause. The B2, A6/C1, PR-029/PR-032, and D2
citation corrections are applied. A final stale-document sweep additionally
corrected disabled proxy profiling (D3), existing multi-replica PP behavior
(D4), and the corresponding TODO entries.

---

## 1. Errors and false claims found

### E1. PR-019 cites correct code as buggy (wrong file; cited code is innocent)

The audit claims the "latest-result selector" at `eval/lib/replay_engine.py:670-679`
sorts filenames lexicographically so `result9.json` beats `result10.json`.
That function (`_next_result_path`) parses the numeric suffix correctly
(`if suffix.isdigit(): next_index = max(next_index, int(suffix) + 1)`) — there
is no bug there.

The lexicographic-sort bug is **real but lives elsewhere**:
`eval/lib/run_executor.py:348-358` returns `sorted(candidates)[-1]` over
`result*.json` names, so `result9.json` is selected over `result10.json`.

**Disposition:** substance TRUE, citation FALSE. Fix the cite before using
PR-019 as a work item.

> **Codex recheck — accepted.** `_next_result_path()` at
> `eval/lib/replay_engine.py:670-679` is numeric and correct. The faulty
> selector is `_latest_result_path()` at
> `eval/lib/run_executor.py:348-358`. The original audit already included
> `run_executor.py:330-358`, so this was one erroneous extra region rather than
> a missing defect. PR-019's regions have been sharpened accordingly.

### E2. PR-020 contains a false sub-claim (node agreement IS validated)

The audit lists "scheduler-node consistency with deployment-node count" /
"scheduler/deployment node agreement" among unchecked values. In fact
`eval/lib/spec_io.py:243-256` raises when `dest=direct` with `local`/`paired`
arms and `client.num_nodes != deployment.num_nodes`.

Secondary citation problem in the same finding: `eval/lib/backends/ray.py:90-115`
is pre-launch **config** validation (scheduler type, proxy type, ray_node_cpus,
ray_head_port) and does not support the "validates only a limited result
subset" claim. That claim is nonetheless true via the other cite:
`eval/lib/run_executor.py:271-279` validates only the **last** dispatch arm
that produced a result (`reversed(...)` + `next(...)`); earlier arms escape
semantic validation.

The remaining PR-020 gaps are confirmed: no enum checks for
`scheduler.type` / `deployment.engine` / `workload.generation_mode` /
`workload.arrival`; no bounds on `client.go_concurrency`.

> **Codex recheck — pushback.** `spec_io.py:243-254` compares
> `spec.client.num_nodes` with `spec.deployment.num_nodes`; it never reads
> `spec.scheduler.nodes`. The only centralized scheduler-node check is
> `scheduler.nodes >= 1` at `spec_io.py:220-223`. A lightweight probe with
> `deployment.num_nodes=2` and `scheduler.nodes=1` passed
> `validate_experiment_spec`; downstream, the scheduler allocation uses
> `scheduler.nodes` at `eval/lib/backends/ray.py:130` while the deployment uses
> `deployment.num_nodes` at `:154-155`. The original scheduler/deployment gap
> is therefore real. The categorical statement that there are “no enum checks”
> is also too broad: backend-specific and scheduler-registry checks exist; the
> audit correctly says validation is not **consistent** or centralized. The
> regions in PR-020 are aggregate evidence, so `ray.py:90-115` need not support
> the separate last-result-arm sentence by itself.

### E3. Original baseline discrepancy — superseded by round-2 reconciliation

Audit "Validation record" claims **36 collected, 25 passed, 11 failed** with
7 eval control-plane network failures. Re-run on 2026-08-04
(frameworks python 3.12.12, pytest 8.3.5, `PYTHONUSERBASE` set, login node):

```
36 collected, 24 passed, 12 failed
```

Failures: **8** in `eval/tests/test_eval_control_plane.py` (not 7), plus the
same 1 clientlab (`test_summarize_point_prefers_queue_bound_when_queue_fraction_is_high`),
2 HAProxy (`tests/test_haproxy_proxy.py`), and 1 submit
(`tests/test_submit.py::test_submit_serve_dry_run_renders_self_contained_pbs`).
Failure **categories** match the audit exactly. The original hypothesis that the
extra failure was network/environment flakiness was wrong; round 2 identifies
the undeclared `rg` executable as the exact discriminator.

The plan then repeated the stale numbers twice: §2 "25 of 36 tests pass and 11 fail"
and WP0 item 4 / WP11 item 5 "the 11 failing/current failures". The plan
should reference "the failing tests" by name or category, not a count that
already drifted between the audit and the next day's run.

> **Final cross-verification:** The original 25/11 result and Claude's 24/12
> result are both genuine. The additional node is
> `test_eval_runtime_has_no_legacy_import_hacks`; it passes with a real `rg`
> executable and fails without one. The eleven common failures comprise six
> invalid-Hugging-Face-ID/forkserver failures, one src-layout plotting-subprocess
> import failure, one ClientLab `faults` failure, two stale HAProxy tests, and one
> PBS/PSI-J expectation. Future closure is keyed by node ID and root cause, not a
> hardcoded count.

### E4. PR-026 double-counts the vendored-overlay line count

Measured: `src/exaserve/_sitecustomize.py` = **1,629 lines** (audit exact ✓).
The Ray Serve overlay `src/exaserve/patches/ray_serve_overlay/.../_private/*.py`
totals **10,945 lines**, not "approximately 12,500". 1,629 + 10,945 = 12,574 ≈
the audit's 12,500 — i.e. the "vendored" figure silently includes the
sitecustomize file that the same sentence already counts separately. The
dependency claims in PR-026 are confirmed (`ray[serve]>=2.49` open lower
bound; `vllm` and `litellm` unpinned in `pyproject.toml`).

> **Codex recheck — accepted.** `wc -l` reports 1,629 lines for
> `_sitecustomize.py` and 10,945 across the eight overlay files, or 12,574
> combined. PR-026 now distinguishes the overlay count from the combined count.

### E5. PR-001 misattributes part of the failure path to launch_cluster.sh

The driver-side claim is fully confirmed:

- `src/exaserve/driver.py:616-621` logs the ExaServe child's nonzero return
  code but never propagates it; no `sys.exit`/`exit(` anywhere in the file.
- `driver.py:640` waits on the Ray worker without reading its return code.
- `driver.py:644-645` `except Exception as e: print(...)` swallows the FATAL
  RuntimeErrors raised at lines 416/580/594 → process exits 0.

However, `src/exaserve/resources/launch_cluster.sh:514-515` is cited as a
contributing region, and the audit says "Because launch_cluster.sh invokes the
driver under MPI/srun, the scheduler can record a successful job even when
... serving failed." The script has `set -e` (line 2), so a **nonzero** exit
from the mpiexec-launched driver would abort the script and surface to the
scheduler. The false-success mechanism is entirely the driver exiting 0; the
launcher invocation itself is not defective.

> **Codex recheck — qualification/pushback.** The shell is not defective, but
> the original audit did not claim it masks a nonzero status. It cites
> `launch_cluster.sh:514-515` to show how the driver's false zero becomes the
> scheduler-visible job result. That causal statement is correct: `set -e`
> cannot help when the driver returns zero. PR-001 now says this explicitly,
> while retaining the launcher lines as context.

### E6. PR-017 cites the wrong region for the trace-cache write path

The claims are true, but `eval/lib/trace_generators.py:182-198` is
`_arrival_times()` — unrelated to caching or writing. Correct locations:

- exists-only cache: `eval/lib/trace_store.py:75`
  (`if force or not os.path.exists(trace_path):`)
- non-atomic final write, no lock/tmp+rename: `write_trace()` at
  `eval/lib/trace_generators.py:42-68`.

The headline claim is confirmed and worth emphasizing for eval correctness:
`_trace_identity()` (`trace_store.py:29-60`) omits `workload.arrival`, while
`trace_generators.py:190` branches on it (`fixed` vs `poisson` timestamps) —
so fixed- and poisson-arrival variants collide on the same cached trace. The
identity also stores prompt/trace **paths**, not content hashes, as claimed.

> **Codex recheck — partially accepted.** Add
> `trace_generators.py:42-68`, which is the direct non-atomic trace write.
> Do not remove `trace_generators.py:182-198`: it proves `workload.arrival`
> changes timestamps, while `trace_store.py:29-60` proves arrival is omitted
> from identity. The original region was relevant to the collision claim even
> though it was not the write site. PR-017 now cites both regions.

---

## 2. Claims verified TRUE (line-level, by finding)

- **PR-002** TRUE, exact lines. `driver.py:240` and `:266` hardcode
  `"--num-gpus=12"`; `num_gpus_per_node` never read in driver.py.
  `get_ray_env()` (`driver.py:152-155,177`) unconditionally sets
  `VLLM_TARGET_DEVICE=xpu`, `ZE_FLAT_DEVICE_HIERARCHY`, etc.; no
  `EXASERVE_VENDOR` reference in driver.py, while `launch_cluster.sh:311,397`
  do gate the same vars on vendor.
- **PR-003** TRUE, exact lines. `write_ray_cluster_head_ip()`
  (`launch_cluster.sh:98-114`) rewrites the **source** YAML in place; the
  audit copy is taken earlier (`:160`) than the mutation (`:298`), so the
  archived config differs from the config used. `server.py:1876` and
  `driver.py:378-423` write `ray_node_ips.txt` / proxy artifacts beside the
  config.
- **PR-004** TRUE, all sub-claims. `bcast.c:74,86,89` build `tar`/`mkdir`
  shell strings with unquoted paths; snprintf return discarded (truncation
  executes a different command); `system()` (`:87`) and both `pclose()`
  (`:141,144`) returns ignored — `main` returns 0 regardless; 1 GiB/rank
  buffer (`:17,94-95`, and the `assert(buf)` is compiled out under NDEBUG).
- **PR-005** TRUE, all sub-claims. Completeness = `config.json` + any one
  top-level weight file (`model_staging.py:43-52`), non-recursive, no index/
  shard/checksum/marker; `snapshot_download` writes straight into the final
  dir with no repo-level lock/tempdir/atomic rename (`:163,173-178`);
  `_resolve_hf_cache_snapshot()` falls back to `sorted(...)[0]` — the
  lexicographically smallest commit-SHA dir (`:79-83`).
- **PR-006** TRUE. `bool(d.get(...))` coercions (`schemas.py:108-109,130`) so
  `"false"` → True; invalid `num_replicas` → `None` via silent
  `except (TypeError, ValueError)` (`:94-100`), and `None` means auto-planning
  (`server.py:1288-1293`); unknown keys silently dropped everywhere.
- **PR-007** TRUE; both collision examples are mathematically correct given
  `model_paths.py:5-12` (`/`→`--` then `.`→`-`): `a/b--c` and `a--b/c` →
  storage `a--b--c`; `a.b/c` and `a-b/c` → route `a-b--c`. Uniqueness check
  is raw-string only (`schemas.py:152-154`).
- **PR-008** TRUE. 600 s GPU deadline then "Proceeding with reduced capacity"
  warning (`server.py:1821,1857-1863`); proxy `ray.wait` loop with no overall
  deadline (`:2100-2106`); unhealthy proxies and status-collection exceptions
  are prints (`:2157-2163`); unconditional `✓✓✓ CLUSTER FULLY READY ✓✓✓`
  (`:2197-2199`). Bonus defect found during verification: `server.py:1856`
  reads `alive_nodes`, which is unbound if the deadline expired before the
  first loop iteration → NameError.
- **PR-009** TRUE (start block is `driver.py:599-604`, minor drift). No
  monitor on `proxy_process` after start; only `serve_process.wait()` blocks;
  `ray_process.terminate()` followed by **unbounded** `wait()`
  (`driver.py:664-667`), unlike the serve process which gets
  `wait(timeout=10)` + `kill()`.
- **PR-010** TRUE. `serve.start(HTTPOptions(host="0.0.0.0", port=8000))`
  (`server.py:1804-1809`); no auth/TLS/rate-limit anywhere in the handlers;
  HAProxy emits `stats admin if TRUE` on `bind *:9999` with no `stats auth`
  (`haproxy_proxy.py:169-177`, default port `:62`); LiteLLM master key
  defaults to `""` and is only set if truthy (`litellm_proxy.py:73,104-106`)
  — note the docstring (`:55-56`) claims a `"sk-aurora"` default the code
  does not implement — and config incl. secrets is written plaintext with no
  chmod (`:117-119`); SGLang sets `trust_remote_code=True` twice
  (`sglang.py:61,72`).
- **PR-011** TRUE. Raw `await request.json()` + bare `float()`/`int()` in
  `_parse_sampling` (`server.py:1004,1030,1063-1079`) → 500s on malformed
  input; `body["model"]` never read; responses always echo `self.model_id`.
- **PR-012** TRUE. Bind-and-close TOCTOU probe (`server.py:284-291`) feeding
  the real bind later (`engines/vllm.py:112,131`); `submit.py:202-206` builds
  the URL from configured port / literal 4001 and never reads the published
  `proxy_port` file written by `driver.py:420-423`; scheduler state `"R"`
  alone returns the URL (`submit.py:211-215`), no HTTP probe.
- **PR-013** TRUE, all five sub-claims. Check-then-write lock, no O_EXCL
  (`run_executor.py:429,467,471-481`); foreign-host live lock removed as
  "STALE_LOCK_CLEANED" (`:442,453-467`); job ID only appended to an in-memory
  list (`:546-553`), never persisted; discovery skips only `"succeeded"`
  (`:600-619`) so queued/running runs are re-submitted; `failed` dict declared
  (`:529`) and read (`:573-576`) but never written — failures loop forever.
- **PR-014** TRUE. `except Exception: return {}` in both eval scheduler
  counters (`eval/lib/schedulers/pbs.py:70-73`, `slurm.py:84-87`; both also
  ignore returncode); package Slurm `job_state` treats empty `squeue` output
  as done — `if not raw: return "D"` (`src/exaserve/schedulers/slurm.py:62-74`).
- **PR-015** TRUE with one nuance. All `#PBS`/`#SBATCH` directive lines
  interpolate job name/account/queue/walltime/log paths raw (both stacks);
  the eval-side script body has no quoting at all
  (`eval/lib/schedulers/base.py:65-79`). Nuance: the package-side body
  (`src/exaserve/schedulers/base.py:75-92`) **does** `shlex.quote` most
  fields — the raw holes there are `spec.env_setup` (intentionally raw shell,
  as the audit itself notes) and the directive lines. The audit's wording
  "without **consistent** validation and quoting" remains fair.
- **PR-016** TRUE. `eval(expr, {"__builtins__": {}}, namespace)` at
  `eval/lib/matrix.py:40`, namespace exposes `math`; spec-YAML expressions are
  executable code.
- **PR-018** TRUE. `_next_run_group_id()` list-then-`ensure_dir`, no exclusive
  create (`run_planner.py:119-124,271-272`).
- **PR-021** TRUE only for the legacy pull API, with the active-path correction
  now reflected in the primary audit. `to_dict()` returns
  `summary`/`sample`/`scheduler_snapshots` (`server.py:783-787`), while
  `vllm.py:286` reads `data["finished_requests"]`; that unused method remains
  internally broken. Current evaluation uses the push-based
  `ServingStatsCollector` and has a recorded successful 12/12-replica output.
  The remaining production issue is that requested stats can still degrade to a
  warning without making the run non-successful.
- **PR-022** TRUE. `enable_log_requests` in the schema (`schemas.py:28,44,109`)
  but absent from `EngineSpec` (`engines/base.py:30-43`) and from the
  `EngineSpec(...)` construction (`server.py:913-924`); `vllm.py:146-147`
  hardcodes it True — the operator's `False` is never honored.
- **PR-023** TRUE. Skipped model plans are printed and deployment proceeds
  (`server.py:1534-1540,1550`); only total wipeout raises (`:1529-1532`);
  no strict/required-model flag exists (`replica_planner.py:256-260`).
- **PR-024** (drift sub-claim) TRUE. Implementation emits
  `option httpchk GET /-/healthz` (`haproxy_proxy.py:421-424`); tests still
  assert the old `/health` and per-model-route checks
  (`tests/test_haproxy_proxy.py:34,68-69`) — genuine test/design drift,
  matching TODO.md's own "stale HAProxy unit tests" entry and commit 3d130c8.
- **PR-027** TRUE, all five sub-claims. Module header says default `pbs`
  (`schedulers/__init__.py:5,8`) vs `get_scheduler()` defaulting `psij`
  (`:45`); `submit.py:1-22` still narrates a pure-PBS workflow; **no psij /
  psij-python anywhere in pyproject.toml** (core deps are PyYAML+packaging);
  `eval/lib/backends/ray.py:96-97` whitelists `pbs` only (rejects
  slurm/psij); `src/exaserve/schedulers/slurm.py:1` says "UNTESTED".
- **PR-028** TRUE, exact lines. `while True: time.sleep(10)` with only
  `KeyboardInterrupt` handled (`server.py:2202-2206`); zero `signal.`
  references in server.py or driver.py — SIGTERM from qdel/scancel bypasses
  all cleanup.
- **PR-030** TRUE, all three. Unconditional `run_config["faults"]` index
  (`clientlab/analysis/diagnostics.py:78`); the test omits `faults` and fails
  with KeyError (`clientlab/tests/test_spec_and_analysis.py:37-40,59`;
  reproduced in the pytest run above); `os.system(f"ssh {node} ...")` at
  `clientlab/collectors/netstats.py:39-40`.
- **PR-031** TRUE (modulo the count in E3). No `.github/`, no lockfile, no
  pre-commit/ruff/flake8/mypy config anywhere in the repo root;
  at the time of Claude's review, `doc/TODO.md:26-27` still said "No unit or
  integration tests / Zero formal test coverage" while `tests/`, `eval/tests/`,
  `clientlab/tests/` existed — the documentation-drift point was confirmed.
  The TODO entry was corrected during the Codex adjudication.
- **PR-034** TRUE. `_warn_dirty_repo()` prints only
  (`run_planner.py:175-182`); snapshot is `git archive ... HEAD` (`:211`);
  dirtiness recorded but not blocking (`:281-282`). Matches KNOWN_ISSUES C2.
- **PR-035** TRUE. `dump_yaml_file`/`dump_json_file` open the final path with
  `"w"`, no fsync/rename (`eval/lib/utils.py:85-98`); used for mutable run
  state (`run_planner.py:527` among others). The repo demonstrably knows the
  atomic pattern elsewhere (`run_planner.py:254` `os.replace`;
  `replay_engine.py:125-128` tmp+rename shards), so these helpers are the
  inconsistent path — exactly the audit's framing.
- **PR-019** (other two sub-claims) TRUE. Shard gather drops missing ranks
  with a warning after the deadline (`replay_engine.py:150-159`); result
  validation passes with a single successful request
  (`run_executor.py:321-323`, errors/shortfalls print-only `:329-337`).
  Matches KNOWN_ISSUES C4's "watch the collected N/M warning".

## 3. Document cross-references verified

- Every audit citation into `doc/KNOWN_ISSUES.md` was rechecked by durable entry
  identifier rather than mutable line number. D1 remains the readiness
  false-positive, A2 the port race, C1 now records the superseded silent-no-op
  history plus active push-path debt, and C2 remains the committed-HEAD gotcha.
- The plan's ledger IDs A1–A7, B1–B3, C1–C6, D1–D4 all exist in
  KNOWN_ISSUES.md; the closure map in plan §7 covers **all** of PR-001..035
  with no orphans (several intentionally appear in two WPs).
- Plan §2 "retains all 35 findings" ✓ — the audit numbers PR-001..PR-035.
- Plan WP12's Aurora workflow reference `~/script/env_aurora` exists.
- Plan §2 "Aurora is the only credible current platform" is consistent with
  TODO.md's own status notes (Slurm e2e unproven offsite, AMD bring-up
  unverified, SGLang not smoke-validated).

## 4. Not independently verified

Line-level verification was **not** performed for: PR-025 (Pingora
"benchmark component" characterization), PR-029 (detached telemetry actor
lifecycle), PR-032 (observability gaps), PR-033 (scale limits — this finding
mostly restates KNOWN_ISSUES/findings docs, which were confirmed to say what
the audit says they say), the finer sanitization subparts of PR-024, and
PR-026's "several additional private Ray APIs" claim about server.py imports.
The consistent pattern in everything that was checked gives no reason to
expect these to diverge, but they have not had the same treatment.

> **Codex follow-up verification.** These claims were checked after Claude's
> review:
>
> - **PR-024:** Confirmed. HAProxy and NGINX interpolate incompletely sanitized
>   identifiers, route prefixes, hosts, options, and unbounded numeric values;
>   neither runs `haproxy -c` / `nginx -t`, and both readiness checks are
>   TCP-only. Envoy/Pingora's YAML serialization reduces directive-injection
>   risk but does not supply strict option/range schemas.
> - **PR-025:** Confirmed after correcting its path to `scripts/pingora_lb/`.
>   The Rust crate calls itself a minimal benchmark LB; Python rejects
>   multi-model use; unknown LB methods fall back silently; requested threads
>   are ignored; readiness is TCP-only. The original title was narrowed because
>   this does not prove every other proxy is merely a benchmark component.
> - **PR-026:** Confirmed and understated. `server.py` uses private Serve start,
>   multi-run, deploy utilities, protobufs, deployment fields,
>   client/controller methods, and actor names; `ray_start.py` also imports
>   `ray._private` services.
> - **PR-029:** Confirmed with nuance. Push/sample sizes have rudimentary bounds,
>   but fixed detached collectors can collide or retain stale deployment data;
>   error cleanup, reset/eviction, backpressure, and deployment scoping are
>   absent. The collector is cluster-wide, not guaranteed head-pinned.
> - **PR-032:** Confirmed when stated as the lack of an ExaServe-owned unified
>   operational contract. Ray has dependency-level metrics/request IDs and
>   ExaServe has replica-local handlers behind a load-balanced route. The driver
>   sets Ray's disabling metrics flag, and EngineWorker creates an application
>   completion ID without linking it to an incoming transport correlation ID.
> - **PR-033:** Requires material correction. Current GCS/controller, static
>   port, proxy-startup, and centralized streaming limits remain. The former
>   app-source Lustre stampede now has MPI source/optional-venv staging, and
>   newer 256-node evidence reports 27.1k non-streaming HAProxy RPS and explicitly
>   falsifies a general HAProxy throughput ceiling. The active measured proxy
>   constraint is centralized no-coalescing streaming/network concentration.

## 5. Suggested corrections to the two documents

1. PR-019: change the lexicographic-sort cite from
   `replay_engine.py:670-679` to `run_executor.py:348-358`.
   **Codex disposition: accepted and applied.**
2. PR-020: drop the "scheduler/deployment node agreement" bullet (or narrow
   it to cases outside `spec_io.py:243-256`); replace the `ray.py:90-115`
   cite for the result-subset claim with `run_executor.py:271-279`.
   **Codex disposition: rejected.** The cited block checks client/deployment,
   not scheduler/deployment. The aggregate regions support different parts of
   PR-020.
3. PR-017: change `trace_generators.py:182-198` to `trace_store.py:75` +
   `trace_generators.py:42-68`.
   **Codex disposition: partially applied.** The write region was added; the
   arrival region was retained because it proves the identity-affecting semantic
   difference.
4. PR-026: change "approximately 12,500 vendored lines" to "approximately
   10,900 vendored lines (≈12,600 including the sitecustomize module)".
   **Codex disposition: accepted and applied.**
5. PR-001: drop `launch_cluster.sh:514-515` as a defect region (script has
   `set -e`); keep it only as context for where the driver is invoked.
   **Codex disposition: qualification applied.** The region remains as context;
   PR-001 now explicitly states that the driver, not `set -e`, creates the false
   success.
6. Record both conditional observations: 25/11 with a real `rg` executable and
   24/12 without one. Identify the delta as an undeclared executable dependency,
   not network flakiness. Keep implementation targets keyed by pytest node ID and
   root cause.
   **Final disposition: reconciled and applied.**
