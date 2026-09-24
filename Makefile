SHELL := /bin/bash
.DEFAULT_GOAL := help

UV ?= uv
BUILD_OUTPUT ?= dist
PYTEST_ENV = PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1

.PHONY: help install install-dev lock lock-check lint format-check type-check test test-cov build check-compute-session

help:
	@printf '%s\n' \
	  'ExaServe portable development commands (uv 0.10.1)' \
	  '  install / install-dev  Sync locked portable dev and test groups; no GPU extras' \
	  '  lock                   Deliberately update uv.lock from dependency metadata' \
	  '  lock-check             Check uv.lock without changing it' \
	  '  lint                   Run the existing correctness, runtime, and security rules' \
	  '  format-check           Check formatting without rewriting source' \
	  '  type-check             Check the typed contract core' \
	  '  test                   Run the fixed-order portable suite' \
	  '  test-cov               Run that suite with terminal and XML coverage reports' \
	  '  build                  Audit an sdist and wheel; BUILD_OUTPUT must be new/empty' \
	  '' \
	  'Aurora: full tests and builds require a validated compute session and environment.' \
	  'Use a verified interactive PBS compute shell; see docs/getting_started.md#aurora-development-and-serving.' \
	  'The session guard is a safety check, not allocation or environment setup.' \
	  'Portable CI covers Linux Python 3.10 and 3.12, not GPU/runtime qualification.'

install install-dev:
	$(UV) sync --locked --group dev --group test --no-python-downloads

lock:
	$(UV) lock --no-build --no-python-downloads

lock-check:
	$(UV) lock --check --no-build --no-python-downloads

lint:
	$(UV) run --locked ruff check .
	$(UV) run --locked ruff check --select E4,E7,F src/exaserve eval/lib eval/cli.py clientlab tests conftest.py
	$(UV) run --locked ruff check --select S102,S307,S602,S608,S609 src/exaserve eval/lib clientlab

format-check:
	$(UV) run --locked ruff format --check .

type-check:
	$(UV) run --locked mypy --ignore-missing-imports --follow-imports=skip \
	  src/exaserve/plan/contracts.py src/exaserve/control/contracts.py \
	  src/exaserve/telemetry.py src/exaserve/state/results.py

# Aurora uses non-"aurora" compute hostnames too, so also detect the site tree.
# Non-Aurora developer machines and hosted CI do not need a PBS allocation.
# Accept verified interactive PBS shells and retain legacy lease compatibility.
check-compute-session:
	@node=$$(hostname -s); \
	if [[ -d /opt/aurora || "$$node" == aurora-* ]]; then \
	  if [[ "$$node" == aurora-uan-* || -z "$${PBS_JOBID:-}" || ! -r "$${PBS_NODEFILE:-}" ]]; then \
	    printf '%s\n' 'Aurora tests/builds require a validated compute session; see docs/getting_started.md#aurora-development-and-serving and AGENTS.md.' >&2; exit 1; \
	  fi; \
	  if ! awk -v node="$$node" '{ split($$1, host, "."); if (host[1] == node) found = 1 } END { exit !found }' "$$PBS_NODEFILE"; then \
	    printf '%s\n' 'Current host is not in PBS_NODEFILE; refusing tests/builds.' >&2; exit 1; \
	  fi; \
	  if [[ "$${AURORA_SUBJOB:-}" != 1 && "$${PBS_ENVIRONMENT:-}" != PBS_INTERACTIVE ]]; then \
	    printf '%s\n' 'Use a verified interactive PBS compute shell; see docs/getting_started.md#aurora-development-and-serving.' >&2; exit 1; \
	  fi; \
	fi

test: check-compute-session
	$(PYTEST_ENV) $(UV) run --locked python -m pytest -q -p no:randomly

test-cov: check-compute-session
	$(PYTEST_ENV) $(UV) run --locked python -m pytest -q -p no:randomly -p pytest_cov \
	  --cov=exaserve --cov=eval --cov=clientlab --cov-report=term-missing --cov-report=xml

build: check-compute-session
	$(UV) run --locked python scripts/hardening/build_release_artifacts.py --output-dir "$(BUILD_OUTPUT)"
