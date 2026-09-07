# Dependency profiles

`ci.lock` is the complete portable Python dependency set for hermetic tests,
wheel tests, lint, and the selected typed-core check. CI installs it without
dependency re-resolution before installing the ExaServe wheel with
`--no-deps`.

`aurora-frameworks-2025.3.1.lock` records the exact heavy stack supplied by
ALCF. It is verified at runtime by `CompatibilityProfile`; it is not a PyPI
recipe because the XPU wheels are site-built. Python itself is pinned by that
profile to 3.12.12, and the replay/staging MPI API is bound to the listed
mpi4py distribution. A new site stack requires a new lock, compatibility
profile, source hashes, semantic probes, and qualification evidence.

Pytest plugin autoload is disabled in CI. Fixed-order tests load no third-party
plugin; the randomized job explicitly loads `pytest-randomly` and prints its
replayable seed.
