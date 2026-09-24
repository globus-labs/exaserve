# Contributing to ExaServe

Contributions to ExaServe are welcome. Please read the [README](README.md) and
[Code of Conduct](CODE_OF_CONDUCT.md) before participating.

## Project scope and sources of authority

[AGENTS.md](AGENTS.md) governs agent execution safety and where commands may run.
[Getting started](docs/getting_started.md) documents environment preparation and
compute-session checks for contributors and deployers. The
[production-hardening execution plan](doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md)
is the authoritative architecture and implementation specification. Historical
audits, Known Issues, TODOs, and design drafts provide context, not overrides.

The [hardening status](doc/hardening/STATUS.md) records candidate identities,
qualification evidence, supersessions, and unresolved release gates. A passing
unit suite is not hardware qualification. Do not describe a platform, engine,
gateway, scale, or source snapshot as supported without the corresponding
approved, exact-candidate evidence. New platform support requires an explicitly
scoped and qualified `SiteProfile`; an issue, example, or CI job does not create
that qualification.

## Issues and proposals

Search existing issues before opening a new one, and use the bug-report or
feature-request template. For bugs, include a minimal reproducer, expected and
actual behavior, the exact source commit or wheel hash, relevant site/runtime
identities, and sanitized first-cause status and log excerpts. Distinguish an
observed failure from a configuration rejected by the current qualification
policy. Do not attach credentials, private prompts, datasets, full environment
dumps, or restricted site information.

For enhancements, explain the use case, alternatives, affected contracts, and
how the change could be validated. Discuss substantial architecture or platform
changes with maintainers before implementing them. Report suspected
vulnerabilities privately according to [SECURITY.md](SECURITY.md), not in a
public issue or pull request.

## Development setup

Use Linux, a supported portable CI interpreter (Python 3.10 or 3.12), Git, and
`uv==0.10.1`. Fork the repository if needed, clone
your fork, and create a focused branch. The setup target uses the committed lock
file to create or update the managed `.venv` development environment:

```bash
make install-dev
```

On Aurora, follow the [environment preparation guide](docs/getting_started.md#aurora-development-and-serving)
before running checks. Server dependencies and the vendor runtime have their
own qualified environment; installing development dependencies does not install
or validate that stack. No maintainer-private allocation helpers are required.

The common development commands are:

```bash
make lint
make format-check
make type-check
make test
make test-cov
make build
```

Run full test suites, coverage runs, and any nontrivial experiment in a validated
compute session on Aurora, following [getting started](docs/getting_started.md#aurora-development-and-serving).
Only clearly brief, lightweight checks belong on a login node. GPU, MPI, distributed, and
resource-intensive work must never run there. Allocation, environment setup,
preflight, monitoring, walltime, and cleanup requirements are documented there;
these commands do not bypass them. CI's hermetic checks are a separate lane
from live hardware and installed-wheel qualification.

## Code, documentation, and tests

- Keep changes focused and preserve unrelated work, outputs, and evidence.
- Follow the repository's Ruff and type-check configuration. Use type hints and
  clear docstrings for public interfaces; explain non-obvious invariants.
- Add regression tests for fixes and tests for new behavior, including relevant
  failure paths. Keep hermetic tests independent of credentials, scheduler
  access, GPUs, and undeclared network services.
- Update user-facing documentation and examples when behavior changes. Keep
  examples consistent with the canonical configuration and qualification scope.
- Never modify historical artifact hashes or evidence to make them match new
  code. Record new candidate identities and pending gates explicitly.
- End files with a newline and use concise, imperative commit subjects. Explain
  the motivation and relevant issue references in the commit or pull request.

## Pull requests and review

Use the pull request template. Explain the change, relevant issue, compatibility
impact, and validation performed. Record exact commands, outcomes, and the
tested source/artifact identity. Clearly separate tests passed, failed, skipped,
and not run, with reasons. State when hardware qualification or a new
`SiteProfile` remains necessary; do not infer it from generic CI results.

Every pull request requires a human review before merging, including changes
created or reviewed with AI assistance. A human reviewer must assess correctness,
security, maintainability, scope, and evidence. Automated or AI review can
supplement but never replace that review. Maintainers should enforce this policy
with protected-branch or ruleset settings on the default branch and required CI
checks; repository files alone cannot enable those settings.

## AI-generated and AI-assisted contributions

- **Remain accountable.** The human author is responsible for the accuracy,
  quality, appropriateness, and consequences of every contribution. Responsibility
  is not transferred to a model, agent, or tool.
- **Understand and review the work.** Review proposed code line by line, verify
  its provenance and licensing, and be able to explain the changes. Critically
  check AI output for correctness, security, maintainability, clarity, scope,
  documentation, and reproducibility. Validate it in proportion to its impact.
- **Disclose AI assistance.** State in the pull request whether AI tools were
  used, which parts they helped produce or review, and how a human checked the
  output. Clearly identify primarily AI-generated code, analysis, and artifacts.
  When there is no pull request, disclose assistance in the commit description.
  Keep human author metadata; do not add agent co-author trailers by default.
  Disclosure does not attest that human review has occurred.
- **Keep review human-accountable.** AI feedback is optional supporting input.
  A human reviewer must review every pull request and is responsible for any
  review feedback they adopt.
- **Protect information.** Never send proprietary or personal information to
  code generators or AI tools. Do not submit secrets, restricted datasets,
  private prompts, or confidential operational details in contributions.
- **Be transparent and constructive.** Assume goodwill, discuss limitations
  openly, and share useful lessons from AI-assisted work.

## Licensing and provenance

Unless separately agreed, contributions are provided under the project's
[Apache License 2.0](LICENSE). Contributors retain copyright in their work;
contributing does not require assigning copyright to the maintainers. You must
have authority to contribute the material, including any required employer or
third-party permission. Preserve applicable copyright, license, and attribution
notices, including notices for third-party material, and identify its source.
Do not assume that AI-generated material is free of third-party obligations.

## Questions

For non-sensitive questions, open a GitHub issue with enough context for others
to help. Use the private reporting instructions in [SECURITY.md](SECURITY.md)
or the [Code of Conduct](CODE_OF_CONDUCT.md) for sensitive concerns.
