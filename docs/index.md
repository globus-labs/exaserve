# ExaServe documentation

ExaServe is an allocation-scoped control plane for Ray Serve-based,
OpenAI-compatible LLM inference. Its Aurora runtime combines immutable plans,
supervised processes, and generation-bound readiness. Portable Python tooling
and hardware-qualified serving are separate support boundaries.

## Start here

- [Getting started](getting_started.md): portable development and Aurora setup.
- [Usage](usage.md): configuration, submission, status, evaluation, and ClientLab.
- [API reference](api.md): public plan and status interfaces and installed commands.
- [FAQ](faq.md): environment, qualification, readiness, and contribution questions.
- [AI-ModCon checklist](modcon_compliance.md): requirement mapping, validation
  limits, and administrator steps for the organization transfer.

## Architecture and operational authority

The established `doc/` tree remains in place. This `docs/` directory is a reader
entry point, not a replacement specification.

| Topic | Authoritative document or entry point |
| --- | --- |
| Safety, permissions, and Aurora compute sessions | [Repository operating rules](../AGENTS.md) |
| Architecture, implementation, and acceptance gates | [Production-hardening execution plan](../doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md) |
| Candidate identities and qualification caveats | [Hardening status](../doc/hardening/STATUS.md) |
| Implemented boundaries and design notes | [Design index](../doc/design/README.md) |
| Submit-to-readiness control flow | [Call graph](../doc/design/call_graph.md) |
| Dependency and site-stack separation | [Dependency profiles](../requirements/README.md) |
| Evaluation control plane | [Eval design](../eval/DESIGN.md) |
| Client measurement studies | [ClientLab](../clientlab/README.md) |

`AGENTS.md` governs where commands may run. The production-hardening execution
plan is the sole architecture and implementation specification; audits, TODOs,
historical results, and these introductory pages cannot override it. Read
supersession notices in the status documents before reusing qualification
evidence. Historical runs do not qualify a changed artifact.

## Project and community

- [Project overview](../README.md)
- [Contribution guidelines](../CONTRIBUTING.md)
- [Code of conduct](../CODE_OF_CONDUCT.md)
- [Security reporting](../SECURITY.md)
- [Changelog](../CHANGELOG.md)
- [Apache License 2.0](../LICENSE) and [notice](../NOTICE)
- [Source repository](https://github.com/globus-labs/exaserve)
- [Issue tracker](https://github.com/globus-labs/exaserve/issues)

For a bug report, include the command, package or commit identity, environment,
and relevant redacted errors. Do not include credentials, private model data,
or unredacted deployment artifacts in public issues.
