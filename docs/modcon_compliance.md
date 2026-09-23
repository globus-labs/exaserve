# AI-ModCon repository preparation and transfer checklist

This checklist applies the **Required Elements for ModCon Base Public
Repositories** in [AI-ModCon/BaseTemplate](https://github.com/AI-ModCon/BaseTemplate/blob/fcfc042ffe287af9526520dd3e10b88dcd127d3b/README.md),
reviewed at commit `fcfc042ffe287af9526520dd3e10b88dcd127d3b` on 2026-09-23.
It records repository preparation, not approval of a production release,
completion of an organization transfer, or new hardware qualification.

## Requirement mapping

| BaseTemplate requirement | ExaServe implementation | Remaining gate |
|---|---|---|
| Open-source license and package declaration | [LICENSE](../LICENSE), [NOTICE](../NOTICE), Apache-2.0 metadata in [pyproject.toml](../pyproject.toml) | Maintainers retain responsibility for contribution and third-party rights |
| `docs/`, README, onboarding and examples | [Documentation index](index.md), [getting started](getting_started.md), [usage](usage.md), [API](api.md), [FAQ](faq.md), [README](../README.md) | Maintain links and examples as the project evolves |
| pytest suite and coverage | Existing three test trees; explicit coverage configuration and CI reports | Run and review the updated CI; coverage is not hardware evidence |
| CI/CD test integration | Existing hermetic, randomized-order, installed-wheel, Go, ledger and static jobs retained; portable lock lane added | Require these checks in the destination repository rules |
| Contribution and AI-assistance policy | [CONTRIBUTING](../CONTRIBUTING.md), [PR template](../.github/pull_request_template.md) | A human must review and approve changes |
| Conduct policy and issue procedures | [CODE_OF_CONDUCT](../CODE_OF_CONDUCT.md), [issue templates](../.github/ISSUE_TEMPLATE), [SECURITY](../SECURITY.md) | Maintain the private contact and triage reports |
| Main-branch protection | Administrator instructions below | **Requires GitHub settings; repository files do not enforce this** |
| Project, author/contact, dependency and Python metadata | [pyproject.toml](../pyproject.toml); Wenyi Wang, `wenyiw@uchicago.edu` | Update URLs and contacts when the transfer occurs |
| Development automation and lock | [Makefile](../Makefile), [uv.lock](../uv.lock), [dependency profiles](../requirements/README.md) | Run the locked portable CI on both declared CI interpreters |
| Genesis Mission acknowledgment | [README acknowledgment](../README.md#acknowledgments) credits support for the ModCon Base collaboration | Confirm project-specific wording if an institution requires it |

The additional changelog and private security policy follow the template's
repository conventions. No automatic PyPI publication, external coverage
service, new access token, or unreviewed deployment workflow is enabled.

## Adaptations that preserve ExaServe's contracts

- Keep `src/exaserve`, rather than moving the package to match the example
  layout. Keep `doc/PRODUCTION_HARDENING_EXECUTION_PLAN.md` authoritative and
  retain historical artifact paths. `docs/` is an onboarding/navigation layer.
- Preserve Python `>=3.10` package metadata and the existing 3.10/3.12 portable
  CI matrix. The uv resolution envelope is Linux and Python 3.10–3.13 because
  the existing optional vLLM extra excludes Python 3.14. This does not qualify
  another interpreter, operating system, engine or accelerator.
- Preserve existing server extras and site runtime pins. The default uv groups
  install portable development/test tools, not Ray, vLLM or PyTorch. The lock
  contains optional dependency resolutions too; their presence does not mean
  they are installed by default or qualify an HPC stack.
- Keep the standalone `requirements/ci.lock` installation for the independent
  installed-wheel gate. The portable uv lane checks installed versions against
  that profile and rejects accidental installation of the GPU stack.
- Keep exact source-distribution/package-member checks. Release input staging
  explicitly includes the license, notices and lock; wheel auditing verifies
  SPDX metadata and the exact packaged license/notice bytes.
- Do not copy BaseTemplate's broad recursive cleanup commands, placeholder
  security email, unsupported version table, funding identifiers, Python 3.14
  support claim, or automatic publishing setup.
- Do not add a Frontier profile or change serving/compatibility behavior in this
  repository-conventions pass. Hardware evidence remains tied to its original
  commit, profile, dependencies and configuration.

## GitHub administrator steps

These steps are **not completed by adding files** and must be reviewed by the
repository owners. Do not mark this checklist complete until the actual settings
and successful CI runs have been checked.

**Owner decision, 2026-09-23:** branch-protection changes are deferred. Proceed
with the approved commit/push to `main` without changing GitHub administration
settings. Keep branch protection as a follow-up before declaring all ModCon
hosting requirements complete; this deferral is not a claim that protection is
already enabled or that the template requirement has been waived.

1. Confirm the destination repository name and transfer authority. Preserve
   commit history, branches, tags, releases, issues and evidence links; do not
   create a replacement history by copying files into an empty template repo.
2. Confirm the destination's Actions policy permits the pinned third-party
   actions in [CI](../.github/workflows/ci.yml). Use read-only workflow token
   permissions and do not expose privileged credentials to untrusted PR code.
3. Protect `main` with a ruleset/branch protection requiring pull requests,
   at least one approval from a human reviewer other than the author, dismissal
   of stale approvals, resolved review conversations, and required CI checks.
   Restrict bypass permissions; disable force pushes and branch deletion.
   Configure actual human/team ownership after the team names and access are
   confirmed; do not use a guessed CODEOWNERS entry.
4. After the first destination CI run, require the exact emitted checks:
   `portable uv lock (py3.10)`, `portable uv lock (py3.12)`,
   `Go replay client tests`, `hermetic tests (py3.10, fixed)`,
   `hermetic tests (py3.12, fixed)`, `hermetic tests (py3.12, random)`,
   `packaged wheel test (not an editable checkout)`,
   `findings ledger validator`, and `format + lint + type + security`.
   Reconcile these names with the actual runs before enforcing the rule.
5. Enable private vulnerability reporting if the organization uses it. The
   published email route works independently of that GitHub feature; confirm
   a responsible maintainer monitors it. Do not publish reports as issues.
6. Update the live repository/documentation/issue URLs in `pyproject.toml`,
   README, docs and community guidance to the confirmed destination. Current
   links intentionally point to `globus-labs/exaserve`, not a presumed future
   repository. Update maintainer contact details whenever ownership changes.
7. Review any future package publishing separately: registry ownership, release
   evidence, protected environments and trusted publishing are distinct actions.
   Do not introduce a publish token or deploy-on-push workflow as part of transfer.

## Validation boundary for this preparation

Initial preparation inspected metadata, resolved/checked the dependency lock,
parsed configuration, checked documentation links, and performed small static
checks. It did not launch experiments, compute allocations, full test suites,
or package builds. The subsequently approved push to `main` can trigger the
configured GitHub-hosted CI; this does not authorize resuming HPC experiments
or adding a Frontier profile. The updated CI and package gates must run before this candidate is
declared verified. Prior 1,773-test results belong to the earlier scaling
candidate, not these repository/packaging changes.

The known sequential 405B model-staging storage failure, missing 64-/128-node
paper points, and stale paper selectors are not repaired by this preparation.
They remain separate work; a repository transfer is not evidence of closure.

### Static checks performed in this pass

- Generated the lock with uv 0.10.1 using metadata-only resolution, disabled
  third-party builds and Python downloads; 241 packages are resolved across
  optional and default dependency branches. No environment sync or GPU
  installation was performed.
- Offline locked export matches all 33 portable CI pins for the 3.10 and 3.12
  CI interpreter environments, with no Ray, vLLM or PyTorch in the default set.
- Verified that the published Python requirement, existing optional/runtime
  dependencies, and build-system pins are unchanged.
- Parsed Python, TOML and workflow YAML; checked shell/Makefile command syntax,
  retained CI jobs, local Markdown links, Ruff correctness/style, and whitespace.
- Added regression cases for missing/modified license artifacts and wrong SPDX
  wheel metadata, but did not run tests or builds in this preparation pass.

CI execution, a fresh package gate, GitHub protection settings and transfer
remain pending. None of the static checks is reported as a hardware experiment
or a passing release qualification.
