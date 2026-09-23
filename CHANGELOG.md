# Changelog

This records project changes, not an assertion that a deployment or release has
passed hardware qualification. Exact artifacts and scope are recorded in
[`doc/hardening/STATUS.md`](doc/hardening/STATUS.md) and the linked evidence.

## Unreleased

### Repository preparation for AI-ModCon

- Adopt Apache-2.0 licensing and explicit maintainer/contact metadata.
- Add contribution and conduct policies, private security reporting, and issue
  and pull-request templates with human-review and AI-assistance disclosure.
- Add a `docs/` navigation and onboarding layer while retaining the existing
  `doc/` architecture contracts, design notes, and immutable evidence references.
- Add locked portable development tooling, coverage configuration, build
  automation, and a transfer/branch-protection checklist.
- Preserve the existing hermetic, randomized, installed-wheel, static, and Go
  CI gates; do not imply new hardware validation from repository housekeeping.

### Existing scaling and hardening baseline

`main` incorporates the history through `044d3ef` from
`paper/missing-scale-v0.4.0`: head-owned source/model distribution, typed MPI
replay aggregation, bounded lifecycle ownership, exact readiness evidence,
node-local compatibility state, and qualified finite allocation-subset tooling.

The 405B sequential campaign's model-staging storage failure and missing
64-/128-node measurements remain unresolved. Paper results and earlier package
gates do not establish a complete production release or Frontier support.

## Historical 0.4.0 candidate

The package version remains `0.4.0`; this entry does not create a release or tag.
The `release-v0.4.0` baseline and `v0.4.0-rc2` evidence are preserved in
[`doc/hardening/STATUS.md`](doc/hardening/STATUS.md). Its supersession notice
explains why earlier artifacts do not automatically qualify the current tree.
