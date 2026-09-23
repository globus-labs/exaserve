---
name: Bug report
about: Report a reproducible ExaServe defect with sanitized evidence
title: "[BUG] "
labels: bug
assignees: ''
---

<!-- For suspected vulnerabilities, follow SECURITY.md instead of filing a
public issue. Do not attach secrets, environment dumps, private prompts,
restricted data, or sensitive site details. -->

## Summary

Describe the observed problem and its impact.

## Expected and actual behavior

- Expected:
- Actual:
- Is this a runtime failure or an intentional qualification/configuration rejection?

## Minimal reproduction

Include the exact command and smallest sanitized configuration that reproduces
the problem. Describe the steps and whether it reproduces consistently. Follow
AGENTS.md for any cluster execution; do not reproduce heavy workloads on a login
node.

```yaml
# Minimal sanitized configuration
```

## Exact source and environment

- Source commit, branch, and relevant local changes:
- Package version and wheel SHA-256 (if using an installed artifact):
- OS and Python version:
- Site and SiteProfile identity/hash:
- Scheduler and version:
- Vendor/device and runtime version:
- Engine and version:
- Ray version and compatibility profile/manifest identity:
- Gateway and version:
- Logical nodes / physical allocated nodes, GPUs per node, TP/PP layout:
- Compute session type and environment setup used (if applicable):
- Known qualification status for this exact candidate and deployment dimensions:

## Status and sanitized logs

Include the first-cause error, relevant canonical status, and short stdout/stderr
excerpts around the initial failure. Include sanitized generation/run identity
and timestamps when helpful. Distinguish scheduler state from canonical READY;
a listening port or scheduler RUNNING state does not prove readiness.

```text
First-cause status and relevant sanitized log excerpts
```

## Additional context

Describe recent changes, attempted workarounds, and related issues. Mark missing
evidence as unknown or not collected rather than guessing.
