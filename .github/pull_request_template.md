## Summary

Describe the change, motivation, related issue, and relevant contracts.

## Scope and compatibility

- Type: bug fix / feature / documentation / maintenance / breaking change
- Configuration, API, lifecycle, readiness, or cleanup impact:
- Documentation and migration notes:
- Relationship to the authoritative production-hardening execution plan:

## Validation evidence

Record the exact tested commit or artifact identity, commands, environment,
results, and sanitized evidence locations. Do not attach credentials, private
prompts, restricted data, or full environment dumps.

- Passed:
- Failed:
- Skipped, with reasons:
- Not run, with reasons:
- Compute session/site/runtime and artifact hashes for any live validation:

Generic CI or unit tests do not qualify a hardware platform or deployment scale.
All cluster execution must follow AGENTS.md.

## Qualification impact

- Existing qualified dimensions affected, if any:
- Unqualified platforms or dimensions, if any:
- New or changed `SiteProfile` required: yes / no / not applicable
- Pending installed-wheel, hardware/scale, or owner-approval gates:

Do not promote historical or different-snapshot results into evidence for this
change. State explicitly when no live hardware tests were run.

## AI assistance

- AI tools used: none / list tools
- Code, analysis, documentation, or review they assisted with:
- Human review and validation of their output:

The author remains accountable for all submitted work. Review generated code
line by line and disclose primarily AI-generated artifacts. Never send
proprietary or personal information to AI tools. AI review does not replace the
required human review.

## Author checklist

- [ ] I understand and have reviewed the complete change.
- [ ] I added appropriate regression tests or explained why they are not needed.
- [ ] I updated affected documentation and examples.
- [ ] I reported failures, skips, and tests not run without implying qualification.
- [ ] I preserved existing evidence and unrelated work.
- [ ] I checked that the contribution contains no secrets or restricted information.
- [ ] I disclosed AI assistance and checked provenance/licensing where relevant.

## Required human review

- [ ] A human reviewer has reviewed this pull request before merge.

This box is for the human reviewer. Automated and AI reviews may supplement but
cannot satisfy this requirement; branch protection/rulesets must enforce the
repository's review policy.
