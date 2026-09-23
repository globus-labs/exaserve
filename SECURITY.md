# Security policy

## Reporting a vulnerability

Do not disclose suspected vulnerabilities, exploit instructions, credentials, or
private operational details in public GitHub issues or pull requests.

Send vulnerability reports privately to the current maintainer, Wenyi Wang, at
[wenyiw@uchicago.edu](mailto:wenyiw@uchicago.edu). Do not use a public issue as a
substitute for a private vulnerability report.

In a private report, include:

- A description of the issue and its potential impact.
- The exact source commit or package version and artifact hash, if known.
- The affected configuration, site/profile, scheduler, vendor runtime, engine,
  gateway, and relevant dependency versions.
- Minimal reproduction steps and sanitized evidence; share only what is needed.
- Any known mitigation or proposed fix.

Do not send live credentials, access tokens, private keys, personal information,
restricted datasets, or sensitive prompts. If credentials may be compromised,
follow your organization's credential-revocation and incident-response process.
Reports affecting a shared HPC system should also follow the site's security
reporting procedure; this project cannot replace the site's incident response.

## Versions and qualification

ExaServe is under active production hardening. There is no published long-term
security-support or backport matrix. Report suspected vulnerabilities against
any identifiable revision; maintainers must determine affected revisions and
the appropriate remediation scope. Do not infer support from a version number,
the existence of a development branch, or a passing CI check.

The [README](README.md) and [hardening status](doc/hardening/STATUS.md) describe
the intended runtime boundary, exact candidates, superseded evidence, and open
qualification gates. Qualification is specific to the artifact and deployment
dimensions; it is not a guarantee that a deployment is free of vulnerabilities.
The current production-candidate design assumes a trusted allocation-internal
network and is not a public Internet service security boundary.

## Coordination and updates

Maintainers should coordinate investigation, fixes, and disclosure privately,
then publish appropriate remediation guidance and security advisories after
assessing exposure and affected users. No response-time or fix-time service
level is currently promised. Avoid public disclosure of actionable details
while a private report is being assessed; agree on disclosure timing with the
maintainers and affected operators.
