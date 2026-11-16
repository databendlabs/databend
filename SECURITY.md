# Security Policy

We appreciate every effort to discover and responsibly disclose security vulnerabilities in Databend.

## Reporting a Vulnerability

Please do **not** open a public GitHub issue for security problems.

Email **security@databend.com** with:

- a description of the issue and its impact
- the Databend version or commit you tested against
- steps or a script to reproduce (SQL, `curl`/`mysql` transcripts, crafted files, or a Rust test)
- the user/role you ran as, for isolation or privilege findings

We will acknowledge your report within 3 business days, keep you informed of progress, and credit you
in the release notes unless you prefer otherwise.

## Scope

In scope: anything reachable by a database user over the HTTP, MySQL or Arrow Flight SQL handlers,
including cross-tenant access, authentication or RBAC bypass, row access policy / data masking bypass,
credential exposure, SSRF through stage or connection URIs, memory safety when parsing staged files or
table metadata, and UDF sandbox escapes.

Out of scope: issues that require operator-level access (configuration files, CLI flags, the admin API
and metrics ports), vulnerabilities in dependencies that Databend does not reach, and findings that only
affect test harnesses or CI scripts.

## Supported Versions

Security fixes land on `main` and are included in the next nightly and stable release. We do not
backport to older release lines unless the issue is critical and widely deployed.
