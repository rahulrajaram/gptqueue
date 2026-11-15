# Security policy

## Reporting a vulnerability

Please do **not** open a public issue for a security problem.

Use GitHub's private vulnerability reporting for this repository:
**Security -> Advisories -> Report a vulnerability**
(https://github.com/rahulrajaram/gptqueue/security/advisories/new).

Include the affected version/commit, a description, and a minimal reproduction. We will
acknowledge the report and coordinate a fix and disclosure.

## Supported versions

Security fixes are applied to the latest `0.1.x` release line on `master`. Older snapshots
are not maintained.

## Scope notes for operators

- GPTQueue binds its HTTP transport to loopback by default. If you expose it, put an
  authenticating reverse proxy in front of it; the built-in token auth is optional.
- Redis is a trust boundary. Do not expose an unauthenticated Redis instance to a network.
- Never commit credentials, personal data, or private paths. The repository's CI and
  contribution rules expect hermetic tests.
