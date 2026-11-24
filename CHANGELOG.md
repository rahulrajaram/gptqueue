# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-11-24

### Changed
- Published on npm as `@rahulrajaram/gptqueue` after npm refused the unscoped
  name. Installed command names are unchanged.

### Added
- Tag-triggered releases use npm trusted publishing (OIDC) to stage packages
  with provenance for maintainer approval with 2FA.

## [0.1.0] - 2026-11-22

Initial public release.

### Changed
- Node.js 22 is the minimum supported version (`engines` and CI now cover 22 and 24); Node.js 20 reached end of life in
  April 2026.

### Added
- MCP server over stdio and Streamable HTTP, backed by Redis.
- Durable agent registration and session leases; bounded per-agent inboxes.
- At-least-once task delivery with `claim_tasks` / `acknowledge_tasks`, lease renewal,
  and bounded lazy recovery of expired claims.
- Caller-bound recovery helpers: `claim_details`, `release_claim`, `list_my_claims`.
- Dead-letter queue with `dlq_status` / `dlq_requeue` and dead-letter notices.
- Inbox wake events (Redis Streams) so idle runtimes wake on new mail.
- Registered-shell sidecars for Claude Code, Codex, Pi and OpenCode, plus a headless
  worker mode.
- PTY wrapper and Claude Code hook/stop-gate integration.
- Immutable, verified runtime releases with readiness-gated activation and rollback
  (`scripts/install-runtime.mjs`).
- A private, socket-only Redis started by the test suite for hermetic tests.

[Unreleased]: https://github.com/rahulrajaram/gptqueue/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/rahulrajaram/gptqueue/compare/v0.1.0...v0.1.1
