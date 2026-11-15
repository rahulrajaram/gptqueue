# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - unreleased

Initial public release candidate.

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

[Unreleased]: https://github.com/rahulrajaram/gptqueue/compare/v0.1.0...HEAD
