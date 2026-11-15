# gptqueue Docs

Start with the [Quickstart](./QUICKSTART.md) to install GPTQueue, connect two
agents, and complete an acknowledged task.

## Operations

- [Operating rules](./OPERATING_RULES.md) — delivery semantics, acknowledgement, recovery and ownership.
- [Queue capacity](./QUEUE-CAPACITY.md) — waking and quiet mail quotas and admission rules.
- [Watchdog](./WATCHDOG.md) — supervise actor presence and diagnose runtime status.
- [Migration](./MIGRATION.md) — session-aware Redis keys and transport migration.
- [Acceptance](./ACCEPTANCE.md) — integration criteria and verification procedures.

## Runtime integrations

- [Claude Code integration](./CLAUDE_CODE_INTEGRATION.md) — session identity, hooks and inbox watcher.
- [Automatic shell registration](./AUTOMATIC_SHELL_REGISTRATION.md) — Codex and Pi registration, activation and recovery.
- [OpenCode integration](./OPENCODE_INTEGRATION.md) — plugin setup and session lifecycle.
- [Experimental registered wrapper](./EXPERIMENTAL_REGISTERED_WRAPPER.md) — opt-in identity-bound wrapper and its limitations.
