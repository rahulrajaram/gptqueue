# Live acceptance tests (real runtime CLIs)

These tests drive the installed runtime CLIs with this checkout's gptqueue build to
prove what the fake hosts in `tests/acceptance-e2e/` cannot: WK-1 idle wake with the
DL-5 exact-exchange oracle (`tests/acceptance/oracle.ts`), WK-2 busy-then-next-turn,
ID-5 distinct identities for two sessions in one directory, and real LT-3
(send to turn start) and LT-4 (turn end to next turn start). They are skipped unless
`GPTQUEUE_LIVE_ACCEPTANCE=1`, so the normal suite is unaffected. They spend real model
calls; run one file at a time, after `npm run build`:

    GPTQUEUE_LIVE_ACCEPTANCE=1 npx --no-install vitest run tests/acceptance-live/codex.live.test.ts

| File | Runtime wiring | Model |
|---|---|---|
| `codex.live.test.ts` | private `codex app-server --listen unix://<receipt>/app.sock --disable hooks`; per-invocation `-c` disables every user MCP server and points `gptqueue-shared` at `bin/gptqueue-session --client codex` with the private Redis and `GPTQUEUE_CODEX_APP_SERVER_SOCKET`; the sidecar binds itself; turns observed from `turn/started` / `turn/completed` notifications | `gpt-6-luna` |
| `pi.live.test.ts` | two `pi --mode rpc` processes in one directory, `-ne -e <generated wrapper of dist/registered-shell/pi-extension.js>`, no skills/context/builtin tools, sessions under the receipt dir; turns observed from RPC `agent_start` / `turn_start` / `turn_end` events | `zai/glm-5.3-flash` |
| `claude.live.test.ts` | interactive `claude` under a PTY (operator decision D5: it may write only its own transcript under `~/.claude/projects` and update `~/.claude.json`); `--strict-mcp-config` with this build's `bin/gptqueue-claude-mcp`, `--settings` with this build's SessionStart/Stop hooks, `--setting-sources local`; the model starts the `gptqueue-inbox-peek --watch` watcher as the hook says | `claude-opus-5-5` |

Isolation: vitest starts the run's private socket-only Redis, and `support.ts`
refuses any other Redis (every TCP URL included); gptqueue logs and state for
spawned sidecars go to the receipt directory.
Receipts (`receipt.json`, runtime logs) are written to
`$GPTQUEUE_LIVE_RECEIPT_DIR/<runtime>-<timestamp>/` (default directory: `gptqueue-live-receipts`
under the OS temporary directory); they are never committed.

In `codex.live.test.ts`, thread A gets one operator turn before the WK-1 measurement so
it covers an already-materialized thread; a second, normal test wakes a bound thread that
was never prompted (Codex rejects `thread/turns/list` on it as not materialized, which the
runtime treats as "no earlier submission"). LT-3/LT-4 here are single samples, not p95.
