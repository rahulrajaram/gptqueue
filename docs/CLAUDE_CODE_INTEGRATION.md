# Claude Code integration

Entry points in `bin/` are thin Node shims over `dist/claude/*` (run `npm run build` first):

| Entry | Role |
|---|---|
| `bin/gptqueue-claude-mcp` | stdio MCP server for Claude Code; registers under the project's agent name and publishes the name it holds for the session |
| `bin/gptqueue-claude-hook session-start\|stop` | SessionStart context and Stop gate (hook JSON on stdin) |
| `bin/gptqueue-inbox-peek` | non-consuming inbox counts and the event-driven background watcher |
| `bin/gptqueue-claude-name [dir]` | fallback name: `GPTQ_AGENT_NAME`, then the longest prefix in `~/.config/gptqueue/claude-agent-names.tsv`, then `claude-<git top-level basename>` |
| `scripts/verify-claude-mcp.mjs` | drive the MCP server over stdio as Claude would (`selftest`, `send`, `claim`, `list`, `unregister`) |

## Which inbox a session uses

The MCP server, running in-process under `gptqueue-claude-mcp`, is a direct child of the Claude
process. After every identity change (startup registration, `register_agent`, `close_session`,
unregister) it writes `session-<claude pid>-<claude start time>.json` with the agent name and
gptqueue session id. It removes the file on exit. Hooks, peek and the watcher are also
descendants of that Claude process. They walk their `/proc` ancestry and use the record of the
nearest ancestor that has one. The start time (`/proc/<pid>/stat` field 22) keeps a recycled pid
from matching an old record. A record whose server process is gone, or whose server pid now has a different start time, is ignored. Set `GPTQ_CLAUDE_PID`
when a launcher sits between Claude and the server.

Sessions whose MCP server predates session records (started before the wrapper swap) are
confirmed another way. The old wrapper exported `GPTQ_AGENT_NAME` and exec'd
`node .../dist/mcp-server/index.js`, so that server is a direct child of its Claude process.
With no record, hooks and the name-free watcher look at the direct children of their nearest
ancestors. They read `/proc` only for processes owned by the same uid. If exactly one such
server is found under the nearest ancestor that has any, its `GPTQ_AGENT_NAME` is confirmed
(source `legacy_env`) and the Stop hook gates on it. If several are found, the name stays
unconfirmed. `GPTQ_CLAUDE_LEGACY_SCAN=off` disables the scan.

If no record exists, the hook falls back to the old resolution (on `CLAUDE_PROJECT_DIR`, then the
hook's `cwd`) and marks the name `UNCONFIRMED` in its output.

If a live session already holds the name (`AGENT_NAME_HELD`), the server registers
`<name>-<first 8 chars of CLAUDE_SESSION_ID>`, or `<name>-<claude pid>` when that variable is
absent, and records that name instead. `GPTQ_AGENT_SHARED=1` opts into sharing the name instead.
On a clean exit the server closes its session, so a resumed session can take the name back
straight away.

Runtime directory: `$GPTQ_CLAUDE_RUNTIME_DIR`, else `$XDG_RUNTIME_DIR/gptqueue-claude`, else
`<tmpdir>/gptqueue-claude-<uid>`. It is created with mode 0700. The tools refuse to use it if
another user owns it or the group or others can access it.

### Startup ordering and name collisions

- Claude Code runs SessionStart before the MCP server has registered. SessionStart waits up to
  2 s (`GPTQ_CLAUDE_RECORD_WAIT_MS`) for the session record. If none appears, it reports the name
  as UNCONFIRMED, reports no counts for the guessed name (which may belong to another session in
  the same directory), and still gives the watcher command.
- The watcher command carries no name: `gptqueue-inbox-peek --watch --session S`. The watcher
  resolves this session's registered name from the record, waiting up to 40 s (longer than the 32 s worst-case
  registration wait below). If no record
  appears, it exits 5 with `{"error":"session_identity_unconfirmed"}` instead of watching a
  guessed mailbox. An explicit `--name` always wins, as in the old script.
- A Stop hook with no record allows the stop with a visible `systemMessage` and does not gate on
  the guessed name.
- If the requested name is held (`AGENT_NAME_HELD`), the server tries these names in order, at
  most four: `<name>-<first 8 chars of CLAUDE_SESSION_ID or CLAUDE_CODE_SESSION_ID>`, then
  `<name>-<claude pid>`, then two random `<name>-<8 hex>` names. A Claude started from inside
  another Claude inherits the parent's session id, which is why the later names do not use it.
  If every name is held, startup fails with a clear `AGENT_NAME_HELD` error. The record's
  `name_origin` says which kind of name was used (`requested`, `session_id`, `pid` or `random`).
- A holder registered by older code has no pid or hostname metadata, so the core reports
  `holder_liveness_verifiable: false` and `holder_lease_ttl_ms` in the error details. The server
  then waits for that lease to lapse and retries the stable name, for at most lease TTL + 2 s
  (32 s). It falls back only if the name is still held after that. Under Claude, registration
  runs in the background: `initialize` is answered at once, and `tools/call` requests wait until
  registration has settled.

## Stop gate

- The hook counts three things separately: unclaimed messages, tasks this session has claimed
  but not acknowledged, and tasks held by other sessions. It blocks while the first two add up to
  more than zero.
- On a Redis error, the hook blocks once and reports the error. When Claude re-enters the hook
  (`stop_hook_active`) and the error persists, the stop goes through with a visible
  `systemMessage`.
- On a re-entered stop, the hook blocks again only if new mail arrived after its last block. It
  checks this through the `inbox-events` stream id, kept in `stop-<session_id>.json`.
- If the inbox is empty and no watcher is running for this Claude `session_id`
  (`watch-<session_id>.pid`), the hook blocks once and gives the exact command to start one.
  `GPTQ_CLAUDE_WATCH=off` turns this reminder off.

## Watcher

`gptqueue-inbox-peek --watch --name N --session S` reads the newest `inbox-events` id, then checks
LLEN, then waits with `XREAD BLOCK` on that stream. Every send appends to the stream in the same
Lua call as the RPUSH, so the watcher wakes on the event without polling. It rechecks LLEN at
least every `--block-ms` (default 5000), which catches requeued mail that emits no event. While
Redis fails it falls back to an LLEN check every second, and exits 4 after 30 s of continuous
failure. With an explicit `--name` (the legacy invocation) it keeps the old script's behaviour
instead: it retries at `--interval` (default 30 s) and never exits on a Redis error. It never pops a message. It prints one JSON line and exits 0 when mail arrives. Started
with `run_in_background`, its exit wakes the idle session.

## Install note (not applied)

1. `npm run build` in the deployed checkout (`<path-to-gptqueue>`).
2. Replace the untracked `~/.local/bin` scripts with symlinks to the checkout. The shims resolve
   `dist/` relative to their real path.
   ```
   ln -sf <path-to-gptqueue>/bin/gptqueue-claude-mcp   ~/.local/bin/gptqueue-claude-mcp
   ln -sf <path-to-gptqueue>/bin/gptqueue-claude-hook  ~/.local/bin/gptqueue-claude-hook
   ln -sf <path-to-gptqueue>/bin/gptqueue-claude-name  ~/.local/bin/gptqueue-claude-name
   ln -sf <path-to-gptqueue>/bin/gptqueue-inbox-peek   ~/.local/bin/gptqueue-inbox-peek
   ```
   `gptqueue-claude-verify.mjs` becomes `scripts/verify-claude-mcp.mjs`.
3. In `~/.claude/settings.json`, only the hook timeouts change, from 10 s to 15 s. The commands
   stay the same if the symlinks are used. The hook entry (`dist/claude/hook-main.js`) loads only
   ioredis and the small `src/claude` modules: no MCP server, SDK or zod. Measured on 2026-10-05,
   median of 5 runs of `node bin/gptqueue-claude-hook stop` against a private Redis:
   8.8 s from a shared virtiofs mount under contention (worst run 8.9 s), and 0.2 s from a local
   tmpfs copy.
   ```
   "SessionStart": [{"matcher": "", "hooks": [{"type": "command", "command": "$HOME/.local/bin/gptqueue-claude-hook session-start", "timeout": 15}]}]
   "Stop":         [{"matcher": "", "hooks": [{"type": "command", "command": "$HOME/.local/bin/gptqueue-claude-hook stop", "timeout": 15}]}]
   ```
4. Keep the MCP server entry that runs `gptqueue-claude-mcp`. It must start the shim directly, not
   through a shell that stays alive, or the record will be keyed by the shell's pid.
