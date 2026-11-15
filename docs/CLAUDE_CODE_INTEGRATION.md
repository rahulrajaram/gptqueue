# Claude Code integration

Entry points in `bin/` are thin Node shims over `dist/claude/*` (run `npm run build` first):

| Entry | Role |
|---|---|
| `bin/gptqueue-claude-mcp` | stdio MCP server for Claude Code; registers under the project's agent name and publishes the name it holds for the session |
| `bin/gptqueue-claude-hook session-start\|stop\|user-prompt-submit\|pre-tool-use` | SessionStart context, Stop gate, turn state, and the optional AskUserQuestion guard (hook JSON on stdin) |
| `bin/gptqueue-inbox-peek` | non-consuming inbox counts, the `--list` listing and the event-driven background watcher |
| `bin/gptqueue-claude-name [dir]` | fallback name: `GPTQ_AGENT_NAME`, then the longest prefix in `~/.config/gptqueue/claude-agent-names.tsv`, then `claude-<git top-level basename>` |
| `scripts/verify-claude-mcp.mjs` | drive the MCP server over stdio as Claude would (`selftest`, `send`, `claim`, `list`, `unregister`) |

## Which inbox a session uses

The MCP server, running in-process under `gptqueue-claude-mcp`, is a direct child of the Claude
process. After every identity change (startup registration, `register_agent`, `close_session`,
unregister) it writes `session-<claude pid>-<claude start time>.json` with the agent name and
gptqueue session id. It removes the file on exit, but only if the file is still its own (same
server pid and start time), so an old server's late exit never removes a newer server's record. Hooks, peek and the watcher are also
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
(source `legacy_env`) and the Stop hook gates on it, but only while `gptq:heartbeat:<name>`
exists. The old server refreshes that key and deletes it on `close_session` or a rename, so a
server that has since renamed or closed is reported as UNCONFIRMED. If several are found, the
name stays unconfirmed. `GPTQ_CLAUDE_LEGACY_SCAN=off` disables the scan.

If no record exists, the hook falls back to the old resolution (on `CLAUDE_PROJECT_DIR`, then the
hook's `cwd`) and marks the name `UNCONFIRMED` in its output.

If a live session already holds the name (`AGENT_NAME_HELD`), the server registers
`<name>-<first 8 chars of CLAUDE_SESSION_ID>`, or `<name>-<claude pid>` when that variable is
absent, and records that name instead. `GPTQ_AGENT_SHARED=1` opts into sharing the name instead.
On a clean exit the server closes its session, so a resumed session can take the name back
straight away.

Runtime directory: `$GPTQ_CLAUDE_RUNTIME_DIR`, else `$XDG_RUNTIME_DIR/gptqueue-claude`, else
`<tmpdir>/gptqueue-claude-<uid>`. It is created with mode 0700. The tools refuse to use it if
another user owns it, it is a symlink, or the group or others can access it. Readers check
this without creating the directory and ignore records from an unsafe one (reported as an
ignored record). Records are opened without following symlinks, must be owned by the same uid,
and must carry an agent name of at most 128 characters from `[A-Za-z0-9._:@+-]`. Hook text names
the gptqueue session only by its `sid-sha256:` tag, never by the raw id.

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
  registration has settled. A transient failure (for example Redis unavailable) is retried in
  the background with capped exponential backoff (0.5 s doubling to 30 s); calls that wait
  longer than 45 s get that error with `retryable: true`, and calls after a later success go
  through. Only a non-retryable failure (such as `AGENT_NAME_HELD` after every fallback) is final.

## Stop gate

- The hook counts three things separately: unclaimed messages, tasks this session has claimed
  but not acknowledged, and tasks held by other sessions. Other sessions' claims whose lease has
  expired (claim-index score below Redis `TIME`), such as those of a crashed or replaced earlier
  session, are flagged too. It blocks while unclaimed, own and expired tasks add up to more than
  zero; `claim_tasks` recovers the expired ones.
- On a Redis error, the hook blocks once and reports the error. When Claude re-enters the hook
  (`stop_hook_active`) and the error persists, the stop goes through with a visible
  `systemMessage`.
- On a re-entered stop, the hook blocks again only if new mail arrived after its last block. It
  checks this through the `inbox-events` stream id, kept in `stop-<session_id>.json`.
- If the inbox is empty and no watcher is running for this Claude `session_id`
  (`watch-<session_id>.pid`), the hook blocks once and gives the exact command to start one.
  A watcher counts only if its pidfile names this session's Claude process (pid and start time),
  so a watcher orphaned by a crashed Claude does not satisfy a resumed session. It also counts
  only while its parent is still the launcher its pidfile records: a watcher started with `&` or
  disown, whose launching shell has exited, does not count, even before it notices and exits 7
  (see below). Pidfiles written before launcher tracking have no launcher and count by the
  other rules alone. The command uses
  the path the hook was invoked by (the installed wrapper or `<prefix>/current/bin`), not a
  resolved release directory.
  `GPTQ_CLAUDE_WATCH=off` turns this reminder off.

## Headless worker sessions

A `claude -p` child (print mode) inherits the user's hooks and the gptqueue MCP server, but it is
a one-shot worker, not a mailbox owner. Claude Code sets `CLAUDE_CODE_ENTRYPOINT=cli` for
interactive sessions and `CLAUDE_CODE_ENTRYPOINT=sdk-cli` for print mode, and hooks and MCP
servers inherit it. A session is a **worker** when (`src/claude/worker-mode.ts`):

```
GPTQUEUE_WORKER === "1"
  || (GPTQUEUE_WORKER !== "0" && CLAUDE_CODE_ENTRYPOINT === "sdk-cli" && GPTQ_AGENT_NAME unset or empty)
```

| Variable | Effect |
|---|---|
| `GPTQUEUE_WORKER=1` | force worker mode in any session |
| `GPTQUEUE_WORKER=0` | opt a print-mode session back in as a mailbox owner |
| `GPTQ_AGENT_NAME=<name>` | a deliberately named print-mode actor keeps today's behavior and owns its mailbox |

In worker mode:

- the MCP server (`gptqueue-claude-mcp`, and the plain server) does not auto-register from its
  environment: no mailbox, no session record, no lease. Its tools still work after an explicit
  `register_agent`. Exception: a name passed as a command-line argument to the plain server
  (`dist/mcp-server/index.js <name>`) is an explicit choice and registers even in worker mode,
  including under `GPTQUEUE_WORKER=1`. The Claude wrapper `bin/gptqueue-claude-mcp` clears argv, so
  this never applies to it;
- SessionStart prints only one context line ("headless worker session ... Set GPTQUEUE_WORKER=0
  to opt in."): no probe, no seat binding;
- Stop allows with no output and writes no state file; UserPromptSubmit records nothing;
  PreToolUse (the AskUserQuestion guard) is a no-op;
- the hook opens no Redis connection at all.

Recommended isolation for print-mode reviewers: launch them with `GPTQUEUE_WORKER=1` (explicit,
independent of how Claude Code labels the entrypoint), and, when the reviewer needs no gptqueue
tools, also without the gptqueue MCP server (for example `--strict-mcp-config` with an empty or
reviewer-specific MCP config) and in its own worktree. Never set `GPTQ_AGENT_NAME` for a reviewer
unless it should own a mailbox.

### Other entry points (sidecar, Pi, OpenCode)

`GPTQUEUE_WORKER=1` is honored at four entry points:

| Entry point | Check | Worker-mode behavior |
|---|---|---|
| Claude hooks and MCP (`gptqueue-claude-mcp`) | `isWorkerSession` (explicit or print-mode inference) | as described above |
| Sidecar binary `bin/gptqueue-session` (Codex `gptqueue-shared`) | `isExplicitWorker`, in the sidecar process | serves MCP, no Redis; tools return `HEADLESS_WORKER` |
| Pi extension | `isExplicitWorker`, in the Pi process before any connection | spawns no sidecar, registers no gptqueue tools; stays degraded with code `headless_worker` and no retry |
| OpenCode plugin | `isExplicitWorker`, in the OpenCode process before any backend exists | opens no Redis connection, registers nothing; session hooks are no-ops; `gptqueue_*` tools fail with `HEADLESS_WORKER` |

The Pi check runs in the Pi process because Pi starts the sidecar through the MCP stdio
transport, which forwards only a default environment whitelist; `GPTQUEUE_WORKER` would not reach
the sidecar.

#### Sidecar details

The registered-shell sidecar `bin/gptqueue-session`, which Codex (`gptqueue-shared`) and Pi
launch, is not a Claude print-mode child, so it honors only the explicit form
(`isExplicitWorker` in `src/claude/worker-mode.ts`): `GPTQUEUE_WORKER=1` selects worker mode;
`0`, unset or any other value keeps today's behavior. In worker mode the sidecar:

- opens no Redis connection: no registration, session record, presence or heartbeat, runtime
  binding, registration-audit event or inbox dispatcher;
- still serves MCP and lists its usual tools, so a client that marks the server required (Codex
  does) starts normally; every tool returns the error code `HEADLESS_WORKER` ("this session runs
  with GPTQUEUE_WORKER=1 and has no mailbox; unset it (or set 0) to use gptqueue");
- writes one stderr line at startup stating worker mode.

Codex passes only whitelisted environment variables to stdio MCP servers, so for a headless
`codex exec` worker the variable arrives only with
`-c 'mcp_servers.gptqueue-shared.env_vars=["GPTQUEUE_WORKER"]'` (plus `GPTQUEUE_WORKER=1` in the
caller's environment). The qualified alternative is not to start the sidecar at all:
`-c mcp_servers.gptqueue-shared.enabled=false`.

To prove a worker never registered, even transiently, query the registration audit for its time
window and directory: `gptqueue-doctor registrations --since ... --until ... --cwd <worktree> --json`
must return no events with `coverage: "complete"`, and every writer that could register in that
window must run an audit-emitting release (older releases write no events). The audit is
best-effort and never blocks a registration; a registration it could not record leaves an audit
gap marker that makes the window `incomplete` (see "Registration audit" in
AUTOMATIC_SHELL_REGISTRATION.md).

## Optional: AskUserQuestion guard

`AskUserQuestion` blocks the session until the operator answers. An orchestrator that asks while
leads are mailing it stalls them all. The guard is a PreToolUse hook that denies
`AskUserQuestion` **once** per session and latest inbox event while the session's confirmed inbox
has actionable mail (the same counts the Stop gate uses). The denial reason names the agent and
the counts and tells the model to claim and handle the mail first, then ask. Asking again without
new mail goes through; new mail since the denial denies again. The denial is recorded in the
runtime dir as `ask-<session>.json` (mode 0600, forced on an existing file) **before** the deny is
emitted. When it cannot be recorded (no usable `session_id`, an unsafe or unavailable runtime dir,
an existing state path that is not this user's regular file, or a failed write) the guard fails
open: no deny, only a `systemMessage` saying the guard is inactive. So it never denies the same
event twice.

It never blocks on gptqueue trouble: worker mode, an unconfirmed identity, or a failed probe let
the question through (a failed probe adds a `systemMessage`). It never emits
`permissionDecision: "allow"`, which would bypass permission prompts; other tools get no output.

It is **opt-in and off by default**; the operator enables it by adding this entry to
`~/.claude/settings.json` (`<bin>` is the directory holding `gptqueue-claude-hook`):

```
"PreToolUse": [{"matcher": "AskUserQuestion", "hooks": [{"type": "command", "command": "<bin>/gptqueue-claude-hook pre-tool-use", "timeout": 10}]}]
```

## Watcher

`gptqueue-inbox-peek --watch --name N --session S` reads the newest `inbox-events` id, then checks
LLEN, then waits with `XREAD BLOCK` on that stream. Every send appends to the stream in the same
Lua call as the RPUSH, so the watcher wakes on the event without polling. It rechecks LLEN at
least every `--block-ms` (default 5000), which catches requeued mail that emits no event. While
Redis fails it falls back to an LLEN check every second, and exits 4 after 30 s of continuous
failure. With an explicit `--name` (the legacy invocation) it keeps the old script's behaviour
instead: it retries at `--interval` (default 30 s) and never exits on a Redis error. It never pops a message. It prints one JSON line and exits 0 when mail arrives, or when a claim
on the inbox has expired (`expired_claims` in the line). It exits 6 when the Claude process it
serves has gone, checked every block period. An explicit `--dir D` names the inbox derived from
`D`, like `--name`, and ignores this session's record. Started
with `run_in_background`, its exit wakes the idle session.

Start the watcher only with the session's background-task mechanism (`run_in_background`), never
with `&`, `disown` or `nohup` inside a foreground command. Such a watcher's launching shell exits
when the command ends, so the watcher's own exit wakes nobody. With `--session` (the `--name` and
`--dir` modes without `--session` are unchanged):

- It records its parent process (pid and start time) as its launcher in the pidfile
  (`launcher_pid`, `launcher_start`), before the session-record wait.
- It exits 7 with `{"agent", "error": "watcher_orphaned", "message"}` once its parent is no
  longer that launcher, and removes its pidfile if the pidfile is still its own. It checks
  before writing the pidfile and then every block period, alongside the Claude-process check
  (exit 6 wins when both apply). A watcher already adopted by init (parent pid 1) when it starts
  exits 7 at once, before the record wait; its `agent` is the `--name`/`--dir` inbox or null.
  On a host where a child subreaper (for example a systemd user manager) adopts orphans instead
  of pid 1, such an immediate orphan is not caught at startup; a name-free one finds no session
  record and exits 5, and the Stop hook does not count it.
- It exits 8 with `{"agent", "error": "watcher_already_running", "pid"}`, without touching the
  pidfile, when the pidfile names a watcher the Stop hook would count as running: alive (same pid
  and start time), still under its launcher, for the same agent and the same Claude process. A
  recorded watcher that is dead, orphaned, or serves another Claude process does not count; the
  new watcher replaces its pidfile. Two watchers started at the same instant can both pass this
  check.

## Listing an inbox

`gptqueue-inbox-peek --list [--name N] [--dir D] [--limit K] [--json]` lists an inbox's unclaimed
messages in queue order, and its open claims, without consuming anything. Queue order is mostly
oldest first, but mail recovered from an expired claim is appended at the tail, so an older message
can appear after newer ones (and beyond a small `--limit`); `oldest_unclaimed_age_s` always covers
the whole queue. An operator or observer
can use it on another agent's mailbox. It names the inbox like the count form: `--name`, `--dir`,
or this session's identity. It reads only: one MULTI/EXEC of TIME, LLEN, LRANGE on the queue,
ZRANGE on the claim index and LLEN/GET on the dead-letter queue and its drop counter, then MGET on outstanding-request keys and HMGET on the claim records.
The second phase is best effort: a claim acknowledged or renewed in between can show null record
fields or a later expiry, and `wakes` reflects reply ownership at that later moment.
`--limit` caps the listed messages (default 50, max 500). `--list` with `--watch` is a usage error
(exit 2). A Redis failure prints `gptqueue-inbox-peek: <code>: <message>` on stderr and exits 1.

Each message shows `id`, `from`, `type`, `timestamp`, `age_s` (against Redis TIME), `in_reply_to`,
`wakes` and a `preview`: the first 80 characters of the content, whitespace collapsed, with `…`
when cut. An entry that is not a JSON object appears as `{raw_index, error: "unparseable"}`.
The summary gives `unclaimed` (LLEN), `listed`, `truncated`, `oldest_unclaimed_age_s` (over the
whole queue, since recovered mail returns to the tail; null when empty or no timestamp is readable), `wake_eligible_count`
(among the listed messages), each claim (`claim_id`, `session_id`, `claimed_at`, `age_s`,
`expires_at`, `expired`, `task_count`) and `expired_claims`. `--json` prints one object; without
it, a table and summary lines.

The listing also reports `dead_lettered_count` (the mailbox's dead-letter queue length, LLEN) and
`dead_lettered_dropped` (the claim-recovery drop counter: entries recovery's DLQ bound of 1000
discarded; other DLQ trims, on malformed-message receive or mailbox migration, do not count, so it
is a lower bound). Both are read in the same MULTI/EXEC with LLEN and GET; a missing key reads as 0
and nothing is created. Each is `null` when unreadable (a wrong-type key or a corrupt counter); that
affects only the field, and the rest of the listing is still reported. These are counts only: no
DLQ content, message ids or senders. The human form ends with `dead-lettered: <count>, dropped
<dropped>` (`?` for null). A non-zero count means tasks were quarantined, usually after repeated
claim-expiry recoveries: the mailbox's owner inspects them with `dlq_status` and re-queues them
with `dlq_requeue` (also on the sidecar after the parity release). `get_queue_status` reports the
same two fields per queue (`docs/QUEUE-CAPACITY.md`).

`wakes` is the dispatcher's own rule (`wakesRecipient` in `src/core/inbox-events.ts`). A `task`
always wakes the recipient. A `result` or `error` wakes it only when it answers the recipient's own
outstanding request: `in_reply_to` names a request whose `gptq:outstanding:<recipient>:<id>` key
holds the reply's sender. Status mail never wakes, nor does ping, an unsolicited reply or a reply
from another sender. Mail that needs the recipient to act must be sent as `type=task`.

## Install note (not applied)

1. `npm run build` in your checkout (`<path-to-gptqueue>`).
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
   "UserPromptSubmit": [{"matcher": "", "hooks": [{"type": "command", "command": "$HOME/.local/bin/gptqueue-claude-hook user-prompt-submit", "timeout": 15}]}]
   ```
   The `UserPromptSubmit` entry is optional. It records the session's agent as `running`
   (`gptq:turn-state:<agent>`) and prints nothing; an allowed Stop records it as `idle`.
   The loop adapter's watchdog reads this turn state. Only a confirmed identity is recorded,
   and a failure never blocks the prompt.
4. Keep the MCP server entry that runs `gptqueue-claude-mcp`. It must start the shim directly, not
   through a shell that stays alive, or the record will be keyed by the shell's pid.
