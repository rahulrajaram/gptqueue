<p align="center">
  <img src="assets/logo.png" alt="gptqueue logo" width="300">
</p>

# gptqueue

[![CI](https://github.com/rahulrajaram/gptqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/rahulrajaram/gptqueue/actions/workflows/ci.yml)

Inter-agent message queue over MCP + Redis.

**Start here:** [docs/QUICKSTART.md](docs/QUICKSTART.md) — install, connect two agents, and land your first acknowledged task in ten minutes. The delivery contract you operate under (at-least-once semantics, idempotency obligations, dead-letter and renewal policy) is specified in [docs/OPERATING_RULES.md](docs/OPERATING_RULES.md).

gptqueue lets AI agents (Claude Code, Codex, Gemini CLI, or any MCP-compatible client) discover each other and exchange messages through a shared Redis-backed queue. Each agent registers with a name and description, then sends and receives typed messages via MCP tool calls.

## Architecture

```
                           MCP (stdio)
┌─────────────┐  ◄──────────────────────►  ┌──────────────┐
│  AI Agent A  │                            │              │
└─────────────┘                             │  gptqueue    │
                           MCP (HTTP)       │  MCP server  │◄─────► Redis
┌─────────────┐  ◄──────────────────────►  │              │
│  AI Agent B  │                            │  (sessions)  │
└─────────────┘                             └──────────────┘
```

Each agent gets its own bounded inbox queue in Redis. Messages are delivered atomically via a Lua script that enforces queue size limits.

Agent identity is backed by Redis session records with TTL-based leases, so sessions survive process restarts and work across transport boundaries.

## Components

| Component | Description |
|---|---|
| **MCP server** (`src/mcp-server/`) | MCP server exposing tools for agent communication |
| **HTTP transport** (`src/transports/http.ts`) | Streamable HTTP server -- no bridge needed for shared hosting |
| **Core** (`src/core/`) | Transport-agnostic session store, mailbox store, and type definitions |
| **PTY wrapper** (`src/pty-wrapper/`) | Wraps a CLI process in a PTY, watches Redis for incoming messages, and injects notifications when the process is idle |
| **Hook script** (`scripts/check-queue.sh`) | Claude Code hook for startup context injection and stop-gate (blocks exit if inbox has unread messages) |

## Design Reports

- [Autonomous Agent Coordination Product Thesis](docs/AUTONOMOUS_COORDINATION_PRODUCT_THESIS.md)
- [Session and Transport Redesign Report](docs/SESSION_TRANSPORT_REDESIGN_REPORT.md)

## MCP Tools

| Tool | Description |
|---|---|
| `register_agent` | Register with a name, role, and description. Returns a `session_id` for session resumption |
| `send_message` | Send a typed message (`task`/`result`/`status`/`error`/`ping`) to another agent's inbox. Supports optional `metadata`, `in_reply_to`, `session_id`, and a caller `idempotency_key` (retained for 24 hours) for retry-safe delivery. Recipients are resolved BEFORE any queue write: the target must be a durable actor-directory record or a registered agent, otherwise the send is rejected with a structured `unknown_recipient` error and no mailbox/queue keys are created (a typo'd or unknown name can never silently create an orphan mailbox). Self-send remains valid. For `wake_if_offline` durable-actor recipients whose runtime is offline, the message is persisted first and an additive `wake` field on the result reports whether the actor's runtime was dispatched (`wake_dispatched`), coalesced onto an in-flight wake (`wake_coalesced`), or failed to launch (`launch_failed`) |
| `receive_message` | Blocking pop from your inbox (default timeout: 5s; accepted range: 0–60 whole seconds). Supports optional `session_id` for stateless transports. Plain agents (no actor-directory record) keep this legacy at-most-once `BLPOP`. Durable actors (those with an actor-directory record) are rejected with a structured `durable_actor_claim_required` error and must consume their inbox via `claim_tasks`/`acknowledge_tasks` instead |
| `list_agents` | Discover agents with readable labels, messaging names, directories, UUIDs, and online/offline status |
| `get_queue_status` | Check queue depth and capacity for one or all agents |
| `close_session` | Close the current session but preserve the mailbox. Messages remain queued for later reconnection. Supports optional `session_id` for stateless transports |
| `unregister_agent` | Unregister and delete all queue data (destructive). Supports optional `session_id` for stateless transports |
| `custody_claim` | Claim custody of a worktree for this session. Handles initial claim, graceful re-claim, and successor takeover (forfeited worktrees require an `inventory`) |
| `custody_release` | Release a held worktree, recording a structured handoff for the next custodian. Only the current custodian session may release |
| `custody_status` | Inspect a worktree's custody record, or list every stored record. Expired leases are forfeited lazily |
| `actor_register` | Register a durable actor profile and launch contract in the shared actor directory. The durable actor identity is DERIVED from the calling session's registered agent name (there is no `actor_id` argument), so the registered name, directory key, wake/presence key, and delivery/claim identity can never diverge. The calling session owns the actor's profile; `wake_if_offline` actors must declare a `launch_command` |
| `actor_status` | Classify a durable actor's runtime presence (`active`/`idle`/`starting`/`offline_launchable`/`offline_store_only`/`unavailable`) from its launch contract, live sessions, and any outstanding wake lease |
| `claim_tasks` | Atomically claim up to `max_batch` messages (default 1, range 1–16) from your own durable inbox as an at-least-once delivery batch for the calling session. Returns the claim (`claim_id`, `tasks`, `expires_at`) or an explicit empty-batch result when nothing is pending. `ttl_seconds` (default 300, range 1–3600) bounds how long an unacknowledged claim stays out of the inbox before lazy recovery re-queues it. For registered durable actors, the directory's admitted `max_concurrency` caps the number of simultaneously outstanding unacked claims: once the ceiling is reached, a further claim is refused with a `concurrency_limit_reached` error until an existing claim is acknowledged or lazily recovered. Plain agents (no directory record) claim without any ceiling |
| `acknowledge_tasks` | Acknowledge a `claim_id` returned by `claim_tasks`, confirming delivery of that batch. Only the claiming session may acknowledge its own claim; acknowledged tasks are removed so they are not re-delivered |
| `renew_claim` | Renew an outstanding `claim_id` returned by `claim_tasks`, extending its expiry by `ttl_seconds` (default 300, range 1–3600) from the renew instant. Only the claiming session may renew its own claim (`not_claim_owner` otherwise). The extension is capped by the claim's provisional lifetime budget rendered from its `claimed_at`, so an endlessly-renewing runtime cannot hold a batch forever: a post-expiry renewal is `claim_expired` and budget exhaustion is `budget_exceeded` |
| `dlq_status` | List the calling agent's dead-letter queue (DLQ) entries, newest first. A message is dead-lettered after it has been recovered (re-queued) more than `RECOVER_CAP` times without an acknowledge, so a perpetually failing message cannot loop through lazy recovery forever. Supports an optional `limit` (default 50, range 1–1000) |
| `dlq_requeue` | Move one dead-lettered message (by `message_id` from `dlq_status`) from the calling agent's DLQ back to the tail of its own inbox, restoring a fresh recovery budget. Not found is a structured `dlq_entry_not_found` error |

### Registered shell (additional tools)

The registered shell (`bin/gptqueue-session`) registers four additive tools on top of the table above:

| Tool | Description |
|---|---|
| `find_agents` | Find exact agent candidates by declared purpose and identity. Ambiguous matches are never routed automatically; online does not imply activation readiness |
| `get_agent_details` | Inspect an exact mailbox, runtime binding, published capabilities, declared role and activation readiness. Omit `agent` for the current connection. No message content or credentials |
| `get_delivery_status` | Inspect one message's queue, claim, acknowledgement or dead-letter evidence without consuming it. Missing retained evidence means unknown, not delivered |
| `set_agent_profile` | Declare this connection's readable label, purpose and kind. A declaration is a discovery hint, never proof of controller authority or permission to take over another mailbox |

`package.json` maps only `gptqueue-server`, `gptqueue-http`, and `gptqueue-pty` as installed commands; the registered-shell and wrapper entry points are repo-local — invoke them as `node bin/<entry>` (for example `node bin/gptqueue-session`).

### Dead-letter queue (provisional policy)

Lazy recovery (in `claims-recover.lua`) counts, per message, how many times a delivered-but-unacked task has been re-queued. Once that count exceeds a cap, the task is moved to the actor's dead-letter queue (`gptq:dlq:<actor>`) instead of the inbox, so a message that repeatedly fails after expiry cannot bounce forever. The competing constants below are **provisional policy**: they are named, documented placeholders pending principal calibration, and tuning them is policy, not code.

- `RECOVER_CAP = 5` — max recoveries of one message before lazy recovery quarantines it to the DLQ.
- `DLQ_MAX_LENGTH = 1000` — per-actor DLQ bound; the newest entries are kept and older trimmed entries are dropped (they were already dead-lettered once).
- `RECOVER_COUNTER_TTL_SECONDS = 604800` — TTL on each per-message recovery counter, bounding orphans (7 days).

Acknowledging a claim clears the counters of its tasks, and `dlq_requeue` restores a fresh budget, so a message can be inspected and re-driven indefinitely. Only id-bearing envelopes are counted; a legacy envelope without a stable message id is re-queued without counter accounting.

### Claim renewal (provisional budget policy)

`renew_claim` extends an outstanding claim's expiry so a runtime can keep a long-running batch alive without surrendering it to lazy recovery. The extension is applied from the renew instant but is capped so a claim can never be renewed more than `CLAIM_LIFETIME_BUDGET_SECONDS` past its original `claimed_at`. This budget is **provisional policy**: it is a named, documented placeholder pending principal calibration, and tuning it is policy, not code.

- `CLAIM_LIFETIME_BUDGET_SECONDS = 86400` — max lifetime of any claim measured from `claimed_at` (1 day); `renew_claim` refuses an extension that would push past it (`budget_exceeded`) and cannot resurrect an already-expired claim (`claim_expired`).

## Prerequisites

- **Node.js** >= 20
- **Redis** running locally (default `redis://127.0.0.1:6379`)

## Install

From npm (ships prebuilt; provides `gptqueue-server`, `gptqueue-http` and `gptqueue-pty`):

```bash
npm install -g gptqueue
```

`gptqueue-pty` needs the native `node-pty` module. If your npm holds back
dependency install scripts, approve `node-pty` (`npm install-scripts approve node-pty`)
so it can build.

From source:

```bash
git clone https://github.com/rahulrajaram/gptqueue.git
cd gptqueue
npm install   # builds automatically via prepare
```

## Transports

### Stdio (direct, single-client)

For local use with one MCP client:

```bash
node /path/to/gptqueue/dist/mcp-server/index.js [agent-name]
```

Or set `GPTQ_AGENT_NAME` in the environment.

### HTTP (shared, multi-client)

For shared hosting without bridges like supergateway:

```bash
node /path/to/gptqueue/dist/transports/http.js --port 3001
```

Each connecting client gets its own MCP session backed by Redis. Sessions survive reconnects.

#### Security boundary

The HTTP server binds **loopback (`127.0.0.1`) by default**. For the local
single-host deployment the security boundary *is* this loopback bind: only
processes on the same machine can reach the MCP endpoint, so no token is
required by default.

Two environment variables control the surface:

| Variable | Default | Description |
|---|---|---|
| `GPTQUEUE_HOST` | `127.0.0.1` | Interface to bind. Set to a non-loopback address only when the server must be reachable beyond one host. |
| `GPTQUEUE_HTTP_TOKEN` | _(none)_ | Shared `Bearer` secret. When set (regardless of host), every `/mcp` request must present `Authorization: Bearer <token>`. `/health` stays unauthenticated so process managers can liveness-check the server. |

Refusal rule: if `GPTQUEUE_HOST` is set to a **non-loopback** address and
`GPTQUEUE_HTTP_TOKEN` is **unset or empty**, the server **refuses to start**
(exit non-zero before listening, with a clear message) rather than expose an
unauthenticated MCP surface to every reachable host. If the host is loopback
(`127.0.0.1` / `localhost`), the token is optional and the local default remains
loopback + tokenless. The startup log states the bind host and whether token
auth is active (it never prints the token value).

Example — token-authenticated server reachable from other machines:

```bash
GPTQUEUE_HOST=0.0.0.0 \
GPTQUEUE_HTTP_TOKEN=$(openssl rand -hex 32) \
  nohup node /path/to/gptqueue/dist/transports/http.js --port 3001 > /tmp/gptqueue.log 2>&1 &
```
GPTQueue is an external/shared coordination plane; use it in place of native
in-session agent collaboration for a workflow, not concurrently with it.

Tool responses retain their legacy text JSON and also expose normalized
`structuredContent`. Failures use stable codes such as `REDIS_UNAVAILABLE`,
`SESSION_UNAVAILABLE`, `AGENT_NOT_REGISTERED`, and `QUEUE_FULL`.

Health check: `GET /health` returns `{"status":"ok","sessions":N}`.

### Wake launch allowlist (PROVISIONAL)

When a durable `wake_if_offline` actor is woken, the server spawns the
actor's registered `launch_command` directly (`shell:false`, never
interpolated into a string). Historically that command was copied verbatim,
which let a caller register an arbitrary program (or a shell like
`/bin/sh -c <payload>`) and trigger it via `send_message` → wake. To close
that hole, GPTQueue now gates every runtime launch behind an **operator
allowlist**.

The allowlist is an operator-authored file at
`$XDG_CONFIG_HOME/gptqueue/launch-allowlist.json` (default
`~/.config/gptqueue/launch-allowlist.json`), deliberately outside any agent
workspace: an allowlist that agents can edit is one they can authorize
themselves with. Override the path with `GPTQUEUE_LAUNCH_ALLOWLIST`, and keep it
somewhere agents cannot write. Symlinks and group- or world-writable files are
refused. A legacy `./.gptqueue/launch-allowlist.json` in the working directory
is no longer read; the refusal message names the new location. Format (version 2):

```json
{
  "version": 2,
  "commands": [
    {
      "command": "/absolute/path/or/name",
      "allowed_args": [["--agent", "alice"], []],
      "comment": "optional human note"
    }
  ]
}
```

Matching rules (identical at admission and at dispatch):

- A requested `launch_command` must satisfy an allowlisted entry's identity
  **exactly** — never by basename aliasing. A bare-name entry matches only the
  byte-identical bare name; an absolute-path entry matches only an absolute
  request that `path.resolve`s to the same path. `/attacker/work/node` does
  NOT match an allowlisted `node`.
- A requested arg vector must **equal one `allowed_args` template exactly**:
  same length, every element identical. There is no suffix freedom, and `[]`
  accepts only a request with no args.
- `launch_cwd`, when provided, must be an existing directory **within the
  server workspace root** (`path.resolve` + prefix check; realpath-based
  symlink-escape handling is intentionally out of scope).

Fail-closed semantics:

- A **new** `wake_if_offline` registration is refused with a typed error
  (`launch_not_allowlisted`) if the allowlist file is absent, unparseable, or
  does not permit the requested command/args. Already-admitted actors keep
  functioning.
- Dangerous delegators — a command whose basename is a shell (`sh`, `bash`,
  `zsh`, `dash`, `fish`, `ksh`, `cmd`, `powershell`, `pwsh`) or a shell
  carrying `-c`/`-lc`/`-Command` — are rejected **regardless of the
  allowlist** (`launch_command_rejected`).
- Interpreter inline-code flags are rejected **regardless of the allowlist**
  (`launch_command_rejected`): a command whose basename is an interpreter
  (`node`, `deno`, `bun`, `tsx`, `ts-node`, `python`, `python2`, `python3`,
  `ruby`, `perl`, `php`, `awk`) carrying `-e`/`--eval`/`-c`/`--command` is an
  arbitrary-code channel even under an exact-template grant. Point the
  interpreter at a fixed script file instead (`node /path/to/runtime.mjs`).
- Version-1 documents (basename matching + unbounded `allowed_args_prefixes`
  suffixes) are **rejected at parse time** with a migration message; they
  cannot be soundly auto-converted to exact templates.
- Rejections apply again at **dispatch** (`dispatchLaunch` re-reads the
  allowlist), so a stale actor-directory entry cannot spawn a command the
  operator has since disallowed. A refused dispatch surfaces as
  `wake.status: "launch_failed"` on the send.

> **PROVISIONAL**: the global operator allowlist is a stop-gap. It is slated
> to be replaced by **per-actor operator grants** (each actor may only launch
> programs its own operator explicitly granted). Operators should treat the
> allowlist as the minimum permit set and audit it regularly.

## Configuration

### Claude Code (stdio)

Add to `~/.claude.json` under `mcpServers`:

```json
{
  "gptqueue": {
    "type": "stdio",
    "command": "node",
    "args": ["/path/to/gptqueue/dist/mcp-server/index.js"]
  }
}
```

### Claude Code (HTTP)

Start the HTTP server, then configure the client to connect:

```bash
# Start the server
node /path/to/gptqueue/dist/transports/http.js --port 3001
```

```json
{
  "gptqueue": {
    "type": "streamable-http",
    "url": "http://127.0.0.1:3001/mcp"
  }
}
```

### Hook script

Optionally add the hook script to `~/.claude/settings.json` for automatic startup context and exit gating:

```json
{
  "hooks": {
    "SessionStart": [{
      "matcher": "",
      "hooks": [{
        "type": "command",
        "command": "/path/to/gptqueue/scripts/check-queue.sh --startup"
      }]
    }],
    "Stop": [{
      "matcher": "",
      "hooks": [{
        "type": "command",
        "command": "/path/to/gptqueue/scripts/check-queue.sh --stop"
      }]
    }]
  }
}
```

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL |
| `GPTQUEUE_HOST` | `127.0.0.1` | HTTP server bind host; non-loopback requires `GPTQUEUE_HTTP_TOKEN` |
| `GPTQUEUE_LAUNCH_ALLOWLIST` | `~/.config/gptqueue/launch-allowlist.json` | Path of the operator wake-launch allowlist; keep it outside agent workspaces (see the security section) |
| `GPTQUEUE_HTTP_TOKEN` | _(none)_ | Bearer token required on every `/mcp` request when set |
| `GPTQUEUE_HTTP_IDLE_TIMEOUT_MS` | `3600000` (1 hour) | Close an HTTP MCP session after this long with no open request (an open SSE stream counts as open); the client gets `404` and re-initializes. `0` disables |
| `GPTQUEUE_HTTP_MAX_SESSIONS` | `256` | Maximum concurrent HTTP MCP sessions; further `initialize` requests get `503` |
| `GPTQ_AGENT_NAME` | _(none)_ | Pre-register with this agent name on startup (stdio only) |
| `GPTQ_QUEUE_BOUND` | `10` | Max messages per agent inbox |
| `GPTQ_HTTP_PORT` | `3001` | HTTP server port |
| `REDIS_HOST` | `127.0.0.1` | Redis host (hook script only) |
| `REDIS_PORT` | `6379` | Redis port (hook script only) |

## PTY wrapper

The PTY wrapper lets you run any CLI (e.g. `claude`, `codex`) inside a PTY that monitors Redis for incoming messages and injects prompts when the process goes idle:

```bash
node bin/gptqueue-pty --agent alice --cmd claude
```

When a woken agent has pending messages, the injected prompt instructs it to
consume via `claim_tasks` (optional `max_batch`, `ttl_seconds`) and confirm
with `acknowledge_tasks` (`claim_id`) rather than `receive_message`, so a
durable actor woken through the PTY consumes its batch at-least-once.

## How it works

1. **Registration** -- An agent calls `register_agent` with a name, role, and description. This creates a Redis-backed session with a TTL lease and returns a `session_id`.

2. **Sessions** -- Each registration creates a session record in Redis. Sessions have TTL-based leases that are automatically refreshed. An agent can have multiple concurrent sessions (e.g. from different transports).

3. **Discovery** -- Any agent (even unregistered) can call `list_agents` to see all registered agents and whether they're online. Online status is computed from active session leases.

4. **Messaging** -- `send_message` pushes to the target agent's Redis list (`gptq:q:<name>`). A Lua script enforces the queue bound atomically. If the queue is full, the sender retries with exponential backoff (up to 10 attempts).

   **Resolve-then-push.** Before any queue write, `send_message` resolves the recipient: it proceeds when the target has a durable actor-directory record or is a registered agent; otherwise it rejects the send with a structured `unknown_recipient` error and creates no queue (`gptq:q:<name>`) or metadata (`gptq:meta:<name>`) keys. A typo'd or unknown name can never silently create an orphan mailbox. Self-send (to the caller's own registered name) remains valid.

   **Wake-on-send.** When the recipient is a durable `wake_if_offline` actor (see `actor_register`) with no live runtime and a runnable launch contract, `send_message` persists the message first, then attempts to wake the actor: it acquires a bounded (60s) per-actor wake lease, and if it owns the lease, spawns the actor's launch command (detached, no shell). The send always succeeds regardless of the wake outcome, and the additive result field `wake` reports `wake_dispatched`/`wake_coalesced`/`launch_failed`/`wake_error`. `store_only` actors and plain agents (no durable record) never wake, so their send behavior is unchanged.

   **Pid-liveness reconciliation.** When a wake lease records a `spawned_pid` and the launched runtime never registers (no live session), the actor would otherwise stay pinned in `starting` until the lease TTL lapses. Presence assembly (both `actor_status` and `send_message`'s wake gate) reconciles this observationally: if the lease's spawned process is no longer alive, the lease is cleared as a failed activation, so the actor returns to offline and re-wake becomes possible. A live or un-probed (no pid) lease is retained and reported as `starting`; `actor_status`'s `wake_lease` payload includes an additive `pid_liveness` of `alive`/`dead`/`unknown`.

5. **Receiving** -- `receive_message` does a blocking pop (`BLPOP`) with a configurable timeout. For plain agents (no durable actor-directory record) this is unchanged. A durable actor (one with an actor-directory record) is refused with a `durable_actor_claim_required` error and must consume at-least-once via `claim_tasks` (returns a `claim_id`) then `acknowledge_tasks` (`claim_id`) instead, so the legacy destructive pop never silently drops a durable actor's message.

6. **Session close** -- `close_session` drops the live session but preserves the mailbox. Queued messages remain available for a future session.

7. **Unregister** -- `unregister_agent` closes the session AND deletes the mailbox. This is a destructive operation.

## Known Limitations

### Bridge-based transport (supergateway)

When using the stdio transport behind a bridge like `supergateway`, tool calls may land in different worker processes. The recommended solution is to use the native HTTP transport instead, which eliminates the need for bridges entirely.

If you must use a stateless or bridge-based transport, capture the
`session_id` returned by `register_agent` and pass it to session-scoped
tools such as `send_message`, `receive_message`, `close_session`, and
`unregister_agent`.

### Child-process accumulation

If still using bridges, they can accumulate orphaned gptqueue worker processes. To check and clean up:

```bash
# Count gptqueue child workers
pgrep -f 'gptqueue' | wc -l

# Review candidates first: -f matches any command line containing the text,
# including servers from other checkouts
pgrep -af 'dist/mcp-server/index.js'
# then stop only the PIDs you have confirmed are orphaned
kill <pid> ...
```

Stdio servers now exit on their own when their client closes stdin, so
orphans should only come from older builds.

## License

MIT
