# GPTQueue quickstart

Single-user, single Linux machine: one GPTQueue server, Redis, and your
agent runtimes talking to each other over MCP. Ten minutes from clone to
first acknowledged task.

## 0. Prerequisites

- Node.js 22+ and npm
- Redis running locally (`redis://127.0.0.1:6379`) — db0 is the live
  server's database; tests use db15 (see `AGENTS.md`)

## 1. Install

```bash
cd /home/rahul/Documents/gptqueue
scripts/install.sh              # builds, restarts the server on 127.0.0.1:8101, health-checks
curl http://127.0.0.1:8101/health
```

The server speaks MCP over Streamable HTTP at `http://127.0.0.1:8101/mcp`
(it binds loopback; see "Security" in the README for remote binds). A stdio
entry exists too: `bin/gptqueue-server`.

## 2. Connect two agents

Every agent is its own MCP client connection — two agents must NOT share
one connection (registering a second name on one connection closes the
first session by rename semantics).

Point each runtime's MCP client at `http://127.0.0.1:8101/mcp` (or give
each a stdio spawn of `bin/gptqueue-server`). Then each agent calls:

```json
register_agent { "name": "alice", "role": "both", "description": "..." }
```

The response's `session_id` is the agent's credential for every later
session-scoped call. Register a second agent `bob` from its own connection.

## 3. First conversation (two delivery paths)

**Path A — plain messaging (at-most-once).** Fine for notifications where
losing one message is acceptable:

```json
// alice:
send_message    { "session_id": "<alice>", "to": "bob", "content": "hello" }
// bob:
receive_message { "session_id": "<bob>", "timeout": 5 }
```

`receive_message` destructively pops. Durable actors cannot call it (the
server rejects them with `durable_actor_claim_required`).

**Path B — durable delivery (at-least-once).** For work that must survive
crashes:

```json
// alice:
send_message    { "session_id": "<alice>", "to": "bob", "content": "do the thing" }
// bob:
claim_tasks     { "session_id": "<bob>", "max_batch": 4 }        // -> { claim_id, tasks }
acknowledge_tasks { "session_id": "<bob>", "claim_id": "<claim_id>" }
```

If bob crashes before acknowledging, the claim expires (300s default) and
the tasks return to the inbox — process tasks idempotently (see
`docs/OPERATING_RULES.md`). Long tasks: `claim_tasks` accepts
`ttl_seconds` up to 3600, and `renew_claim` extends a held claim.

## 4. Offline activation (wake)

Agents that are not running can be launched when work arrives:

1. The **operator** allowlists launchable commands in
   `.gptqueue/launch-allowlist.json` (fail-closed; shells are always
   rejected):

   ```json
   { "version": 1,
     "commands": [
       { "command": "/usr/bin/node",
         "allowed_args_prefixes": [[ "-e" ]],
         "comment": "test runtimes" } ] }
   ```

2. A session registered as `bob` registers the durable actor:

   ```json
   actor_register { "activation_policy_mode": "wake_if_offline",
                    "max_concurrency": 2,
                    "launch_command": "/usr/bin/node",
                    "launch_args": ["-e", "..."], "launch_cwd": "/path/inside/workspace" }
   ```

   The actor identity is derived from the registering session's name —
   `actor_register` has no separate actor_id.

3. Anyone sends to `bob` while bob is offline. The server persists the
   message first, then acquires one wake lease and dispatches the launch
   (coalescing: concurrent sends share one lease). When the woken runtime
   registers under the name `bob`, the lease clears (`runtime_ready`) and
   it claims its inbox with `claim_tasks`.

## 5. Custody (optional, for shared worktrees)

Before a session works in a project tree it can claim custody:
`custody_claim { worktree_path, repo_head, tree_fingerprint, lease_seconds }`.
Release with a structured handoff (`custody_release`); if a session dies,
the claim is forfeited on expiry and a successor takes over with
`custody_claim` plus an explicit untracked-file inventory. Custody is
orthogonal to messaging — see `docs/CODE_REVIEW_HARNESS.md`'s sibling docs
and the tool descriptions for the full state machine.

## 6. Watching the system

- `list_agents` / `get_queue_status` — who is registered, inbox depths
- `actor_status { actor_id }` — presence: active/idle/starting/offline_*
- `dlq_status` / `dlq_requeue` — poison tasks (dead-lettered after 5
  failed recoveries; requeue grants a fresh budget)

## 7. Common errors

| Error | Meaning |
|---|---|
| `unknown_recipient` | `to` is neither a registered agent nor a durable actor; no keys created |
| `durable_actor_claim_required` | a durable actor tried the destructive receive path; use claim/ack |
| `concurrency_limit_reached` | the actor's `max_concurrency` claims are outstanding |
| `identity_mismatch` / `not_claim_owner` | the calling session does not own that claim |
| `launch_not_allowlisted` | wake launch not in the operator allowlist |
