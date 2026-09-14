# Automatic shell registration

The local integration registers each new Codex or Pi MCP connection before the
first model turn. An ordinary interactive `codex` or `pi` launch gets a fresh
`gptqueue-shell-<client>-<directory>-<uuid>` identity. This is a connection identity, not a durable address for the person
or project. Once an exact native runtime is bound, reconnecting that same runtime
restores its public queue identity and backlog. A different native session gets
its own identity, even in the same directory.

Codex starts `bin/gptqueue-session` through its required `gptqueue-shared` stdio
MCP entry. Pi loads a native extension which starts the same sidecar and exposes
nine bound tools: `send_message`, `receive_message`, `list_agents`,
`get_queue_status`, `claim_tasks`, `acknowledge_tasks`, `renew_claim`,
`bind_runtime`, and `get_runtime_status`. The connection supplies its session internally. There is no
need for the model to call `register_agent` or pass a session ID.

When an owned Codex app-server is started with `codex app-server --listen
unix://PATH`, set `GPTQUEUE_CODEX_APP_SERVER_SOCKET=PATH` in every GPTQueue
process that must reach it. This includes both the Codex hook command and the
registered-shell sidecar process. The value is read when each
`CodexSocketClient` is constructed and affects only that process; it does not
change Codex global configuration or start a daemon. If unset, the integration
uses Codex's managed `$CODEX_HOME/app-server-control/app-server-control.sock`
endpoint.

## Agent labels and discovery

`list_agents({})` returns each agent's readable `label` alongside its exact
messaging `name`, public connection `uuid`, `client`, absolute
`working_directory`, `registered_at`, `pid`, description, role, and presence.
For example, a new wrapper registration appears in the structured response as:

```json
{
  "status": "ok",
  "agents": [
    {
      "name": "gptqueue-shell-codex-gptqueue-c0e7d21f-a327-4eda-b12b-779ae9a44e8a",
      "label": "gptqueue · codex · c0e7d21f",
      "uuid": "c0e7d21f-a327-4eda-b12b-779ae9a44e8a",
      "client": "codex",
      "working_directory": "/home/rahul/Documents/gptqueue",
      "registered_at": "2026-09-04T23:36:49.554Z",
      "pid": 3709271,
      "role": "both",
      "description": "codex interactive shell in /home/rahul/Documents/gptqueue",
      "online": true
    }
  ]
}
```

This is an illustrative record, not a backfill of an existing connection. The
text response remains a JSON array of the same agent objects; the structured
response retains its existing `{ "status": "ok", "agents": [...] }` envelope.
Use an object's full `name` as `send_message.to`. The label's short UUID suffix
helps scanning; labels are not routing aliases or guaranteed unique keys. The
full name and UUID distinguish concurrent connections in the same directory.
The public UUID identifies the connection and is different from its private
session credential, which discovery never returns.

`working_directory` records the resolved absolute directory at wrapper launch.
It preserves spaces and Unicode and does not claim to identify a Git root or
track subsequent directory changes. Legacy registrations use their existing
name as their label, with `null` for identity details they never recorded.
Stored registration times and process IDs remain available when valid.

Rebuild the checkout to apply the response extension to new wrapper connections.
Existing clients and shared server processes keep their loaded code until they
are restarted through their normal lifecycle. New readers can discover legacy
records, and old readers continue to receive their original fields.

The sidecar connects to the explicitly configured Redis database. It does not
restart or deploy the shared HTTP server. Closing the MCP connection retires its
GPTQueue session and preserves its registry entry and mailbox. Pi closes that
connection on shell exit. The installed Codex shared app-server can keep its agent
session and MCP connection alive after the terminal closes; its registration
therefore remains online. Terminal exit alone is not an unregister operation for
that Codex mode. This preserves existing background-session behavior.

Unread messages survive connection closure. Binding a resumed native session
restores its address; registration without binding is provisional. Abrupt process death or
an unavailable Redis can leave a lease until its 30-second expiry; retained
mailboxes and registry entries require an independent retention policy.

## Install or roll back

Build the checkout, then review the plan. The plan prints paths and fingerprints,
not configuration values:

```sh
npm run build
python3 scripts/install-shell-registration.py \
  --redis-url redis://127.0.0.1:6379/0 \
  --node-bin /home/rahul/nodeenv2251-311/bin/node
```

Apply the same command with `--apply`. It changes only:

- `~/.codex/config.toml`: replace the exact `gptqueue-shared` MCP server entry.
- `~/.pi/agent/mcp.json`: remove the exact `gptqueue-shared` adapter entry.
- `~/.pi/agent/extensions/gptqueue-registration.ts`: load the native Pi integration.

Other settings, tools, skills, model preferences, and launch arguments remain
available. Pi's existing Nudge launcher continues to handle its normal commands.
No dependency installation is involved. The installation points to this checkout
and its compiled `dist` files, so keep the checkout and rebuild it after changes.

The installer saves original bytes and modes in a private backup directory before
changing settings. It uses atomic file replacement and attempts restoration if
an apply fails. To restore the prior settings:

```sh
python3 scripts/install-shell-registration.py --rollback
```

Rollback refuses to overwrite a target changed since installation. The backup is
under `~/.local/state/gptqueue/shell-registration/manifest.json`; successful
rollback retains it as `rolled-back.json`. A new installation requires rolling
back the previous one first. For isolated configuration tests, supply separate
`--codex-dir`, `--pi-dir`, and `--state-dir` paths.

## Boundaries

Registration failure prevents normal inference: Codex requires MCP startup, and
Pi exits with a visible error because its host normally ignores extension errors.
Pi also checks that its connection and bound tools are ready before each turn,
while preserving the complete existing system prompt and other active tools.

This applies to launches that load these settings. Explicitly disabling Pi
extensions, overriding the queue integration, choosing a different configuration
home, or connecting Codex to an unrelated remote server can bypass it. Internal
subagents do not automatically acquire independent queue identities unless their
runtime starts an independent configured MCP connection. Existing open shells
continue using the integration they loaded earlier.

Tests use Redis db15. Real CLI startup probes use db14 and remove only records
owned by each probe. Verification receipts live under
`.gptqueue/automatic-shell/`; they record actual startup, session retirement,
configuration preservation, and the review scope.

## Lifecycle logs

The sidecar writes JSON lines to stderr and a separate file for each connection:
`~/.local/state/gptqueue/logs/<agent-name>.jsonl`. An absolute `XDG_STATE_HOME`
changes the state root; `GPTQ_LOG_DIR` overrides the complete log directory.
New log directories are private and new files are readable only by their owner.
The files remain after the connection closes and are not automatically pruned.

Each event carries a UTC timestamp, client, process ID, agent name, and
`elapsed_ms` measured with a monotonic clock from entry into wrapper startup.
The session ID is included once available. Completion events also report their
phase's `duration_ms`.

| Event | Meaning |
| --- | --- |
| `startup_started` | The wrapper has begun its startup function. |
| `registration_complete` | Redis registration and mailbox setup have completed. |
| `transport_connected` | The wrapper has connected its MCP transport. |
| `mcp_initialized` | The client has sent its MCP initialized notification. |
| `startup_failed` | Startup failed; the event identifies its phase. |
| `shutdown_started` | Connection cleanup has begun. |
| `shutdown_complete` / `shutdown_failed` | Cleanup completed or failed. |

Use `registration_complete.elapsed_ms` for startup-to-registration time and
`mcp_initialized.elapsed_ms` for startup-to-MCP-initialization time. These begin
after Node has loaded the wrapper's modules; they do not measure time spent
launching the CLI or waiting for its other integrations. MCP initialization also
does not establish that Pi has finished installing and enabling its native tools.

For the default location, find registration and initialization records with:

```sh
rg '"event":"(registration_complete|mcp_initialized)"' \
  "$HOME/.local/state/gptqueue/logs"
```

Logs contain lifecycle metadata rather than message contents, prompts, Redis
URLs, or raw exception messages. Logging failures do not prevent registration
or cleanup. Stdout remains reserved for MCP JSON-RPC traffic. Existing wrapper
processes keep their loaded code; rebuilt logging takes effect when a new
connection starts.

## Automatic inbox activation

Registration and activation readiness are separate. `get_runtime_status({})`
returns `agent`, `activation_ready`, and the exact bound runtime's `client`,
`runtime_id`, `epoch`, and `working_directory`. Never infer a native thread ID
from an agent UUID or choose a thread by directory alone.

Pi binds from its native `session_start` context. Codex needs the local
SessionStart/UserPromptSubmit command hook. Review the plan after building:

```sh
python3 scripts/install-inbox-activation.py \
  --node-bin /home/rahul/nodeenv2251-311/bin/node
```

Add `--apply` to install it into `~/.codex/hooks.json`, preserving other hooks.
Then open `/hooks` in Codex and review/trust the two GPTQueue command hooks.
Installation does not grant trust: Codex skips untrusted hooks even when enabled.
The installer never bypasses this native approval boundary. See the
[Codex hook trust documentation](https://learn.chatgpt.com/docs/hooks#review-and-trust-hooks).
Use `--rollback` to restore the saved original bytes. The separate backup lives
under `~/.local/state/gptqueue/inbox-activation`. Installation does not bind
already-open sessions retroactively; resume or submit a prompt so the hook runs.
The hook retries MCP readiness for up to 30 seconds without blocking startup.

A queued task or a result/error correlated to a task sent to that exact peer
notifies a per-agent Redis stream. The dispatcher also reconciles durable
backlog, so a missed notification does not lose work. Empty-inbox reconciliation
does not invoke a model. Codex starts a turn when idle and waits when busy; Pi
uses native follow-up delivery. Native permission policies remain in force.
A policy that forbids required MCP calls prevents task processing even if a
turn is successfully triggered.

The model claims its inbox, performs authorized work, sends a result with
`in_reply_to` and a stable `idempotency_key`, then acknowledges its claim.
Unacknowledged claims expire and recover through the existing claim protocol.
Results are consumed without automatically replying to results. Native
submission uncertainty is retained for recovery rather than blindly retried;
three completed activation turns without progress exhaust the activation attempt
budget. This provides at-least-once task handling, not exactly-once external
side effects. Task code must make its own external operations idempotent.

Metadata traces are bounded Redis streams at `gptq:inbox-events:<agent>` and
`gptq:inbox-trace:<agent>`. They connect enqueue, activation request, native turn,
claim, reply, and acknowledgement by message, operation, turn, and claim IDs.
They contain no message content. Trace failures produce a generic stderr event
and do not change a successful queue operation into an error. The existing
connection lifecycle log retains the provisional startup identity; runtime
status and runtime traces are authoritative after mailbox restoration.

`node scripts/verify-inbox-activation.mjs` runs an isolated two-thread native
Codex round trip on Redis db15 using Luna with low reasoning and normal automatic
approval review. It archives only its own test threads and records a JSON receipt
under `.gptqueue/automatic-shell/event-delivery/`. Do not run Redis tests alongside
this probe. The installed daemon must support its local control socket; older
history APIs are handled by reading the exact daemon-provided local transcript
and validating its native identity.

Add `--automatic-hook` to verify that the installed, trusted hooks bind the test
threads without the verifier calling `bind_runtime` itself. This sends an initial
READY prompt before queuing work: the installed daemon runs the hook lifecycle
on the first prompt, not merely when `thread/start` allocates an empty thread.
Registration alone therefore does not promise activation before that first
prompt; check `activation_ready` before relying on unattended delivery.
`node scripts/verify-pi-inbox-activation.mjs` exercises the installed native Pi
SDK's idle/busy lifecycle with a deterministic local inference stream. Add
`--real-sidecar` for the real MCP/Redis task-claim-reply-ack path on db15; this
checks native transport and execution without using external model credentials.

### Discovering the intended peer and diagnosing delivery

New registered sidecars publish protocol version 2 and add `find_agents`,
`get_agent_details`, `get_delivery_status`, and `set_agent_profile`. The original
seven shared MCP tool request/response contracts are unchanged. Existing loaded
sidecars retain their old implementation until their MCP connection is refreshed.
Pi accepts the previous nine-tool runtime catalog and the new optional diagnostic
tools; an old four-tool connection cannot perform native activation.

Use `set_agent_profile` to declare a readable label, purpose and kind (`controller`,
`worker`, `interactive`, or `unknown`). These are self-declarations, not permissions
or proof of authority. `find_agents` returns candidates and explicitly reports
ambiguity, including when pagination hides further matches. Never choose a main
controller by directory, PID, UUID prefix, registration recency or declared kind
alone. Confirm the exact intended messaging address with the operator when the
candidates remain ambiguous.

`get_agent_details` keeps online presence separate from native binding readiness.
Capabilities describe the implementation instantiated by that sidecar; they are
not a live probe of another connection's MCP tool table. A binding lease alone reports `bound_unverified` with `activation_ready: null`;
it can briefly outlive a failed process. A local-controller check, or
`get_agent_details` with `probe: true` for an exact Codex peer, can establish
readiness. A remote Pi lease remains unverified until checked through its own
connection. None of these states proves successful message processing. `get_delivery_status` inspects
a specified recipient and message ID without consuming the message. Queue, claim
and DLQ locations are evidence; acknowledgement requires a matching message claim
and acknowledgement trace. An empty queue without retained evidence means
`unknown_history`. Observations are bounded, non-atomic snapshots; trace retention
is approximately 1,024 events and trace writes can fail independently of delivery.
Neither diagnostic returns message content or private session credentials.

For a legacy connection, use the local read-only doctor (Node 24 on this host):

```sh
node scripts/gptqueue-doctor.mjs connection --redis-url redis://127.0.0.1:6379/0 --thread-id EXACT_NATIVE_THREAD --agent EXACT_MAILBOX
node scripts/gptqueue-doctor.mjs delivery --redis-url redis://127.0.0.1:6379/0 --agent EXACT_MAILBOX --message-id MESSAGE_ID
```

The connection probe invokes `get_runtime_status` on the exact thread. It does
not trust a daemon-wide catalog listing as proof that this thread loaded the tool.
If unavailable, the hook now reports `legacy_connection_requires_reconnect`
instead of retrying the missing tool for 30 seconds.

### Explicit continuity for a legacy mailbox

Ordinary reconnects retain the native runtime's existing mailbox mapping. The
replacement connection attaches its session atomically to that mailbox. It refuses
an active old owner, a changed mapping, or a provisional mailbox that already has
queued messages, claims, or outbound activity. It never moves envelopes to a
newly named queue. Stale session membership without a live lease does not by itself
block recovery. Original mailbox addresses and reply correlations remain intact.

When a legacy mailbox never had a runtime mapping, a local operator can prepare an
explicit plan for a verified native conversation. This does not infer succession
from directory or label:

```sh
node scripts/gptqueue-doctor.mjs continuity-plan --redis-url redis://127.0.0.1:6379/0 --client codex --runtime-id EXACT_NATIVE_THREAD --cwd ABSOLUTE_DIRECTORY --agent EXACT_OLD_MAILBOX --legacy yes --out PLAN.json
```

Review the plan and confirm the operator's identity evidence before applying it.
The target and any replaced mapped mailbox must have no live owner or outstanding
claims. A replaced address must also have no queued messages or outbound activity.
The plan is tied to the Redis namespace, runtime mapping and registry fingerprints;
changes cause a conflict rather than a guessed retry. Applying requires explicit
operator approval:

```sh
node scripts/gptqueue-doctor.mjs continuity-apply --redis-url redis://127.0.0.1:6379/0 --plan PLAN.json --apply yes
```

Application updates only the native mapping and an audit record; queued messages
stay at their original address. A new connection binding that exact native runtime
then attaches to the preserved mailbox. A successful mapping application is not a
delivery receipt: verify the replacement connection and each pending message.
The plan file is a reviewed administrative instruction, not an authentication
credential. This interface assumes the operator is already authorized to write
that Redis namespace; it does not secure Redis against another administrator.

Do not repoint a busy successor with its own outstanding conversations. For that
case, preserve both mailboxes and arrange an explicitly authorized resend to the
verified current address; do not silently rewrite historical envelopes. The doctor
never reloads the shared Codex daemon or migrates live mailboxes automatically.
