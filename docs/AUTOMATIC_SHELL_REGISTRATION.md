# Automatic shell registration

The local integration registers each new Codex or Pi MCP connection before the
first model turn. An ordinary interactive `codex` or `pi` launch gets a fresh
`gptqueue-shell-<client>-<directory>-<uuid>` identity. This is a connection identity, not a durable address for the person
or project. Once an exact native runtime is bound, reconnecting that same runtime
restores its public queue identity and backlog. A different native session gets
its own identity, even in the same directory.

Codex starts `bin/gptqueue-session` through its required `gptqueue-shared` stdio
MCP entry. Its `--redis-url` is either `redis://host[:port]/<db>` or a unix
socket path with an optional database, `/path/redis.sock?db=<0-15>` (the form
ioredis accepts; any other query, `//`, or characters outside
`[A-Za-z0-9._~@+,=:/-]` are refused). Pi loads a native extension which starts the same sidecar and exposes
19 bound tools: the four base tools `send_message`, `receive_message`,
`list_agents`, and `get_queue_status`, plus the 15-tool `SHELL_TOOL_NAMES` set:
`claim_tasks`, `acknowledge_tasks`, `renew_claim`, `claim_details`, `release_claim`,
`bind_runtime`, `get_runtime_status`, `acknowledge_messages`, `dlq_requeue`,
`find_agents`, `get_agent_details`, `get_delivery_status`, `set_agent_profile`,
`dlq_status`, and `list_my_claims`. The connection supplies its session internally. There is no
need for the model to call `register_agent` or pass a session ID.

When an owned Codex app-server is started with `codex app-server --listen
unix://PATH`, set `GPTQUEUE_CODEX_APP_SERVER_SOCKET=PATH` in every GPTQueue
process that must reach it. This includes both the Codex hook command and the
registered-shell sidecar process. The value is read when each
`CodexSocketClient` is constructed and affects only that process; it does not
change Codex global configuration or start a daemon. If unset, the integration
uses Codex's managed `$CODEX_HOME/app-server-control/app-server-control.sock`
endpoint.

Codex daemon discovery fails with `proc_unreadable` (reported by
`gptqueue retire-thread` as `daemon_proc_unreadable`) if some of a visible
process's real, effective, saved, and filesystem UIDs match the caller's UID
and others differ, because the scan cannot prove socket ownership.

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
      "working_directory": "/path/to/gptqueue",
      "registered_at": "2026-09-04T23:36:49.554Z",
      "pid": 3709271,
      "role": "both",
      "description": "codex interactive shell in /path/to/gptqueue",
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
restores its address; registration without binding is provisional — except
when GPTQ_AGENT_NAME pins a fixed role name (set GPTQ_AGENT_SHARED=1 on every
incarnation), which registers as a shared, non-provisional identity. Abrupt process death or
an unavailable Redis can leave a lease until its 30-second expiry; retained
mailboxes and registry entries require an independent retention policy.

## Install or roll back

Build the checkout, then review the plan. The plan prints paths and fingerprints,
not configuration values:

```sh
npm run build
python3 scripts/install-shell-registration.py \
  --redis-url redis://127.0.0.1:6379/0 \
  --node-bin "$(command -v node)"
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

Tests and verification scripts never use a shared Redis: each vitest run and
each verify script starts its own private, socket-only instance
(`scripts/lib/owned-redis.mjs`; `docs/OPERATING_RULES.md` §10). The exception
is the operator tool `scripts/verify-claude-mcp.mjs`, which drives the server
the configured `REDIS_URL` names; its `selftest` writes throwaway
registrations and messages there (private-instance isolation is a backlog item). Real CLI
startup probes use db14 of their own private instance and remove only records
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
command hooks: SessionStart and UserPromptSubmit bind the thread in the
background, and a synchronous Stop hook gates the end of a turn on pending mail.
Review the plan after building:

```sh
python3 scripts/install-inbox-activation.py \
  --node-bin "$(command -v node)" \
  --redis-url redis://REDIS_HOST:6379/0
```

The URL must match the sidecar's `--redis-url` so the hooks probe the same Redis instance and database.

Add `--apply` to install them into `~/.codex/hooks.json`, preserving other hooks.
Then open `/hooks` in Codex and review/trust the three GPTQueue command hooks.

The Stop gate runs inside the app-server for every thread, so a thread that is
not bound to gptqueue gets `{}` at once, and a Redis failure while resolving the
binding also allows the stop. For a bound thread with unclaimed or
expired-claim mail it blocks once per new mail event: Codex continues the turn
with the block reason as a new prompt, and the next Stop (with
`stop_hook_active`) is allowed unless more mail arrived. An allowed stop
records the agent as `idle` in its turn state; a bound prompt records `running`.
Installation does not grant trust: Codex skips untrusted hooks even when enabled.
The installer never bypasses this native approval boundary. See the
[Codex hook trust documentation](https://learn.chatgpt.com/docs/hooks#review-and-trust-hooks).
Sessions that must run under `approval_policy = "never"` additionally need the
operator-scoped `default_tools_approval_mode = "approve"` opt-in on the gptqueue
MCP server entry, or Codex rejects every gptqueue tool call client-side — see
"Approval-restricted Codex sessions" in `docs/QUICKSTART.md`. The installer
never writes that line either.
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
Codex round trip on its own private socket-only Redis (db15). It archives only its own test
threads and records a JSON receipt under
`.gptqueue/automatic-shell/event-delivery/`. The installed daemon must support its local control socket; older
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
`--real-sidecar` for the real MCP/Redis task-claim-reply-ack path on a private
socket-only Redis (db15) it starts itself; this
checks native transport and execution without using external model credentials.

### Discovering the intended peer and diagnosing delivery

New registered sidecars publish protocol version 2 and add `find_agents`,
`get_agent_details`, `get_delivery_status`, `set_agent_profile`, and the caller-bound
`dlq_status` / `dlq_requeue` (OpenCode: `gptqueue_dlq_status` / `gptqueue_dlq_requeue`).
When a runtime's tasks were dead-lettered, it recovers them itself: `dlq_status` lists
only its own mailbox's DLQ and `dlq_requeue` returns one entry to its inbox with a fresh
recovery budget. Neither accepts a target mailbox. The original
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

For a legacy connection, use the local read-only doctor:

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

### Refused runtime mailbox adoption

When a reconnecting sidecar cannot adopt its mapped mailbox, no inbox dispatcher
runs. `get_runtime_status` reports `dispatcher: "off"`, `readiness:
"awaiting_adoption"` and a `continuity` record with these fields:
- the sidecar's `current_mailbox` and the mapped `target`;
- a `reason`: `mapping_changed`, `registration_missing`, `source_session_invalid`,
  `source_ownership_conflict`, `target_occupied` or `source_busy`;
- the first blocking `signal` (other checks may also block);
- `first_refused_at`, `last_attempt_at` and `operator_action_required`;
- `recheck_after`, a hint and not a promise. It is the earliest unexpired claim
  expiry or the outbound-activity TTL. Other claims may remain, and renewals or
  further sends move it.

`continuity` is null when no adoption is pending.

For Codex, the binding supervisor's retry fields stay current on every attempt:
`binding_attempts`, `next_attempt_at` and `last_attempt_at`. The Codex lifecycle
log records `native_binding_retrying` with code `adoption_refused`, reason, signal
and target when the refusal changes, not on every retry. Pi has no retry loop, so
it reports the same status fields but writes no such log line.

If the mapped mailbox stays dormant while the sidecar keeps working under its current
name, an operator can repoint that one runtime's mapping to the live name:

```sh
node scripts/gptqueue-doctor.mjs remap-runtime-mailbox --redis-url redis://127.0.0.1:6379/0 --client codex --runtime-id EXACT_NATIVE_THREAD --from MAPPED_MAILBOX --to LIVE_MAILBOX
```

Without `--apply yes` this is a dry run. It previews both mailboxes' retained work:
- queue length;
- unexpired claims with their expiry;
- registration pid and liveness;
- runtime binding and heartbeat;
- outstanding reply correlations: requests each mailbox still awaits replies to,
  and replies it still owes, with a sample showing each task's subject and
  whether it was answered.

No index records correlations by mailbox, so the doctor finds them with a
read-only SCAN of the outstanding-request keys. The SCAN is capped at about
`--max-correlation-scan` keyspace entries (default 200000). If it stops at the
cap, or an existing outstanding record cannot be read as a correlation (for
example one that lost its expiry, which may still name either mailbox), the
disclosure is reported as incomplete, its counts are lower bounds, and the
remap is refused unless you pass `--accept-incomplete-disclosure yes`.

The dry run also reports the guards:
- the mapping names FROM;
- TO is registered with a running pid, checked by the doctor process;
- TO has a heartbeat or a live session lease;
- FROM has no runtime binding and no heartbeat;
- the correlation disclosure is complete, or its incompleteness was accepted.

Applying works in one atomic step. It first checks the type of every key it
touches, refusing without writing if any key has an unexpected type. It then
re-checks the mapping, TO's registration and liveness, and FROM's dormancy. Only
then does it repoint the mapping, clear TO's provisional mark and record an
`operator_remap` audit event.

The remap moves no work: FROM's queued messages, claims and correlations stay at
FROM. A sidecar whose adoption was refused will not undo a remap when it shuts
down, because its shutdown retirement requires the mapping to still name the
refused target.

Exit codes:
- 0: the remap was applied, or the dry run found every guard holding;
- 6: refused, nothing was written, and each refusal is printed;
- 1: a bad or missing option;
- 2: a connection or other error. After `--apply yes` the outcome is then
  unknown, so run the dry run again before retrying.

### Pre-update and recovery procedure for a refused mailbox

Before restarting or updating a runtime whose mailbox has work in flight:

1. Record the exact owner state: `get_runtime_status` for the runtime UUID and
   current mailbox, plus the metadata-only IDs of queued and claimed messages
   (IDs and counts, never contents).
2. Stop taking new claims for that mailbox; let in-flight claims run or expire
   through the normal protocol.
3. Reply to and acknowledge only FINISHED work. Never acknowledge an incomplete
   task just to drain the mailbox before a restart.

After the restart, inspect `get_runtime_status` before triggering any check
replies: a legacy client may still fence replies. If the replacement connection
is refused adoption (`dispatcher: "off"`, `readiness: "awaiting_adoption"`), an
operator can repoint the mapping after the guards are disclosed and confirmed
holding:

```sh
node scripts/gptqueue-doctor.mjs remap-runtime-mailbox \
  --redis-url redis://127.0.0.1:6379/0 --client codex \
  --runtime-id EXACT_OWNER_RUNTIME_UUID --from MAPPED_MAILBOX --to CURRENT_MAILBOX
```

Run the dry run first, then repeat with `--apply yes` only with explicit
operator approval; the atomic apply and its `operator_remap` audit event are the
receipt. The remap makes CURRENT canonical but moves no old messages, claims or
reply correlations: old work remains at OLD, and normal expiry recovery is OLD.
Ask original senders to supersede or resend to the current address. Do not
acknowledge another owner's tasks, reuse credentials, or edit Redis keys by
hand.

Confirm afterwards that the binding names the exact runtime UUID, that the
dispatcher is on (`dispatcher: "on"`), and that the API reports
`activation_ready: true` — not a readiness value of `ready`. Record whether the
canary arrived while the runtime was idle or during an active turn: active
turns can consume mail too, so consumption during an active turn does not prove
an idle automatic wake. Note the non-task exemption only prevents the 24-hour
reply fence; it does not release unexpired claims.

### Registration audit: proving who registered

Every committed registration of a new session (any runtime: the Claude MCP server, registered
shells, the plain MCP server, HTTP) appends one entry to the stream `gptq:registration-audit`
(`MAXLEN ~ 10000`). The entry is written by the same Lua script that publishes the registry entry
(`PUBLISH_REGISTRATION_AUDITED_SCRIPT` in `src/core/session-store.ts`), so an entry exists only for
a registration that committed. A failed or rolled-back attempt never reaches that script and writes
no event (there is no `rolled_back` event). A runtime continuity adoption (a live provisional
session adopting its mapped mailbox name) is audited in the adoption script itself, as
`event=registered` with `via=adoption` for the target name; ordinary registrations carry
`via=register`. The operator continuity and remap commands only change a mapping and are not
audited here. Unregistering, reaping or rolling back later never removes the entry, so it proves a
transient registration that a registry snapshot no longer shows. A registered shell's later
metadata republish is not a new registration and is not audited.

**The audit is best-effort and never blocks registration.** A registration, rename or adoption
never fails or rolls back because of the audit: its failure modes are exactly those it had before
the audit existed (a wrong-type registry or rollback-bookkeeping key still fails it, now checked
before the script's first write, so such a failure changes nothing). When the audit key cannot take the event (it holds another type), the script
still publishes the registration and instead records an **audit gap**: it sets the string
`gptq:registration-audit:gap` to the Redis server's current epoch milliseconds (never lowering
it). If the gap marker itself holds another type, it is left alone and the query reports `unknown`.

Fields: `event=registered`, `name`, `session_tag` (the `sid-sha256:` tag, never the raw session
id), `runtime` (`claude` | `codex` | `pi` | `generic` | `unknown`; a declared client outside that
list, such as `opencode`, is `unknown`), `pid`, `cwd` (the shell's working directory when it
declared one, else the registering process's cwd), and `claude_session` (the Claude session UUID
from `CLAUDE_SESSION_ID` / `CLAUDE_CODE_SESSION_ID` when the Claude MCP path has it, else empty), and `via` (`register` | `adoption`).

Query it read-only (TYPE, XRANGE, XINFO STREAM and GET only; it never creates a key):

```
bin/gptqueue-doctor registrations --since 2026-10-19T00:00:00Z --until 2026-10-19T12:00:00Z \
  [--cwd /abs/dir] [--claude-session UUID] [--name NAME] [--redis-url URL] --json
```

`--redis-url` defaults to `$REDIS_URL`, then `redis://127.0.0.1:6379/0`. The output is one JSON
object: `{schema_version: 1, since, until, coverage, complete_from, events: [{at, name,
session_tag, runtime, pid, cwd, claude_session, via}], truncated}`, with at most 500 events
(`truncated: true` beyond). Filters are exact matches (`--claude-session` ignores case).

`coverage` says whether an empty result proves absence:

- `complete`: the stream provably holds every registration since `--since`
  (`complete_from <= since`). Nothing was removed and the stream's first entry is older, or
  entries were removed (`entries-added > length` for MAXLEN trims, `max-deleted-entry-id` for
  XDEL) but only before `--since`.
- `incomplete`: events before `complete_from` were trimmed or deleted, the audit started
  writing to this Redis after `--since`, or an audit gap was recorded at or after `--since`.
  When the audit was deployed after `--since`, `complete_from` is the first audited
  registration, so the caller sees the deploy time there. An audit gap moves `complete_from` to
  1 ms after the gap (`max(stream bound, gap + 1 ms)`); when the stream itself gives no bound (it
  is missing or not a stream) a gap in the window still gives `incomplete`, with `complete_from:
  null`. An empty stream gives `unknown`, since nothing proves when it started.
- `unknown`: no audit stream on this Redis (missing, or a key of another type), a Redis older
  than 7.0 without the XINFO fields, or a gap marker that is not a string of epoch milliseconds.

Coverage is computed from stream metadata and the gap marker, both read after the scan, so a
trim, an XDEL or an unaudited registration during the query downgrades it instead of hiding a
missed event.

**Mixed versions.** Writers on releases without the audit emit nothing (no event and no gap
marker), and the stream cannot tell which writers ran which release. An empty `complete` report
proves non-registration only when every process that could register in the queried interval ran
an audit-emitting release throughout it.

Exit codes: 0 success, 2 invalid arguments, 4 Redis unavailable (or the query failed).
`bin/gptqueue-doctor registrations --help` prints the same summary.

## Outbound fence scoping (bound wrapper `send_message`)

The bound experimental wrapper keeps a 24-hour pre-send outbound fence
(`gptq:outbound-activity:<agent>`, set before `send_message` runs) so a mailbox
with outbound correlations is never silently renamed during identity recovery.

Only a normalized **task** send creates that fence. The message is validated and
normalized before the marker is touched, so malformed input is rejected without
writing anything. A send that omits `type` defaults to `task`, so it is
normalized as a task and sets the fence like any other task send. An explicit
`result`, `error`, `status` or `ping` send never sets the fence and never clears
or refreshes an existing one. Each exempt send writes one
bounded, non-waking trace entry (`stage: outbound_fence_skipped`,
`code: message_type=<type>`) to the agent's inbox trace stream; the entry
carries no message body or session material and is capped by the trace stream's
existing MAXLEN. Failed or ambiguous task sends keep the fence (conservative);
all ownership, adoption, claim and admission guards are unchanged.
