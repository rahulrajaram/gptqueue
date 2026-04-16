# gptqueue Session and Transport Redesign Report

Date: 2026-04-15

## Executive Summary

This report documents a recommended architectural redesign for `gptqueue`
after field evidence showed that the current process model does not hold up
under bridge-based MCP usage. The present implementation assumes a long-lived
single-process stdio server with in-memory agent identity. In practice, the
package is also being used behind transport bridges such as `supergateway`,
where tool calls may not be served by one stable process. That mismatch can
produce two severe failures:

1. transport/session drift, where `register_agent` appears to succeed but
   later calls such as `receive_message` and `send_message` fail with
   "Agent not registered"
2. child-process accumulation, where large numbers of MCP helper workers are
   left behind and consume substantial host memory

The core recommendation is to make `gptqueue` a Redis-backed mailbox and
session service with thin transport adapters. Agent identity and online state
should no longer depend on one process holding mutable in-memory state.

## Background

`gptqueue` exists to let agents discover each other and exchange messages over
MCP with Redis as the durable queue backend. Today the repository presents
three main elements:

- `src/mcp-server/`: the MCP server
- `src/pty-wrapper/`: a PTY wrapper that runs a CLI and nudges it when inbox
  messages arrive
- Redis queue and registry keys for message delivery and online/offline status

The current README describes the architecture as a stdio MCP server connected
to Redis. That shape is valid for a direct one-client-to-one-process setup.
The trouble begins when the package is used through a transport bridge that
proxies or respawns stdio workers.

## Current Implementation Shape

The current server wiring creates a single `RedisClient` instance at process
startup:

- `src/mcp-server/index.ts`

That `RedisClient` stores mutable agent identity in `_agentName`:

- `src/mcp-server/redis-client.ts`

Tool behavior then depends on that in-memory field:

- `register_agent` updates `_agentName`
- `send_message` and `receive_message` call `requireRegistered()`
- heartbeat is driven by a process-local `setInterval`
- `unregister_agent` deletes queue and metadata and clears `_agentName`

This design assumes:

1. one MCP session maps to one server process
2. the process remains alive across all tool calls
3. heartbeats are tied to process lifetime
4. queue ownership is the same thing as connection ownership

Those assumptions are too strong for bridge-based or multi-transport usage.

## Field Evidence That Triggered This Report

During downstream use on 2026-04-15, two distinct symptoms were observed:

### 1. Registration worked, but later inbox operations failed

`list_agents` and queue depth lookups showed the agent as online, but
`receive_message` and `send_message` still returned:

`Agent not registered. Call register_agent first with a name.`

That symptom strongly suggests the registration was durable in Redis, while
the later tool call landed in a different process that did not hold the same
in-memory `_agentName`.

### 2. Extreme child-process accumulation

Observed on the host:

- about 985 stale `gptqueue` child node workers consuming about 24.2 GiB RSS
- about 480 stale Perplexity MCP child workers consuming about 13.6 GiB RSS
- only two long-lived `supergateway` root processes above them

After clearing the orphaned child workers, host memory improved from roughly
1.7 GiB free to about 47 GiB free. That was an operational win, but it also
demonstrated that the transport/process model is currently too leak-prone to
be trusted as-is.

## Root Cause Analysis

The main design problem is not Redis. It is identity ownership.

`gptqueue` currently mixes three concerns inside one mutable process object:

1. queue operations
2. transport lifecycle
3. agent session identity

That coupling is acceptable for direct stdio with one long-lived process, but
it breaks down under any of the following:

- bridge/proxy transports
- worker respawn behavior
- HTTP-style request handling
- multiple clients sharing one service endpoint
- process restarts while mailboxes should remain valid

The most important conceptual error is this:

`agent registered` is treated as `this process remembers an agent name`

but the system actually needs:

`agent/session registered` to be durable and transport-independent

## Design Goals

The redesign should satisfy the following:

1. Agent identity survives transport process churn.
2. Mailboxes survive disconnects and reconnects.
3. Online/offline state is lease-based, not bound to one local timer in one
   worker.
4. Direct stdio remains easy for simple local usage.
5. HTTP and bridge-based usage become first-class instead of accidental.
6. PTY notification behavior remains available without owning message-state
   semantics.
7. The server becomes testable in transport and session scenarios.

## Recommended Architecture

### A. Introduce a Redis-backed session layer

Add an explicit session abstraction instead of keeping `_agentName` as the
authoritative registration state in process memory.

Suggested concepts:

- mailbox: durable queue for a named agent
- session: a live lease representing one connected transport/client
- lease: TTL-backed online presence record

Suggested Redis model:

- `gptq:agent:<name>`: canonical agent metadata
- `gptq:queue:<name>`: durable mailbox
- `gptq:session:<session_id>`: session record containing `agent`, `role`,
  `description`, `last_seen`, optional transport metadata
- `gptq:lease:<session_id>`: TTL-backed liveness indicator
- `gptq:agent-sessions:<name>`: set of active session ids

With that model:

- a process can reconnect and resume a session or create a new one
- an agent can be online via one or more sessions
- queue ownership is decoupled from one worker's heap

### B. Make tool calls explicit about session or agent context

There are two viable approaches:

#### Option 1: session token on every session-scoped tool

Examples:

- `register_agent -> { session_id, agent_name }`
- `send_message(session_id, to, ...)`
- `receive_message(session_id, timeout)`
- `unregister_session(session_id)`

This is the cleanest model under stateless transports.

#### Option 2: transport-managed session binding

If using a native stateful transport, bind one session to one transport
instance and keep the session id in transport state, not in the mailbox core.

This can make client ergonomics nicer, but the server core should still be
written as if session id were explicit and durable.

Recommendation: implement the core as explicit-session-first, then allow the
stdio adapter to hide the session token for convenience.

### C. Promote native streamable HTTP to a first-class transport

The installed MCP SDK already includes server-side streamable HTTP transport.
That means `gptqueue` does not need to rely on `supergateway --stdio ...` for
shared or bridge-based hosting.

Recommendation:

- keep stdio support for local direct-client usage
- add a native streamable HTTP server entrypoint
- document stdio as the simple adapter, not the only real architecture

This reduces bridge complexity and makes the transport topology honest.

### D. Separate queue-core from transport adapters

Split the code into layers:

1. `core/queue-store`
   - Redis operations
   - registration/session persistence
   - send/receive primitives
   - lease refresh

2. `core/session-policy`
   - mailbox/session rules
   - online/offline computation
   - rename/migration behavior

3. `transports/stdio`
   - MCP stdio adapter
   - convenience binding to one session

4. `transports/http`
   - streamable HTTP adapter
   - stateful or stateless MCP session management

5. `pty-wrapper`
   - remains a separate UX tool
   - only observes queue state and nudges the user
   - does not own registration semantics

### E. Redefine unregister behavior

Today `unregister_agent` deletes queue data entirely. That is too destructive
for a mailbox abstraction.

Recommendation:

- split "close my live session" from "delete this mailbox"
- make mailbox deletion an explicit administrative action
- default disconnect behavior should preserve messages and metadata

This is especially important when transports are flaky or ephemeral.

## Why This Change Is Worth Doing

### Reliability

The redesign removes the observed class of "registered, then not registered"
failures because the truth moves from process memory into Redis-backed session
state.

### Operational Stability

A transport that can safely respawn or reconnect without multiplying hidden
workers will materially reduce host memory risk.

### Architectural Honesty

The current implementation is advertised as an inter-agent queue, but the
actual operational contract is "a single lucky stdio worker that must stay
alive." The redesign aligns implementation with product intent.

### Easier Multi-Client and Future Integrations

Once the core is session-aware and transport-agnostic, adding HTTP hosting,
browser tools, daemonized operation, or richer observability gets much easier.

## Proposed Implementation Plan

### Phase 0: Immediate Safeguards

1. Document that bridge-based usage is risky under the current design.
2. Add a troubleshooting note for child-process buildup.
3. Add tests reproducing the current failure mode with separate server
   instances.

### Phase 1: Session Core Extraction

1. Extract Redis queue operations from `RedisClient` into a mailbox/core
   module.
2. Introduce explicit session records and lease refresh operations.
3. Replace `_agentName` as the authoritative source of registration state.

### Phase 2: Tool Contract Redesign

1. Return `session_id` from registration.
2. Make `send_message`, `receive_message`, and session-scoped operations accept
   a session id or operate through a transport-bound session context.
3. Introduce `close_session` separate from mailbox deletion.

### Phase 3: Native HTTP Transport

1. Add a streamable HTTP server entrypoint.
2. Keep stdio entrypoint as a thin adapter.
3. Stop treating `supergateway` as the default path for shared hosting.

### Phase 4: Presence and Mailbox Semantics

1. Convert online/offline to session lease aggregation.
2. Decide policy for multiple sessions per agent name.
3. Preserve queues across disconnects.

### Phase 5: Cleanup and Compatibility

1. Provide compatibility wrappers for older stdio clients.
2. Update README and examples.
3. Add migration notes.

## Suggested Redis API Direction

These are illustrative, not final:

- `register_session(agent_name, role, description) -> session_id`
- `refresh_session(session_id)`
- `close_session(session_id)`
- `delete_mailbox(agent_name)` admin-only
- `send_message(session_id, to, ...)`
- `receive_message(session_id, timeout)`
- `list_agents()` computed from agent metadata plus live leases
- `get_queue_status(agent_name?)`

## Test Plan

The redesign should come with tests that cover cases missing today:

1. registration in one process followed by send/receive in another process
2. session reconnect after process restart
3. heartbeat/lease expiry marks agent offline without deleting mailbox
4. multiple sessions for one agent
5. HTTP transport request sequence across distinct worker instances
6. stdio compatibility path with a stable single process
7. unregister session vs delete mailbox semantics

## Risks and Tradeoffs

### More explicit state

The design becomes more formal. That is good, but it means slightly more
surface area and some migration work.

### Client ergonomics

Explicit session ids are slightly less magical. The stdio adapter can hide
some of that, but the core should remain explicit.

### Backward Compatibility

Existing clients may assume registration is a one-time in-process effect. A
compatibility layer or migration window will help.

## Recommended First Implementation Slice

If only one architecture slice is taken next, it should be this:

1. introduce Redis-backed session records
2. return a `session_id` from registration
3. stop using `_agentName` as the only truth for later calls

That slice directly addresses the correctness bug even before HTTP transport is
added.

If a second slice is available, make it:

4. add native streamable HTTP transport

That slice removes the need for a bridge that can hide or multiply stdio
workers.

## Conclusion

`gptqueue` should evolve from a clever stdio-local MCP helper into a real
message service with durable mailbox semantics and explicit session ownership.
The current design is close enough to be useful, but not robust enough for the
way it is already being used. The field evidence is strong enough that this
should be treated as an architectural follow-up, not a minor bugfix.
