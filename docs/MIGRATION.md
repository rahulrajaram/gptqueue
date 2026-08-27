# Migration Guide: Session-Aware gptqueue

This document covers changes from the original single-process stdio design to the session-aware architecture.

## What Changed

### Registration returns a session_id

**Before:**
```json
{ "status": "registered", "name": "my-agent", "role": "both" }
```

**After:**
```json
{ "status": "registered", "name": "my-agent", "session_id": "abc-123", "role": "both" }
```

The `session_id` can be used to reconnect from a different process without re-registering.

For stateless or bridge-based transports, pass that `session_id` to
session-scoped tools:

- `send_message(session_id?, to, ...)`
- `receive_message(session_id?, timeout)`
- `close_session(session_id?)`
- `unregister_agent(session_id?)`

### New tool: close_session

`close_session` drops the live session but **preserves** the mailbox. Queued messages remain available for a future session.

`unregister_agent` remains available and is **destructive** -- it deletes the mailbox and all queue data.

### Online status from session leases

Online/offline status is now computed from session leases (TTL-based keys in Redis) rather than a single heartbeat key. An agent with multiple sessions is online if any session has a live lease.

### Native HTTP transport

A new HTTP transport eliminates the need for `supergateway` or other stdio bridges:

```bash
node dist/transports/http.js --port 3001
```

## Redis Key Changes

### New keys

| Key | Type | Purpose |
|---|---|---|
| `gptq:session:<session_id>` | Hash | Session record (agent_name, role, timestamps) |
| `gptq:lease:<session_id>` | String with TTL | Session liveness indicator |
| `gptq:agent-sessions:<name>` | Set | Active session IDs for an agent |

### Preserved keys

| Key | Type | Purpose |
|---|---|---|
| `gptq:registry` | Hash | Agent discovery index (unchanged) |
| `gptq:q:<name>` | List | Mailbox queue (unchanged) |
| `gptq:meta:<name>` | Hash | Queue metadata (unchanged) |
| `gptq:heartbeat:<name>` | String with TTL | Legacy heartbeat (kept for backward compat) |

## Backward Compatibility

- Existing stdio clients continue to work without changes
- `GPTQ_AGENT_NAME` auto-registration still works (now creates a session automatically)
- Legacy heartbeat keys are still written alongside session leases
- `list_agents` checks both session leases and legacy heartbeats
- `unregister_agent` still performs the same destructive cleanup

## Upgrading

1. Update your gptqueue installation: `git pull && npm install`
2. No Redis data migration needed -- new keys are additive
3. To use HTTP transport, start the HTTP server and update your MCP client config
4. To use stateless or bridge-friendly session reconnection, capture
   `session_id` from `register_agent` responses and pass it to
   session-scoped tools
