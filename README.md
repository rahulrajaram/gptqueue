<p align="center">
  <img src="assets/logo.png" alt="gptqueue logo" width="300">
</p>

# gptqueue

[![CI](https://github.com/rahulrajaram/gptqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/rahulrajaram/gptqueue/actions/workflows/ci.yml)

Inter-agent message queue over MCP + Redis.

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

- [Session and Transport Redesign Report](docs/SESSION_TRANSPORT_REDESIGN_REPORT.md)

## MCP Tools

| Tool | Description |
|---|---|
| `register_agent` | Register with a name, role, and description. Returns a `session_id` for session resumption |
| `send_message` | Send a typed message (`task`/`result`/`status`/`error`/`ping`) to another agent's inbox. Supports optional `metadata`, `in_reply_to`, and `session_id` for stateless transports |
| `receive_message` | Blocking pop from your inbox (default timeout: 5s). Supports optional `session_id` for stateless transports |
| `list_agents` | Discover all registered agents with online/offline status |
| `get_queue_status` | Check queue depth and capacity for one or all agents |
| `close_session` | Close the current session but preserve the mailbox. Messages remain queued for later reconnection. Supports optional `session_id` for stateless transports |
| `unregister_agent` | Unregister and delete all queue data (destructive). Supports optional `session_id` for stateless transports |

## Prerequisites

- **Node.js** >= 18
- **Redis** running locally (default `redis://127.0.0.1:6379`)

## Install

```bash
git clone https://github.com/rahulrajaram/gptqueue.git
cd gptqueue
npm install   # builds automatically via postinstall
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

Health check: `GET /health` returns `{"status":"ok","sessions":N}`.

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
| `GPTQ_AGENT_NAME` | _(none)_ | Pre-register with this agent name on startup (stdio only) |
| `GPTQ_QUEUE_BOUND` | `10` | Max messages per agent inbox |
| `GPTQ_HTTP_PORT` | `3001` | HTTP server port |
| `REDIS_HOST` | `127.0.0.1` | Redis host (hook script only) |
| `REDIS_PORT` | `6379` | Redis port (hook script only) |

## PTY wrapper

The PTY wrapper lets you run any CLI (e.g. `claude`, `codex`) inside a PTY that monitors Redis for incoming messages and injects prompts when the process goes idle:

```bash
gptqueue-pty --agent alice --cmd claude
```

## How it works

1. **Registration** -- An agent calls `register_agent` with a name, role, and description. This creates a Redis-backed session with a TTL lease and returns a `session_id`.

2. **Sessions** -- Each registration creates a session record in Redis. Sessions have TTL-based leases that are automatically refreshed. An agent can have multiple concurrent sessions (e.g. from different transports).

3. **Discovery** -- Any agent (even unregistered) can call `list_agents` to see all registered agents and whether they're online. Online status is computed from active session leases.

4. **Messaging** -- `send_message` pushes to the target agent's Redis list (`gptq:q:<name>`). A Lua script enforces the queue bound atomically. If the queue is full, the sender retries with exponential backoff (up to 10 attempts).

5. **Receiving** -- `receive_message` does a blocking pop (`BLPOP`) with a configurable timeout.

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

# Kill orphaned workers (use with caution)
pkill -f 'dist/mcp-server/index.js'
```

## License

MIT
