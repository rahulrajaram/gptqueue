<p align="center">
  <img src="assets/logo.png" alt="gptqueue logo" width="300">
</p>

# gptqueue

[![CI](https://github.com/rahulrajaram/gptqueue/actions/workflows/ci.yml/badge.svg)](https://github.com/rahulrajaram/gptqueue/actions/workflows/ci.yml)

Inter-agent message queue over MCP + Redis.

gptqueue lets AI agents (Claude Code, Codex, Gemini CLI, or any MCP-compatible client) discover each other and exchange messages through a shared Redis-backed queue. Each agent registers with a name and description, then sends and receives typed messages via MCP tool calls.

## Architecture

```
┌─────────────┐       MCP (stdio)       ┌──────────────┐
│  AI Agent A  │◄──────────────────────►│              │
└─────────────┘                         │  gptqueue    │       ┌───────┐
                                        │  MCP server  │◄─────►│ Redis │
┌─────────────┐       MCP (stdio)       │              │       └───────┘
│  AI Agent B  │◄──────────────────────►│              │
└─────────────┘                         └──────────────┘
```

Each agent gets its own bounded inbox queue in Redis. Messages are delivered atomically via a Lua script that enforces queue size limits. Agents publish heartbeats so others can see who is online.

## Components

| Component | Description |
|---|---|
| **MCP server** (`src/mcp-server/`) | Stdio-based MCP server exposing 6 tools for agent communication |
| **PTY wrapper** (`src/pty-wrapper/`) | Wraps a CLI process in a PTY, watches Redis for incoming messages, and injects notifications when the process is idle |
| **Hook script** (`scripts/check-queue.sh`) | Claude Code hook for startup context injection and stop-gate (blocks exit if inbox has unread messages) |

## MCP Tools

| Tool | Description |
|---|---|
| `register_agent` | Register with a name, role (`publisher`/`consumer`/`both`), and description |
| `send_message` | Send a typed message (`task`/`result`/`status`/`error`/`ping`) to another agent's inbox. Supports optional `metadata` object and `in_reply_to` message ID for threading |
| `receive_message` | Blocking pop from your inbox (default timeout: 5s) |
| `list_agents` | Discover all registered agents with online/offline status |
| `get_queue_status` | Check queue depth and capacity for one or all agents |
| `unregister_agent` | Unregister and clean up queue data |

## Prerequisites

- **Node.js** >= 18
- **Redis** running locally (default `redis://127.0.0.1:6379`)

## Install

```bash
git clone https://github.com/rahulrajaram/gptqueue.git
cd gptqueue
npm install   # builds automatically via postinstall
```

## Configuration

### Claude Code

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

### Any MCP client

The server speaks stdio MCP. Point your client at:

```bash
node /path/to/gptqueue/dist/mcp-server/index.js [agent-name]
```

Or set `GPTQ_AGENT_NAME` in the environment.

### Environment variables

| Variable | Default | Description |
|---|---|---|
| `REDIS_URL` | `redis://127.0.0.1:6379` | Redis connection URL (MCP server and PTY wrapper) |
| `GPTQ_AGENT_NAME` | _(none)_ | Pre-register with this agent name on startup |
| `GPTQ_QUEUE_BOUND` | `10` | Max messages per agent inbox |
| `REDIS_HOST` | `127.0.0.1` | Redis host (hook script only) |
| `REDIS_PORT` | `6379` | Redis port (hook script only) |

## PTY wrapper

The PTY wrapper lets you run any CLI (e.g. `claude`, `codex`) inside a PTY that monitors Redis for incoming messages and injects prompts when the process goes idle:

```bash
gptqueue-pty --agent alice --cmd claude
```

## How it works

1. **Registration** -- An agent calls `register_agent` with a name, role, and description. This writes to a Redis hash (`gptq:registry`) and starts a heartbeat (10s interval, 30s TTL).

2. **Discovery** -- Any agent (even unregistered) can call `list_agents` to see all registered agents and whether they're online.

3. **Messaging** -- `send_message` pushes to the target agent's Redis list (`gptq:q:<name>`). A Lua script enforces the queue bound atomically. If the queue is full, the sender retries with exponential backoff (up to 10 attempts).

4. **Receiving** -- `receive_message` does a blocking pop (`BLPOP`) with a configurable timeout.

5. **Cleanup** -- `unregister_agent` removes the agent from the registry and deletes its queue, metadata, and heartbeat keys.

## License

MIT
