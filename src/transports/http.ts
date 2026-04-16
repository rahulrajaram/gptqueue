#!/usr/bin/env node

/**
 * Streamable HTTP transport for gptqueue.
 *
 * Starts an Express server with the MCP Streamable HTTP transport.
 * Each HTTP client gets its own MCP session and transport instance.
 * This replaces the need for supergateway or other stdio bridges.
 *
 * Usage:
 *   node dist/transports/http.js [--port 3001]
 */

import { randomUUID } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { RedisClient } from "../mcp-server/redis-client.js";
import { registerTools } from "./setup-tools.js";

const DEFAULT_PORT = 3001;

function parsePort(): number {
  const idx = process.argv.indexOf("--port");
  const portArg = idx !== -1 ? process.argv[idx + 1] : undefined;
  if (portArg) {
    const p = parseInt(portArg, 10);
    if (!isNaN(p)) return p;
  }
  return parseInt(process.env.GPTQ_HTTP_PORT || String(DEFAULT_PORT), 10);
}

const port = parsePort();
const app = createMcpExpressApp();

// Store transports by session ID
const transports: Record<string, StreamableHTTPServerTransport> = {};

// Create a fresh MCP server + RedisClient per session
function createSessionServer(): { server: McpServer; redisClient: RedisClient } {
  const redisClient = new RedisClient(null);
  const server = new McpServer({
    name: "gptqueue",
    version: "1.0.0",
  });
  registerTools(server, redisClient);
  return { server, redisClient };
}

// POST /mcp -- handle tool calls and initialization
app.post("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"] as string | undefined;

  if (sessionId && transports[sessionId]) {
    // Existing session
    await transports[sessionId].handleRequest(req, res, req.body);
    return;
  }

  if (!sessionId && isInitializeRequest(req.body)) {
    // New session
    const { server, redisClient } = createSessionServer();

    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      onsessioninitialized: (sid) => {
        transports[sid] = transport;
      },
    });

    transport.onclose = () => {
      const sid = transport.sessionId;
      if (sid && transports[sid]) {
        delete transports[sid];
      }
      redisClient.shutdown().catch(() => {});
    };

    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
    return;
  }

  res.status(400).json({ error: "Bad request: missing session or not an init request" });
});

// GET /mcp -- SSE stream for server-initiated messages
app.get("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"] as string | undefined;
  if (!sessionId || !transports[sessionId]) {
    res.status(404).json({ error: "Session not found" });
    return;
  }
  await transports[sessionId].handleRequest(req, res);
});

// DELETE /mcp -- close session
app.delete("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"] as string | undefined;
  if (!sessionId || !transports[sessionId]) {
    res.status(404).json({ error: "Session not found" });
    return;
  }
  const transport = transports[sessionId];
  await transport.handleRequest(req, res);
});

// Health check
app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    sessions: Object.keys(transports).length,
  });
});

app.listen(port, () => {
  console.log(`gptqueue HTTP server listening on port ${port}`);
  console.log(`MCP endpoint: http://127.0.0.1:${port}/mcp`);
  console.log(`Health check: http://127.0.0.1:${port}/health`);
});

// Graceful shutdown
async function shutdown() {
  for (const [sid, transport] of Object.entries(transports)) {
    await transport.close();
    delete transports[sid];
  }
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
