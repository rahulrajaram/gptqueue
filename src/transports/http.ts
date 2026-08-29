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

import { createHash, randomUUID, timingSafeEqual } from "node:crypto";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { RedisClient } from "../mcp-server/redis-client.js";
import { GPTQUEUE_INSTRUCTIONS, registerTools } from "./setup-tools.js";

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

// ---------------------------------------------------------------------------
// Bind host + token auth (security boundary)
//
// Local single-host deployments bind the loopback interface by default. A
// non-loopback bind is only allowed when a shared Bearer token has been
// configured; otherwise the server REFUSES to start, because exposing an
// unauthenticated MCP surface to every reachable host is unsafe.
// ---------------------------------------------------------------------------
const port = parsePort();

/** Host to bind. Defaults to loopback unless GPTQUEUE_HOST is provided. */
function resolveHost(): string {
  return process.env.GPTQUEUE_HOST || "127.0.0.1";
}

/** True when the host is a loopback address so token auth is optional. */
function isLoopbackHost(host: string): boolean {
  return host === "127.0.0.1" || host === "localhost" || host === "::1";
}

const httpToken = process.env.GPTQUEUE_HTTP_TOKEN || "";
const tokenActive = httpToken.length > 0;
const host = resolveHost();

if (!isLoopbackHost(host) && !tokenActive) {
  console.error(
    `[gptqueue-http] REFUSING TO START: binding to non-loopback host "${host}" ` +
      `requires GPTQUEUE_HTTP_TOKEN to be set, but it is unset or empty. ` +
      `Exposing an unauthenticated MCP server to a non-loopback address lets any ` +
      `reachable host call every tool. Set GPTQUEUE_HTTP_TOKEN to a shared secret ` +
      `to bind non-loopback, or leave GPTQUEUE_HOST unset to bind 127.0.0.1.`
  );
  process.exit(1);
}

/** Constant-time token comparison (timing-safe regardless of length). */
function tokenMatches(expected: string, provided: string): boolean {
  const a = createHash("sha256").update(expected).digest();
  const b = createHash("sha256").update(provided).digest();
  return timingSafeEqual(a, b);
}

/** Extract the `Bearer <token>` value from an Authorization header, or null. */
function extractBearerToken(req: {
  headers: { authorization?: string };
}): string | null {
  const auth = req.headers.authorization;
  if (!auth) return null;
  const m = /^Bearer\s+(.+)$/i.exec(auth);
  const captured = m?.[1];
  return typeof captured === "string" && captured.length > 0
    ? captured.trim()
    : null;
}

const app = createMcpExpressApp();

// Gate every /mcp request behind the token when it is configured. `/health` is
// intentionally excluded so process managers can still liveness-check the
// server without a credential. Failures never reach tool execution.
if (tokenActive) {
  app.use("/mcp", (req: unknown, res: unknown, next: unknown) => {
    const reqTyped = req as { headers: { authorization?: string } };
    const resTyped = res as {
      status: (s: number) => { json: (b: unknown) => void };
    };
    const nextTyped = next as () => void;
    const provided = extractBearerToken(reqTyped);
    if (provided !== null && tokenMatches(httpToken, provided)) {
      nextTyped();
      return;
    }
    resTyped.status(401).json({
      jsonrpc: "2.0",
      id: null,
      error: {
        code: -32001,
        message: "Unauthorized: missing or invalid Bearer token",
      },
    });
  });
}

// Store transports by session ID
const transports: Record<string, StreamableHTTPServerTransport> = {};

// Create a fresh MCP server + RedisClient per session
function createSessionServer(): { server: McpServer; redisClient: RedisClient } {
  const redisClient = new RedisClient(null);
  const server = new McpServer({
    name: "gptqueue",
    version: "1.0.0",
  }, { instructions: GPTQUEUE_INSTRUCTIONS });
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

app.listen(port, host, () => {
  console.log(`gptqueue HTTP server listening on ${host}:${port}`);
  console.log(`  token auth: ${tokenActive ? "active" : "disabled"}`);
  console.log(`  MCP endpoint: http://${host}:${port}/mcp`);
  console.log(`  Health check: http://${host}:${port}/health`);
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
