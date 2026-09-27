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
import { VERSION } from "../version.js";

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
// A Map, not an object literal: session ids come from a client header, and a
// lookup like `constructor` must not resolve to an inherited property.
const transports = new Map<string, StreamableHTTPServerTransport>();

// ---------------------------------------------------------------------------
// Session limits
//
// Each session owns a RedisClient (two connections plus lease-refresh and
// heartbeat timers). A client that vanishes without DELETE would hold them,
// and keep its agent leased online, until the process exits. A session with
// no open request (an SSE GET stream counts as open) for
// GPTQUEUE_HTTP_IDLE_TIMEOUT_MS (default 1 hour; 0 disables) is closed; the
// client's next call gets the spec'd 404 and re-initializes. At most
// GPTQUEUE_HTTP_MAX_SESSIONS (default 256) sessions exist at once; further
// initialize requests get 503.
// ---------------------------------------------------------------------------
const envInt = (name: string, fallback: number): number => {
  const parsed = parseInt(process.env[name] ?? "", 10);
  return Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
};
const idleTimeoutMs = envInt("GPTQUEUE_HTTP_IDLE_TIMEOUT_MS", 60 * 60 * 1000);
const maxSessions = envInt("GPTQUEUE_HTTP_MAX_SESSIONS", 256);
const activity = new Map<string, { lastSeen: number; open: number }>();

/** Mark a request on a session open until its response closes. */
function trackRequest(sessionId: string, res: { on(event: "close", cb: () => void): unknown }): void {
  const entry = activity.get(sessionId) ?? { lastSeen: Date.now(), open: 0 };
  entry.open += 1;
  entry.lastSeen = Date.now();
  activity.set(sessionId, entry);
  res.on("close", () => {
    entry.open -= 1;
    entry.lastSeen = Date.now();
  });
}

if (idleTimeoutMs > 0) {
  setInterval(() => {
    const cutoff = Date.now() - idleTimeoutMs;
    for (const [sid, entry] of activity) {
      const transport = transports.get(sid);
      if (!transport) {
        activity.delete(sid);
      } else if (entry.open === 0 && entry.lastSeen < cutoff) {
        activity.delete(sid);
        console.log(`[gptqueue-http] closing idle session ${sid}`);
        transport.close().catch(() => {});
      }
    }
  }, Math.min(Math.max(Math.floor(idleTimeoutMs / 2), 250), 60_000)).unref();
}

// Create a fresh MCP server + RedisClient per session
function createSessionServer(): { server: McpServer; redisClient: RedisClient } {
  const redisClient = new RedisClient(null);
  const server = new McpServer({
    name: "gptqueue",
    version: VERSION,
  }, { instructions: GPTQUEUE_INSTRUCTIONS });
  registerTools(server, redisClient);
  return { server, redisClient };
}

// POST /mcp -- handle tool calls and initialization
app.post("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"] as string | undefined;

  const existing = sessionId ? transports.get(sessionId) : undefined;
  if (sessionId && existing) {
    // Existing session
    trackRequest(sessionId, res);
    await existing.handleRequest(req, res, req.body);
    return;
  }

  if (isInitializeRequest(req.body)) {
    if (transports.size >= maxSessions) {
      res.status(503).json({
        error: `session limit reached (${maxSessions}); close idle sessions or raise GPTQUEUE_HTTP_MAX_SESSIONS`,
      });
      return;
    }
    // New (or re-)session: an initialize with a stale/unknown session id
    // (e.g. after a server restart) starts a fresh session; the client
    // adopts the new id from the response header. This makes server
    // restarts self-healing for clients that cache their session id.
    const { server, redisClient } = createSessionServer();

    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      onsessioninitialized: (sid) => {
        transports.set(sid, transport);
        activity.set(sid, { lastSeen: Date.now(), open: 0 });
      },
    });

    transport.onclose = () => {
      const sid = transport.sessionId;
      if (sid) transports.delete(sid);
      if (sid) activity.delete(sid);
      redisClient.shutdown().catch(() => {});
    };

    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
    return;
  }

  if (sessionId) {
    // Unknown/expired session with a non-initialize request: the MCP spec
    // reserves 404 for this so spec-compliant clients re-initialize.
    res.status(404).json({
      error: "session expired or unknown; re-initialize to obtain a new session",
    });
    return;
  }

  res.status(400).json({
    error:
      "Bad request: no mcp-session-id header and the body is not an initialize request. Send an initialize request first.",
  });
});

// GET /mcp -- SSE stream for server-initiated messages
app.get("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"] as string | undefined;
  const transport = sessionId ? transports.get(sessionId) : undefined;
  if (!sessionId || !transport) {
    res.status(404).json({ error: "Session not found" });
    return;
  }
  trackRequest(sessionId, res);
  await transport.handleRequest(req, res);
});

// DELETE /mcp -- close session
app.delete("/mcp", async (req, res) => {
  const sessionId = req.headers["mcp-session-id"] as string | undefined;
  const transport = sessionId ? transports.get(sessionId) : undefined;
  if (!sessionId || !transport) {
    res.status(404).json({ error: "Session not found" });
    return;
  }
  await transport.handleRequest(req, res);
});

// Health check
app.get("/health", (_req, res) => {
  res.json({
    status: "ok",
    sessions: transports.size,
  });
});

// Opt-in Unix-domain-socket listen mode (GPTQUEUE_HTTP_SOCKET). When set, the
// server listens on that filesystem socket path instead of TCP host:port.
// Intended for network-less confined environments (e.g. sandboxed harness
// runners) where TCP loopback is unavailable; wire behavior on /mcp and
// /health is otherwise identical.
const udsSocketPath = process.env.GPTQUEUE_HTTP_SOCKET || "";

if (udsSocketPath) {
  app.listen(udsSocketPath, () => {
    console.log(`gptqueue HTTP server listening on unix:${udsSocketPath}`);
    console.log(`  token auth: ${tokenActive ? "active" : "disabled"}`);
    console.log(`  MCP endpoint: http://unix:${udsSocketPath}/mcp`);
    console.log(`  Health check: http://unix:${udsSocketPath}/health`);
  });
} else {
  app.listen(port, host, () => {
    console.log(`gptqueue HTTP server listening on ${host}:${port}`);
    console.log(`  token auth: ${tokenActive ? "active" : "disabled"}`);
    console.log(`  MCP endpoint: http://${host}:${port}/mcp`);
    console.log(`  Health check: http://${host}:${port}/health`);
  });
}

// Graceful shutdown
async function shutdown() {
  for (const [sid, transport] of transports) {
    await transport.close();
    transports.delete(sid);
  }
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
