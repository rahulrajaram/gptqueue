import { createHash, randomBytes, randomUUID, timingSafeEqual } from "node:crypto";
import type { Server as HttpServer } from "node:http";
import type { AddressInfo, Socket } from "node:net";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { createMcpExpressApp } from "@modelcontextprotocol/sdk/server/express.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import type { RedisClient } from "../mcp-server/redis-client.js";
import { getQueueStatus, queueStatusSchema } from "../mcp-server/tools/queue-status.js";
import { listAgents } from "../mcp-server/tools/list-agents.js";
import {
  receiveMessage,
  receiveMessageSchema,
} from "../mcp-server/tools/receive-message.js";
import { sendMessage, sendMessageSchema } from "../mcp-server/tools/send-message.js";
import { stableToolError, toolResult } from "../mcp-server/tool-result.js";

const LOOPBACK_HOST = "127.0.0.1";
const MAX_TRANSPORT_SESSIONS = 4;
const BRIDGE_CLOSE_GRACE_MS = 1_000;

export const WRAPPER_VISIBLE_TOOLS = Object.freeze([
  "send_message",
  "receive_message",
  "list_agents",
  "get_queue_status",
] as const);

export interface BoundBridge {
  readonly url: string;
  readonly bearerToken: string;
  readonly close: () => Promise<void>;
}

interface BridgeOptions {
  readonly agentName: string;
  readonly redisClient: RedisClient;
}

const digest = (value: string): Buffer =>
  createHash("sha256").update(value).digest();

const tokensMatch = (expected: string, provided: string): boolean =>
  timingSafeEqual(digest(expected), digest(provided));

const bearerFrom = (authorization: string | undefined): string | null => {
  const match = /^Bearer\s+(.+)$/i.exec(authorization ?? "");
  const token = match?.[1]?.trim();
  return token ? token : null;
};

const boundSendMessageSchema = sendMessageSchema.omit({ session_id: true });
const boundReceiveMessageSchema = receiveMessageSchema
  .omit({ session_id: true })
  .extend({
    timeout: z
      .number()
      .int()
      .min(1)
      .max(60)
      .default(5)
      .describe("Blocking timeout in whole seconds (default 5; range 1-60)"),
  });

type BoundReceiveParams = z.infer<typeof boundReceiveMessageSchema>;

const safeToolCall = async <T>(call: () => Promise<T>) => {
  try {
    return await call();
  } catch (error) {
    return stableToolError(error);
  }
};

const receiveBoundMessage = async (
  redisClient: RedisClient,
  params: BoundReceiveParams,
  isClosing: () => boolean
) => {
  const deadline = Date.now() + params.timeout * 1_000;
  while (!isClosing()) {
    const result = await receiveMessage(redisClient, {
      ...params,
      timeout: 1,
    });
    if (result.structuredContent.status !== "no_messages") return result;
    if (Date.now() >= deadline) {
      return toolResult({ status: "no_messages", timeout: params.timeout });
    }
  }
  return toolResult({ status: "no_messages", timeout: params.timeout });
};

const registerBoundTools = (
  server: McpServer,
  redisClient: RedisClient,
  isClosing: () => boolean
): void => {
  server.tool(
    "send_message",
    "[safety: writable] Send a message as the identity already bound to this wrapper.",
    boundSendMessageSchema.shape,
    async (params) =>
      safeToolCall(() =>
        sendMessage(redisClient, boundSendMessageSchema.parse(params))
      )
  );
  server.tool(
    "receive_message",
    "[safety: writable] Receive and consume the next message for the identity already bound to this wrapper.",
    boundReceiveMessageSchema.shape,
    async (params) =>
      safeToolCall(() =>
        receiveBoundMessage(
          redisClient,
          boundReceiveMessageSchema.parse(params),
          isClosing
        )
      )
  );
  server.tool(
    "list_agents",
    "[safety: readonly] List registered GPTQueue agents and their presence.",
    {},
    async () => safeToolCall(() => listAgents(redisClient))
  );
  server.tool(
    "get_queue_status",
    "[safety: readonly] Get queue depth and metadata for an agent or all agents.",
    queueStatusSchema.shape,
    async (params) =>
      safeToolCall(() =>
        getQueueStatus(redisClient, queueStatusSchema.parse(params))
      )
  );
};

const listen = (app: ReturnType<typeof createMcpExpressApp>): Promise<HttpServer> =>
  new Promise((resolve, reject) => {
    const server = app.listen(0, LOOPBACK_HOST, () => resolve(server));
    server.once("error", reject);
  });

const closeServer = (server: HttpServer): Promise<void> =>
  new Promise((resolve, reject) => {
    server.close((error) => (error ? reject(error) : resolve()));
  });

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolveDelay) => {
    const timer = setTimeout(resolveDelay, milliseconds);
    timer.unref();
  });

/**
 * Start a single-runtime MCP bridge around an already registered RedisClient.
 *
 * The app-level GPTQueue session remains private to this process. The wrapped
 * CLI receives only a random, per-launch HTTP bearer token and sees a normal
 * MCP server whose session-scoped tools are already bound to `agentName`.
 */
export async function startBoundBridge(
  options: BridgeOptions
): Promise<BoundBridge> {
  if (!options.redisClient.registered || !options.redisClient.sessionId) {
    throw new Error("Cannot start bridge before GPTQueue registration succeeds.");
  }
  if (options.redisClient.agentName !== options.agentName) {
    throw new Error(
      `Bridge identity mismatch: expected ${options.agentName}, got ${options.redisClient.agentName ?? "unregistered"}.`
    );
  }

  const bearerToken = randomBytes(32).toString("base64url");
  const transports = new Map<string, StreamableHTTPServerTransport>();
  const app = createMcpExpressApp();
  let closing = false;

  app.use("/mcp", (req, res, next) => {
    const supplied = bearerFrom(req.headers.authorization);
    if (supplied !== null && tokensMatch(bearerToken, supplied)) {
      next();
      return;
    }
    res.status(401).json({
      jsonrpc: "2.0",
      id: null,
      error: { code: -32001, message: "Unauthorized bridge connection" },
    });
  });

  const createSessionServer = (): McpServer => {
    const server = new McpServer(
      { name: "gptqueue-registered-wrapper", version: "1.0.0-experimental" },
      {
        instructions:
          `This MCP connection is already registered in GPTQueue as "${options.agentName}". ` +
          "Do not call register_agent or supply session_id. Use the available messaging tools directly.",
      }
    );
    registerBoundTools(server, options.redisClient, () => closing);
    return server;
  };

  app.post("/mcp", async (req, res) => {
    const transportSessionId = req.headers["mcp-session-id"] as
      | string
      | undefined;
    const requestId = (req.body as { id?: unknown } | undefined)?.id ?? null;
    const existing = transportSessionId
      ? transports.get(transportSessionId)
      : undefined;
    if (existing) {
      await existing.handleRequest(req, res, req.body);
      return;
    }

    if (!isInitializeRequest(req.body)) {
      const expired = Boolean(transportSessionId);
      res.status(expired ? 404 : 400).json({
        jsonrpc: "2.0",
        id: requestId,
        error: {
          code: expired ? -32003 : -32600,
          message: expired
            ? "bridge transport session expired; re-initialize"
            : "initialize request required",
        },
      });
      return;
    }

    if (transports.size >= MAX_TRANSPORT_SESSIONS) {
      res.status(429).json({
        jsonrpc: "2.0",
        id: requestId,
        error: { code: -32002, message: "bridge transport capacity reached" },
      });
      return;
    }

    const server = createSessionServer();
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: () => randomUUID(),
      onsessioninitialized: (sessionId) => {
        transports.set(sessionId, transport);
      },
    });
    transport.onclose = () => {
      const sessionId = transport.sessionId;
      if (sessionId) transports.delete(sessionId);
    };
    await server.connect(transport);
    await transport.handleRequest(req, res, req.body);
  });

  app.get("/mcp", async (req, res) => {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    const transport = sessionId ? transports.get(sessionId) : undefined;
    if (!transport) {
      res.status(404).json({
        jsonrpc: "2.0",
        id: null,
        error: { code: -32003, message: "bridge transport session not found" },
      });
      return;
    }
    await transport.handleRequest(req, res);
  });

  app.delete("/mcp", async (req, res) => {
    const sessionId = req.headers["mcp-session-id"] as string | undefined;
    const transport = sessionId ? transports.get(sessionId) : undefined;
    if (!transport) {
      res.status(404).json({
        jsonrpc: "2.0",
        id: null,
        error: { code: -32003, message: "bridge transport session not found" },
      });
      return;
    }
    await transport.handleRequest(req, res);
  });

  app.get("/health", (_req, res) => {
    res.json({ status: "ok", bound: true });
  });

  const httpServer = await listen(app);
  const sockets = new Set<Socket>();
  httpServer.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });
  const address = httpServer.address() as AddressInfo | null;
  if (!address) {
    await closeServer(httpServer);
    throw new Error("Bridge started without a TCP address.");
  }

  let closePromise: Promise<void> | null = null;
  const close = async (): Promise<void> => {
    if (closePromise) return closePromise;
    closing = true;
    closePromise = (async () => {
      const transportsClosed = Promise.allSettled(
        [...transports.values()].map((transport) => transport.close())
      );
      const serverClosed = closeServer(httpServer);

      await Promise.race([
        Promise.all([transportsClosed, serverClosed]),
        delay(BRIDGE_CLOSE_GRACE_MS),
      ]);

      // An MCP client can leave an SSE request open indefinitely. Stop
      // accepting connections first, then force-close anything that ignored
      // the graceful transport shutdown so wrapper teardown stays bounded.
      for (const socket of sockets) socket.destroy();
      await Promise.race([serverClosed, delay(BRIDGE_CLOSE_GRACE_MS)]);
      transports.clear();
    })();
    try {
      await closePromise;
    } catch (error) {
      closePromise = null;
      throw error;
    }
  };

  return Object.freeze({
    url: `http://${LOOPBACK_HOST}:${address.port}/mcp`,
    bearerToken,
    close,
  });
}
