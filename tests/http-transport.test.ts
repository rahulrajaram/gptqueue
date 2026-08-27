import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { createServer, type Server } from "node:http";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerTools } from "../src/transports/setup-tools.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";
const TEST_PORT = 3198;

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

describe("HTTP transport (NXT-018)", () => {
  let httpServer: Server;
  let cleanup: Redis;
  const transports: Record<string, StreamableHTTPServerTransport> = {};
  const redisClients: RedisClient[] = [];

  beforeAll(async () => {
    // Start a minimal HTTP server with MCP Streamable HTTP transport
    const { createMcpExpressApp } = await import(
      "@modelcontextprotocol/sdk/server/express.js"
    );
    const app = createMcpExpressApp();

    app.post("/mcp", async (req: any, res: any) => {
      const sessionId = req.headers["mcp-session-id"] as string | undefined;

      if (sessionId && transports[sessionId]) {
        await transports[sessionId].handleRequest(req, res, req.body);
        return;
      }

      if (!sessionId && isInitializeRequest(req.body)) {
        const redisClient = new RedisClient(null, TEST_REDIS_URL);
        redisClients.push(redisClient);
        const server = new McpServer({ name: "gptqueue-test", version: "1.0.0" });
        registerTools(server, redisClient);

        const transport = new StreamableHTTPServerTransport({
          sessionIdGenerator: () => randomUUID(),
          onsessioninitialized: (sid: string) => {
            transports[sid] = transport;
          },
        });

        transport.onclose = () => {
          const sid = transport.sessionId;
          if (sid) delete transports[sid];
          redisClient.shutdown().catch(() => {});
        };

        await server.connect(transport);
        await transport.handleRequest(req, res, req.body);
        return;
      }

      res.status(400).json({ error: "Bad request" });
    });

    app.get("/mcp", async (req: any, res: any) => {
      const sessionId = req.headers["mcp-session-id"] as string | undefined;
      if (!sessionId || !transports[sessionId]) {
        res.status(404).json({ error: "Session not found" });
        return;
      }
      await transports[sessionId].handleRequest(req, res);
    });

    app.delete("/mcp", async (req: any, res: any) => {
      const sessionId = req.headers["mcp-session-id"] as string | undefined;
      if (!sessionId || !transports[sessionId]) {
        res.status(404).json({ error: "Session not found" });
        return;
      }
      await transports[sessionId].handleRequest(req, res);
    });

    httpServer = createServer(app);
    await new Promise<void>((resolve) => httpServer.listen(TEST_PORT, resolve));
  });

  afterAll(async () => {
    for (const transport of Object.values(transports)) {
      await transport.close();
    }
    for (const rc of redisClients) {
      await rc.shutdown().catch(() => {});
    }
    await new Promise<void>((resolve) => httpServer.close(() => resolve()));
  });

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup);
    await cleanup.quit();
  });

  it("connects via HTTP, registers, lists agents", async () => {
    const clientTransport = new StreamableHTTPClientTransport(
      new URL(`http://127.0.0.1:${TEST_PORT}/mcp`)
    );
    const client = new Client({ name: "test-client", version: "1.0.0" });
    await client.connect(clientTransport);

    // Register
    const regResult = await client.callTool({
      name: "register_agent",
      arguments: {
        name: "http-agent-1",
        role: "both",
        description: "HTTP test agent",
      },
    });
    const regParsed = JSON.parse((regResult.content as any)[0].text);
    expect(regParsed.status).toBe("registered");
    expect(regParsed.name).toBe("http-agent-1");
    expect(regParsed.session_id).toBeTruthy();

    // List agents
    const listResult = await client.callTool({
      name: "list_agents",
      arguments: {},
    });
    const agents = JSON.parse((listResult.content as any)[0].text);
    expect(agents.find((a: any) => a.name === "http-agent-1")).toBeDefined();

    await client.close();
  });

  it("sends and receives messages over HTTP between two clients", async () => {
    // Client A
    const transportA = new StreamableHTTPClientTransport(
      new URL(`http://127.0.0.1:${TEST_PORT}/mcp`)
    );
    const clientA = new Client({ name: "client-a", version: "1.0.0" });
    await clientA.connect(transportA);

    await clientA.callTool({
      name: "register_agent",
      arguments: { name: "http-sender", role: "publisher", description: "sends" },
    });

    // Client B
    const transportB = new StreamableHTTPClientTransport(
      new URL(`http://127.0.0.1:${TEST_PORT}/mcp`)
    );
    const clientB = new Client({ name: "client-b", version: "1.0.0" });
    await clientB.connect(transportB);

    await clientB.callTool({
      name: "register_agent",
      arguments: { name: "http-receiver", role: "consumer", description: "receives" },
    });

    // Send from A to B
    const sendResult = await clientA.callTool({
      name: "send_message",
      arguments: {
        to: "http-receiver",
        content: "hello over HTTP",
        type: "task",
      },
    });
    const sendParsed = JSON.parse((sendResult.content as any)[0].text);
    expect(sendParsed.status).toBe("sent");

    // Receive on B
    const recvResult = await clientB.callTool({
      name: "receive_message",
      arguments: { timeout: 3 },
    });
    const msg = JSON.parse((recvResult.content as any)[0].text);
    expect(msg.payload.content).toBe("hello over HTTP");
    expect(msg.from).toBe("http-sender");

    await clientA.close();
    await clientB.close();
  });
});
