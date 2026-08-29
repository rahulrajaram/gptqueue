import { describe, it, expect, beforeAll, afterAll, beforeEach } from "vitest";
import { createServer, type Server } from "node:http";
import { execSync, spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerTools } from "../src/transports/setup-tools.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const TEST_PORT = 3198;
// Root of the repo (tests/ -> repo root), used to spawn the built server.
const ROOT = new URL("..", import.meta.url).pathname;

// MCP initialize JSON-RPC body used for raw HTTP assertions.
const JSON_HEADERS = {
  "content-type": "application/json",
  accept: "application/json, text/event-stream",
};
const INIT_BODY = JSON.stringify({
  jsonrpc: "2.0",
  id: 1,
  method: "initialize",
  params: {
    protocolVersion: "2025-03-26",
    capabilities: {},
    clientInfo: { name: "auth-probe", version: "1.0.0" },
  },
});

// Module-level build guard (mirrors tests/helpers/integration-server.ts) so the
// spawned-server tests always run against a fresh dist build.
let buildPromise: Promise<void> | null = null;
function ensureBuilt(): Promise<void> {
  if (!buildPromise) {
    buildPromise = (async () => {
      execSync("npm run build", { cwd: ROOT, stdio: "pipe", env: process.env });
    })();
  }
  return buildPromise;
}

async function getFreePort(): Promise<number> {
  const net = await import("node:net");
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.unref();
    srv.once("error", reject);
    srv.listen(0, "127.0.0.1", () => {
      const addr = srv.address() as net.AddressInfo;
      const port = addr.port;
      srv.close(() => resolve(port));
    });
  });
}

interface Spawned {
  child: ChildProcess;
  port: number;
  url: string;
  stdout: string;
  stderr: string;
}

/** Spawn the real built HTTP server with an explicit env (REDIS_URL db-n scoped). */
async function spawnHttpServer(
  env: Record<string, string>
): Promise<Spawned> {
  await ensureBuilt();
  const port = await getFreePort();
  const child = spawn(
    process.execPath,
    ["dist/transports/http.js", "--port", String(port)],
    {
      cwd: ROOT,
      env: { ...process.env, REDIS_URL: TEST_REDIS_URL, ...env },
      stdio: ["ignore", "pipe", "pipe"],
    }
  );
  const rec = { stdout: "", stderr: "" };
  child.stdout?.on("data", (d: Buffer) => {
    rec.stdout = (rec.stdout + d.toString()).slice(-8000);
  });
  child.stderr?.on("data", (d: Buffer) => {
    rec.stderr = (rec.stderr + d.toString()).slice(-8000);
  });
  // Return the SAME live object the data handlers mutate, so captured output
  // keeps accumulating after spawn (a spread copy would freeze stdout/stderr at
  // spawn time).
  return Object.assign(rec, {
    child,
    port,
    url: `http://127.0.0.1:${port}`,
  });
}

async function waitHealthy(s: Spawned, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (s.child.exitCode !== null) {
      throw new Error(
        `server exited early (code=${s.child.exitCode})
STDOUT:\n${s.stdout}
STDERR:\n${s.stderr}`
      );
    }
    try {
      const r = await fetch(`${s.url}/health`, {
        signal: AbortSignal.timeout(1000),
      });
      if (r.ok) return;
    } catch {
      /* not ready yet */
    }
    await new Promise((res) => setTimeout(res, 150));
  }
  throw new Error(`server not healthy in ${timeoutMs}ms\nSTDERR:\n${s.stderr}`);
}

async function killChild(s: Spawned): Promise<void> {
  if (s.child.exitCode === null) {
    s.child.kill("SIGKILL");
    await new Promise((res) => setTimeout(res, 150));
  }
}

/** Poll the spawned server's captured stdout until it contains `marker`. */
async function waitForStdout(
  s: Spawned,
  marker: string,
  timeoutMs = 5000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (s.stdout.includes(marker)) return;
    if (s.child.exitCode !== null) {
      throw new Error(
        `server exited before logging '${marker}' (code=${s.child.exitCode})\nSTDERR:\n${s.stderr}`
      );
    }
    await new Promise((res) => setTimeout(res, 100));
  }
  throw new Error(
    `stdout never contained '${marker}' within ${timeoutMs}ms.\nSTDOUT:\n${s.stdout}\nSTDERR:\n${s.stderr}`
  );
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
    await flushTestKeys(cleanup, TEST_REDIS_URL);
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

describe("HTTP transport security (loopback bind + Bearer token)", () => {
  it("binds loopback by default; health + MCP work tokenless", async () => {
    const s = await spawnHttpServer({});
    try {
      await waitHealthy(s);
      // Startup log states the bind host and that token auth is disabled.
      await waitForStdout(s, "listening on 127.0.0.1:");
      expect(s.stdout).toContain("127.0.0.1");
      expect(s.stdout).toContain("token auth: disabled");

      const health = await fetch(`${s.url}/health`);
      expect(health.ok).toBe(true);
      expect((await health.json()).status).toBe("ok");

      // A request to 127.0.0.1 reaches the MCP endpoint (initialize accepted).
      const init = await fetch(`${s.url}/mcp`, {
        method: "POST",
        headers: JSON_HEADERS,
        body: INIT_BODY,
      });
      expect(init.status).toBeGreaterThanOrEqual(200);
      expect(init.status).toBeLessThan(300);
    } finally {
      await killChild(s);
    }
  });

  it("enforces Bearer token: missing/wrong 401, correct token executes a tool", async () => {
    const bearer = "itest-bearer-local-01";
    const s = await spawnHttpServer({ GPTQUEUE_HTTP_TOKEN: bearer });
    try {
      await waitHealthy(s);
      await waitForStdout(s, "token auth: active");
      expect(s.stdout).toContain("token auth: active");

      // /health stays unauthenticated (liveness for process managers).
      const health = await fetch(`${s.url}/health`);
      expect(health.ok).toBe(true);

      // Missing token -> 401 with a JSON-RPC error body.
      const missing = await fetch(`${s.url}/mcp`, {
        method: "POST",
        headers: JSON_HEADERS,
        body: INIT_BODY,
      });
      expect(missing.status).toBe(401);
      const missingBody = (await missing.json()) as {
        error?: { code?: number };
      };
      expect(missingBody.error?.code).toBe(-32001);

      // Wrong token -> 401.
      const wrong = await fetch(`${s.url}/mcp`, {
        method: "POST",
        headers: { ...JSON_HEADERS, authorization: "Bearer wrong-token" },
        body: INIT_BODY,
      });
      expect(wrong.status).toBe(401);

      // Correct token -> passes the guard and a tool actually executes.
      const clientTransport = new StreamableHTTPClientTransport(
        new URL(`${s.url}/mcp`),
        { requestInit: { headers: { authorization: `Bearer ${bearer}` } } }
      );
      const client = new Client({ name: "auth-client", version: "1.0.0" });
      await client.connect(clientTransport);
      const regResult = await client.callTool({
        name: "register_agent",
        arguments: {
          name: "auth-agent",
          role: "both",
          description: "auth token test",
        },
      });
      const parsed = JSON.parse((regResult.content as any)[0].text);
      expect(parsed.status).toBe("registered");
      expect(parsed.name).toBe("auth-agent");
      await client.close();
    } finally {
      await killChild(s);
    }
  });

  it("on a non-loopback host without a token, refuses to start (exit non-zero, clear message)", async () => {
    await ensureBuilt();
    const port = await getFreePort();
    const child = spawn(
      process.execPath,
      ["dist/transports/http.js", "--port", String(port)],
      {
        cwd: ROOT,
        // GPTQUEUE_HOST set to a non-loopback; token explicitly emptied so the
        // host env can never leak a value into this refusal test.
        env: {
          ...process.env,
          REDIS_URL: TEST_REDIS_URL,
          GPTQUEUE_HOST: "0.0.0.0",
          GPTQUEUE_HTTP_TOKEN: "",
        },
        stdio: ["ignore", "pipe", "pipe"],
      }
    );
    let stderr = "";
    let stdout = "";
    child.stderr?.on("data", (d: Buffer) => (stderr += d.toString()));
    child.stdout?.on("data", (d: Buffer) => (stdout += d.toString()));

    const code = await new Promise<number | null>((resolve) => {
      child.on("exit", (c) => resolve(c));
      // Safety net: never hang the suite if the child fails to exit.
      setTimeout(() => {
        child.kill("SIGKILL");
        resolve(child.exitCode);
      }, 8000).unref?.();
    });

    expect(code).not.toBe(0);
    expect(stderr).toContain("REFUSING TO START");
    expect(stdout).not.toContain("listening");
  });

  it("on a non-loopback host with a token, starts and enforces the token", async () => {
    const bearer = "itest-bearer-nonloop-02";
    const s = await spawnHttpServer({
      GPTQUEUE_HOST: "0.0.0.0",
      GPTQUEUE_HTTP_TOKEN: bearer,
    });
    try {
      await waitHealthy(s);
      await waitForStdout(s, "listening on 0.0.0.0:");
      expect(s.stdout).toContain("0.0.0.0");
      expect(s.stdout).toContain("token auth: active");

      // /health is unauthenticated even on non-loopback with token active.
      const health = await fetch(`${s.url}/health`);
      expect(health.ok).toBe(true);

      // Missing token -> 401.
      const missing = await fetch(`${s.url}/mcp`, {
        method: "POST",
        headers: JSON_HEADERS,
        body: INIT_BODY,
      });
      expect(missing.status).toBe(401);

      // Correct token -> accepted.
      const ok = await fetch(`${s.url}/mcp`, {
        method: "POST",
        headers: { ...JSON_HEADERS, authorization: `Bearer ${bearer}` },
        body: INIT_BODY,
      });
      expect(ok.status).toBeGreaterThanOrEqual(200);
      expect(ok.status).toBeLessThan(300);
    } finally {
      await killChild(s);
    }
  });
});
