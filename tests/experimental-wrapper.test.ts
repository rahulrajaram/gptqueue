import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import { once } from "node:events";
import { createHash } from "node:crypto";
import { connect } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { Redis } from "ioredis";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import {
  startBoundBridge,
  WRAPPER_VISIBLE_TOOLS,
} from "../src/experimental-wrapper/bridge.js";
import {
  buildCodexInvocation,
  buildPiInvocation,
  DEFAULT_REDIS_URL,
  parseWrapperArgs,
  renderPiExtension,
} from "../src/experimental-wrapper/config.js";
import {
  exitCodeFor,
  requireWorkspaceInsideRepository,
  runExperimentalWrapper,
} from "../src/experimental-wrapper/index.js";
import { acquireWrapperIdentityClaim } from "../src/experimental-wrapper/identity-claim.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const TEST_REDIS_URL = "redis://127.0.0.1:6379/15";
const REPOSITORY_ROOT = resolve(
  dirname(fileURLToPath(import.meta.url)),
  ".."
);

const toolPayload = (result: Awaited<ReturnType<Client["callTool"]>>) =>
  JSON.parse((result.content as Array<{ text: string }>)[0]?.text ?? "null");

describe("experimental registered-wrapper configuration", () => {
  it("parses a non-db0 launch and rejects the live Redis database", () => {
    const parsed = parseWrapperArgs([
      "codex",
      "--agent",
      "wrapper-codex",
      "--workspace",
      ".gptqueue/live",
      "--",
      "list agents",
    ]);
    expect(parsed.agent).toBe("wrapper-codex");
    expect(parsed.redisUrl).toBe(DEFAULT_REDIS_URL);
    expect(parsed.prompt).toBe("list agents");

    expect(() =>
      parseWrapperArgs([
        "pi",
        "--agent",
        "wrapper-pi",
        "--workspace",
        ".gptqueue/live",
        "--redis-url",
        "redis://127.0.0.1:6379/0",
        "--",
        "list agents",
      ])
    ).toThrow(/refuses Redis db0/u);

    expect(() =>
      parseWrapperArgs([
        "codex",
        "--agent",
        "wrapper-codex",
        "--workspace",
        ".gptqueue/live",
        "--redis-url",
        "redis://127.0.0.1:6379/15abc",
        "--",
        "list agents",
      ])
    ).toThrow(/invalid database/u);
    expect(() =>
      parseWrapperArgs([
        "codex",
        "--agent",
        "--workspace",
        ".gptqueue/live",
        "--",
        "list agents",
      ])
    ).toThrow(/--agent requires a value/u);
    expect(() =>
      parseWrapperArgs([
        "codex",
        "--bogus",
        "--agent",
        "wrapper-codex",
        "--workspace",
        ".gptqueue/live",
        "--",
        "list agents",
      ])
    ).toThrow(/Unknown option: --bogus/u);
    expect(() =>
      parseWrapperArgs([
        "codex",
        "--agent",
        "wrapper-codex",
        "--workspace",
        ".gptqueue/live",
        "--cleanup",
        "unregister",
        "--",
        "list agents",
      ])
    ).toThrow(/requires an agent name starting with gptqueue-experiment-/u);
  });

  it("points Codex at a required authenticated bridge without exposing registration", () => {
    const options = parseWrapperArgs([
      "codex",
      "--agent",
      "wrapper-codex",
      "--workspace",
      ".gptqueue/live",
      "--",
      "send a message",
    ]);
    const invocation = buildCodexInvocation(
      options,
      "http://127.0.0.1:43210/mcp"
    );
    expect(invocation.command).toBe(
      process.env.GPTQ_EXPERIMENT_CODEX_BIN || "codex"
    );
    expect(invocation.args).toContain(
      'mcp_servers.gptqueue_wrapper.url="http://127.0.0.1:43210/mcp"'
    );
    expect(invocation.args).toContain("mcp_servers.gptqueue_wrapper.required=true");
    expect(invocation.args.join("\n")).toContain("GPTQ_BRIDGE_TOKEN");
    const enabledTools = invocation.args.find((arg) =>
      arg.startsWith("mcp_servers.gptqueue_wrapper.enabled_tools=")
    );
    expect(enabledTools).toBeTruthy();
    expect(JSON.parse(enabledTools!.split("=", 2)[1]!)).toEqual([
      ...WRAPPER_VISIBLE_TOOLS,
    ]);
    expect(invocation.args).toContain("--ignore-user-config");
    expect(invocation.args).toContain("--ephemeral");
  });

  it("loads only the explicit Pi extension and renders isolated adapter config", () => {
    const options = parseWrapperArgs([
      "pi",
      "--agent",
      "wrapper-pi",
      "--workspace",
      ".gptqueue/live",
      "--",
      "receive a message",
    ]);
    const invocation = buildPiInvocation(options, "/tmp/pi-extension.ts");
    expect(invocation.args.slice(0, 2)).toEqual(["-p", "--offline"]);
    expect(invocation.args).toContain("--no-extensions");
    expect(invocation.args).toContain("--no-builtin-tools");
    expect(invocation.args).toContain("/tmp/pi-extension.ts");
    expect(invocation.args).toContain("/tmp/pi-sessions");

    const source = renderPiExtension("/trusted/pi-mcp-adapter/index.ts");
    expect(source).toContain('lifecycle: "eager"');
    expect(source).toContain('auth: "bearer"');
    expect(source).toContain("disableProxyTool: true");
    expect(source).toContain('toolPrefix: "none"');
    expect(source).toContain("GPTQ_PI_ADAPTER_STATE_DIR");
    expect(source).toContain(JSON.stringify(WRAPPER_VISIBLE_TOOLS));
    expect(source).not.toContain("GPTQ_SESSION_ID");
  });

  it("uses conventional signal exit codes", () => {
    expect(exitCodeFor({ code: null, signal: "SIGINT" })).toBe(130);
    expect(exitCodeFor({ code: null, signal: "SIGTERM" })).toBe(143);
    expect(exitCodeFor({ code: 7, signal: null })).toBe(7);
  });

  it("canonicalizes workspaces and rejects a symlink escape", async () => {
    const ignoredRoot = join(REPOSITORY_ROOT, ".gptqueue");
    await mkdir(ignoredRoot, { recursive: true });
    const inside = await mkdtemp(join(ignoredRoot, "wrapper-inside-"));
    const outside = await mkdtemp(join(tmpdir(), "gptqueue-wrapper-outside-"));
    const link = join(inside, "escape");
    await symlink(outside, link);

    try {
      await expect(requireWorkspaceInsideRepository(inside)).resolves.toBe(
        inside
      );
      await expect(requireWorkspaceInsideRepository(link)).rejects.toThrow(
        /must remain inside/u
      );
      await expect(
        requireWorkspaceInsideRepository(join(inside, "missing"))
      ).rejects.toThrow(/does not exist or is unreadable/u);
    } finally {
      await rm(inside, { recursive: true, force: true });
      await rm(outside, { recursive: true, force: true });
    }
  });
});

describe("experimental authenticated bound bridge", () => {
  let cleanupRedis: Redis;

  beforeEach(async () => {
    cleanupRedis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanupRedis, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await flushTestKeys(cleanupRedis, TEST_REDIS_URL);
    await cleanupRedis.quit();
  });

  it("serves session-bound tools without giving the app session ID to the MCP client", async () => {
    const registration = new RedisClient(null, TEST_REDIS_URL);
    const name = `wrapper-bridge-${Date.now()}`;
    await registration.register("both", name, "bound bridge test");
    const other = new RedisClient(null, TEST_REDIS_URL);
    const otherName = `${name}-other`;
    await other.register("both", otherName, "must not be impersonated");
    const bridge = await startBoundBridge({ agentName: name, redisClient: registration });

    try {
      const unauthorized = await fetch(bridge.url, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json, text/event-stream",
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 1,
          method: "initialize",
          params: {
            protocolVersion: "2025-03-26",
            capabilities: {},
            clientInfo: { name: "unauthorized", version: "1" },
          },
        }),
      });
      expect(unauthorized.status).toBe(401);

      const wrongToken = await fetch(bridge.url, {
        method: "POST",
        headers: {
          Authorization: "Bearer definitely-not-the-wrapper-token",
          "Content-Type": "application/json",
          Accept: "application/json, text/event-stream",
        },
        body: JSON.stringify({
          jsonrpc: "2.0",
          id: 2,
          method: "initialize",
          params: {
            protocolVersion: "2025-03-26",
            capabilities: {},
            clientInfo: { name: "wrong-token", version: "1" },
          },
        }),
      });
      expect(wrongToken.status).toBe(401);

      const transport = new StreamableHTTPClientTransport(new URL(bridge.url), {
        requestInit: {
          headers: { authorization: `Bearer ${bridge.bearerToken}` },
        },
      });
      const client = new Client({ name: "bound-bridge-test", version: "1.0.0" });
      await client.connect(transport);

      const catalog = await client.listTools();
      expect(catalog.tools.map((tool) => tool.name)).toEqual([
        ...WRAPPER_VISIBLE_TOOLS,
      ]);
      for (const tool of catalog.tools) {
        expect(tool.inputSchema.properties ?? {}).not.toHaveProperty(
          "session_id"
        );
      }
      const forbidden = await client.callTool({
        name: "register_agent",
        arguments: {
          name: "hijack",
          role: "both",
          description: "must remain unavailable",
        },
      });
      expect(forbidden.isError).toBe(true);
      expect((forbidden.content as Array<{ text: string }>)[0]?.text).toMatch(
        /not found/u
      );

      const listed = toolPayload(
        await client.callTool({ name: "list_agents", arguments: {} })
      ) as Array<{ name: string }>;
      expect(listed.some((agent) => agent.name === name)).toBe(true);

      const sent = toolPayload(
        await client.callTool({
          name: "send_message",
          arguments: { to: name, content: "bound-without-session-id", session_id: other.sessionId },
        })
      );
      expect(sent.status).toBe("sent");

      const unboundedReceive = await client.callTool({
        name: "receive_message",
        arguments: { timeout: 0 },
      });
      expect(unboundedReceive.isError).toBe(true);

      const received = toolPayload(
        await client.callTool({
          name: "receive_message",
          arguments: { timeout: 1 },
        })
      );
      expect(received.from).toBe(name);
      expect(received.payload.content).toBe("bound-without-session-id");

      const emptyReceive = toolPayload(
        await client.callTool({
          name: "receive_message",
          arguments: { timeout: 2 },
        })
      );
      expect(emptyReceive).toEqual({ status: "no_messages", timeout: 2 });

      await client.callTool({
        name: "send_message",
        arguments: { to: otherName, content: "private-to-other-mailbox" },
      });
      const forgedReceive = toolPayload(await client.callTool({
        name: "receive_message", arguments: { timeout: 1, session_id: other.sessionId },
      }));
      expect(forgedReceive.status).toBe("no_messages");
      expect((await other.receiveMessage(1))?.payload.content).toBe("private-to-other-mailbox");
      const status = toolPayload(await client.callTool({
        name: "get_queue_status", arguments: { agent: name },
      }));
      expect(status[0]).toMatchObject({ agent: name, depth: 0 });

      await client.close();

      const bridgeUrl = new URL(bridge.url);
      const openConnection = connect({
        host: bridgeUrl.hostname,
        port: Number(bridgeUrl.port),
      });
      await once(openConnection, "connect");
      openConnection.write(
        `GET /mcp HTTP/1.1\r\nHost: ${bridgeUrl.host}\r\n` +
          `Authorization: Bearer ${bridge.bearerToken}\r\n`
      );
      const closeStarted = Date.now();
      try {
        await bridge.close();
        expect(Date.now() - closeStarted).toBeLessThan(3_000);
      } finally {
        openConnection.destroy();
      }
    } finally {
      await bridge.close();
      if (registration.registered) await registration.unregister();
      await registration.shutdown();
      if (other.registered) await other.unregister();
      await other.shutdown();
    }
  });

  it("registers before a stub child and unregisters a fresh disposable identity", async () => {
    const name = `gptqueue-experiment-lifecycle-${Date.now()}`;
    const workspace = join(REPOSITORY_ROOT, ".gptqueue");
    const probePath = join(workspace, `registration-probe-${Date.now()}.mjs`);
    const previousBinary = process.env.GPTQ_EXPERIMENT_CODEX_BIN;
    vi.stubEnv("GPTQ_SESSION_ID", "wrapper-test-session-sentinel");
    vi.stubEnv("GPTQUEUE_HTTP_TOKEN", "wrapper-test-token-sentinel");
    vi.stubEnv("REDIS_URL", TEST_REDIS_URL);
    await writeFile(
      probePath,
      [
        "#!/usr/bin/env node",
        'import { Redis } from "ioredis";',
        'if (["REDIS_URL", "GPTQ_SESSION_ID", "GPTQUEUE_HTTP_TOKEN"].some(key => key in process.env)) {',
        '  console.error("private parent environment leaked into wrapped CLI");',
        "  process.exit(18);",
        "}",
        `const redis = new Redis(${JSON.stringify(TEST_REDIS_URL)}, { maxRetriesPerRequest: 3 });`,
        `const present = await redis.hexists(${JSON.stringify(SESSION_KEYS.registry)}, process.env.GPTQ_AGENT_NAME);`,
        `const sessions = await redis.smembers(${JSON.stringify(SESSION_KEYS.agentSessions(name))});`,
        'const live = sessions.length === 1 && await redis.exists("gptq:lease:" + sessions[0]) === 1;',
        "await redis.quit();",
        "process.exitCode = present === 1 && live ? 0 : 17;",
        "",
      ].join("\n"),
      { encoding: "utf8", mode: 0o700 }
    );
    process.env.GPTQ_EXPERIMENT_CODEX_BIN = probePath;
    const stderr = vi.spyOn(console, "error").mockImplementation(() => {});

    try {
      await expect(
        runExperimentalWrapper([
          "codex",
          "--agent",
          name,
          "--workspace",
          workspace,
          "--redis-url",
          TEST_REDIS_URL,
          "--cleanup",
          "unregister",
          "--",
          "exit successfully",
        ])
      ).resolves.toBe(0);
      expect(await cleanupRedis.hexists(SESSION_KEYS.registry, name)).toBe(0);
      expect(await cleanupRedis.exists(SESSION_KEYS.queue(name))).toBe(0);
      expect(await cleanupRedis.exists(SESSION_KEYS.heartbeat(name))).toBe(0);
    } finally {
      vi.unstubAllEnvs();
      stderr.mockRestore();
      await rm(probePath, { force: true });
      if (previousBinary === undefined) {
        delete process.env.GPTQ_EXPERIMENT_CODEX_BIN;
      } else {
        process.env.GPTQ_EXPERIMENT_CODEX_BIN = previousBinary;
      }
    }
  });

  it.each(["missing", "replaced"] as const)(
    "closes its claim connection after a %s claim prevents cleanup",
    async (failure) => {
      const name = `gptqueue-experiment-lost-claim-${failure}-${Date.now()}`;
      const claimKey = "gptq:experimental-wrapper-claim:" +
        createHash("sha256").update(name).digest("hex");
      const workspace = join(REPOSITORY_ROOT, ".gptqueue");
      const probePath = join(workspace, `${name}.mjs`);
      const observedConnections = new Set<Redis>();
      const originalEval = Redis.prototype.eval;
      const evalSpy = vi.spyOn(Redis.prototype, "eval").mockImplementation(function (
        this: Redis, ...args: Parameters<Redis["eval"]>
      ) {
        if (args.includes(claimKey)) observedConnections.add(this);
        return originalEval.apply(this, args);
      });
      const stderr = vi.spyOn(console, "error").mockImplementation(() => {});
      await writeFile(probePath, [
        "#!/usr/bin/env node",
        'import { Redis } from "ioredis";',
        `const redis = new Redis(${JSON.stringify(TEST_REDIS_URL)}, { maxRetriesPerRequest: 3 });`,
        failure === "missing"
          ? `await redis.del(${JSON.stringify(claimKey)});`
          : `await redis.set(${JSON.stringify(claimKey)}, "foreign-claim-token");`,
        "await redis.quit();",
      ].join("\n"), { mode: 0o700 });
      vi.stubEnv("GPTQ_EXPERIMENT_CODEX_BIN", probePath);
      try {
        await expect(runExperimentalWrapper([
          "codex", "--agent", name, "--workspace", workspace,
          "--redis-url", TEST_REDIS_URL, "--cleanup", "unregister", "--", "exit",
        ])).resolves.toBe(1);
        expect(observedConnections.size).toBe(1);
        await expect.poll(() => [...observedConnections].every(
          connection => connection.status === "end"
        )).toBe(true);
        expect(await cleanupRedis.get(claimKey)).toBe(
          failure === "missing" ? null : "foreign-claim-token"
        );
        expect(await cleanupRedis.hexists(SESSION_KEYS.registry, name)).toBe(1);
        expect(await cleanupRedis.scard(SESSION_KEYS.agentSessions(name))).toBe(1);
      } finally {
        for (const connection of observedConnections) connection.disconnect();
        evalSpy.mockRestore();
        stderr.mockRestore();
        vi.unstubAllEnvs();
        await rm(probePath, { force: true });
      }
    }
  );

  it("refuses destructive cleanup for a pre-existing identity", async () => {
    const name = `gptqueue-experiment-existing-${Date.now()}`;
    const existing = new RedisClient(null, TEST_REDIS_URL);
    await existing.register("both", name, "must survive collision check");
    const stderr = vi.spyOn(console, "error").mockImplementation(() => {});

    try {
      await expect(
        runExperimentalWrapper([
          "codex",
          "--agent",
          name,
          "--workspace",
          join(REPOSITORY_ROOT, ".gptqueue"),
          "--redis-url",
          TEST_REDIS_URL,
          "--cleanup",
          "unregister",
          "--",
          "must never launch",
        ])
      ).rejects.toThrow(/pre-existing agent/u);
      expect(await cleanupRedis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    } finally {
      stderr.mockRestore();
      if (existing.registered) await existing.unregister();
      await existing.shutdown();
    }
  });

  it("refuses active identity reuse but permits the same offline identity", async () => {
    const name = `gptqueue-experiment-active-${Date.now()}`;
    const existing = new RedisClient(null, TEST_REDIS_URL);
    await existing.register("both", name, "active identity must not be shared");
    const previousBinary = process.env.GPTQ_EXPERIMENT_CODEX_BIN;
    const stderr = vi.spyOn(console, "error").mockImplementation(() => {});

    try {
      await expect(
        runExperimentalWrapper([
          "codex",
          "--agent",
          name,
          "--workspace",
          join(REPOSITORY_ROOT, ".gptqueue"),
          "--redis-url",
          TEST_REDIS_URL,
          "--",
          "must never launch",
        ])
      ).rejects.toThrow(/concurrent wrapper ownership of active agent/u);
      expect(await cleanupRedis.hexists(SESSION_KEYS.registry, name)).toBe(1);

      await existing.closeCurrentSession();
      process.env.GPTQ_EXPERIMENT_CODEX_BIN = "/usr/bin/true";
      await expect(
        runExperimentalWrapper([
          "codex",
          "--agent",
          name,
          "--workspace",
          join(REPOSITORY_ROOT, ".gptqueue"),
          "--redis-url",
          TEST_REDIS_URL,
          "--",
          "reuse the now-offline identity",
        ])
      ).resolves.toBe(0);
      expect(await cleanupRedis.hexists(SESSION_KEYS.registry, name)).toBe(1);
      expect(
        await cleanupRedis.scard(SESSION_KEYS.agentSessions(name))
      ).toBe(0);
    } finally {
      stderr.mockRestore();
      if (existing.registered) await existing.unregister();
      await existing.shutdown();
      if (previousBinary === undefined) {
        delete process.env.GPTQ_EXPERIMENT_CODEX_BIN;
      } else {
        process.env.GPTQ_EXPERIMENT_CODEX_BIN = previousBinary;
      }
    }
  });

  it("atomically excludes a second destructive wrapper claim", async () => {
    const name = `gptqueue-experiment-claim-${Date.now()}`;
    const first = await acquireWrapperIdentityClaim(TEST_REDIS_URL, name, true);

    try {
      await expect(
        acquireWrapperIdentityClaim(TEST_REDIS_URL, name, true)
      ).rejects.toThrow(/already owns/u);
    } finally {
      await first.release();
    }

    const next = await acquireWrapperIdentityClaim(TEST_REDIS_URL, name, true);
    await next.release();
  });

  it("detects a non-wrapper session collision after claiming a name", async () => {
    const name = `gptqueue-experiment-session-race-${Date.now()}`;
    const claim = await acquireWrapperIdentityClaim(TEST_REDIS_URL, name, false);
    const firstSession = new RedisClient(null, TEST_REDIS_URL);
    const competingSession = new RedisClient(null, TEST_REDIS_URL);

    try {
      const first = await firstSession.register("both", name, "first session");
      await expect(claim.assertExclusiveSession(first.session_id)).resolves.toBe(
        undefined
      );
      const competing = await competingSession.register(
        "both",
        name,
        "competing session"
      );
      await expect(
        claim.assertExclusiveSession(first.session_id)
      ).rejects.toThrow(/lost exclusive session ownership/u);
      await expect(
        claim.unregisterExclusiveSession(first.session_id)
      ).resolves.toBe("session_closed_only");
      expect(
        await cleanupRedis.exists(SESSION_KEYS.session(first.session_id))
      ).toBe(0);
      expect(
        await cleanupRedis.exists(SESSION_KEYS.session(competing.session_id))
      ).toBe(1);
      expect(await cleanupRedis.hexists(SESSION_KEYS.registry, name)).toBe(1);
    } finally {
      firstSession.forceDisconnect();
      if (competingSession.registered) await competingSession.unregister();
      await firstSession.shutdown();
      await competingSession.shutdown();
      await claim.release();
    }
  });
});
