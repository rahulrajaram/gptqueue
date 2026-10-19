import { testRedisUrl } from "./helpers/test-redis.js";
import { resolve } from "node:path";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { SESSION_KEYS } from "../src/core/keys.js";
import { WORKER_SHELL_TOOL_NAMES } from "../src/registered-shell/worker-shell.js";

/** The child env, with GPTQUEUE_WORKER set (or removed) explicitly per spawn. */
const childEnv = (worker: string | undefined): Record<string, string> => {
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(process.env)) if (value !== undefined) env[key] = value;
  delete env.GPTQUEUE_WORKER;
  if (worker !== undefined) env.GPTQUEUE_WORKER = worker;
  return env;
};

const connect = async (worker: string | undefined, url = testRedisUrl()) => {
  const transport = new StdioClientTransport({ command: process.execPath,
    args: [resolve("bin/gptqueue-session"), "--client", "codex", "--redis-url", url], env: childEnv(worker), stderr: "pipe" });
  let stderr = "";
  transport.stderr?.on("data", (data) => { stderr += data; });
  const client = new Client({ name: "worker-mode-test", version: "1" });
  await client.connect(transport);
  return { client, stderr: () => stderr };
};

const snapshot = async (redis: Redis) => ({
  keys: (await redis.keys("*")).sort(),
  audit: await redis.xlen(SESSION_KEYS.registrationAudit),
});

describe("registered-shell sidecar worker mode (GPTQUEUE_WORKER=1)", () => {
  let redis: Redis;
  beforeAll(() => { redis = new Redis(testRedisUrl()); });
  afterAll(async () => { await redis.quit(); });

  it("starts, lists tools, refuses mailbox tools and writes nothing to Redis", async () => {
    const before = await snapshot(redis);
    const { client, stderr } = await connect("1");
    try {
      const tools = (await client.listTools()).tools.map((tool) => tool.name).sort();
      expect(tools).toEqual([...WORKER_SHELL_TOOL_NAMES].sort());
      // No registration, session, presence, binding or audit event: no key changes at all.
      expect(await redis.hkeys(SESSION_KEYS.registry)).not.toEqual(expect.arrayContaining([expect.stringMatching(/^gptqueue-shell-codex-/u)]));
      expect(await snapshot(redis)).toEqual(before);
      for (const name of ["send_message", "claim_tasks", "get_runtime_status", "list_agents"]) {
        const result = await client.callTool({ name, arguments: name === "send_message" ? { recipient: "x", message: "y" } : {} });
        expect(result.isError).toBe(true);
        expect((result.structuredContent as { error: { code: string; message: string } }).error)
          .toMatchObject({ code: "HEADLESS_WORKER", message: expect.stringContaining("GPTQUEUE_WORKER=1") });
      }
      expect(stderr()).toContain("headless worker mode");
    } finally { await client.close(); }
  });

  it("needs no Redis at all: starts against an unreachable socket", async () => {
    const { client } = await connect("1", testRedisUrl().replace(/redis\.sock/u, "absent.sock"));
    try { expect((await client.listTools()).tools.length).toBe(WORKER_SHELL_TOOL_NAMES.length); }
    finally { await client.close(); }
  });

  it.each([{ worker: "0" }, { worker: undefined }])("GPTQUEUE_WORKER=$worker registers as before", async ({ worker }) => {
    const before = new Set(await redis.hkeys(SESSION_KEYS.registry));
    const auditBefore = await redis.xlen(SESSION_KEYS.registrationAudit);
    const { client, stderr } = await connect(worker);
    let name: string | undefined;
    try {
      const added = (await redis.hkeys(SESSION_KEYS.registry)).filter((key) => !before.has(key));
      expect(added).toHaveLength(1);
      name = added[0]!;
      expect(name).toMatch(/^gptqueue-shell-codex-/u);
      expect(await redis.xlen(SESSION_KEYS.registrationAudit)).toBeGreaterThan(auditBefore);
      expect(stderr()).not.toContain("headless worker mode");
      const status = await client.callTool({ name: "get_runtime_status", arguments: {} });
      expect(status.isError).toBeFalsy();
    } finally {
      await client.close();
      if (name) {
        await redis.hdel(SESSION_KEYS.registry, name);
        await redis.del(SESSION_KEYS.agent(name), SESSION_KEYS.queue(name), SESSION_KEYS.mailboxMeta(name), SESSION_KEYS.heartbeat(name), SESSION_KEYS.agentSessions(name));
      }
    }
  });
});
