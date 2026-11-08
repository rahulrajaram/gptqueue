import { testRedisUrl } from "./helpers/test-redis.js";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { runtimeMailboxKey } from "../src/core/mailbox-continuity.js";
import { restoreRuntimeMailbox } from "../src/registered-shell/runtime-mailbox.js";
import type { RuntimeBinding } from "../src/registered-shell/runtime.js";
import { startBoundBridge } from "../src/experimental-wrapper/bridge.js";
import { flushTestKeys, assertPrivateTestRedis } from "./helpers/redis-test-utils.js";

const TEST_REDIS_URL = testRedisUrl();
assertPrivateTestRedis(TEST_REDIS_URL, "outbound fence tests");
let cleanup: Redis;

beforeEach(async () => {
  cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
  await flushTestKeys(cleanup, TEST_REDIS_URL);
});

afterEach(async () => {
  await flushTestKeys(cleanup, TEST_REDIS_URL);
  await cleanup.quit();
});

const toolPayload = (result: Awaited<ReturnType<Client["callTool"]>>) => {
  return JSON.parse((result.content as Array<{ text: string }>)[0]?.text ?? "null") as { status?: string };
};
const expectSendFailure = (result: Awaited<ReturnType<Client["callTool"]>>) => {
  const payload = toolPayload(result);
  expect(result.isError, JSON.stringify(result)).toBe(true);
  expect(payload.status).toBe("error");
  return payload;
};

const bridgeFor = async () => {
  const redisClient = new RedisClient(null, TEST_REDIS_URL);
  const name = `fence-${randomUUID()}`;
  await redisClient.register("both", name, "outbound fence test", undefined, { provisional: true });
  const bridge = await startBoundBridge({ agentName: name, redisClient });
  const transport = new StreamableHTTPClientTransport(new URL(bridge.url), {
    requestInit: { headers: { authorization: `Bearer ${bridge.bearerToken}` } },
  });
  const client = new Client({ name: "outbound-fence-test", version: "1.0.0" });
  await client.connect(transport);
  const call = (payload: Record<string, unknown>) => client.callTool({ name: "send_message", arguments: payload });
  const fence = async () => cleanup.get(SESSION_KEYS.outboundActivity(name));
  const trace = async () => cleanup.xrange(SESSION_KEYS.inboxTrace(name), "-", "+");
  return { call, fence, trace, close: async () => { await client.close(); await bridge.close(); }, name, redisClient };
};

describe("outbound fence scoping in the bound bridge", () => {
  it("sets the fence for a send whose default type normalizes to task", async () => {
    const h = await bridgeFor();
    try {
      expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "work" }));
      expect(await h.fence()).toBe("1");
    } finally { await h.close(); }
  });

  it("sets the fence for an explicit task type too", async () => {
    const h = await bridgeFor();
    try {
      expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "work", type: "task" }));
      expect(await h.fence()).toBe("1");
    } finally { await h.close(); }
  });

  it.each(["result", "error", "status", "ping"] as const)(
    "does not set the fence for a %s send and audits the exemption without waking",
    async (type) => {
      const h = await bridgeFor();
      try {
        expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "reply", type }));
        expect(await h.fence()).toBeNull();
        const entries = await h.trace();
        const skipped = entries.filter(([, body]) => body.includes("outbound_fence_skipped"));
        expect(skipped).toHaveLength(1);
        expect(skipped[0]![1]).toContain(`message_type=${type}`);
        expect(skipped[0]![1]).not.toContain("reply");
      } finally { await h.close(); }
    }
  );

  it("never clears or shortens an existing task fence", async () => {
    const h = await bridgeFor();
    try {
      const fenced = SESSION_KEYS.outboundActivity(h.name);
      expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "work" }));
      expect(await cleanup.get(fenced)).toBe("1");
      const before = await cleanup.ttl(fenced);
      expect(before).toBeGreaterThan(0);
      expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "reply", type: "result" }));
      expect(await cleanup.get(fenced)).toBe("1");
      const after = await cleanup.ttl(fenced);
      expect(after).toBeLessThanOrEqual(before + 1);
      expect(after).toBeGreaterThan(0);
    } finally { await h.close(); }
  });

  it("rejects malformed input before any fence write", async () => {
    const h = await bridgeFor();
    try {
      const response = await h.call({ to: "peer-no-such-recipient", content: "x", type: "newsletter" });
      expect(response.isError).toBe(true);
      expect(await h.fence()).toBeNull();
    } finally { await h.close(); }
  });

  it("conservatively fences a failed task attempt (unknown recipient)", async () => {
    const h = await bridgeFor();
    try {
      expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "work" }));
      expect(await h.fence()).toBe("1");
    } finally { await h.close(); }
  });

  it.each(["result", "status"] as const)("allows guarded restart adoption after a %s send", async (type) => {
    const h = await bridgeFor();
    const previous = new RedisClient(null, TEST_REDIS_URL);
    try {
      const binding: RuntimeBinding = { client: "codex", runtime_id: randomUUID(), epoch: "one", working_directory: "/workspace" };
      const canonical = `canonical-${randomUUID()}`;
      await previous.register("both", canonical, "previous runtime");
      expect(await restoreRuntimeMailbox(previous, binding)).toBe(canonical);
      await previous.closeCurrentSession();
      expect(await cleanup.get(SESSION_KEYS.heartbeat(canonical))).toBeNull();
      expect(await cleanup.hget(SESSION_KEYS.registry, canonical)).toBeTruthy();
      expect(JSON.parse((await cleanup.get(runtimeMailboxKey(binding)))!).agent).toBe(canonical);
      const retained = { id: randomUUID(), from: "peer", to: canonical, payload: { content: "kept" }, type: "task" };
      await cleanup.rpush(SESSION_KEYS.queue(canonical), JSON.stringify(retained));

      expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "reply", type }));
      expect(await h.fence()).toBeNull();
      expect(await restoreRuntimeMailbox(h.redisClient, binding)).toBe(canonical);
      expect(h.redisClient.agentName).toBe(canonical);
      expect(await cleanup.hget(SESSION_KEYS.session(h.redisClient.sessionId!), "agent_name")).toBe(canonical);
      expect(JSON.parse((await cleanup.lindex(SESSION_KEYS.queue(canonical), 0))!)).toEqual(retained);
      expect(await cleanup.hget(SESSION_KEYS.registry, h.name)).toBeNull();
    } finally { previous.forceDisconnect(); await h.close(); }
  });

  it("keeps task-fenced provisional work from adopting after restart", async () => {
    const h = await bridgeFor();
    try {
      const binding: RuntimeBinding = { client: "codex", runtime_id: randomUUID(), epoch: "one", working_directory: "/workspace" };
      const canonical = `canonical-${randomUUID()}`;
      await cleanup.hset(SESSION_KEYS.registry, canonical, JSON.stringify({ name: canonical }));
      await cleanup.set(runtimeMailboxKey(binding), JSON.stringify({ agent: canonical, working_directory: binding.working_directory }));
      expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "work", type: "task" }));
      await expect(restoreRuntimeMailbox(h.redisClient, binding)).rejects.toThrow(/outbound_activity/u);
      expect(h.redisClient.agentName).toBe(h.name);
      expect(await cleanup.hget(SESSION_KEYS.registry, h.name)).toBeTruthy();
    } finally { await h.close(); }
  });

  it("does not adopt over a live foreign owner of the mapped mailbox", async () => {
    const h = await bridgeFor();
    const owner = new RedisClient(null, TEST_REDIS_URL);
    try {
      const binding: RuntimeBinding = { client: "codex", runtime_id: randomUUID(), epoch: "one", working_directory: "/workspace" };
      const canonical = `canonical-${randomUUID()}`;
      await owner.register("both", canonical, "live owner");
      await cleanup.set(runtimeMailboxKey(binding), JSON.stringify({ agent: canonical, working_directory: binding.working_directory }));
      expectSendFailure(await h.call({ to: "peer-no-such-recipient", content: "reply", type: "status" }));
      await expect(restoreRuntimeMailbox(h.redisClient, binding)).rejects.toThrow(/target_occupied/u);
      expect(h.redisClient.agentName).toBe(h.name);
      expect(await cleanup.hget(SESSION_KEYS.registry, canonical)).toBeTruthy();
    } finally { owner.forceDisconnect(); await h.close(); }
  });
});
