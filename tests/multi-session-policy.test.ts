import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { Redis } from "ioredis";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

/**
 * Multi-session policy (NXT-020):
 * - Any session for an agent can send messages on that agent's behalf
 * - All sessions share a single mailbox queue (last-writer-wins for send)
 * - Any session can consume from the shared mailbox
 * - Agent is online if ANY session has a live lease
 */
describe("Multi-session policy (NXT-020)", () => {
  let cleanup: Redis;

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup);
  });

  afterEach(async () => {
    await flushTestKeys(cleanup);
    await cleanup.quit();
  });

  it("two sessions for the same agent can both send messages", async () => {
    const sessionA = new RedisClient(null, TEST_REDIS_URL);
    const sessionB = new RedisClient(null, TEST_REDIS_URL);
    const receiver = new RedisClient(null, TEST_REDIS_URL);

    await sessionA.register("both", "multi-sender", "session A");
    await sessionB.register("both", "multi-sender", "session B");
    await receiver.register("consumer", "multi-receiver", "receives");

    // Both sessions can send on behalf of multi-sender
    await sessionA.sendMessage({
      id: "ms-1",
      from: "multi-sender",
      to: "multi-receiver",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "from session A" },
    });

    await sessionB.sendMessage({
      id: "ms-2",
      from: "multi-sender",
      to: "multi-receiver",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "from session B" },
    });

    const depth = await receiver.getQueueDepth();
    expect(depth).toBe(2);

    // Receiver gets both messages
    const msg1 = await receiver.receiveMessage(2);
    const msg2 = await receiver.receiveMessage(2);
    expect(msg1).not.toBeNull();
    expect(msg2).not.toBeNull();

    const contents = [msg1!.payload.content, msg2!.payload.content].sort();
    expect(contents).toEqual(["from session A", "from session B"]);

    await sessionA.shutdown();
    await sessionB.shutdown();
    await receiver.shutdown();
  });

  it("closing one session keeps agent online via the other session", async () => {
    const sessionA = new RedisClient(null, TEST_REDIS_URL);
    const sessionB = new RedisClient(null, TEST_REDIS_URL);

    await sessionA.register("both", "dual-session", "session A");
    await sessionB.register("both", "dual-session", "session B");

    // Both sessions active -- agent is online
    let agents = await sessionA.listAgents();
    let found = agents.find((a) => a.name === "dual-session");
    expect(found?.online).toBe(true);

    // Close session A
    await sessionA.closeCurrentSession();

    // Agent should still be online via session B
    agents = await sessionB.listAgents();
    found = agents.find((a) => a.name === "dual-session");
    expect(found?.online).toBe(true);

    await sessionA.shutdown();
    await sessionB.shutdown();
  });
});
