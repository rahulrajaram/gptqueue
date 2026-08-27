import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { Redis } from "ioredis";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

describe("Tool contract (NXT-015)", () => {
  let cleanup: Redis;

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup);
  });

  afterEach(async () => {
    await flushTestKeys(cleanup);
    await cleanup.quit();
  });

  it("register -> send -> crash -> reconnect -> receive (session survives crash)", async () => {
    // Agent A sends a message to agent B
    const clientA = new RedisClient(null, TEST_REDIS_URL);
    const clientB1 = new RedisClient(null, TEST_REDIS_URL);

    await clientA.register("publisher", "tool-sender", "sends stuff");
    const regB = await clientB1.register("consumer", "tool-receiver", "receives stuff");

    await clientA.sendMessage({
      id: "tc-1",
      from: "tool-sender",
      to: "tool-receiver",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "important work" },
    });

    // B "crashes" -- shutdown without close_session (session remains in Redis)
    await clientB1.shutdown();

    // New process for B reconnects with original session_id
    const clientB2 = new RedisClient(null, TEST_REDIS_URL);
    const resumed = await clientB2.reconnectSession(regB.session_id);
    expect(resumed).toBe("tool-receiver");

    // B2 can receive the queued message
    const msg = await clientB2.receiveMessage(2);
    expect(msg).not.toBeNull();
    expect(msg!.payload.content).toBe("important work");

    await clientA.shutdown();
    await clientB2.shutdown();
  });

  it("close_session preserves mailbox for later re-registration", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    await client.register("both", "close-test", "will close session");

    // Send a message to self
    await client.sendMessage({
      id: "cs-1",
      from: "close-test",
      to: "close-test",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "preserved message" },
    });

    // Close session -- mailbox preserved
    const closedName = await client.closeCurrentSession();
    expect(closedName).toBe("close-test");
    expect(client.registered).toBe(false);

    // Verify mailbox still has the message
    const depth = await cleanup.llen("gptq:q:close-test");
    expect(depth).toBe(1);

    // Re-register as the same agent (new session) and receive the message
    const client2 = new RedisClient(null, TEST_REDIS_URL);
    await client2.register("both", "close-test", "re-registered");

    const msg = await client2.receiveMessage(2);
    expect(msg).not.toBeNull();
    expect(msg!.payload.content).toBe("preserved message");

    await client.shutdown();
    await client2.shutdown();
  });

  it("unregister deletes mailbox while close_session preserves it", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    await client.register("both", "delete-test", "will be deleted");

    // Send a message to self
    await client.sendMessage({
      id: "dt-1",
      from: "delete-test",
      to: "delete-test",
      timestamp: new Date().toISOString(),
      type: "ping",
      payload: { content: "keep me?" },
    });

    // Unregister (destructive) -- mailbox should be deleted
    await client.unregister();

    const depth = await cleanup.llen("gptq:q:delete-test");
    expect(depth).toBe(0);

    // Agent should not appear in list
    const client2 = new RedisClient(null, TEST_REDIS_URL);
    const agents = await client2.listAgents();
    expect(agents.find((a) => a.name === "delete-test")).toBeUndefined();

    await client.shutdown();
    await client2.shutdown();
  });

  it("register returns session_id that can be used to reconnect", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    const result = await client.register("both", "session-test", "test");

    expect(result.session_id).toBeTruthy();
    expect(result.name).toBe("session-test");
    expect(client.sessionId).toBe(result.session_id);

    await client.shutdown();
  });
});
