import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { Redis } from "ioredis";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

/** Flush all gptq:* keys used by tests. */
async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

describe("RedisClient", () => {
  let cleanup: Redis;

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup);
  });

  afterEach(async () => {
    await flushTestKeys(cleanup);
    await cleanup.quit();
  });

  it("registers an agent and verifies registered state", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    expect(client.registered).toBe(false);

    const result = await client.register("both", "test-agent-1", "a test agent");
    expect(result.name).toBe("test-agent-1");
    expect(result.session_id).toBeTruthy();
    expect(client.registered).toBe(true);
    expect(client.agentName).toBe("test-agent-1");

    await client.shutdown();
  });

  it("sends and receives a message round-trip", async () => {
    const sender = new RedisClient(null, TEST_REDIS_URL);
    const receiver = new RedisClient(null, TEST_REDIS_URL);

    await sender.register("publisher", "sender-1", "sends messages");
    await receiver.register("consumer", "receiver-1", "receives messages");

    const sent = await sender.sendMessage({
      id: "msg-001",
      from: "sender-1",
      to: "receiver-1",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "hello from sender" },
    });
    expect(sent).toBe(true);

    const received = await receiver.receiveMessage(2);
    expect(received).not.toBeNull();
    expect(received!.id).toBe("msg-001");
    expect(received!.payload.content).toBe("hello from sender");

    await sender.shutdown();
    await receiver.shutdown();
  });

  it("lists registered agents with online status", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    await client.register("both", "list-test-agent", "for listing");

    const agents = await client.listAgents();
    const found = agents.find((a) => a.name === "list-test-agent");
    expect(found).toBeDefined();
    expect(found!.online).toBe(true);
    expect(found!.role).toBe("both");

    await client.shutdown();
  });

  it("unregisters and cleans up all keys", async () => {
    const client = new RedisClient(null, TEST_REDIS_URL);
    await client.register("both", "unreg-agent", "will be removed");

    await client.unregister();
    expect(client.registered).toBe(false);

    const agents = await client.listAgents();
    const found = agents.find((a) => a.name === "unreg-agent");
    expect(found).toBeUndefined();

    await client.shutdown();
  });

  it("reports queue depth correctly", async () => {
    const sender = new RedisClient(null, TEST_REDIS_URL);
    const receiver = new RedisClient(null, TEST_REDIS_URL);

    await sender.register("publisher", "depth-sender", "sends");
    await receiver.register("consumer", "depth-receiver", "receives");

    await sender.sendMessage({
      id: "d-1",
      from: "depth-sender",
      to: "depth-receiver",
      timestamp: new Date().toISOString(),
      type: "ping",
      payload: { content: "ping" },
    });
    await sender.sendMessage({
      id: "d-2",
      from: "depth-sender",
      to: "depth-receiver",
      timestamp: new Date().toISOString(),
      type: "ping",
      payload: { content: "ping2" },
    });

    const depth = await receiver.getQueueDepth();
    expect(depth).toBe(2);

    await sender.shutdown();
    await receiver.shutdown();
  });
});
