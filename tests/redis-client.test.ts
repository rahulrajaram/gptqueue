import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { Redis } from "ioredis";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";

/** Flush all gptq:* keys used by tests. */

describe("RedisClient", () => {
  let cleanup: Redis;

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await flushTestKeys(cleanup, TEST_REDIS_URL);
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

  it("unregister leaves no claims or DLQ for a later agent with the same name", async () => {
    const first = new RedisClient(null, TEST_REDIS_URL);
    const { session_id } = await first.register("both", "reuse-agent", "first owner");
    const raw = new Redis(TEST_REDIS_URL);
    try {
      await raw.rpush("gptq:q:reuse-agent", JSON.stringify({ id: "t-1", from: "x", to: "reuse-agent", type: "task", timestamp: new Date().toISOString(), payload: { content: "do" } }));
      const claimed = await first.taskClaim.claim({ actor_id: "reuse-agent", session_id, max_batch: 1, ttl_seconds: 300, now: new Date().toISOString() });
      expect(claimed.ok && claimed.claim !== null).toBe(true);
      await raw.rpush("gptq:dlq:reuse-agent", JSON.stringify({ id: "dead-1" }));

      await first.unregister();
      await first.shutdown();

      expect(await raw.exists("gptq:claims-index:reuse-agent", "gptq:dlq:reuse-agent")).toBe(0);
      const second = new RedisClient(null, TEST_REDIS_URL);
      const again = await second.register("both", "reuse-agent", "second owner");
      const dlq = await second.taskClaim.deadLetterEntries({ actor_id: "reuse-agent" });
      expect(dlq.ok && dlq.entries).toEqual([]);
      const next = await second.taskClaim.claim({ actor_id: "reuse-agent", session_id: again.session_id, max_batch: 1, ttl_seconds: 300, now: new Date(Date.now() + 3_600_000).toISOString() });
      expect(next.ok && next.claim).toBeNull();
      await second.unregister();
      await second.shutdown();
    } finally {
      await raw.quit();
    }
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
