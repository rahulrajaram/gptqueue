import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { Redis } from "ioredis";

const TEST_REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379";

async function flushTestKeys(redis: Redis): Promise<void> {
  const keys = await redis.keys("gptq:*");
  if (keys.length > 0) await redis.del(...keys);
}

describe("Cross-process registration bug (NXT-002)", () => {
  let cleanup: Redis;

  beforeEach(async () => {
    cleanup = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(cleanup);
  });

  afterEach(async () => {
    await flushTestKeys(cleanup);
    await cleanup.quit();
  });

  it("process B cannot operate on agent registered by process A", async () => {
    // Process A registers the agent
    const processA = new RedisClient(null, TEST_REDIS_URL);
    await processA.register("both", "shared-agent", "registered by A");

    // Verify registration is durable in Redis
    const agents = await processA.listAgents();
    expect(agents.find((a) => a.name === "shared-agent")).toBeDefined();

    // Process B is a fresh RedisClient (simulates a respawned worker)
    const processB = new RedisClient(null, TEST_REDIS_URL);

    // Process B can see the agent in list_agents (Redis-backed)
    const agentsFromB = await processB.listAgents();
    expect(agentsFromB.find((a) => a.name === "shared-agent")).toBeDefined();

    // BUG: Process B cannot send or receive because _agentName is null
    expect(processB.registered).toBe(false);
    expect(() => processB.requireRegistered()).toThrow(
      "Agent not registered"
    );

    // Process B cannot receive messages meant for shared-agent
    // even though the agent is demonstrably registered in Redis
    await processA.sendMessage({
      id: "cross-1",
      from: "shared-agent",
      to: "shared-agent",
      timestamp: new Date().toISOString(),
      type: "ping",
      payload: { content: "self-ping" },
    });

    // Process B should be able to receive this -- but it cannot
    // because requireRegistered() checks in-memory _agentName, not Redis
    expect(() => processB.requireRegistered()).toThrow();

    await processA.shutdown();
    await processB.shutdown();
  });

  it("re-registration in process B works but is a workaround, not a fix", async () => {
    const processA = new RedisClient(null, TEST_REDIS_URL);
    await processA.register("both", "workaround-agent", "registered by A");

    // Process B must re-register to work -- this is the workaround
    const processB = new RedisClient(null, TEST_REDIS_URL);
    await processB.register("both", "workaround-agent", "re-registered by B");

    // Send message after B is registered so BLPOP subscriber is ready
    await processA.sendMessage({
      id: "wa-1",
      from: "workaround-agent",
      to: "workaround-agent",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "message from A" },
    });

    // Verify queue has data
    const depth = await processB.getQueueDepth();
    expect(depth).toBe(1);

    // Now B can receive -- but this required a full re-registration
    // which overwrites metadata and restarts heartbeat
    const msg = await processB.receiveMessage(2);
    expect(msg).not.toBeNull();
    expect(msg!.payload.content).toBe("message from A");

    await processA.shutdown();
    await processB.shutdown();
  });

  it("reconnectSession lets process B resume a session without re-registering", async () => {
    // Process A registers and gets a session_id
    const processA = new RedisClient(null, TEST_REDIS_URL);
    const result = await processA.register("both", "reconnect-agent", "will be reconnected");
    const sessionId = result.session_id;

    // Send a message to self
    await processA.sendMessage({
      id: "rc-1",
      from: "reconnect-agent",
      to: "reconnect-agent",
      timestamp: new Date().toISOString(),
      type: "task",
      payload: { content: "message before reconnect" },
    });

    // Process B is a fresh instance -- simulates a new worker
    const processB = new RedisClient(null, TEST_REDIS_URL);
    expect(processB.registered).toBe(false);

    // Process B reconnects using the session_id from process A
    const agentName = await processB.reconnectSession(sessionId);
    expect(agentName).toBe("reconnect-agent");
    expect(processB.registered).toBe(true);
    expect(processB.agentName).toBe("reconnect-agent");

    // Process B can now receive the queued message
    const msg = await processB.receiveMessage(2);
    expect(msg).not.toBeNull();
    expect(msg!.payload.content).toBe("message before reconnect");

    await processA.shutdown();
    await processB.shutdown();
  });
});
