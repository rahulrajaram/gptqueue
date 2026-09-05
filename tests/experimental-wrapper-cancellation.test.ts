import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { setTimeout as delay } from "node:timers/promises";
import { Redis } from "ioredis";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { startBoundBridge, type BoundBridge } from "../src/experimental-wrapper/bridge.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const TEST_REDIS_URL = "redis://127.0.0.1:6379/15";
const payload = (result: Awaited<ReturnType<Client["callTool"]>>) =>
  JSON.parse((result.content as Array<{ text: string }>)[0]!.text);

describe("bound receive cancellation", () => {
  let redis: Redis;
  let registration: RedisClient;
  let bridge: BoundBridge;
  let clients: Array<{ client: Client; transport: StreamableHTTPClientTransport }>;
  const name = "gptqueue-experiment-cancellation";

  const connect = async () => {
    const transport = new StreamableHTTPClientTransport(new URL(bridge.url), {
      requestInit: { headers: { authorization: `Bearer ${bridge.bearerToken}` } },
    });
    const client = new Client({ name: "cancellation-test", version: "1" });
    clients.push({ client, transport });
    await client.connect(transport);
    return { client, transport };
  };

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
    clients = [];
    registration = new RedisClient(null, TEST_REDIS_URL);
    await registration.register("both", name);
    bridge = await startBoundBridge({ agentName: name, redisClient: registration });
  });

  afterEach(async () => {
    await bridge.close();
    await Promise.all(clients.map(({ client }) => client.close()));
    if (registration.registered) await registration.unregister();
    await registration.shutdown();
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("keeps a message arriving after cancellation for the next receive", async () => {
    const { client } = await connect();
    const abort = new AbortController();
    const cancelled = client.callTool(
      { name: "receive_message", arguments: { timeout: 30 } },
      undefined,
      { signal: abort.signal }
    ).catch((error: Error) => error);
    await delay(150);
    abort.abort();
    expect(await cancelled).toBeInstanceOf(Error);
    // Past the old one-second polling interval: catches abandoned re-polling.
    await delay(2_500);
    const sent = payload(await client.callTool({
      name: "send_message", arguments: { to: name, content: "after-cancel" },
    }));
    const received = payload(await client.callTool({
      name: "receive_message", arguments: { timeout: 1 },
    }));
    expect(received.id).toBe(sent.message_id);
    expect(received.payload.content).toBe("after-cancel");
  });

  it("cancels only its own blocking socket while another receive stays live", async () => {
    const first = await connect();
    const second = await connect();
    const abort = new AbortController();
    const cancelled = first.client.callTool(
      { name: "receive_message", arguments: { timeout: 30 } },
      undefined, { signal: abort.signal }
    ).catch((error: Error) => error);
    await delay(150);
    const waiting = second.client.callTool({
      name: "receive_message", arguments: { timeout: 5 },
    });
    abort.abort();
    expect(await cancelled).toBeInstanceOf(Error);
    await delay(150);
    const sent = payload(await second.client.callTool({
      name: "send_message", arguments: { to: name, content: "other-request" },
    }));
    expect(payload(await waiting).id).toBe(sent.message_id);
    expect(registration.registered).toBe(true);
  });

  it("stops a pending receive when its MCP transport session is deleted", async () => {
    const first = await connect();
    const second = await connect();
    const pending = first.client.callTool({
      name: "receive_message", arguments: { timeout: 30 },
    }).catch((error: Error) => error);
    await delay(150);
    await first.transport.terminateSession();
    await first.client.close();
    await pending;
    await delay(150);
    const sent = payload(await second.client.callTool({
      name: "send_message", arguments: { to: name, content: "after-session-close" },
    }));
    const received = payload(await second.client.callTool({
      name: "receive_message", arguments: { timeout: 1 },
    }));
    expect(received.id).toBe(sent.message_id);
  });

  it("closes the bridge promptly during a long receive", async () => {
    const { client } = await connect();
    const receive = registration.receiveMessage.bind(registration);
    let receiveSettled = false;
    vi.spyOn(registration, "receiveMessage").mockImplementation(async (...args) => {
      try { return await receive(...args); }
      finally { receiveSettled = true; }
    });
    const pending = client.callTool({
      name: "receive_message", arguments: { timeout: 60 },
    }).catch((error: Error) => error);
    await delay(150);
    const start = Date.now();
    await bridge.close();
    expect(Date.now() - start).toBeLessThan(2_500);
    await expect.poll(() => receiveSettled).toBe(true);
    // The SDK may retry SSE after a remote close; end the local client too.
    await client.close();
    await pending;
    expect(await registration.getQueueDepth()).toBe(0);
  });
});
