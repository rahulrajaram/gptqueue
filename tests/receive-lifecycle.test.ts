import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerTools } from "../src/transports/setup-tools.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const TEST_REDIS_URL = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";

/**
 * Connections on this test's database currently parked in a blocking pop.
 * The server is shared with other databases, so a client blocked anywhere
 * else is not counted (FIX-F1): only db=<test db>, the blocked flag and
 * cmd=blpop together identify this test's receives.
 */
const parkedPops = async (redis: Redis): Promise<number> => {
  const db = String(redis.options.db ?? 0);
  return String(await redis.client("LIST"))
    .split("\n")
    .map((line) => new Map(line.split(" ").map((field) => {
      const at = field.indexOf("=");
      return [field.slice(0, at), field.slice(at + 1)] as const;
    })))
    .filter((fields) => fields.get("db") === db && (fields.get("flags") ?? "").includes("b") && fields.get("cmd") === "blpop")
    .length;
};

const payload = (result: Awaited<ReturnType<Client["callTool"]>>) =>
  JSON.parse((result.content as Array<{ text: string }>)[0]!.text);

/**
 * Every receive owns a duplicate blocking connection. Its lifetime must be
 * bounded by the request that started it and by the owning client: once
 * either ends, nothing may stay parked in BLPOP and later consume mail
 * whose response can no longer be delivered (review finding RF4).
 */
describe("receive connection lifecycle", () => {
  let redis: Redis;
  const clients: RedisClient[] = [];

  beforeEach(async () => {
    redis = new Redis(TEST_REDIS_URL, { maxRetriesPerRequest: 3 });
    await flushTestKeys(redis, TEST_REDIS_URL);
  });

  afterEach(async () => {
    await Promise.all(clients.splice(0).map((client) => client.shutdown()));
    await flushTestKeys(redis, TEST_REDIS_URL);
    await redis.quit();
  });

  it("client shutdown releases an unsignalled timeout-0 receive and leaves later mail queued", async () => {
    const name = `rf4-shutdown-${randomUUID()}`;
    const client = new RedisClient(null, TEST_REDIS_URL);
    clients.push(client);
    await client.register("both", name);

    // No caller signal: the receive is owned only by the client.
    const pending = client.receiveMessage(0).then(
      () => "settled",
      () => "settled"
    );
    await expect.poll(() => parkedPops(redis)).toBe(1);

    await client.shutdown();
    const outcome = await Promise.race([pending, delay(2_000).then(() => "still blocked")]);

    // Mail that arrives after shutdown must stay queued for the next session.
    await redis.rpush(SESSION_KEYS.queue(name), JSON.stringify({ id: "after-shutdown" }));
    await delay(300);
    const queued = await redis.llen(SESSION_KEYS.queue(name));
    const parked = await parkedPops(redis);

    expect({ outcome, queued, parked }).toEqual({ outcome: "settled", queued: 1, parked: 0 });
  });

  it("main receive_message registration stops its blocking pop when the MCP request is cancelled", async () => {
    const name = `rf4-cancel-${randomUUID()}`;
    const client = new RedisClient(null, TEST_REDIS_URL);
    clients.push(client);
    await client.register("both", name);

    const server = new McpServer({ name: "rf4", version: "1" });
    registerTools(server, client);
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    const mcp = new Client({ name: "rf4-test", version: "1" });
    await mcp.connect(clientTransport);

    try {
      const abort = new AbortController();
      const cancelled = mcp
        .callTool({ name: "receive_message", arguments: { timeout: 0 } }, undefined, {
          signal: abort.signal,
        })
        .catch((error: Error) => error);
      await expect.poll(() => parkedPops(redis)).toBe(1);

      abort.abort();
      expect(await cancelled).toBeInstanceOf(Error);
      // Give the cancellation notification time to reach the handler.
      await delay(300);

      const sent = payload(
        await mcp.callTool({ name: "send_message", arguments: { to: name, content: "after-cancel" } })
      );
      await delay(300);
      const queued = await redis.llen(SESSION_KEYS.queue(name));
      const parked = await parkedPops(redis);
      expect({ queued, parked }).toEqual({ queued: 1, parked: 0 });

      // The next receive gets the message the cancelled one must not consume.
      const received = payload(
        await mcp.callTool({ name: "receive_message", arguments: { timeout: 1 } })
      );
      expect(received.id).toBe(sent.message_id);
    } finally {
      await mcp.close();
    }
  });
});
