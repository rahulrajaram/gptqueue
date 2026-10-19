/**
 * The generic claim/ack/send tools write inbox trace stages (fc975d6). A
 * failing trace write must never fail the tool call: the claim, ack and send
 * are already committed.
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerTools } from "../src/transports/setup-tools.js";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

const URL = testRedisUrl();
const payload = (result: Awaited<ReturnType<Client["callTool"]>>) =>
  JSON.parse((result.content as Array<{ text: string }>)[0]!.text);

describe("generic tool trace writes are best effort", () => {
  let redis: Redis;
  const clients: RedisClient[] = [];
  beforeEach(async () => { redis = new Redis(URL); await flushTestKeys(redis, URL); });
  afterEach(async () => {
    vi.restoreAllMocks();
    for (const c of clients.splice(0)) await c.shutdown().catch(() => undefined);
    await flushTestKeys(redis, URL); await redis.quit();
  });

  const connect = async (name: string) => {
    const client = new RedisClient(null, URL); clients.push(client);
    await client.register("both", name);
    const server = new McpServer({ name: "trace", version: "1" });
    registerTools(server, client);
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    const mcp = new Client({ name: "trace-test", version: "1" });
    await mcp.connect(clientTransport);
    return mcp;
  };

  it("send, claim, reply and acknowledge all succeed while every trace write fails", async () => {
    vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    const sender = `s-${randomUUID()}`; const worker = `w-${randomUUID()}`;
    // Wrong-typed trace streams make every XADD to them fail.
    for (const agent of [sender, worker]) await redis.set(`gptq:inbox-trace:${agent}`, "wrong-type");
    const s = await connect(sender); const w = await connect(worker);

    const sent = await s.callTool({ name: "send_message", arguments: { to: worker, content: "do it", type: "task" } });
    expect(sent.isError).toBeFalsy();
    const taskId = payload(sent).message_id;
    const claimed = await w.callTool({ name: "claim_tasks", arguments: {} });
    expect(claimed.isError).toBeFalsy();
    const claim = payload(claimed).claim;
    expect(claim.claim_id).toBeTruthy();
    const reply = await w.callTool({ name: "send_message", arguments: { to: sender, content: "done", type: "result", in_reply_to: taskId } });
    expect(reply.isError).toBeFalsy();
    const acked = await w.callTool({ name: "acknowledge_tasks", arguments: { claim_id: claim.claim_id } });
    expect(acked.isError).toBeFalsy();

    expect(await redis.zcard(CLAIM_KEYS.index(worker))).toBe(0);
    expect(await redis.llen(SESSION_KEYS.queue(sender))).toBe(1);
    expect(await redis.type(`gptq:inbox-trace:${worker}`)).toBe("string"); // the trace really could not be written
  });
});
