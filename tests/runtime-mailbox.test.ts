import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { describe, expect, it, afterEach } from "vitest";
import { randomUUID } from "node:crypto";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { restoreRuntimeMailbox } from "../src/registered-shell/runtime-mailbox.js";
import type { RuntimeBinding } from "../src/registered-shell/runtime.js";
import { Redis } from "ioredis";

const redisUrl = process.env.REDIS_URL ?? "redis://127.0.0.1:6379/15";
const clients: RedisClient[] = []; const redisConnections: Redis[] = [];
afterEach(async () => { for (const c of clients.splice(0)) c.forceDisconnect(); for (const r of redisConnections.splice(0)) { await flushTestKeys(r, redisUrl); await r.quit(); } });

describe("runtime mailbox restoration", () => {
  it("preserves the mailbox and public agent across native reconnect", async () => {
    const first = new RedisClient(null, redisUrl); clients.push(first); const agent = `runtime-mailbox-${randomUUID()}`; const binding: RuntimeBinding = { client: "codex", runtime_id: randomUUID(), epoch: "one", working_directory: "/workspace" };
    await first.register("both", agent, "stable runtime"); await restoreRuntimeMailbox(first, binding); await first.sendMessage({ id: randomUUID(), from: "peer", to: agent, timestamp: new Date().toISOString(), type: "task", payload: { content: "backlog" } }); await first.closeCurrentSession();
    const second = new RedisClient(null, redisUrl); clients.push(second); await second.register("both", `provisional-${randomUUID()}`); const restored = await restoreRuntimeMailbox(second, binding); expect(restored).toBe(agent); expect(second.agentName).toBe(agent); expect(await second.receiveMessage(1)).toMatchObject({ to: agent, payload: { content: "backlog" } });
  });
  it("rejects a mapping reused from a different working directory", async () => {
    const first = new RedisClient(null, redisUrl); clients.push(first); const agent = `runtime-mailbox-${randomUUID()}`; const id = randomUUID(); await first.register("both", agent); const good: RuntimeBinding = { client: "codex", runtime_id: id, epoch: "one", working_directory: "/workspace" }; await restoreRuntimeMailbox(first, good); const bad: RuntimeBinding = { ...good, epoch: "two", working_directory: "/other" }; const second = new RedisClient(null, redisUrl); clients.push(second); await second.register("both", `provisional-${randomUUID()}`); await expect(restoreRuntimeMailbox(second, bad)).rejects.toThrow(/mapping does not match/);
  });
});
