/**
 * DL-2 across runtime-mailbox adoption: a restarted sidecar registers a
 * per-startup provisional name before it adopts the session's bound mailbox.
 * Peers can discover and send to that name, so neither a refused adoption
 * (the sidecar then retires and retries under a new provisional name) nor a
 * successful one may strand a message accepted for it.
 */
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { restoreRuntimeMailbox } from "../src/registered-shell/runtime-mailbox.js";
import { createRuntimeController } from "../src/registered-shell/runtime-controller.js";
import type { RuntimeAdapter } from "../src/registered-shell/runtime.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { InboxEvents } from "../src/core/inbox-events.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";
import { retireProvisional } from "../src/core/provisional-sweeper.js";
import type { RuntimeBinding } from "../src/registered-shell/runtime.js";

const URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
const binding: RuntimeBinding = { client: "pi", runtime_id: "pi-session-1", epoch: "e1", working_directory: "/workspace/project" };
const task = (to: string, id: string) => ({ id, from: "peer", to, timestamp: new Date().toISOString(), type: "task" as const, payload: { content: id } });
const ids = async (redis: Redis, agent: string) =>
  (await redis.lrange(SESSION_KEYS.queue(agent), 0, -1)).map((raw) => JSON.parse(raw).id as string);

describe("provisional sidecar mailboxes (DL-2)", () => {
  let redis: Redis;
  const clients: RedisClient[] = [];
  const client = () => { const c = new RedisClient(null, URL); clients.push(c); return c; };
  beforeEach(async () => { redis = new Redis(URL); await flushTestKeys(redis, URL); });
  afterEach(async () => {
    for (const c of clients.splice(0)) await c.shutdown().catch(() => undefined);
    await flushTestKeys(redis, URL); await redis.quit();
  });

  it("a send to a provisional name during a refused adoption reaches the bound mailbox exactly once", async () => {
    const first = client();
    await first.register("both", `prov-${randomUUID()}`);
    const bound = await restoreRuntimeMailbox(first, binding);
    // The bound mailbox is still occupied (its old dispatcher holds the binding lease).
    await redis.set(SESSION_KEYS.runtimeBinding(bound), "old-owner", "EX", 30);
    await first.closeCurrentSession();

    const retrying = client();
    const provisional = `prov-${randomUUID()}`;
    await retrying.register("both", provisional);
    expect(await retrying.sessions.resolveRegistered(provisional)).toBe(true); // discoverable, so a peer may send
    await retrying.sendMessage(task(provisional, "m-early"));
    const adapter = async (identity: RuntimeBinding): Promise<RuntimeAdapter> =>
      ({ binding: identity, activate: async () => ({ status: "queued" }), close: async () => undefined });
    const controller = createRuntimeController(retrying, { client: "pi", working_directory: binding.working_directory }, adapter);
    await expect(controller.bind(binding)).rejects.toThrow(/adoption refused \(-5\)/);
    expect(await retrying.sessions.resolveRegistered(provisional)).toBe(true); // a refused bind alone retires nothing
    // The sidecar shuts down (shell cleanup closes the controller, then the
    // session) and Pi retries under a new provisional name.
    await controller.close();
    await retrying.closeCurrentSession();

    // The old owner goes away; the next attempt adopts.
    await redis.del(SESSION_KEYS.runtimeBinding(bound));
    const next = client();
    const provisional2 = `prov-${randomUUID()}`;
    await next.register("both", provisional2);
    await next.sendMessage(task(provisional2, "m-late"));
    expect(await restoreRuntimeMailbox(next, binding)).toBe(bound);

    expect((await ids(redis, bound)).sort()).toEqual(["m-early", "m-late"]);
    // Re-addressed, so the bound identity's dispatcher sees them as its own pending work.
    const forwarded = (await redis.lrange(SESSION_KEYS.queue(bound), 0, -1)).map((raw) => JSON.parse(raw));
    expect(forwarded.map((m) => m.to)).toEqual([bound, bound]);
    expect(forwarded.find((m) => m.id === "m-early").payload).toEqual({ content: "m-early" });
    expect((await new InboxEvents(redis).pending(bound)).map((m) => m.id).sort()).toEqual(["m-early", "m-late"]);
    expect(await ids(redis, provisional)).toEqual([]);
    expect(await ids(redis, provisional2)).toEqual([]);
    // Neither provisional name stays discoverable, so nothing new can be accepted for it.
    expect(await next.sessions.resolveRegistered(provisional)).toBe(false);
    expect(await next.sessions.resolveRegistered(provisional2)).toBe(false);
  });

  const reply = (from: string, inReplyTo: string) =>
    ({ id: `r-${randomUUID()}`, from, to: "peer", timestamp: new Date().toISOString(), type: "result" as const, payload: { content: "done", in_reply_to: inReplyTo } });
  const replyWakesPeer = async (replier: RedisClient, from: string, taskId: string) => {
    const r = reply(from, taskId);
    expect(await replier.sendMessage(r)).toBe(true);
    const events = (await redis.xrange(SESSION_KEYS.inboxEvents("peer"), "-", "+")).map(([, f]) => f);
    expect(events.some((f) => f.includes(r.id))).toBe(true);
    expect((await new InboxEvents(redis).pending("peer")).map((m) => m.id)).toContain(r.id);
  };

  it("COR-F1: adoption re-points the sender's outstanding owner, so the bound mailbox's reply wakes it", async () => {
    const first = client(); await first.register("both", `prov-${randomUUID()}`);
    const bound = await restoreRuntimeMailbox(first, binding);
    await first.closeCurrentSession();
    const next = client(); const p = `prov-${randomUUID()}`; await next.register("both", p);
    await next.sendMessage(task(p, "m-adopt"));
    expect(await redis.get(SESSION_KEYS.outstanding("peer", "m-adopt"))).toBe(p);
    expect(await restoreRuntimeMailbox(next, binding)).toBe(bound);
    expect(await redis.get(SESSION_KEYS.outstanding("peer", "m-adopt"))).toBe(bound);
    await replyWakesPeer(next, bound, "m-adopt");
  });

  it("COR-F1: a refused adoption's retirement re-points the outstanding owner too", async () => {
    const c = client(); const p = `prov-${randomUUID()}`; await c.register("both", p);
    const target = `bound-${randomUUID()}`;
    await c.sendMessage(task(p, "m-retire"));
    expect(await c.retireProvisionalMailbox(p, target)).toBe(true);
    expect(await redis.get(SESSION_KEYS.outstanding("peer", "m-retire"))).toBe(target);
    await replyWakesPeer(c, target, "m-retire");
  });

  it("COR-F2: resolve, then retire into the bound mailbox, then push: the send is forwarded, not stranded", async () => {
    const c = client(); const p = `prov-${randomUUID()}`; await c.register("both", p);
    const target = `bound-${randomUUID()}`;
    const peer = client(); await peer.register("both", "peer");
    expect(await peer.sessions.resolveRegistered(p)).toBe(true);
    expect(await c.retireProvisionalMailbox(p, target)).toBe(true);
    expect(await peer.sendMessage(task(p, "m-race"))).toBe(true);
    expect((await peer.sendMessageIdempotent(task(p, "m-race-2"), "idem-1")).status).toBe("sent");
    expect(await ids(redis, p)).toEqual([]);
    const moved = (await redis.lrange(SESSION_KEYS.queue(target), 0, -1)).map((raw) => JSON.parse(raw));
    expect(moved.map((m) => [m.id, m.to])).toEqual([["m-race", target], ["m-race-2", target]]);
    expect(await redis.get(SESSION_KEYS.outstanding("peer", "m-race"))).toBe(target);
  });

  it("COR-F5: first start clears the provisional mark with the mapping, so a sweep refuses the bound name", async () => {
    const c = client(); const p = `prov-${randomUUID()}`;
    const { session_id } = await c.register("both", p, "sidecar", undefined, { provisional: true });
    expect(await redis.sismember(SESSION_KEYS.provisional, p)).toBe(1);
    expect(await restoreRuntimeMailbox(c, binding)).toBe(p);
    expect(await redis.sismember(SESSION_KEYS.provisional, p)).toBe(0);
    c.forceDisconnect();
    await redis.del(SESSION_KEYS.lease(session_id), SESSION_KEYS.heartbeat(p));
    expect((await retireProvisional(redis, p)).outcome).toBe("unmarked");
    expect(await redis.hexists(SESSION_KEYS.registry, p)).toBe(1);
  });
});
