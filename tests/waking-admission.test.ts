/**
 * Waking admission (Phase A): a task or an owned result/error is admitted
 * while fewer than max_size WAKING entries are queued, whatever quiet mail
 * fills the queue; quiet mail keeps the LLEN < max_size rule. The system under
 * test is the real Lua (waking-admission-lib.lua composed in front of the
 * bounded-push scripts) on Redis db 15.
 */
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Redis } from "ioredis";
import { MailboxStore, OUTSTANDING_PREFIX } from "../src/core/mailbox-store.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { LUA_DIR } from "../src/core/stored-read.js";
import { parseQueueEntry, replyOwners, wakesRecipient } from "../src/core/inbox-events.js";
import type { QueueMessage } from "../src/core/types.js";
import { assertNotLiveDb } from "./helpers/redis-test-utils.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { registerAgent, registerAgentSchema } from "../src/mcp-server/tools/register-agent.js";
import { sendMessage, sendMessageSchema } from "../src/mcp-server/tools/send-message.js";
import { getQueueStatus } from "../src/mcp-server/tools/queue-status.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
assertNotLiveDb(REDIS_URL, "waking-admission");
const BOUND = 10;

describe("waking admission (real Lua)", () => {
  let redis: Redis;
  const tracked = new Set<string>();
  const track = (...keys: string[]) => { for (const key of keys) tracked.add(key); };
  beforeAll(() => { redis = new Redis(REDIS_URL); });
  afterAll(async () => { if (tracked.size > 0) await redis.del(...tracked); redis.disconnect(); });

  const mailbox = () => {
    const to = `wa-${randomUUID().slice(0, 8)}`;
    track(SESSION_KEYS.queue(to), SESSION_KEYS.mailboxMeta(to), SESSION_KEYS.inboxEvents(to), SESSION_KEYS.forward(to));
    return to;
  };
  const envelope = (to: string, type: QueueMessage["type"], extra: Partial<QueueMessage["payload"]> = {}, from = "peer"): QueueMessage => {
    const id = randomUUID();
    track(SESSION_KEYS.outstanding(from, id), SESSION_KEYS.taskLedger(id), SESSION_KEYS.taskAnswered(extra.in_reply_to || id));
    return { id, from, to, timestamp: "2030-01-01T00:00:00.000Z", type, payload: { content: `${type} body`, ...extra } };
  };
  const legacy = async (to: string, messages: readonly (QueueMessage | string)[]) => {
    if (messages.length > 0) await redis.rpush(SESSION_KEYS.queue(to), ...messages.map((m) => typeof m === "string" ? m : JSON.stringify(m)));
  };
  /** An owned reply: the mailbox `to` has an outstanding request answered by `from`. */
  const owned = async (to: string, from = "peer"): Promise<QueueMessage> => {
    const request = randomUUID();
    track(SESSION_KEYS.outstanding(to, request));
    await redis.set(SESSION_KEYS.outstanding(to, request), from, "EX", 600);
    return envelope(to, "result", { in_reply_to: request }, from);
  };
  const statusOf = async (store: MailboxStore, to: string) => (await store.status(to))[0]!;

  it("(a/R1) ten legacy status entries do not refuse a task; a status is refused with `full`", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, BOUND);
    await legacy(to, Array.from({ length: 10 }, () => envelope(to, "status")));
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "sent" });
    expect(await store.sendWithOutcome(envelope(to, "status"), 1)).toMatchObject({ status: "full", reason: "full" });
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(11);
  });

  it("(b/R2) an owned result whose outstanding key was deleted or changed counts as quiet", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, BOUND);
    const replies = await Promise.all(Array.from({ length: 10 }, () => owned(to)));
    await legacy(to, replies);
    expect(await statusOf(store, to)).toMatchObject({ waking_count: 10, quiet_count: 0 });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" });
    await redis.del(SESSION_KEYS.outstanding(to, replies[0]!.payload.in_reply_to!));
    await redis.set(SESSION_KEYS.outstanding(to, replies[1]!.payload.in_reply_to!), "someone-else");
    expect(await statusOf(store, to)).toMatchObject({ waking_count: 8, quiet_count: 2 });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "sent" });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "sent" });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" });
  });

  it("(c/R3) an overshoot past 2 x max_size via migration counts waking entries at the head and at the tail", async () => {
    const store = new MailboxStore(redis, redis, 3);
    for (const where of ["head", "tail"] as const) {
      const to = mailbox();
      const source = mailbox();
      const tasks = Array.from({ length: 3 }, () => envelope(to, "task"));
      const quiet = Array.from({ length: 10 }, () => envelope(to, "status"));
      await legacy(source, where === "head" ? [...tasks, ...quiet] : [...quiet, ...tasks]);
      expect(await store.migrateMessages(source, to)).toBe(13); // lossless: no admission
      expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(13);
      expect(await statusOf(store, to)).toMatchObject({ depth: 13, waking_count: 3, quiet_count: 10 });
      expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" });
      expect(await store.sendWithOutcome(envelope(to, "ping"), 1)).toMatchObject({ status: "full", reason: "full" });
      // Draining one waking entry admits exactly one more waking send.
      const index = where === "head" ? 0 : 12;
      await redis.lset(SESSION_KEYS.queue(to), index, "drained");
      await redis.lrem(SESSION_KEYS.queue(to), 1, "drained");
      expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "sent" });
      expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" });
    }
  });

  it("(d) each class's boundary is exactly max_size", async () => {
    const store = new MailboxStore(redis, redis, BOUND);
    const waking = mailbox();
    await legacy(waking, [...Array.from({ length: 9 }, () => envelope(waking, "task")), ...Array.from({ length: 5 }, () => envelope(waking, "status"))]);
    expect(await store.sendWithOutcome(await owned(waking), 1)).toMatchObject({ status: "sent" }); // 9 -> admit
    expect(await store.sendWithOutcome(envelope(waking, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" }); // 10 -> refuse
    expect(await store.sendWithOutcome(await owned(waking), 1)).toMatchObject({ status: "full", reason: "waking_full" });
    const quiet = mailbox();
    await legacy(quiet, Array.from({ length: 9 }, () => envelope(quiet, "task")));
    expect(await store.sendWithOutcome(envelope(quiet, "status"), 1)).toMatchObject({ status: "sent" }); // LLEN 9 -> admit
    expect(await store.sendWithOutcome(envelope(quiet, "status"), 1)).toMatchObject({ status: "full", reason: "full" }); // LLEN 10 -> refuse
    // An unowned reply (no outstanding key) is quiet.
    expect(await store.sendWithOutcome(envelope(quiet, "result", { in_reply_to: randomUUID() }), 1)).toMatchObject({ status: "full", reason: "full" });
  });

  it("(e) concurrent waking sends never exceed max_size waking entries", async () => {
    const to = mailbox();
    await legacy(to, Array.from({ length: 12 }, () => envelope(to, "ping")));
    const connections = Array.from({ length: 4 }, () => new Redis(REDIS_URL));
    try {
      const stores = connections.map((c) => new MailboxStore(c, c, BOUND));
      const results = await Promise.all(Array.from({ length: 24 }, (_, i) => stores[i % 4]!.sendWithOutcome(envelope(to, "task"), 1)));
      expect(results.filter((r) => r.status === "sent")).toHaveLength(BOUND);
      expect(results.filter((r) => r.status === "full" && r.reason === "waking_full")).toHaveLength(24 - BOUND);
      expect(await statusOf(stores[0]!, to)).toMatchObject({ waking_count: BOUND, quiet_count: 12, depth: 22 });
    } finally {
      for (const c of connections) c.disconnect();
    }
  });

  const snapshot = async (keys: readonly string[]) => Promise.all(keys.map(async (key) => {
    const type = await redis.type(key);
    switch (type) {
      case "list": return [key, type, await redis.lrange(key, 0, -1)];
      case "hash": return [key, type, await redis.hgetall(key)];
      case "stream": return [key, type, await redis.xrange(key, "-", "+")];
      case "string": return [key, type, await redis.get(key)];
      default: return [key, type, null];
    }
  }));

  it("(f) a refused send writes nothing, through both push scripts", async () => {
    const store = new MailboxStore(redis, redis, 2);
    for (const variant of ["plain", "idempotent"] as const) {
      for (const kind of ["task", "owned", "status"] as const) {
        const to = mailbox();
        await redis.hset(SESSION_KEYS.mailboxMeta(to), "current_size", 2);
        await legacy(to, [envelope(to, "task"), envelope(to, "task")]);
        const message = kind === "task" ? envelope(to, "task") : kind === "owned" ? await owned(to) : envelope(to, "status");
        const idem = SESSION_KEYS.idempotencyFor(message.from, to, `k-${message.id}`);
        track(idem);
        const keys = [SESSION_KEYS.queue(to), SESSION_KEYS.mailboxMeta(to), SESSION_KEYS.inboxEvents(to), idem,
          SESSION_KEYS.taskLedger(message.id), SESSION_KEYS.taskAnswered(message.payload.in_reply_to || message.id),
          SESSION_KEYS.outstanding(message.from, message.id),
          ...(message.payload.in_reply_to ? [SESSION_KEYS.outstanding(to, message.payload.in_reply_to)] : [])];
        const before = await snapshot(keys);
        const outcome = variant === "plain"
          ? await store.sendWithOutcome(message, 1)
          : await store.sendIdempotent(message, message.from, `k-${message.id}`, 1);
        expect(outcome).toMatchObject({ status: "full", reason: kind === "status" ? "full" : "waking_full" });
        expect(await snapshot(keys)).toEqual(before);
      }
    }
  });

  it("(g) an idempotent replay at full capacity returns the original id with no duplicate", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, 2);
    const first = envelope(to, "task");
    track(SESSION_KEYS.idempotencyFor("peer", to, "replay"));
    expect(await store.sendIdempotent(first, "peer", "replay", 1)).toEqual({ status: "sent", messageId: first.id });
    await legacy(to, [envelope(to, "task"), envelope(to, "status")]);
    expect(await store.sendIdempotent(envelope(to, "task"), "peer", "fresh", 1)).toMatchObject({ status: "full", reason: "waking_full" });
    track(SESSION_KEYS.idempotencyFor("peer", to, "fresh"));
    expect(await store.sendIdempotent({ ...envelope(to, "task") }, "peer", "replay", 1)).toEqual({ status: "duplicate", messageId: first.id });
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(3);
  });

  it("(h) the watchdog script refuses with a reason and admits a task past quiet mail", async () => {
    const store = new MailboxStore(redis, redis, 1);
    const blocked = mailbox();
    track(SESSION_KEYS.watchdogSent(blocked));
    await legacy(blocked, [envelope(blocked, "task")]);
    const refused = envelope(blocked, "task");
    track(SESSION_KEYS.idempotencyFor("peer", blocked, "watchdog:f"));
    expect(await store.sendWatchdogTask(refused, "peer", "watchdog:f", { fireKey: "f", cap: 6 }, 1))
      .toEqual({ status: "full", messageId: refused.id, reason: "waking_full" });
    expect(await redis.zcard(SESSION_KEYS.watchdogSent(blocked))).toBe(0);
    const quiet = mailbox();
    track(SESSION_KEYS.watchdogSent(quiet), SESSION_KEYS.idempotencyFor("peer", quiet, "watchdog:q"));
    await legacy(quiet, [envelope(quiet, "status"), "{}"]);
    expect(await store.sendWatchdogTask(envelope(quiet, "task"), "peer", "watchdog:q", { fireKey: "q", cap: 6 }, 1)).toMatchObject({ status: "sent" });
    expect(await redis.zcard(SESSION_KEYS.watchdogSent(quiet))).toBe(1);
  });

  it("(i) above 1024 entries every ordinary send is refused with `oversize`", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, BOUND);
    await legacy(to, Array.from({ length: 1024 }, () => "{}"));
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "sent" }); // 1024 is not above the ceiling
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "oversize" });
    expect(await store.sendWithOutcome(envelope(to, "status"), 1)).toMatchObject({ status: "full", reason: "oversize" });
    const message = envelope(to, "task");
    track(SESSION_KEYS.idempotencyFor("peer", to, "o"));
    expect(await store.sendIdempotent(message, "peer", "o", 1)).toMatchObject({ status: "full", reason: "oversize" });
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(1025);
  });

  it("(j) decodable malformed entries count as quiet", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, 2);
    await redis.set(SESSION_KEYS.outstanding(to, "r-num"), "peer");
    track(SESSION_KEYS.outstanding(to, "r-num"));
    await legacy(to, [
      "[1,2]", "42", "null", "true", JSON.stringify({ to, type: "task", payload: { content: "x" } }),
      JSON.stringify({ id: "x", to, type: "task" }), JSON.stringify({ id: "x", to, type: "task", payload: null }),
      JSON.stringify({ id: "x", to, type: "task", payload: 0 }), JSON.stringify({ id: 7, to, type: "task", payload: {} }),
      JSON.stringify({ ...envelope("someone-else", "task") }),
      JSON.stringify({ id: "x", from: "peer", to, type: "result", payload: "r-num" }),
    ]);
    expect(await statusOf(store, to)).toMatchObject({ depth: 11, waking_count: 0, quiet_count: 11 });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "sent" });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "sent" });
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" });
  });

  it("(k) get_queue_status reports waking and quiet counts", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, BOUND);
    await legacy(to, [envelope(to, "task"), await owned(to), envelope(to, "status"), envelope(to, "result", { in_reply_to: "gone" }), "[]"]);
    expect(await statusOf(store, to)).toEqual({ agent: to, depth: 5, max_size: BOUND, waking_count: 2, quiet_count: 3 });
    const empty = mailbox();
    expect(await statusOf(store, empty)).toMatchObject({ depth: 0, waking_count: 0, quiet_count: 0 });
  });

  it("the Lua classifier agrees with wakesRecipient on every entry both decoders accept", async () => {
    const to = mailbox();
    const lib = readFileSync(join(LUA_DIR, "waking-admission-lib.lua"), "utf8");
    const classify = `${lib}\nlocal out = {}\nfor i, raw in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do out[i] = adm_entry_wakes(raw, ARGV[1], ARGV[2]) and 1 or 0 end\nreturn out`;
    const live = await owned(to);
    const changed = await owned(to);
    await redis.set(SESSION_KEYS.outstanding(to, changed.payload.in_reply_to!), "other");
    const wrongType = randomUUID();
    track(SESSION_KEYS.outstanding(to, wrongType));
    await redis.rpush(SESSION_KEYS.outstanding(to, wrongType), "peer");
    const entries: string[] = [
      envelope(to, "task"), envelope(to, "status"), envelope(to, "ping"), live, changed,
      { ...live, type: "error" }, { ...live, from: "other" }, { ...live, to: "elsewhere" },
      envelope(to, "result", { in_reply_to: "" }), envelope(to, "result", { in_reply_to: wrongType }),
      envelope(to, "error", { in_reply_to: randomUUID() }), { ...envelope(to, "task"), payload: { content: "" } },
    ].map((m) => JSON.stringify(m)).concat(["[]", "{}", "1", "true", "null", JSON.stringify({ id: "a", to, type: "task", payload: [] }),
      JSON.stringify({ id: "a", to, type: "task", payload: "s" }), JSON.stringify({ id: "a", to, type: "result", from: "peer", payload: "s" })]);
    await redis.rpush(SESSION_KEYS.queue(to), ...entries);
    const lua = (await redis.eval(classify, 1, SESSION_KEYS.queue(to), to, OUTSTANDING_PREFIX)) as number[];
    const parsed = entries.map(parseQueueEntry);
    const messages = parsed.map((m) => m ?? ({} as QueueMessage));
    const ownerKeys = await replyOwners(redis, to, messages); // MGET answers nil for a wrong-type key
    const ts = parsed.map((m, i) => (m !== null && wakesRecipient(to, m, ownerKeys[i] ?? null) ? 1 : 0));
    expect(lua).toEqual(ts);
    expect(ts.filter((v) => v === 1).length).toBeGreaterThanOrEqual(4);
  });

  it("(F5) entries cjson cannot decode count as waking, so admission never undercounts consumers", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, BOUND);
    const deep = (depth: number) => "[".repeat(depth) + "]".repeat(depth);
    const task = (payloadJson: string) => `{"id":"${randomUUID()}","from":"peer","to":${JSON.stringify(to)},"type":"task","payload":${payloadJson}}`;
    // Raw text, not JSON.stringify: these exercise decoder acceptance limits.
    const cases: Record<string, { raw: string; lua: 0 | 1 | null }> = {
      unpaired_surrogate_task: { raw: task('{"content":"\\ud800"}'), lua: 1 },
      too_deep_task: { raw: task(`{"content":"d","n":${deep(1100)}}`), lua: 1 },
      not_json: { raw: "x", lua: 1 },
      empty: { raw: "", lua: 1 },
      hex_number_task: { raw: task('{"content":"h","n":0x10}'), lua: null }, // cjson may accept what JSON.parse rejects
    };
    for (const [name, { raw, lua }] of Object.entries(cases)) {
      await redis.del(SESSION_KEYS.queue(to));
      await redis.rpush(SESSION_KEYS.queue(to), raw);
      const luaWakes = (await store.status(to))[0]!.waking_count === 1;
      const message = parseQueueEntry(raw);
      const tsWakes = message !== null && wakesRecipient(to, message, (await replyOwners(redis, to, [message]))[0] ?? null);
      expect({ name, undercount: tsWakes && !luaWakes }).toEqual({ name, undercount: false });
      if (lua !== null) expect({ name, lua: luaWakes ? 1 : 0 }).toEqual({ name, lua });
    }
    // The surrogate task really wakes consumers, and occupies the only waking slot.
    const surrogate = cases.unpaired_surrogate_task!.raw;
    expect(parseQueueEntry(surrogate)?.type).toBe("task");
    const tight = new MailboxStore(redis, redis, 1);
    await redis.del(SESSION_KEYS.queue(to));
    await redis.rpush(SESSION_KEYS.queue(to), surrogate);
    expect(await tight.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" });
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(1);
  });

  it("send_message reports QUEUE_FULL with its reason; get_queue_status shows the counts", async () => {
    const sender = new RedisClient(null, REDIS_URL);
    const receiver = new RedisClient(null, REDIS_URL);
    const names = [`wa-s-${randomUUID().slice(0, 8)}`, `wa-r-${randomUUID().slice(0, 8)}`] as const;
    try {
      const reg = async (client: RedisClient, name: string) =>
        JSON.parse((await registerAgent(client, registerAgentSchema.parse({ name, role: "both", description: "waking admission" }))).content[0]!.text);
      const s = await reg(sender, names[0]);
      await reg(receiver, names[1]);
      const to = names[1];
      track(SESSION_KEYS.queue(to), SESSION_KEYS.mailboxMeta(to), SESSION_KEYS.inboxEvents(to));
      await legacy(to, Array.from({ length: sender.queueBound }, () => envelope(to, "task")));
      const call = async (extra: Record<string, unknown>) =>
        JSON.parse((await sendMessage(sender, sendMessageSchema.parse({ session_id: s.session_id, to, content: "x", ...extra }))).content[0]!.text);
      // One tool call (it retries for ~20 s); the store-level reasons of every variant are covered above.
      const refused = await call({ idempotency_key: `k-${randomUUID()}` });
      expect(refused.error).toMatchObject({ code: "QUEUE_FULL", retryable: true, reason: "waking_full" });
      const status = (await getQueueStatus(receiver, { agent: to })).structuredContent as { queues: unknown[] };
      expect(status.queues[0]).toMatchObject({ waking_count: sender.queueBound, quiet_count: 0 });
    } finally {
      await sender.unregister().catch(() => undefined);
      await receiver.unregister().catch(() => undefined);
      await sender.shutdown();
      await receiver.shutdown();
    }
  }, 120_000);

  // Baseline-compatible regressions (F2): boolean send() and raw LLEN only.
  it("(R1, baseline API) ten legacy status entries do not refuse a task", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, BOUND);
    await legacy(to, Array.from({ length: 10 }, () => envelope(to, "status")));
    expect(await store.send(envelope(to, "task"), 1)).toBe(true); // admission assertion
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(11);
    expect(await store.send(envelope(to, "status"), 1)).toBe(false);
  });

  it("(R2, baseline API) a reply whose ownership changed stops occupying the waking quota", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, 3);
    const replies = await Promise.all(Array.from({ length: 3 }, () => owned(to)));
    await legacy(to, replies);
    expect(await store.send(envelope(to, "task"), 1)).toBe(false);
    await redis.set(SESSION_KEYS.outstanding(to, replies[0]!.payload.in_reply_to!), "someone-else");
    expect(await store.send(envelope(to, "task"), 1)).toBe(true); // admission assertion
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(4);
  });

  it("(F1) saturated waking quota: max_size 1 and one queued owned reply refuse another waking send", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, 1);
    await legacy(to, [await owned(to)]);
    expect(await store.sendWithOutcome(envelope(to, "task"), 1)).toMatchObject({ status: "full", reason: "waking_full" });
    expect(await store.sendWithOutcome(await owned(to), 1)).toMatchObject({ status: "full", reason: "waking_full" });
    expect(await redis.llen(SESSION_KEYS.queue(to))).toBe(1);
  });

  it("(F1) differential: Lua status counts and wakesRecipient classify malformed replies identically", async () => {
    const to = mailbox();
    const store = new MailboxStore(redis, redis, BOUND);
    const ownerOf = async (requestId: string, owner: string) => {
      track(SESSION_KEYS.outstanding(to, requestId));
      await redis.set(SESSION_KEYS.outstanding(to, requestId), owner, "EX", 600);
    };
    await ownerOf("7", "peer");
    await ownerOf("true", "peer");
    await ownerOf("[object Object]", "peer");
    await ownerOf("r-num-from", "5");
    const valid = await owned(to);
    const changed = await owned(to);
    await redis.set(SESSION_KEYS.outstanding(to, changed.payload.in_reply_to!), "other");
    const reply = (fields: Record<string, unknown>, payload: Record<string, unknown>) =>
      JSON.stringify({ id: randomUUID(), from: "peer", to, type: "result", timestamp: "2030-01-01T00:00:00.000Z", ...fields, payload: { content: "r", ...payload } });
    const cases: Record<string, string> = {
      numeric_in_reply_to: reply({}, { in_reply_to: 7 }),
      boolean_in_reply_to: reply({}, { in_reply_to: true }),
      object_in_reply_to: reply({}, { in_reply_to: { id: "x" } }),
      empty_in_reply_to: reply({}, { in_reply_to: "" }),
      null_from_no_owner: reply({ from: null }, { in_reply_to: `absent-${randomUUID()}` }),
      missing_from: JSON.stringify({ id: randomUUID(), to, type: "error", payload: { content: "r", in_reply_to: `absent-${randomUUID()}` } }),
      numeric_from: reply({ from: 5 }, { in_reply_to: "r-num-from" }),
      valid_owned: JSON.stringify(valid),
      changed_owner: JSON.stringify(changed),
    };
    const wakesInTs = async (raw: string) => {
      const message = parseQueueEntry(raw);
      if (message === null) return false;
      const [owner] = await replyOwners(redis, to, [message]);
      return wakesRecipient(to, message, owner ?? null);
    };
    for (const [name, raw] of Object.entries(cases)) {
      await redis.del(SESSION_KEYS.queue(to));
      await redis.rpush(SESSION_KEYS.queue(to), raw);
      const { waking_count: luaWaking } = (await store.status(to))[0]!;
      expect({ name, waking: luaWaking === 1 }).toEqual({ name, waking: await wakesInTs(raw) });
      expect({ name, waking: luaWaking === 1 }).toEqual({ name, waking: name === "valid_owned" });
    }
  });
});
