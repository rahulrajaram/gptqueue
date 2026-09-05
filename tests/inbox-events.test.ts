import { beforeEach, afterEach, describe, expect, it, vi } from "vitest";
import { Redis } from "ioredis";
import { InboxEvents } from "../src/core/inbox-events.js";
import { MailboxStore } from "../src/core/mailbox-store.js";
import type { QueueMessage } from "../src/mcp-server/types.js";
import { flushTestKeys } from "./helpers/redis-test-utils.js";

describe("InboxEvents", () => {
  const url = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
  let redis: Redis;
  let subscriber: Redis;
  let store: MailboxStore;
  let events: InboxEvents;
  beforeEach(async () => { redis = new Redis(url); subscriber = new Redis(url); await flushTestKeys(redis, url); store = new MailboxStore(redis, subscriber, 10); events = new InboxEvents(redis); });
  afterEach(async () => { await flushTestKeys(redis, url); await Promise.all([redis.quit(), subscriber.quit()]); });
  const message = (overrides: Partial<QueueMessage> = {}): QueueMessage => ({ id: "task-1", from: "a", to: "b", timestamp: new Date().toISOString(), type: "task", payload: { content: "secret", ...overrides.payload }, ...overrides });

  it("emits task event, records correlation, and recovers backlog", async () => {
    await redis.hset("gptq:meta:b", "max_size", 10);
    await expect(store.send(message())).resolves.toBe(true);
    expect(await redis.xlen("gptq:inbox-events:b")).toBe(1);
    expect(await redis.get("gptq:outstanding:a:task-1")).toBe("b");
    expect((await events.pending("b"))[0].id).toBe("task-1");
  });

  it("only notifies a correlated reply from the expected peer", async () => {
    await redis.hset("gptq:meta:b", "max_size", 10); await redis.hset("gptq:meta:a", "max_size", 10);
    await store.send(message());
    await store.send({ ...message({ id: "reply", from: "b", to: "a", type: "result", payload: { content: "ok", in_reply_to: "task-1" } }) });
    await store.send({ ...message({ id: "wrong", from: "c", to: "a", type: "result", payload: { content: "x", in_reply_to: "task-1" } }) });
    await store.send({ ...message({ id: "status", from: "b", to: "a", type: "status", payload: { content: "x" } }) });
    expect(await redis.xlen("gptq:inbox-events:a")).toBe(1);
  });

  it("preflights a bad stream key before enqueue", async () => {
    await redis.hset("gptq:meta:b", "max_size", 10); await redis.set("gptq:inbox-events:b", "bad");
    await expect(store.send(message())).rejects.toThrow(); expect(await redis.llen("gptq:q:b")).toBe(0);
  });

  it("does not duplicate events on idempotent retry and preserves reply correlation", async () => {
    await redis.hset("gptq:meta:b", "max_size", 10); await redis.hset("gptq:meta:a", "max_size", 10);
    const task = message({ id: "idem-task" });
    await expect(store.sendIdempotent(task, "a", "same-key")).resolves.toMatchObject({ status: "sent", messageId: "idem-task" });
    await expect(store.sendIdempotent(task, "a", "same-key")).resolves.toMatchObject({ status: "duplicate", messageId: "idem-task" });
    expect(await redis.llen("gptq:q:b")).toBe(1); expect(await redis.xlen("gptq:inbox-events:b")).toBe(1);
    const reply = message({ id: "idem-reply", from: "b", to: "a", type: "result", payload: { content: "ok", in_reply_to: "idem-task" } });
    await expect(store.sendIdempotent(reply, "b", "reply-key")).resolves.toMatchObject({ status: "sent" });
    expect(await redis.xlen("gptq:inbox-events:a")).toBe(1);
  });

  it("does not emit an event for a full mailbox", async () => {
    const bounded = new MailboxStore(redis, subscriber, 1); await redis.hset("gptq:meta:b", "max_size", 1);
    await expect(bounded.send(message({ id: "full-1" }))).resolves.toBe(true);
    await expect(bounded.send(message({ id: "full-2" }))).resolves.toBe(false);
    expect(await redis.llen("gptq:q:b")).toBe(1); expect(await redis.xlen("gptq:inbox-events:b")).toBe(1);
  }, 30000);

  it("preflights the idempotent event stream before mutation", async () => {
    await redis.hset("gptq:meta:b", "max_size", 10); await redis.set("gptq:inbox-events:b", "wrong-type");
    await expect(store.sendIdempotent(message({ id: "bad-idem" }), "a", "bad-stream")).rejects.toThrow();
    expect(await redis.llen("gptq:q:b")).toBe(0); expect(await redis.get("gptq:idempotency:a:bad-stream")).toBeNull();
  });

  it("makes trace failure best effort and emits only a generic diagnostic", async () => {
    await redis.set("gptq:inbox-trace:b", "wrong-type");
    const write = vi.spyOn(process.stderr, "write").mockImplementation(() => true);
    await expect(events.trace("b", { stage: "reply_sent", timestamp: new Date().toISOString(), message_id: "secret-id", code: "E_TEST" })).resolves.toBeUndefined();
    expect(write).toHaveBeenCalledWith(expect.stringContaining('"event":"inbox_trace_failed"'));
    expect(write.mock.calls[0]?.[0]).not.toContain("secret-id");
    write.mockRestore();
  });

  it("waits for an event and cancellation owns only its connection", async () => {
    await redis.hset("gptq:meta:b", "max_size", 10); const controller = new AbortController();
    const waiting = events.wait("b", "0-0", controller.signal, 2000); await store.send(message());
    await expect(waiting).resolves.toBeTruthy(); controller.abort();
  });
});
