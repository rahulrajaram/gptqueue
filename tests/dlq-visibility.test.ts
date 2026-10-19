/**
 * Dead-letter visibility for observers: `dead_lettered_count` (DLQ LLEN) and
 * `dead_lettered_dropped` (the DLQ drop counter) per mailbox, in
 * get_queue_status and `gptqueue-inbox-peek --list`. Reads only: no key is
 * created, and no DLQ content appears in the listing. DLQ entries are made by
 * the real recovery Lua (claims-recover.lua) with a small length bound.
 * Every test uses its own agent names and deletes only its own keys.
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { assertPrivateTestRedis } from "./helpers/redis-test-utils.js";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { MailboxStore } from "../src/core/mailbox-store.js";
import { TaskClaimStore, claimLuaScript } from "../src/core/task-claim-store.js";
import { CLAIM_KEYS, DLQ_KEYS, SESSION_KEYS, parseDeadLetterDropped } from "../src/core/keys.js";
import { runPeek } from "../src/claude/peek.js";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { getQueueStatus } from "../src/mcp-server/tools/queue-status.js";

const REDIS_URL = testRedisUrl();
assertPrivateTestRedis(REDIS_URL, "dlq-visibility");
const T0 = Date.parse("2030-01-01T00:00:00.000Z");
const DLQ_BODY = "dlq-hidden-body";

let redis: Redis;
let home = "";
const keys = new Set<string>();
const recoverScript = claimLuaScript("claims-recover.lua");

beforeAll(async () => {
  redis = new Redis(REDIS_URL);
  home = await mkdtemp(join(tmpdir(), "gptq-dlq-vis-"));
});
afterEach(async () => { if (keys.size > 0) { await redis.del(...keys); keys.clear(); } });
afterAll(async () => { redis.disconnect(); await rm(home, { recursive: true, force: true }); });

const allKeys = (agent: string): string[] => [
  SESSION_KEYS.queue(agent), SESSION_KEYS.mailboxMeta(agent), SESSION_KEYS.inboxEvents(agent),
  CLAIM_KEYS.index(agent), DLQ_KEYS.list(agent), DLQ_KEYS.dropped(agent), DLQ_KEYS.reasons(agent),
];
const mailbox = (prefix: string): string => {
  const agent = `${prefix}-${randomUUID().slice(0, 8)}`;
  for (const key of allKeys(agent)) keys.add(key);
  return agent;
};

/** Dead-letter `n` tasks through the real recovery Lua with cap 0 and DLQ bound `bound`. */
const deadLetter = async (agent: string, n: number, bound: number): Promise<void> => {
  const store = new TaskClaimStore(redis, { clock: "caller" });
  for (let i = 0; i < n; i++) {
    const id = `${agent}-m${i}`;
    keys.add(CLAIM_KEYS.recoverCount(agent, id));
    await redis.rpush(SESSION_KEYS.queue(agent), JSON.stringify({
      id, from: "sender", to: agent, timestamp: "2030-01-01T00:00:00.000Z", type: "task", payload: { content: `${DLQ_BODY} ${i}` },
    }));
    const claimed = await store.claim({ actor_id: agent, session_id: "s", max_batch: 1, ttl_seconds: 1, now: new Date(T0 + i * 10_000).toISOString() });
    expect(claimed.ok).toBe(true);
    await redis.eval(recoverScript, 7, CLAIM_KEYS.index(agent), CLAIM_KEYS.claims, SESSION_KEYS.queue(agent), DLQ_KEYS.list(agent),
      SESSION_KEYS.inboxEvents(agent), SESSION_KEYS.mailboxMeta(agent), DLQ_KEYS.dropped(agent), T0 + i * 10_000 + 5_000, 0, bound, 604800);
  }
};

const peek = async (argv: readonly string[]) => {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const out = vi.spyOn(process.stdout, "write").mockImplementation((chunk) => { stdout.push(String(chunk)); return true; });
  const err = vi.spyOn(process.stderr, "write").mockImplementation((chunk) => { stderr.push(String(chunk)); return true; });
  try {
    const code = await runPeek(argv, { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: join(home, "rt"), GPTQ_CLAUDE_LEGACY_SCAN: "off" });
    return { code, stdout: stdout.join(""), stderr: stderr.join("") };
  } finally { out.mockRestore(); err.mockRestore(); }
};
const listJson = async (agent: string) => {
  const result = await peek(["--name", agent, "--list", "--json"]);
  expect(result).toMatchObject({ code: 0, stderr: "" });
  return { raw: result.stdout, json: JSON.parse(result.stdout) as Record<string, unknown> };
};

describe("parseDeadLetterDropped", () => {
  it("missing is 0, a canonical integer is itself, anything else is null", () => {
    expect(parseDeadLetterDropped(null)).toBe(0);
    expect(parseDeadLetterDropped("0")).toBe(0);
    expect(parseDeadLetterDropped("12")).toBe(12);
    for (const bad of ["-1", "01", "1.5", "x", "", "99999999999999999999"]) expect(parseDeadLetterDropped(bad), bad).toBeNull();
  });
});

describe("dead-lettered counts", () => {
  const cases = [
    { label: "no DLQ", n: 0, count: 0, dropped: 0 },
    { label: "two entries", n: 2, count: 2, dropped: 0 },
    { label: "more than the bound (3) drops the oldest", n: 5, count: 3, dropped: 2 },
  ];
  for (const { label, n, count, dropped } of cases) {
    it(`${label}: get_queue_status, MailboxStore.status and --list agree`, async () => {
      const agent = mailbox("dv");
      await deadLetter(agent, n, 3);
      expect(await redis.llen(DLQ_KEYS.list(agent))).toBe(count);

      const status = (await new MailboxStore(redis, redis, 10).status(agent))[0]!;
      expect(status).toMatchObject({ agent, depth: 0, dead_lettered_count: count, dead_lettered_dropped: dropped });

      const client = new RedisClient(null, REDIS_URL);
      try {
        const tool = await getQueueStatus(client, { agent });
        const body = tool.structuredContent as { queues: Record<string, unknown>[] };
        expect(body.queues).toEqual([{ agent, depth: 0, max_size: expect.any(Number), waking_count: 0, quiet_count: 0, dead_lettered_count: count, dead_lettered_dropped: dropped }]);
      } finally { await client.shutdown(); }

      const { raw, json } = await listJson(agent);
      expect(json).toMatchObject({ agent, dead_lettered_count: count, dead_lettered_dropped: dropped });
      expect(raw).not.toContain(DLQ_BODY);
      expect(raw).not.toContain(`${agent}-m`); // no DLQ message ids
      expect(raw).not.toContain("sender"); // no DLQ senders

      const human = await peek(["--name", agent, "--list"]);
      expect(human.code).toBe(0);
      expect(human.stdout.trimEnd().split("\n").at(-1)).toBe(`dead-lettered: ${count}, dropped ${dropped}`);
      expect(human.stdout).not.toContain(DLQ_BODY);
      expect(human.stdout).not.toContain(`${agent}-m`);
    });
  }

  it("reading a mailbox with no DLQ creates no key", async () => {
    const agent = mailbox("dv-nokey");
    await redis.hset(SESSION_KEYS.registry, agent, "{}");
    try {
      await new MailboxStore(redis, redis, 10).status(agent);
      await new MailboxStore(redis, redis, 10).status(); // the all-agents form too
      await listJson(agent);
      await peek(["--name", agent, "--list"]);
      expect(await redis.exists(...allKeys(agent))).toBe(0);
    } finally { await redis.hdel(SESSION_KEYS.registry, agent); }
  });

  it("an unreadable drop counter is reported as null, not guessed", async () => {
    const agent = mailbox("dv-corrupt");
    await redis.set(DLQ_KEYS.dropped(agent), "not-a-number");
    expect((await new MailboxStore(redis, redis, 10).status(agent))[0]).toMatchObject({ dead_lettered_count: 0, dead_lettered_dropped: null });
    expect((await listJson(agent)).json).toMatchObject({ dead_lettered_count: 0, dead_lettered_dropped: null });
    expect((await peek(["--name", agent, "--list"])).stdout.trimEnd().split("\n").at(-1)).toBe("dead-lettered: 0, dropped ?");
  });
});
