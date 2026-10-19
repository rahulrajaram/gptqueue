/**
 * gptqueue-inbox-peek --list: a non-consuming listing of an inbox's unclaimed
 * mail and open claims, marking the mail the dispatcher would wake on.
 * Every test uses its own agent names and deletes only its own keys.
 */
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { runPeek } from "../src/claude/peek.js";
import { resolveAgentName } from "../src/claude/agent-name.js";
import { InboxEvents, replyOwnerKey, wakesRecipient } from "../src/core/inbox-events.js";
import { CLAIM_KEYS, SESSION_KEYS } from "../src/core/keys.js";
import type { QueueMessage } from "../src/core/types.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
let home = "";
let env: NodeJS.ProcessEnv = {};
let redis: Redis;
const keys: string[] = [];
const claimIds: string[] = [];

beforeAll(async () => {
  home = await mkdtemp(join(tmpdir(), "gptq-peek-list-"));
  env = { PATH: process.env.PATH, HOME: home, REDIS_URL, GPTQ_CLAUDE_RUNTIME_DIR: join(home, "rt"), GPTQ_CLAUDE_LEGACY_SCAN: "off" };
  redis = new Redis(REDIS_URL);
});
afterEach(async () => {
  if (keys.length > 0) await redis.del(...keys.splice(0));
  if (claimIds.length > 0) await redis.hdel(CLAIM_KEYS.claims, ...claimIds.splice(0));
});
afterAll(async () => { await redis.quit(); await rm(home, { recursive: true, force: true }); });

const inbox = (prefix: string, name = `${prefix}-${randomUUID().slice(0, 8)}`): string => {
  keys.push(SESSION_KEYS.queue(name), CLAIM_KEYS.index(name), SESSION_KEYS.inboxEvents(name));
  return name;
};
type Mail = Partial<Omit<QueueMessage, "payload">> & { id: string; ageS?: number; content?: string; in_reply_to?: string };
const mail = (to: string, { id, ageS = 1, content = "hello", in_reply_to, ...rest }: Mail): string => JSON.stringify({
  id, from: "peer", to, timestamp: new Date(Date.now() - ageS * 1000).toISOString(), type: "task",
  payload: { content, ...(in_reply_to === undefined ? {} : { in_reply_to }) }, ...rest,
});
const outstandingOwner = async (agent: string, requestId: string, owner: string): Promise<void> => {
  const key = SESSION_KEYS.outstanding(agent, requestId);
  keys.push(key);
  await redis.set(key, owner, "EX", 600);
};

const peek = async (argv: readonly string[], extra: NodeJS.ProcessEnv = {}) => {
  const stdout: string[] = [];
  const stderr: string[] = [];
  const out = vi.spyOn(process.stdout, "write").mockImplementation((chunk) => { stdout.push(String(chunk)); return true; });
  const err = vi.spyOn(process.stderr, "write").mockImplementation((chunk) => { stderr.push(String(chunk)); return true; });
  try {
    const code = await runPeek(argv, { ...env, ...extra });
    return { code, stdout: stdout.join(""), stderr: stderr.join("") };
  } finally { out.mockRestore(); err.mockRestore(); }
};
type Listing = {
  agent: string; source: string; unclaimed: number; listed: number; truncated: boolean; oldest_unclaimed_age_s: number | null; wake_eligible_count: number;
  messages: ({ raw_index: number; error: string } & Record<string, unknown>)[];
  claims: { claim_id: string; session_id: string | null; claimed_at: string | null; age_s: number | null; expires_at: string | null; expired: boolean; task_count: number | null }[];
  expired_claims: number;
};
const list = async (name: string, ...more: string[]): Promise<Listing> => {
  const result = await peek(["--name", name, "--list", "--json", ...more]);
  expect(result).toMatchObject({ code: 0, stderr: "" });
  expect(result.stdout.split("\n")).toEqual([expect.stringMatching(/^\{.*\}$/), ""]); // one JSON object on one line
  return JSON.parse(result.stdout) as Listing;
};

describe("wake eligibility (the dispatcher's rule, shared with --list)", () => {
  const message = (overrides: Partial<QueueMessage>): QueueMessage => ({ id: "m", from: "b", to: "a", timestamp: "", type: "task", payload: { content: "" }, ...overrides });

  it("a task wakes; status never does; a reply wakes only when its request's owner is the sender", () => {
    expect(wakesRecipient("a", message({ type: "task" }), null)).toBe(true);
    expect(wakesRecipient("a", message({ type: "status" }), "b")).toBe(false);
    expect(wakesRecipient("a", message({ type: "ping" }), "b")).toBe(false);
    const reply = message({ type: "result", payload: { content: "", in_reply_to: "req" } });
    expect(wakesRecipient("a", reply, "b")).toBe(true);
    expect(wakesRecipient("a", { ...reply, type: "error" }, "b")).toBe(true);
    expect(wakesRecipient("a", reply, "c")).toBe(false);
    expect(wakesRecipient("a", reply, null)).toBe(false);
    expect(wakesRecipient("a", message({ type: "result" }), "b")).toBe(false); // no in_reply_to
    expect(wakesRecipient("x", message({ type: "task" }), null)).toBe(false); // addressed to someone else
  });

  it("only an addressed result or error with in_reply_to needs the owner lookup", () => {
    expect(replyOwnerKey("a", message({ type: "result", payload: { content: "", in_reply_to: "req" } }))).toBe(SESSION_KEYS.outstanding("a", "req"));
    expect(replyOwnerKey("a", message({ type: "task" }))).toBeNull();
    expect(replyOwnerKey("x", message({ type: "result", payload: { content: "", in_reply_to: "req" } }))).toBeNull();
  });
});

describe("gptqueue-inbox-peek --list", () => {
  it("marks exactly the mail InboxEvents.pending would wake on", async () => {
    const me = inbox("pl-wake");
    await outstandingOwner(me, "req-own", "peer");
    await outstandingOwner(me, "req-own-2", "peer");
    await redis.rpush(SESSION_KEYS.queue(me),
      mail(me, { id: "task" }),
      mail(me, { id: "status", type: "status" }),
      mail(me, { id: "own-reply", type: "result", in_reply_to: "req-own" }),
      mail(me, { id: "wrong-sender", type: "result", from: "intruder", in_reply_to: "req-own" }),
      mail(me, { id: "unsolicited", type: "result", in_reply_to: "req-nobody-sent" }),
      mail(me, { id: "own-error", type: "error", in_reply_to: "req-own-2" }),
      mail(me, { id: "no-reply-to", type: "result" }),
      mail(me, { id: "misaddressed", to: "someone-else" }));
    const listing = await list(me);
    const wakes = Object.fromEntries(listing.messages.map((m) => [m.id, m.wakes]));
    expect(wakes).toEqual({ task: true, status: false, "own-reply": true, "wrong-sender": false, unsolicited: false, "own-error": true, "no-reply-to": false, misaddressed: false });
    expect(listing.wake_eligible_count).toBe(3);
    const pending = await new InboxEvents(redis).pending(me);
    expect(listing.messages.filter((m) => m.wakes).map((m) => m.id)).toEqual(pending.map((m) => m.id));
    expect(listing.messages.find((m) => m.id === "own-reply")).toMatchObject({ from: "peer", type: "result", in_reply_to: "req-own" });
    expect(listing.messages.find((m) => m.id === "task")).toMatchObject({ in_reply_to: null });
  });

  it("lists in queue order with ages from Redis TIME; the oldest counts even beyond --limit", async () => {
    const me = inbox("pl-age");
    await redis.rpush(SESSION_KEYS.queue(me), mail(me, { id: "a", ageS: 300 }), mail(me, { id: "b", ageS: 5 }), mail(me, { id: "recovered", ageS: 900 }));
    const listing = await list(me, "--limit", "2");
    expect(listing.messages.map((m) => m.id)).toEqual(["a", "b"]);
    const [a, b] = listing.messages;
    expect(a!.age_s).toBeGreaterThanOrEqual(299); expect(a!.age_s).toBeLessThanOrEqual(302);
    expect(b!.age_s).toBeGreaterThanOrEqual(4); expect(b!.age_s).toBeLessThanOrEqual(7);
    expect(typeof a!.timestamp).toBe("string");
    expect(listing.oldest_unclaimed_age_s).toBeGreaterThanOrEqual(899);
    expect(listing.oldest_unclaimed_age_s).toBeLessThanOrEqual(902);
  });

  it("caps the listing at --limit (default 50) and reports truncation and the full count", async () => {
    const me = inbox("pl-limit");
    await redis.rpush(SESSION_KEYS.queue(me), ...Array.from({ length: 52 }, (_, i) => mail(me, { id: `m${i}`, type: i % 2 === 0 ? "task" : "status" })));
    const byDefault = await list(me);
    expect(byDefault).toMatchObject({ unclaimed: 52, listed: 50, truncated: true, wake_eligible_count: 25 });
    expect(byDefault.messages.at(-1)!.id).toBe("m49");
    const three = await list(me, "--limit", "3");
    expect(three).toMatchObject({ unclaimed: 52, listed: 3, truncated: true, wake_eligible_count: 2 });
    expect(three.messages.map((m) => m.id)).toEqual(["m0", "m1", "m2"]);
    expect(await list(me, "--limit", "52")).toMatchObject({ listed: 52, truncated: false, wake_eligible_count: 26 });
    expect(await list(me, "--limit", "500")).toMatchObject({ listed: 52, truncated: false });
  });

  it("previews the first 80 characters with whitespace collapsed and an ellipsis when cut", async () => {
    const me = inbox("pl-preview");
    const words = Array.from({ length: 30 }, (_, i) => `w${i}`);
    await redis.rpush(SESSION_KEYS.queue(me),
      mail(me, { id: "short", content: "  hello\n\n\tworld   again \r\n" }),
      mail(me, { id: "long", content: words.join("\n\t  ") }),
      mail(me, { id: "exact", content: "y".repeat(80) }),
      mail(me, { id: "over", content: "y".repeat(81) }),
      mail(me, { id: "emoji", content: "😀".repeat(81) }),
      JSON.stringify({ id: "no-content", from: "peer", to: me, timestamp: new Date().toISOString(), type: "task", payload: {} }));
    const preview = Object.fromEntries((await list(me)).messages.map((m) => [m.id, m.preview]));
    expect(preview.short).toBe("hello world again");
    expect(preview.long).toBe(`${words.join(" ").slice(0, 80)}…`);
    expect(preview.exact).toBe("y".repeat(80));
    expect(preview.over).toBe(`${"y".repeat(80)}…`);
    expect(preview.emoji).toBe(`${"😀".repeat(80)}…`); // characters, not UTF-16 code units
    expect(preview["no-content"]).toBeNull();
  });

  it("reports claims from the index and the hash with age, expiry and task count", async () => {
    const me = inbox("pl-claims");
    const now = Date.now();
    const iso = (ms: number) => new Date(ms).toISOString();
    const [expired, live, orphan] = ["expired", "live", "orphan"].map((suffix) => `${me}-${suffix}`) as [string, string, string];
    claimIds.push(expired, live, orphan);
    await redis.hset(CLAIM_KEYS.claims,
      expired, JSON.stringify({ claim_id: expired, actor_id: me, session_id: "s-old", claimed_at: iso(now - 400_000), expires_at: iso(now - 60_000), tasks: ["{}", "{}"] }),
      live, JSON.stringify({ claim_id: live, actor_id: me, session_id: "s-new", claimed_at: iso(now - 30_000), expires_at: iso(now + 600_000), tasks: ["{}"] }));
    await redis.zadd(CLAIM_KEYS.index(me), now - 60_000, expired, now + 600_000, live, now + 1_200_000, orphan);
    const listing = await list(me);
    expect(listing).toMatchObject({ unclaimed: 0, listed: 0, oldest_unclaimed_age_s: null, expired_claims: 1 });
    expect(listing.claims.map((c) => c.claim_id)).toEqual([expired, live, orphan]);
    const [old, current, missing] = listing.claims;
    expect(old).toMatchObject({ session_id: "s-old", claimed_at: iso(now - 400_000), expires_at: iso(now - 60_000), expired: true, task_count: 2 });
    expect(old!.age_s).toBeGreaterThanOrEqual(399); expect(old!.age_s).toBeLessThanOrEqual(402);
    expect(current).toMatchObject({ session_id: "s-new", expires_at: iso(now + 600_000), expired: false, task_count: 1 });
    expect(current!.age_s).toBeGreaterThanOrEqual(29); expect(current!.age_s).toBeLessThanOrEqual(32);
    expect(missing).toEqual({ claim_id: orphan, session_id: null, claimed_at: null, age_s: null, expires_at: iso(now + 1_200_000), expired: false, task_count: null });
  });

  it("reports an unparseable queue entry by its index", async () => {
    const me = inbox("pl-bad");
    await redis.rpush(SESSION_KEYS.queue(me), mail(me, { id: "first" }), "not json{", "42", mail(me, { id: "last" }));
    const listing = await list(me);
    expect(listing).toMatchObject({ unclaimed: 4, listed: 4, wake_eligible_count: 2 });
    expect(listing.messages[1]).toEqual({ raw_index: 1, error: "unparseable" });
    expect(listing.messages[2]).toEqual({ raw_index: 2, error: "unparseable" });
    expect(listing.messages.map((m) => m.id ?? null)).toEqual(["first", null, null, "last"]);
  });

  it("an empty inbox lists nothing and has no oldest age", async () => {
    const me = inbox("pl-empty");
    expect(await list(me)).toEqual({
      agent: me, source: "env", note: "name given with --name", unclaimed: 0, listed: 0, truncated: false,
      oldest_unclaimed_age_s: null, wake_eligible_count: 0, messages: [], claims: [], expired_claims: 0,
    });
  });

  it("consumes nothing: the queue, claim index and claims hash are unchanged, and only reads are sent", async () => {
    const me = inbox("pl-readonly");
    await outstandingOwner(me, "req", "peer");
    await redis.rpush(SESSION_KEYS.queue(me), mail(me, { id: "t" }), mail(me, { id: "r", type: "result", in_reply_to: "req" }), "garbage");
    const claim = `${me}-claim`;
    claimIds.push(claim);
    await redis.hset(CLAIM_KEYS.claims, claim, JSON.stringify({ claim_id: claim, session_id: "s", claimed_at: new Date().toISOString(), tasks: ["{}"] }));
    await redis.zadd(CLAIM_KEYS.index(me), Date.now() - 1_000, claim);
    const snapshot = async () => ({
      length: await redis.llen(SESSION_KEYS.queue(me)),
      queue: await redis.lrange(SESSION_KEYS.queue(me), 0, -1),
      index: await redis.zrange(CLAIM_KEYS.index(me), 0, -1, "WITHSCORES"),
      claim: await redis.hget(CLAIM_KEYS.claims, claim),
      owner: await redis.get(SESSION_KEYS.outstanding(me, "req")),
      events: await redis.exists(SESSION_KEYS.inboxEvents(me)),
    });
    const before = await snapshot();
    // Every command the CLI's own client sends (MULTI's queued commands included).
    const send = vi.spyOn(Redis.prototype, "sendCommand");
    let sent: string[];
    try {
      await list(me);
      expect((await peek(["--name", me, "--list"])).code).toBe(0);
      sent = send.mock.calls.flatMap(([command], i) => send.mock.contexts[i] === redis ? [] : [[command.name, ...command.args.slice(0, 1).map(String)].join(" ").toLowerCase()]);
    } finally { send.mockRestore(); } // mockRestore also clears the recorded calls
    expect(await snapshot()).toEqual(before);
    // ioredis's connection setup: the URL's database, client-library tags and the ready check.
    const connectionSetup = /^(select|client setinfo|info)\b/;
    const reads = ["multi", "time", "llen", "lrange", "zrange", "exec", "mget", "hmget"];
    const commands = sent.filter((line) => !connectionSetup.test(line)).map((line) => line.split(" ")[0]!);
    expect(commands.filter((name) => !reads.includes(name))).toEqual([]);
    expect(reads.filter((name) => !commands.includes(name))).toEqual([]); // every listing read was exercised
  });

  it("rejects --list with --watch and a bad --limit (exit 2)", async () => {
    for (const argv of [["--list", "--watch"], ["--watch", "--list", "--name", "x"]]) {
      const out = await peek(argv);
      expect(out.code).toBe(2);
      expect(out.stderr).toMatch(/--list cannot be combined with --watch/);
    }
    for (const bad of ["0", "-1", "1.5", "abc", "501", "1e2", ""]) {
      const out = await peek(["--name", "x", "--list", "--limit", bad]);
      expect(out.code, `--limit ${JSON.stringify(bad)}`).toBe(2);
      expect(out.stderr).toMatch(/invalid --limit/);
    }
    expect(await peek(["--name", "x", "--list", "--limit"])).toMatchObject({ code: 2, stderr: "--limit needs a value\n" });
    expect(await peek(["--name", "x", "--limit", "5"])).toMatchObject({ code: 2, stderr: "--limit needs --list\n" });
    const help = await peek(["--help"]);
    expect(help.code).toBe(0);
    expect(help.stdout).toMatch(/--list \[--name N\] \[--dir D\] \[--limit K\] \[--json\]/);
  });

  it("names the inbox like the count path: --dir, and this session's identity", async () => {
    const dir = join(home, `PL${randomUUID().slice(0, 8)}`);
    const derived = inbox("unused", resolveAgentName(dir, { ...env, GPTQ_AGENT_NAME: "" }));
    await redis.rpush(SESSION_KEYS.queue(derived), mail(derived, { id: "via-dir" }));
    const viaDir = await peek(["--dir", dir, "--list", "--json"]);
    expect(viaDir.code).toBe(0);
    expect(JSON.parse(viaDir.stdout)).toMatchObject({ agent: derived, unclaimed: 1, messages: [{ id: "via-dir" }] });

    const own = inbox("pl-session");
    await redis.rpush(SESSION_KEYS.queue(own), mail(own, { id: "via-session" }));
    const viaSession = await peek(["--list", "--json"], { GPTQ_AGENT_NAME: own });
    expect(viaSession.code).toBe(0);
    expect(JSON.parse(viaSession.stdout)).toMatchObject({ agent: own, source: "env", unclaimed: 1, messages: [{ id: "via-session" }] });
  });

  it("prints a compact table and summary lines without --json, with control characters replaced", async () => {
    const me = inbox("pl-human");
    const claim = `${me}-claim`;
    claimIds.push(claim);
    await redis.rpush(SESSION_KEYS.queue(me), mail(me, { id: "t-1", ageS: 120, content: "please\nreview" }), mail(me, { id: "st\u001b[31m", type: "status", content: "fyi" }), "garbage");
    await redis.hset(CLAIM_KEYS.claims, claim, JSON.stringify({ claim_id: claim, session_id: "s-1", claimed_at: new Date(Date.now() - 400_000).toISOString(), tasks: ["{}", "{}"] }));
    await redis.zadd(CLAIM_KEYS.index(me), Date.now() - 60_000, claim);
    const out = await peek(["--name", me, "--list"]);
    expect(out.code).toBe(0);
    const lines = out.stdout.trimEnd().split("\n");
    expect(lines[0]).toMatch(new RegExp(`^${me}: 3 unclaimed, 3 listed, 1 would wake, oldest 1[12]\\ds$`));
    expect(lines[1]).toMatch(/^#\s+AGE\s+TYPE\s+WAKES\s+FROM\s+ID\s+PREVIEW$/);
    expect(lines[2]).toMatch(/^0\s+1[12]\ds\s+task\s+yes\s+peer\s+t-1\s+please review$/);
    expect(lines[3]).toMatch(/^1\s+\d+s\s+status\s+no\s+peer\s+st\?\[31m\s+fyi$/);
    expect(lines[4]).toMatch(/^2\s+-\s+-\s+-\s+-\s+-\s+\(unparseable entry\)$/);
    expect(lines[5]).toBe("claims: 1 open, 1 expired");
    expect(lines[6]).toMatch(/^CLAIM\s+SESSION\s+AGE\s+EXPIRES\s+EXPIRED\s+TASKS$/);
    expect(lines[7]).toMatch(new RegExp(`^${claim}\\s+s-1\\s+\\d+s\\s+\\S+Z\\s+yes\\s+2$`));
    expect(out.stdout).not.toContain("\u001b");
  });

  it("reports Redis being unreachable on stderr with exit 1", async () => {
    const out = await peek(["--name", "x", "--list", "--json"], { REDIS_URL: "redis://127.0.0.1:1/15" });
    expect(out.code).toBe(1);
    expect(out.stderr).toMatch(/^gptqueue-inbox-peek: redis_unavailable: /);
    expect(out.stdout).toBe("");
  });
});
