/**
 * gptqueue-inbox-peek --watch --wakes-only (f-3009): the watcher exits only
 * for mail that would wake the recipient — the same rule --list marks with
 * "wakes" (a task, or a result/error answering the agent's own outstanding
 * request). Status mail stays unclaimed and batches at the next real wake.
 * Also: a --wakes-only watcher's pidfile satisfies the Stop hook's
 * "is a watcher running?" check, and the hook's suggested command uses
 * --wakes-only. Every test uses its own agent names and deletes only its own keys.
 */
import { testRedisUrl } from "./helpers/test-redis.js";
import { randomUUID } from "node:crypto";
import { mkdirSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Redis } from "ioredis";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { parsePeekArgs, watchInbox } from "../src/claude/peek.js";
import { wakeScan } from "../src/core/inbox-events.js";
import { SESSION_KEYS } from "../src/core/keys.js";
import { readProcess, watcherPidPath, watcherRunning, writeWatcherPid } from "../src/claude/session-record.js";
import { watchCommandFor } from "../src/claude/hook.js";
import type { QueueMessage } from "../src/core/types.js";

const REDIS_URL = testRedisUrl();
let home = "";
let redis: Redis;
const keys: string[] = [];

beforeAll(async () => {
  home = await mkdtemp(join(tmpdir(), "gptq-wakes-only-"));
  redis = new Redis(REDIS_URL);
});
afterEach(async () => { if (keys.length > 0) await redis.del(...keys.splice(0)); });
afterAll(async () => { await redis.quit(); await rm(home, { recursive: true, force: true }); });

const inbox = (prefix: string, name = `${prefix}-${randomUUID().slice(0, 8)}`): string => {
  keys.push(SESSION_KEYS.queue(name), SESSION_KEYS.inboxEvents(name), SESSION_KEYS.outstanding(name, "*"));
  return name;
};
const message = (agent: string, fields: Partial<QueueMessage>): QueueMessage =>
  ({ id: randomUUID(), from: "peer", to: agent, timestamp: new Date().toISOString(), type: "status", payload: {}, ...fields }) as QueueMessage;
const push = async (agent: string, fields: Partial<QueueMessage>): Promise<QueueMessage> => {
  const m = message(agent, fields);
  await redis.rpush(SESSION_KEYS.queue(agent), JSON.stringify(m));
  return m;
};

describe("--wakes-only argument handling", () => {
  it("requires --watch and excludes --list", () => {
    expect(parsePeekArgs(["--watch", "--wakes-only"])).toMatchObject({ watch: true, wakesOnly: true });
    expect(parsePeekArgs(["--wakes-only"])).toBe("--wakes-only needs --watch");
    // --list --wakes-only (no --watch) trips the --watch requirement first; both messages reject it.
    expect(["--wakes-only needs --watch", "--wakes-only cannot be combined with --list"]).toContain(parsePeekArgs(["--list", "--wakes-only"]));
    expect(parsePeekArgs(["--watch"])).toMatchObject({ wakesOnly: false });
  });
});

describe("wakeScan: the --list rule over the whole queue", () => {
  it("counts tasks and own-request replies, never status or foreign replies", async () => {
    const agent = inbox("scan");
    await push(agent, { type: "status" });
    await push(agent, { type: "task" });
    const request = randomUUID();
    await redis.set(SESSION_KEYS.outstanding(agent, request), "peer", "EX", 60);
    keys.push(SESSION_KEYS.outstanding(agent, request));
    await push(agent, { type: "result", payload: { in_reply_to: request } });
    await push(agent, { type: "result", payload: { in_reply_to: randomUUID() } }); // request not outstanding
    const scan = await wakeScan(redis, agent);
    expect(scan).toEqual({ unclaimed: 4, wakeEligible: 2 });
  });
});

describe("watchInbox --wakes-only", () => {
  const opts = (over: Partial<Parameters<typeof watchInbox>[2]> = {}) =>
    ({ timeoutSec: 5, blockMs: 100, wakesOnly: true, giveUpAfterMs: null, retryMs: 100, ...over });

  it("status mail alone times out without waking; a task then wakes with the eligible count", { timeout: 30_000 }, async () => {
    const agent = inbox("watch");
    const waiting = watchInbox(REDIS_URL, agent, opts());
    await push(agent, { type: "status", payload: { content: "status-only mail" } });
    // Still waiting: the status mail must not exit the watcher. Give it a few block periods.
    await new Promise((r) => setTimeout(r, 500));
    const pending = await Promise.race([waiting.then(() => "done"), new Promise((r) => setTimeout(() => r("waiting"), 200))]);
    expect(pending).toBe("waiting");
    await push(agent, { type: "task", payload: { content: "real work" } });
    const outcome = await waiting;
    expect(outcome).toEqual({ kind: "mail", pending: 2, expired: 0, wakeEligible: 1 });
  });

  it("an own-request reply wakes even when older status mail is queued first", { timeout: 30_000 }, async () => {
    const agent = inbox("reply");
    const waiting = watchInbox(REDIS_URL, agent, opts());
    await push(agent, { type: "status" });
    const request = randomUUID();
    await redis.set(SESSION_KEYS.outstanding(agent, request), "peer", "EX", 60);
    keys.push(SESSION_KEYS.outstanding(agent, request));
    await push(agent, { type: "result", from: "peer", payload: { in_reply_to: request } });
    const outcome = await waiting;
    expect(outcome).toEqual({ kind: "mail", pending: 2, expired: 0, wakeEligible: 1 });
  });

  it("the naive watcher still wakes on any mail (pending only)", { timeout: 30_000 }, async () => {
    const agent = inbox("naive");
    const waiting = watchInbox(REDIS_URL, agent, opts({ wakesOnly: false }));
    await push(agent, { type: "status" });
    const outcome = await waiting;
    expect(outcome).toEqual({ kind: "mail", pending: 1, expired: 0, wakeEligible: 1 });
  });

  // Regression (pedantic, 2026-11-10): an expired claim still wakes (COR-F3),
  // but the reported wakeEligible must be the true wake-eligible count, not
  // the raw pending count — status mail queued alongside an expired claim is
  // not wake-eligible.
  it("expired-claim wake reports the true wake-eligible count, not pending", { timeout: 30_000 }, async () => {
    const agent = inbox("expired");
    keys.push(`gptq:claims-index:${agent}`);
    // One expired claim (score far in the past) and two queued messages of
    // which only the task is wake-eligible.
    await redis.zadd(`gptq:claims-index:${agent}`, Date.now() - 60_000, "stale-claim");
    await push(agent, { type: "status" });
    await push(agent, { type: "task", payload: { content: "real work" } });
    const outcome = await watchInbox(REDIS_URL, agent, opts());
    expect(outcome).toEqual({ kind: "mail", pending: 2, expired: 1, wakeEligible: 1 });
  });

  it("an expired claim alone (no wake-eligible mail) still wakes with wakeEligible 0", { timeout: 30_000 }, async () => {
    const agent = inbox("expired-only");
    keys.push(`gptq:claims-index:${agent}`);
    await redis.zadd(`gptq:claims-index:${agent}`, Date.now() - 60_000, "stale-claim");
    await push(agent, { type: "status" }); // queued but not wake-eligible
    const outcome = await watchInbox(REDIS_URL, agent, opts());
    expect(outcome).toEqual({ kind: "mail", pending: 1, expired: 1, wakeEligible: 0 });
  });
});

describe("the Stop hook accepts a --wakes-only watcher", () => {
  it("a pidfile with mode \"wakes-only\" counts as a running watcher", () => {
    const self = readProcess(process.pid)!;
    const claude = { pid: self.ppid, start: readProcess(self.ppid)!.start };
    mkdirSync(join(home, "pid"), { recursive: true });
    const path = watcherPidPath(join(home, "pid"), "s");
    writeWatcherPid(path, { pid: self.pid, start: self.start, agent: "a", claude_pid: claude.pid, claude_start: claude.start, mode: "wakes-only" });
    expect(watcherRunning(path, "a", claude)).toBe(true);
    writeWatcherPid(path, { pid: self.pid, start: self.start, agent: "a", claude_pid: claude.pid, claude_start: claude.start }); // naive: also fine
    expect(watcherRunning(path, "a", claude)).toBe(true);
  });

  it("the hook's suggested watcher command defaults to --wakes-only", () => {
    expect(watchCommandFor("/bin/dir", "session-1")).toBe("'/bin/dir/gptqueue-inbox-peek' --watch --wakes-only --session 'session-1'");
  });
});
