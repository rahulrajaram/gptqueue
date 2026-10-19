import { randomUUID } from "node:crypto";
import { spawnSync } from "node:child_process";
import { createServer, type Socket } from "node:net";
import { existsSync } from "node:fs";
import { chmod, mkdir, mkdtemp, readFile, readdir, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it, onTestFinished } from "vitest";
import { Redis } from "ioredis";
import { RedisClient } from "../src/mcp-server/redis-client.js";
import { SESSION_KEYS, SYSTEM_SENDER } from "../src/core/keys.js";
import { TurnStateStore } from "../src/core/turn-state.js";
import { runWatchdog, WATCHDOG_KEYS, WATCHDOG_LAST_PASS, WATCHDOG_LAST_SUBSTANTIVE_PASS, WATCHDOG_PASS_LOCK, type WatchdogReport, type WatchdogResult } from "../src/core/watchdog-shell.js";
import { TASK_LEDGER_TTL_SECONDS, taskLedgerKey } from "../src/core/task-ledger.js";
import { assertNotLiveDb } from "./helpers/redis-test-utils.js";
import type { QueueMessage } from "../src/core/types.js";

const REDIS_URL = process.env.REDIS_URL || "redis://127.0.0.1:6379/15";
assertNotLiveDb(REDIS_URL, "watchdog-integration");

const OUTSTANDING_TTL_MS = 604800 * 1000;
const reportOf = (result: WatchdogResult): WatchdogReport => {
  if (result.exitCode !== 0 || !("leads" in result.output)) throw new Error(`watchdog did not report: ${JSON.stringify(result.output)}`);
  return result.output;
};
const ago = (min: number): string => new Date(Date.now() - min * 60_000).toISOString();

describe("watchdog --once against Redis db 15", () => {
  let redis: Redis;
  let dir: string;
  const clients: RedisClient[] = [];
  const names: string[] = [];
  const extraKeys: string[] = [];

  beforeAll(async () => {
    redis = new Redis(REDIS_URL);
    dir = await mkdtemp(join(tmpdir(), "gptq-watchdog-"));
  });
  afterAll(async () => {
    for (const c of clients.splice(0)) { await c.unregister().catch(() => undefined); await c.shutdown(); }
    // Every message any test name received: its outstanding owner and ledger.
    for (const name of names) {
      for (const raw of await redis.lrange(SESSION_KEYS.queue(name), 0, -1)) {
        const msg = JSON.parse(raw) as QueueMessage;
        extraKeys.push(SESSION_KEYS.outstanding(msg.from, msg.id), taskLedgerKey(msg.id));
      }
      extraKeys.push(SESSION_KEYS.queue(name), SESSION_KEYS.mailboxMeta(name), SESSION_KEYS.inboxEvents(name), SESSION_KEYS.turnState(name));
      const fired = await redis.keys(`${WATCHDOG_KEYS.fired("")}*:${name}:*`);
      extraKeys.push(...fired);
    }
    if (extraKeys.length > 0) await redis.del(...extraKeys);
    redis.disconnect();
    await rm(dir, { recursive: true, force: true });
  });

  const name = (prefix: string): string => { const n = `${prefix}-${randomUUID().slice(0, 8)}`; names.push(n); return n; };

  /** A lead that sent a task `ageMin` minutes ago to `to`, then went idle. */
  const leadWaitingOn = async (to: string, ageMin: number, content = "please review the plan\nsecond line") => {
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const lead = name("wd-lead");
    await client.register("both", lead);
    const id = randomUUID();
    expect(await client.sendMessage({ id, from: lead, to, timestamp: new Date().toISOString(), type: "task", payload: { content } })).toBe(true);
    // Age the request: send time is derived from the remaining TTL.
    await redis.pexpire(SESSION_KEYS.outstanding(lead, id), OUTSTANDING_TTL_MS - ageMin * 60_000);
    await new TurnStateStore(redis).report(lead, { phase: "idle", at: ago(ageMin), runtime: "codex", session_id: randomUUID() });
    return { lead, id };
  };

  const writeConfig = async (value: unknown): Promise<string> => {
    const path = join(dir, `${randomUUID()}.json`);
    await writeFile(path, typeof value === "string" ? value : JSON.stringify(value));
    return path;
  };
  const config = (leads: string[], root: string, operator: string | null) => ({
    schema_version: 1, root, leads, operator, thresholds_min: { waiting: 10, escalate_after: 10, idle: 15 }, max_actions_per_run: 20,
  });

  it("reminds root once across two runs, as a SYSTEM_SENDER task with an inbox event", async () => {
    const root = name("wd-root");
    const { lead, id } = await leadWaitingOn(root, 20);
    const configPath = await writeConfig(config([lead], root, null));
    const stateHome = join(dir, "state-a");

    const first = await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts") });
    expect(first.exitCode).toBe(0);
    expect(first.output).toMatchObject({ dry_run: false, deferred: 0 });
    expect(reportOf(first).leads).toEqual([expect.objectContaining({ lead, sw4rm_state: "WAITING", state: expect.objectContaining({ kind: "waiting", on: root, for: id, subject: "please review the plan second line" }) })]);
    expect(reportOf(first).actions).toEqual([expect.objectContaining({ kind: "remind", rule: 1, to: root, fire_key: `remind:${lead}:${id}` })]);

    const second = await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts") });
    expect(second.exitCode).toBe(0);
    expect(reportOf(second).actions).toEqual([]);

    const queue = (await redis.lrange(SESSION_KEYS.queue(root), 0, -1)).map((raw) => JSON.parse(raw) as QueueMessage);
    const reminders = queue.filter((m) => m.from === SYSTEM_SENDER);
    expect(reminders).toHaveLength(1);
    expect(reminders[0]).toMatchObject({ type: "task", to: root });
    expect(reminders[0]!.payload.content).toContain(JSON.stringify(id));

    const events = await redis.xrange(SESSION_KEYS.inboxEvents(root), "-", "+");
    const fields = events.map(([, flat]) => Object.fromEntries(flat.flatMap((v, i) => (i % 2 === 0 ? [[v, flat[i + 1]]] : []))));
    expect(fields).toContainEqual(expect.objectContaining({ message_id: reminders[0]!.id, type: "task", from: SYSTEM_SENDER }));

    const fired = JSON.parse((await redis.get(WATCHDOG_KEYS.fired(`remind:${lead}:${id}`)))!);
    expect(Date.parse(fired.fired_at)).not.toBeNaN();
    expect(await redis.ttl(WATCHDOG_KEYS.fired(`remind:${lead}:${id}`))).toBeGreaterThan(6 * 86400);
  });

  it("--dry-run reports the plan and writes nothing", async () => {
    const root = name("wd-root");
    const operator = name("wd-op");
    const { lead, id } = await leadWaitingOn(root, 20);
    const asker = await leadWaitingOn(operator, 1);
    const configPath = await writeConfig(config([lead, asker.lead], root, operator));
    const stateHome = join(dir, "state-dry");
    const before = await redis.llen(SESSION_KEYS.queue(root));

    const result = await runWatchdog({ configPath, dryRun: true, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts") });
    expect(result.exitCode).toBe(0);
    expect(reportOf(result).dry_run).toBe(true);
    expect(reportOf(result).actions.map((a) => a.fire_key).sort()).toEqual([`escalate:${asker.lead}:${asker.id}`, `remind:${lead}:${id}`].sort());

    expect(await redis.llen(SESSION_KEYS.queue(root))).toBe(before);
    expect(await redis.llen(SESSION_KEYS.queue(operator))).toBe(1);
    expect(await redis.exists(WATCHDOG_KEYS.fired(`remind:${lead}:${id}`), WATCHDOG_KEYS.fired(`escalate:${asker.lead}:${asker.id}`))).toBe(0);
    await expect(stat(stateHome)).rejects.toThrow();
  });

  it("appends escalations to the JSONL log and the operator mailbox, once", async () => {
    const root = name("wd-root");
    const operator = name("wd-op");
    const { lead, id } = await leadWaitingOn(operator, 1, "may I delete the old branch?");
    const configPath = await writeConfig(config([lead], root, operator));
    const stateHome = join(dir, "state-esc");

    expect((await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts") })).exitCode).toBe(0);
    expect((await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts") })).exitCode).toBe(0);

    const lines = (await readFile(join(stateHome, "gptqueue", "watchdog", "escalations.jsonl"), "utf8")).trim().split("\n").map((l) => JSON.parse(l));
    expect(lines).toEqual([expect.objectContaining({ rule: 3, lead, task_id: id, fire_key: `escalate:${lead}:${id}`, question: "may I delete the old branch?", operator_alerts: "disabled", operator_mailbox: "ok" })]);
    const toOperator = (await redis.lrange(SESSION_KEYS.queue(operator), 0, -1)).map((raw) => JSON.parse(raw) as QueueMessage).filter((m) => m.from === SYSTEM_SENDER);
    expect(toOperator).toEqual([expect.objectContaining({ type: "task" })]);
  });

  it("delivers escalations through the operator alert sink: dry_run outbox, address redacted everywhere", async () => {
    const root = name("wd-root");
    const { lead, id } = await leadWaitingOn("operator-mailbox-unused", 1, "may I rotate the logs?");
    const configPath = await writeConfig(config([lead], root, "operator-mailbox-unused"));
    names.push("operator-mailbox-unused");
    const stateHome = join(dir, "state-alerts");
    const configHome = join(dir, "cfg-alerts");
    await mkdir(join(configHome, "gptqueue"), { recursive: true });
    const alertsPath = join(configHome, "gptqueue", "operator-alerts.json");
    await writeFile(alertsPath, JSON.stringify({ schema_version: 1, enabled: true, mode: "dry_run", transport: { kind: "direct_mx" },
      to: "operator@example.invalid", from: "watchdog@example.invalid" }));
    await chmod(alertsPath, 0o600);

    const result = await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome });
    expect(result.exitCode).toBe(0);
    expect(JSON.stringify(result.output)).not.toContain("operator@example.invalid");
    const logText = await readFile(join(stateHome, "gptqueue", "watchdog", "escalations.jsonl"), "utf8");
    expect(logText).not.toContain("operator@example.invalid");
    const outbox = join(stateHome, "gptqueue", "watchdog", "outbox");
    const [line] = logText.trim().split("\n").map((l) => JSON.parse(l));
    expect(line).toMatchObject({ rule: 3, lead, task_id: id, operator_alerts: "outboxed", outbox: [join(outbox, `escalate_${lead}_${id}.eml`)] });
    expect(await readdir(outbox)).toEqual([`escalate_${lead}_${id}.eml`]);
    const eml = await readFile(join(outbox, `escalate_${lead}_${id}.eml`), "utf8");
    expect(eml).toContain("To: <operator>");
    expect(eml).toContain("may I rotate the logs?");
    expect(eml).not.toContain("operator@example.invalid");
  });

  it("records a refused alerts config (not 0600) in the log and still escalates", async () => {
    const root = name("wd-root");
    const operator = name("wd-op");
    const { lead } = await leadWaitingOn(operator, 1);
    const configPath = await writeConfig(config([lead], root, operator));
    const stateHome = join(dir, "state-loose");
    const configHome = join(dir, "cfg-loose");
    await mkdir(join(configHome, "gptqueue"), { recursive: true });
    await writeFile(join(configHome, "gptqueue", "operator-alerts.json"), JSON.stringify({ enabled: true }));
    await chmod(join(configHome, "gptqueue", "operator-alerts.json"), 0o644);
    const result = await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome });
    expect(reportOf(result).operator_alerts).toEqual({ status: "refused", error: "alerts_config_mode" });
    const [line] = (await readFile(join(stateHome, "gptqueue", "watchdog", "escalations.jsonl"), "utf8")).trim().split("\n").map((l) => JSON.parse(l));
    expect(line).toMatchObject({ operator_alerts: "refused:alerts_config_mode" });
  });

  const peer = async (prefix: string) => {
    const client = new RedisClient(null, REDIS_URL);
    clients.push(client);
    const peerName = name(prefix);
    await client.register("both", peerName);
    const reply = async (to: string, inReplyTo: string) => {
      extraKeys.push(SESSION_KEYS.taskAnswered(inReplyTo));
      expect(await client.sendMessage({ id: randomUUID(), from: peerName, to, timestamp: new Date().toISOString(), type: "result", payload: { content: "done", in_reply_to: inReplyTo } })).toBe(true);
    };
    return { name: peerName, reply };
  };

  it("F1: a task answered through the real send path is not waiting, and nothing is sent", async () => {
    const root = name("wd-root");
    const worker = await peer("wd-worker");
    const { lead, id } = await leadWaitingOn(worker.name, 12);
    await worker.reply(lead, id);
    expect(await redis.get(SESSION_KEYS.taskAnswered(id))).not.toBeNull();
    const result = await runWatchdog({ configPath: await writeConfig(config([lead], root, null)), dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-answered"), configHome: join(dir, "no-alerts") });
    const out = reportOf(result);
    expect(out.leads).toEqual([expect.objectContaining({ lead, state: expect.objectContaining({ kind: "idle" }) })]);
    expect(out.actions).toEqual([]);
    expect(await redis.llen(SESSION_KEYS.queue(worker.name))).toBe(1);
  });

  it("F1: a reply from a non-owner does not mark the task answered", async () => {
    const root = name("wd-root");
    const worker = await peer("wd-worker");
    const stranger = await peer("wd-stranger");
    const { lead, id } = await leadWaitingOn(worker.name, 12);
    await stranger.reply(lead, id);
    expect(await redis.exists(SESSION_KEYS.taskAnswered(id))).toBe(0);
    const out = reportOf(await runWatchdog({ configPath: await writeConfig(config([lead], root, null)), dryRun: true, redisUrl: REDIS_URL, stateHome: join(dir, "state-stranger"), configHome: join(dir, "no-alerts") }));
    expect(out.leads[0]!.state).toMatchObject({ kind: "waiting", for: id });
  });

  it("F1: a ledger-less request is uncorrelated: counted, not acted on", async () => {
    const root = name("wd-root");
    const worker = await peer("wd-worker");
    const { lead, id } = await leadWaitingOn(worker.name, 12);
    await redis.del(taskLedgerKey(id));
    const out = reportOf(await runWatchdog({ configPath: await writeConfig(config([lead], root, null)), dryRun: true, redisUrl: REDIS_URL, stateHome: join(dir, "state-uncorrelated"), configHome: join(dir, "no-alerts") }));
    expect(out.uncorrelated).toBe(1);
    expect(out.leads[0]!.state.kind).toBe("idle");
    expect(out.actions).toEqual([]);
  });

  // Fillers are tasks: under waking admission only waking mail fills a task's quota.
  it("F3: a full operator mailbox is a recorded partial success; a second run duplicates nothing", async () => {
    const root = name("wd-root");
    const operator = name("wd-op");
    const { lead, id } = await leadWaitingOn(operator, 1, "may I purge the cache?");
    await redis.rpush(SESSION_KEYS.queue(operator), ...Array.from({ length: 20 }, () => JSON.stringify({ id: randomUUID(), from: "filler", to: operator, type: "task", timestamp: new Date().toISOString(), payload: { content: "x" } })));
    const stateHome = join(dir, "state-full");
    const configHome = join(dir, "cfg-full");
    await mkdir(join(configHome, "gptqueue"), { recursive: true });
    const alertsPath = join(configHome, "gptqueue", "operator-alerts.json");
    await writeFile(alertsPath, JSON.stringify({ schema_version: 1, enabled: true, mode: "dry_run", transport: { kind: "direct_mx" }, to: "operator@example.invalid", from: "watchdog@example.invalid" }));
    await chmod(alertsPath, 0o600);
    const configPath = await writeConfig(config([lead], root, operator));

    const first = reportOf(await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome }));
    expect(first.actions).toEqual([expect.objectContaining({ kind: "escalate", status: "done" })]);
    reportOf(await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome }));

    const lines = (await readFile(join(stateHome, "gptqueue", "watchdog", "escalations.jsonl"), "utf8")).trim().split("\n").map((l) => JSON.parse(l));
    expect(lines).toEqual([expect.objectContaining({ fire_key: `escalate:${lead}:${id}`, operator_mailbox: "full", operator_alerts: "outboxed" })]);
    expect(await readdir(join(stateHome, "gptqueue", "watchdog", "outbox"))).toHaveLength(1);
  }, 30_000);

  const systemTasksTo = async (mailbox: string): Promise<QueueMessage[]> =>
    (await redis.lrange(SESSION_KEYS.queue(mailbox), 0, -1)).map((raw) => JSON.parse(raw) as QueueMessage).filter((m) => m.from === SYSTEM_SENDER);

  it("S4: two concurrent passes produce exactly one effect; the other is skipped", async () => {
    const root = name("wd-root");
    const { lead } = await leadWaitingOn(root, 20);
    const configPath = await writeConfig(config([lead], root, null));
    const run = () => runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-concurrent"), configHome: join(dir, "no-alerts") });
    const results = await Promise.all([run(), run()]);
    expect(results.map((r) => r.exitCode)).toEqual([0, 0]);
    expect(results.filter((r) => "skipped" in r.output).map((r) => r.output)).toEqual([{ skipped: "pass_in_progress" }]);
    expect(await systemTasksTo(root)).toHaveLength(1);
    expect(await redis.exists(WATCHDOG_PASS_LOCK)).toBe(0);
  });

  it("S4: a pending fire key left by a crash suppresses the action until it expires, then the action retries", async () => {
    const root = name("wd-root");
    const { lead, id } = await leadWaitingOn(root, 20);
    const configPath = await writeConfig(config([lead], root, null));
    const fireKey = WATCHDOG_KEYS.fired(`remind:${lead}:${id}`);
    await redis.set(fireKey, `pending:${new Date().toISOString()}`, "PX", 400);
    const run = () => runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-crash"), configHome: join(dir, "no-alerts") });
    expect(reportOf(await run()).actions).toEqual([]);
    expect(await systemTasksTo(root)).toHaveLength(0);
    await new Promise((r) => setTimeout(r, 500));
    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ fire_key: `remind:${lead}:${id}`, status: "done" })]);
    expect(await systemTasksTo(root)).toHaveLength(1);
    expect(JSON.parse((await redis.get(fireKey))!)).toMatchObject({ rule: 1 });
  });

  it("S4: a crash after delivery but before finalize does not resend after the claim expires", async () => {
    const root = name("wd-root");
    const { lead, id } = await leadWaitingOn(root, 20);
    const configPath = await writeConfig(config([lead], root, null));
    const fireKey = WATCHDOG_KEYS.fired(`remind:${lead}:${id}`);
    const run = () => runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-crash-after"), configHome: join(dir, "no-alerts") });
    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ status: "done" })]);
    // Simulate the crash: the effect happened, finalize did not; the pending claim then expires.
    await redis.set(fireKey, `pending:${new Date().toISOString()}`, "PX", 900_000);
    await redis.pexpire(fireKey, 1);
    await new Promise((r) => setTimeout(r, 20));
    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ fire_key: `remind:${lead}:${id}`, status: "done" })]);
    expect(await systemTasksTo(root)).toHaveLength(1);
    extraKeys.push(SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, root, `watchdog:remind:${lead}:${id}`));
  });

  it("S6: when the sink cannot persist, the escalation fails whole and the next pass delivers it exactly once", async () => {
    const root = name("wd-root");
    const operator = name("wd-op");
    const { lead, id } = await leadWaitingOn(operator, 1, "may I restart the worker?");
    const configPath = await writeConfig(config([lead], root, operator));
    const stateHome = join(dir, "state-savefail");
    const configHome = join(dir, "cfg-savefail");
    await mkdir(join(configHome, "gptqueue"), { recursive: true });
    const alertsPath = join(configHome, "gptqueue", "operator-alerts.json");
    await writeFile(alertsPath, JSON.stringify({ schema_version: 1, enabled: true, mode: "dry_run", transport: { kind: "direct_mx" }, to: "operator@example.invalid", from: "watchdog@example.invalid" }));
    await chmod(alertsPath, 0o600);
    // A directory where the held-state file belongs makes the state save fail once.
    const statePath = join(stateHome, "gptqueue", "watchdog", "operator-alerts-state.json");
    await mkdir(statePath, { recursive: true });
    const log = join(stateHome, "gptqueue", "watchdog", "escalations.jsonl");
    const run = () => runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome });

    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ kind: "escalate", status: "failed" })]);
    await expect(stat(log)).rejects.toThrow();
    expect(await redis.exists(WATCHDOG_KEYS.fired(`escalate:${lead}:${id}`))).toBe(0);

    await rm(statePath, { recursive: true });
    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ kind: "escalate", status: "done" })]);
    expect(reportOf(await run()).actions).toEqual([]);
    expect((await readFile(log, "utf8")).trim().split("\n").map((l) => JSON.parse(l))).toEqual([expect.objectContaining({ fire_key: `escalate:${lead}:${id}`, operator_alerts: "outboxed" })]);
    expect(await readdir(join(stateHome, "gptqueue", "watchdog", "outbox"))).toEqual([`escalate_${lead}_${id}.eml`]);
    expect(await systemTasksTo(operator)).toHaveLength(1);
    extraKeys.push(SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, operator, `watchdog:escalate:${lead}:${id}`));
  });

  it("R1: log append failing twice, then succeeding, yields one emission, one artifact, unchanged budget and one line per fire_key", async () => {
    const root = name("wd-root");
    const operator = name("wd-op");
    const { lead, id } = await leadWaitingOn(operator, 1, "may I rotate the keys?");
    const configPath = await writeConfig(config([lead], root, operator));
    const stateHome = join(dir, "state-logfail");
    const configHome = join(dir, "cfg-logfail");
    await mkdir(join(configHome, "gptqueue"), { recursive: true });
    const alertsPath = join(configHome, "gptqueue", "operator-alerts.json");
    await writeFile(alertsPath, JSON.stringify({ schema_version: 1, enabled: true, mode: "dry_run", transport: { kind: "direct_mx" }, to: "operator@example.invalid", from: "watchdog@example.invalid" }));
    await chmod(alertsPath, 0o600);
    const watchdogDir = join(stateHome, "gptqueue", "watchdog");
    const log = join(watchdogDir, "escalations.jsonl");
    await mkdir(log, { recursive: true }); // a directory where the log belongs: appends fail
    const run = () => runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome });
    const fireKey = `escalate:${lead}:${id}`;
    extraKeys.push(SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, operator, `watchdog:${fireKey}`));

    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ status: "failed" })]);
    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ status: "failed" })]);
    await rm(log, { recursive: true });
    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ status: "done" })]);

    expect(await readdir(join(watchdogDir, "outbox"))).toEqual([`escalate_${lead}_${id}.eml`]);
    const sinkState = JSON.parse(await readFile(join(watchdogDir, "operator-alerts-state.json"), "utf8")) as { sent_at: string[]; held: unknown[]; emitted: Array<{ id: string }> };
    expect(sinkState.sent_at).toHaveLength(1);
    expect(sinkState.held).toEqual([]);
    expect(sinkState.emitted.map((e) => e.id)).toEqual([fireKey]);
    const lines = (await readFile(log, "utf8")).trim().split("\n").map((l) => JSON.parse(l) as { fire_key: string; operator_alerts: string });
    expect(lines).toEqual([expect.objectContaining({ fire_key: fireKey, operator_alerts: "already_emitted" })]);

    // Finalize lost after the append: the retry's guard sees the same fire_key on the last line.
    await redis.del(WATCHDOG_KEYS.fired(fireKey));
    expect(reportOf(await run()).actions).toEqual([expect.objectContaining({ status: "done" })]);
    expect((await readFile(log, "utf8")).trim().split("\n")).toHaveLength(1);
    expect(await systemTasksTo(operator)).toHaveLength(1);
  });

  const escalatingLead = async (stateTag: string) => {
    const root = name("wd-root");
    const operator = name("wd-op");
    const { lead, id } = await leadWaitingOn(operator, 1);
    extraKeys.push(SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, operator, `watchdog:escalate:${lead}:${id}`));
    const stateHome = join(dir, stateTag);
    const watchdogDir = join(stateHome, "gptqueue", "watchdog");
    await mkdir(join(watchdogDir, "outbox"), { recursive: true });
    return { configPath: await writeConfig(config([lead], root, operator)), stateHome, watchdogDir, lead, id };
  };
  const DAY = 86_400_000;
  const today = (): string => new Date().toISOString().slice(0, 10);

  it("W2: rotates the escalation log by size, with a numeric suffix on collision", async () => {
    const { configPath, stateHome, watchdogDir, lead, id } = await escalatingLead("state-rot-size");
    const log = join(watchdogDir, "escalations.jsonl");
    await writeFile(log, `${JSON.stringify({ at: new Date().toISOString(), fire_key: "old" })}\n`);
    await writeFile(join(watchdogDir, `escalations-${today()}.jsonl`), "taken\n");
    const result = await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts"),
      retention: { maxLogBytes: 10, maxLogAgeMs: 30 * DAY, pruneAgeMs: 30 * DAY } });
    expect(reportOf(result).actions).toEqual([expect.objectContaining({ status: "done" })]);
    expect(await readFile(join(watchdogDir, `escalations-${today()}-1.jsonl`), "utf8")).toContain('"fire_key":"old"');
    expect(await readFile(join(watchdogDir, `escalations-${today()}.jsonl`), "utf8")).toBe("taken\n");
    const lines = (await readFile(log, "utf8")).trim().split("\n").map((l) => JSON.parse(l) as { fire_key: string });
    expect(lines.map((l) => l.fire_key)).toEqual([`escalate:${lead}:${id}`]);
  });

  it("W2: rotates the escalation log when its first line is older than the age limit", async () => {
    const { configPath, stateHome, watchdogDir } = await escalatingLead("state-rot-age");
    const log = join(watchdogDir, "escalations.jsonl");
    await writeFile(log, `${JSON.stringify({ at: new Date(Date.now() - 31 * DAY).toISOString(), fire_key: "ancient" })}\n`);
    reportOf(await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts") }));
    expect(await readFile(join(watchdogDir, `escalations-${today()}.jsonl`), "utf8")).toContain("ancient");
    expect(await readFile(log, "utf8")).not.toContain("ancient");
  });

  it("W2: a real pass prunes rotated logs and outbox files older than 30 days; a dry run prunes nothing", async () => {
    const stateHome = join(dir, "state-prune");
    const watchdogDir = join(stateHome, "gptqueue", "watchdog");
    await mkdir(join(watchdogDir, "outbox"), { recursive: true });
    const old = new Date(Date.now() - 31 * DAY);
    const files = {
      oldLog: join(watchdogDir, "escalations-2020-01-01.jsonl"), newLog: join(watchdogDir, "escalations-2030-01-01.jsonl"),
      oldEml: join(watchdogDir, "outbox", "old.eml"), newEml: join(watchdogDir, "outbox", "new.eml"), live: join(watchdogDir, "escalations.jsonl"),
    };
    for (const f of Object.values(files)) await writeFile(f, "x\n");
    for (const f of [files.oldLog, files.oldEml, files.live]) await utimes(f, old, old);
    const configPath = await writeConfig(config([name("wd-nobody")], name("wd-root"), null));
    reportOf(await runWatchdog({ configPath, dryRun: true, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts") }));
    expect(await readdir(join(watchdogDir, "outbox"))).toHaveLength(2);
    reportOf(await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome, configHome: join(dir, "no-alerts") }));
    expect((await readdir(watchdogDir)).sort()).toEqual(["escalations-2030-01-01.jsonl", "escalations.jsonl", "outbox"]);
    expect(await readdir(join(watchdogDir, "outbox"))).toEqual(["new.eml"]);
  });

  it("W3: the 7th reminder to one recipient within an hour is rate limited, unclaimed, and sent after the window", async () => {
    const target = name("wd-busy");
    extraKeys.push(`gptq:watchdog:sent:${target}`);
    const waits = [];
    for (let i = 0; i < 7; i += 1) waits.push(await leadWaitingOn(target, 20 + i));
    const configPath = await writeConfig(config(waits.map((w) => w.lead), name("wd-root"), null));
    const t0 = Date.now();
    // The recipient holds 7 tasks plus up to 7 reminders: lift the default mailbox bound (10) for the watchdog's sends.
    const bound = process.env.GPTQ_QUEUE_BOUND;
    process.env.GPTQ_QUEUE_BOUND = "50";
    onTestFinished(() => { if (bound === undefined) delete process.env.GPTQ_QUEUE_BOUND; else process.env.GPTQ_QUEUE_BOUND = bound; });
    const run = (offsetMs: number) => runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-rate"),
      configHome: join(dir, "no-alerts"), now: () => new Date(t0 + offsetMs) });

    const first = reportOf(await run(0));
    expect(first.rate_limited).toBe(1);
    const limited = first.actions.filter((a) => a.status === "rate_limited");
    expect(limited).toHaveLength(1);
    expect(await redis.exists(WATCHDOG_KEYS.fired(limited[0]!.fire_key))).toBe(0);
    expect(await systemTasksTo(target)).toHaveLength(6);
    const ttl = await redis.ttl(`gptq:watchdog:sent:${target}`);
    expect(ttl).toBeGreaterThan(3600);
    expect(ttl).toBeLessThanOrEqual(7200);

    expect(reportOf(await run(30 * 60_000)).rate_limited).toBe(1);
    expect(await systemTasksTo(target)).toHaveLength(6);
    // The window is counted by Redis TIME (X3): age the six entries past the hour instead of moving the pass clock.
    const sentKey = WATCHDOG_KEYS.sent(target);
    const entries = await redis.zrange(sentKey, 0, -1, "WITHSCORES");
    for (let i = 0; i < entries.length; i += 2) await redis.zadd(sentKey, Number(entries[i + 1]) - 3_600_001, entries[i]!);
    const later = reportOf(await run(61 * 60_000));
    expect(later.actions).toContainEqual(expect.objectContaining({ fire_key: limited[0]!.fire_key, status: "done" }));
    expect(later.rate_limited).toBe(0);
    expect(await systemTasksTo(target)).toHaveLength(7);
    for (const w of waits) extraKeys.push(SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, target, `watchdog:remind:${w.lead}:${w.id}`));
  }, 30_000);

  it("W1: a failed send frees its slot", async () => {
    const target = name("wd-full");
    extraKeys.push(WATCHDOG_KEYS.sent(target));
    const { lead, id } = await leadWaitingOn(target, 20);
    await redis.rpush(SESSION_KEYS.queue(target), ...Array.from({ length: 20 }, () => JSON.stringify({ id: randomUUID(), from: "filler", to: target, type: "task", timestamp: new Date().toISOString(), payload: { content: "x" } })));
    const configPath = await writeConfig({ ...config([lead], name("wd-root"), null), max_reminders_per_recipient_per_hour: 1 });
    const out = reportOf(await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-w1-full"), configHome: join(dir, "no-alerts") }));
    expect(out.actions).toEqual([expect.objectContaining({ fire_key: `remind:${lead}:${id}`, status: "failed" })]);
    expect(await redis.zcard(WATCHDOG_KEYS.sent(target))).toBe(0);
    expect(await redis.exists(WATCHDOG_KEYS.fired(`remind:${lead}:${id}`))).toBe(0);
  }, 30_000);

  it("X3: a slow pass (stale pass clock) cannot let a 7th reminder into the hour: the script counts by Redis TIME", async () => {
    const target = name("wd-slow");
    extraKeys.push(WATCHDOG_KEYS.sent(target));
    const waits = [];
    for (let i = 0; i < 7; i += 1) waits.push(await leadWaitingOn(target, 20 + i));
    for (const w of waits) extraKeys.push(SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, target, `watchdog:remind:${w.lead}:${w.id}`));
    const configPath = await writeConfig(config(waits.map((w) => w.lead), name("wd-root"), null));
    const bound = process.env.GPTQ_QUEUE_BOUND;
    process.env.GPTQ_QUEUE_BOUND = "50";
    onTestFinished(() => { if (bound === undefined) delete process.env.GPTQ_QUEUE_BOUND; else process.env.GPTQ_QUEUE_BOUND = bound; });
    const run = (offsetMs: number) => runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-x3"),
      configHome: join(dir, "no-alerts"), now: () => new Date(Date.now() + offsetMs) });

    const first = reportOf(await run(0));
    expect(first.rate_limited).toBe(1);
    expect(await systemTasksTo(target)).toHaveLength(6);
    // A pass whose clock runs two hours ahead plans the 7th as eligible (its read-only count sees an empty hour),
    // but the send script trims by Redis TIME and refuses it; the claim is released for a later pass.
    const skewed = reportOf(await run(2 * 3_600_000));
    const seventh = skewed.actions.find((a) => a.kind === "remind" && a.status !== "skipped");
    expect(seventh).toMatchObject({ status: "rate_limited" });
    expect(await redis.exists(WATCHDOG_KEYS.fired(seventh!.fire_key))).toBe(0);
    expect(await systemTasksTo(target)).toHaveLength(6);
    expect(await redis.zcard(WATCHDOG_KEYS.sent(target))).toBe(6);
  }, 30_000);

  it("X2: a send that enqueues and then throws (lost ack) keeps the slot and the claim; the retry does not duplicate", async () => {
    const target = name("wd-ack");
    extraKeys.push(WATCHDOG_KEYS.sent(target));
    const { lead, id } = await leadWaitingOn(target, 20);
    const fireKey = `remind:${lead}:${id}`;
    extraKeys.push(SESSION_KEYS.idempotencyFor(SYSTEM_SENDER, target, `watchdog:${fireKey}`));
    const configPath = await writeConfig({ ...config([lead], name("wd-root"), null), max_reminders_per_recipient_per_hour: 1 });
    const run = (lose: boolean) => runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-x2"), configHome: join(dir, "no-alerts"),
      faults: lose ? { afterSend: () => { throw new Error("connection lost after enqueue"); } } : undefined });
    expect(reportOf(await run(true)).actions).toEqual([expect.objectContaining({ fire_key: fireKey, status: "failed" })]);
    expect(await systemTasksTo(target)).toHaveLength(1);
    expect(await redis.zscore(WATCHDOG_KEYS.sent(target), fireKey)).not.toBeNull();
    expect(await redis.get(WATCHDOG_KEYS.fired(fireKey))).toMatch(/^pending:/u);
    // The pending claim expires; the retry reuses the reservation and the idempotent send returns the original.
    await redis.pexpire(WATCHDOG_KEYS.fired(fireKey), 1);
    await new Promise((r) => setTimeout(r, 20));
    expect(reportOf(await run(false)).actions).toEqual([expect.objectContaining({ fire_key: fireKey, status: "done" })]);
    expect(await systemTasksTo(target)).toHaveLength(1);
    expect(await redis.zcard(WATCHDOG_KEYS.sent(target))).toBe(1);
  });

  it("health: a config failure is recorded in both status records when Redis is reachable", async () => {
    extraKeys.push(WATCHDOG_LAST_PASS, WATCHDOG_LAST_SUBSTANTIVE_PASS);
    const missing = await runWatchdog({ configPath: join(dir, `absent-${randomUUID()}.json`), dryRun: false, redisUrl: REDIS_URL, stateHome: dir, configHome: join(dir, "no-alerts") });
    expect(missing.exitCode).toBe(2);
    expect(JSON.parse((await redis.get(WATCHDOG_LAST_SUBSTANTIVE_PASS))!)).toMatchObject({ exit: 2, error: "config_missing" });
    expect(JSON.parse((await redis.get(WATCHDOG_LAST_PASS))!)).toMatchObject({ exit: 2, error: "config_missing", skipped: false });
    await runWatchdog({ configPath: await writeConfig("{bad"), dryRun: false, redisUrl: REDIS_URL, stateHome: dir, configHome: join(dir, "no-alerts") });
    expect(JSON.parse((await redis.get(WATCHDOG_LAST_SUBSTANTIVE_PASS))!)).toMatchObject({ exit: 2, error: "config_invalid" });
    // A dry run with a bad config records nothing.
    await redis.del(WATCHDOG_LAST_PASS);
    await runWatchdog({ configPath: await writeConfig("{bad"), dryRun: true, redisUrl: REDIS_URL, stateHome: dir, configHome: join(dir, "no-alerts") });
    expect(await redis.exists(WATCHDOG_LAST_PASS)).toBe(0);
  });

  it("W6: a real pass records gptq:watchdog:last-pass with a 7-day TTL; a dry run does not", async () => {
    await redis.del(WATCHDOG_LAST_PASS);
    extraKeys.push(WATCHDOG_LAST_PASS);
    const configPath = await writeConfig(config([name("wd-nobody")], name("wd-root"), null));
    reportOf(await runWatchdog({ configPath, dryRun: true, redisUrl: REDIS_URL, stateHome: join(dir, "state-last"), configHome: join(dir, "no-alerts") }));
    expect(await redis.exists(WATCHDOG_LAST_PASS)).toBe(0);
    extraKeys.push(WATCHDOG_LAST_SUBSTANTIVE_PASS);
    reportOf(await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-last"), configHome: join(dir, "no-alerts") }));
    expect(JSON.parse((await redis.get(WATCHDOG_LAST_PASS))!)).toMatchObject({ exit: 0, leads: 1, actions: 0, rate_limited: 0, deferred: 0, uncorrelated: 0, skipped: false });
    expect(JSON.parse((await redis.get(WATCHDOG_LAST_SUBSTANTIVE_PASS))!)).toEqual(expect.objectContaining({ exit: 0, leads: 1, actions: 0, failed: 0, rate_limited: 0, deferred: 0, uncorrelated: 0 }));
    expect(await redis.ttl(WATCHDOG_LAST_SUBSTANTIVE_PASS)).toBeGreaterThan(6 * 86400);
    expect(await redis.ttl(WATCHDOG_LAST_PASS)).toBeGreaterThan(6 * 86400);
    // A skipped pass (lock held) is recorded as skipped.
    await redis.set(WATCHDOG_PASS_LOCK, "someone-else", "PX", 5_000);
    try {
      const before = await redis.get(WATCHDOG_LAST_SUBSTANTIVE_PASS);
      await runWatchdog({ configPath, dryRun: false, redisUrl: REDIS_URL, stateHome: join(dir, "state-last"), configHome: join(dir, "no-alerts") });
      expect(JSON.parse((await redis.get(WATCHDOG_LAST_PASS))!)).toMatchObject({ exit: 0, skipped: true });
      // W3: a skip never replaces the substantive record that --status judges.
      expect(await redis.get(WATCHDOG_LAST_SUBSTANTIVE_PASS)).toBe(before);
    } finally {
      await redis.del(WATCHDOG_PASS_LOCK);
    }
  });

  it("S7: a Redis that goes silent after connect ends the pass at its deadline with exit 1", async () => {
    const sockets: Socket[] = [];
    const silent = createServer((socket) => { sockets.push(socket); });
    await new Promise<void>((r) => silent.listen(0, "127.0.0.1", r));
    const port = (silent.address() as { port: number }).port;
    try {
      const started = Date.now();
      const result = await runWatchdog({ configPath: await writeConfig(config(["x"], "r", null)), dryRun: false, redisUrl: `redis://127.0.0.1:${port}/15`,
        stateHome: join(dir, "state-silent"), configHome: join(dir, "no-alerts"), passDeadlineMs: 1_500 });
      expect(result).toMatchObject({ exitCode: 1, output: { error: "pass_deadline" } });
      expect(Date.now() - started).toBeLessThan(3_000);
    } finally {
      for (const s of sockets) s.destroy();
      await new Promise((r) => silent.close(r));
    }
  });

  it("exits 2 on a missing or invalid config without touching Redis, and 1 when Redis is unavailable", async () => {
    const missing = await runWatchdog({ configPath: join(dir, "absent.json"), dryRun: false, redisUrl: "redis://127.0.0.1:1/15", stateHome: dir, configHome: join(dir, "no-alerts") });
    expect(missing).toMatchObject({ exitCode: 2, output: { error: "config_missing" } });
    const invalid = await runWatchdog({ configPath: await writeConfig("{not json"), dryRun: false, redisUrl: "redis://127.0.0.1:1/15", stateHome: dir, configHome: join(dir, "no-alerts") });
    expect(invalid).toMatchObject({ exitCode: 2, output: { error: "config_invalid" } });
    const wrongShape = await runWatchdog({ configPath: await writeConfig({ schema_version: 1 }), dryRun: false, redisUrl: "redis://127.0.0.1:1/15", stateHome: dir, configHome: join(dir, "no-alerts") });
    expect(wrongShape.exitCode).toBe(2);
    const down = await runWatchdog({ configPath: await writeConfig(config(["x"], "r", null)), dryRun: false, redisUrl: "redis://127.0.0.1:1/15", stateHome: dir, configHome: join(dir, "no-alerts") });
    expect(down).toMatchObject({ exitCode: 1, output: { error: "redis_unavailable" } });
  });
});

describe("task ledger", () => {
  let redis: Redis;
  const keys: string[] = [];
  beforeAll(() => { redis = new Redis(REDIS_URL); });
  afterAll(async () => { if (keys.length > 0) await redis.del(...keys); redis.disconnect(); });

  it("a task send records a one-line subject with a TTL; a non-task send records nothing", async () => {
    const client = new RedisClient(null, REDIS_URL);
    const sender = `wd-ledger-${randomUUID().slice(0, 8)}`;
    const to = `wd-ledger-to-${randomUUID().slice(0, 8)}`;
    await client.register("both", sender);
    try {
      const task = randomUUID();
      const status = randomUUID();
      const long = `line one\n\tline   two ${"x".repeat(300)}`;
      const timestamp = new Date().toISOString();
      await client.sendMessage({ id: task, from: sender, to, timestamp, type: "task", payload: { content: long } });
      await client.sendMessage({ id: status, from: sender, to, timestamp, type: "status", payload: { content: "fyi" } });
      keys.push(taskLedgerKey(task), taskLedgerKey(status), SESSION_KEYS.outstanding(sender, task), SESSION_KEYS.queue(to), SESSION_KEYS.mailboxMeta(to), SESSION_KEYS.inboxEvents(to));

      const entry = JSON.parse((await redis.get(taskLedgerKey(task)))!);
      expect(entry).toEqual({ from: sender, to, subject: `line one line two ${"x".repeat(300)}`.slice(0, 160), sent_at: timestamp });
      expect(await redis.ttl(taskLedgerKey(task))).toBeGreaterThan(TASK_LEDGER_TTL_SECONDS - 60);
      expect(await redis.exists(taskLedgerKey(status))).toBe(0);
      expect(await redis.exists(SESSION_KEYS.taskAnswered(task))).toBe(0);
    } finally {
      await client.unregister().catch(() => undefined);
      await client.shutdown();
    }
  });
});

const BUILT = existsSync(join(process.cwd(), "dist", "core", "watchdog-cli.js"));

describe.skipIf(!BUILT)("gptqueue-watchdog bin (needs npm run build)", () => {
  const run = (...args: string[]) => spawnSync(process.execPath, [join(process.cwd(), "bin", "gptqueue-watchdog"), ...args], { encoding: "utf8", timeout: 10_000 });

  it("exits 2 on a config error and on a missing --once, printing nothing from Redis", () => {
    const missing = run("--once", "--config", join(tmpdir(), `absent-${randomUUID()}.json`), "--redis-url", REDIS_URL);
    expect(missing.status).toBe(2);
    expect(JSON.parse(missing.stdout)).toMatchObject({ error: "config_missing" });
    expect(run("--config", "x").status).toBe(2);
  });

  it("W5/W6: a quiet pass prints one line, --json forces the report, --status reads the last pass", async () => {
    const dirQuiet = await mkdtemp(join(tmpdir(), "gptq-watchdog-cli-"));
    try {
      const configPath = join(dirQuiet, "w.json");
      await writeFile(configPath, JSON.stringify({ schema_version: 1, root: "r", leads: [`wd-cli-${randomUUID().slice(0, 8)}`], operator: null,
        thresholds_min: { waiting: 10, escalate_after: 10, idle: 15 }, max_actions_per_run: 5 }));
      const env = { ...process.env, XDG_STATE_HOME: dirQuiet, XDG_CONFIG_HOME: dirQuiet, REDIS_URL };
      const exec = (...args: string[]) => spawnSync(process.execPath, [join(process.cwd(), "bin", "gptqueue-watchdog"), ...args], { encoding: "utf8", timeout: 10_000, env });
      const quiet = exec("--once", "--config", configPath);
      expect(quiet.status).toBe(0);
      expect(quiet.stdout.trim().split("\n")).toHaveLength(1);
      expect(quiet.stdout).toMatch(/^gptqueue watchdog: ok at=\S+ leads=1 waiting=0 stalled=0 idle=0 uncorrelated=\d+\n$/u);
      const full = exec("--once", "--json", "--config", configPath);
      expect(JSON.parse(full.stdout)).toMatchObject({ dry_run: false, actions: [] });
      const status = exec("--status");
      expect(status.status).toBe(0);
      expect(JSON.parse(status.stdout)).toMatchObject({ healthy: true, last_pass: { exit: 0 } });
    } finally {
      await rm(dirQuiet, { recursive: true, force: true });
    }
  });
});
