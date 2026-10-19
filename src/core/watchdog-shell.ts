/**
 * Watchdog (loop adapter S2), imperative shell: one `--once` pass. Reads the
 * config, turn states and outstanding requests, plans with the pure core
 * (core/watchdog.ts), then performs each action and records its fire key only
 * after the effect succeeded. `dryRun` reads and plans but writes nothing.
 *
 * Reminders are ordinary `type: "task"` sends from SYSTEM_SENDER through the
 * existing MailboxStore path, so the recipient's normal wake path (inbox
 * event, Codex turn/start, Claude watcher and Stop gate, Pi followUp) fires.
 */
import { randomUUID } from "node:crypto";
import { appendFile, mkdir, open, readFile } from "node:fs/promises";
import { join } from "node:path";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "./keys.js";
import { MailboxStore, type QueueFullReason } from "./mailbox-store.js";
import { TurnStateStore } from "./turn-state.js";
import { admitTaskLedgerEntry, taskLedgerKey } from "./task-ledger.js";
import { WATCHDOG_RATE_WINDOW_MS } from "./watchdog-push.js";
import { pruneWatchdogState, rotateEscalationLog, WATCHDOG_RETENTION, type WatchdogRetention } from "./watchdog-retention.js";
import { createOperatorAlertSink, loadOperatorAlertsConfig, type DeliveryResult, type OperatorAlertSink } from "./operator-alerts.js";
import {
  admitWatchdogConfig,
  candidateFireKeys,
  deriveLeadState,
  parseOutstanding,
  planActions,
  sw4rmState,
  classifyRequests,
  type ClassifiedRequests,
  WATCHDOG_SENDER,
  type LeadReport,
  type Sw4rmState,
  type RecipientCapacity,
  type WatchdogAction,
  type WatchdogConfig,
} from "./watchdog.js";

export const WATCHDOG_KEYS = { fired: SESSION_KEYS.watchdogFired, sent: SESSION_KEYS.watchdogSent } as const;
/** W6: the last real pass's summary, read by `gptqueue-watchdog --status`. */
export const WATCHDOG_LAST_PASS = SESSION_KEYS.watchdogLastPass;
/** W3: the last non-skipped real pass; --status judges health from this one. */
export const WATCHDOG_LAST_SUBSTANTIVE_PASS = SESSION_KEYS.watchdogLastSubstantivePass;
const LAST_PASS_TTL_SECONDS = 7 * 24 * 60 * 60;
const LAST_PASS_BUDGET_MS = 1_000;
/** W3: reminders per recipient are counted over a rolling hour; the set itself lives two hours. */
/** One pass at a time (S4/S5): passes and the operator-alert state file are serialized by this lock. */
export const WATCHDOG_PASS_LOCK = SESSION_KEYS.watchdogPassLock;
const PASS_LOCK_PX = 55_000;
/** Overall budget for one pass (S7); below the pass lock's lifetime. */
export const PASS_DEADLINE_MS = 45_000;
/** Fire keys live seven days, like the requests they describe. */
export const FIRED_TTL_SECONDS = 7 * 24 * 60 * 60;
/**
 * Two-phase fire keys (S4): `pending:<iso>` is claimed before an effect and
 * replaced by the done record after it. A crash between the two leaves the
 * pending claim, which suppresses the action until it expires, so a crashed
 * effect is retried after at most 15 minutes and never concurrently.
 */
export const PENDING_TTL_SECONDS = 900;
const PENDING_PREFIX = "pending:";

const RELEASE_LOCK = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`;
/** A full mailbox is retried briefly (about 0.7 s), not the interactive send path's ~20 s, so one pass stays short. */
const SEND_ATTEMPTS = 3;

export interface WatchdogOptions {
  readonly configPath: string;
  readonly dryRun: boolean;
  readonly redisUrl: string;
  /** $XDG_STATE_HOME (or ~/.local/state); escalations go to <stateHome>/gptqueue/watchdog/escalations.jsonl. */
  readonly stateHome: string;
  /** $XDG_CONFIG_HOME (or ~/.config); the operator alert setting is <configHome>/gptqueue/operator-alerts.json. */
  readonly configHome: string;
  readonly now?: () => Date;
  /** Tests only; defaults to PASS_DEADLINE_MS. */
  readonly passDeadlineMs?: number;
  /** Tests only; defaults to WATCHDOG_RETENTION (W2). */
  readonly retention?: WatchdogRetention;
  /** Tests only: fault injection (W1 crash between reserve and send). */
  readonly faults?: Readonly<{ afterSend?: (fireKey: string) => void }>;
}

export type ActionStatus = "planned" | "done" | "failed" | "skipped" | "rate_limited";
export interface ReportedAction {
  readonly kind: WatchdogAction["kind"];
  readonly rule: WatchdogAction["rule"];
  readonly to: string;
  readonly fire_key: string;
  readonly text: string;
  readonly status: ActionStatus;
  readonly error?: string;
}
export interface WatchdogReport {
  readonly at: string;
  readonly leads: readonly Readonly<{ lead: string; state: LeadReport["state"]; sw4rm_state: Sw4rmState }>[];
  readonly actions: readonly ReportedAction[];
  readonly deferred: number;
  /** Outstanding requests with no task-ledger record (sent before S2): not correlatable with replies, never acted on. */
  readonly uncorrelated: number;
  /** W3: reminders held back by the per-recipient hourly cap; left unclaimed for a later pass. */
  readonly rate_limited: number;
  readonly dry_run: boolean;
  /** The S3 operator alert sink's status; never carries the operator address. */
  readonly operator_alerts: OperatorAlertsStatus;
}
export type OperatorAlertsStatus =
  | Readonly<{ status: "disabled" }>
  | Readonly<{ status: "refused"; error: string }>
  | Readonly<{ status: "enabled"; mode: "dry_run" | "live"; flush?: DeliveryResult }>;

/** The sink for this run, and how its status reads in a log line. */
type AlertsWiring = Readonly<{ sink: OperatorAlertSink | null; status: OperatorAlertsStatus; logValue: string }>;

const wireAlerts = async (configHome: string, stateHome: string): Promise<AlertsWiring> => {
  const loaded = await loadOperatorAlertsConfig(configHome);
  switch (loaded.kind) {
    case "missing":
    case "disabled":
      return { sink: null, status: { status: "disabled" }, logValue: "disabled" };
    case "refused":
      return { sink: null, status: { status: "refused", error: loaded.error }, logValue: `refused:${loaded.error}` };
    case "enabled":
      return { sink: createOperatorAlertSink({ config: loaded.config, stateHome }), status: { status: "enabled", mode: loaded.config.mode }, logValue: "enabled" };
    default: {
      const unreachable: never = loaded;
      throw new Error(`Unhandled alerts config ${JSON.stringify(unreachable)}`);
    }
  }
};

/**
 * Log fields for one escalation's sink delivery. A sink failure (emission or
 * held-state persistence) fails the whole escalation (S6): its claim is then
 * released and the next pass retries it, which the idempotent operator send
 * and the id-keyed sink make safe.
 */
const deliverAlert = async (alerts: AlertsWiring, action: WatchdogAction, question: string, now: Date): Promise<Record<string, unknown>> => {
  if (alerts.sink === null) return { operator_alerts: alerts.logValue };
  const result = await alerts.sink.deliver({ id: action.fire_key, lead: action.lead, question, options: ["yes", "no"], raised_at: now.toISOString(), rule: action.rule }, now);
  return result.status === "outboxed" ? { operator_alerts: "outboxed", outbox: result.outbox } : { operator_alerts: result.status };
};
export type WatchdogFailure = Readonly<{ error: "config_missing" | "config_invalid" | "redis_unavailable" | "pass_deadline"; reason: string }>;
export type WatchdogSkipped = Readonly<{ skipped: "pass_in_progress" }>;
export type WatchdogResult =
  | Readonly<{ exitCode: 0; output: WatchdogReport | WatchdogSkipped }>
  | Readonly<{ exitCode: 1 | 2; output: WatchdogFailure }>;

const message = (error: unknown): string => (error instanceof Error ? error.message : String(error));

type ConfigLoad = Readonly<{ ok: true; config: WatchdogConfig }> | Readonly<{ ok: false; failure: WatchdogFailure }>;

const loadConfig = async (path: string): Promise<ConfigLoad> => {
  let text: string;
  try {
    text = await readFile(path, "utf8");
  } catch (error) {
    const code = (error as NodeJS.ErrnoException)?.code;
    return { ok: false, failure: { error: code === "ENOENT" ? "config_missing" : "config_invalid", reason: `${path}: ${message(error)}` } };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    return { ok: false, failure: { error: "config_invalid", reason: `${path}: ${message(error)}` } };
  }
  const admitted = admitWatchdogConfig(parsed);
  return admitted.ok ? admitted : { ok: false, failure: { error: "config_invalid", reason: `${path}: ${admitted.reason}` } };
};

/** Every outstanding request touching a lead, classified with its task ledger and answered marker (F1). */
const readOutstanding = async (redis: Redis, leads: ReadonlySet<string>, nowMs: number): Promise<ClassifiedRequests> => {
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [next, batch] = await redis.scan(cursor, "MATCH", `${SESSION_KEYS.outstanding("", "").slice(0, -1)}*`, "COUNT", 1000);
    keys.push(...batch);
    cursor = next;
  } while (cursor !== "0");
  if (keys.length === 0) return classifyRequests([]);
  const pipeline = redis.pipeline();
  for (const key of keys) pipeline.get(key).pttl(key);
  const replies = (await pipeline.exec()) ?? [];
  const parsed = keys.flatMap((key, i) => {
    const recipient = replies[2 * i]?.[1];
    const pttl = replies[2 * i + 1]?.[1];
    if (typeof recipient !== "string" || typeof pttl !== "number") return [];
    const request = parseOutstanding(key, recipient, pttl, nowMs);
    return request !== null && (leads.has(request.sender) || leads.has(request.recipient)) ? [request] : [];
  });
  if (parsed.length === 0) return classifyRequests([]);
  const [ledgers, answered] = await Promise.all([
    redis.mget(parsed.map((r) => taskLedgerKey(r.task_id))),
    redis.mget(parsed.map((r) => SESSION_KEYS.taskAnswered(r.task_id))),
  ]);
  return classifyRequests(parsed.map((request, i) => ({ request, ledger: admitTaskLedgerEntry(ledgers[i] ?? null), answered: answered[i] ?? null })));
};

type FireRecords = Readonly<{ done: Map<string, string>; pending: Set<string> }>;

/** Done records (with their fire time, for rule 2) and pending claims (S4), by fire key. */
const readFired = async (redis: Redis, keys: readonly string[]): Promise<FireRecords> => {
  const done = new Map<string, string>();
  const pending = new Set<string>();
  if (keys.length === 0) return { done, pending };
  const values = await redis.mget(keys.map((k) => WATCHDOG_KEYS.fired(k)));
  keys.forEach((key, i) => {
    const raw = values[i];
    if (raw === null || raw === undefined) return;
    if (raw.startsWith(PENDING_PREFIX)) { pending.add(key); return; }
    try {
      const firedAt = (JSON.parse(raw) as { fired_at?: unknown }).fired_at;
      done.set(key, typeof firedAt === "string" && !Number.isNaN(Date.parse(firedAt)) ? firedAt : new Date(0).toISOString());
    } catch {
      // A present but unreadable record still dedupes; it cannot time rule 2, so it counts as long ago.
      done.set(key, new Date(0).toISOString());
    }
  });
  return { done, pending };
};

/**
 * S4: every watchdog send is idempotent per (recipient, fire key). A retried
 * effect (a crash after delivery, before finalize) gets the original message
 * back as a duplicate instead of enqueueing a second one.
 */
const sendOnce = async (
  mailbox: MailboxStore, to: string, action: WatchdogAction, now: Date, faults?: WatchdogOptions["faults"], rateCap?: number,
): Promise<Readonly<{ status: "ok" | "full" | "limited"; reason?: QueueFullReason }>> => {
  let outcome: Awaited<ReturnType<MailboxStore["sendWatchdogTask"]>>;
  try {
    const message = systemTask(to, action, now);
    const key = `watchdog:${action.fire_key}`;
    // X3: a reminder's rate admission and enqueue are one script on Redis TIME (core/watchdog-push.ts).
    outcome = rateCap === undefined
      ? await mailbox.sendIdempotent(message, WATCHDOG_SENDER, key, SEND_ATTEMPTS)
      : await mailbox.sendWatchdogTask(message, WATCHDOG_SENDER, key, { fireKey: action.fire_key, cap: rateCap }, SEND_ATTEMPTS);
    faults?.afterSend?.(action.fire_key);
  } catch (error) {
    // X2: a thrown send (timeout, lost connection, lost ack) may or may not have enqueued.
    throw new UncertainDeliveryError(error);
  }
  if (outcome.status === "full") return { status: "full", reason: outcome.reason };
  return { status: outcome.status === "limited" ? "limited" : "ok" };
};

/** X2: the mailbox positively refused the message (full, nothing stored). */
class NotEnqueuedError extends Error {}
/** X3: the send script refused the reminder over the recipient's hourly cap; nothing changed. */
class RateLimitedError extends Error {}
/** X2: the outcome of a send is unknown; it may have been enqueued. */
class UncertainDeliveryError extends Error {
  constructor(cause: unknown) {
    super(`delivery outcome unknown: ${message(cause)}`);
  }
}

const systemTask = (to: string, action: WatchdogAction, now: Date) => ({
  id: randomUUID(),
  from: WATCHDOG_SENDER,
  to,
  timestamp: now.toISOString(),
  type: "task" as const,
  payload: { content: action.text, metadata: { watchdog: { rule: action.rule, fire_key: action.fire_key, lead: action.lead, task_id: action.task_id } } },
});

type PassClock = { expired: boolean };

/** Claim, perform, complete. Returns false when another claim holds the key. */
const perform = async (
  redis: Redis, mailbox: MailboxStore, action: WatchdogAction, config: WatchdogConfig, question: string | null, stateHome: string, alerts: AlertsWiring, now: Date, clock: PassClock,
  retention: WatchdogRetention, faults?: WatchdogOptions["faults"],
): Promise<boolean> => {
  const fireKey = WATCHDOG_KEYS.fired(action.fire_key);
  if ((await redis.set(fireKey, `${PENDING_PREFIX}${now.toISOString()}`, "EX", PENDING_TTL_SECONDS, "NX")) !== "OK") return false;
  try {
    await effect(mailbox, action, config, question, stateHome, alerts, now, retention, faults);
  } catch (error) {
    // X2: an uncertain delivery keeps the claim; it expires and the idempotent send is retried, which
    // returns "duplicate" (no second entry, no second rate count) if the first attempt was enqueued.
    if (error instanceof UncertainDeliveryError) throw error;
    // A definite non-delivery (full, rate limited) or a local failure releases the claim for a later pass.
    await redis.del(fireKey).catch(() => undefined);
    throw error;
  }
  // S7: past the deadline the claim stays pending and expires; no completion is recorded for unfinished work.
  if (clock.expired) throw new Error("pass deadline reached");
  await redis.set(fireKey, JSON.stringify({ fired_at: now.toISOString(), rule: action.rule }), "EX", FIRED_TTL_SECONDS);
  return true;
};

const effect = async (
  mailbox: MailboxStore, action: WatchdogAction, config: WatchdogConfig, question: string | null, stateHome: string, alerts: AlertsWiring, now: Date,
  retention: WatchdogRetention, faults?: WatchdogOptions["faults"],
): Promise<void> => {
  switch (action.kind) {
    case "remind": {
      const sent = await sendOnce(mailbox, action.to, action, now, faults, config.max_reminders_per_recipient_per_hour);
      if (sent.status === "full") throw new NotEnqueuedError(`mailbox ${action.to} is full (${sent.reason ?? "full"})`);
      if (sent.status === "limited") throw new RateLimitedError(`recipient ${action.to} reached its hourly reminder cap`);
      break;
    }
    case "escalate": {
      const dir = join(stateHome, "gptqueue", "watchdog");
      await mkdir(dir, { recursive: true });
      // Order (S6): (1) idempotent operator send, (2) sink delivery, which throws on any failure,
      // (3) the JSONL line last, so a retried escalation writes it exactly once; finalize follows.
      // A full operator mailbox is a recorded partial success (F3), not a failure.
      const operatorMailbox: "ok" | "full" | "none" = config.operator === null
        ? "none"
        : (await sendOnce(mailbox, config.operator, action, now, faults)).status === "full" ? "full" : "ok"; // escalations are never rate limited
      const delivered = await deliverAlert(alerts, action, question ?? action.text, now);
      const line = { at: now.toISOString(), rule: action.rule, lead: action.lead, task_id: action.task_id, to: action.to, fire_key: action.fire_key, text: action.text, question, operator_mailbox: operatorMailbox, ...delivered };
      // R1: the log is at-least-once (consumers dedupe by fire_key); this guard skips the common retry,
      // where the previous attempt appended the line and then failed to finalize.
      const logPath = join(dir, "escalations.jsonl");
      await rotateEscalationLog(dir, now, retention); // W2
      if ((await lastLogFireKey(logPath)) !== action.fire_key) await appendFile(logPath, `${JSON.stringify(line)}\n`);
      break;
    }
    default: {
      const unreachable: never = action.kind;
      throw new Error(`Unhandled action kind ${String(unreachable)}`);
    }
  }
};

/** The fire_key of the last JSONL line, reading only the file's tail; null when absent or unreadable as JSON. */
const lastLogFireKey = async (path: string): Promise<string | null> => {
  let handle;
  try {
    handle = await open(path, "r");
  } catch (error) {
    if ((error as NodeJS.ErrnoException)?.code === "ENOENT") return null;
    throw error;
  }
  try {
    const { size } = await handle.stat();
    const length = Math.min(size, 65_536);
    const buffer = Buffer.alloc(length);
    await handle.read(buffer, 0, length, size - length);
    const last = buffer.toString("utf8").trimEnd().split("\n").at(-1) ?? "";
    try {
      const key = (JSON.parse(last) as { fire_key?: unknown }).fire_key;
      return typeof key === "string" ? key : null;
    } catch {
      return null;
    }
  } finally {
    await handle.close();
  }
};

const report = (action: WatchdogAction, status: ActionStatus, error?: string): ReportedAction =>
  Object.freeze({ kind: action.kind, rule: action.rule, to: action.to, fire_key: action.fire_key, text: action.text, status, ...(error === undefined ? {} : { error }) });

/**
 * One watchdog pass. Never throws: failures map to exit codes 2 (config) and
 * 1 (Redis, or the pass deadline). A real pass holds the pass lock; a dry run
 * writes nothing, so it takes no lock.
 */
export const runWatchdog = async (options: WatchdogOptions): Promise<WatchdogResult> => {
  const loaded = await loadConfig(options.configPath);
  if (!loaded.ok) {
    // The pass itself touches no Redis with a bad config; only after deciding to exit is the failure
    // recorded (same 1 s budget), so a stale healthy status cannot outlive a broken config.
    const failed: WatchdogResult = { exitCode: 2, output: loaded.failure };
    if (!options.dryRun) await recordLastPass(options.redisUrl, failed, new Date());
    return failed;
  }
  const deadlineMs = options.passDeadlineMs ?? PASS_DEADLINE_MS;
  const redis = new Redis(options.redisUrl, {
    lazyConnect: true, maxRetriesPerRequest: 1, enableOfflineQueue: false, connectTimeout: 3000, commandTimeout: 3000, retryStrategy: () => null,
  });
  redis.on("error", () => { /* surfaced through the awaited command */ });
  const clock: PassClock = { expired: false };
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expiry = new Promise<WatchdogResult>((resolveExpiry) => {
    timer = setTimeout(() => {
      clock.expired = true;
      redis.disconnect();
      resolveExpiry({ exitCode: 1, output: { error: "pass_deadline", reason: `the pass exceeded ${deadlineMs} ms` } });
    }, deadlineMs);
  });
  let result: WatchdogResult;
  try {
    result = await Promise.race([pass(options, loaded.config, redis, clock), expiry]);
  } finally {
    clearTimeout(timer);
    redis.disconnect();
  }
  if (!options.dryRun) await recordLastPass(options.redisUrl, result, new Date());
  return result;
};

/** W6/W3: the summary of one invocation (every real invocation, including skipped ones). */
export const lastPassRecord = (result: WatchdogResult, at: Date): Record<string, unknown> => {
  const output = result.output;
  if ("skipped" in output) return { at: at.toISOString(), exit: result.exitCode, skipped: true };
  return { ...substantiveRecord(result, at), skipped: false };
};

/** W3: the summary --status judges: {at, exit, leads, actions, failed, rate_limited, deferred, uncorrelated, error?}. */
const substantiveRecord = (result: WatchdogResult, at: Date): Record<string, unknown> => {
  const output = result.output;
  if ("skipped" in output || "error" in output) {
    return { at: at.toISOString(), exit: result.exitCode, leads: 0, actions: 0, failed: 0, rate_limited: 0, deferred: 0, uncorrelated: 0,
      ...("error" in output ? { error: output.error } : {}) };
  }
  return {
    at: output.at, exit: result.exitCode, leads: output.leads.length, actions: output.actions.filter((a) => a.status !== "rate_limited").length,
    failed: output.actions.filter((a) => a.status === "failed").length,
    rate_limited: output.rate_limited, deferred: output.deferred, uncorrelated: output.uncorrelated,
  };
};

/**
 * Write the last-pass record on a fresh short-lived connection, so a pass that
 * ended at its deadline or on a Redis error still records itself when Redis is
 * reachable. Best effort.
 */
const recordLastPass = async (redisUrl: string, result: WatchdogResult, at: Date): Promise<void> => {
  const client = new Redis(redisUrl, {
    lazyConnect: true, maxRetriesPerRequest: 0, enableOfflineQueue: false, enableReadyCheck: false,
    connectTimeout: LAST_PASS_BUDGET_MS, commandTimeout: LAST_PASS_BUDGET_MS, retryStrategy: () => null,
  });
  client.on("error", () => undefined);
  let timer: ReturnType<typeof setTimeout> | undefined;
  // One overall budget: a hung server must not hold the timer's pass past its deadline.
  const budget = new Promise<void>((resolveBudget) => { timer = setTimeout(resolveBudget, LAST_PASS_BUDGET_MS); });
  const write = (async () => {
    await client.connect();
    const multi = client.multi().set(WATCHDOG_LAST_PASS, JSON.stringify(lastPassRecord(result, at)), "EX", LAST_PASS_TTL_SECONDS);
    // W3: a skipped invocation never replaces the substantive record.
    if (!("skipped" in result.output)) multi.set(WATCHDOG_LAST_SUBSTANTIVE_PASS, JSON.stringify(substantiveRecord(result, at)), "EX", LAST_PASS_TTL_SECONDS);
    await multi.exec();
  })().catch(() => { /* Redis unreachable: --status then reports the previous (aging) record */ });
  try {
    await Promise.race([write, budget]);
  } finally {
    clearTimeout(timer);
    client.disconnect();
  }
};

/** W2: read-only planning view of each recipient's remaining slots and current reservations. */
const readCapacity = async (redis: Redis, recipients: readonly string[], cap: number, nowMs: number): Promise<RecipientCapacity> => {
  const unique = [...new Set(recipients)];
  const members = await Promise.all(unique.map((r) => redis.zrangebyscore(WATCHDOG_KEYS.sent(r), nowMs - WATCHDOG_RATE_WINDOW_MS, "+inf")));
  return new Map(unique.map((r, i) => [r, { remaining: Math.max(0, cap - members[i]!.length), reserved: new Set(members[i]) }]));
};

const pass = async (options: WatchdogOptions, config: WatchdogConfig, redis: Redis, clock: PassClock): Promise<WatchdogResult> => {
  const now = (options.now ?? (() => new Date()))();
  try {
    await redis.connect();
  } catch (error) {
    return { exitCode: 1, output: { error: "redis_unavailable", reason: message(error) } };
  }
  const token = randomUUID();
  let locked = false;
  try {
    if (!options.dryRun) {
      locked = (await redis.set(WATCHDOG_PASS_LOCK, token, "PX", PASS_LOCK_PX, "NX")) === "OK";
      if (!locked) return { exitCode: 0, output: Object.freeze({ skipped: "pass_in_progress" }) };
    }
    const { open: requests, uncorrelated } = await readOutstanding(redis, new Set(config.leads), now.getTime());
    const turns = new TurnStateStore(redis);
    const leads: LeadReport[] = await Promise.all(config.leads.map(async (lead) =>
      Object.freeze({ lead, state: deriveLeadState(lead, await turns.read(lead), requests) })));
    const fired = await readFired(redis, candidateFireKeys(leads));
    // A pending claim (an effect in flight, or one that crashed) suppresses its action until the claim expires;
    // it is excluded before the cap (N2).
    // Reminders go only to a waiting lead's awaited agent (rule 1) or to root (rules 4 and 5).
    const recipients = [...leads.flatMap(({ state }) => (state.kind === "waiting" ? [state.on] : [])), config.root];
    const capacity = await readCapacity(redis, recipients, config.max_reminders_per_recipient_per_hour, now.getTime());
    // W2: rate eligibility is applied before max_actions_per_run, like pending claims.
    const plan = planActions(leads, config, fired.done, now, fired.pending, capacity);
    const due = plan.actions;
    const questionOf = (action: WatchdogAction): string | null => {
      const state = leads.find((l) => l.lead === action.lead)?.state;
      return action.rule === 3 && state?.kind === "waiting" ? state.subject : null;
    };

    const actions: ReportedAction[] = [];
    // Reading the setting writes nothing, so a dry run still reports its status.
    const alerts = await wireAlerts(options.configHome, options.stateHome);
    let alertsStatus = alerts.status;
    actions.push(...plan.rate_limited.map((a) => report(a, "rate_limited")));
    if (options.dryRun) {
      actions.push(...due.map((a) => report(a, "planned")));
    } else {
      const mailbox = new MailboxStore(redis, redis);
      const retention = options.retention ?? WATCHDOG_RETENTION;
      for (const action of due) {
        if (clock.expired) break;
        try {
          const performed = await perform(redis, mailbox, action, config, questionOf(action), options.stateHome, alerts, now, clock, retention, options.faults);
          actions.push(report(action, performed ? "done" : "skipped"));
        } catch (error) {
          // X3: the send script is the authority on the hourly cap; a refused reminder is unclaimed for a later pass.
          actions.push(error instanceof RateLimitedError ? report(action, "rate_limited") : report(action, "failed", message(error)));
        }
      }
      // A digest falls due when a quiet window ends, the hourly budget frees up, or a failed delivery waits for retry.
      if (alerts.sink !== null && alertsStatus.status === "enabled" && !clock.expired) {
        const flush = await alerts.sink.flush(now).catch((): DeliveryResult => ({ status: "idle" }));
        alertsStatus = { ...alertsStatus, flush };
      }
      // W2: retention runs on real passes only, under the pass lock.
      if (!clock.expired) await pruneWatchdogState(join(options.stateHome, "gptqueue", "watchdog"), now, retention);
    }
    return {
      exitCode: 0,
      output: Object.freeze({
        at: now.toISOString(),
        leads: leads.map(({ lead, state }) => Object.freeze({ lead, state, sw4rm_state: sw4rmState(state) })),
        actions,
        deferred: plan.deferred,
        uncorrelated,
        rate_limited: actions.filter((a) => a.status === "rate_limited").length,
        dry_run: options.dryRun,
        operator_alerts: alertsStatus,
      }),
    };
  } catch (error) {
    return { exitCode: 1, output: { error: "redis_unavailable", reason: message(error) } };
  } finally {
    if (locked && !clock.expired) await redis.eval(RELEASE_LOCK, 1, WATCHDOG_PASS_LOCK, token).catch(() => undefined);
  }
};
