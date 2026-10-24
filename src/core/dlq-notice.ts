/** Atomic, opportunistic dead-letter notices using ordinary waking admission. */
import type { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { DLQ_NOTICE_KEYS, SESSION_DEFAULTS, SESSION_KEYS, SYSTEM_SENDER } from "./keys.js";
import { LUA_DIR } from "./stored-read.js";

export const DLQ_NOTICE_INSTRUCTION =
  "Your messages were dead-lettered after repeated claim expiry. Inspect with dlq_status and restore with dlq_requeue " +
  "(also in the registered-shell sidecar). Listed ids may already be restored or trimmed. " +
  "For gated work, renew before expiry or ACK and track it. ACK this notice; do not reply to the system mailbox.";

const replaceOnce = (text: string, anchor: string, replacement: string): string => {
  const at = text.indexOf(anchor);
  if (at < 0 || text.indexOf(anchor, at + anchor.length) >= 0) {
    throw new Error(`DLQ notice: expected exactly one push anchor: ${anchor}`);
  }
  return text.slice(0, at) + replacement + text.slice(at + anchor.length);
};

/** Retain ordinary admission/enqueue effects; replace only retry deduplication. */
export const composeDeadLetterNotice = (push: string, prepare: string): string => {
  const withoutCheck = replaceOnce(push,
    "local existing = redis.call('GET', KEYS[3])\nif existing then return {2, existing} end", prepare);
  const withoutRecord = replaceOnce(withoutCheck,
    "redis.call('SET', KEYS[3], ARGV[3], 'EX', tonumber(ARGV[4]))", "-- No independent idempotency key.");
  return replaceOnce(withoutRecord, "return {1, ARGV[3]}", `
-- Success-only consumption; every destination was validated before enqueue.
for _, id in ipairs(sent_ids) do redis.call('SREM', KEYS[9], id) end
redis.call('DEL', KEYS[10])
redis.call('SET', KEYS[11], '1', 'EX', 900)
return {1, ARGV[3]}`);
};

const lua = (file: string): string => readFileSync(join(LUA_DIR, file), "utf8");
const script = `${lua("waking-admission-lib.lua")}\n${lua("dlq-notice-lib.lua")}\n${composeDeadLetterNotice(
  lua("bounded-push-idempotent.lua"), lua("dlq-notice-prepare.lua"))}`;

export type DeadLetterNoticeResult = Readonly<{ sent: boolean; reason?: string }>;

/** One atomic attempt, including overflow-only state. No activity means no retry. */
export const notifyDeadLetters = async (redis: Redis, actor: string): Promise<DeadLetterNoticeResult> => {
  const id = randomUUID();
  const now = new Date().toISOString();
  const envelope = { id, from: SYSTEM_SENDER, to: actor, timestamp: now, type: "task",
    payload: { content: DLQ_NOTICE_INSTRUCTION, metadata: { kind: "dlq_notice" } } };
  const [flag, , reason] = await redis.eval(script, 14,
    SESSION_KEYS.queue(actor), SESSION_KEYS.mailboxMeta(actor), "",
    SESSION_KEYS.inboxEvents(actor), SESSION_KEYS.outstanding(SYSTEM_SENDER, id),
    SESSION_KEYS.forward(actor), SESSION_KEYS.taskLedger(id), SESSION_KEYS.taskAnswered(id),
    DLQ_NOTICE_KEYS.pending(actor), DLQ_NOTICE_KEYS.overflow(actor), DLQ_NOTICE_KEYS.window(actor),
    SESSION_KEYS.registry, SESSION_KEYS.agentSessions(SYSTEM_SENDER), DLQ_NOTICE_KEYS.collisions(actor),
    JSON.stringify(envelope), process.env.GPTQ_QUEUE_BOUND || SESSION_DEFAULTS.DEFAULT_QUEUE_BOUND,
    id, 86400, "0", "task", "1", actor, "", "Dead-letter notice", now,
    SESSION_KEYS.outstanding("", "").slice(0, -1), SYSTEM_SENDER,
  ) as [number, string, string?];
  return Object.freeze({ sent: flag === 1, ...(flag === 1 ? {} : { reason: reason ?? "forwarded" }) });
};

/** A committed recovery/claim must survive notification transport or state errors. */
export const tryNotifyDeadLetters = async (redis: Redis, actor: string): Promise<void> => {
  try {
    const result = await notifyDeadLetters(redis, actor);
    if (!result.sent && result.reason !== "empty" && result.reason !== "window") {
      console.warn("DLQ notice refused", { actor, reason: result.reason });
    }
  } catch (error) {
    console.warn("DLQ notice failed", { actor, error });
  }
};
