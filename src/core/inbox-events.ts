import { Redis } from "ioredis";
import type { QueueMessage } from "./types.js";
import { SESSION_KEYS } from "./keys.js";

export type InboxEventType = QueueMessage["type"];
export type InboxTraceStage = "activation_requested" | "activation_deferred" | "turn_started" | "activation_queued" | "activation_failed" | "activation_rearmed" | "task_claimed" | "task_acknowledged" | "reply_sent" | "runtime_bound" | "runtime_unbound";
export type InboxTraceFields = Readonly<{ stage: InboxTraceStage; timestamp: string; message_id?: string; in_reply_to?: string; claim_id?: string; operation_id?: string; runtime_id?: string; turn_id?: string; code?: string }>;
const stream = SESSION_KEYS.inboxEvents;
const traceStream = SESSION_KEYS.inboxTrace;
const outstanding = SESSION_KEYS.outstanding;
const nonEmptyString = (value: unknown): value is string => typeof value === "string" && value !== "";
/**
 * A reply that can wake: a result or error whose sender (`from`) and
 * correlation (`payload.in_reply_to`) are both non-empty strings. Every
 * in-repo writer produces strings for both; a hand-written or malformed raw
 * entry with any other shape never wakes. waking-admission-lib.lua applies
 * the identical rule (tests/waking-admission.test.ts checks they agree).
 */
const awaitsOutstanding = (message: QueueMessage): message is QueueMessage & { payload: { in_reply_to: string } } =>
  (message.type === "result" || message.type === "error") && nonEmptyString(message.from) && nonEmptyString(message.payload.in_reply_to);

/** A raw inbox-list entry as a message object, or null when it is not JSON or not a JSON object. */
export const parseQueueEntry = (raw: string): QueueMessage | null => {
  try {
    const value: unknown = JSON.parse(raw);
    return typeof value === "object" && value !== null && !Array.isArray(value) ? value as QueueMessage : null;
  } catch { return null; }
};

/** Mail the dispatcher considers at all: addressed to `agent`, with a string id and a payload. */
const addressedTo = (agent: string, message: QueueMessage): boolean =>
  message.to === agent && typeof message.id === "string" && !!message.payload;

/**
 * The outstanding-request key whose value decides whether `message` wakes
 * `agent`: set for a result or error addressed to `agent` that names the
 * request it answers (payload.in_reply_to), null for every other message.
 */
export const replyOwnerKey = (agent: string, message: QueueMessage): string | null =>
  addressedTo(agent, message) && awaitsOutstanding(message) ? outstanding(message.to, message.payload.in_reply_to) : null;

/**
 * The dispatcher's wake-eligibility rule, pure: whether `message` in
 * `agent`'s inbox wakes `agent`. A task always does. A result or error does
 * only when it answers `agent`'s own outstanding request: `owner`, the value
 * stored at replyOwnerKey(agent, message), names the reply's sender. Status,
 * ping, unsolicited or misaddressed mail never wakes.
 */
export const wakesRecipient = (agent: string, message: QueueMessage, owner: string | null): boolean =>
  addressedTo(agent, message) && (message.type === "task" || (awaitsOutstanding(message) && owner === message.from));

/**
 * The owner lookup wakesRecipient needs, index-aligned with `messages` (null
 * where a message needs none or the request is no longer outstanding).
 */
export const replyOwners = async (redis: Pick<Redis, "mget">, agent: string, messages: readonly QueueMessage[]): Promise<readonly (string | null)[]> => {
  const keys = messages.map((message) => replyOwnerKey(agent, message));
  const lookups = keys.flatMap((key, index) => key === null ? [] : [[index, key] as const]);
  // One MGET resolves every reply's outstanding-request owner instead of a GET per message.
  const owners = lookups.length === 0 ? [] : await redis.mget(lookups.map(([, key]) => key));
  const ownerAt = new Map(lookups.map(([index], i) => [index, owners[i] ?? null]));
  return Object.freeze(keys.map((_, index) => ownerAt.get(index) ?? null));
};

export class InboxEvents {
  constructor(private readonly redis: Redis) {}

  async pending(agent: string): Promise<readonly QueueMessage[]> {
    const raw = await this.redis.lrange(SESSION_KEYS.queue(agent), 0, -1);
    const messages = raw.flatMap((value) => { const message = parseQueueEntry(value); return message === null ? [] : [message]; });
    const owners = await replyOwners(this.redis, agent, messages);
    return Object.freeze(messages.filter((message, index) => wakesRecipient(agent, message, owners[index] ?? null)));
  }

  /**
   * A waiter reuses one blocking connection across waits instead of opening
   * one per call (a dispatcher waits roughly once a second). Aborting a wait
   * disconnects it, which is how a blocked XREAD is interrupted; a broken or
   * aborted connection is replaced on the next wait. Call close() when done.
   */
  createWaiter(): Readonly<{
    wait(agent: string, afterId: string, signal: AbortSignal, blockMs?: number): Promise<string | null>;
    close(): void;
  }> {
    let connection: Redis | null = null;
    const fresh = async (): Promise<Redis> => {
      if (connection && connection.status === "ready") return connection;
      connection?.disconnect();
      const next = this.redis.duplicate({ lazyConnect: true, retryStrategy: () => null, maxRetriesPerRequest: 0, enableOfflineQueue: false, autoResendUnfulfilledCommands: false });
      next.on("error", () => undefined); // The awaited command reports failures to the caller.
      connection = next;
      await next.connect();
      return next;
    };
    return Object.freeze({
      wait: async (agent: string, afterId: string, signal: AbortSignal, blockMs = 1000): Promise<string | null> => {
        signal.throwIfAborted();
        const current = await fresh();
        const cancel = () => current.disconnect();
        signal.addEventListener("abort", cancel, { once: true });
        try {
          signal.throwIfAborted();
          const result = await current.xread("BLOCK", blockMs, "STREAMS", stream(agent), afterId) as [string, [string, string[]][]][] | null;
          return result?.[0]?.[1]?.at(-1)?.[0] ?? null;
        } catch (error) {
          current.disconnect();
          throw error;
        } finally {
          signal.removeEventListener("abort", cancel);
        }
      },
      close: () => { connection?.disconnect(); connection = null; },
    });
  }

  async wait(agent: string, afterId: string, signal: AbortSignal, blockMs = 1000): Promise<string | null> {
    signal.throwIfAborted();
    const connection = this.redis.duplicate({ lazyConnect: true, retryStrategy: () => null, maxRetriesPerRequest: 0, enableOfflineQueue: false, autoResendUnfulfilledCommands: false });
    connection.on("error", () => undefined); // The awaited command reports failures to the dispatcher.
    const cancel = () => connection.disconnect();
    signal.addEventListener("abort", cancel, { once: true });
    try { await connection.connect(); signal.throwIfAborted(); const result = await connection.xread("BLOCK", blockMs, "STREAMS", stream(agent), afterId) as [string, [string, string[]][]][] | null; return result?.[0]?.[1]?.at(-1)?.[0] ?? null; }
    finally { signal.removeEventListener("abort", cancel); connection.disconnect(); }
  }

  async trace(agent: string, fields: InboxTraceFields): Promise<void> {
    const values: string[] = ["stage", fields.stage, "timestamp", fields.timestamp];
    for (const key of ["message_id", "in_reply_to", "claim_id", "operation_id", "runtime_id", "turn_id", "code"] as const) {
      const value = fields[key];
      if (value !== undefined) values.push(key, value);
    }
    // Observability must never turn an already committed claim/send/ack into a failed tool call.
    await this.redis.xadd(traceStream(agent), "MAXLEN", "~", 1024, "*", ...values).catch(() => {
      process.stderr.write(JSON.stringify({ event: "inbox_trace_failed", stage: fields.stage }) + "\n");
    });
  }
}
