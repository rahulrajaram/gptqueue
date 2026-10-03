import { Redis } from "ioredis";
import type { QueueMessage } from "./types.js";
import { SESSION_KEYS } from "./keys.js";

export type InboxEventType = QueueMessage["type"];
export type InboxTraceStage = "activation_requested" | "activation_deferred" | "turn_started" | "activation_queued" | "activation_failed" | "task_claimed" | "task_acknowledged" | "reply_sent" | "runtime_bound" | "runtime_unbound";
export type InboxTraceFields = Readonly<{ stage: InboxTraceStage; timestamp: string; message_id?: string; in_reply_to?: string; claim_id?: string; operation_id?: string; runtime_id?: string; turn_id?: string; code?: string }>;
const stream = SESSION_KEYS.inboxEvents;
const traceStream = SESSION_KEYS.inboxTrace;
const outstanding = SESSION_KEYS.outstanding;
const awaitsOutstanding = (message: QueueMessage): message is QueueMessage & { payload: { in_reply_to: string } } =>
  (message.type === "result" || message.type === "error") && !!message.payload.in_reply_to;

export class InboxEvents {
  constructor(private readonly redis: Redis) {}

  async pending(agent: string): Promise<readonly QueueMessage[]> {
    const raw = await this.redis.lrange(SESSION_KEYS.queue(agent), 0, -1);
    const candidates = raw.flatMap((value) => {
      let message: QueueMessage;
      try { message = JSON.parse(value) as QueueMessage; } catch { return []; }
      if (message?.to !== agent || typeof message.id !== "string" || !message.payload) return [];
      return message.type === "task" || awaitsOutstanding(message) ? [message] : [];
    });
    // One MGET resolves every reply's outstanding-request owner instead of a GET per message.
    const replies = candidates.filter(awaitsOutstanding);
    const owners = replies.length === 0 ? [] : await this.redis.mget(replies.map((m) => outstanding(m.to, m.payload.in_reply_to)));
    const ownerOf = new Map<QueueMessage, string | null>(replies.map((m, i) => [m, owners[i] ?? null]));
    return Object.freeze(candidates.filter((m) => m.type === "task" || ownerOf.get(m) === m.from));
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
