import { Redis } from "ioredis";
import type { QueueMessage } from "../mcp-server/types.js";

export type InboxEventType = QueueMessage["type"];
export type InboxTraceStage = "activation_requested" | "turn_started" | "activation_queued" | "activation_failed" | "task_claimed" | "task_acknowledged" | "reply_sent" | "runtime_bound" | "runtime_unbound";
export type InboxTraceFields = Readonly<{ stage: InboxTraceStage; timestamp: string; message_id?: string; in_reply_to?: string; claim_id?: string; operation_id?: string; runtime_id?: string; turn_id?: string; code?: string }>;
const stream = (agent: string) => `gptq:inbox-events:${agent}`;
const traceStream = (agent: string) => `gptq:inbox-trace:${agent}`;
const outstanding = (agent: string, id: string) => `gptq:outstanding:${agent}:${id}`;
const eligible = async (redis: Redis, message: QueueMessage): Promise<boolean> =>
  message.type === "task" || ((message.type === "result" || message.type === "error") &&
    !!message.payload.in_reply_to && await redis.get(outstanding(message.to, message.payload.in_reply_to)) === message.from);

export class InboxEvents {
  constructor(private readonly redis: Redis) {}

  async pending(agent: string): Promise<readonly QueueMessage[]> {
    const raw = await this.redis.lrange(`gptq:q:${agent}`, 0, -1);
    const messages: QueueMessage[] = [];
    for (const value of raw) {
      let message: QueueMessage;
      try { message = JSON.parse(value) as QueueMessage; } catch { continue; }
      if (message?.to !== agent || typeof message.id !== "string" || !message.payload) continue;
      if (await eligible(this.redis, message)) messages.push(message);
    }
    return Object.freeze(messages);
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
