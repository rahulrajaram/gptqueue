/**
 * MailboxStore: pure queue operations backed by Redis.
 *
 * This module owns send, receive, depth, bounded-push, and queue status.
 * It does NOT own agent identity, session state, or heartbeats.
 */

import { Redis } from "ioredis";
import { readFileSync } from "fs";
import { join } from "path";
import { SESSION_KEYS, SESSION_DEFAULTS, DLQ_KEYS, DLQ_PROVISIONAL } from "./keys.js";
import type { QueueMessage } from "./types.js";
import { LUA_DIR } from "./stored-read.js";
import { taskSubject } from "./task-ledger.js";
import { composeWatchdogPush } from "./watchdog-push.js";
import { GptQueueError } from "../mcp-server/errors.js";

const eventKey = SESSION_KEYS.inboxEvents;
const outstandingKey = SESSION_KEYS.outstanding;
const outstandingKeyFor = (message: QueueMessage): string =>
  outstandingKey(message.type === "task" ? message.from : message.to, message.type === "task" ? message.id : message.payload.in_reply_to || message.id);

/**
 * COR-F2: follow a recipient's forwarding tombstone. A name adopted into its
 * bound mailbox forwards there (one hop: a bound mailbox is never itself
 * tombstoned; value "to:<target>"); a retired name ("retired") refuses with RECIPIENT_RETIRED, never "sent".
 */
const forwardedMessage = (message: QueueMessage, tombstone: string): QueueMessage => {
  const target = tombstone.startsWith("to:") ? tombstone.slice(3) : "";
  if (target === "" || target === message.to) {
    throw new GptQueueError("RECIPIENT_RETIRED",
      `Recipient '${message.to}' was a temporary startup identity that has been retired; resend to the agent's bound mailbox.`,
      { recipient: message.to });
  }
  return { ...message, to: target };
};

const eligible = (message: QueueMessage): boolean =>
  message.type === "task" || (message.type === "result" || message.type === "error") && !!message.payload.in_reply_to;


export class MailboxStore {
  private readonly redis: Redis;
  private readonly subscriber: Redis;
  private readonly boundedPushScript: string;
  private readonly boundedPushIdempotentScript: string;
  private readonly watchdogPushScript: string;
  private readonly migrateMessagesScript: string;
  /** Blocking connections owned by in-flight receives, broken on client shutdown. */
  private readonly receiveConnections = new Set<Redis>();
  private receivesClosed = false;
  readonly queueBound: number;

  constructor(redis: Redis, subscriber: Redis, queueBound?: number) {
    this.redis = redis;
    this.subscriber = subscriber;
    this.queueBound =
      queueBound ??
      parseInt(
        process.env.GPTQ_QUEUE_BOUND ||
          String(SESSION_DEFAULTS.DEFAULT_QUEUE_BOUND),
        10
      );

    this.boundedPushScript = readFileSync(
      join(LUA_DIR, "bounded-push.lua"),
      "utf-8"
    );
    this.boundedPushIdempotentScript = readFileSync(
      join(LUA_DIR, "bounded-push-idempotent.lua"),
      "utf-8"
    );
    this.watchdogPushScript = composeWatchdogPush(this.boundedPushIdempotentScript);
    this.migrateMessagesScript = readFileSync(
      join(LUA_DIR, "migrate-messages.lua"),
      "utf-8"
    );
  }

  /** Push a message to a target agent's mailbox with bounded retry (a full mailbox is retried up to `maxAttempts` times). */
  async send(message: QueueMessage, maxAttempts = 10): Promise<boolean> {
    let current = message;
    let delay = 100;
    const maxDelay = 5000;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const raw = await this.redis.eval(
        this.boundedPushScript,
        7,
        SESSION_KEYS.queue(current.to),
        SESSION_KEYS.mailboxMeta(current.to),
        eventKey(current.to),
        outstandingKeyFor(current),
        SESSION_KEYS.forward(current.to),
        SESSION_KEYS.taskLedger(current.id),
        SESSION_KEYS.taskAnswered(current.payload.in_reply_to || current.id),
        JSON.stringify(current),
        this.queueBound,
        current.id,
        current.type,
        eligible(current) ? 1 : 0,
        current.type === "task" ? 1 : 0,
        current.to,
        current.from,
        taskSubject(current.payload.content),
        new Date().toISOString()
      ) as [number, string];
      if (raw[0] === 1) return true;
      if (raw[0] === -1) {
        current = forwardedMessage(current, raw[1]);
        continue;
      }

      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, maxDelay);
    }
    return false;
  }

  /**
   * Push once per (sender, recipient, caller key), returning the original id
   * on retries. Reusing a key for a different recipient is a different send.
   * Legacy sender-only keys written before this scoping are not consulted:
   * honoring one could drop a send to a new recipient, while ignoring one can
   * at worst duplicate a retry that straddles the upgrade (at-least-once).
   */
  async sendIdempotent(
    message: QueueMessage,
    sender: string,
    idempotencyKey: string,
    maxAttempts = 10
  ): Promise<{ status: "sent" | "duplicate" | "full"; messageId: string }> {
    let current = message;
    let delay = 100;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const raw = await this.redis.eval(
        this.boundedPushIdempotentScript,
        8,
        SESSION_KEYS.queue(current.to),
        SESSION_KEYS.mailboxMeta(current.to),
        SESSION_KEYS.idempotencyFor(sender, current.to, idempotencyKey),
        eventKey(current.to),
        outstandingKeyFor(current),
        SESSION_KEYS.forward(current.to),
        SESSION_KEYS.taskLedger(current.id),
        SESSION_KEYS.taskAnswered(current.payload.in_reply_to || current.id),
        JSON.stringify(current),
        this.queueBound,
        current.id,
        86400,
        eligible(current) ? 1 : 0,
        current.type,
        current.type === "task" ? 1 : 0,
        current.to,
        current.from,
        taskSubject(current.payload.content),
        new Date().toISOString()
      ) as [number, string];
      if (raw[0] === 1) return { status: "sent", messageId: raw[1] };
      if (raw[0] === 2) return { status: "duplicate", messageId: raw[1] };
      if (raw[0] === -1) {
        current = forwardedMessage(current, raw[1]);
        continue;
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 5000);
    }
    return { status: "full", messageId: message.id };
  }

  /**
   * Watchdog reminder: the normal idempotent send plus per-recipient rate
   * admission in the same script, timed by Redis TIME (see core/watchdog-push.ts).
   * `limited` and `full` change nothing; `duplicate` returns the original id.
   */
  async sendWatchdogTask(
    message: QueueMessage,
    sender: string,
    idempotencyKey: string,
    rate: Readonly<{ fireKey: string; cap: number }>,
    maxAttempts = 10
  ): Promise<{ status: "sent" | "duplicate" | "full" | "limited"; messageId: string }> {
    let current = message;
    let delay = 100;
    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const raw = await this.redis.eval(
        this.watchdogPushScript,
        9,
        SESSION_KEYS.queue(current.to),
        SESSION_KEYS.mailboxMeta(current.to),
        SESSION_KEYS.idempotencyFor(sender, current.to, idempotencyKey),
        eventKey(current.to),
        outstandingKeyFor(current),
        SESSION_KEYS.forward(current.to),
        SESSION_KEYS.taskLedger(current.id),
        SESSION_KEYS.taskAnswered(current.payload.in_reply_to || current.id),
        SESSION_KEYS.watchdogSent(current.to),
        JSON.stringify(current),
        this.queueBound,
        current.id,
        86400,
        eligible(current) ? 1 : 0,
        current.type,
        current.type === "task" ? 1 : 0,
        current.to,
        current.from,
        taskSubject(current.payload.content),
        new Date().toISOString(),
        rate.fireKey,
        rate.cap
      ) as [number, string];
      if (raw[0] === 1) return { status: "sent", messageId: raw[1] };
      if (raw[0] === 2) return { status: "duplicate", messageId: raw[1] };
      if (raw[0] === 3) return { status: "limited", messageId: raw[1] };
      if (raw[0] === -1) {
        current = forwardedMessage(current, raw[1]);
        continue;
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
      delay = Math.min(delay * 2, 5000);
    }
    return { status: "full", messageId: message.id };
  }

  /** Blocking pop from an agent's mailbox. */
  async receive(
    agentName: string,
    timeout: number = 5,
    signal?: AbortSignal
  ): Promise<QueueMessage | null> {
    const key = SESSION_KEYS.queue(agentName);
    // Every pop gets its own connection: a long or unbounded BLPOP (timeout 0
    // blocks until a message arrives) must never hold the shared subscriber
    // and stall other receives behind it.
    const result = await this.cancellablePop(key, timeout, signal);
    if (!result) return null;

    let message: QueueMessage;
    try {
      message = JSON.parse(result[1]) as QueueMessage;
    } catch {
      // BLPOP already removed it: park the unparseable payload in the DLQ so
      // it stays inspectable instead of vanishing inside a thrown error.
      await this.redis
        .multi()
        .lpush(DLQ_KEYS.list(agentName), result[1])
        .ltrim(DLQ_KEYS.list(agentName), 0, DLQ_PROVISIONAL.DLQ_MAX_LENGTH - 1)
        .exec();
      return null;
    }

    // One round trip: record the post-pop depth for get_queue_status.
    await this.redis.eval(
      "return redis.call('HSET', KEYS[2], 'current_size', redis.call('LLEN', KEYS[1]))",
      2,
      SESSION_KEYS.queue(agentName),
      SESSION_KEYS.mailboxMeta(agentName)
    );

    return message;
  }

  /**
   * Break every in-flight receive's blocking connection and refuse new ones.
   * The owning client calls this on shutdown: a receive whose caller passed no
   * signal is otherwise owned by nobody, and would stay parked in BLPOP and
   * later consume a message whose response can no longer be delivered.
   */
  closeReceives(): void {
    this.receivesClosed = true;
    for (const connection of this.receiveConnections) connection.disconnect();
    this.receiveConnections.clear();
  }

  private throwIfReceivesClosed(): void {
    if (this.receivesClosed) throw new Error("receive refused: the client is shutting down");
  }

  /** Own the blocking socket so cancelling one receive cannot affect another. */
  private async cancellablePop(
    key: string,
    timeout: number,
    signal: AbortSignal | undefined
  ): Promise<[string, string] | null> {
    signal?.throwIfAborted();
    this.throwIfReceivesClosed();
    const connection = this.subscriber.duplicate({
      lazyConnect: true,
      // A destructive pop must never be replayed after a connection failure.
      retryStrategy: () => null,
      autoResendUnfulfilledCommands: false,
      maxRetriesPerRequest: 0,
      enableOfflineQueue: false,
    });
    const cancel = () => connection.disconnect();
    signal?.addEventListener("abort", cancel, { once: true });
    this.receiveConnections.add(connection);
    try {
      await connection.connect();
      signal?.throwIfAborted();
      this.throwIfReceivesClosed();
      return await connection.blpop(key, timeout);
    } finally {
      this.receiveConnections.delete(connection);
      signal?.removeEventListener("abort", cancel);
      connection.disconnect();
    }
  }

  /** Get queue depth for a named agent. */
  async depth(agentName: string): Promise<number> {
    return this.redis.llen(SESSION_KEYS.queue(agentName));
  }

  /** Get queue status for one or all agents. */
  async status(
    agentName?: string
  ): Promise<{ agent: string; depth: number; max_size: number }[]> {
    const targets = agentName
      ? [agentName]
      : Object.keys(await this.redis.hgetall(SESSION_KEYS.registry));

    if (targets.length === 0) return [];
    // One pipelined round trip for every agent's depth and bound.
    const pipe = this.redis.pipeline();
    for (const name of targets) {
      pipe.llen(SESSION_KEYS.queue(name)).hget(SESSION_KEYS.mailboxMeta(name), "max_size");
    }
    const results = (await pipe.exec()) ?? [];
    return targets.map((name, i) => {
      const [depthError, depth] = results[i * 2] ?? [null, 0];
      const [boundError, bound] = results[i * 2 + 1] ?? [null, null];
      if (depthError) throw depthError;
      if (boundError) throw boundError;
      return {
        agent: name,
        depth: Number(depth),
        max_size: parseInt(
          (bound as string | null) || String(SESSION_DEFAULTS.DEFAULT_QUEUE_BOUND),
          10
        ),
      };
    });
  }

  /** Initialize mailbox metadata for an agent. */
  async ensureMailbox(agentName: string): Promise<void> {
    const metaKey = SESSION_KEYS.mailboxMeta(agentName);
    const exists = await this.redis.exists(metaKey);
    if (!exists) {
      await this.redis.hset(metaKey, {
        max_size: this.queueBound,
        created_at: new Date().toISOString(),
      });
    }
  }

  /** Delete a mailbox entirely (admin action). */
  async deleteMailbox(agentName: string): Promise<void> {
    await this.redis.del(
      SESSION_KEYS.queue(agentName),
      SESSION_KEYS.mailboxMeta(agentName)
    );
  }

  /**
   * Migrate every message from one mailbox to another, atomically and in
   * order (review finding F2). A single Lua script moves the whole source
   * list to the destination tail: either every message has moved or none
   * has, so a failure or crash mid-migration can never strand or lose a
   * message the way the legacy per-message LPOP/RPUSH loop could. Returns
   * the number of messages transferred. With `sourceClaimsIndex`, the
   * transfer is refused while that index holds any claim; callers recover
   * expired claims first. With `deadLetterBound`, the source's dead-letter
   * queue moves too, in the same step, to the head of the destination's,
   * which is then trimmed to that bound (FIX5).
   */
  async migrateMessages(
    fromAgent: string,
    toAgent: string,
    sourceClaimsIndex?: string,
    deadLetterBound?: number
  ): Promise<number> {
    const keys = [SESSION_KEYS.queue(fromAgent), SESSION_KEYS.queue(toAgent)];
    if (sourceClaimsIndex) keys.push(sourceClaimsIndex);
    const args: number[] = [];
    if (deadLetterBound !== undefined) {
      keys.push(DLQ_KEYS.list(fromAgent), DLQ_KEYS.list(toAgent));
      args.push(deadLetterBound);
    }
    const raw = await this.redis.eval(this.migrateMessagesScript, keys.length, ...keys, ...args);
    const count = typeof raw === "number" ? raw : parseInt(String(raw), 10);
    if (count === -1) {
      throw new Error(
        `cannot move '${fromAgent}' to '${toAgent}' while it has unexpired claimed tasks; acknowledge them, or wait for their claims to expire, first`
      );
    }
    return count;
  }
}
