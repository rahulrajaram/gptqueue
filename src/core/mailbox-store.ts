/**
 * MailboxStore: pure queue operations backed by Redis.
 *
 * This module owns send, receive, depth, bounded-push, and queue status.
 * It does NOT own agent identity, session state, or heartbeats.
 */

import { Redis } from "ioredis";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { SESSION_KEYS, SESSION_DEFAULTS } from "./keys.js";
import type { QueueMessage } from "../mcp-server/types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const LUA_DIR = join(__dirname, "..", "mcp-server", "lua");

export class MailboxStore {
  private readonly redis: Redis;
  private readonly subscriber: Redis;
  private readonly boundedPushScript: string;
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
  }

  /** Push a message to a target agent's mailbox with bounded retry. */
  async send(message: QueueMessage): Promise<boolean> {
    const queueKey = SESSION_KEYS.queue(message.to);
    const metaKey = SESSION_KEYS.mailboxMeta(message.to);
    const serialized = JSON.stringify(message);

    let delay = 100;
    const maxDelay = 5000;
    const maxAttempts = 10;

    for (let attempt = 0; attempt < maxAttempts; attempt++) {
      const result = await this.redis.eval(
        this.boundedPushScript,
        2,
        queueKey,
        metaKey,
        serialized,
        this.queueBound
      );
      if (result === 1) return true;

      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, maxDelay);
    }
    return false;
  }

  /** Blocking pop from an agent's mailbox. */
  async receive(
    agentName: string,
    timeout: number = 5
  ): Promise<QueueMessage | null> {
    const result = await this.subscriber.blpop(
      SESSION_KEYS.queue(agentName),
      timeout
    );
    if (!result) return null;

    const message: QueueMessage = JSON.parse(result[1]);

    const len = await this.redis.llen(SESSION_KEYS.queue(agentName));
    await this.redis.hset(
      SESSION_KEYS.mailboxMeta(agentName),
      "current_size",
      len
    );

    return message;
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

    const statuses = [];
    for (const name of targets) {
      const d = await this.redis.llen(SESSION_KEYS.queue(name));
      const meta = await this.redis.hgetall(SESSION_KEYS.mailboxMeta(name));
      statuses.push({
        agent: name,
        depth: d,
        max_size: parseInt(
          meta["max_size"] ||
            String(SESSION_DEFAULTS.DEFAULT_QUEUE_BOUND),
          10
        ),
      });
    }
    return statuses;
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

  /** Migrate messages from one mailbox to another. */
  async migrateMessages(
    fromAgent: string,
    toAgent: string
  ): Promise<number> {
    let count = 0;
    let msg: string | null;
    while (
      (msg = await this.redis.lpop(SESSION_KEYS.queue(fromAgent))) !== null
    ) {
      await this.redis.rpush(SESSION_KEYS.queue(toAgent), msg);
      count++;
    }
    return count;
  }
}
