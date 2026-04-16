import { Redis } from "ioredis";
import { EventEmitter } from "events";
import { SESSION_KEYS } from "../core/keys.js";

/**
 * Watches for incoming messages in an agent's mailbox queue using
 * Redis keyspace notifications instead of polling.
 *
 * Emits "message" with the queue depth whenever a new message arrives.
 *
 * Requires Redis to have keyspace notifications enabled for list events:
 *   CONFIG SET notify-keyspace-events Kl
 *
 * Falls back to LLEN polling if keyspace notifications are unavailable.
 */
export class RedisWatcher extends EventEmitter {
  private redis: Redis;
  private subscriber: Redis | null = null;
  private running = false;
  private readonly agentName: string;
  private readonly redisUrl: string;

  constructor(agentName: string, redisUrl?: string) {
    super();
    this.agentName = agentName;
    this.redisUrl = redisUrl || process.env.REDIS_URL || "redis://127.0.0.1:6379";
    this.redis = new Redis(this.redisUrl, { maxRetriesPerRequest: null });
  }

  async start(): Promise<void> {
    this.running = true;
    const queueKey = SESSION_KEYS.queue(this.agentName);

    // Try to enable keyspace notifications and use pub/sub
    try {
      await this.redis.config("SET", "notify-keyspace-events", "Kl");
      await this.startKeyspaceWatch(queueKey);
    } catch {
      // Keyspace notifications unavailable (e.g., managed Redis), fall back to polling
      await this.startPolling(queueKey);
    }
  }

  /** Watch via Redis keyspace notifications (SUBSCRIBE). */
  private async startKeyspaceWatch(queueKey: string): Promise<void> {
    this.subscriber = new Redis(this.redisUrl, { maxRetriesPerRequest: null });

    // Subscribe to list events on the queue key
    const db = 0;
    const channel = `__keyspace@${db}__:${queueKey}`;

    this.subscriber.on("message", async (_ch: string, event: string) => {
      if (!this.running) return;
      // rpush/lpush indicate a new message was pushed
      if (event === "rpush" || event === "lpush") {
        try {
          const len = await this.redis.llen(queueKey);
          if (len > 0) {
            this.emit("message", len);
          }
        } catch {
          // Redis error during llen, ignore
        }
      }
    });

    await this.subscriber.subscribe(channel);

    // Also check for any messages already in the queue at startup
    try {
      const len = await this.redis.llen(queueKey);
      if (len > 0) {
        this.emit("message", len);
      }
    } catch {
      // Ignore startup check errors
    }
  }

  /** Fallback: poll LLEN on the queue key. */
  private async startPolling(queueKey: string): Promise<void> {
    while (this.running) {
      try {
        const len = await this.redis.llen(queueKey);
        if (len > 0) {
          this.emit("message", len);
          // Wait before checking again to avoid spamming
          await new Promise((r) => setTimeout(r, 5000));
        } else {
          // Poll interval
          await new Promise((r) => setTimeout(r, 1000));
        }
      } catch {
        if (this.running) {
          await new Promise((r) => setTimeout(r, 2000));
        }
      }
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.subscriber) {
      await this.subscriber.quit().catch(() => {});
      this.subscriber = null;
    }
    await this.redis.quit();
  }
}
