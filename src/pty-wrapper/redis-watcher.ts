import { Redis } from "ioredis";
import { EventEmitter } from "events";
import { REDIS_KEYS } from "../mcp-server/types.js";

export class RedisWatcher extends EventEmitter {
  private redis: Redis;
  private running = false;
  private readonly agentName: string;

  constructor(agentName: string, redisUrl?: string) {
    super();
    this.agentName = agentName;
    const url = redisUrl || process.env.REDIS_URL || "redis://127.0.0.1:6379";
    this.redis = new Redis(url, { maxRetriesPerRequest: null });
  }

  /** Start the BLPOP watch loop. Emits "message" when a message arrives. */
  async start(): Promise<void> {
    this.running = true;
    const queueKey = REDIS_KEYS.queue(this.agentName);

    while (this.running) {
      try {
        // Peek (don't consume) -- we just want to know there's a message.
        // The agent will consume it via receive_message MCP tool.
        const len = await this.redis.llen(queueKey);
        if (len > 0) {
          this.emit("message", len);
          // Wait before checking again to avoid spamming
          await new Promise((r) => setTimeout(r, 5000));
        } else {
          // Poll interval
          await new Promise((r) => setTimeout(r, 1000));
        }
      } catch (err) {
        if (this.running) {
          // Connection error, retry after delay
          await new Promise((r) => setTimeout(r, 2000));
        }
      }
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.redis.quit();
  }
}
