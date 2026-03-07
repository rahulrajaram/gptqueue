import { Redis } from "ioredis";
import { readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  REDIS_KEYS,
  HEARTBEAT_TTL,
  HEARTBEAT_INTERVAL,
  DEFAULT_QUEUE_BOUND,
} from "./types.js";
import type { QueueMessage } from "./types.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

export class RedisClient {
  private redis: Redis;
  private subscriber: Redis;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private boundedPushScript: string;
  private _agentName: string | null;
  readonly queueBound: number;

  get agentName(): string | null {
    return this._agentName;
  }

  get registered(): boolean {
    return this._agentName !== null;
  }

  constructor(agentName: string | null, redisUrl?: string) {
    this._agentName = agentName;
    this.queueBound = parseInt(
      process.env.GPTQ_QUEUE_BOUND || String(DEFAULT_QUEUE_BOUND),
      10
    );
    const url = redisUrl || process.env.REDIS_URL || "redis://127.0.0.1:6379";
    this.redis = new Redis(url, { maxRetriesPerRequest: 3 });
    this.subscriber = new Redis(url, { maxRetriesPerRequest: 3 });

    // Load Lua script
    this.boundedPushScript = readFileSync(
      join(__dirname, "lua", "bounded-push.lua"),
      "utf-8"
    );
  }

  requireRegistered(): string {
    if (!this._agentName) {
      throw new Error(
        "Agent not registered. Call register_agent first with a name."
      );
    }
    return this._agentName;
  }

  async register(
    role: string,
    name: string,
    description?: string
  ): Promise<string> {
    const oldName = this._agentName;

    if (oldName && name !== oldName) {
      // Migrate pending messages from old queue to new queue
      const oldQueue = REDIS_KEYS.queue(oldName);
      const newQueue = REDIS_KEYS.queue(name);
      let msg: string | null;
      while ((msg = await this.redis.lpop(oldQueue)) !== null) {
        await this.redis.rpush(newQueue, msg);
      }

      // Clean up old keys
      await this.redis.hdel(REDIS_KEYS.registry, oldName);
      await this.redis.del(
        REDIS_KEYS.meta(oldName),
        REDIS_KEYS.heartbeat(oldName)
      );
      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
      }
    }

    this._agentName = name;

    const registration = {
      name,
      role,
      description: description || undefined,
      registered_at: new Date().toISOString(),
      pid: process.pid,
    };
    await this.redis.hset(
      REDIS_KEYS.registry,
      name,
      JSON.stringify(registration)
    );
    await this.redis.hset(REDIS_KEYS.meta(name), "max_size", this.queueBound);
    this.startHeartbeat();
    return name;
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const name = this.requireRegistered();
    const beat = async () => {
      await this.redis.set(
        REDIS_KEYS.heartbeat(name),
        "alive",
        "EX",
        HEARTBEAT_TTL
      );
    };
    beat();
    this.heartbeatTimer = setInterval(beat, HEARTBEAT_INTERVAL * 1000);
  }

  async unregister(): Promise<void> {
    const name = this.requireRegistered();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    await this.redis.hdel(REDIS_KEYS.registry, name);
    await this.redis.del(
      REDIS_KEYS.queue(name),
      REDIS_KEYS.meta(name),
      REDIS_KEYS.heartbeat(name)
    );
    this._agentName = null;
  }

  async sendMessage(message: QueueMessage): Promise<boolean> {
    this.requireRegistered();
    const queueKey = REDIS_KEYS.queue(message.to);
    const metaKey = REDIS_KEYS.meta(message.to);
    const serialized = JSON.stringify(message);

    // Try bounded push with exponential backoff
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

      // Queue full, backoff
      await new Promise((r) => setTimeout(r, delay));
      delay = Math.min(delay * 2, maxDelay);
    }
    return false;
  }

  async receiveMessage(timeout: number = 5): Promise<QueueMessage | null> {
    const name = this.requireRegistered();
    const result = await this.subscriber.blpop(
      REDIS_KEYS.queue(name),
      timeout
    );
    if (!result) return null;

    const message: QueueMessage = JSON.parse(result[1]);

    // Update meta
    const len = await this.redis.llen(REDIS_KEYS.queue(name));
    await this.redis.hset(REDIS_KEYS.meta(name), "current_size", len);

    return message;
  }

  async listAgents(): Promise<
    Array<{
      name: string;
      role: string;
      description?: string;
      online: boolean;
    }>
  > {
    const registry = await this.redis.hgetall(REDIS_KEYS.registry);
    const agents = [];
    for (const [name, json] of Object.entries(registry)) {
      const reg = JSON.parse(json);
      const heartbeat = await this.redis.get(REDIS_KEYS.heartbeat(name));
      agents.push({
        name,
        role: reg.role,
        description: reg.description,
        online: heartbeat !== null,
      });
    }
    return agents;
  }

  async getQueueStatus(
    agent?: string
  ): Promise<{ agent: string; depth: number; max_size: number }[]> {
    const targets = agent
      ? [agent]
      : Object.keys(await this.redis.hgetall(REDIS_KEYS.registry));

    const statuses = [];
    for (const name of targets) {
      const depth = await this.redis.llen(REDIS_KEYS.queue(name));
      const meta = await this.redis.hgetall(REDIS_KEYS.meta(name));
      statuses.push({
        agent: name,
        depth,
        max_size: parseInt(
          meta["max_size"] || String(DEFAULT_QUEUE_BOUND),
          10
        ),
      });
    }
    return statuses;
  }

  async getQueueDepth(agent?: string): Promise<number> {
    const name = agent || this.requireRegistered();
    return this.redis.llen(REDIS_KEYS.queue(name));
  }

  async shutdown(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    await this.redis.quit();
    await this.subscriber.quit();
  }
}
