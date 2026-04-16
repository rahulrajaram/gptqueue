import { Redis } from "ioredis";
import {
  REDIS_KEYS,
  HEARTBEAT_TTL,
  HEARTBEAT_INTERVAL,
  DEFAULT_QUEUE_BOUND,
} from "./types.js";
import type { QueueMessage } from "./types.js";
import { MailboxStore } from "../core/mailbox-store.js";
import { SessionStore } from "../core/session-store.js";

export class RedisClient {
  private redis: Redis;
  private subscriber: Redis;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private _agentName: string | null;
  private _sessionId: string | null = null;
  private readonly mailbox: MailboxStore;
  private readonly sessions: SessionStore;
  readonly queueBound: number;

  get agentName(): string | null {
    return this._agentName;
  }

  get sessionId(): string | null {
    return this._sessionId;
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
    this.mailbox = new MailboxStore(this.redis, this.subscriber, this.queueBound);
    this.sessions = new SessionStore(this.redis);
  }

  requireRegistered(): string {
    if (!this._agentName) {
      throw new Error(
        "Agent not registered. Call register_agent first with a name."
      );
    }
    return this._agentName;
  }

  /**
   * Reconnect to an existing session by session_id.
   * Looks up the session in Redis and populates local state.
   * This is the fix for the cross-process registration bug:
   * a new process can resume a session without re-registering.
   */
  async reconnectSession(sessionId: string): Promise<string> {
    const session = await this.sessions.getSession(sessionId);
    if (!session) {
      throw new Error(`Session ${sessionId} not found in Redis.`);
    }

    this._sessionId = sessionId;
    this._agentName = session.agent_name;

    // Refresh the lease to prove we're alive
    this.sessions.startLeaseRefresh(sessionId);

    // Also maintain legacy heartbeat for backward compat
    this.startHeartbeat();

    return session.agent_name;
  }

  async register(
    role: string,
    name: string,
    description?: string
  ): Promise<{ name: string; session_id: string }> {
    const oldName = this._agentName;

    // Close previous session if renaming
    if (this._sessionId && oldName && name !== oldName) {
      await this.sessions.closeSession(this._sessionId);
      await this.mailbox.migrateMessages(oldName, name);

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

    // Create a new session
    const session = await this.sessions.createSession(
      name,
      role as "publisher" | "consumer" | "both",
      description
    );

    this._agentName = name;
    this._sessionId = session.session_id;

    // Legacy registry entry (for backward compat with old listAgents)
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
    await this.mailbox.ensureMailbox(name);

    // Start both session lease refresh and legacy heartbeat
    this.sessions.startLeaseRefresh(session.session_id);
    this.startHeartbeat();

    return { name, session_id: session.session_id };
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const name = this.requireRegistered();
    const beat = async () => {
      try {
        await this.redis.set(
          REDIS_KEYS.heartbeat(name),
          "alive",
          "EX",
          HEARTBEAT_TTL
        );
      } catch {
        // Swallow errors from closed connections during shutdown
      }
    };
    beat();
    this.heartbeatTimer = setInterval(beat, HEARTBEAT_INTERVAL * 1000);
  }

  /** Close the current session but preserve the mailbox and registry entry. */
  async closeCurrentSession(): Promise<string> {
    const name = this.requireRegistered();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    this.sessions.stopLeaseRefresh();

    if (this._sessionId) {
      await this.sessions.closeSession(this._sessionId);
    }

    // Remove heartbeat but keep registry and mailbox
    await this.redis.del(REDIS_KEYS.heartbeat(name));

    this._agentName = null;
    this._sessionId = null;
    return name;
  }

  /** Full unregister: close session AND delete the mailbox (destructive). */
  async unregister(): Promise<void> {
    const name = this.requireRegistered();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    this.sessions.stopLeaseRefresh();

    if (this._sessionId) {
      await this.sessions.closeSession(this._sessionId);
    }

    // Delete everything
    await this.redis.hdel(REDIS_KEYS.registry, name);
    await this.mailbox.deleteMailbox(name);
    await this.redis.del(REDIS_KEYS.heartbeat(name));

    this._agentName = null;
    this._sessionId = null;
  }

  async sendMessage(message: QueueMessage): Promise<boolean> {
    this.requireRegistered();
    return this.mailbox.send(message);
  }

  async receiveMessage(timeout: number = 5): Promise<QueueMessage | null> {
    const name = this.requireRegistered();
    return this.mailbox.receive(name, timeout);
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
      // Prefer session-based presence; fall back to legacy heartbeat
      const presence = await this.sessions.getPresence(name);
      const legacyHeartbeat = await this.redis.get(REDIS_KEYS.heartbeat(name));
      agents.push({
        name,
        role: reg.role,
        description: reg.description,
        online: presence.online || legacyHeartbeat !== null,
      });
    }
    return agents;
  }

  async getQueueStatus(
    agent?: string
  ): Promise<{ agent: string; depth: number; max_size: number }[]> {
    return this.mailbox.status(agent);
  }

  async getQueueDepth(agent?: string): Promise<number> {
    const name = agent || this.requireRegistered();
    return this.mailbox.depth(name);
  }

  async shutdown(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.sessions.stopLeaseRefresh();
    await this.redis.quit();
    await this.subscriber.quit();
  }
}
