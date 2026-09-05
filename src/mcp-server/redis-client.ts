import { Redis } from "ioredis";
import { HEARTBEAT_TTL, HEARTBEAT_INTERVAL } from "./types.js";
import type { QueueMessage } from "./types.js";
import { SESSION_KEYS, SESSION_DEFAULTS } from "../core/keys.js";
import { MailboxStore } from "../core/mailbox-store.js";
import { SessionStore } from "../core/session-store.js";
import { CustodyStore } from "../core/custody-store.js";
import { ActorDirectory } from "../core/actor-directory.js";
import { WakeLeaseStore } from "../core/wake-lease.js";
import { TaskClaimStore } from "../core/task-claim-store.js";
import { discoveryRecord, type AgentDiscoveryMetadata, type AgentDiscoveryRecord } from "../core/agent-discovery.js";

export class RedisClient {
  private redis: Redis;
  private subscriber: Redis;
  private connectionsForcedClosed = false;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private _agentName: string | null;
  private _sessionId: string | null = null;
  private readonly mailbox: MailboxStore;
  private readonly sessionStore: SessionStore;
  private readonly custodyStore: CustodyStore;
  private readonly actorDirectoryStore: ActorDirectory;
  private readonly wakeLeaseStore: WakeLeaseStore;
  private readonly taskClaimStore: TaskClaimStore;
  readonly queueBound: number;

  /** Internal adapter connection; lifecycle remains owned by this client. */
  get adapterConnection(): Redis {
    return this.redis;
  }

  get custody(): CustodyStore {
    return this.custodyStore;
  }

  get sessions(): SessionStore {
    return this.sessionStore;
  }

  get actorDirectory(): ActorDirectory {
    return this.actorDirectoryStore;
  }

  get wakeLease(): WakeLeaseStore {
    return this.wakeLeaseStore;
  }

  get taskClaim(): TaskClaimStore {
    return this.taskClaimStore;
  }

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
      process.env.GPTQ_QUEUE_BOUND || String(SESSION_DEFAULTS.DEFAULT_QUEUE_BOUND),
      10
    );
    const url = redisUrl || process.env.REDIS_URL || "redis://127.0.0.1:6379";
    this.redis = new Redis(url, { maxRetriesPerRequest: 3 });
    this.subscriber = new Redis(url, { maxRetriesPerRequest: 3 });
    this.mailbox = new MailboxStore(this.redis, this.subscriber, this.queueBound);
    this.sessionStore = new SessionStore(this.redis);
    this.custodyStore = new CustodyStore(this.redis);
    this.actorDirectoryStore = new ActorDirectory(this.redis);
    this.wakeLeaseStore = new WakeLeaseStore(this.redis);
    this.taskClaimStore = new TaskClaimStore(this.redis);
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
    const session = await this.sessionStore.getSession(sessionId);
    if (!session) {
      throw new Error(`Session ${sessionId} not found in Redis.`);
    }

    this._sessionId = sessionId;
    this._agentName = session.agent_name;

    // Refresh the lease to prove we're alive
    this.sessionStore.startLeaseRefresh(sessionId);

    // Also maintain legacy heartbeat for backward compat
    this.startHeartbeat();

    return session.agent_name;
  }

  async register(
    role: string,
    name: string,
    description?: string,
    metadata?: AgentDiscoveryMetadata
  ): Promise<{ name: string; session_id: string }> {
    const oldName = this._agentName;

    // Close previous session if renaming
    if (this._sessionId && oldName && name !== oldName) {
      await this.sessionStore.closeSession(this._sessionId);
      await this.mailbox.migrateMessages(oldName, name);

      await this.redis.hdel(SESSION_KEYS.registry, oldName);
      await this.redis.del(
        SESSION_KEYS.mailboxMeta(oldName),
        SESSION_KEYS.heartbeat(oldName)
      );
      if (this.heartbeatTimer) {
        clearInterval(this.heartbeatTimer);
        this.heartbeatTimer = null;
      }
    }

    // Create a new session
    const session = await this.sessionStore.createSession(
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
      ...(metadata ? {
        metadata: {
          label: metadata.label,
          uuid: metadata.uuid,
          client: metadata.client,
          working_directory: metadata.working_directory,
        },
      } : {}),
    };
    await this.redis.hset(
      SESSION_KEYS.registry,
      name,
      JSON.stringify(registration)
    );
    await this.mailbox.ensureMailbox(name);

    // Start both session lease refresh and legacy heartbeat
    this.sessionStore.startLeaseRefresh(session.session_id);
    this.startHeartbeat();

    return { name, session_id: session.session_id };
  }

  private startHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    const name = this.requireRegistered();
    const beat = async () => {
      try {
        await this.redis.set(
          SESSION_KEYS.heartbeat(name),
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
    this.sessionStore.stopLeaseRefresh();

    if (this._sessionId) {
      await this.sessionStore.closeSession(this._sessionId);
    }

    // Remove heartbeat but keep registry and mailbox
    await this.redis.del(SESSION_KEYS.heartbeat(name));

    this._agentName = null;
    this._sessionId = null;
    return name;
  }

  /** Full unregister: close session AND delete the mailbox (destructive). */
  async unregister(): Promise<void> {
    const name = this.requireRegistered();
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    this.sessionStore.stopLeaseRefresh();

    if (this._sessionId) {
      await this.sessionStore.closeSession(this._sessionId);
    }

    // Delete everything
    await this.redis.hdel(SESSION_KEYS.registry, name);
    await this.mailbox.deleteMailbox(name);
    await this.redis.del(SESSION_KEYS.heartbeat(name));

    this._agentName = null;
    this._sessionId = null;
  }

  async sendMessage(message: QueueMessage): Promise<boolean> {
    this.requireRegistered();
    return this.mailbox.send(message);
  }

  async sendMessageIdempotent(message: QueueMessage, idempotencyKey: string) {
    const sender = this.requireRegistered();
    return this.mailbox.sendIdempotent(message, sender, idempotencyKey);
  }

  async receiveMessage(timeout: number = 5, signal?: AbortSignal): Promise<QueueMessage | null> {
    const name = this.requireRegistered();
    return this.mailbox.receive(name, timeout, signal);
  }

  async listAgents(): Promise<AgentDiscoveryRecord[]> {
    const registry = await this.redis.hgetall(SESSION_KEYS.registry);
    const agents = [];
    for (const [name, json] of Object.entries(registry)) {
      let parsed: unknown;
      try { parsed = JSON.parse(json); } catch { continue; }
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) continue;
      const reg = parsed as Record<string, unknown>;
      // Prefer session-based presence; fall back to legacy heartbeat
      const presence = await this.sessionStore.getPresence(name);
      const legacyHeartbeat = await this.redis.get(SESSION_KEYS.heartbeat(name));
      agents.push(discoveryRecord({
        name,
        role: typeof reg.role === "string" ? reg.role : "",
        description: typeof reg.description === "string" ? reg.description : undefined,
        online: presence.online || legacyHeartbeat !== null,
        registered_at: reg.registered_at,
        pid: reg.pid,
        metadata: reg.metadata,
      }));
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
    this.sessionStore.stopLeaseRefresh();
    if (this.connectionsForcedClosed) return;
    await this.redis.quit();
    await this.subscriber.quit();
  }

  /** Immediately break both connections when a bounded caller must stop. */
  forceDisconnect(): void {
    if (this.connectionsForcedClosed) return;
    this.connectionsForcedClosed = true;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
    this.sessionStore.stopLeaseRefresh();
    this.redis.disconnect();
    this.subscriber.disconnect();
  }
}
