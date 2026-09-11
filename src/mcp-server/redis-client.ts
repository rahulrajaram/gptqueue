import { Redis } from "ioredis";
import { createHash } from "node:crypto";
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

  /** Atomically attach this provisional session to an offline, exactly mapped mailbox. */
  async adoptIdentity(source: string, target: string, mappingKey: string, expectedMapping: string): Promise<void> {
    if (!this._sessionId || this._agentName !== source || source === target) throw new Error("Continuity source ownership mismatch");
    const wrapper = (name: string) => `gptq:experimental-wrapper-claim:${createHash("sha256").update(name).digest("hex")}`;
    // Target-occupancy guard set mirrors applyContinuity's occupied()
    // (core/mailbox-continuity.ts) exactly: runtime-binding, heartbeat,
    // wrapper claim, live session leases, and unacknowledged claims-index
    // entries (review finding F5 — the claims-index check was previously
    // missing here, letting the runtime path adopt a claimed target that the
    // operator path would refuse). Source-side guards (-4, -7) are
    // runtime-path-specific (sole-session ownership, quiet source) and have
    // no operator-path counterpart.
    const result = await this.redis.eval(`
      if redis.call('GET', KEYS[1]) ~= ARGV[4] then return -1 end
      local sourceRaw = redis.call('HGET', KEYS[2], ARGV[1])
      local targetRaw = redis.call('HGET', KEYS[2], ARGV[2])
      if not sourceRaw or not targetRaw then return -2 end
      local sourceReg = cjson.decode(sourceRaw)
      local targetReg = cjson.decode(targetRaw)
      if redis.call('HGET', KEYS[3], 'agent_name') ~= ARGV[1] or redis.call('EXISTS', KEYS[4]) ~= 1 then return -3 end
      if redis.call('SCARD', KEYS[5]) ~= 1 or redis.call('SISMEMBER', KEYS[5], ARGV[3]) ~= 1 then return -4 end
      if redis.call('EXISTS', KEYS[7]) == 1 or redis.call('EXISTS', KEYS[8]) == 1 or redis.call('EXISTS', KEYS[9]) == 1 or redis.call('ZCARD', KEYS[15]) > 0 then return -5 end
      for _, sid in ipairs(redis.call('SMEMBERS', KEYS[6])) do
        if redis.call('EXISTS', 'gptq:lease:'..sid) == 1 then return -6 end
      end
      if redis.call('LLEN', KEYS[10]) > 0 or redis.call('ZCARD', KEYS[11]) > 0 or redis.call('EXISTS', KEYS[12]) == 1 or redis.call('EXISTS', KEYS[13]) == 1 then return -7 end
      targetReg.pid = sourceReg.pid
      targetReg.metadata = targetReg.metadata or {}
      if sourceReg.metadata then
        targetReg.metadata.protocol_version = sourceReg.metadata.protocol_version
        targetReg.metadata.tool_names = sourceReg.metadata.tool_names
      end
      redis.call('HSET', KEYS[3], 'agent_name', ARGV[2])
      redis.call('SREM', KEYS[5], ARGV[3])
      redis.call('SADD', KEYS[6], ARGV[3])
      redis.call('HSET', KEYS[2], ARGV[2], cjson.encode(targetReg))
      redis.call('HDEL', KEYS[2], ARGV[1])
      redis.call('DEL', KEYS[14])
      return 1
    `, 15, mappingKey, SESSION_KEYS.registry, SESSION_KEYS.session(this._sessionId), SESSION_KEYS.lease(this._sessionId),
      SESSION_KEYS.agentSessions(source), SESSION_KEYS.agentSessions(target), SESSION_KEYS.heartbeat(target),
      `gptq:runtime-binding:${target}`, wrapper(target), SESSION_KEYS.queue(source), `gptq:claims-index:${source}`,
      `gptq:outbound-activity:${source}`, wrapper(source), SESSION_KEYS.heartbeat(source), `gptq:claims-index:${target}`, source, target, this._sessionId, expectedMapping);
    if (result !== 1) throw new Error(`Continuity adoption refused (${result})`);
    this._agentName = target;
    this.startHeartbeat();
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
    const oldSessionId = this._sessionId;
    const renaming =
      oldSessionId !== null && oldName !== null && name !== oldName;

    // F2 (atomic, recoverable rename): the old session/registry state is
    // kept intact until the replacement session exists AND the mailbox
    // transfer has atomically completed. Ordering:
    //   1. create the new session — a failure changes nothing (the caller
    //      stays registered under the old name with the mailbox untouched);
    //   2. migrate the mailbox with one all-or-nothing Lua transfer — on
    //      failure the messages are provably still under the old name, the
    //      just-created session is rolled back, and the error propagates;
    //   3. only then retire the old session/registry/heartbeat state — a
    //      failure here cannot lose messages (worst case: stale old-name
    //      keys that lease expiry cleans up), so cleanup is best-effort.
    if (renaming) {
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

    if (renaming) {
      try {
        await this.mailbox.migrateMessages(oldName!, name);
      } catch (error) {
        // The transfer is all-or-nothing: every message is still under the
        // old name. Roll the new session back so the visible state is
        // unchanged, then surface the failure.
        await this.sessionStore
          .closeSession(session.session_id)
          .catch(() => {});
        await this.redis.hdel(SESSION_KEYS.registry, name).catch(() => {});
        throw error;
      }

      // Messages are safe under the new name; retire the old identity.
      await this.sessionStore.closeSession(oldSessionId!).catch(() => {});
      await this.redis.hdel(SESSION_KEYS.registry, oldName!).catch(() => {});
      await this.redis
        .del(SESSION_KEYS.mailboxMeta(oldName!), SESSION_KEYS.heartbeat(oldName!))
        .catch(() => {});
    }

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
