/**
 * SessionStore: Redis-backed session lifecycle management.
 *
 * Owns session creation, lease refresh, session close, and presence queries.
 * Does NOT own queue/mailbox operations (see MailboxStore).
 */

import { Redis, type ChainableCommander } from "ioredis";
import { v4 as uuidv4 } from "uuid";
import { SESSION_KEYS, SESSION_DEFAULTS } from "./keys.js";
import type { SessionRecord, LeaseState, AgentPresence } from "./types.js";

/** KEYS order shared by the registry rollback scripts below. */
const REGISTRY_ROLLBACK_KEYS = [
  SESSION_KEYS.registry,
  SESSION_KEYS.registryPending,
  SESSION_KEYS.registryRestore,
] as const;

/**
 * Renew a lease only while its session record exists, re-adding the session
 * to its agent's set. A closed or pruned session is never resurrected as an
 * orphan record, and a session that merely stalled past its TTL comes back.
 */
const REFRESH_LEASE_SCRIPT = `
local agent = redis.call('HGET', KEYS[1], 'agent_name')
if not agent or agent == '' then return 0 end
redis.call('SET', KEYS[2], 'alive', 'EX', ARGV[2])
redis.call('HSET', KEYS[1], 'last_seen', ARGV[3])
redis.call('SADD', 'gptq:agent-sessions:' .. agent, ARGV[1])
return 1
`;

/** Remove an agent's sessions whose lease has expired, atomically with respect to refresh. */
const PRUNE_EXPIRED_SCRIPT = `
local pruned = 0
for _, sid in ipairs(redis.call('SMEMBERS', KEYS[1])) do
  if redis.call('EXISTS', 'gptq:lease:' .. sid) == 0 then
    redis.call('SREM', KEYS[1], sid)
    redis.call('DEL', 'gptq:session:' .. sid)
    pruned = pruned + 1
  end
end
return pruned
`;

/*
 * Registry rollback bookkeeping (R2). Registry values stay exactly what
 * JavaScript wrote; no script here decodes JSON, they only compare raw
 * strings. Two side hashes keyed by name, never read by any public path,
 * hold what a failed registration needs to undo its write:
 *   SESSION_KEYS.registryPending: the raw provisional value the latest
 *     in-flight registration wrote;
 *   SESSION_KEYS.registryRestore: the raw last completed value to put back
 *     (no field when the name was free).
 * KEYS for all three scripts: 1 registry, 2 pending, 3 restore.
 */

/**
 * Write a provisional registry value. The current value is provisional when
 * it equals the pending one; its restore value is then carried forward.
 * Otherwise the current value (completed, or none) is what to restore. All
 * reads precede the writes. ARGV: 1 name, 2 value.
 */
const WRITE_PROVISIONAL_REGISTRY_SCRIPT = `
local prev = redis.call('HGET', KEYS[1], ARGV[1])
local pend = redis.call('HGET', KEYS[2], ARGV[1])
local carried = redis.call('HGET', KEYS[3], ARGV[1])
local restore = prev
if prev and prev == pend then restore = carried end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HSET', KEYS[2], ARGV[1], ARGV[2])
if restore then
  redis.call('HSET', KEYS[3], ARGV[1], restore)
else
  redis.call('HDEL', KEYS[3], ARGV[1])
end
return 1
`;

/**
 * Undo this attempt's provisional value v. When the registry no longer holds
 * v a later write owns the name and nothing is restored, but a pending entry
 * still equal to v is cleared. Otherwise restore the last completed value (or
 * delete the entry over a free name) and clear the bookkeeping.
 * ARGV: 1 name, 2 v. Returns 1 when it undid the write.
 */
const UNDO_REGISTRY_WRITE_SCRIPT = `
if redis.call('HGET', KEYS[1], ARGV[1]) ~= ARGV[2] then
  if redis.call('HGET', KEYS[2], ARGV[1]) == ARGV[2] then
    redis.call('HDEL', KEYS[2], ARGV[1])
  end
  return 0
end
local restore = redis.call('HGET', KEYS[3], ARGV[1])
if restore then
  redis.call('HSET', KEYS[1], ARGV[1], restore)
else
  redis.call('HDEL', KEYS[1], ARGV[1])
end
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
return 1
`;

/** Publish a completed registry value and drop the name's rollback state. ARGV: 1 name, 2 value. */
const PUBLISH_REGISTRATION_SCRIPT = `
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
return 1
`;

/** The provisional registry value a createSession stored. */
export interface RegistryWrite {
  readonly agent_name: string;
  readonly written: string;
}

/** A new session plus the registry write that created it, so a rollback can undo exactly that write. */
export interface CreatedSession extends SessionRecord {
  readonly registry_write: RegistryWrite;
}

export class SessionStore {
  private readonly redis: Redis;
  private refreshTimer: ReturnType<typeof setInterval> | null = null;
  private refreshSessionId: string | null = null;

  constructor(redis: Redis) {
    this.redis = redis;
  }

  /** Create a new session for an agent. Returns the session record. */
  async createSession(
    agentName: string,
    role: "publisher" | "consumer" | "both",
    description?: string,
    transport?: string
  ): Promise<CreatedSession> {
    const sessionId = uuidv4();
    const now = new Date().toISOString();

    // Registration is the write path that bounds an agent's session set.
    await this.pruneExpiredSessions(agentName);

    const record: SessionRecord = {
      session_id: sessionId,
      agent_name: agentName,
      role,
      description,
      created_at: now,
      last_seen: now,
      transport,
    };

    // Record, initial lease, and set membership land together so a concurrent
    // prune can never see this session as a leaseless (expired) member.
    await this.redis
      .multi()
      .hset(
        SESSION_KEYS.session(sessionId),
        "session_id", sessionId,
        "agent_name", agentName,
        "role", role,
        "description", description ?? "",
        "created_at", now,
        "last_seen", now,
        "transport", transport ?? ""
      )
      .set(SESSION_KEYS.lease(sessionId), "alive", "EX", SESSION_DEFAULTS.LEASE_TTL_SECONDS)
      .sadd(SESSION_KEYS.agentSessions(agentName), sessionId)
      .exec();

    // Publish the name's provisional registry value, recording what a
    // rollback restores (RF3, FIX2, R2). It is exactly the JSON JavaScript
    // writes. If this step fails, remove the session just created (R2), so
    // no session, lease or membership outlives the failed registration.
    const written = JSON.stringify({
      name: agentName,
      role,
      description: description ?? undefined,
      registered_at: now,
    });
    try {
      await this.redis.eval(
        WRITE_PROVISIONAL_REGISTRY_SCRIPT,
        3,
        ...REGISTRY_ROLLBACK_KEYS,
        agentName,
        written
      );
    } catch (error) {
      await this.redis
        .multi()
        .del(SESSION_KEYS.session(sessionId), SESSION_KEYS.lease(sessionId))
        .srem(SESSION_KEYS.agentSessions(agentName), sessionId)
        .exec()
        .catch(() => {}); // a lease-bounded leftover expires and is pruned
      throw error;
    }

    return {
      ...record,
      registry_write: { agent_name: agentName, written },
    };
  }

  /**
   * Publish a completed registration's registry value and clear the name's
   * rollback bookkeeping in one step. Every writer of a completed value goes
   * through this, so a failed attempt's undo can never overwrite it.
   */
  async publishRegistration(agentName: string, value: string): Promise<void> {
    await this.redis.eval(PUBLISH_REGISTRATION_SCRIPT, 3, ...REGISTRY_ROLLBACK_KEYS, agentName, value);
  }

  /**
   * Undo a createSession registry write if the entry still holds the value
   * it stored. Returns false when a later write has replaced it.
   */
  async undoRegistryWrite(write: RegistryWrite): Promise<boolean> {
    const undone = await this.redis.eval(
      UNDO_REGISTRY_WRITE_SCRIPT,
      3,
      ...REGISTRY_ROLLBACK_KEYS,
      write.agent_name,
      write.written
    );
    return undone === 1;
  }

  /** Refresh a session's lease TTL. Returns false when the session no longer exists. */
  async refreshLease(sessionId: string): Promise<boolean> {
    const renewed = await this.redis.eval(
      REFRESH_LEASE_SCRIPT,
      2,
      SESSION_KEYS.session(sessionId),
      SESSION_KEYS.lease(sessionId),
      sessionId,
      SESSION_DEFAULTS.LEASE_TTL_SECONDS,
      new Date().toISOString()
    );
    return renewed === 1;
  }

  /** Drop an agent's expired sessions. Returns how many were removed. */
  async pruneExpiredSessions(agentName: string): Promise<number> {
    return (await this.redis.eval(
      PRUNE_EXPIRED_SCRIPT,
      1,
      SESSION_KEYS.agentSessions(agentName)
    )) as number;
  }

  /** Start automatic lease refresh for a session. */
  startLeaseRefresh(sessionId: string): void {
    this.stopLeaseRefresh();
    this.refreshSessionId = sessionId;
    this.refreshLease(sessionId).catch(() => {});
    this.refreshTimer = setInterval(
      () => this.refreshLease(sessionId).catch(() => {}),
      SESSION_DEFAULTS.LEASE_REFRESH_INTERVAL_SECONDS * 1000
    );
  }

  /** Stop automatic lease refresh. */
  stopLeaseRefresh(): void {
    if (this.refreshTimer) {
      clearInterval(this.refreshTimer);
      this.refreshTimer = null;
    }
    this.refreshSessionId = null;
  }

  /**
   * Close a session (preserves mailbox). Stops the lease refresh only when
   * it is refreshing THIS session (RF2): closing another one, such as a
   * failed rename's temporary replacement, must not let the active
   * session's lease lapse.
   */
  async closeSession(sessionId: string): Promise<string | null> {
    if (this.refreshSessionId === sessionId) this.stopLeaseRefresh();

    // Look up which agent this session belongs to
    const agentName = await this.redis.hget(
      SESSION_KEYS.session(sessionId),
      "agent_name"
    );
    if (!agentName) return null;

    // Delete the record first so a racing lease refresh cannot re-add the session
    await this.redis.del(
      SESSION_KEYS.session(sessionId),
      SESSION_KEYS.lease(sessionId)
    );

    // Remove session from agent's set
    await this.redis.srem(SESSION_KEYS.agentSessions(agentName), sessionId);

    return agentName;
  }

  /** Get a session record by ID. */
  async getSession(sessionId: string): Promise<SessionRecord | null> {
    const data = await this.redis.hgetall(SESSION_KEYS.session(sessionId));
    if (!data.session_id) return null;

    return {
      session_id: data.session_id!,
      agent_name: data.agent_name ?? "",
      role: (data.role ?? "both") as SessionRecord["role"],
      description: data.description || undefined,
      created_at: data.created_at ?? "",
      last_seen: data.last_seen ?? "",
      transport: data.transport || undefined,
    };
  }

  /**
   * Whether a name currently has a REGISTERED agent (any session) in the
   * canonical registry hash. This is a name->existence lookup independent of
   * any single session: a registered-but-offline agent still counts. Used by
   * send_message's resolve-then-push recipient validation so a typo'd or
   * unknown name cannot silently create an orphan mailbox.
   */
  async resolveRegistered(name: string): Promise<boolean> {
    return (await this.redis.hexists(SESSION_KEYS.registry, name)) === 1;
  }

  /** Get lease state for a session. */
  async getLeaseState(sessionId: string): Promise<LeaseState> {
    const ttl = await this.redis.ttl(SESSION_KEYS.lease(sessionId));
    return {
      session_id: sessionId,
      alive: ttl > 0,
      ttl_seconds: Math.max(ttl, 0),
    };
  }

  /** Compute agent presence from session leases. */
  async getPresence(agentName: string): Promise<AgentPresence> {
    return (await this.getPresenceMany([agentName]))[0]!;
  }

  /**
   * Compute presence for several agents in at most two round trips
   * (SMEMBERS batch, TTL batch) instead of one sequential read per session.
   * Read-only: expired sessions are reported offline and pruned on
   * registration. Results follow the order of `agentNames`.
   */
  async getPresenceMany(
    agentNames: readonly string[]
  ): Promise<AgentPresence[]> {
    if (agentNames.length === 0) return [];
    const members = await execPipeline<string[]>(
      agentNames.reduce(
        (pipe, name) => pipe.smembers(SESSION_KEYS.agentSessions(name)),
        this.redis.pipeline()
      )
    );
    const allSessionIds = members.flat();
    const ttls = allSessionIds.length === 0 ? [] : await execPipeline<number>(
      allSessionIds.reduce(
        (pipe, sid) => pipe.ttl(SESSION_KEYS.lease(sid)),
        this.redis.pipeline()
      )
    );

    let offset = 0;
    return agentNames.map((agentName, i) => {
      const sessionIds = members[i]!;
      const activeSessions = sessionIds.filter(
        (_, j) => ttls[offset + j]! > 0
      );
      offset += sessionIds.length;
      return {
        agent_name: agentName,
        online: activeSessions.length > 0,
        active_sessions: activeSessions,
      };
    });
  }
}

/** Run a pipeline, rethrowing the first command error as sequential awaits would. */
async function execPipeline<T>(pipe: ChainableCommander): Promise<T[]> {
  const results = (await pipe.exec()) ?? [];
  return results.map(([error, value]) => {
    if (error) throw error;
    return value as T;
  });
}
