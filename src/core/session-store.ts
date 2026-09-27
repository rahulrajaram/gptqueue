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

export class SessionStore {
  private readonly redis: Redis;
  private refreshTimer: ReturnType<typeof setInterval> | null = null;

  constructor(redis: Redis) {
    this.redis = redis;
  }

  /** Create a new session for an agent. Returns the session record. */
  async createSession(
    agentName: string,
    role: "publisher" | "consumer" | "both",
    description?: string,
    transport?: string
  ): Promise<SessionRecord> {
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

    // Update agent metadata in registry
    const agentMeta = {
      name: agentName,
      role,
      description: description ?? undefined,
      registered_at: now,
    };
    await this.redis.hset(
      SESSION_KEYS.registry,
      agentName,
      JSON.stringify(agentMeta)
    );

    return record;
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
  }

  /** Close a session (preserves mailbox). */
  async closeSession(sessionId: string): Promise<string | null> {
    this.stopLeaseRefresh();

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
