/**
 * SessionStore: Redis-backed session lifecycle management.
 *
 * Owns session creation, lease refresh, session close, and presence queries.
 * Does NOT own queue/mailbox operations (see MailboxStore).
 */

import { Redis } from "ioredis";
import { v4 as uuidv4 } from "uuid";
import { SESSION_KEYS, SESSION_DEFAULTS } from "./keys.js";
import type { SessionRecord, LeaseState, AgentPresence } from "./types.js";

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

    const record: SessionRecord = {
      session_id: sessionId,
      agent_name: agentName,
      role,
      description,
      created_at: now,
      last_seen: now,
      transport,
    };

    // Store session record
    await this.redis.hset(
      SESSION_KEYS.session(sessionId),
      "session_id", sessionId,
      "agent_name", agentName,
      "role", role,
      "description", description ?? "",
      "created_at", now,
      "last_seen", now,
      "transport", transport ?? ""
    );

    // Register in agent's session set
    await this.redis.sadd(SESSION_KEYS.agentSessions(agentName), sessionId);

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

    // Set initial lease
    await this.refreshLease(sessionId);

    return record;
  }

  /** Refresh a session's lease TTL. */
  async refreshLease(sessionId: string): Promise<void> {
    await this.redis.set(
      SESSION_KEYS.lease(sessionId),
      "alive",
      "EX",
      SESSION_DEFAULTS.LEASE_TTL_SECONDS
    );

    // Update last_seen on the session record
    await this.redis.hset(
      SESSION_KEYS.session(sessionId),
      "last_seen",
      new Date().toISOString()
    );
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

    // Remove session from agent's set
    await this.redis.srem(SESSION_KEYS.agentSessions(agentName), sessionId);

    // Delete session record and lease
    await this.redis.del(
      SESSION_KEYS.session(sessionId),
      SESSION_KEYS.lease(sessionId)
    );

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

  /** Resolve agent name from a session ID (Redis lookup). */
  async resolveAgent(sessionId: string): Promise<string | null> {
    return this.redis.hget(
      SESSION_KEYS.session(sessionId),
      "agent_name"
    );
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
    const sessionIds = await this.redis.smembers(
      SESSION_KEYS.agentSessions(agentName)
    );

    const activeSessions: string[] = [];
    for (const sid of sessionIds) {
      const lease = await this.getLeaseState(sid);
      if (lease.alive) {
        activeSessions.push(sid);
      } else {
        // Clean up expired session from the set
        await this.redis.srem(
          SESSION_KEYS.agentSessions(agentName),
          sid
        );
        await this.redis.del(SESSION_KEYS.session(sid));
      }
    }

    return {
      agent_name: agentName,
      online: activeSessions.length > 0,
      active_sessions: activeSessions,
    };
  }

  /** List all sessions for an agent. */
  async listSessions(agentName: string): Promise<SessionRecord[]> {
    const sessionIds = await this.redis.smembers(
      SESSION_KEYS.agentSessions(agentName)
    );

    const sessions: SessionRecord[] = [];
    for (const sid of sessionIds) {
      const session = await this.getSession(sid);
      if (session) sessions.push(session);
    }
    return sessions;
  }
}
