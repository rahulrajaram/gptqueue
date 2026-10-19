/**
 * SessionStore: Redis-backed session lifecycle management.
 *
 * Owns session creation, lease refresh, session close, and presence queries.
 * Does NOT own queue/mailbox operations (see MailboxStore).
 */

import { readlinkSync } from "node:fs";
import { hostname } from "node:os";
import { Redis, type ChainableCommander } from "ioredis";
import { v4 as uuidv4 } from "uuid";
import { SESSION_KEYS, SESSION_DEFAULTS, SYSTEM_SENDER } from "./keys.js";
import type { SessionRecord, LeaseState, AgentPresence } from "./types.js";
import { GptQueueError } from "../mcp-server/errors.js";
import { sessionTag } from "./session-tag.js";
import { auditFieldList, REGISTRATION_AUDIT_MAXLEN, type RegistrationAuditEvent } from "./registration-audit.js";

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

/**
 * Exclusive name claim (D2, ID-1, ID-5). Atomically decides whether a new
 * session may hold an agent name and, if so, creates it. Every member of the
 * name's session set is classified:
 *   - no lease: expired; removed (taken over);
 *   - ARGV[5] (the caller's own current session): ignored;
 *   - listed in ARGV[6] (a holder JavaScript saw with a dead pid on this
 *     host): removed (taken over);
 *   - otherwise live: admitted only when both it and the claimant opted into
 *     sharing; any other live holder refuses the claim with its session id.
 * Two claimants racing for one dead holder both list it, but the first
 * script's new session is live and unlisted for the second, so exactly one
 * wins. Takeover removes only the old session record, lease and membership;
 * the mailbox, claims and history are untouched. Each takeover is audited.
 * KEYS: 1 agent sessions, 2 new session, 3 new lease, 4 identity audit.
 * ARGV: 1 name, 2 new sid, 3 shared '1'|'0', 4 lease ttl, 5 self sid,
 *   6 dead sids (space separated), 7.. HSET field/value pairs for the record.
 * Returns {'ok', taken-over...} or {'held', holder sid}.
 */
const CLAIM_NAME_SCRIPT = `
local dead = {}
for sid in string.gmatch(ARGV[6], '%S+') do dead[sid] = true end
local removals = {}
for _, sid in ipairs(redis.call('SMEMBERS', KEYS[1])) do
  if redis.call('EXISTS', 'gptq:lease:' .. sid) == 0 then
    table.insert(removals, {sid, 'lease_expired'})
  elseif sid == ARGV[5] then
    -- the caller's own session never blocks its re-registration
  elseif dead[sid] then
    table.insert(removals, {sid, 'holder_pid_dead'})
  elseif not (ARGV[3] == '1' and redis.call('HGET', 'gptq:session:' .. sid, 'shared') == '1') then
    return {'held', sid}
  end
end
local taken = {'ok'}
for _, r in ipairs(removals) do
  redis.call('SREM', KEYS[1], r[1])
  redis.call('DEL', 'gptq:session:' .. r[1], 'gptq:lease:' .. r[1])
  redis.call('XADD', KEYS[4], 'MAXLEN', '~', 1000, '*', 'event', 'name_takeover',
    'name', ARGV[1], 'old_session_id', r[1], 'new_session_id', ARGV[2], 'reason', r[2])
  table.insert(taken, r[1])
end
local fields = {}
for i = 8, #ARGV do table.insert(fields, ARGV[i]) end
redis.call('HSET', KEYS[2], unpack(fields))
redis.call('SET', KEYS[3], 'alive', 'EX', ARGV[4])
redis.call('SADD', KEYS[1], ARGV[2])
if ARGV[7] == '1' then redis.call('SADD', KEYS[5], ARGV[1]) else redis.call('SREM', KEYS[5], ARGV[1]) end
return taken
`;

/** Whether a local pid is alive. EPERM means it exists under another user. */
function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
}

/**
 * This process's PID-namespace identity (e.g. "pid:[4026531836]"), or null
 * when unreadable. A matching hostname alone does not prove a shared PID
 * namespace (containers), so pid liveness is only trusted when both match.
 */
export function pidNamespaceId(): string | null {
  try {
    return readlinkSync("/proc/self/ns/pid");
  } catch {
    return null;
  }
}

/** Options for an exclusive name claim. */
export interface ClaimOptions {
  /** Opt into sharing the name with other sessions that also opted in. */
  readonly shared?: boolean;
  /** The caller's own current session, which never blocks its re-registration. */
  readonly selfSessionId?: string | null;
  /**
   * DL-2: mark the name as a sidecar's provisional identity, atomically with a
   * successful claim (never on a refused one). A non-provisional claim clears
   * any mark, so a sweep can only ever retire a name its holder declared.
   */
  readonly provisional?: boolean;
}

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

/**
 * PUBLISH_REGISTRATION_SCRIPT plus the registration audit event, in the same
 * script so the event exists iff the registration committed (no event for a
 * failed or rolled-back attempt). Scripts do not roll back, so every
 * predictable failure (a wrong-type key) is checked before the first write
 * (F2): after the checks nothing can fail. KEYS[4] audit stream; ARGV 3
 * MAXLEN, then the event's field/value pairs.
 */
const PUBLISH_REGISTRATION_AUDITED_SCRIPT = `
for i = 1, 3 do
  local t = redis.call('TYPE', KEYS[i]).ok
  if t ~= 'hash' and t ~= 'none' then return redis.error_reply('WRONGTYPE registration key ' .. KEYS[i] .. ' is ' .. t) end
end
local st = redis.call('TYPE', KEYS[4]).ok
if st ~= 'stream' and st ~= 'none' then return redis.error_reply('WRONGTYPE registration audit key ' .. KEYS[4] .. ' is ' .. st) end
local fields = {}
for i = 4, #ARGV do fields[#fields + 1] = ARGV[i] end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('XADD', KEYS[4], 'MAXLEN', '~', ARGV[3], '*', unpack(fields))
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
    transport?: string,
    options: ClaimOptions = {}
  ): Promise<CreatedSession> {
    if (options.provisional && options.shared) {
      // A provisional name is one sidecar's own startup identity: sharing it
      // would let another session clear its mark and hide an abandoned queue.
      throw new GptQueueError("AGENT_REGISTRATION_INVALID",
        `Agent name '${agentName}' cannot be both provisional and shared.`, { name: agentName });
    }
    if (agentName === SYSTEM_SENDER) {
      throw new GptQueueError("AGENT_NAME_RESERVED", `Agent name '${agentName}' is reserved for gptqueue's own messages.`, { name: agentName });
    }
    const sessionId = uuidv4();
    const now = new Date().toISOString();

    // Expired members are removed (and audited) by the claim script itself,
    // so no separate prune runs here: an unaudited prune would hide the
    // takeover of an expired holder.

    const record: SessionRecord = {
      session_id: sessionId,
      agent_name: agentName,
      role,
      description,
      created_at: now,
      last_seen: now,
      transport,
    };

    // Record, initial lease, and set membership land together (so a
    // concurrent prune never sees a leaseless member) and only after the
    // exclusive name check passes, in one script (D2).
    const host = hostname();
    const pidns = pidNamespaceId();
    const deadHolders = await this.deadLocalHolders(agentName, host, pidns);
    const claim = (await this.redis.eval(
      CLAIM_NAME_SCRIPT,
      5,
      SESSION_KEYS.agentSessions(agentName),
      SESSION_KEYS.session(sessionId),
      SESSION_KEYS.lease(sessionId),
      SESSION_KEYS.identityAudit,
      SESSION_KEYS.provisional,
      agentName,
      sessionId,
      options.shared ? "1" : "0",
      SESSION_DEFAULTS.LEASE_TTL_SECONDS,
      options.selfSessionId ?? "",
      deadHolders.join(" "),
      options.provisional ? "1" : "0",
      "session_id", sessionId,
      "agent_name", agentName,
      "role", role,
      "description", description ?? "",
      "created_at", now,
      "last_seen", now,
      "transport", transport ?? "",
      "pid", process.pid,
      "hostname", host,
      "pidns", pidns ?? "",
      "shared", options.shared ? "1" : "0"
    )) as string[];
    if (claim[0] === "held") {
      const holder = claim[1]!;
      // Sessions registered by older code carry no pid/hostname, so their
      // liveness can only be judged by lease expiry; tell the caller so it
      // can wait for the lease instead of treating the holder as live (ID-3).
      const [[pid, holderHost], ttlMs] = await Promise.all([
        this.redis.hmget(SESSION_KEYS.session(holder), "pid", "hostname"),
        this.redis.pttl(SESSION_KEYS.lease(holder)),
      ]).catch(() => [[null, null], -2] as [[null, null], number]);
      throw new GptQueueError(
        "AGENT_NAME_HELD",
        `Agent name '${agentName}' is held by a live session (${sessionTag(holder)}). ` +
          "Choose another name, or have both registrants opt into sharing (shared: true / GPTQ_AGENT_SHARED=1).",
        { name: agentName, holder_session_tag: sessionTag(holder), holder_liveness_verifiable: !!pid && !!holderHost, holder_lease_ttl_ms: ttlMs }
      );
    }

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
      // COR-F2: a fresh registration of a name supersedes any forwarding
      // tombstone a retired holder of that name left behind.
      await this.redis.del(SESSION_KEYS.forward(agentName));
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
      // A mark this failed attempt set must not outlive it unless another
      // provisional session still holds the name. If this cleanup is lost,
      // the sweep still sees no live session and the empty name retires harmlessly.
      if (options.provisional && (await this.redis.scard(SESSION_KEYS.agentSessions(agentName)).catch(() => 1)) === 0) {
        await this.redis.srem(SESSION_KEYS.provisional, agentName).catch(() => 0);
      }
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
  async publishRegistration(agentName: string, value: string, audit?: RegistrationAuditEvent): Promise<void> {
    if (audit === undefined) {
      // A metadata republish of an already-audited session.
      await this.redis.eval(PUBLISH_REGISTRATION_SCRIPT, 3, ...REGISTRY_ROLLBACK_KEYS, agentName, value);
      return;
    }
    await this.redis.eval(PUBLISH_REGISTRATION_AUDITED_SCRIPT, 4, ...REGISTRY_ROLLBACK_KEYS, SESSION_KEYS.registrationAudit,
      agentName, value, REGISTRATION_AUDIT_MAXLEN, ...auditFieldList(audit));
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

  /** Sessions holding a name whose recorded pid on this host is gone. */
  private async deadLocalHolders(agentName: string, host: string, pidns: string | null): Promise<string[]> {
    if (pidns === null) return []; // cannot prove a shared PID namespace: rely on lease expiry
    const sids = await this.redis.smembers(SESSION_KEYS.agentSessions(agentName));
    const holders = await Promise.all(
      sids.map(async (sid) => {
        const [pid, holderHost, holderPidns] = await this.redis.hmget(SESSION_KEYS.session(sid), "pid", "hostname", "pidns");
        const pidNumber = Number(pid);
        const dead = holderHost === host && holderPidns === pidns && Number.isInteger(pidNumber) && pidNumber > 0 && !pidAlive(pidNumber);
        return dead ? sid : null;
      })
    );
    return holders.filter((sid): sid is string => sid !== null);
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
