/**
 * Read-only inbox probe for Claude hooks and peek (AC-4): unclaimed messages,
 * this session's claimed-but-unacked tasks and other sessions' claimed tasks
 * are counted separately. Never pops, claims or acknowledges anything.
 * Errors are returned as typed values, never as "0".
 */
import { Redis } from "ioredis";
import { CLAIM_KEYS, SESSION_KEYS } from "../core/keys.js";
import { ancestry, findSessionRecord, quoteAgent, readProcess, type ClaudeProcess, type RecordLookup } from "./session-record.js";
import { findLegacyIdentity, type LegacyLookup } from "./legacy-identity.js";
import { resolveAgentName } from "./agent-name.js";

export type InboxCounts = Readonly<{
  /** Messages waiting in the inbox list (claimable now). */
  unclaimed: number;
  /** Tasks held by unacknowledged claims of THIS gptqueue session (null when the session is unknown). */
  claimed_by_session: number | null;
  /** Tasks held by unacknowledged claims of any other session (or of any session when ours is unknown). */
  claimed_by_others: number;
  /**
   * Tasks in other sessions' claims whose lease already expired (claim-index
   * score < Redis TIME), included in claimed_by_others. A crashed or replaced
   * session's work: claim_tasks recovers it now, so it is actionable (COR-F3).
   */
  expired_by_others: number;
  /** Newest inbox-events stream id, or null when the stream is empty. */
  latest_event_id: string | null;
}>;

export type ProbeErrorCode = "redis_unavailable" | "redis_error";
export type ProbeResult =
  | Readonly<{ ok: true; counts: InboxCounts }>
  | Readonly<{ ok: false; code: ProbeErrorCode; message: string }>;

type StoredClaim = { session_id?: unknown; tasks?: unknown };

export const tallyClaims = (raw: readonly (string | null)[], sessionId: string | null, expired: readonly boolean[] = []): Readonly<{ mine: number | null; others: number; expiredOthers: number }> => {
  let mine = 0, others = 0, expiredOthers = 0;
  raw.forEach((value, index) => {
    if (value === null) return;
    let claim: StoredClaim;
    try { claim = JSON.parse(value) as StoredClaim; } catch { return; }
    const size = Array.isArray(claim.tasks) ? claim.tasks.length : 0;
    if (sessionId !== null && claim.session_id === sessionId) { mine += size; return; }
    others += size;
    if (expired[index] === true) expiredOthers += size;
  });
  return Object.freeze({ mine: sessionId === null ? null : mine, others, expiredOthers });
};

/**
 * One atomic snapshot (single EVAL): queue length, claim index, claim records
 * and newest event id. A claim moves messages from the list to the claim
 * index inside one Lua call, so separate reads could straddle it and report
 * zero work while this session holds an unacknowledged claim.
 */
export const PROBE_SCRIPT = `
local n = redis.call('LLEN', KEYS[1])
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ids = redis.call('ZRANGE', KEYS[2], 0, -1, 'WITHSCORES')
local raws, expired = {}, {}
for i = 1, #ids, 2 do
  raws[#raws + 1] = redis.call('HGET', KEYS[3], ids[i]) or ''
  expired[#expired + 1] = (tonumber(ids[i + 1]) < now) and 1 or 0
end
local latest = redis.call('XREVRANGE', KEYS[4], '+', '-', 'COUNT', 1)
local lid = ''
if latest[1] then lid = latest[1][1] end
return { n, lid, raws, expired }
`;

export const probeInbox = async (redis: Redis, agent: string, sessionId: string | null): Promise<ProbeResult> => {
  try {
    const [unclaimed, latest, raw, expiredFlags] = await redis.eval(PROBE_SCRIPT, 4, SESSION_KEYS.queue(agent), CLAIM_KEYS.index(agent), CLAIM_KEYS.claims, SESSION_KEYS.inboxEvents(agent)) as [number, string, string[], number[]];
    const { mine, others, expiredOthers } = tallyClaims(raw.map((value) => value === "" ? null : value), sessionId, expiredFlags.map((flag) => flag === 1));
    return Object.freeze({ ok: true, counts: Object.freeze({ unclaimed, claimed_by_session: mine, claimed_by_others: others, expired_by_others: expiredOthers, latest_event_id: latest === "" ? null : latest }) });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const unavailable = /ECONNREFUSED|ENOTFOUND|ETIMEDOUT|Connection is closed|max retries|connect/i.test(message);
    return Object.freeze({ ok: false, code: unavailable ? "redis_unavailable" : "redis_error", message });
  }
};

/** A short-lived client that fails fast instead of queueing forever. */
export const probeClient = (url = process.env.REDIS_URL || "redis://127.0.0.1:6379/0"): Redis => {
  const redis = new Redis(url, { maxRetriesPerRequest: 0, enableOfflineQueue: true, connectTimeout: 2000, retryStrategy: () => null, lazyConnect: false });
  redis.on("error", () => undefined); // surfaced through the awaited command
  return redis;
};

export type IdentitySource = "session_record" | "legacy_env" | "env" | "derived";

/** Sources that name the inbox this session's own MCP server actually registered. */
export const isConfirmed = (identity: Pick<Identity, "source">): boolean =>
  identity.source === "session_record" || identity.source === "legacy_env";
export type Identity = Readonly<{
  agent: string;
  gptqueue_session_id: string | null;
  source: IdentitySource;
  /** Why no session record was used, when source is not session_record. */
  note: string | null;
  /** The Claude process this identity belongs to, when known (ties the watcher to it, LC-F2). */
  claude: ClaudeProcess | null;
  /** legacy_env only: pid of the pre-record MCP server whose environ named the agent. */
  legacy_server_pid?: number;
}>;

/**
 * The inbox this process acts on: the name the enclosing Claude session's MCP
 * server registered (session record), else today's resolution (GPTQ_AGENT_NAME,
 * names file, git top level of CLAUDE_PROJECT_DIR or `dir`), with a note.
 */
export const resolveIdentity = (
  dir: string | undefined,
  env: NodeJS.ProcessEnv = process.env,
  lookup?: RecordLookup,
  legacy?: () => LegacyLookup,
): Identity => {
  const chain = lookup && legacy ? [] : ancestry(env.GPTQ_CLAUDE_PID ? Number(env.GPTQ_CLAUDE_PID) : process.pid);
  const found = lookup ?? findSessionRecord(chain, env);
  if (found.kind === "found") {
    return Object.freeze({ agent: found.record.agent, gptqueue_session_id: found.record.gptqueue_session_id, source: "session_record", note: null, claude: Object.freeze({ pid: found.record.claude_pid, start: found.record.claude_start }) });
  }
  // A running session started before session records existed: its old MCP
  // server (a direct child of this Claude) carries the name it registered.
  const old = legacy ? legacy() : env.GPTQ_CLAUDE_LEGACY_SCAN === "off" ? { kind: "none" as const } : findLegacyIdentity(chain);
  if (old.kind === "confirmed") {
    const claude = readProcess(old.claude_pid);
    return Object.freeze({ agent: old.agent, gptqueue_session_id: null, source: "legacy_env", note: `confirmed from the environment of this session's pre-record gptqueue MCP server (pid ${old.server_pid})`, claude: claude ? Object.freeze({ pid: claude.pid, start: claude.start }) : null, legacy_server_pid: old.server_pid });
  }
  const ambiguity = old.kind === "ambiguous" ? `; ${old.agents.length} legacy gptqueue MCP servers under pid ${old.claude_pid} (${old.agents.map(quoteAgent).join(", ")})` : "";
  const note = (found.kind === "stale"
    ? `session record ignored (${found.reason}); name re-derived`
    : "no session record from this session's gptqueue MCP server; name re-derived") + ambiguity;
  return Object.freeze({
    agent: resolveAgentName(env.CLAUDE_PROJECT_DIR || dir, env),
    gptqueue_session_id: null,
    source: env.GPTQ_AGENT_NAME ? "env" : "derived",
    note,
    claude: null,
  });
};

/**
 * BC-F2: a legacy server's environ is fixed at exec, but it may since have
 * renamed (register_agent) or closed its session. legacy_env stays confirmed
 * only while (1) gptq:heartbeat:<name> exists (a live old server refreshes
 * it; close and rename delete it) AND (2) the name still belongs to that very
 * server: its registry entry's pid (7347a85 writes `pid` there) or the pid of
 * one of the name's current sessions equals the legacy server's pid. Another
 * client that took the name after a rename keeps the heartbeat alive, so (1)
 * alone is not enough. Otherwise, or when Redis cannot say, the identity is
 * downgraded to UNCONFIRMED. Other sources pass through.
 */
export const LEGACY_OWNER_SCRIPT = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 'no_heartbeat' end
local pid = ARGV[1]
local reg = redis.call('HGET', KEYS[2], ARGV[2])
if reg then
  local ok, value = pcall(cjson.decode, reg)
  if ok and type(value) == 'table' and tostring(value.pid) == pid then return 'owned' end
end
for _, sid in ipairs(redis.call('SMEMBERS', KEYS[3])) do
  if redis.call('HGET', ARGV[3] .. sid, 'pid') == pid then return 'owned' end
end
return 'other_holder'
`;

export const confirmLegacyLiveness = async (redis: Pick<Redis, "eval">, identity: Identity, env: NodeJS.ProcessEnv = process.env): Promise<Identity> => {
  if (identity.source !== "legacy_env") return identity;
  const name = quoteAgent(identity.agent);
  let reason: string;
  try {
    const verdict = identity.legacy_server_pid === undefined ? "other_holder" : await redis.eval(LEGACY_OWNER_SCRIPT, 3,
      SESSION_KEYS.heartbeat(identity.agent), SESSION_KEYS.registry, SESSION_KEYS.agentSessions(identity.agent),
      String(identity.legacy_server_pid), identity.agent, SESSION_KEYS.session("")) as string;
    if (verdict === "owned") return identity;
    reason = verdict === "no_heartbeat"
      ? `the pre-record gptqueue MCP server's startup name ${name} has no live heartbeat (renamed or closed since)`
      : `the pre-record gptqueue MCP server's startup name ${name} is now held by another process (renamed since)`;
  } catch (error) {
    reason = `could not check the pre-record gptqueue MCP server's name ${name} (${(error as Error).message})`;
  }
  return Object.freeze({ agent: identity.agent, gptqueue_session_id: null, source: env.GPTQ_AGENT_NAME ? "env" : "derived", note: reason, claude: identity.claude });
};

/**
 * Wait up to `waitMs` (polling) for this Claude session's MCP server to
 * publish its record: Claude runs SessionStart before the server finishes
 * registering, so an immediate lookup would fall back to a guessed name that
 * may be another session's. Returns the fallback identity on timeout.
 */
export const waitForIdentity = async (dir: string | undefined, waitMs: number, env: NodeJS.ProcessEnv = process.env, pollMs = 50): Promise<Identity> => {
  const deadline = Date.now() + waitMs;
  for (;;) {
    const identity = resolveIdentity(dir, env);
    if (isConfirmed(identity) || Date.now() >= deadline) return identity;
    await new Promise((r) => setTimeout(r, pollMs));
  }
};

export const recordWaitMs = (env: NodeJS.ProcessEnv, fallback: number): number => {
  const parsed = Number(env.GPTQ_CLAUDE_RECORD_WAIT_MS);
  return env.GPTQ_CLAUDE_RECORD_WAIT_MS !== undefined && Number.isFinite(parsed) && parsed >= 0 ? parsed : fallback;
};
