/**
 * Read-only inbox probe for Claude hooks and peek (AC-4): unclaimed messages,
 * this session's claimed-but-unacked tasks and other sessions' claimed tasks
 * are counted separately. Never pops, claims or acknowledges anything.
 * Errors are returned as typed values, never as "0".
 */
import { Redis } from "ioredis";
import { CLAIM_KEYS, SESSION_KEYS } from "../core/keys.js";
import { ancestry, findSessionRecord, type RecordLookup } from "./session-record.js";
import { resolveAgentName } from "./agent-name.js";

export type InboxCounts = Readonly<{
  /** Messages waiting in the inbox list (claimable now). */
  unclaimed: number;
  /** Tasks held by unacknowledged claims of THIS gptqueue session (null when the session is unknown). */
  claimed_by_session: number | null;
  /** Tasks held by unacknowledged claims of any other session (or of any session when ours is unknown). */
  claimed_by_others: number;
  /** Newest inbox-events stream id, or null when the stream is empty. */
  latest_event_id: string | null;
}>;

export type ProbeErrorCode = "redis_unavailable" | "redis_error";
export type ProbeResult =
  | Readonly<{ ok: true; counts: InboxCounts }>
  | Readonly<{ ok: false; code: ProbeErrorCode; message: string }>;

type StoredClaim = { session_id?: unknown; tasks?: unknown };

export const tallyClaims = (raw: readonly (string | null)[], sessionId: string | null): Readonly<{ mine: number | null; others: number }> => {
  let mine = 0, others = 0;
  for (const value of raw) {
    if (value === null) continue;
    let claim: StoredClaim;
    try { claim = JSON.parse(value) as StoredClaim; } catch { continue; }
    const size = Array.isArray(claim.tasks) ? claim.tasks.length : 0;
    if (sessionId !== null && claim.session_id === sessionId) mine += size; else others += size;
  }
  return Object.freeze({ mine: sessionId === null ? null : mine, others });
};

/**
 * One atomic snapshot (single EVAL): queue length, claim index, claim records
 * and newest event id. A claim moves messages from the list to the claim
 * index inside one Lua call, so separate reads could straddle it and report
 * zero work while this session holds an unacknowledged claim.
 */
export const PROBE_SCRIPT = `
local n = redis.call('LLEN', KEYS[1])
local ids = redis.call('ZRANGE', KEYS[2], 0, -1)
local raws = {}
for i, id in ipairs(ids) do raws[i] = redis.call('HGET', KEYS[3], id) or '' end
local latest = redis.call('XREVRANGE', KEYS[4], '+', '-', 'COUNT', 1)
local lid = ''
if latest[1] then lid = latest[1][1] end
return { n, lid, raws }
`;

export const probeInbox = async (redis: Redis, agent: string, sessionId: string | null): Promise<ProbeResult> => {
  try {
    const [unclaimed, latest, raw] = await redis.eval(PROBE_SCRIPT, 4, SESSION_KEYS.queue(agent), CLAIM_KEYS.index(agent), CLAIM_KEYS.claims, SESSION_KEYS.inboxEvents(agent)) as [number, string, string[]];
    const { mine, others } = tallyClaims(raw.map((value) => value === "" ? null : value), sessionId);
    return Object.freeze({ ok: true, counts: Object.freeze({ unclaimed, claimed_by_session: mine, claimed_by_others: others, latest_event_id: latest === "" ? null : latest }) });
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

export type IdentitySource = "session_record" | "env" | "derived";
export type Identity = Readonly<{
  agent: string;
  gptqueue_session_id: string | null;
  source: IdentitySource;
  /** Why no session record was used, when source is not session_record. */
  note: string | null;
}>;

/**
 * The inbox this process acts on: the name the enclosing Claude session's MCP
 * server registered (session record), else today's resolution (GPTQ_AGENT_NAME,
 * names file, git top level of CLAUDE_PROJECT_DIR or `dir`), with a note.
 */
export const resolveIdentity = (dir: string | undefined, env: NodeJS.ProcessEnv = process.env, lookup?: RecordLookup): Identity => {
  const found = lookup ?? findSessionRecord(ancestry(env.GPTQ_CLAUDE_PID ? Number(env.GPTQ_CLAUDE_PID) : process.pid), env);
  if (found.kind === "found") {
    return Object.freeze({ agent: found.record.agent, gptqueue_session_id: found.record.gptqueue_session_id, source: "session_record", note: null });
  }
  const note = found.kind === "stale"
    ? `session record ignored (${found.reason}); name re-derived`
    : "no session record from this session's gptqueue MCP server; name re-derived";
  return Object.freeze({
    agent: resolveAgentName(env.CLAUDE_PROJECT_DIR || dir, env),
    gptqueue_session_id: null,
    source: env.GPTQ_AGENT_NAME ? "env" : "derived",
    note,
  });
};
