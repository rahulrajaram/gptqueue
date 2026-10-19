/**
 * Append-only registration audit (stream SESSION_KEYS.registrationAudit) and
 * its read-only query (`gptqueue-doctor registrations`).
 *
 * Every committed registration of a new session appends one event=registered
 * entry, atomically with the registry publication (session-store.ts
 * PUBLISH_REGISTRATION_AUDITED_SCRIPT). A failed or rolled-back attempt never
 * reaches that commit and emits nothing; a later unregister, reap or rollback
 * never removes the event. So the stream proves, for a time window it fully
 * covers, whether a name, directory or Claude session ever registered.
 *
 * Events carry the session's sessionTag(), never the raw session id (a bearer
 * credential).
 */
import type { Redis } from "ioredis";
import { SESSION_KEYS } from "./keys.js";

export const REGISTRATION_AUDIT_MAXLEN = 10_000;
export const REGISTRATIONS_MAX_EVENTS = 500;
/** Entries examined per query at most (the stream is trimmed near 10000). */
const SCAN_PAGE = 1_000;
const SCAN_CAP = 4 * REGISTRATION_AUDIT_MAXLEN;

export type AuditRuntime = "claude" | "codex" | "pi" | "generic" | "unknown";

export type RegistrationAuditEvent = Readonly<{
  name: string;
  session_tag: string;
  runtime: AuditRuntime;
  pid: number;
  cwd: string;
  /** The Claude session UUID when the registering Claude MCP path knows it, else "". */
  claude_session: string;
  /** register: a new session's registration; adoption: a live session adopted this name (runtime continuity). */
  via: "register" | "adoption";
}>;

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Values the registering process already has: the client the caller declared
 * (registered shells), else the Claude MCP marker (GPTQ_CLAUDE_SESSION_RECORD=1),
 * else a generic registration. A declared client outside the enum is "unknown".
 */
export const auditRuntime = (client: string | null | undefined, env: NodeJS.ProcessEnv): AuditRuntime => {
  switch (client) {
    case "codex": return "codex";
    case "pi": return "pi";
    case "claude": return "claude";
    case null:
    case undefined:
    case "": return env.GPTQ_CLAUDE_SESSION_RECORD === "1" ? "claude" : "generic";
    default: return "unknown";
  }
};

export const auditClaudeSession = (runtime: AuditRuntime, env: NodeJS.ProcessEnv): string => {
  if (runtime !== "claude") return "";
  const candidate = env.CLAUDE_SESSION_ID || env.CLAUDE_CODE_SESSION_ID || "";
  return UUID.test(candidate) ? candidate.toLowerCase() : "";
};

/** Flat XADD field list for one event (event=registered first). */
export const auditFieldList = (event: RegistrationAuditEvent): string[] => [
  "event", "registered", "name", event.name, "session_tag", event.session_tag, "runtime", event.runtime,
  "pid", String(event.pid), "cwd", event.cwd, "claude_session", event.claude_session, "via", event.via,
];

// ---------------------------------------------------------------- query

export type Coverage = "complete" | "incomplete" | "unknown";

export type RegistrationsQuery = Readonly<{
  sinceMs: number;
  untilMs: number;
  cwd?: string;
  claudeSession?: string;
  name?: string;
}>;

export type RegistrationsReport = Readonly<{
  schema_version: 1;
  since: string;
  until: string;
  coverage: Coverage;
  /**
   * The earliest instant the stream provably holds every event from (ISO), or
   * null when unknown. coverage is "complete" iff this is at or before since.
   */
  complete_from: string | null;
  events: ReadonlyArray<Readonly<{ at: string; name: string; session_tag: string; runtime: string; pid: number | null; cwd: string; claude_session: string; via: string }>>;
  truncated: boolean;
}>;

/** The subset of XINFO STREAM the coverage decision reads. */
export type StreamInfo = Readonly<{
  exists: boolean;
  /** Absent on Redis older than 7.0. */
  maxDeletedEntryId?: string;
  entriesAdded?: number;
  length: number;
  firstEntryId: string | null;
}>;

const idMs = (id: string): number => Number(id.split("-")[0]);

/**
 * Coverage from the stream's own history (Redis >= 7 XINFO STREAM fields).
 * - Nothing removed (entries-added == length, max-deleted 0-0): every event
 *   since the first entry is present, so coverage starts at the first entry,
 *   i.e. when the audit first wrote to this Redis (its deploy).
 * - Entries removed: MAXLEN trimming removes the oldest entries and does NOT
 *   move max-deleted-entry-id (only XDEL does), so a trim shows as
 *   entries-added > length. The retained first entry then bounds what was
 *   trimmed (events in its own millisecond may be gone: start 1 ms later), and
 *   any XDEL bounds holes at max-deleted-entry-id + 1 ms.
 * - No stream, missing fields (older Redis) or no evidence: unknown.
 */
export const coverageOf = (info: StreamInfo, sinceMs: number): Readonly<{ coverage: Coverage; complete_from: string | null }> => {
  if (!info.exists || info.maxDeletedEntryId === undefined || info.entriesAdded === undefined) return { coverage: "unknown", complete_from: null };
  const trimmed = info.entriesAdded > info.length;
  const deletedMs = info.maxDeletedEntryId === "0-0" ? null : idMs(info.maxDeletedEntryId) + 1;
  const firstMs = info.firstEntryId === null ? null : idMs(info.firstEntryId) + (trimmed ? 1 : 0);
  if (firstMs === null) return { coverage: "unknown", complete_from: null };
  const fromMs = deletedMs === null ? firstMs : Math.max(firstMs, deletedMs);
  return { coverage: fromMs <= sinceMs ? "complete" : "incomplete", complete_from: new Date(fromMs).toISOString() };
};

const readInfo = async (redis: Redis, key: string): Promise<StreamInfo> => {
  if ((await redis.exists(key)) === 0) return { exists: false, length: 0, firstEntryId: null };
  const raw = (await redis.call("XINFO", "STREAM", key)) as unknown[];
  const fields = new Map<string, unknown>();
  for (let i = 0; i + 1 < raw.length; i += 2) fields.set(String(raw[i]), raw[i + 1]);
  const first = fields.get("first-entry");
  const maxDeleted = fields.get("max-deleted-entry-id");
  const added = fields.get("entries-added");
  return {
    exists: true,
    length: Number(fields.get("length") ?? 0),
    firstEntryId: Array.isArray(first) && typeof first[0] === "string" ? first[0] : null,
    ...(typeof maxDeleted === "string" ? { maxDeletedEntryId: maxDeleted } : {}),
    ...(added === undefined ? {} : { entriesAdded: Number(added) }),
  };
};

const toObject = (fields: readonly string[]): Record<string, string> => {
  const out: Record<string, string> = {};
  for (let i = 0; i + 1 < fields.length; i += 2) out[fields[i]!] = fields[i + 1]!;
  return out;
};

/** Test-only seam: runs after each XRANGE page, before the next read. */
export type QueryHooks = Readonly<{ onPage?: (pageIndex: number) => Promise<void> }>;

/**
 * Read-only: EXISTS, XINFO STREAM and XRANGE only.
 *
 * Coverage comes from an XINFO taken AFTER the last XRANGE page (F4), never
 * from metadata read before the scan. That is sound without a snapshot:
 * trimming only ever removes a prefix of the stream, so any matching entry
 * trimmed before or during the scan has an id below the post-scan first
 * entry, and entries-added > length marks that a trim happened; complete_from
 * is then after it. An XDEL during the scan moves max-deleted-entry-id to at
 * least the deleted id, which also moves complete_from past it. Either way a
 * matching event the scan missed lies at or after since and before
 * complete_from, so coverage cannot be "complete". A stream deleted during
 * the scan reads as missing afterwards: coverage "unknown".
 */
export const queryRegistrations = async (redis: Redis, query: RegistrationsQuery, key: string = SESSION_KEYS.registrationAudit, hooks: QueryHooks = {}): Promise<RegistrationsReport> => {
  const events: Array<RegistrationsReport["events"][number]> = [];
  let truncated = false;
  if ((await redis.exists(key)) === 1) {
    let start = `${query.sinceMs}-0`;
    const end = `${query.untilMs}-18446744073709551615`;
    let scanned = 0;
    let pageIndex = 0;
    scan: while (scanned < SCAN_CAP) {
      const page = (await redis.xrange(key, start, end, "COUNT", SCAN_PAGE)) as Array<[string, string[]]>;
      for (const [id, fields] of page) {
        scanned += 1;
        const f = toObject(fields);
        if (f.event !== "registered") continue;
        if (query.name !== undefined && f.name !== query.name) continue;
        if (query.cwd !== undefined && f.cwd !== query.cwd) continue;
        if (query.claudeSession !== undefined && (f.claude_session ?? "").toLowerCase() !== query.claudeSession.toLowerCase()) continue;
        if (events.length === REGISTRATIONS_MAX_EVENTS) { truncated = true; break scan; }
        const pid = Number(f.pid);
        events.push({ at: new Date(idMs(id)).toISOString(), name: f.name ?? "", session_tag: f.session_tag ?? "", runtime: f.runtime ?? "unknown",
          pid: Number.isInteger(pid) && f.pid !== "" ? pid : null, cwd: f.cwd ?? "", claude_session: f.claude_session ?? "", via: f.via ?? "register" });
      }
      if (hooks.onPage) await hooks.onPage(pageIndex);
      pageIndex += 1;
      if (page.length < SCAN_PAGE) break;
      start = `(${page[page.length - 1]![0]}`;
    }
    if (scanned >= SCAN_CAP) truncated = true;
  }
  const { coverage, complete_from } = coverageOf(await readInfo(redis, key), query.sinceMs);
  return { schema_version: 1, since: new Date(query.sinceMs).toISOString(), until: new Date(query.untilMs).toISOString(), coverage, complete_from, events, truncated };
};

// ---------------------------------------------------------------- CLI

export const REGISTRATIONS_HELP = [
  "usage: gptqueue-doctor registrations --since <iso> --until <iso> [--cwd <path>] [--claude-session <uuid>] [--name <n>] [--redis-url <url>] --json",
  "  Read-only (EXISTS, XINFO STREAM, XRANGE) query of the append-only registration audit stream",
  `  ${SESSION_KEYS.registrationAudit}: one event per committed registration, kept even after the session`,
  "  unregisters, is reaped or rolled back. At most 500 events (truncated:true beyond).",
  "  coverage: complete  = the stream provably holds every registration since --since (complete_from <= since);",
  "            incomplete = events before complete_from were trimmed, or the audit started after --since;",
  "            unknown   = no audit stream on this Redis, or a Redis without XINFO max-deleted-entry-id.",
  "  --redis-url defaults to $REDIS_URL, then redis://127.0.0.1:6379/0.",
  "  Mixed versions: writers on releases without the audit emit nothing. An empty complete report proves",
  "  non-registration only if every writer that could register in the window ran an audit-emitting release.",
  "  Exit codes: 0 success, 2 invalid arguments, 4 Redis unavailable.",
].join("\n");

export type CliOutcome = Readonly<{ stdout: string; stderr: string; exitCode: number }>;

type ParsedArgs = Readonly<{ query: RegistrationsQuery; redisUrl: string }>;
const VALUE_OPTIONS = new Set(["--since", "--until", "--cwd", "--claude-session", "--name", "--redis-url"]);

const parseIso = (label: string, value: string | undefined): number | string => {
  if (value === undefined) return `Required ${label}`;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && /^\d{4}-\d{2}-\d{2}/.test(value) ? ms : `${label} must be an ISO-8601 timestamp`;
};

export const parseRegistrationsArgs = (args: readonly string[], env: NodeJS.ProcessEnv): ParsedArgs | string => {
  const options = new Map<string, string>();
  let json = false;
  for (let i = 0; i < args.length; i += 1) {
    const arg = args[i]!;
    if (arg === "--json") { if (json) return "Duplicate flag --json"; json = true; continue; }
    if (!VALUE_OPTIONS.has(arg)) return `Unknown option ${arg}`;
    const value = args[i + 1];
    if (value === undefined || options.has(arg)) return "Expected unique option/value pairs";
    options.set(arg, value);
    i += 1;
  }
  if (!json) return "Required --json (the only output format)";
  const since = parseIso("--since", options.get("--since"));
  if (typeof since === "string") return since;
  const until = parseIso("--until", options.get("--until"));
  if (typeof until === "string") return until;
  if (until < since) return "--until must not be earlier than --since";
  return {
    redisUrl: options.get("--redis-url") || env.REDIS_URL || "redis://127.0.0.1:6379/0",
    query: {
      sinceMs: since, untilMs: until,
      ...(options.has("--cwd") ? { cwd: options.get("--cwd")! } : {}),
      ...(options.has("--claude-session") ? { claudeSession: options.get("--claude-session")! } : {}),
      ...(options.has("--name") ? { name: options.get("--name")! } : {}),
    },
  };
};

export const runRegistrationsCli = async (
  args: readonly string[],
  env: NodeJS.ProcessEnv,
  connect: (url: string) => Redis,
): Promise<CliOutcome> => {
  if (args.includes("--help") || args.includes("-h")) return { stdout: REGISTRATIONS_HELP, stderr: "", exitCode: 0 };
  const parsed = parseRegistrationsArgs(args, env);
  if (typeof parsed === "string") return { stdout: "", stderr: `error: ${parsed}\n${REGISTRATIONS_HELP}`, exitCode: 2 };
  const redis = connect(parsed.redisUrl);
  try {
    const report = await queryRegistrations(redis, parsed.query);
    return { stdout: JSON.stringify(report), stderr: "", exitCode: 0 };
  } catch (error) {
    return { stdout: "", stderr: `error: Redis unavailable or query failed: ${(error as Error).message}`, exitCode: 4 };
  } finally {
    redis.disconnect();
  }
};
