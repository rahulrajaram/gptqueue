/**
 * Non-consuming inbox listing for `gptqueue-inbox-peek --list`: the unclaimed
 * messages in queue order, whether each would wake the recipient (the
 * dispatcher's own rule, core/inbox-events.ts) and the inbox's open claims.
 *
 * Reads only. One MULTI/EXEC snapshot holds TIME, LLEN and LRANGE on the
 * queue and ZRANGE on the claim index; then MGET reads the outstanding-request
 * owners of the listed replies and HMGET reads the claim records. It never
 * pops, claims, leases or writes anything.
 */
import type { Redis } from "ioredis";
import { CLAIM_KEYS, SESSION_KEYS } from "../core/keys.js";
import { parseQueueEntry, replyOwners, wakesRecipient } from "../core/inbox-events.js";
import type { QueueMessage } from "../core/types.js";
import { probeFailure, type ProbeErrorCode } from "./inbox-probe.js";

export const LIST_LIMIT = Object.freeze({ default: 50, max: 500 });
const PREVIEW_CHARS = 80;

export type ListedMessage = Readonly<{
  id: string | null;
  from: string | null;
  type: string | null;
  timestamp: string | null;
  /** Seconds since `timestamp`, measured against Redis TIME; null when the timestamp is unreadable. */
  age_s: number | null;
  in_reply_to: string | null;
  /** Whether the dispatcher would wake the recipient for this message. */
  wakes: boolean;
  preview: string | null;
}>;
export type UnparseableEntry = Readonly<{ raw_index: number; error: "unparseable" }>;
export type ListedEntry = ListedMessage | UnparseableEntry;
export type ListedClaim = Readonly<{
  claim_id: string;
  session_id: string | null;
  claimed_at: string | null;
  age_s: number | null;
  /** From the claim-index score, the expiry recovery acts on (renewal moves it with the record). */
  expires_at: string | null;
  expired: boolean;
  task_count: number | null;
}>;
export type InboxListing = Readonly<{
  agent: string;
  unclaimed: number;
  listed: number;
  truncated: boolean;
  /** Age of the oldest unclaimed message in the whole queue; null when it is empty or no timestamp is readable. */
  oldest_unclaimed_age_s: number | null;
  /** Listed messages that would wake the recipient. */
  wake_eligible_count: number;
  messages: readonly ListedEntry[];
  claims: readonly ListedClaim[];
  expired_claims: number;
}>;
export type ListResult =
  | Readonly<{ ok: true; listing: InboxListing }>
  | Readonly<{ ok: false; code: ProbeErrorCode; message: string }>;

const text = (value: unknown): string | null => typeof value === "string" ? value : null;

/** Whole seconds from an ISO time to `nowMs`; null when the time is unreadable. */
const ageSeconds = (nowMs: number, iso: unknown): number | null => {
  const at = typeof iso === "string" ? Date.parse(iso) : NaN;
  return Number.isFinite(at) ? Math.floor((nowMs - at) / 1000) : null;
};

/** The first 80 characters (code points) of `content`, whitespace collapsed, "…" appended when cut. */
export const previewOf = (content: unknown): string | null => {
  if (typeof content !== "string") return null;
  const chars = Array.from(content.replace(/\s+/g, " ").trim());
  return chars.length > PREVIEW_CHARS ? `${chars.slice(0, PREVIEW_CHARS).join("")}…` : chars.join("");
};

const listMessage = (agent: string, nowMs: number, message: QueueMessage, owner: string | null): ListedMessage => Object.freeze({
  id: text(message.id),
  from: text(message.from),
  type: text(message.type),
  timestamp: text(message.timestamp),
  age_s: ageSeconds(nowMs, message.timestamp),
  in_reply_to: text(message.payload?.in_reply_to),
  wakes: wakesRecipient(agent, message, owner),
  preview: previewOf(message.payload?.content),
});

type StoredClaim = Readonly<{ session_id?: unknown; claimed_at?: unknown; tasks?: unknown }>;

const parseClaim = (raw: string | null): StoredClaim | null => {
  if (raw === null) return null;
  try {
    const value: unknown = JSON.parse(raw);
    return typeof value === "object" && value !== null ? value as StoredClaim : null;
  } catch { return null; }
};

const listClaim = (nowMs: number, claimId: string, score: string, raw: string | null): ListedClaim => {
  const expiresMs = Number(score);
  const record = parseClaim(raw);
  return Object.freeze({
    claim_id: claimId,
    session_id: text(record?.session_id),
    claimed_at: text(record?.claimed_at),
    age_s: ageSeconds(nowMs, record?.claimed_at),
    expires_at: Number.isFinite(expiresMs) ? new Date(expiresMs).toISOString() : null,
    // The same comparison as the probe and the watcher: score < Redis TIME.
    expired: expiresMs < nowMs,
    task_count: Array.isArray(record?.tasks) ? record.tasks.length : null,
  });
};

const execResults = (replies: [Error | null, unknown][] | null): readonly unknown[] => {
  if (replies === null) throw new Error("inbox snapshot transaction was aborted");
  return replies.map(([error, value]) => { if (error) throw error; return value; });
};

/** List `agent`'s inbox (up to `limit` messages, oldest first) without consuming anything. */
export const listInbox = async (redis: Redis, agent: string, limit: number): Promise<ListResult> => {
  try {
    const queue = SESSION_KEYS.queue(agent);
    // One transaction of reads: a claim moves mail from the list to the claim
    // index in one Lua call, so separate reads could straddle it. The whole
    // list is read because claims-recover.lua RPUSHes recovered (older) tasks
    // to the tail, so the oldest unclaimed mail may lie beyond `limit`.
    const [time, unclaimed, raw, index] = execResults(await redis.multi().time().llen(queue).lrange(queue, 0, -1).zrange(CLAIM_KEYS.index(agent), 0, -1, "WITHSCORES").exec()) as [[string, string], number, string[], string[]];
    const nowMs = Number(time[0]) * 1000 + Math.floor(Number(time[1]) / 1000);
    const entries = raw.map(parseQueueEntry);
    const shown = entries.slice(0, limit);
    const parsed = shown.filter((message): message is QueueMessage => message !== null);
    const claimIds = index.filter((_, i) => i % 2 === 0);
    const [owners, records] = await Promise.all([
      replyOwners(redis, agent, parsed),
      claimIds.length === 0 ? Promise.resolve([] as (string | null)[]) : redis.hmget(CLAIM_KEYS.claims, ...claimIds),
    ]);
    const ownerOf = new Map(parsed.map((message, i) => [message, owners[i] ?? null]));
    const messages = shown.map((message, rawIndex): ListedEntry => message === null
      ? Object.freeze({ raw_index: rawIndex, error: "unparseable" as const })
      : listMessage(agent, nowMs, message, ownerOf.get(message) ?? null));
    const oldest = entries.reduce<number | null>((max, message) => {
      const age = message === null ? null : ageSeconds(nowMs, message.timestamp);
      return age === null ? max : Math.max(max ?? age, age);
    }, null);
    const claims = claimIds.map((claimId, i) => listClaim(nowMs, claimId, index[2 * i + 1] ?? "", records[i] ?? null));
    return Object.freeze({ ok: true, listing: Object.freeze({
      agent,
      unclaimed,
      listed: messages.length,
      truncated: unclaimed > limit,
      oldest_unclaimed_age_s: oldest,
      wake_eligible_count: messages.filter((entry) => "wakes" in entry && entry.wakes).length,
      messages: Object.freeze(messages),
      claims: Object.freeze(claims),
      expired_claims: claims.filter((claim) => claim.expired).length,
    }) });
  } catch (error) {
    return probeFailure(error);
  }
};

/** Sender-controlled text is printed to a terminal: replace control characters. */
const printable = (value: string): string => value.replace(/\p{Cc}/gu, "?");
const seconds = (value: number | null): string => value === null ? "-" : `${value}s`;

/** Columns padded to their widest cell, two spaces apart; the last column is not padded. */
const table = (rows: readonly (readonly string[])[]): readonly string[] => {
  const widths = (rows[0] ?? []).map((_, column) => Math.max(...rows.map((row) => (row[column] ?? "").length)));
  return rows.map((row) => row.map((cell, column) => column === row.length - 1 ? cell : cell.padEnd(widths[column] ?? 0)).join("  ").trimEnd());
};

/** The human form of a listing: a summary line, the message table, then the claims. */
export const formatListing = (listing: InboxListing): string => {
  const summary = `${printable(listing.agent)}: ${listing.unclaimed} unclaimed, ${listing.listed} listed`
    + `${listing.truncated ? ` (truncated; --limit up to ${LIST_LIMIT.max})` : ""}, ${listing.wake_eligible_count} would wake, oldest ${seconds(listing.oldest_unclaimed_age_s)}`;
  const messageRows = listing.messages.map((entry, i) => "error" in entry
    ? [String(entry.raw_index), "-", "-", "-", "-", "-", "(unparseable entry)"]
    : [String(i), seconds(entry.age_s), entry.type ?? "-", entry.wakes ? "yes" : "no", entry.from ?? "-", entry.id ?? "-", entry.preview ?? ""].map(printable));
  const claimRows = listing.claims.map((claim) => [claim.claim_id, claim.session_id ?? "-", seconds(claim.age_s), claim.expires_at ?? "-", claim.expired ? "yes" : "no", claim.task_count === null ? "-" : String(claim.task_count)].map(printable));
  const lines = [
    summary,
    ...(messageRows.length === 0 ? [] : table([["#", "AGE", "TYPE", "WAKES", "FROM", "ID", "PREVIEW"], ...messageRows])),
    listing.claims.length === 0 ? "claims: none" : `claims: ${listing.claims.length} open, ${listing.expired_claims} expired`,
    ...(claimRows.length === 0 ? [] : table([["CLAIM", "SESSION", "AGE", "EXPIRES", "EXPIRED", "TASKS"], ...claimRows])),
  ];
  return `${lines.join("\n")}\n`;
};
