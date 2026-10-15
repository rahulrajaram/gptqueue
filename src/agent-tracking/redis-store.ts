/**
 * Redis SeatStore. It needs only a client with `eval` (ioredis satisfies this
 * structurally), so the library takes no Redis dependency. Each operation is
 * one Lua script: a commit compares the stored revision, writes the tenure and
 * appends history atomically. Stored records are re-admitted on every read;
 * anything malformed is store_corrupt, never guessed at.
 */
import type { SeatStore, StoreCommit, StoreHistory, StoreRead, StoredTenure } from "./ports.js";
import type { HolderIdentity, SeatErrorCode, SeatTenure, TenureHistoryEntry } from "./types.js";

/** Any Redis client with `eval`. A nil reply may come back as null or undefined. */
export interface EvalClient {
  eval(script: string, numKeys: number, ...args: (string | number)[]): Promise<unknown>;
}

export interface RedisSeatStoreOptions {
  /** Key prefix, e.g. "gptq:seat:". Tenure at `<prefix>tenure:<seat>`, history at `<prefix>history:<seat>`. */
  readonly prefix: string;
}

const READ_SCRIPT = "return redis.call('GET', KEYS[1])";
const HISTORY_SCRIPT = "return redis.call('LRANGE', KEYS[1], 0, -1)";
/** KEYS: tenure, history. ARGV: expected rev, new record JSON, entry JSON... Returns 1 written, 0 stale, -1 corrupt. */
const COMMIT_SCRIPT = `
-- Check every key's type before the first write: Redis does not roll back a
-- script that fails midway, so a wrong-typed history must not leave the tenure
-- changed without its history.
local htype = redis.call('TYPE', KEYS[2]).ok
if htype ~= 'none' and htype ~= 'list' then return -1 end
local ttype = redis.call('TYPE', KEYS[1]).ok
if ttype ~= 'none' and ttype ~= 'string' then return -1 end
local current = redis.call('GET', KEYS[1])
local rev = 0
if current then
  local ok, decoded = pcall(cjson.decode, current)
  if not ok or type(decoded) ~= 'table' or type(decoded.rev) ~= 'number' then return -1 end
  rev = decoded.rev
end
if rev ~= tonumber(ARGV[1]) then return 0 end
redis.call('SET', KEYS[1], ARGV[2])
for i = 3, #ARGV do redis.call('RPUSH', KEYS[2], ARGV[i]) end
return 1
`;

const fail = (code: SeatErrorCode, message: string) =>
  Object.freeze({ ok: false as const, error: Object.freeze({ code, message }) });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const isText = (value: unknown): value is string => typeof value === "string" && value.length > 0;
const isCount = (value: unknown): value is number => Number.isSafeInteger(value) && (value as number) >= 0;

const admitHolder = (value: unknown): HolderIdentity | null =>
  isRecord(value) &&
  isText(value.runtime) && isText(value.session_id) && isText(value.host) && isText(value.pid_start) &&
  [value.machine_id, value.boot_id, value.pidns].every((s) => s === null || isText(s)) &&
  isCount(value.pid) && value.pid > 0
    ? Object.freeze({
        runtime: value.runtime, session_id: value.session_id, host: value.host,
        machine_id: value.machine_id as string | null, boot_id: value.boot_id as string | null,
        pidns: value.pidns as string | null, pid: value.pid, pid_start: value.pid_start,
      })
    : null;

/** Re-admit a stored record, or null when it is malformed or belongs to another seat. */
const admitStored = (value: unknown, seat_id: string): StoredTenure | null => {
  if (!isRecord(value) || !isCount(value.rev) || value.rev < 1 || !isRecord(value.tenure)) return null;
  const t = value.tenure;
  if (t.seat_id !== seat_id || !isCount(t.epoch)) return null;
  if (t.state === "vacant") return Object.freeze({ rev: value.rev, tenure: Object.freeze({ seat_id, state: "vacant", epoch: t.epoch }) });
  if (t.state !== "held" || t.epoch < 1) return null; // the first tenure is epoch 1
  const holder = admitHolder(t.holder);
  if (holder === null || !isText(t.bound_at) || !isText(t.lease_expires_at)) return null;
  const tenure: SeatTenure = Object.freeze({
    seat_id, state: "held", epoch: t.epoch, holder, bound_at: t.bound_at, lease_expires_at: t.lease_expires_at,
  });
  return Object.freeze({ rev: value.rev, tenure });
};

const admitEntry = (value: unknown, seat_id: string): TenureHistoryEntry | null => {
  if (!isRecord(value) || value.seat_id !== seat_id || !isCount(value.epoch)) return null;
  if (!isText(value.runtime) || !isText(value.session_id) || !isText(value.at)) return null;
  const base = { seat_id, epoch: value.epoch, runtime: value.runtime, session_id: value.session_id, at: value.at };
  switch (value.kind) {
    case "bind":
      return value.reason === "vacant" || value.reason === "takeover"
        ? Object.freeze({ kind: "bind", ...base, reason: value.reason })
        : null;
    case "rebind":
      return isText(value.previous_session_id)
        ? Object.freeze({ kind: "rebind", ...base, previous_session_id: value.previous_session_id })
        : null;
    case "vacate":
      return value.reason === "released" || value.reason === "holder_exit_proven" || value.reason === "lease_expired"
        ? Object.freeze({ kind: "vacate", ...base, reason: value.reason })
        : null;
    default:
      return null;
  }
};

const parse = (raw: string): unknown => {
  try {
    return JSON.parse(raw);
  } catch {
    return undefined;
  }
};

export const createRedisSeatStore = (client: EvalClient, options: RedisSeatStoreOptions): SeatStore => {
  const tenureKey = (seat_id: string): string => `${options.prefix}tenure:${seat_id}`;
  const historyKey = (seat_id: string): string => `${options.prefix}history:${seat_id}`;

  const read = async (seat_id: string): Promise<StoreRead> => {
    const raw = await client.eval(READ_SCRIPT, 1, tenureKey(seat_id));
    if (raw === null || raw === undefined) return { ok: true, stored: null }; // Redis nil, however the client spells it
    const stored = typeof raw === "string" ? admitStored(parse(raw), seat_id) : null;
    return stored === null
      ? fail("store_corrupt", `stored tenure for seat '${seat_id}' is malformed; refusing to operate on it`)
      : { ok: true, stored };
  };

  const commit = async (
    seat_id: string, expected_rev: number, tenure: SeatTenure, entries: readonly TenureHistoryEntry[]
  ): Promise<StoreCommit> => {
    const record = JSON.stringify({ rev: expected_rev + 1, tenure });
    const result = await client.eval(
      COMMIT_SCRIPT, 2, tenureKey(seat_id), historyKey(seat_id),
      expected_rev, record, ...entries.map((entry) => JSON.stringify(entry))
    );
    switch (result) {
      case 1: return { ok: true, committed: true };
      case 0: return { ok: true, committed: false };
      default: return fail("store_corrupt", `stored tenure for seat '${seat_id}' is malformed; refusing to overwrite it`);
    }
  };

  const history = async (seat_id: string): Promise<StoreHistory> => {
    const raw = await client.eval(HISTORY_SCRIPT, 1, historyKey(seat_id));
    if (!Array.isArray(raw)) return fail("store_corrupt", `history for seat '${seat_id}' is not a list`);
    const entries = raw.map((item) => (typeof item === "string" ? admitEntry(parse(item), seat_id) : null));
    return entries.every((entry): entry is TenureHistoryEntry => entry !== null)
      ? { ok: true, entries: Object.freeze(entries) }
      : fail("store_corrupt", `history for seat '${seat_id}' holds a malformed entry`);
  };

  return Object.freeze({ read, commit, history });
};
