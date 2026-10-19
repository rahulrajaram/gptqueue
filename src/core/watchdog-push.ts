/**
 * Watchdog reminder send (X3 re-plan): per-recipient rate admission and the
 * enqueue are ONE atomic script, timed by Redis's own clock (`TIME`), so no
 * caller clock, slow pass or stalled connection can skew the rolling hour.
 *
 * The script is composed from bounded-push-idempotent.lua, the normal
 * idempotent send, by inserting at fixed anchors, so the delivery effects
 * (queue entry, metadata, inbox event, outstanding key, task ledger,
 * idempotency record) cannot drift from the normal send path. A missing
 * anchor throws at load time.
 *
 * Order: idempotency check (duplicate: nothing changes, no rate entry),
 * forwarding tombstone, waking admission (refused: nothing changes, reply
 * {0, id, reason}), then the rate
 * check (limited: nothing changes beyond trimming expired rate entries), then
 * the normal enqueue, and finally the rate entry at Redis TIME.
 *
 * Extra KEYS[9]: the recipient's rate set. Extra ARGV[13]: fire key, ARGV[14]: cap
 * (ARGV[12] is the base script's outstanding-key prefix).
 * Returns {3, id} when limited; otherwise the base script's replies.
 */

export const WATCHDOG_RATE_WINDOW_MS = 60 * 60 * 1000;
export const WATCHDOG_RATE_SET_TTL_SECONDS = 2 * 60 * 60;

const FULL_CHECK = "if refusal then return {0, ARGV[3], refusal} end";
const SENT_REPLY = "return {1, ARGV[3]}";

const RATE_CHECK = `
-- X3: rate admission, after the waking-admission check and before any write, by Redis TIME.
local clock = redis.call('TIME')
local rate_now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
redis.call('ZREMRANGEBYSCORE', KEYS[9], '-inf', rate_now - ${WATCHDOG_RATE_WINDOW_MS})
local rate_member = redis.call('ZSCORE', KEYS[9], ARGV[13])
if not rate_member and redis.call('ZCARD', KEYS[9]) >= tonumber(ARGV[14]) then return {3, ARGV[3]} end`;

const RATE_RECORD = `-- X3: the enqueue happened; count it at Redis TIME (an existing member takes no new slot).
if not rate_member then redis.call('ZADD', KEYS[9], rate_now, ARGV[13]) end
redis.call('EXPIRE', KEYS[9], ${WATCHDOG_RATE_SET_TTL_SECONDS})
`;

const insertAt = (text: string, anchor: string, build: (anchor: string) => string): string => {
  const at = text.indexOf(anchor);
  if (at < 0 || text.indexOf(anchor, at + anchor.length) >= 0) {
    throw new Error(`watchdog push: anchor not found exactly once in bounded-push-idempotent.lua: ${anchor}`);
  }
  return text.slice(0, at) + build(anchor) + text.slice(at + anchor.length);
};

/** Pure: the watchdog send script, composed from the idempotent push script's text. */
export const composeWatchdogPush = (idempotentPushLua: string): string =>
  insertAt(
    insertAt(idempotentPushLua, FULL_CHECK, (anchor) => `${anchor}${RATE_CHECK}`),
    SENT_REPLY,
    (anchor) => `${RATE_RECORD}${anchor}`,
  );
