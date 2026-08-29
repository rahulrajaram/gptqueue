import { Redis } from "ioredis";

/**
 * Shared `gptq:*` flush for test Redis clients, with a live-db0 guard (M11).
 *
 * The live GPTQueue server runs on Redis db0 (the default database), so a test
 * that flushes `gptq:*` on db0 would silently wipe the live server's data. To
 * make that impossible, EVERY `gptq:*` flush in the test suite funnels through
 * `flushTestKeys`, which:
 *
 *   1. resolves the target database index from the connection URL,
 *   2. REFUSES to touch db0 unless the operator explicitly opts in with
 *      GPTQUEUE_ALLOW_DB0=1, and
 *   3. deletes only `gptq:*` keys (never FLUSHDB).
 *
 * Test suites point REDIS_URL (or pass an explicit redisUrl) at an isolated
 * database such as redis://127.0.0.1:6379/15, which is what the shared unit
 * defaults and the integration server use. A bare `npx vitest run` therefore
 * either runs on db15 or refuses — it can never wipe db0.
 */

const DEFAULT_URL = "redis://127.0.0.1:6379";
const ALLOW_DB0_ENV = "GPTQUEUE_ALLOW_DB0";

/** Extract the database index from a Redis URL; 0 when no db is specified. */
export function dbIndexOf(url: string): number {
  const m = url.match(/\/(\d+)(?:[?#].*)?$/);
  return m ? Number.parseInt(m[1], 10) : 0;
}

/**
 * Fail closed unless the target database is not the live server's db0, or the
 * operator explicitly allows db0 with GPTQUEUE_ALLOW_DB0=1.
 */
export function assertNotLiveDb(redisUrl: string, context: string): void {
  if (dbIndexOf(redisUrl) === 0 && process.env[ALLOW_DB0_ENV] !== "1") {
    throw new Error(
      `${context}: refusing to flush gptq:* keys on Redis db 0. The live ` +
        `GPTQueue server (port 8101) and its mailboxes live on db0, and a ` +
        `test flush here would wipe the live server's data. Point REDIS_URL ` +
        `at an isolated database (e.g. redis://127.0.0.1:6379/15) or set ` +
        `${ALLOW_DB0_ENV}=1 to explicitly allow operating on db0.`
    );
  }
}

/**
 * Scan-and-delete every `gptq:*` key on the connected database, after the
 * db0 guard. `redisUrl` is the URL of the connection the client uses; it
 * drives the db0 guard and defaults to REDIS_URL (or the plain db0 default,
 * which the guard will then refuse).
 */
export async function flushTestKeys(
  redis: Redis,
  redisUrl?: string
): Promise<void> {
  const url = redisUrl ?? process.env.REDIS_URL ?? DEFAULT_URL;
  assertNotLiveDb(url, "flushTestKeys");
  const keys: string[] = [];
  let cursor = "0";
  do {
    const [next, batch] = (await redis.scan(
      cursor,
      "MATCH",
      "gptq:*",
      "COUNT",
      500
    )) as [string, string[]];
    cursor = next;
    keys.push(...batch);
  } while (cursor !== "0");
  if (keys.length > 0) {
    // Chunked DEL to stay well under any command-size limits.
    for (let i = 0; i < keys.length; i += 200) {
      await redis.del(...keys.slice(i, i + 200));
    }
  }
}