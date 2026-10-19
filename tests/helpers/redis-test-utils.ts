import type { Redis } from "ioredis";
import { parsePrivateRedisUrl, privateRedisUrl } from "../../scripts/lib/owned-redis.mjs";

/**
 * Shared `gptq:*` flush for test Redis clients.
 *
 * The live GPTQueue server uses the production Redis (TCP 127.0.0.1:6379,
 * db0). Tests never reach it: every vitest run owns a private, socket-only
 * redis-server (tests/helpers/global-setup.ts, scripts/lib/owned-redis.mjs)
 * and EVERY `gptq:*` flush in the suite funnels through `flushTestKeys`, which:
 *
 *   1. accepts only that run's private socket URL (any TCP URL is refused),
 *   2. requires the CLIENT itself to be connected to that exact socket and
 *      database (never db0), and
 *   3. deletes only `gptq:*` keys (never FLUSHDB).
 */

/** Fail closed unless `redisUrl` is this run's private socket instance. */
export function assertPrivateTestRedis(redisUrl: string | undefined, context: string): void {
  try {
    privateRedisUrl(redisUrl);
  } catch (error) {
    throw new Error(`${context}: ${(error as Error).message}`);
  }
}

/**
 * Scan-and-delete every `gptq:*` key on the connected database, after checking
 * both the URL (defaults to REDIS_URL) and the client's actual connection.
 */
export async function flushTestKeys(
  redis: Redis,
  redisUrl?: string
): Promise<void> {
  const url = redisUrl ?? process.env.REDIS_URL;
  assertPrivateTestRedis(url, "flushTestKeys");
  const target = parsePrivateRedisUrl(url);
  const { path, db } = redis.options;
  if (path !== target.path || Number(db ?? 0) !== target.db) {
    throw new Error("flushTestKeys: the client is not connected to the run's private test Redis socket and database");
  }
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
