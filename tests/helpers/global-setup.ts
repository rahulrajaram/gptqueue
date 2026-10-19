/**
 * vitest globalSetup: one private, socket-only redis-server per vitest run.
 *
 * Runs in the main process before any worker or child starts (vitest awaits
 * globalSetup before it creates the pool, and workers inherit process.env).
 * startOwnedRedis verifies the instance is private (port 0, our socket, our
 * pid) BEFORE it returns, so a failed check exports nothing and fails the run.
 */
import { privateRedisEnv, startOwnedRedis, type OwnedRedis, type StartOwnedRedisOptions } from "../../scripts/lib/owned-redis.mjs";

/**
 * Start and verify a private instance, then (and only then) point EVERY Redis
 * endpoint variable in `env` (REDIS_URL, GPTQUEUE_REDIS_URL, ...; see
 * REDIS_ENDPOINT_ENV) at it. Inherited values, e.g. a TCP GPTQUEUE_REDIS_URL,
 * are replaced, never used.
 */
export async function exportPrivateRedis(
  options: StartOwnedRedisOptions = {},
  env: NodeJS.ProcessEnv = process.env
): Promise<OwnedRedis> {
  const redis = await startOwnedRedis({ db: 15, ...options });
  Object.assign(env, privateRedisEnv(env, redis.url));
  return redis;
}

export default async function setup(): Promise<() => Promise<void>> {
  const redis = await exportPrivateRedis();
  return async () => {
    await redis.close();
  };
}
