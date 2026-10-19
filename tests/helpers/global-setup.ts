/**
 * vitest globalSetup: one private, socket-only redis-server per vitest run.
 *
 * Runs in the main process before any worker or child starts (vitest awaits
 * globalSetup before it creates the pool, and workers inherit process.env).
 * startOwnedRedis verifies the instance is private (port 0, our socket, our
 * pid) BEFORE it returns, so a failed check exports nothing and fails the run.
 */
import { startOwnedRedis, type OwnedRedis, type StartOwnedRedisOptions } from "../../scripts/lib/owned-redis.mjs";

/** Start and verify a private instance, then (and only then) export REDIS_URL into `env`. */
export async function exportPrivateRedis(
  options: StartOwnedRedisOptions = {},
  env: NodeJS.ProcessEnv = process.env
): Promise<OwnedRedis> {
  const redis = await startOwnedRedis({ db: 15, ...options });
  // Any inherited REDIS_URL (e.g. a TCP one) is replaced, never used.
  env.REDIS_URL = redis.url;
  return redis;
}

export default async function setup(): Promise<() => Promise<void>> {
  const redis = await exportPrivateRedis();
  return async () => {
    await redis.close();
  };
}
