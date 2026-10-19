/**
 * The ONE accessor for the run's private test Redis.
 *
 * vitest globalSetup (tests/helpers/global-setup.ts) starts a private,
 * socket-only redis-server per run and exports REDIS_URL as
 * `<runDir>/redis.sock?db=15`. Tests read it only through testRedisUrl(), which
 * refuses anything else: an unset value, any TCP URL (including the
 * production 127.0.0.1:6379) and any socket outside a 0700 run directory owned
 * by this user. There is no fallback. Validation lives in
 * scripts/lib/owned-redis.mjs, shared with the verify scripts.
 */
import { parsePrivateRedisUrl, privateRedisUrl, type PrivateRedisTarget } from "../../scripts/lib/owned-redis.mjs";

/** REDIS_URL, verified to be this run's private socket instance. Throws otherwise. */
export const testRedisUrl = (): string => privateRedisUrl(process.env.REDIS_URL);

/** The parsed private target ({ path, dir, db }) of a validated URL. */
export const testRedisTarget = (url: string = testRedisUrl()): PrivateRedisTarget => parsePrivateRedisUrl(privateRedisUrl(url));
