/**
 * Acceptance fixtures use the ONE private Redis implementation: a socket-only
 * redis-server per call, verified private before use (scripts/lib/owned-redis.mjs).
 */
export { startOwnedRedis, type OwnedRedis } from "../../scripts/lib/owned-redis.mjs";
