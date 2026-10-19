/**
 * Acceptance fixtures use the ONE private Redis implementation: a socket-only
 * redis-server per call, verified private before use (scripts/lib/owned-redis.mjs).
 *
 * Its URL is a unix-socket path (`<run dir>/redis.sock?db=N`), not a TCP URL:
 * never parse it with `new URL()`. Receipts and child configuration are built
 * here, from socket-aware metadata, and covered by owned-redis-metadata.test.ts.
 */
import { parsePrivateRedisUrl, privateRedisEnv } from "../../scripts/lib/owned-redis.mjs";

export { startOwnedRedis, type OwnedRedis } from "../../scripts/lib/owned-redis.mjs";

export type OwnedRedisReceipt = Readonly<{ transport: "unix-socket"; database: string; owned_process: true }>;

/** Receipt metadata for an owned private Redis: its transport and database (no host, port or path). */
export const ownedRedisReceipt = (url: string): OwnedRedisReceipt =>
  Object.freeze({ transport: "unix-socket", database: String(parsePrivateRedisUrl(url).db), owned_process: true });

/**
 * Gemini CLI workspace settings that start the gptqueue stdio MCP server
 * against an owned private Redis; the server env names it under every Redis
 * endpoint alias (privateRedisEnv).
 */
export const geminiMcpSettings = (serverScript: string, url: string, nodePath: string = process.execPath) => ({
  hooksConfig: { enabled: false },
  tools: { core: [] as string[] },
  security: { folderTrust: { enabled: false } },
  mcpServers: { gptqueue: { command: nodePath, args: [serverScript], env: privateRedisEnv({}, url), trust: true } },
});
