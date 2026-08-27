import type { RedisClient } from "../redis-client.js";

/**
 * Ensure a session-scoped tool call is bound to a durable session.
 *
 * Stateful transports can keep agent identity in the RedisClient instance.
 * Stateless or bridge-based transports may hand each tool call a fresh
 * RedisClient with no in-memory registration state. In that case callers
 * can pass the `session_id` returned by `register_agent` and we reconnect
 * on demand before touching requireRegistered().
 */
export async function ensureSessionBinding(
  client: RedisClient,
  sessionId?: string
): Promise<void> {
  if (!sessionId) {
    return;
  }

  if (client.sessionId === sessionId && client.registered) {
    return;
  }

  await client.reconnectSession(sessionId);
}
