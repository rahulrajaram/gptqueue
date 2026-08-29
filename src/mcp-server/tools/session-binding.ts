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

/**
 * Session-bound tool prologue (M6): the repeated `ensureSessionBinding ->
 * requireRegistered -> throw if no session` pattern consolidated into one
 * helper. Every session-bound tool calls this and uses the returned agent
 * name + session id, so a lost/unknown session reports the same condition
 * under the same error code (SESSION_UNAVAILABLE via stableToolError).
 *
 * Throws "Session binding not found..." (matches tool-result.ts's
 * SESSION_UNAVAILABLE regex) when the caller is registered but has no bound
 * session, and lets requireRegistered() throw AGENT_NOT_REGISTERED when the
 * caller was never registered at all.
 */
export async function bindSession(
  client: RedisClient,
  sessionId?: string
): Promise<{ agent: string; sessionId: string }> {
  await ensureSessionBinding(client, sessionId);
  const agent = client.requireRegistered();
  const bound = client.sessionId;
  if (!bound) {
    throw new Error(
      "Session binding not found. Call register_agent first with a name and retain the session_id."
    );
  }
  return { agent, sessionId: bound };
}
