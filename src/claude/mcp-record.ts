/**
 * Publish this MCP server's held identity for the enclosing Claude session
 * (see session-record.ts for the key). Enabled by GPTQ_CLAUDE_SESSION_RECORD=1,
 * which bin/gptqueue-claude-mcp sets; other runtimes are unaffected.
 */
import { processKey, publishSessionRecord, readProcess, type ProcessInfo } from "./session-record.js";

export type IdentitySource = { onIdentityChange(observer: (name: string | null, sessionId: string | null) => void): void; agentName: string | null; sessionId: string | null };

export const claudeProcess = (env: NodeJS.ProcessEnv = process.env): ProcessInfo | null =>
  readProcess(env.GPTQ_CLAUDE_PID ? Number(env.GPTQ_CLAUDE_PID) : process.ppid);

/** Returns a cleanup that removes the record, or null when disabled/unavailable. */
export const startClaudeSessionRecord = (client: IdentitySource, env: NodeJS.ProcessEnv = process.env): (() => void) | null => {
  if (env.GPTQ_CLAUDE_SESSION_RECORD !== "1") return null;
  const claude = claudeProcess(env);
  if (!claude) return null;
  const publish = (name: string | null, sessionId: string | null) => {
    try { publishSessionRecord(claude, name, sessionId, env); }
    catch (error) { process.stderr.write(`${JSON.stringify({ event: "claude_session_record_failed", key: processKey(claude), message: (error as Error).message })}\n`); }
  };
  client.onIdentityChange(publish);
  publish(client.agentName, client.sessionId);
  return () => publish(null, null);
};

/** True for the core's typed refusal AGENT_NAME_HELD of a name held by a live session (decision D2; compared case-insensitively). */
export const isNameHeld = (error: unknown): boolean => {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" && code.toLowerCase() === "agent_name_held";
};

/**
 * Deterministic distinct name for a Claude session whose preferred name is
 * held: `<name>-<first 8 chars of the Claude session id>`, else `<name>-<claude pid>`.
 */
export const fallbackName = (name: string, env: NodeJS.ProcessEnv = process.env, claudePid: number | null = claudeProcess(env)?.pid ?? null): string => {
  const sessionId = (env.CLAUDE_SESSION_ID || env.CLAUDE_CODE_SESSION_ID || "").replace(/[^A-Za-z0-9]/g, "").slice(0, 8);
  return `${name}-${sessionId || String(claudePid ?? process.pid)}`;
};

export type Registrar = { register(role: string, name: string): Promise<{ name: string; session_id: string }> };

/**
 * Register `name`; under Claude (GPTQ_CLAUDE_SESSION_RECORD=1) and unless
 * sharing was requested (GPTQ_AGENT_SHARED=1), a live holder's refusal falls
 * back to fallbackName(). Every other error, and a refusal of the fallback,
 * propagates. The record published through onIdentityChange carries the
 * name actually registered.
 */
export const registerClaudeAware = async (client: Registrar, name: string, env: NodeJS.ProcessEnv = process.env): Promise<{ name: string; session_id: string }> => {
  try {
    return await client.register("both", name);
  } catch (error) {
    if (!isNameHeld(error) || env.GPTQ_CLAUDE_SESSION_RECORD !== "1" || env.GPTQ_AGENT_SHARED === "1") throw error;
    const distinct = fallbackName(name, env);
    process.stderr.write(`${JSON.stringify({ event: "agent_name_held_fallback", requested: name, registered: distinct })}\n`);
    return client.register("both", distinct);
  }
};
