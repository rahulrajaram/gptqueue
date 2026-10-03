import * as path from "node:path";

/**
 * Generic attribution env for PTY-wrapped child CLI processes.
 * Queue-mediated agents can attribute work to the wrapper (caller), the
 * project, and the agent session. Inherited nonempty overrides are preserved.
 */
export function agentAttributionEnv(
  agentName: string,
  env: NodeJS.ProcessEnv = process.env,
  cwd: string = process.cwd()
): Record<string, string> {
  return {
    AGENT_ATTRIBUTION_CALLER: env.AGENT_ATTRIBUTION_CALLER || "gptqueue-pty",
    AGENT_ATTRIBUTION_PROJECT:
      env.AGENT_ATTRIBUTION_PROJECT || path.basename(cwd),
    AGENT_ATTRIBUTION_SESSION: env.AGENT_ATTRIBUTION_SESSION || agentName,
  };
}
