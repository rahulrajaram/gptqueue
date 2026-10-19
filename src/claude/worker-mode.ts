/**
 * Headless worker sessions (f-1965). A `claude -p` child (print mode, which
 * Claude Code marks with CLAUDE_CODE_ENTRYPOINT=sdk-cli) inherits the user's
 * hooks and the gptqueue MCP server, but it is a one-shot worker, not a
 * mailbox owner: it must not be auto-registered, gated on an inbox, or
 * recorded as a turn-taking agent.
 *
 *   worker = GPTQUEUE_WORKER === "1"
 *         || (GPTQUEUE_WORKER !== "0" && CLAUDE_CODE_ENTRYPOINT === "sdk-cli" && !GPTQ_AGENT_NAME)
 *
 * An explicit GPTQ_AGENT_NAME keeps mailbox ownership: a deliberately named
 * print-mode actor may own a mailbox. GPTQUEUE_WORKER=0 opts a print-mode
 * session back in; GPTQUEUE_WORKER=1 forces worker mode anywhere.
 *
 * Dependency-free: bin/gptqueue-claude-mcp evaluates it on the inherited env
 * before it sets GPTQ_AGENT_NAME itself.
 */
export const isWorkerSession = (env: NodeJS.ProcessEnv = process.env): boolean =>
  env.GPTQUEUE_WORKER === "1"
  || (env.GPTQUEUE_WORKER !== "0" && env.CLAUDE_CODE_ENTRYPOINT === "sdk-cli" && !env.GPTQ_AGENT_NAME);

export const WORKER_SESSION_CONTEXT =
  "gptqueue: headless worker session (print mode); not registered as a mailbox owner, no inbox to claim. Set GPTQUEUE_WORKER=0 to opt in.";

/**
 * Explicit-only worker mode for processes that are not Claude print-mode
 * children, such as the registered-shell sidecar (bin/gptqueue-session) that
 * Codex, Pi and OpenCode launch. Only GPTQUEUE_WORKER === "1" selects it;
 * "0", unset or any other value keeps normal mailbox ownership.
 */
export const isExplicitWorker = (env: NodeJS.ProcessEnv = process.env): boolean =>
  env.GPTQUEUE_WORKER === "1";
