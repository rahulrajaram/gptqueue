/**
 * Names of the tools a registered shell exposes beyond the base gptqueue set.
 * Dependency-free so the Pi extension can import it without loading the
 * server. runtime-tools.ts and diagnostic-tools.ts register exactly these
 * (tests/registered-shell-tool-names.test.ts checks it).
 */

/** Claim protocol and runtime binding; a runtime shell cannot work without them. */
export const RUNTIME_TOOL_NAMES = Object.freeze([
  "claim_tasks", "acknowledge_tasks", "renew_claim", "bind_runtime", "get_runtime_status",
] as const);

/**
 * Claim-protocol extensions a runtime may use but does not require, so a
 * newer runtime still works against an older shell server without them.
 * dlq_requeue restores one of the caller's own dead-lettered messages.
 */
export const OPTIONAL_RUNTIME_TOOL_NAMES = Object.freeze(["acknowledge_messages", "dlq_requeue"] as const);

/** Read-mostly diagnostics; useful but optional. */
export const DIAGNOSTIC_TOOL_NAMES = Object.freeze([
  "find_agents", "get_agent_details", "get_delivery_status", "set_agent_profile", "dlq_status", "list_my_claims",
] as const);

/** Every registered-shell tool, runtime first. */
export const SHELL_TOOL_NAMES = Object.freeze([...RUNTIME_TOOL_NAMES, ...OPTIONAL_RUNTIME_TOOL_NAMES, ...DIAGNOSTIC_TOOL_NAMES] as const);

/** The runtimes that receive an inbox wake prompt, and how each exposes our tools. */
export type ToolNamingClient = "codex" | "pi" | "opencode";
/** OpenCode plugin tools are namespaced (opencode-plugin-factory.ts registers gptqueue_*). */
export const OPENCODE_TOOL_PREFIX = "gptqueue_";
export const toolNamePrefix = (client: ToolNamingClient): string => {
  switch (client) {
    case "codex":
    case "pi":
      return "";
    case "opencode":
      return OPENCODE_TOOL_PREFIX;
    default: {
      const exhaustive: never = client;
      throw new Error(`Unknown tool-naming client: ${String(exhaustive)}`);
    }
  }
};
