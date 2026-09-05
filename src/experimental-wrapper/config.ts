import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { WRAPPER_VISIBLE_TOOLS } from "./bridge.js";

export type WrappedClient = "codex" | "pi";
export type CleanupMode = "close" | "unregister";

export interface WrapperOptions {
  readonly client: WrappedClient;
  readonly agent: string;
  readonly description: string;
  readonly workspace: string;
  readonly redisUrl: string;
  readonly cleanup: CleanupMode;
  readonly model?: string;
  readonly prompt: string;
}

export interface ChildInvocation {
  readonly command: string;
  readonly args: readonly string[];
}

interface ParseState {
  readonly agent?: string;
  readonly description?: string;
  readonly workspace?: string;
  readonly redisUrl?: string;
  readonly cleanup?: CleanupMode;
  readonly model?: string;
}

export const DEFAULT_REDIS_URL = "redis://127.0.0.1:6379/14";
export const TOOL_TIMEOUT_SECONDS = 70;
const DISPOSABLE_AGENT_PREFIX = "gptqueue-experiment-";

export const usage = (): string =>
  [
    "Usage:",
    "  gptqueue-experiment <codex|pi> --agent <name> --workspace <path>",
    "    [--description <text>] [--redis-url <non-db0-url>]",
    "    [--cleanup <close|unregister>] [--model <model>] -- <prompt>",
  ].join("\n");

const isWrappedClient = (value: string | undefined): value is WrappedClient =>
  value === "codex" || value === "pi";

const readValue = (
  tokens: readonly string[],
  index: number,
  option: string
): readonly [string, number] => {
  const value = tokens[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`${option} requires a value.`);
  }
  return [value, index + 2] as const;
};

const parseOptions = (
  tokens: readonly string[],
  index: number,
  state: ParseState
): readonly [ParseState, readonly string[]] => {
  const token = tokens[index];
  if (token === undefined) {
    return [state, []] as const;
  }
  if (token === "--") {
    return [state, tokens.slice(index + 1)] as const;
  }

  switch (token) {
    case "--agent": {
      const [value, next] = readValue(tokens, index, token);
      return parseOptions(tokens, next, { ...state, agent: value });
    }
    case "--description": {
      const [value, next] = readValue(tokens, index, token);
      return parseOptions(tokens, next, { ...state, description: value });
    }
    case "--workspace": {
      const [value, next] = readValue(tokens, index, token);
      return parseOptions(tokens, next, { ...state, workspace: value });
    }
    case "--redis-url": {
      const [value, next] = readValue(tokens, index, token);
      return parseOptions(tokens, next, { ...state, redisUrl: value });
    }
    case "--cleanup": {
      const [value, next] = readValue(tokens, index, token);
      if (value !== "close" && value !== "unregister") {
        throw new Error("--cleanup must be close or unregister.");
      }
      return parseOptions(tokens, next, { ...state, cleanup: value });
    }
    case "--model": {
      const [value, next] = readValue(tokens, index, token);
      return parseOptions(tokens, next, { ...state, model: value });
    }
    default:
      throw new Error(`Unknown option: ${token}`);
  }
};

const requireSafeAgentName = (value: string | undefined): string => {
  const name = value?.trim() ?? "";
  if (
    !name ||
    [...name].some((character) => {
      const codePoint = character.codePointAt(0) ?? 0;
      return codePoint < 32 || codePoint === 127;
    })
  ) {
    throw new Error("--agent must be a non-empty name without control characters.");
  }
  return name;
};

export const redisDatabase = (redisUrl: string): number => {
  const parsed = new URL(redisUrl);
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new Error(`Redis URL must use redis:// or rediss://: ${redisUrl}`);
  }
  const raw = parsed.pathname.replace(/^\//u, "");
  if (raw !== "" && !/^(?:0|[1-9][0-9]*)$/u.test(raw)) {
    throw new Error(`Redis URL has an invalid database: ${redisUrl}`);
  }
  const database = raw === "" ? 0 : Number(raw);
  if (!Number.isSafeInteger(database) || database < 0) {
    throw new Error(`Redis URL has an invalid database: ${redisUrl}`);
  }
  return database;
};

export const requireIsolatedRedis = (redisUrl: string): string => {
  if (redisDatabase(redisUrl) === 0) {
    throw new Error(
      "The experimental wrapper refuses Redis db0; pass an isolated Redis URL such as redis://127.0.0.1:6379/14."
    );
  }
  return redisUrl;
};

export function parseWrapperArgs(argv: readonly string[]): WrapperOptions {
  const [clientToken, ...rest] = argv;
  if (!isWrappedClient(clientToken)) {
    throw new Error("First argument must be codex or pi.");
  }
  const [state, promptTokens] = parseOptions(rest, 0, {});
  const agent = requireSafeAgentName(state.agent);
  const prompt = promptTokens.join(" ").trim();
  if (!prompt) throw new Error("A prompt is required after --.");
  if (!state.workspace) throw new Error("--workspace is required.");

  const cleanup = state.cleanup ?? "close";
  if (cleanup === "unregister" && !agent.startsWith(DISPOSABLE_AGENT_PREFIX)) {
    throw new Error(
      `--cleanup unregister requires an agent name starting with ${DISPOSABLE_AGENT_PREFIX}`
    );
  }

  return Object.freeze({
    client: clientToken,
    agent,
    description:
      state.description ?? `${clientToken} launched by the experimental registered wrapper`,
    workspace: resolve(state.workspace),
    redisUrl: requireIsolatedRedis(state.redisUrl ?? DEFAULT_REDIS_URL),
    cleanup,
    ...(state.model ? { model: state.model } : {}),
    prompt,
  });
}

const quotedString = (value: string): string => JSON.stringify(value);
const quotedStringArray = (values: readonly string[]): string =>
  JSON.stringify(values);

const registeredIdentityPrompt = (agent: string): string =>
  `You are already registered in GPTQueue as "${agent}". ` +
  "Do not call register_agent and do not supply a session_id. ";

export const buildCodexInvocation = (
  options: WrapperOptions,
  bridgeUrl: string
): ChildInvocation => {
  const server = "mcp_servers.gptqueue_wrapper";
  const config = [
    `${server}.url=${quotedString(bridgeUrl)}`,
    `${server}.bearer_token_env_var=${quotedString("GPTQ_BRIDGE_TOKEN")}`,
    `${server}.required=true`,
    `${server}.startup_timeout_sec=15`,
    `${server}.tool_timeout_sec=${TOOL_TIMEOUT_SECONDS}`,
    `${server}.enabled_tools=${quotedStringArray(WRAPPER_VISIBLE_TOOLS)}`,
    `${server}.default_tools_approval_mode=${quotedString("auto")}`,
    `approval_policy=${quotedString("on-request")}`,
  ].flatMap((entry) => ["-c", entry]);
  const prompt = registeredIdentityPrompt(options.agent) + options.prompt;

  return Object.freeze({
    command: process.env.GPTQ_EXPERIMENT_CODEX_BIN || "codex",
    args: Object.freeze([
      ...config,
      "exec",
      "--ignore-user-config",
      "--strict-config",
      "--skip-git-repo-check",
      "--ephemeral",
      "--approve-for-me",
      "-C",
      options.workspace,
      ...(options.model ? ["--model", options.model] : []),
      prompt,
    ]),
  });
};

export const buildPiInvocation = (
  options: WrapperOptions,
  extensionPath: string
): ChildInvocation => {
  const prompt = registeredIdentityPrompt(options.agent) + options.prompt;
  return Object.freeze({
    command: process.env.GPTQ_EXPERIMENT_PI_BIN || "pi",
    // Keep -p first: the locally installed Nudge shim treats it as an
    // infrastructure invocation and forwards directly to the real Pi CLI.
    args: Object.freeze([
      "-p",
      "--offline",
      "--no-session",
      "--session-dir",
      resolve(dirname(extensionPath), "pi-sessions"),
      "--no-extensions",
      "--extension",
      extensionPath,
      "--no-builtin-tools",
      "--no-skills",
      "--no-prompt-templates",
      "--no-context-files",
      "--approve",
      ...(options.model ? ["--model", options.model] : []),
      prompt,
    ]),
  });
};

export const renderPiExtension = (adapterEntry: string): string =>
  [
    `import { createMcpAdapter } from ${JSON.stringify(adapterEntry)};`,
    `import { restrictPiTools, requirePiTools } from ${JSON.stringify(fileURLToPath(new URL("./pi-tools.js", import.meta.url)))};`,
    "",
    "const requiredEnv = (name) => {",
    "  const value = process.env[name];",
    "  if (!value) throw new Error(`Missing required environment variable: ${name}`);",
    "  return value;",
    "};",
    "",
    '// Redirect adapter metadata/cache writes into this one-run workspace after Pi has loaded its own auth and model configuration.',
    'process.env.PI_CODING_AGENT_DIR = requiredEnv("GPTQ_PI_ADAPTER_STATE_DIR");',
    "",
    `const allowed = ${JSON.stringify(WRAPPER_VISIBLE_TOOLS)};`,
    "const adapter = createMcpAdapter({",
    "  config: {",
    "    settings: {",
    "      disableProxyTool: true,",
    "      scriptMode: false,",
    "      freezeDirectTools: true,",
    "      sampling: false,",
    "      elicitation: false",
    "    },",
    "    mcpServers: {",
    "      gptqueue_wrapper: {",
    '        url: requiredEnv("GPTQ_BRIDGE_URL"),',
    '        auth: "bearer",',
    '        bearerTokenEnv: "GPTQ_BRIDGE_TOKEN",',
    '        lifecycle: "eager",',
    `        requestTimeoutMs: ${TOOL_TIMEOUT_SECONDS * 1000},`,
    "        directTools: true,",
    '        toolPrefix: "none",',
    `        includeTools: ${JSON.stringify(WRAPPER_VISIBLE_TOOLS)}`,
    "      }",
    "    }",
    "  },",
    "});",
    "",
    "export default function messagingOnlyPi(pi) {",
    "  adapter(restrictPiTools(pi, allowed));",
    '  pi.on("before_agent_start", async () => {',
    "    try {",
    "      const active = await requirePiTools(pi, allowed);",
    '      console.error("[gptqueue-experiment] Pi active tools: " + JSON.stringify(active));',
    "    } catch (error) {",
    '      console.error("[gptqueue-experiment] Pi tool isolation failed: " + error.message);',
    "      // Pi reports extension exceptions and may continue; terminate this child",
    "      // so the parent wrapper cleans up instead of allowing inference.",
    "      process.exit(1);",
    "    }",
    "  });",
    "}",
    "",
  ].join("\n");
