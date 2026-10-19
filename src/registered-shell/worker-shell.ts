import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import { z } from "zod";
import { WRAPPER_VISIBLE_TOOLS } from "../experimental-wrapper/bridge.js";
import { toolResult } from "../mcp-server/tool-result.js";
import { VERSION } from "../version.js";
import { SHELL_TOOL_NAMES } from "./tool-names.js";

/** Every tool a normal registered shell lists; each needs a mailbox. */
export const WORKER_SHELL_TOOL_NAMES = Object.freeze([...WRAPPER_VISIBLE_TOOLS, ...SHELL_TOOL_NAMES] as const);

export const HEADLESS_WORKER_MESSAGE =
  "HEADLESS_WORKER: this session runs with GPTQUEUE_WORKER=1 and has no mailbox; unset it (or set 0) to use gptqueue";

export const WORKER_STARTUP_NOTICE =
  "gptqueue-session: headless worker mode (GPTQUEUE_WORKER=1); no mailbox registered, mailbox tools refuse with HEADLESS_WORKER\n";

export const headlessWorkerResult = () => toolResult({
  status: "error",
  error: { code: "HEADLESS_WORKER", message: HEADLESS_WORKER_MESSAGE, retryable: false },
}, true);

export interface WorkerShellHandle {
  readonly closed: Promise<void>;
  readonly close: () => Promise<void>;
}

/**
 * Worker-mode sidecar (GPTQUEUE_WORKER=1). It never opens Redis, registers,
 * binds a runtime or writes presence; it still serves MCP, because a client
 * may mark this server required and must start normally. Every tool refuses.
 */
export const createWorkerMcpServer = (): McpServer => {
  const server = new McpServer({ name: "gptqueue-registered-wrapper", version: `${VERSION}-worker` }, {
    instructions: "This is a headless gptqueue worker session (GPTQUEUE_WORKER=1). It has no mailbox: every gptqueue tool refuses with HEADLESS_WORKER. Do not try to register.",
  });
  const anyArgs = z.object({}).passthrough();
  for (const name of WORKER_SHELL_TOOL_NAMES) {
    server.registerTool(name, {
      description: "[safety: readonly] Unavailable: headless worker session (GPTQUEUE_WORKER=1) has no mailbox; always returns HEADLESS_WORKER.",
      inputSchema: anyArgs, annotations: { readOnlyHint: true },
    }, async () => headlessWorkerResult());
  }
  return server;
};

export const startWorkerShell = async (
  callerSignal: AbortSignal = new AbortController().signal,
  transportFactory: () => Transport = () => new StdioServerTransport(),
  log: (line: string) => void = (line) => { process.stderr.write(line); }
): Promise<WorkerShellHandle> => {
  callerSignal.throwIfAborted();
  log(WORKER_STARTUP_NOTICE);
  const server = createWorkerMcpServer();
  let resolveClosed!: () => void;
  const closed = new Promise<void>((resolve) => { resolveClosed = resolve; });
  let closing: Promise<void> | undefined;
  const close = (): Promise<void> => {
    closing ??= Promise.resolve().then(async () => {
      callerSignal.removeEventListener("abort", onAbort);
      try { await server.close(); } finally { resolveClosed(); }
    });
    return closing;
  };
  const onAbort = () => { void close().catch(() => undefined); };
  callerSignal.addEventListener("abort", onAbort, { once: true });
  server.server.onclose = () => { void close().catch(() => undefined); };
  await server.connect(transportFactory());
  return Object.freeze({ closed, close });
};
