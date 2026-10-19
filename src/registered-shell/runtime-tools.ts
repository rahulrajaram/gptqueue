import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { RedisClient } from "../mcp-server/redis-client.js";
import { claimTasks, claimTasksSchema } from "../mcp-server/tools/claim-tasks.js";
import { acknowledgeTasks, acknowledgeTasksSchema } from "../mcp-server/tools/acknowledge-tasks.js";
import { acknowledgeMessages, acknowledgeMessagesSchema } from "../mcp-server/tools/acknowledge-messages.js";
import { renewClaim, renewClaimSchema } from "../mcp-server/tools/renew-claim.js";
import { dlqRequeue, dlqRequeueSchema } from "../mcp-server/tools/dlq-requeue.js";
import { stableToolError, toolResult } from "../mcp-server/tool-result.js";
import { InboxEvents } from "../core/inbox-events.js";
import { traceAcknowledged, traceClaimed, traceMessagesAcknowledged } from "../mcp-server/tool-trace.js";
import { runtimeBindingSchema, type RuntimeBinding } from "./runtime.js";
import { registerDiagnosticTools } from "./diagnostic-tools.js";

/** Machine-readable MCP annotations mirroring the prose [safety: ...] prefixes. */
const READ_ONLY = { readOnlyHint: true } as const;
const WRITABLE = { readOnlyHint: false } as const;

export { RUNTIME_TOOL_NAMES } from "./tool-names.js";

export interface RuntimeTools {
  bind(binding: RuntimeBinding): Promise<Record<string, unknown>>;
  status(): Record<string, unknown>;
}

const safe = async (work: () => Promise<ReturnType<typeof toolResult>>) => {
  try { return await work(); } catch (error) { return stableToolError(error); }
};

/** Bound variants reuse the existing claim protocol and never expose session credentials. */
export const registerRuntimeTools = (server: McpServer, client: RedisClient, runtime: RuntimeTools): void => {
  const events = new InboxEvents(client.adapterConnection);
  const claimSchema = claimTasksSchema.omit({ session_id: true }).strict();
  server.tool("claim_tasks", "Claim a recoverable batch from your bound inbox; acknowledge only after processing and replying.",
    claimSchema.shape, WRITABLE, async (params) => safe(async () => {
      const result = await claimTasks(client, claimSchema.parse(params));
      const payload = result.structuredContent as Record<string, unknown>;
      const claim = payload.claim as Record<string, unknown> | null | undefined;
      if (!claim) return result;
      const { session_id: _privateSession, ...publicClaim } = claim;
      await traceClaimed(events, client.requireRegistered(), claim);
      return toolResult({ ...payload, claim: publicClaim });
    }));
  const ackSchema = acknowledgeTasksSchema.omit({ session_id: true }).strict();
  server.tool("acknowledge_tasks", "Acknowledge your completed claim after sending any required correlated reply.",
    ackSchema.shape, WRITABLE, async (params) => safe(async () => {
      const result = await acknowledgeTasks(client, ackSchema.parse(params));
      if (!result.isError) await traceAcknowledged(events, client.requireRegistered(), params.claim_id);
      return result;
    }));
  const renewSchema = renewClaimSchema.omit({ session_id: true }).strict();
  server.tool("renew_claim", "Extend your claim while authorized processing remains underway.", renewSchema.shape,
    WRITABLE, async (params) => safe(() => renewClaim(client, renewSchema.parse(params))));
  server.tool("bind_runtime", "Bind the exact runtime-supplied session identity to this connection. Never infer it from a directory or agent UUID.",
    runtimeBindingSchema.shape, WRITABLE, async (params) => safe(async () => toolResult(await runtime.bind(runtimeBindingSchema.parse(params)))));
  server.tool("get_runtime_status", "Report this connection's exact runtime binding and automatic inbox activation readiness.",
    z.object({}).shape, READ_ONLY, async () => toolResult(runtime.status()));
  const ackMessagesSchema = acknowledgeMessagesSchema.omit({ session_id: true }).strict();
  // registerTool with the whole strict schema so unknown fields are refused, not stripped.
  server.registerTool("acknowledge_messages", {
    description: "Acknowledge selected messages (message_ids) of your claim after replying to them; the rest stay claimed. Use acknowledge_tasks for the whole claim.",
    inputSchema: ackMessagesSchema, annotations: WRITABLE,
  }, async (params) => safe(async () => {
    const parsed = ackMessagesSchema.parse(params);
    const result = await acknowledgeMessages(client, parsed);
    if (!result.isError) await traceMessagesAcknowledged(events, client.requireRegistered(), parsed.claim_id, parsed.message_ids);
    return result;
  }));
  // Caller-bound like claim_tasks: no session_id or target mailbox is accepted.
  const requeueSchema = dlqRequeueSchema.omit({ session_id: true }).strict();
  server.registerTool("dlq_requeue", {
    description: "[safety: writable] Move one of your own dead-lettered messages (message_id from dlq_status) back to your bound inbox tail with a fresh recovery budget. Not found is a structured dlq_entry_not_found error.",
    inputSchema: requeueSchema, annotations: WRITABLE,
  }, async (params) => safe(() => dlqRequeue(client, requeueSchema.parse(params))));
  registerDiagnosticTools(server, client, runtime);
};
