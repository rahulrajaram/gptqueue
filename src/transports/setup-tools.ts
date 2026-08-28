/**
 * Shared tool registration for all transports.
 *
 * Both stdio and HTTP entrypoints call this to register
 * the same set of MCP tools on a server instance.
 */

import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { RedisClient } from "../mcp-server/redis-client.js";
import { registerAgentSchema, registerAgent } from "../mcp-server/tools/register-agent.js";
import { sendMessageSchema, sendMessage } from "../mcp-server/tools/send-message.js";
import { receiveMessageSchema, receiveMessage } from "../mcp-server/tools/receive-message.js";
import { listAgents } from "../mcp-server/tools/list-agents.js";
import { queueStatusSchema, getQueueStatus } from "../mcp-server/tools/queue-status.js";
import {
  unregisterAgent,
  unregisterAgentSchema,
} from "../mcp-server/tools/unregister-agent.js";
import {
  closeSession,
  closeSessionSchema,
} from "../mcp-server/tools/close-session.js";
import { custodyClaimSchema, custodyClaim } from "../mcp-server/tools/custody-claim.js";
import {
  custodyReleaseSchema,
  custodyRelease,
} from "../mcp-server/tools/custody-release.js";
import { custodyStatusSchema, custodyStatus } from "../mcp-server/tools/custody-status.js";
import { stableToolError } from "../mcp-server/tool-result.js";

export const GPTQUEUE_INSTRUCTIONS =
  "GPTQueue is an external/shared Redis-backed coordination plane. Choose it instead of native in-session collaboration; do not operate both for the same workflow. Register before session-scoped calls, retain the returned session_id for stateless reconnection, use idempotency_key when retrying sends, and prefer close_session over destructive unregister_agent when preserving the mailbox matters.";

async function safeToolCall<T>(call: () => Promise<T>) {
  try {
    return await call();
  } catch (error) {
    return stableToolError(error);
  }
}

export function registerTools(server: McpServer, redisClient: RedisClient): void {
  server.tool(
    "register_agent",
    "[safety: writable] Register this agent and create a new durable session. Re-registration is not idempotent: it creates a new session; renaming also migrates pending messages.",
    registerAgentSchema.shape,
    async (params) => safeToolCall(() => registerAgent(redisClient, registerAgentSchema.parse(params)))
  );

  server.tool(
    "send_message",
    "[safety: writable] Send a message to another GPTQueue inbox. Supply idempotency_key for safely retryable delivery.",
    sendMessageSchema.shape,
    async (params) => safeToolCall(() => sendMessage(redisClient, sendMessageSchema.parse(params)))
  );

  server.tool(
    "receive_message",
    "[safety: writable] Receive and consume the next message from this agent's GPTQueue inbox. Returns the message or timeout.",
    receiveMessageSchema.shape,
    async (params) => safeToolCall(() => receiveMessage(redisClient, receiveMessageSchema.parse(params)))
  );

  server.tool(
    "list_agents",
    "[safety: readonly] List registered GPTQueue agents and their presence. Works before registration.",
    {},
    async () => safeToolCall(() => listAgents(redisClient))
  );

  server.tool(
    "get_queue_status",
    "[safety: readonly] Get queue depth and metadata for an agent or all agents. Works before registration.",
    queueStatusSchema.shape,
    async (params) => safeToolCall(() => getQueueStatus(redisClient, queueStatusSchema.parse(params)))
  );

  server.tool(
    "close_session",
    "[safety: writable] Close the current GPTQueue session while preserving its mailbox for reconnection.",
    closeSessionSchema.shape,
    async (params) => safeToolCall(() => closeSession(redisClient, closeSessionSchema.parse(params)))
  );

  server.tool(
    "unregister_agent",
    "[safety: destructive] Unregister this GPTQueue agent and permanently delete its queue data.",
    unregisterAgentSchema.shape,
    async (params) =>
      safeToolCall(() => unregisterAgent(redisClient, unregisterAgentSchema.parse(params)))
  );

  server.tool(
    "custody_claim",
    "[safety: writable] Claim custody of a worktree for this session. Handles initial claim, graceful re-claim, and successor takeover (forfeited worktrees require an inventory).",
    custodyClaimSchema.shape,
    async (params) => safeToolCall(() => custodyClaim(redisClient, custodyClaimSchema.parse(params)))
  );

  server.tool(
    "custody_release",
    "[safety: writable] Release a held worktree, recording a structured handoff for the next custodian. Only the current custodian session may release.",
    custodyReleaseSchema.shape,
    async (params) => safeToolCall(() => custodyRelease(redisClient, custodyReleaseSchema.parse(params)))
  );

  server.tool(
    "custody_status",
    "[safety: readonly] Inspect a worktree's custody record, or list every stored record. Expired leases are forfeited lazily. Works before registration.",
    custodyStatusSchema.shape,
    async (params) => safeToolCall(() => custodyStatus(redisClient, custodyStatusSchema.parse(params)))
  );
}
