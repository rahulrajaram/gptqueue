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
import { actorRegisterSchema, actorRegister } from "../mcp-server/tools/actor-register.js";
import { actorStatusSchema, actorStatus } from "../mcp-server/tools/actor-status.js";
import { claimTasksSchema, claimTasks } from "../mcp-server/tools/claim-tasks.js";
import {
  acknowledgeTasksSchema,
  acknowledgeTasks,
} from "../mcp-server/tools/acknowledge-tasks.js";
import { renewClaimSchema, renewClaim } from "../mcp-server/tools/renew-claim.js";
import { dlqStatusSchema, dlqStatus } from "../mcp-server/tools/dlq-status.js";
import { dlqRequeueSchema, dlqRequeue } from "../mcp-server/tools/dlq-requeue.js";
import { stableToolError } from "../mcp-server/tool-result.js";

/**
 * Machine-readable MCP annotations mirroring the prose [safety: ...] prefixes.
 * Clients such as Codex classify tool calls for approval from these hints, not
 * from prose; keep each mapping in sync with the handler's actual state effects.
 */
const READ_ONLY = { readOnlyHint: true } as const;
const WRITABLE = { readOnlyHint: false } as const;
const DESTRUCTIVE = { readOnlyHint: false, destructiveHint: true } as const;
const IDEMPOTENT_WRITE = { readOnlyHint: false, idempotentHint: true } as const;

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
    WRITABLE,
    async (params) => safeToolCall(() => registerAgent(redisClient, registerAgentSchema.parse(params)))
  );

  server.tool(
    "send_message",
    "[safety: writable] Send a message to another GPTQueue inbox. Supply idempotency_key for safely retryable delivery.",
    sendMessageSchema.shape,
    WRITABLE,
    async (params) => safeToolCall(() => sendMessage(redisClient, sendMessageSchema.parse(params)))
  );

  server.tool(
    "receive_message",
    "[safety: writable] Receive and consume the next message from this agent's GPTQueue inbox. Returns the message or timeout.",
    receiveMessageSchema.shape,
    WRITABLE,
    // The request signal ends the receive's blocking pop when the MCP client
    // cancels the call or its session closes.
    async (params, extra) =>
      safeToolCall(() => receiveMessage(redisClient, receiveMessageSchema.parse(params), extra.signal))
  );

  server.tool(
    "list_agents",
    "[safety: readonly] List agents with readable labels, exact messaging names, public UUIDs, working directories, clients, registration times, process IDs, and presence. Send messages to the full name; labels are for display. Works before registration.",
    {},
    READ_ONLY,
    async () => safeToolCall(() => listAgents(redisClient))
  );

  server.tool(
    "get_queue_status",
    "[safety: readonly] Get queue depth and metadata for an agent or all agents. Works before registration.",
    queueStatusSchema.shape,
    READ_ONLY,
    async (params) => safeToolCall(() => getQueueStatus(redisClient, queueStatusSchema.parse(params)))
  );

  server.tool(
    "close_session",
    "[safety: writable] Close the current GPTQueue session while preserving its mailbox for reconnection.",
    closeSessionSchema.shape,
    IDEMPOTENT_WRITE,
    async (params) => safeToolCall(() => closeSession(redisClient, closeSessionSchema.parse(params)))
  );

  server.tool(
    "unregister_agent",
    "[safety: destructive] Unregister this GPTQueue agent and permanently delete its queue data.",
    unregisterAgentSchema.shape,
    DESTRUCTIVE,
    async (params) =>
      safeToolCall(() => unregisterAgent(redisClient, unregisterAgentSchema.parse(params)))
  );

  server.tool(
    "custody_claim",
    "[safety: writable] Claim custody of a worktree for this session. Handles initial claim, graceful re-claim, and successor takeover (forfeited worktrees require an inventory).",
    custodyClaimSchema.shape,
    WRITABLE,
    async (params) => safeToolCall(() => custodyClaim(redisClient, custodyClaimSchema.parse(params)))
  );

  server.tool(
    "custody_release",
    "[safety: writable] Release a held worktree, recording a structured handoff for the next custodian. Only the current custodian session may release.",
    custodyReleaseSchema.shape,
    WRITABLE,
    async (params) => safeToolCall(() => custodyRelease(redisClient, custodyReleaseSchema.parse(params)))
  );

  server.tool(
    "custody_status",
    "[safety: readonly] Inspect a worktree's custody record, or list every stored record. Expired leases are forfeited lazily. Works before registration.",
    custodyStatusSchema.shape,
    READ_ONLY,
    async (params) => safeToolCall(() => custodyStatus(redisClient, custodyStatusSchema.parse(params)))
  );

  server.tool(
    "actor_register",
    "[safety: writable] Register a durable actor profile and launch contract in the shared actor directory, owned by the calling session. Purely additive alongside register_agent.",
    actorRegisterSchema.shape,
    WRITABLE,
    async (params) => safeToolCall(() => actorRegister(redisClient, actorRegisterSchema.parse(params)))
  );

  server.tool(
    "actor_status",
    "[safety: readonly] Classify a durable actor's runtime presence (active/idle/starting/offline_* states) from its launch contract, live sessions, and any outstanding wake lease. Works before registration.",
    actorStatusSchema.shape,
    READ_ONLY,
    async (params) => safeToolCall(() => actorStatus(redisClient, actorStatusSchema.parse(params)))
  );

  server.tool(
    "claim_tasks",
    "[safety: writable] Atomically claim up to max_batch messages from your own durable inbox as an at-least-once delivery batch for the calling session. Returns the claim (claim_id, tasks, expires_at) or an explicit empty-batch result when nothing is pending.",
    claimTasksSchema.shape,
    WRITABLE,
    async (params) => safeToolCall(() => claimTasks(redisClient, claimTasksSchema.parse(params)))
  );

  server.tool(
    "acknowledge_tasks",
    "[safety: writable] Acknowledge a claim_id returned by claim_tasks, confirming delivery of that batch. Only the claiming session may acknowledge its own claim; acknowledged tasks are not re-delivered.",
    acknowledgeTasksSchema.shape,
    WRITABLE,
    async (params) => safeToolCall(() => acknowledgeTasks(redisClient, acknowledgeTasksSchema.parse(params)))
  );

  server.tool(
    "renew_claim",
    "[safety: writable] Renew an outstanding claim_id returned by claim_tasks, extending its expiry by ttl_seconds (default 300, range 1-3600). Only the owning session may renew; the extension is capped by the claim's provisional lifetime budget so an endlessly-renewing runtime cannot hold a batch forever.",
    renewClaimSchema.shape,
    WRITABLE,
    async (params) => safeToolCall(() => renewClaim(redisClient, renewClaimSchema.parse(params)))
  );

  server.tool(
    "dlq_status",
    "[safety: readonly] List the calling agent's dead-letter queue (DLQ) entries, newest first. A message is dead-lettered after repeated unacknowledged recovery cycles (a provisional policy).",
    dlqStatusSchema.shape,
    READ_ONLY,
    async (params) => safeToolCall(() => dlqStatus(redisClient, dlqStatusSchema.parse(params)))
  );

  server.tool(
    "dlq_requeue",
    "[safety: writable] Move one dead-lettered message (by message_id from dlq_status) from the calling agent's DLQ back to the tail of its own inbox with a fresh recovery budget.",
    dlqRequeueSchema.shape,
    WRITABLE,
    async (params) => safeToolCall(() => dlqRequeue(redisClient, dlqRequeueSchema.parse(params)))
  );
}
