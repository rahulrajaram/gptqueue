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

export function registerTools(server: McpServer, redisClient: RedisClient): void {
  server.tool(
    "register_agent",
    "Register this agent with a name, role, and description. The agent MUST ask the user to choose a name if one was not provided via GPTQ_AGENT_NAME. Can also be called again to rename midway -- pending messages are migrated.",
    registerAgentSchema.shape,
    async (params) => registerAgent(redisClient, registerAgentSchema.parse(params))
  );

  server.tool(
    "send_message",
    "Send a message to another agent's inbox queue. Retries with backoff if queue is full. Use session_id when the transport does not preserve process-local registration state.",
    sendMessageSchema.shape,
    async (params) => sendMessage(redisClient, sendMessageSchema.parse(params))
  );

  server.tool(
    "receive_message",
    "Receive the next message from this agent's inbox (blocking). Returns the message or timeout. Use session_id when the transport does not preserve process-local registration state.",
    receiveMessageSchema.shape,
    async (params) => receiveMessage(redisClient, receiveMessageSchema.parse(params))
  );

  server.tool(
    "list_agents",
    "Discover other agents. Returns each agent's name, description (what they do), role, and online/offline status. Use this to find the right agent to send_message to. Works before registration.",
    {},
    async () => listAgents(redisClient)
  );

  server.tool(
    "get_queue_status",
    "Get queue depth and metadata for an agent (or all agents). Works before registration.",
    queueStatusSchema.shape,
    async (params) => getQueueStatus(redisClient, queueStatusSchema.parse(params))
  );

  server.tool(
    "close_session",
    "Close the current session but preserve the mailbox. Messages remain queued and the agent can reconnect later. Use session_id when the transport does not preserve process-local registration state.",
    closeSessionSchema.shape,
    async (params) => closeSession(redisClient, closeSessionSchema.parse(params))
  );

  server.tool(
    "unregister_agent",
    "Unregister this agent and clean up its queue data. Use session_id when the transport does not preserve process-local registration state.",
    unregisterAgentSchema.shape,
    async (params) =>
      unregisterAgent(redisClient, unregisterAgentSchema.parse(params))
  );
}
