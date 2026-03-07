#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { RedisClient } from "./redis-client.js";
import { registerAgentSchema, registerAgent } from "./tools/register-agent.js";
import { sendMessageSchema, sendMessage } from "./tools/send-message.js";
import {
  receiveMessageSchema,
  receiveMessage,
} from "./tools/receive-message.js";
import { listAgents } from "./tools/list-agents.js";
import { queueStatusSchema, getQueueStatus } from "./tools/queue-status.js";
import { unregisterAgent } from "./tools/unregister-agent.js";

// Name comes from CLI arg or env var. If neither, starts unregistered.
const initialName = process.argv[2] || process.env.GPTQ_AGENT_NAME || null;

const redisClient = new RedisClient(initialName);

const server = new McpServer({
  name: "gptqueue",
  version: "1.0.0",
});

server.tool(
  "register_agent",
  "Register this agent with a name, role, and description. The agent MUST ask the user to choose a name if one was not provided via GPTQ_AGENT_NAME. Can also be called again to rename midway -- pending messages are migrated.",
  registerAgentSchema.shape,
  async (params) => registerAgent(redisClient, registerAgentSchema.parse(params))
);

server.tool(
  "send_message",
  "Send a message to another agent's inbox queue. Retries with backoff if queue is full. Requires register_agent first.",
  sendMessageSchema.shape,
  async (params) => sendMessage(redisClient, sendMessageSchema.parse(params))
);

server.tool(
  "receive_message",
  "Receive the next message from this agent's inbox (blocking). Returns the message or timeout. Requires register_agent first.",
  receiveMessageSchema.shape,
  async (params) =>
    receiveMessage(redisClient, receiveMessageSchema.parse(params))
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
  async (params) =>
    getQueueStatus(redisClient, queueStatusSchema.parse(params))
);

server.tool(
  "unregister_agent",
  "Unregister this agent and clean up its queue data",
  {},
  async () => unregisterAgent(redisClient)
);

// Graceful shutdown
async function shutdown() {
  await redisClient.shutdown();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Start
const transport = new StdioServerTransport();
await server.connect(transport);
