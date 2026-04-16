#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { RedisClient } from "./redis-client.js";
import { registerTools } from "../transports/setup-tools.js";

// Name comes from CLI arg or env var. If neither, starts unregistered.
const initialName = process.argv[2] || process.env.GPTQ_AGENT_NAME || null;

const redisClient = new RedisClient(null);

const server = new McpServer({
  name: "gptqueue",
  version: "1.0.0",
});

registerTools(server, redisClient);

// Auto-register if name provided via CLI arg or env var (backward compat)
if (initialName) {
  await redisClient.register("both", initialName);
}

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
