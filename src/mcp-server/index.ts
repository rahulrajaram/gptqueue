#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { RedisClient } from "./redis-client.js";
import { GPTQUEUE_INSTRUCTIONS, registerTools } from "../transports/setup-tools.js";
import { VERSION } from "../version.js";
import { holdToolCallsUntil, registerClaudeAware, registerUntilFinal, startClaudeSessionRecord } from "../claude/mcp-record.js";

// Name comes from CLI arg or env var. If neither, starts unregistered.
const initialName = process.argv[2] || process.env.GPTQ_AGENT_NAME || null;

const redisClient = new RedisClient(null);

const server = new McpServer({
  name: "gptqueue",
  version: VERSION,
}, { instructions: GPTQUEUE_INSTRUCTIONS });

registerTools(server, redisClient);

// Under Claude Code, publish the identity this server holds so the session's
// hooks and inbox watcher use it instead of re-deriving a name (ID-2).
const claudeRecord = startClaudeSessionRecord(redisClient);

// Auto-register if name provided via CLI arg or env var (backward compat).
// Under Claude, registration may wait up to a lease TTL for an old-format
// holder (ID-3), longer than Claude should wait for MCP startup, so it runs in
// the background: initialize is answered at once and tool calls wait for it.
// A transient failure (Redis unavailable) is retried in the background with
// capped backoff, so it never becomes a permanent refusal (BC-F1).
let lastRegistrationError: unknown = null;
const registration: Promise<unknown> = !initialName
  ? Promise.resolve()
  : claudeRecord
    ? registerUntilFinal(
      () => registerClaudeAware(redisClient, initialName, process.env, undefined, (origin) => claudeRecord.setOrigin(origin)),
      (error, delayMs) => {
        lastRegistrationError = error;
        process.stderr.write(`${JSON.stringify({ event: "startup_registration_retry", message: (error as Error).message, delay_ms: delayMs })}\n`);
      })
    : registerClaudeAware(redisClient, initialName);
if (!claudeRecord) await registration;
registration.catch((error: unknown) => {
  process.stderr.write(`${JSON.stringify({ event: "startup_registration_failed", code: (error as { code?: unknown }).code ?? null, message: (error as Error).message })}\n`);
});

// Graceful shutdown. Besides signals, exit when the client goes away (stdin
// ends or the MCP connection closes): an orphaned server would otherwise keep
// refreshing its lease and pin the agent online forever.
let shuttingDown = false;
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  claudeRecord?.remove();
  // Close (not delete) the session so a restarted or resumed client can take
  // the name back at once instead of waiting for the lease TTL. Bounded: a
  // Redis outage must not block exit.
  if (redisClient.registered) {
    await Promise.race([
      redisClient.closeCurrentSession().catch(() => undefined),
      new Promise<void>((resolve) => setTimeout(resolve, 1_000).unref()),
    ]);
  }
  await redisClient.shutdown();
  process.exit(0);
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
process.stdin.on("end", shutdown);
process.stdin.on("close", shutdown);

// Start
const transport = new StdioServerTransport();
await server.connect(transport);
if (claudeRecord) holdToolCallsUntil(transport, registration, undefined, () => lastRegistrationError);
server.server.onclose = () => void shutdown();
