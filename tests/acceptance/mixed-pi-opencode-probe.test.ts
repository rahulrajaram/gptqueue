/**
 * Opt-in mixed-runtime serve probe.  It deliberately keeps the idle interval
 * free of OpenCode prompts, then runs a separately labelled assisted reply.
 */
import { afterAll, describe, expect, it, vi } from "vitest";
import { createHash, randomUUID } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { startPiPeer, runRoot, redisUrl as piRedisUrl, publicEvidence as piEvidence } from "./pi-support.js";
import { startOpenCodeServer, publicEvidence as openCodeEvidence, type OpenCodePeer } from "./opencode-support.js";

const enabled = process.env.GPTQUEUE_MIXED_OPENCODE_TESTS === "1";
const idleTimeoutMs = Math.min(300_000, Math.max(5_000, Number(process.env.GPTQUEUE_MIXED_IDLE_TIMEOUT_MS ?? 300_000)));
const evidenceRoot = join(runRoot, "opencode");
const jsonText = (value: unknown) => JSON.stringify(value);
type ToolRecord = { name: string; callId?: string; input?: unknown; output?: unknown };
function toolRecords(value: unknown, found: ToolRecord[] = []): ToolRecord[] {
  if (Array.isArray(value)) { value.forEach((item) => toolRecords(item, found)); return found; }
  if (!value || typeof value !== "object") return found;
  const row = value as Record<string, unknown>;
  const name = [row.tool, row.name, row.toolName].find((item): item is string => typeof item === "string");
  const state = row.state && typeof row.state === "object" ? row.state as Record<string, unknown> : row;
  const input = state.input ?? row.input ?? row.arguments ?? row.params;
  const output = state.output ?? row.output ?? state.result ?? row.result ?? row.structuredContent ?? row.details ?? row.content;
  const callId = [row.callID, row.callId, row.toolCallId, state.callID, state.callId].find((item): item is string => typeof item === "string");
  if (name && (input !== undefined || output !== undefined)) found.push({ name, callId, input, output });
  Object.values(row).forEach((item) => toolRecords(item, found));
  return found;
}
function toolExchange(value: unknown, names: readonly string[], inputParts: readonly string[], outputParts: readonly string[]) {
  const records = toolRecords(value).filter((record) => names.includes(record.name));
  return records.find((result) => result.output !== undefined && outputParts.every((part) => jsonText(result.output).includes(part)) &&
    records.some((call) => call.input !== undefined && inputParts.every((part) => jsonText(call.input).includes(part)) &&
      (!result.callId || !call.callId || result.callId === call.callId))) ?? undefined;
}
function field(value: unknown, wanted: RegExp): string | undefined {
  if (typeof value === "string") { try { return field(JSON.parse(value), wanted); } catch { return undefined; } }
  if (Array.isArray(value)) { for (const item of value) { const hit = field(item, wanted); if (hit) return hit; } return undefined; }
  if (!value || typeof value !== "object") return undefined;
  for (const [key, item] of Object.entries(value as Record<string, unknown>)) {
    if (wanted.test(key) && typeof item === "string") return item;
    const hit = field(item, wanted); if (hit) return hit;
  }
  return undefined;
}

async function inboxEvidence(redis: Redis, agent: string) {
  const rows = await redis.xrange(`gptq:inbox-trace:${agent}`, "-", "+");
  return rows.map(([, fields]) => Object.fromEntries(Array.from({ length: fields.length / 2 }, (_, i) => [fields[2 * i]!, fields[2 * i + 1]!])))
    .map((row) => piEvidence(row));
}

async function removeOwned(redis: Redis, agents: readonly (string | undefined)[]) {
  for (const agent of agents.filter((item): item is string => Boolean(item))) {
    const sessions = await redis.smembers(SESSION_KEYS.agentSessions(agent));
    await redis.hdel(SESSION_KEYS.registry, agent);
    await redis.del(
      SESSION_KEYS.agent(agent), SESSION_KEYS.queue(agent), SESSION_KEYS.mailboxMeta(agent),
      SESSION_KEYS.heartbeat(agent), SESSION_KEYS.agentSessions(agent),
      `gptq:runtime-binding:${agent}`, `gptq:activation:${agent}`, `gptq:inbox-events:${agent}`,
      `gptq:inbox-trace:${agent}`, ...sessions.flatMap((session) => [SESSION_KEYS.session(session), SESSION_KEYS.lease(session)]),
    );
  }
}

describe.skipIf(!enabled)("mixed Pi/OpenCode owned serve acceptance probe", () => {
  let redis: Redis | undefined;
  let pi: Awaited<ReturnType<typeof startPiPeer>> | undefined;
  let server: Awaited<ReturnType<typeof startOpenCodeServer>> | undefined;
  let openPeer: OpenCodePeer | undefined;
  let receipt: Record<string, unknown>;
  const ownedAgents: string[] = [];

  afterAll(async () => {
    await openPeer?.close().catch(() => undefined);
    await server?.close().catch(() => undefined);
    await pi?.close().catch(() => undefined);
    if (redis) await removeOwned(redis, ownedAgents).catch(() => undefined);
    await redis?.quit().catch(() => undefined);
  });

  it("records idle delivery separately from an assisted diagnostic reply", async () => {
    vi.setConfig({ testTimeout: 420_000, hookTimeout: 30_000 });
    const runId = randomUUID();
    const openAgent = `mixed-open-${Date.now()}-${randomUUID().slice(0, 8)}`;
    ownedAgents.push(openAgent);
    const nonce = randomUUID();
    const idempotency = `mixed-idem-${nonce}`;
    const workDir = join(evidenceRoot, runId);
    mkdirSync(workDir, { recursive: true });
    receipt = {
      schema_version: 1, route: "opencode-serve-plus-pi-rpc", database: 15,
      inference: "live-configured-model", assisted_setup: true, passed: false,
      idle_unassisted: { status: "unobserved", prompt_calls: 0, timeout_ms: idleTimeoutMs },
      assisted_diagnostic: { status: "unobserved" },
    };
    redis = new Redis(piRedisUrl, { maxRetriesPerRequest: 3 });
    try {
      await redis.ping();
      server = await startOpenCodeServer("mixed-serve");
      openPeer = await server.peer(openAgent);
      await openPeer.prompt([
        "Assisted setup only. Use the configured messaging MCP server and no other tools.",
        `Call register_agent exactly once with name '${openAgent}', role 'both', description 'mixed serve ${nonce}'.`,
        "Report the original register tool result.",
      ].join(" "));
      const openRegistry = await redis.hexists(SESSION_KEYS.registry, openAgent);
      expect(openRegistry).toBe(1);

      pi = await startPiPeer(join(workDir, "pi"));
      ownedAgents.push(pi.agent);
      const task = `The complementary record is ${nonce}. Return the record unchanged and do not invent another value.`;
      await pi.prompt([
        "Assisted sender setup. Use the available messaging tool exactly once.",
        `Send one task to '${openAgent}' with content '${task}' and idempotency key '${idempotency}'.`,
        "Do not call receive, retry, or send another message; report the original tool result.",
      ].join(" "));
      const piMessagesAfterSend = await pi.messages().catch(() => []);
      const piSend = toolExchange(piMessagesAfterSend, ["send_message", "gptqueue_send_message"], [openAgent, nonce], ["message_id"]);
      const piSendObserved = Boolean(piSend);
      const sentMessageId = field(piSend?.output, /^message[_-]?id$/i);

      // No OpenCode prompt is issued during this bounded observation.  A queue
      // drain plus history evidence is required for automatic idle activation.
      const idleDeadline = Date.now() + idleTimeoutMs;
      let openHistory: unknown = [];
      let queueLength = -1;
      while (Date.now() < idleDeadline) {
        openHistory = await openPeer.history().catch(() => []);
        queueLength = await redis.llen(SESSION_KEYS.queue(openAgent));
        if (queueLength === 0 && toolExchange(openHistory, ["receive_message", "gptqueue_receive_message"], [], [nonce])) break;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }
      const idleTrace = await inboxEvidence(redis, openAgent);
      const idleReceive = toolExchange(openHistory, ["receive_message", "gptqueue_receive_message"], [], [nonce]);
      const idleHistoryObserved = Boolean(idleReceive);
      const idleStatus = !piSendObserved ? "unobserved" : queueLength === 0 && idleHistoryObserved ? "pass" : "does_not_meet";
      receipt.idle_unassisted = {
        status: idleStatus, prompt_calls: 0, timeout_ms: idleTimeoutMs,
        sender_tool_result_observed: piSendObserved, sender_message_id: sentMessageId, queue_length: queueLength,
        automatically_consumed: idleHistoryObserved, semantic_completion: "unobserved",
        sender_history: piEvidence(piMessagesAfterSend), history: openCodeEvidence(openHistory), trace: idleTrace,
        note: "No OpenCode prompt was issued after the Pi send; history/trace were sampled until the deadline.",
      };

      // Diagnostic assistance is intentionally separate: it asks the idle
      // OpenCode session to receive and answer the pending task explicitly.
      const diagnosticPrompt = [
        "Assisted diagnostic only. Use the configured messaging MCP server.",
        `Call receive_message with timeout 1. If the message content contains '${nonce}', send exactly one reply to its sender with content '${nonce}: acknowledged' and ${sentMessageId ? `in_reply_to='${sentMessageId}'` : "the original message id as in_reply_to"}.`,
        "Report the original receive and send tool results.",
      ].join(" ");
      const beforeDiagnostic = await openPeer.history().catch(() => []);
      await openPeer.prompt(diagnosticPrompt);
      const afterDiagnostic = await openPeer.history().catch(() => []);
      const piMessages = await pi.messages().catch(() => []);
      const diagnosticEvidence = openCodeEvidence({ before: beforeDiagnostic, after: afterDiagnostic, pi: piEvidence(piMessages) });
      const diagnosticReceive = toolExchange(afterDiagnostic, ["receive_message", "gptqueue_receive_message"], [], [nonce]);
      const diagnosticSend = toolExchange(afterDiagnostic, ["send_message", "gptqueue_send_message"], [pi.agent, `${nonce}: acknowledged`, ...(sentMessageId ? [sentMessageId] : [])], ["message_id"]);
      const diagnosticObserved = Boolean(diagnosticReceive && diagnosticSend);
      receipt.assisted_diagnostic = {
        status: !diagnosticReceive ? "unobserved" : diagnosticObserved ? "pass" : "does_not_meet",
        prompt_calls: 1, history: diagnosticEvidence,
        original_tool_results_observed: diagnosticObserved,
      };
      receipt.passed = idleStatus === "pass" && diagnosticObserved;
      expect(diagnosticObserved).toBe(true);
    } catch (error) {
      receipt.error = String(error);
      throw error;
    } finally {
      receipt.owned_agents = { opencode: openAgent, pi: pi?.agent };
      receipt.opencode_server_stderr = server?.stderr();
      receipt.prompt_sha256 = createHash("sha256").update(nonce).digest("hex");
      writeFileSync(join(workDir, "mixed-probe.json"), JSON.stringify(piEvidence(receipt), null, 2) + "\n", { mode: 0o600 });
    }
  }, 420_000);
});
