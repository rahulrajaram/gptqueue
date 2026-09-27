import { z } from "zod";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { RedisClient } from "../mcp-server/redis-client.js";
import { toolResult, stableToolError } from "../mcp-server/tool-result.js";
import { CodexSocketClient } from "./codex-socket.js";
import type { RuntimeTools } from "./runtime-tools.js";
import { AgentDiagnostics } from "../core/agent-diagnostics.js";
import { SESSION_KEYS } from "../core/keys.js";

/** Machine-readable MCP annotations mirroring the prose [safety: ...] prefixes. */
const READ_ONLY = { readOnlyHint: true } as const;
const WRITABLE = { readOnlyHint: false } as const;

const agentProfileSchema = z.object({
  label: z.string().trim().min(1).max(120),
  purpose: z.string().trim().min(1).max(500),
  kind: z.enum(["controller", "worker", "interactive", "unknown"]),
}).strict();
export const findAgentsSchema = z.object({ query: z.string().max(200).optional(), client: z.enum(["codex", "pi"]).optional(),
  working_directory: z.string().max(4096).optional(), kind: agentProfileSchema.shape.kind.optional(),
  online: z.boolean().optional(), activation_ready: z.boolean().optional(), limit: z.number().int().min(1).max(100).default(50),
}).strict();
export const agentDetailsSchema = z.object({ agent: z.string().min(1).max(500).optional(), probe: z.boolean().default(false) }).strict();
export const deliveryStatusSchema = agentDetailsSchema.extend({ message_id: z.string().min(1).max(200) }).strict();

/** Additive tools: original messaging/discovery schemas retain their contracts. */
export const registerDiagnosticTools = (server: McpServer, client: RedisClient, runtime: RuntimeTools): void => {
  const diagnostics = new AgentDiagnostics(client.adapterConnection);
  const safe = async (work: () => Promise<unknown>) => {
    try { return toolResult(await work() as Record<string, unknown>); }
    catch (error) { return stableToolError(error); }
  };
  server.tool("find_agents", "[safety: readonly] Find exact agent candidates by declared purpose and identity. Ambiguous matches are never routed automatically; online does not imply activation readiness.",
    findAgentsSchema.shape, READ_ONLY, params => safe(() => diagnostics.find(findAgentsSchema.parse(params))));
  server.tool("get_agent_details", "[safety: readonly] Inspect an exact mailbox, runtime binding, published capabilities, declared role and activation readiness. Omit agent for this connection. No message content or credentials.",
    agentDetailsSchema.shape, READ_ONLY, params => safe(async () => {
      const agent = params.agent ?? client.requireRegistered();
      const observed = await diagnostics.details(agent);
      if (agent === client.requireRegistered()) {
        const local = runtime.status();
        return { ...observed, activation_ready: local.activation_ready === true,
          readiness: local.activation_ready === true ? "ready" : observed.readiness, readiness_evidence: "local_controller" };
      }
      if (!params.probe || observed.runtime_binding?.client !== "codex")
        return { ...observed, readiness_evidence: "lease_observation_only" };
      const rpc = new CodexSocketClient();
      try {
        const result = await rpc.request("mcpServer/tool/call", { threadId: observed.runtime_binding.runtime_id,
          server: "gptqueue-shared", tool: "get_runtime_status", arguments: {} }, AbortSignal.timeout(10000));
        const status = result.structuredContent as Record<string, unknown> | undefined;
        const binding = status?.runtime as Record<string, unknown> | undefined;
        const ready = !result.isError && status?.agent === agent && status?.activation_ready === true &&
          binding?.runtime_id === observed.runtime_binding.runtime_id && binding?.epoch === observed.runtime_binding.epoch;
        return { ...observed, activation_ready: ready, readiness: ready ? "ready" : "unbound", readiness_evidence: "exact_native_probe" };
      } catch { return { ...observed, activation_ready: null, readiness_evidence: "probe_unavailable" }; }
      finally { await rpc.close(); }
    }));
  server.tool("get_delivery_status", "[safety: readonly] Inspect one message's queue, claim, acknowledgement or dead-letter evidence without consuming it. Missing retained evidence means unknown, not delivered.",
    deliveryStatusSchema.shape, READ_ONLY, params => safe(() => diagnostics.delivery(params.agent ?? client.requireRegistered(), params.message_id)));
  server.tool("set_agent_profile", "[safety: writable] Declare this connection's readable label, purpose and kind. A declaration is a discovery hint, never proof of controller authority or permission to take over another mailbox.",
    agentProfileSchema.shape, WRITABLE, params => safe(async () => {
      const profile = Object.freeze({ ...agentProfileSchema.parse(params), declaration_source: "self", updated_at: new Date().toISOString() });
      await client.adapterConnection.set(SESSION_KEYS.agentProfile(client.requireRegistered()), JSON.stringify(profile));
      return { status: "ok", agent: client.requireRegistered(), profile };
    }));
};
