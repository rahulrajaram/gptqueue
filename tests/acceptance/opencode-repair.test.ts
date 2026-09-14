/**
 * Opt-in native OpenCode identity-repair proof.
 *
 * The proof is intentionally skipped unless the parent supplies an owned
 * Redis URL. It keeps the OpenCode server alive while the parent and native
 * Task child are inspected, so a short-lived `opencode run` cannot create a
 * false lifecycle pass.
 */
import { afterAll, describe, expect, it, vi } from "vitest";
import { randomInt, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { SESSION_KEYS } from "../../src/core/keys.js";
import { checkExchangeEvidence, type ExchangeEvidence, type MessageEvidence } from "./oracle.js";
import {
  cleanupOwnedAgent,
  hashFile,
  hashText,
  openOwnedRedis,
  repairConfig,
  repairEnabled,
  repairPluginPath,
  startRepairOpenCodeServer,
  streamRows,
  writeRepairReceipt,
  type OwnedRedis,
  type RepairOpenCodeServer,
  type RepairSession,
} from "./opencode-repair-support.js";
import { childReadinessEvidence } from "./opencode-qualification-oracles.js";

type JsonObject = Record<string, unknown>;
type ToolTrace = Readonly<{ name: string; status: string; error: boolean; input?: unknown; output?: unknown }>;

const waitFor = async <T>(read: () => Promise<T>, accepted: (value: T) => boolean, timeoutMs = 120_000): Promise<T> => {
  const deadline = Date.now() + timeoutMs;
  let value = await read();
  while (!accepted(value) && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 500));
    value = await read();
  }
  if (!accepted(value)) throw new Error(`OpenCode repair observation timed out after ${timeoutMs}ms`);
  return value;
};

const decode = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value); } catch { return value; }
};

const asObject = (value: unknown): JsonObject | undefined => {
  const decoded = decode(value);
  return decoded && typeof decoded === "object" && !Array.isArray(decoded) ? decoded as JsonObject : undefined;
};

const asArray = (value: unknown): readonly unknown[] => {
  const decoded = decode(value);
  if (Array.isArray(decoded)) return decoded;
  const object = asObject(decoded);
  for (const key of ["sessions", "data", "messages", "items"]) {
    if (object?.[key] !== undefined) {
      const nested = decode(object[key]);
      if (Array.isArray(nested)) return nested;
    }
  }
  return [];
};

const toolTraces = (value: unknown, found: ToolTrace[] = []): readonly ToolTrace[] => {
  const decoded = decode(value);
  if (Array.isArray(decoded)) {
    decoded.forEach((item) => toolTraces(item, found));
    return found;
  }
  const object = asObject(decoded);
  if (!object) return found;
  const state = asObject(object.state);
  if (typeof object.tool === "string" && state) {
    found.push({
      name: object.tool,
      status: typeof state.status === "string" ? state.status : "unknown",
      error: state.status === "error" || state.error !== undefined,
      input: state.input,
      output: state.output,
    });
  }
  Object.values(object).forEach((item) => toolTraces(item, found));
  return found;
};

const records = (value: unknown, found: JsonObject[] = []): readonly JsonObject[] => {
  const decoded = decode(value);
  if (Array.isArray(decoded)) {
    decoded.forEach((item) => records(item, found));
    return found;
  }
  const object = asObject(decoded);
  if (!object) return found;
  const payload = asObject(object.payload);
  if (typeof object.id === "string" && typeof object.from === "string" && typeof object.to === "string" && payload) {
    found.push(object);
  }
  Object.values(object).forEach((item) => records(item, found));
  return found;
};

const namedTool = (traces: readonly ToolTrace[], suffix: string): readonly ToolTrace[] =>
  traces.filter((trace) => trace.name === suffix || trace.name.endsWith(suffix));

const toolInput = (trace: ToolTrace): JsonObject | undefined => asObject(trace.input);
const toolOutput = (trace: ToolTrace): JsonObject | undefined => asObject(trace.output);
const toolPayload = (trace: ToolTrace): JsonObject | undefined => {
  const output = toolOutput(trace);
  const structured = asObject(output?.structuredContent);
  if (structured) return structured;
  const content = Array.isArray(output?.content) ? output.content : [];
  for (const item of content) {
    const text = asObject(asObject(item)?.text);
    if (text) return text;
  }
  return output;
};
const hasEnvelope = (trace: ToolTrace, expected: Readonly<{ id: string; from: string; to: string; type: string; in_reply_to?: string; content: string }>): boolean =>
  records(trace.output).some((record) => {
    const payload = asObject(record.payload);
    if (!payload) return false;
    return record.id === expected.id && record.from === expected.from && record.to === expected.to &&
      record.type === expected.type && payload.content === expected.content &&
      (expected.in_reply_to === undefined || payload.in_reply_to === expected.in_reply_to);
  });

const claimEvidence = async (redis: OwnedRedis["redis"], agent: string, messageID: string) => {
  const rows = await streamRows(redis, `gptq:inbox-trace:${agent}`);
  const claim = rows.find((row) => row.stage === "task_claimed" && row.message_id === messageID && typeof row.claim_id === "string");
  const acknowledgement = claim && rows.find((row) => row.stage === "task_acknowledged" && row.claim_id === claim.claim_id);
  return claim && acknowledgement ? { claim, acknowledgement } : undefined;
};

const activeAgentState = async (redis: OwnedRedis["redis"], agent: string) => {
  const raw = await redis.hget(SESSION_KEYS.registry, agent);
  const sessions = await redis.smembers(SESSION_KEYS.agentSessions(agent));
  const active = (await Promise.all(sessions.map(async (session) =>
    (await redis.exists(SESSION_KEYS.lease(session))) === 1 ? session : undefined))).filter((session): session is string => Boolean(session));
  return { registered: raw !== null, active_sessions: active, metadata: raw ? asObject(raw) : undefined };
};

const requestEvidence = (trace: ToolTrace, sender: string, recipient: string, content: string, requestID: string): MessageEvidence => {
  const input = toolInput(trace);
  const output = toolPayload(trace);
  expect(input).toMatchObject({ to: recipient, type: "task", content });
  expect(output?.message_id).toBe(requestID);
  return { id: requestID, from: sender, to: recipient, content };
};

const replyEvidence = (trace: ToolTrace, sender: string, recipient: string, requestID: string, content: string): MessageEvidence => {
  const input = toolInput(trace);
  const output = toolPayload(trace);
  expect(input).toMatchObject({ to: recipient, type: "result", in_reply_to: requestID, content });
  const replyID = typeof output?.message_id === "string" ? output.message_id : undefined;
  expect(replyID).toBeTruthy();
  return { id: replyID!, from: sender, to: recipient, in_reply_to: requestID, content };
};

describe("OpenCode child readiness oracle", () => {
  const agent = "gptqueue-opencode-child-123";
  const runtimeID = "child-123";
  const retainedReceiptPath = process.env.GPTQUEUE_OPENCODE_REPAIR_RECEIPT;
  const runtimeTool = {
    info: { role: "assistant" },
    parts: [{ type: "tool", tool: "gptqueue_get_runtime_status", state: {
      status: "completed", input: {}, output: { structuredContent: {
        status: "ok", agent, activation_ready: true, runtime: { runtime_id: runtimeID },
      } },
    } }],
  };

  it("rejects a marker that appears only in the user prompt", () => {
    expect(childReadinessEvidence([
      { info: { role: "user" }, parts: [{ type: "text", text: `CHILD_READY ${agent}` }] },
      runtimeTool,
    ], agent, runtimeID)).toBeUndefined();
  });

  it("rejects an assistant marker that reports another agent", () => {
    expect(childReadinessEvidence([
      { info: { role: "assistant" }, parts: [{ type: "text", text: "CHILD_READY gptqueue-opencode-other" }] },
      runtimeTool,
    ], agent, runtimeID)).toBeUndefined();
  });

  it("rejects a marker that appears only in tool output", () => {
    expect(childReadinessEvidence([
      { info: { role: "assistant" }, parts: [{ type: "tool", tool: "task", state: {
        status: "completed", output: `CHILD_READY ${agent}`,
      } }] },
      runtimeTool,
    ], agent, runtimeID)).toBeUndefined();
  });

  it("rejects a correct marker with a wrong runtime binding", () => {
    const wrongRuntimeTool = {
      ...runtimeTool,
      parts: [{ type: "tool", tool: "gptqueue_get_runtime_status", state: {
        status: "completed", input: {}, output: { structuredContent: {
          status: "ok", agent, activation_ready: true, runtime: { runtime_id: "another-child" },
        } },
      } }],
    };
    expect(childReadinessEvidence([
      { info: { role: "assistant" }, parts: [{ type: "text", text: `CHILD_READY ${agent}` }] },
      wrongRuntimeTool,
    ], agent, runtimeID)).toBeUndefined();
  });

  it("accepts the exact child assistant response and runtime probe", () => {
    expect(childReadinessEvidence([
      { info: { role: "assistant" }, parts: [{ type: "text", text: `CHILD_READY ${agent}` }] },
      runtimeTool,
    ], agent, runtimeID)).toMatchObject({ reportedAgent: agent, runtimeID });
  });

  it.skipIf(!retainedReceiptPath)("accepts the retained sanitized native receipt offline", async () => {
    const receipt = JSON.parse(await readFile(retainedReceiptPath!, "utf8")) as JsonObject;
    const child = asObject(receipt.child);
    const expectedAgent = typeof child?.agent === "string" ? child.agent : "";
    const expectedRuntimeID = expectedAgent.replace(/^gptqueue-opencode-/u, "");
    const evidence = childReadinessEvidence(asObject(receipt.creation_phase)?.child_history, expectedAgent, expectedRuntimeID);
    expect(evidence).toMatchObject({ reportedAgent: expectedAgent, runtimeID: expectedRuntimeID });
  });
});

describe.skipIf(!repairEnabled)("OpenCode native identity repair", () => {
  let owned: OwnedRedis | undefined;
  let server: RepairOpenCodeServer | undefined;

  afterAll(async () => {
    await server?.close().catch(() => undefined);
    await owned?.close().catch(() => undefined);
  });

  it("keeps the exact parent and idle native Task child independently active", async () => {
    vi.setConfig({ testTimeout: 480_000, hookTimeout: 30_000 });
    owned = await openOwnedRedis();
    server = await startRepairOpenCodeServer(owned.url);
    const runID = randomUUID();
    const receipt: JsonObject = {
      schema_version: 2,
      route: "opencode-native-task-session-isolation-repair",
      run_id: runID,
      redis_database: 15,
      plugin_path: repairPluginPath,
      native_tool_ids: server.toolIds,
      passed: false,
      evidence_status: "in_progress",
    };
    const persist = () => writeRepairReceipt(runID, receipt);
    let parent: RepairSession | undefined;
    let parentAgent: string | undefined;
    let childAgent: string | undefined;
    persist();
    try {
      parent = await server.session();
      const parentNativeID = parent.id;
      parentAgent = `gptqueue-opencode-${parentNativeID}`;
      const parentBefore = await parent.record();
      const parentStateBefore = await waitFor(
        () => activeAgentState(owned!.redis, parentAgent!),
        (state) => state.registered && state.active_sessions.length > 0,
      );
      const parentRedisSession = parentStateBefore.active_sessions[0];
      expect(parentRedisSession).toBeTruthy();
      receipt.parent = { native_id_sha256: hashText(parentNativeID), agent: parentAgent, redis_session_sha256: hashText(parentRedisSession!), registered: true };
      receipt.milestone = "parent_registered";
      persist();

      const creationPrompt = [
        "Use only the locally installed GPTQueue plugin tools and the native Task tool. Do not use shell, files, direct MCP servers, web, or another OpenCode process.",
        "GPTQueue is already bound to this exact OpenCode session; never register an agent or provide session credentials.",
        "Invoke native Task exactly once with a general-purpose child. The child prompt must call gptqueue_get_runtime_status and gptqueue_list_agents once, report CHILD_READY and its exact bound gptqueue-opencode name, then finish. This first turn creates the child only; do not send or claim a GPTQueue task.",
      ].join(" ");

      try {
        receipt.creation_prompt_returned = await parent.prompt(creationPrompt);
        receipt.milestone = "creation_prompt_returned";
        persist();
      } catch (error) {
        receipt.prompt_error = String(error);
        receipt.parent_history_after_prompt_error = await parent.history().catch((historyError) => ({ read_error: String(historyError) }));
        receipt.sessions_after_prompt_error = await server.sessions().catch((sessionsError) => ({ read_error: String(sessionsError) }));
        receipt.milestone = "parent_prompt_failed_but_evidence_captured";
        persist();
        throw error;
      }

      const parentCreationHistory = await parent.history();
      const parentCreationTraces = toolTraces(parentCreationHistory);
      const taskTrace = namedTool(parentCreationTraces, "task").find((trace) => trace.status === "completed" && !trace.error);
      expect(taskTrace).toBeTruthy();

      const childRecords = await waitFor(
        async () => asArray(await server!.sessions()).map(asObject).filter((record): record is JsonObject => Boolean(record)).filter((record) => record.parentID === parentNativeID),
        (recordsFound) => recordsFound.length === 1,
      );
      const childRecord = childRecords[0]!;
      const childNativeID = typeof childRecord.id === "string" ? childRecord.id : "";
      expect(childNativeID).toBeTruthy();
      expect(childNativeID).not.toBe(parentNativeID);
      expect(childRecord.parentID).toBe(parentNativeID);
      childAgent = `gptqueue-opencode-${childNativeID}`;
      const parentAfterChild = await parent.record();
      expect(parentAfterChild.id).toBe(parentBefore.id);
      const parentStateAfterChild = await activeAgentState(owned.redis, parentAgent);
      expect(parentStateAfterChild.registered).toBe(true);
      expect(parentStateAfterChild.active_sessions).toContain(parentRedisSession);
      const childState = await waitFor(() => activeAgentState(owned!.redis, childAgent!), (state) => state.registered && state.active_sessions.length > 0);
      expect(childState.registered).toBe(true);
      expect(childState.active_sessions.length).toBeGreaterThan(0);
      expect(childState.active_sessions).not.toEqual(parentStateBefore.active_sessions);
      expect(childState.active_sessions.some((session) => !parentStateBefore.active_sessions.includes(session))).toBe(true);
      const child = await server.session(childNativeID);
      const childCreationHistory = await child.history();
      const childCreationTraces = toolTraces(childCreationHistory);
      expect(namedTool(childCreationTraces, "gptqueue_get_runtime_status").some((trace) => trace.status === "completed" && !trace.error)).toBe(true);
      expect(namedTool(childCreationTraces, "gptqueue_list_agents").some((trace) => trace.status === "completed" && !trace.error)).toBe(true);
      expect(childReadinessEvidence(childCreationHistory, childAgent, childNativeID)).toMatchObject({
        reportedAgent: childAgent,
        runtimeID: childNativeID,
      });
      const childStatusRoot = asObject(await child.status());
      const childStatusMap = asObject(childStatusRoot?.data) ?? childStatusRoot;
      const childStatusEntry = asObject(childStatusMap?.[childNativeID]);
      const childStatus = typeof childStatusEntry?.type === "string" ? childStatusEntry.type : "idle";
      expect(childStatus).toBe("idle");
      const leftOperand = randomInt(10, 90);
      const rightOperand = randomInt(10, 90);
      const expectedReply = String(leftOperand + rightOperand);
      const challenge = `Compute ${leftOperand} + ${rightOperand} and reply with the resulting decimal number only.`;
      expect(JSON.stringify(childCreationHistory)).not.toContain(challenge);
      receipt.challenge_sha256 = hashText(challenge);
      receipt.child = { native_id_sha256: hashText(childNativeID), agent: childAgent, parent_id_sha256: hashText(parentNativeID), registered: childState.registered, active_sessions: childState.active_sessions.length };
      receipt.creation_phase = { observed_at: new Date().toISOString(), parent_history: parentCreationHistory, child_history: childCreationHistory, child_status: childStatus, parent_state_after_child: parentStateAfterChild };
      receipt.milestone = "exact_child_session_idle_and_parent_preserved";
      persist();

      const exchangePrompt = [
        "Use only the locally installed GPTQueue plugin tools. Do not use native Task, shell, files, direct MCP servers, web, or another OpenCode process.",
        "GPTQueue is already bound to this exact parent session; never register an agent or provide session credentials.",
        `The host has verified one idle child peer with the exact GPTQueue agent name '${childAgent}'. Send exactly one task to that agent with content '${challenge}', type task, and idempotency_key '${runID}'.`,
        "Then repeatedly claim tasks until the exact correlated child result arrives. Require the exact sender, recipient, message ID and in_reply_to, acknowledge that claim, and report the original tool results.",
      ].join(" ");
      try {
        receipt.exchange_prompt_returned = await parent.prompt(exchangePrompt);
        receipt.milestone = "exchange_prompt_returned";
        persist();
      } catch (error) {
        receipt.exchange_prompt_error = String(error);
        receipt.parent_history_after_exchange_prompt_error = await parent.history().catch((historyError) => ({ read_error: String(historyError) }));
        receipt.milestone = "exchange_prompt_failed_but_evidence_captured";
        persist();
        throw error;
      }

      const parentHistory = await parent.history();
      receipt.exchange_parent_history = parentHistory;
      receipt.milestone = "exchange_history_captured";
      persist();
      const parentTraces = toolTraces(parentHistory);
      const sendTrace = namedTool(parentTraces, "gptqueue_send_message").find((trace) => {
        const input = toolInput(trace);
        return trace.status === "completed" && !trace.error && input?.to === childAgent && input?.type === "task" && input?.content === challenge;
      });
      expect(sendTrace).toBeTruthy();
      const requestID = toolPayload(sendTrace!)?.message_id;
      expect(typeof requestID).toBe("string");
      const request = requestEvidence(sendTrace!, parentAgent, childAgent, challenge, requestID as string);
      const childClaim = await waitFor(() => claimEvidence(owned!.redis, childAgent!, request.id), (value) => value !== undefined);
      expect(childClaim).toBeTruthy();

      const childHistory = await child.history();
      receipt.exchange_child_history = childHistory;
      persist();
      const childTraces = toolTraces(childHistory);
      const childClaimTrace = namedTool(childTraces, "gptqueue_claim_tasks").find((trace) => hasEnvelope(trace, {
        id: request.id, from: parentAgent!, to: childAgent!, type: "task", content: challenge,
      }));
      expect(childClaimTrace).toBeTruthy();
      const replyTrace = namedTool(childTraces, "gptqueue_send_message").find((trace) => {
        const input = toolInput(trace);
        return trace.status === "completed" && !trace.error && input?.to === parentAgent && input?.type === "result" && input?.in_reply_to === request.id && input?.content === expectedReply;
      });
      expect(replyTrace).toBeTruthy();
      const reply = replyEvidence(replyTrace!, childAgent, parentAgent, request.id, expectedReply);
      const parentAfterReplyHistory = await parent.history();
      receipt.parent_after_reply_history = parentAfterReplyHistory;
      persist();
      const parentTracesAfterReply = toolTraces(parentAfterReplyHistory);
      const parentClaimTrace = namedTool(parentTracesAfterReply, "gptqueue_claim_tasks").find((trace) => hasEnvelope(trace, {
        id: reply.id, from: childAgent!, to: parentAgent!, type: "result", in_reply_to: request.id, content: expectedReply,
      }));
      expect(parentClaimTrace).toBeTruthy();
      const parentClaim = await waitFor(() => claimEvidence(owned!.redis, parentAgent!, reply.id), (value) => value !== undefined);
      expect(parentClaim).toBeTruthy();
      const childClaimID = childClaim!.claim.claim_id;
      const parentClaimID = parentClaim!.claim.claim_id;
      expect(typeof childClaimID).toBe("string");
      expect(typeof parentClaimID).toBe("string");

      const evidence: ExchangeEvidence = {
        sender: parentAgent,
        recipient: childAgent,
        request,
        reply,
        request_consumption: { message_id: request.id, actor: childAgent, consumed: true, claim_id: childClaimID as string, acknowledged: true },
        reply_consumption: { message_id: reply.id, actor: parentAgent, consumed: true, claim_id: parentClaimID as string, acknowledged: true },
        expected_reply_content: expectedReply,
        execution: { status: "completed" },
        request_requires_ack: true,
        reply_requires_ack: true,
      };
      const verdict = checkExchangeEvidence(evidence);
      expect(verdict.outcome).toBe("meets");
      receipt.request = request;
      receipt.reply = reply;
      receipt.child_claim = childClaim;
      receipt.parent_claim = parentClaim;
      receipt.verdict = verdict;
      receipt.native_tool_history = { parent: parentHistory, child: childHistory, parent_after_reply: parentAfterReplyHistory };
      receipt.source_hashes = {
        plugin: hashFile(join(process.cwd(), "src/registered-shell/opencode-plugin.ts")),
        backend: hashFile(join(process.cwd(), "src/registered-shell/opencode-backend.ts")),
        runtime: hashFile(join(process.cwd(), "src/registered-shell/opencode-runtime.ts")),
        pool: hashFile(join(process.cwd(), "src/registered-shell/opencode-sessions.ts")),
        plugin_factory: hashFile(join(process.cwd(), "src/registered-shell/opencode-plugin-factory.ts")),
        built_plugin: hashFile(repairPluginPath),
        built_plugin_factory: hashFile(join(process.cwd(), "dist/registered-shell/opencode-plugin-factory.js")),
        built_backend: hashFile(join(process.cwd(), "dist/registered-shell/opencode-backend.js")),
        built_runtime: hashFile(join(process.cwd(), "dist/registered-shell/opencode-runtime.js")),
        built_pool: hashFile(join(process.cwd(), "dist/registered-shell/opencode-sessions.js")),
        config: hashText(JSON.stringify(repairConfig())),
      };
      receipt.passed = true;
      receipt.evidence_status = "complete";
      receipt.milestone = "exact_exchange_and_acknowledgements_observed";
      persist();
    } catch (error) {
      receipt.error = String(error);
      receipt.evidence_status = "incomplete";
      persist();
      throw error;
    } finally {
      await cleanupOwnedAgent(owned.redis, childAgent).catch(() => undefined);
      await cleanupOwnedAgent(owned.redis, parentAgent).catch(() => undefined);
      receipt.milestone = receipt.passed === true ? "cleaned_up" : receipt.milestone;
      persist();
    }
  }, 480_000);
});
