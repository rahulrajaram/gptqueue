import { createHash } from "node:crypto";
import type { ExchangeEvidence, MessageEvidence } from "./oracle.js";
import type { GenericCallRecord, ParticipantIdentity, RawEvidenceRef } from "./qualification-types.js";

export type NativeTrace = Readonly<{
  sourceId: string; name: string; inputHash: string; outputHash: string;
  actor: ParticipantIdentity; runtimeId: string; rawHistoryRef: RawEvidenceRef;
  input?: Json; output?: Json; successful?: boolean; runtimeBound?: boolean;
}>;
export type EnvelopeObservation = Readonly<{
  id: string; from: string; to: string; type: "task" | "result" | "error" | "ping" | "status";
  inReplyTo?: string; payloadHash: string; nonce?: string;
}>;
export type GenericActorHistory = Readonly<{
  actor: ParticipantIdentity;
  records: readonly GenericCallRecord[];
}>;
export type CollectedExchange = Readonly<{
  evidence: ExchangeEvidence; traces: readonly NativeTrace[]; observations: readonly EnvelopeObservation[];
}>;
export type NativeExchangeParticipant = Readonly<{ actor: ParticipantIdentity; traces: readonly NativeTrace[] }>;
export type NativeConsumption = "claim_ack" | "legacy_receive";
export type NativeConsumptionContract = Readonly<{ request: NativeConsumption; reply: NativeConsumption }>;
type Json = Record<string, unknown>;

const object = (value: unknown): Json | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const parse = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value) as unknown; } catch { return value; }
};
const hash = (value: unknown): string => createHash("sha256").update(JSON.stringify(value) ?? "null").digest("hex");
const result = (record: GenericCallRecord): Json | undefined => {
  const response = object(record.response);
  if (!response || response.isError === true) return undefined;
  return object(response.result);
};
const message = (record: GenericCallRecord): Json | undefined => object(result(record)?.message);
const allowedTools = new Set(["send_message", "receive_message", "register_agent", "unregister_agent", "close_session", "get_queue_status", "list_agents"]);
const observedMessageIds = (record: GenericCallRecord): readonly string[] => {
  const value = result(record);
  const ids: string[] = [];
  if (typeof value?.message_id === "string") ids.push(value.message_id);
  const nested = object(value?.message);
  if (typeof nested?.id === "string") ids.push(nested.id);
  return ids;
};


export const extractGenericTraces = (records: readonly GenericCallRecord[], actor: ParticipantIdentity, rawHistoryRef: RawEvidenceRef, allowFailures = false): readonly NativeTrace[] =>
  Object.freeze(records.map((record) => {
    if (record.sourceId.length === 0) throw new Error("generic call record has no source ID");
    const response = object(record.response);
    const output = result(record);
    const failed = !response || response.isError === true || !output || output.status === "error" || output.error !== undefined;
    if (failed) {
      if (!allowFailures) throw new Error(`generic call ${record.sourceId} failed`);
      const failureOutput = { error: `generic call ${record.sourceId} failed` };
      return Object.freeze({ sourceId: record.sourceId, name: record.name, inputHash: hash(record.request), outputHash: hash(record.response), actor, runtimeId: actor.hostRuntimeId, rawHistoryRef, input: record.request, output: failureOutput, successful: false, runtimeBound: false });
    }
    if (!output) throw new Error(`generic call ${record.sourceId} failed`);
    return Object.freeze({ sourceId: record.sourceId, name: record.name, inputHash: hash(record.request), outputHash: hash(record.response), actor, runtimeId: actor.hostRuntimeId, rawHistoryRef, input: record.request, output, successful: true, runtimeBound: true });
  }));

export const extractNativeTraces = (history: unknown, actor: ParticipantIdentity, rawHistoryRef: RawEvidenceRef): readonly NativeTrace[] => {
  if (actor.route === "codex-headless") return extractCodexHeadlessTraces(history, actor, rawHistoryRef);
  const found: NativeTrace[] = [];
  const runtimeBindings: Array<{ id: string; agent: string }> = [];
  const root = object(history);
  const codexItems = Array.isArray(root?.turns) ? root.turns.flatMap((turn) => {
    const value = object(turn);
    return Array.isArray(value?.items) ? value.items.map(object).filter((item): item is Json => item !== undefined) : [];
  }) : [];
  const rawPiRows = Array.isArray(history) ? history.map(object).filter((row): row is Json => row !== undefined) : [];
  const piRows = actor.route === "pi-native-child" || actor.route === "pi-interactive"
    ? (() => {
      const sessions = rawPiRows.filter((row) => row.type === "session");
      if (sessions.length !== 1 || sessions[0]?.id !== actor.hostRuntimeId) throw new Error("native Pi session history requires exactly one matching session header");
      return rawPiRows.flatMap((row) => {
        if (row.type !== "message") return [];
        const nested = object(row.message);
        return nested ? [nested] : [];
      });
    })()
    : rawPiRows;
  const codexCalls = codexItems.filter((row) => row.type === "mcpToolCall");
  const piCalls = piRows.flatMap((row) => row.role === "assistant" && Array.isArray(row.content) ? row.content.map(object).filter((item): item is Json => item?.type === "toolCall") : []);
  const piResults = new Map(piRows.filter((row) => row.role === "toolResult" && typeof row.toolCallId === "string").map((row) => [row.toolCallId as string, row] as const));
  for (const call of codexCalls) {
    if (typeof call.tool !== "string") continue;
    if (typeof call.server === "string" && call.server !== "gptqueue-shared") throw new Error(`native ${call.tool} call uses an unexpected server`);
    if (typeof call.id !== "string" || call.id.length === 0) throw new Error(`native ${call.tool} call has no source ID`);
    const input = object(parse(call.arguments));
    const output = object(parseNativeOutput(call.result));
    const terminal = call.status === undefined || call.status === "completed" || call.status === "succeeded";
    const failed = !terminal || call.isError === true || object(call.result)?.isError === true || !output || output.error !== undefined || output.isError === true;
    const evidenceOutput = output ?? { error: `native ${call.tool} call ${call.id} has no output` };
    if (!failed && call.tool === "get_runtime_status") {
      const runtime = runtimeFromOutput(evidenceOutput);
      if (!runtime) throw new Error(`native runtime status ${call.id} has no exact successful binding`);
      runtimeBindings.push(runtime);
    }
    found.push(Object.freeze({ sourceId: call.id, name: call.tool, inputHash: hash(input), outputHash: hash(evidenceOutput), actor, runtimeId: actor.hostRuntimeId, rawHistoryRef, input, output: evidenceOutput, successful: !failed }));
  }
  for (const call of piCalls) {
    if (typeof call.id !== "string" || call.id.length === 0 || typeof call.name !== "string") throw new Error("native Pi tool call has no exact source ID/name");
    const result = piResults.get(call.id);
    const resultMatchesCall = result !== undefined && (typeof result.toolName !== "string" || result.toolName === call.name);
    const output = resultMatchesCall ? object(parsePiNativeOutput(result)) : undefined;
    const input = object(parse(call.arguments));
    const failed = !resultMatchesCall || result?.isError === true || !output || output.error !== undefined || output.isError === true;
    const evidenceOutput = output ?? { error: `native ${call.name} call ${call.id} has no matching output` };
    if (!failed && call.name === "get_runtime_status") {
      const runtime = runtimeFromOutput(evidenceOutput);
      if (!runtime) throw new Error(`native runtime status ${String(call.id)} has no exact successful binding`);
      runtimeBindings.push(runtime);
    }
    found.push(Object.freeze({ sourceId: call.id, name: call.name, inputHash: hash(input), outputHash: hash(evidenceOutput), actor, runtimeId: actor.hostRuntimeId, rawHistoryRef, input, output: evidenceOutput, successful: !failed }));
  }
  if (runtimeBindings.some((binding) => binding.id !== actor.hostRuntimeId || binding.agent !== actor.agent)) throw new Error(`native history runtime binding does not match ${actor.agent}/${actor.hostRuntimeId}`);
  const runtimeBound = runtimeBindings.length > 0;
  return Object.freeze(found.map((trace) => Object.freeze({ ...trace, runtimeBound: trace.successful === true && runtimeBound })));
};

/** Headless Codex emits exec JSONL events rather than app-server turns. Its
 * thread binding proves communication identity; activation_ready/runtime are
 * intentionally not required because manual headless runs report null there. */
const extractCodexHeadlessTraces = (history: unknown, actor: ParticipantIdentity, rawHistoryRef: RawEvidenceRef): readonly NativeTrace[] => {
  const rows = Array.isArray(history) ? history.map(object).filter((row): row is Json => row !== undefined) : [];
  const threads = rows.filter(row => row.type === "thread.started").map(row => row.thread_id);
  if (threads.length !== 1 || threads[0] !== actor.hostRuntimeId) throw new Error("headless history has foreign or multiple thread IDs");
  const calls = rows.filter(row => row.type === "item.completed").map(row => object(row.item)).filter((call): call is Json => call?.type === "mcp_tool_call" && typeof call.tool === "string");
  const found: readonly NativeTrace[] = calls.map(call => {
    if (typeof call.id !== "string" || call.id.length === 0) throw new Error("headless native call has no source ID");
    const input = object(parse(call.arguments));
    const output = object(parseNativeOutput(call.result));
    const terminal = call.status === "completed" || call.status === "succeeded";
    const successful = call.server === "gptqueue-shared" && terminal && call.error == null && call.isError !== true && object(call.result)?.isError !== true && output !== undefined && output.status !== "error" && output.error === undefined && output.isError !== true;
    return Object.freeze({ sourceId: call.id, name: String(call.tool), inputHash: hash(input), outputHash: hash(call.result), actor, runtimeId: actor.hostRuntimeId, rawHistoryRef, input, output: output ?? { error: "headless call failed" }, successful, runtimeBound: successful });
  });
  if (new Set(found.map(trace => trace.sourceId)).size !== found.length) throw new Error("headless history has duplicate call IDs");
  const bindings = found.filter(trace => trace.name === "get_runtime_status" && trace.successful && trace.output?.status === "ok");
  if (!bindings.length || bindings.some(trace => trace.output?.agent !== actor.agent || (trace.output.runtime != null && object(trace.output.runtime)?.runtime_id !== actor.hostRuntimeId))) throw new Error("headless history has no exact runtime status agent binding");
  return Object.freeze(found);
};

const openCodeToolNames = new Map([
  ["gptqueue_send_message", "send_message"], ["gptqueue_receive_message", "receive_message"],
  ["gptqueue_claim_tasks", "claim_tasks"], ["gptqueue_acknowledge_tasks", "acknowledge_tasks"],
  ["gptqueue_get_runtime_status", "get_runtime_status"],
] as const);
type OpenCodeToolName = "gptqueue_send_message" | "gptqueue_receive_message" | "gptqueue_claim_tasks" | "gptqueue_acknowledge_tasks" | "gptqueue_get_runtime_status";
const openCodeRows = (history: unknown, actor?: ParticipantIdentity): readonly Json[] => {
  const root = object(history);
  const selected: Json | undefined = object(root?.native_tool_history) ?? root;
  if (Array.isArray(history)) return history.map(object).filter((row): row is Json => row !== undefined);
  if (Array.isArray(selected?.parent) && Array.isArray(selected.child) && actor) {
    const streams = [selected.parent, selected.child].map((stream) => stream.map(object).filter((row): row is Json => row !== undefined));
    const matching = streams.find((stream) => stream.some((row) => {
      const parts = Array.isArray(row.parts) ? row.parts.map(object).filter((part): part is Json => part !== undefined) : [];
      return parts.some((part) => {
        if (part.type !== "tool" || part.tool !== "gptqueue_get_runtime_status") return false;
        const state = object(part.state);
        const statusOutput = object(parseNativeOutput(object(state?.output)?.structuredContent ?? state?.output));
        return statusOutput !== undefined && runtimeFromOutput(statusOutput)?.agent === actor.agent;
      });
    }));
    if (matching) return matching;
  }
  if (Array.isArray(selected?.parent)) return selected.parent.map(object).filter((row): row is Json => row !== undefined);
  if (Array.isArray(selected?.child)) return selected.child.map(object).filter((row): row is Json => row !== undefined);
  return selected ? [selected] : [];
};

/** Normalize OpenCode's known `info[].parts[].tool.state` records without walking user/tool payloads. */
export const extractOpenCodeTraces = (history: unknown, actor: ParticipantIdentity, rawHistoryRef: RawEvidenceRef): readonly NativeTrace[] => {
  const found: NativeTrace[] = [];
  const runtimeBindings: Array<{ id: string; agent: string }> = [];
  for (const row of openCodeRows(history, actor)) {
    const parts = Array.isArray(row.parts) ? row.parts.map(object).filter((part): part is Json => part !== undefined) : [];
    for (const part of parts) {
      if (part.type !== "tool" || typeof part.tool !== "string") continue;
      const name = openCodeToolNames.get(part.tool as OpenCodeToolName);
      if (!name) continue;
      if (typeof part.callID !== "string" || part.callID.length === 0) throw new Error(`OpenCode ${part.tool} call has no source ID`);
      const state = object(part.state);
      const input = object(state?.input);
      const output = object(parseNativeOutput(state?.output));
      const successful = state?.status === "completed" && output !== undefined && output.error === undefined && output.isError !== true && object(state?.output)?.isError !== true;
      const evidenceOutput = output ?? { error: `OpenCode ${part.tool} call ${part.callID} has no output` };
      if (successful && name === "get_runtime_status") {
        const runtime = runtimeFromOutput(evidenceOutput);
        if (!runtime) throw new Error(`OpenCode runtime status ${part.callID} has no exact successful binding`);
        runtimeBindings.push(runtime);
      }
      found.push(Object.freeze({ sourceId: part.callID, name, inputHash: hash(input), outputHash: hash(evidenceOutput), actor, runtimeId: actor.hostRuntimeId, rawHistoryRef, input, output: evidenceOutput, successful, runtimeBound: false }));
    }
  }
  if (runtimeBindings.some((binding) => binding.id !== actor.hostRuntimeId || binding.agent !== actor.agent)) throw new Error(`OpenCode history runtime binding does not match ${actor.agent}/${actor.hostRuntimeId}`);
  const runtimeBound = runtimeBindings.length > 0;
  return Object.freeze(found.map((trace) => Object.freeze({ ...trace, runtimeBound: trace.successful === true && runtimeBound })));
};

/** Normalize ACP `session/update` tool call and completion notifications. */
export const extractOpenCodeAcpTraces = (history: unknown, actor: ParticipantIdentity, rawHistoryRef: RawEvidenceRef): readonly NativeTrace[] => {
  const rows = Array.isArray(history) ? history.map(object).filter((row): row is Json => row !== undefined) : [];
  const inputs = new Map<string, Json>();
  const found: NativeTrace[] = [];
  const runtimeBindings: Array<{ id: string; agent: string }> = [];
  for (const row of rows) {
    if (row.method !== "session/update") continue;
    const params = object(row.params), update = object(params?.update);
    if (update?.sessionUpdate === "tool_call" && typeof update.toolCallId === "string") {
      const input = object(update.rawInput);
      if (input) inputs.set(update.toolCallId, input);
      continue;
    }
    if (update?.sessionUpdate !== "tool_call_update" || typeof update.toolCallId !== "string" || typeof update.title !== "string") continue;
    if (!["completed", "failed", "error", "cancelled"].includes(String(update.status))) continue;
    const name = openCodeToolNames.get(update.title as OpenCodeToolName);
    if (!name) continue;
    const output = object(parseNativeOutput(object(update.rawOutput)?.output ?? update.rawOutput));
    const input = inputs.get(update.toolCallId) ?? object(update.rawInput);
    const successful = update.status === "completed" && output !== undefined && output.error === undefined && output.isError !== true && object(update.rawOutput)?.isError !== true;
    const evidenceOutput = output ?? { error: `OpenCode ACP ${update.title} call ${update.toolCallId} has no output` };
    if (successful && name === "get_runtime_status") {
      const runtime = runtimeFromOutput(evidenceOutput);
      if (!runtime) throw new Error(`OpenCode ACP runtime status ${update.toolCallId} has no exact successful binding`);
      runtimeBindings.push(runtime);
    }
    found.push(Object.freeze({ sourceId: update.toolCallId, name, inputHash: hash(input), outputHash: hash(evidenceOutput), actor, runtimeId: actor.hostRuntimeId, rawHistoryRef, input, output: evidenceOutput, successful, runtimeBound: false }));
  }
  if (runtimeBindings.some((binding) => binding.id !== actor.hostRuntimeId || binding.agent !== actor.agent)) throw new Error(`OpenCode ACP history runtime binding does not match ${actor.agent}/${actor.hostRuntimeId}`);
  const runtimeBound = runtimeBindings.length > 0;
  return Object.freeze(found.map((trace) => Object.freeze({ ...trace, runtimeBound: trace.successful === true && runtimeBound })));
};

const parseNativeOutput = (value: unknown): unknown => {
  const direct = object(value);
  if (!direct) return parse(value);
  const structured = object(direct.structuredContent ?? direct.structured_content);
  if (structured) return structured;
  const content = Array.isArray(direct.content) ? direct.content : undefined;
  const text = content?.map(object).find((item) => item?.type === "text")?.text;
  return text === undefined ? value : parse(text);
};
const parsePiNativeOutput = (value: unknown): unknown => {
  const direct = object(value);
  if (!direct) return parse(value);
  const details = object(direct.details);
  const structured = object(details?.structuredContent ?? details?.structured_content ?? direct.structuredContent ?? direct.structured_content);
  if (structured) {
    const nestedError = details?.error ?? (details?.isError === true ? true : undefined);
    const topLevelError = direct.error ?? (direct.isError === true ? true : undefined);
    return Object.freeze({ ...structured, ...(nestedError !== undefined && structured.error === undefined ? { error: nestedError } : {}), ...(topLevelError !== undefined && structured.error === undefined ? { error: topLevelError } : {}), ...(direct.isError === true || details?.isError === true ? { isError: true } : {}) });
  }
  return parseNativeOutput(value);
};
const runtimeFromOutput = (value: Json): Readonly<{ id: string; agent: string }> | undefined => {
  const runtime = object(value.runtime);
  return value.status === "ok" && typeof runtime?.runtime_id === "string" && typeof value.agent === "string" ? { id: runtime.runtime_id, agent: value.agent } : undefined;
};

const envelope = (value: Json | undefined): EnvelopeObservation => {
  const payload = value ? object(value.payload) ?? value : undefined;
  const type = typeof value?.type === "string" ? value.type : payload?.type;
  if (!value || typeof value.id !== "string" || typeof value.from !== "string" || typeof value.to !== "string" || typeof type !== "string") throw new Error("missing exact message envelope");
  if (!["task", "result", "error", "ping", "status"].includes(type)) throw new Error(`unsupported message type ${type}`);
  return Object.freeze({ id: value.id, from: value.from, to: value.to, type: type as EnvelopeObservation["type"], inReplyTo: typeof payload?.in_reply_to === "string" ? payload.in_reply_to : undefined, payloadHash: hash(payload?.content), nonce: typeof payload?.nonce === "string" ? payload.nonce : undefined });
};

export const collectGenericExchange = (input: Readonly<{
  sender: ParticipantIdentity; receiver: ParticipantIdentity; nonce: string; requestContent: string; expectedReplyContent: string;
  sent: unknown; received: unknown; replied: unknown; returned: unknown; traces: readonly NativeTrace[]; priorMessageIds?: readonly string[];
  histories: Readonly<{ sender: GenericActorHistory; receiver: GenericActorHistory }>;
}>): CollectedExchange => {
  const sent = object(input.sent), received = object(input.received), replied = object(input.replied), returned = object(input.returned);
  if (sent?.status !== "sent" || typeof sent.message_id !== "string" || sent.message_id.length === 0) throw new Error("send response has no exact message_id");
  if (typeof sent.to !== "string" || sent.to !== input.receiver.agent) throw new Error("send response has the wrong actor");
  if (replied?.status !== "sent") throw new Error("exchange has an unsuccessful send result");
  const request = envelope(received);
  const returnedEnvelope = envelope(returned);
  if (sent.message_id !== request.id || request.from !== input.sender.agent || request.to !== input.receiver.agent || request.type !== "task") throw new Error("request envelope failed exact identity/type checks");
  const requestPayload = object(requestPayloadOf(received));
  if (requestPayload?.content !== input.requestContent || !String(requestPayload.content).includes(input.nonce)) throw new Error("request content or nonce mismatch");
  if (typeof replied?.message_id !== "string" || replied.message_id.length === 0 || replied.message_id !== returnedEnvelope.id || replied.to !== input.sender.agent || returnedEnvelope.from !== input.receiver.agent || returnedEnvelope.to !== input.sender.agent || returnedEnvelope.type !== "result" || returnedEnvelope.inReplyTo !== request.id) throw new Error("reply envelope failed exact identity/type/correlation checks");
  const replyPayload = object(requestPayloadOf(returned));
  if (replyPayload?.content !== input.expectedReplyContent) throw new Error("reply content mismatch");

  const senderHistory = input.histories.sender;
  const receiverHistory = input.histories.receiver;
  if (senderHistory.actor.agent !== input.sender.agent || receiverHistory.actor.agent !== input.receiver.agent || senderHistory.actor.hostRuntimeId !== input.sender.hostRuntimeId || receiverHistory.actor.hostRuntimeId !== input.receiver.hostRuntimeId) throw new Error("history actor binding failed");
  const senderRecords = senderHistory.records;
  const receiverRecords = receiverHistory.records;
  for (const record of [...senderRecords, ...receiverRecords]) {
    if (typeof record.sourceId !== "string" || record.sourceId.length === 0) throw new Error("generic call record has no source ID");
    if (!allowedTools.has(record.name)) throw new Error(`exchange contains wrong tool ${record.name}`);
    if (!result(record)) throw new Error(`generic call ${record.sourceId} failed or has no result`);
  }
  const senderSend = senderRecords.filter((record) => {
    const value = result(record);
    return record.name === "send_message" && value?.status === "sent" && value.message_id === sent.message_id;
  });
  const receiverReceive = receiverRecords.filter((record) => {
    const value = message(record);
    return record.name === "receive_message" && result(record)?.status === "message" && value?.id === request.id;
  });
  const receiverSend = receiverRecords.filter((record) => {
    const value = result(record);
    return record.name === "send_message" && value?.status === "sent" && value.message_id === replied.message_id;
  });
  const senderReceive = senderRecords.filter((record) => {
    const value = message(record);
    return record.name === "receive_message" && result(record)?.status === "message" && value?.id === returnedEnvelope.id;
  });
  if (senderSend.length !== 1 || receiverReceive.length !== 1 || receiverSend.length !== 1 || senderReceive.length !== 1) throw new Error("exchange is missing exact successful call records or has stale IDs");
  const senderSendRequest = object(senderSend[0]?.request);
  const receiverSendRequest = object(receiverSend[0]?.request);
  if (senderSendRequest?.to !== input.receiver.agent || senderSendRequest.type !== "task" || senderSendRequest.content !== input.requestContent || !String(senderSendRequest.content).includes(input.nonce) || receiverSendRequest?.to !== input.sender.agent || receiverSendRequest.type !== "result" || receiverSendRequest.content !== input.expectedReplyContent || receiverSendRequest.in_reply_to !== request.id) throw new Error("tool call request failed exact type/content/correlation checks");
  const calls = [
    { actor: input.sender, record: senderSend[0] }, { actor: input.receiver, record: receiverReceive[0] },
    { actor: input.receiver, record: receiverSend[0] }, { actor: input.sender, record: senderReceive[0] },
  ] as const;
  for (const { actor, record } of calls) {
    if (!record) throw new Error("exchange call record was unexpectedly absent");
    const trace = input.traces.find((candidate) => candidate.sourceId === record.sourceId);
    if (!trace || trace.name !== record.name || trace.actor.agent !== actor.agent || trace.runtimeId !== actor.hostRuntimeId || trace.inputHash !== hash(record.request) || trace.outputHash !== hash(record.response)) throw new Error(`missing exact trace for ${record.name} call ${record.sourceId}`);
  }
  if (input.traces.length < 4 || input.traces.some((trace) => !((trace.actor.agent === input.sender.agent && trace.runtimeId === input.sender.hostRuntimeId) || (trace.actor.agent === input.receiver.agent && trace.runtimeId === input.receiver.hostRuntimeId)))) throw new Error("exchange traces contain a wrong actor or are incomplete");
  const currentIds = new Set([request.id, returnedEnvelope.id]);
  const observedPrior = [...senderRecords, ...receiverRecords].flatMap(observedMessageIds).filter((id) => !currentIds.has(id));
  const priorMessageIds = Object.freeze([...new Set([...(input.priorMessageIds ?? []), ...observedPrior])]);
  const requestMessage: MessageEvidence = { id: request.id, from: request.from, to: request.to, content: String(requestPayload.content) };
  const replyMessage: MessageEvidence = { id: returnedEnvelope.id, from: returnedEnvelope.from, to: returnedEnvelope.to, in_reply_to: returnedEnvelope.inReplyTo, content: String(replyPayload.content) };
  return Object.freeze({
    evidence: Object.freeze({ sender: input.sender.agent, recipient: input.receiver.agent, request: requestMessage, reply: replyMessage, expected_reply_content: input.expectedReplyContent, prior_message_ids: priorMessageIds, request_consumption: { message_id: request.id, actor: input.receiver.agent, consumed: true, acknowledged: false }, reply_consumption: { message_id: returnedEnvelope.id, actor: input.sender.agent, consumed: true, acknowledged: false }, request_requires_ack: false, reply_requires_ack: false, execution: { status: "completed" as const } }),
    traces: input.traces,
    observations: Object.freeze([request, returnedEnvelope]),
  });
};

const traceInput = (trace: NativeTrace): Json => trace.input ?? {};
const traceOutput = (trace: NativeTrace): Json => trace.output ?? {};
const claimTask = (trace: NativeTrace, messageId: string): Json | undefined => {
  const claim = object(traceOutput(trace).claim);
  if (trace.successful !== true || trace.name !== "claim_tasks" || trace.runtimeId !== trace.actor.hostRuntimeId || traceOutput(trace).claimed !== true || claim?.actor_id !== trace.actor.agent || typeof claim.claim_id !== "string" || !Array.isArray(claim.tasks)) return undefined;
  return claim.tasks.map(parse).map(object).find((task) => task?.id === messageId);
};
const acknowledgement = (traces: readonly NativeTrace[], claimId: string, actor: ParticipantIdentity): NativeTrace | undefined => traces.find((trace) => trace.successful === true && trace.name === "acknowledge_tasks" && trace.actor.agent === actor.agent && trace.runtimeId === actor.hostRuntimeId && traceInput(trace).claim_id === claimId && traceOutput(trace).status === "ok" && Number(traceOutput(trace).acknowledged) > 0);
const sentMessage = (traces: readonly NativeTrace[], type: "task" | "result", actor: ParticipantIdentity, peer: ParticipantIdentity, content: string, inReplyTo?: string): NativeTrace | undefined => traces.find((trace) => trace.successful === true && trace.name === "send_message" && trace.actor.agent === actor.agent && trace.runtimeId === actor.hostRuntimeId && traceInput(trace).to === peer.agent && traceInput(trace).type === type && traceInput(trace).content === content && (inReplyTo === undefined || traceInput(trace).in_reply_to === inReplyTo) && traceOutput(trace).status === "sent" && traceOutput(trace).to === peer.agent && typeof traceOutput(trace).message_id === "string");
const receivedMessage = (traces: readonly NativeTrace[], actor: ParticipantIdentity, peer: ParticipantIdentity, type: "task" | "result", messageId?: string, inReplyTo?: string): Readonly<{ trace: NativeTrace; message: Json }> | undefined => traces.map((trace) => ({ trace, message: object(traceOutput(trace).message) })).find((entry): entry is Readonly<{ trace: NativeTrace; message: Json }> => entry.trace.successful === true && entry.trace.name === "receive_message" && entry.trace.actor.agent === actor.agent && entry.trace.runtimeId === actor.hostRuntimeId && traceOutput(entry.trace).status === "message" && entry.message?.from === peer.agent && entry.message?.to === actor.agent && entry.message?.type === type && (messageId === undefined || entry.message.id === messageId) && (inReplyTo === undefined || (object(requestPayloadOf(entry.message))?.in_reply_to === inReplyTo)));

const collectLegacyNativeExchange = (input: Readonly<{
  sender: NativeExchangeParticipant; receiver: NativeExchangeParticipant; nonce: string; requestContent: string; expectedReplyContent: string; priorMessageIds?: readonly string[];
}>): CollectedExchange => {
  const requestTrace = sentMessage(input.sender.traces, "task", input.sender.actor, input.receiver.actor, input.requestContent);
  if (!requestTrace) throw new Error("native sender has no exact fresh task send");
  const requestId = String(traceOutput(requestTrace).message_id);
  const receiverReceive = receivedMessage(input.receiver.traces, input.receiver.actor, input.sender.actor, "task", requestId);
  if (!receiverReceive) throw new Error("native receiver has no exact legacy task receive");
  const request = envelope(receiverReceive.message);
  const requestPayload = object(requestPayloadOf(receiverReceive.message));
  const replyTrace = sentMessage(input.receiver.traces, "result", input.receiver.actor, input.sender.actor, input.expectedReplyContent, request.id);
  if (!replyTrace) throw new Error("native receiver has no exact correlated reply");
  const replyId = String(traceOutput(replyTrace).message_id);
  const senderReceive = receivedMessage(input.sender.traces, input.sender.actor, input.receiver.actor, "result", replyId, request.id);
  if (!senderReceive) throw new Error("native sender has no exact legacy reply receive");
  const reply = envelope(senderReceive.message);
  const replyPayload = object(requestPayloadOf(senderReceive.message));
  if (request.id !== requestId || request.from !== input.sender.actor.agent || request.to !== input.receiver.actor.agent || request.type !== "task" || requestPayload?.content !== input.requestContent || !String(requestPayload.content).includes(input.nonce) || reply.id !== replyId || reply.from !== input.receiver.actor.agent || reply.to !== input.sender.actor.agent || reply.type !== "result" || reply.inReplyTo !== request.id || replyPayload?.content !== input.expectedReplyContent) throw new Error("legacy native envelope failed exact actor/type/nonce/correlation checks");
  const successful = input.sender.traces.concat(input.receiver.traces).filter((trace) => trace.successful === true);
  const required = [requestTrace, receiverReceive.trace, replyTrace, senderReceive.trace];
  if (required.some((trace) => !successful.includes(trace))) throw new Error("legacy exchange contains a failed required call");
  const observedIds = successful.flatMap((trace) => [
    ...(typeof traceOutput(trace).message_id === "string" ? [String(traceOutput(trace).message_id)] : []),
    ...(object(traceOutput(trace).message) && typeof object(traceOutput(trace).message)?.id === "string" ? [String(object(traceOutput(trace).message)?.id)] : []),
  ]);
  const prior = Object.freeze([...new Set([...(input.priorMessageIds ?? []), ...observedIds.filter((id) => id !== request.id && id !== reply.id)])]);
  const requestMessage: MessageEvidence = { id: request.id, from: request.from, to: request.to, content: String(requestPayload.content) };
  const replyMessage: MessageEvidence = { id: reply.id, from: reply.from, to: reply.to, in_reply_to: reply.inReplyTo, content: String(replyPayload?.content) };
  return Object.freeze({ evidence: Object.freeze({ sender: input.sender.actor.agent, recipient: input.receiver.actor.agent, request: requestMessage, reply: replyMessage, expected_reply_content: input.expectedReplyContent, prior_message_ids: prior, request_consumption: { message_id: request.id, actor: input.receiver.actor.agent, consumed: true, acknowledged: false }, reply_consumption: { message_id: reply.id, actor: input.sender.actor.agent, consumed: true, acknowledged: false }, request_requires_ack: false, reply_requires_ack: false, execution: { status: "completed" as const } }), traces: Object.freeze([...input.sender.traces, ...input.receiver.traces]), observations: Object.freeze([request, reply]) });
};

type NativeConsumptionEvidence = Readonly<{ trace: NativeTrace; message: Json; claim?: NativeTrace }>;

const consumptionContract = (consumption: NativeConsumption | NativeConsumptionContract | undefined): NativeConsumptionContract => {
  if (consumption === undefined || consumption === "claim_ack") return Object.freeze({ request: "claim_ack", reply: "claim_ack" });
  if (consumption === "legacy_receive") return Object.freeze({ request: "legacy_receive", reply: "legacy_receive" });
  if (![consumption.request, consumption.reply].every(mode => mode === "claim_ack" || mode === "legacy_receive"))
    throw new Error("invalid native consumption contract");
  return Object.freeze({ request: consumption.request, reply: consumption.reply });
};

const consumeNativeMessage = (traces: readonly NativeTrace[], mode: NativeConsumption, actor: ParticipantIdentity, peer: ParticipantIdentity, type: "task" | "result", messageId: string, inReplyTo?: string): NativeConsumptionEvidence | undefined => {
  if (mode === "legacy_receive") {
    const received = receivedMessage(traces, actor, peer, type, messageId, inReplyTo);
    return received ? Object.freeze({ trace: received.trace, message: received.message }) : undefined;
  }
  const claimed = traces.map((trace) => ({ trace, message: claimTask(trace, messageId) })).find((entry) => entry.message !== undefined);
  return claimed?.message ? Object.freeze({ trace: claimed.trace, message: claimed.message, claim: claimed.trace }) : undefined;
};

/** Normalize a model-driven exchange from native tool calls and exact claim/ack joins. */
export const collectNativeExchange = (input: Readonly<{
  sender: NativeExchangeParticipant; receiver: NativeExchangeParticipant; nonce: string; requestContent: string; expectedReplyContent: string; priorMessageIds?: readonly string[]; consumption?: NativeConsumption | NativeConsumptionContract;
}>): CollectedExchange => {
  for (const trace of [...input.sender.traces, ...input.receiver.traces].filter((candidate) => candidate.successful === true)) {
    const actor = trace.actor.agent === input.sender.actor.agent ? input.sender.actor : trace.actor.agent === input.receiver.actor.agent ? input.receiver.actor : undefined;
    if (!actor || trace.runtimeId !== actor.hostRuntimeId || trace.runtimeBound !== true) throw new Error("native trace actor/runtime binding failed");
  }
  const contract = consumptionContract(input.consumption);
  if (contract.request === "legacy_receive" && contract.reply === "legacy_receive") return collectLegacyNativeExchange(input);
  const requestTrace = sentMessage(input.sender.traces, "task", input.sender.actor, input.receiver.actor, input.requestContent);
  if (!requestTrace) throw new Error("native sender has no exact fresh task send");
  const requestId = String(traceOutput(requestTrace).message_id);
  const requestConsumed = consumeNativeMessage(input.receiver.traces, contract.request, input.receiver.actor, input.sender.actor, "task", requestId);
  if (!requestConsumed) throw new Error(`native receiver has no exact ${contract.request === "claim_ack" ? "claim" : "legacy task receive"} for the request`);
  const request = envelope(requestConsumed.message);
  const replyTrace = sentMessage(input.receiver.traces, "result", input.receiver.actor, input.sender.actor, input.expectedReplyContent, request.id);
  if (!replyTrace) throw new Error("native receiver has no exact correlated reply");
  const replyId = String(traceOutput(replyTrace).message_id);
  const replyConsumed = consumeNativeMessage(input.sender.traces, contract.reply, input.sender.actor, input.receiver.actor, "result", replyId, request.id);
  if (!replyConsumed) throw new Error(`native sender has no exact ${contract.reply === "claim_ack" ? "claim" : "legacy reply receive"} for the reply`);
  const reply = envelope(replyConsumed.message);
  if (request.from !== input.sender.actor.agent || request.to !== input.receiver.actor.agent || request.type !== "task" || object(requestPayloadOf(requestConsumed.message))?.content !== input.requestContent || !String(object(requestPayloadOf(requestConsumed.message))?.content).includes(input.nonce) || reply.id !== replyId || reply.from !== input.receiver.actor.agent || reply.to !== input.sender.actor.agent || reply.type !== "result" || reply.inReplyTo !== request.id || object(requestPayloadOf(replyConsumed.message))?.content !== input.expectedReplyContent) throw new Error("native envelope failed exact actor/type/nonce/correlation checks");
  const requestClaimId = requestConsumed.claim ? String(object(traceOutput(requestConsumed.claim).claim)?.claim_id) : undefined;
  const replyClaimId = replyConsumed.claim ? String(object(traceOutput(replyConsumed.claim).claim)?.claim_id) : undefined;
  const requestAck = requestClaimId ? acknowledgement(input.receiver.traces, requestClaimId, input.receiver.actor) : undefined;
  const replyAck = replyClaimId ? acknowledgement(input.sender.traces, replyClaimId, input.sender.actor) : undefined;
  if ((contract.request === "claim_ack" && !requestAck) || (contract.reply === "claim_ack" && !replyAck)) throw new Error("native claim acknowledgement is not joined to the exact message");
  const observedIds = input.sender.traces.concat(input.receiver.traces).flatMap((trace) => typeof traceOutput(trace).message_id === "string" ? [String(traceOutput(trace).message_id)] : []);
  const prior = Object.freeze([...new Set([...(input.priorMessageIds ?? []), ...observedIds.filter((id) => id !== request.id && id !== reply.id)])]);
  const requestMessage: MessageEvidence = { id: request.id, from: request.from, to: request.to, content: String(object(requestPayloadOf(requestConsumed.message))?.content) };
  const replyMessage: MessageEvidence = { id: reply.id, from: reply.from, to: reply.to, in_reply_to: reply.inReplyTo, content: String(object(requestPayloadOf(replyConsumed.message))?.content) };
  return Object.freeze({ evidence: Object.freeze({ sender: input.sender.actor.agent, recipient: input.receiver.actor.agent, request: requestMessage, reply: replyMessage, expected_reply_content: input.expectedReplyContent, prior_message_ids: prior, request_consumption: { message_id: request.id, actor: input.receiver.actor.agent, ...(requestClaimId ? { claim_id: requestClaimId } : {}), consumed: true, acknowledged: requestAck !== undefined }, reply_consumption: { message_id: reply.id, actor: input.sender.actor.agent, ...(replyClaimId ? { claim_id: replyClaimId } : {}), consumed: true, acknowledged: replyAck !== undefined }, request_requires_ack: contract.request === "claim_ack", reply_requires_ack: contract.reply === "claim_ack", execution: { status: "completed" as const } }), traces: Object.freeze([...input.sender.traces, ...input.receiver.traces]), observations: Object.freeze([request, reply]) });
};

const requestPayloadOf = (value: unknown): unknown => object(value)?.payload ?? value;
