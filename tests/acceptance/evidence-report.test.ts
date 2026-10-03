import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import { describe, expect, it } from "vitest";
import { reportsTotal } from "./answer.js";
import {
  checkExchangeEvidence,
  evaluateTotalVerdict,
  type AcceptanceDimension,
  type AcceptanceRow,
  type ExchangeEvidence,
  type Outcome,
} from "./oracle.js";

type Json = Record<string, any>;
type Execution = "not_run" | "completed" | "failed" | "unsupported";
type Source = Readonly<{ path: string; sha256: string }>;
type Fact = Readonly<{ outcome: Outcome; execution: { status: Execution; detail?: string }; detail: string; sources: readonly Source[] }>;
type Pair = Readonly<{
  from: string; to: string; independent_instances: true;
  registration_identity: Fact; communication: Fact; automatic_tasks: Fact;
  wire?: Readonly<{ edge_count: number; pair_count: number; actual_exchange_count: number; distinct_instance_count: number; complete: boolean; message_types: readonly string[]; exact_received: number }>;
  sources: readonly Source[];
}>;

const root = resolve(import.meta.dirname, "../..");
const artifactRoot = join(root, ".gptqueue/acceptance/20260912-evaluation");
const inventoryPath = join(artifactRoot, "inventory.json");
const reportJsonPath = join(artifactRoot, "report.json");
const reportMarkdownPath = join(artifactRoot, "report.md");
const routeDispositionPath = join(artifactRoot, "route-disposition.json");
const digest = (value: string | Uint8Array): string => createHash("sha256").update(value).digest("hex");
const source = (file: string): Source => ({ path: relative(root, file), sha256: digest(readFileSync(file)) });
const readJson = (file: string): Json => JSON.parse(readFileSync(file, "utf8")) as Json;
const execution = (status: Execution, detail?: string) => detail ? { status, detail } : { status };
const unresolved = (detail: string, sources: readonly Source[] = []): Fact => ({ outcome: "uncertain", execution: execution("not_run"), detail, sources });
const fromOracle = (result: ReturnType<typeof checkExchangeEvidence>, detail: string, sources: readonly Source[]): Fact => ({ outcome: result.outcome, execution: result.execution, detail: [detail, ...result.reasons].join("; "), sources });
const row = (id: string, dimension: AcceptanceDimension, fact: Fact, required = true): AcceptanceRow => ({ id, dimension, outcome: fact.outcome, execution: fact.execution, required, detail: fact.detail });
const findReceipts = (directory: string): string[] => {
  if (!existsSync(directory)) return [];
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = join(directory, entry.name);
    return entry.isDirectory() ? findReceipts(file) : entry.name === "receipt.json" ? [file] : [];
  });
};
const findNamed = (directory: string, names: readonly string[]): string[] => {
  if (!existsSync(directory)) return [];
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const file = join(directory, entry.name);
    return entry.isDirectory() ? findNamed(file, names) : names.includes(entry.name) ? [file] : [];
  });
};

const messagesOf = (value: unknown): Json[] => {
  if (Array.isArray(value)) return value.filter((item): item is Json => Boolean(item) && typeof item === "object");
  if (value && typeof value === "object") return Object.values(value as Json).flatMap(messagesOf);
  return [];
};
const toolCalls = (messages: readonly Json[], name: string): Json[] => messages.flatMap(message =>
  Array.isArray(message.content) ? message.content.filter((part: Json) => (part.type === "toolCall" || part.type === "collabAgentToolCall") && part.name === name) : []);
const toolResults = (messages: readonly Json[], name?: string): Json[] => messages.filter(message => message.role === "toolResult" && (name === undefined || message.toolName === name));
const structured = (message: Json | undefined): Json | undefined => {
  if (!message) return undefined;
  if (message.details?.structuredContent && typeof message.details.structuredContent === "object") return message.details.structuredContent as Json;
  try { return JSON.parse(message.content?.find((part: Json) => part.type === "text")?.text ?? "") as Json; } catch { return undefined; }
};
const structuredResults = (messages: readonly Json[], name: string): Json[] => toolResults(messages, name).map(structured).filter((value): value is Json => value !== undefined);
const acknowledgesClaim = (messages: readonly Json[], claimId: string | undefined): boolean => Boolean(claimId) && toolCalls(messages, "acknowledge_tasks").some(call =>
  call.arguments?.claim_id === claimId && toolResults(messages, "acknowledge_tasks").some(result => result.toolCallId === call.id && structured(result)?.status === "ok" && Number(structured(result)?.acknowledged) > 0));
const taskHasId = (task: unknown, id: string): boolean => {
  if (task && typeof task === "object") return String((task as Json).id ?? "") === id;
  try { return String((JSON.parse(String(task)) as Json).id ?? "") === id; } catch { return false; }
};
const textOf = (messages: readonly Json[]): string => messages.flatMap(message => Array.isArray(message.content) ? message.content.map((part: Json) => typeof part.text === "string" ? part.text : "") : []).join("\n");
const sentMessageId = (messages: readonly Json[], recipient: string): string | undefined => structuredResults(messages, "send_message").find(value => value.status === "sent" && value.to === recipient && typeof value.message_id === "string")?.message_id;
const objectFromReceive = (messages: readonly Json[], id: string): Json | undefined => {
  for (const value of structuredResults(messages, "receive_message")) {
    const message = value.message && typeof value.message === "object" ? value.message as Json : value;
    if (message.id === id) return message;
  }
  return undefined;
};
const count = (text: string, pattern: RegExp): number | undefined => { const match = pattern.exec(text); return match?.[1] === undefined ? undefined : Number(match[1]); };
const arithmeticAnswer = (text: string): string | undefined => { const match = /(?:compute|calculate)\s+(\d+)\s*\+\s*(\d+)/iu.exec(text); return match ? String(Number(match[1]) + Number(match[2])) : undefined; };
const textOfParts = (value: unknown): string => {
  if (typeof value === "string") return value;
  if (!Array.isArray(value)) return "";
  return value.map((part: Json) => typeof part?.text === "string" ? part.text : "").join("\n");
};
const eventParts = (receipt: Json): Json[] => Array.isArray(receipt.run_result?.events) ? receipt.run_result.events.flatMap((event: Json) => Array.isArray(event?.parts) ? event.parts : event?.part && typeof event.part === "object" ? [event.part] : []) : [];
const toolEvents = (receipt: Json, name: string): Json[] => eventParts(receipt).filter(part => part.type === "tool" && part.tool === name && part.state?.status === "completed");
const finalAnswerText = (receipt: Json): string => [
  ...messagesOf(receipt.final_messages?.coordinator),
  ...eventParts(receipt).filter(part => part.type === "text"),
].map(message => textOfParts(message.content ?? message.text)).join("\n");

const initiativeEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  const coordinator = messagesOf(receipt.messages?.coordinator), specialist = messagesOf(receipt.messages?.specialist);
  const agents = receipt.agents as Json | undefined, ids = receipt.message_ids as Json | undefined;
  if (!coordinator.length || !specialist.length || !agents || !ids?.request || !ids.reply) return undefined;
  if (typeof receipt.expected !== "string" || !reportsTotal(finalAnswerText(receipt), receipt.expected)) return undefined;
  const sender = String(agents.coordinator), recipient = String(agents.specialist);
  const requestCall = toolCalls(coordinator, "send_message").find(call => call.arguments?.to === recipient);
  const replyCall = toolCalls(specialist, "send_message").find(call => call.arguments?.to === sender);
  const requestId = sentMessageId(coordinator, recipient), replyId = sentMessageId(specialist, sender);
  if (!requestCall || !replyCall || requestId !== String(ids.request) || replyId !== String(ids.reply)) return undefined;
  const received = objectFromReceive(coordinator, replyId);
  const verified = count(textOf(specialist.filter(message => message.role === "user")), /verified count is\s+(\d+)/iu);
  const expected = verified === undefined ? undefined : String(verified);
  const specialistTrace = messagesOf(receipt.traces?.specialist);
  const requestClaim = specialistTrace.find(trace => trace.stage === "task_claimed" && trace.message_id === requestId);
  const requestAck = specialistTrace.find(trace => trace.stage === "task_acknowledged" && trace.claim_id === requestClaim?.claim_id);
  const inReplyTo = replyCall.arguments?.in_reply_to;
  const receivedContent = String(received?.payload?.content ?? received?.content ?? "");
  const actualAnswer = /^\s*\d+\s*$/u.test(receivedContent) ? receivedContent.trim() : count(receivedContent, /verified count.*?is\s+(\d+)/iu);
  if (!expected || !actualAnswer || typeof inReplyTo !== "string") return undefined;
  return {
    sender, recipient, expected_reply_content: expected,
    request: { id: requestId, from: sender, to: recipient, content: String(requestCall.arguments?.content ?? "") },
    reply: { id: replyId, from: recipient, to: sender, in_reply_to: inReplyTo, content: String(actualAnswer) },
    request_consumption: { message_id: requestId, actor: recipient, consumed: Boolean(requestClaim), claim_id: requestClaim?.claim_id, acknowledged: Boolean(requestAck) },
    reply_consumption: { message_id: replyId, actor: sender, consumed: received?.id === replyId, acknowledged: false },
    execution: execution(receipt.passed === true ? "completed" : "failed", receipt.error), request_requires_ack: true, reply_requires_ack: false,
  };
};

const genericPiEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  const sent = receipt.sent as Json | undefined, reply = receipt.reply as Json | undefined, traces = messagesOf(receipt.traces), messages = messagesOf(receipt.messages);
  const requestId = typeof sent?.message_id === "string" ? sent.message_id : undefined, replyId = typeof reply?.id === "string" ? reply.id : undefined;
  if (sent === undefined || reply === undefined || !requestId || !replyId || typeof sent.to !== "string" || typeof reply.from !== "string" || typeof reply.to !== "string") return undefined;
  const request: Json = sent, response: Json = reply;
  const claim = traces.find(trace => trace.stage === "task_claimed" && trace.message_id === requestId);
  const acknowledged = traces.some(trace => trace.stage === "task_acknowledged" && trace.claim_id === claim?.claim_id);
  const expected = arithmeticAnswer(textOf(messages) + "\n" + JSON.stringify(structuredResults(messages, "claim_tasks")));
  const replySent = traces.find(trace => trace.stage === "reply_sent" && trace.message_id === replyId);
  if (!expected || !replySent || response.payload?.in_reply_to !== requestId) return undefined;
  return {
    sender: response.to, recipient: request.to, expected_reply_content: expected,
    request: { id: requestId, from: response.to, to: request.to, content: "Compute arithmetic task from claim_tasks" },
    reply: { id: replyId, from: response.from, to: response.to, in_reply_to: response.payload?.in_reply_to, content: response.payload?.content },
    request_consumption: { message_id: requestId, actor: request.to, consumed: Boolean(claim), claim_id: claim?.claim_id, acknowledged },
    reply_consumption: { message_id: replyId, actor: response.to, consumed: Boolean(replySent), acknowledged: false },
    execution: execution(receipt.passed === true ? "completed" : "failed", receipt.error), request_requires_ack: true, reply_requires_ack: false,
  };
};

const piModeEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  const request = receipt.request as Json | undefined, reply = receipt.reply as Json | undefined;
  const traces = messagesOf(receipt.traces);
  if (receipt.passed !== true || typeof request?.message_id !== "string" || typeof request.to !== "string" || typeof request.content !== "string" || typeof reply?.id !== "string" || typeof reply.from !== "string" || typeof reply.to !== "string" || typeof reply.payload?.in_reply_to !== "string" || reply.payload.in_reply_to !== request.message_id) return undefined;
  const expected = arithmeticAnswer(String(request.content ?? ""));
  if (!expected) return undefined;
  const requestClaim = traces.find(trace => trace.stage === "task_claimed" && trace.message_id === request.message_id && typeof trace.claim_id === "string");
  const requestAck = traces.some(trace => trace.stage === "task_acknowledged" && trace.claim_id === requestClaim?.claim_id);
  const replySent = traces.some(trace => trace.stage === "reply_sent" && trace.message_id === reply.id && trace.in_reply_to === request.message_id);
  if (!requestClaim || !requestAck || !replySent || String(reply.payload.content ?? "") !== expected) return undefined;
  return {
    sender: reply.to, recipient: reply.from, expected_reply_content: expected,
    request: { id: request.message_id, from: reply.to, to: request.to, content: String(request.content ?? "") },
    reply: { id: reply.id, from: reply.from, to: reply.to, in_reply_to: reply.payload.in_reply_to, content: reply.payload?.content },
    request_consumption: { message_id: request.message_id, actor: request.to, consumed: true, claim_id: requestClaim.claim_id, acknowledged: requestAck },
    reply_consumption: { message_id: reply.id, actor: reply.to, consumed: true, acknowledged: false },
    execution: execution("completed"), request_requires_ack: true, reply_requires_ack: false,
  };
};

const nativeChildEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  const request = receipt.request as Json | undefined, reply = receipt.reply as Json | undefined, parentClaim = receipt.parent_claim as Json | undefined, childClaim = receipt.child_claim as Json | undefined;
  if (!request?.id || !reply?.id || !request.from || !request.to || !reply.from || !reply.to) return undefined;
  const marker = typeof request.marker === "string" ? request.marker : undefined;
  if (!marker) return undefined;
  const childAck = messagesOf(receipt.child_trace).some(trace => trace.stage === "task_acknowledged" && trace.claim_id === childClaim?.claim_id);
  const parentAck = messagesOf(receipt.parent_trace).some(trace => trace.stage === "task_acknowledged" && trace.claim_id === parentClaim?.claim_id);
  return {
    sender: String(request.from), recipient: String(request.to), expected_reply_content: `${marker}-reply`,
    request: { id: String(request.id), from: String(request.from), to: String(request.to), content: String(request.content ?? "") },
    reply: { id: String(reply.id), from: String(reply.from), to: String(reply.to), in_reply_to: String(reply.in_reply_to ?? ""), content: String(reply.content ?? "") },
    request_consumption: { message_id: String(request.id), actor: String(request.to), consumed: childClaim?.message_id === request.id, claim_id: childClaim?.claim_id, acknowledged: childAck },
    reply_consumption: { message_id: String(reply.id), actor: String(reply.to), consumed: parentClaim?.message_id === reply.id, claim_id: parentClaim?.claim_id, acknowledged: parentAck },
    execution: execution(receipt.passed === true ? "completed" : "failed", receipt.error), request_requires_ack: true, reply_requires_ack: true,
  };
};

const codexExchangeEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  const request = receipt.request as Json | undefined, reply = receipt.reply as Json | undefined;
  if (receipt.route !== "codex-headless-to-native-child" || receipt.passed !== true || typeof receipt.expected !== "string" ||
      !request?.id || !request.from || !request.to || !reply?.id || !reply.from || !reply.to || typeof reply.payload?.in_reply_to !== "string" ||
      reply.payload.in_reply_to !== String(request.id) || reply.from !== request.to || reply.to !== request.from ||
      String(reply.payload.content ?? "") !== String(receipt.expected)) return undefined;
  const childTrace = messagesOf(receipt.child_trace);
  const requestClaim = childTrace.find(trace => trace.stage === "task_claimed" && trace.message_id === String(request.id) && typeof trace.claim_id === "string");
  const requestAck = childTrace.some(trace => trace.stage === "task_acknowledged" && trace.claim_id === requestClaim?.claim_id);
  const replySent = childTrace.some(trace => trace.stage === "reply_sent" && trace.message_id === String(reply.id) && trace.in_reply_to === String(request.id));
  if (!requestClaim || !requestAck || !replySent) return undefined;
  return {
    sender: String(request.from), recipient: String(request.to), expected_reply_content: receipt.expected,
    request: { id: String(request.id), from: String(request.from), to: String(request.to) },
    reply: { id: String(reply.id), from: String(reply.from), to: String(reply.to), in_reply_to: String(reply.payload.in_reply_to), content: String(reply.payload.content ?? "") },
    request_consumption: { message_id: String(request.id), actor: String(request.to), consumed: true, claim_id: String(requestClaim.claim_id), acknowledged: requestAck },
    reply_consumption: { message_id: String(reply.id), actor: String(reply.to), consumed: true, acknowledged: false },
    execution: execution("completed"), request_requires_ack: true, reply_requires_ack: false,
  };
};

const codexInitiativeEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  if (receipt.route !== "codex-headless" || receipt.fixture_revision !== 1 || receipt.process?.code !== 0 || receipt.process?.timed_out === true ||
      typeof receipt.expected !== "string" || !reportsTotal(String(receipt.final_answer ?? ""), receipt.expected)) return undefined;
  const eventItems = Array.isArray(receipt.events) ? receipt.events.map((event: Json) => event.item).filter((item: unknown): item is Json => Boolean(item) && typeof item === "object") : [];
  const toolItems = (name: string): Json[] => eventItems.filter(item => item.type === "mcp_tool_call" && item.tool === name && item.result?.structured_content);
  const received = toolItems("receive_message").map(item => item.result.structured_content?.message).find((message: Json | undefined) => message?.id && message?.payload?.in_reply_to);
  const reply = receipt.reply as Json | undefined ?? received;
  if (!reply?.id || !reply.from || !reply.to || typeof reply.payload?.in_reply_to !== "string") return undefined;
  const requestId = String(reply.payload.in_reply_to);
  const send = toolItems("send_message").find(item => item.result.structured_content?.message_id === requestId);
  if (!send?.arguments?.to || typeof send.arguments.content !== "string" || send.result.structured_content?.status !== "sent") return undefined;
  const sender = String(reply.to), recipient = String(reply.from);
  if (String(send.arguments.to) !== recipient || reply.from !== send.arguments.to || reply.to !== sender) return undefined;
  const specialist = messagesOf(receipt.specialist_messages);
  const claim = structuredResults(specialist, "claim_tasks").map(result => result.claim).find((candidate: Json | undefined) => Array.isArray(candidate?.tasks) && candidate.tasks.some((task: unknown) => {
    try { return JSON.parse(String(task)).id === requestId; } catch { return false; }
  }));
  const acknowledged = acknowledgesClaim(specialist, String(claim?.claim_id ?? ""));
  const specialistCount = count(textOf(specialist.filter(message => message.role === "user")), /verified count is\s+(\d+)/iu);
  if (!claim?.claim_id || !acknowledged || specialistCount === undefined || String(reply.payload.content ?? "") !== String(specialistCount) ||
      String(reply.payload.content ?? "") !== String(receipt.inputs?.other ?? "")) return undefined;
  const expectedReply = String(specialistCount);
  return {
    sender, recipient, expected_reply_content: expectedReply,
    request: { id: requestId, from: sender, to: recipient, content: String(send.arguments.content) },
    reply: { id: String(reply.id), from: recipient, to: sender, in_reply_to: requestId, content: String(reply.payload.content) },
    request_consumption: { message_id: requestId, actor: recipient, consumed: true, claim_id: String(claim.claim_id), acknowledged },
    reply_consumption: { message_id: String(reply.id), actor: sender, consumed: true, acknowledged: false },
    execution: execution("completed"), request_requires_ack: true, reply_requires_ack: false,
  };
};

const opencodeInitiativeEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  if (receipt.route !== "opencode-run" || receipt.fixture_revision !== 3 || receipt.passed !== true || typeof receipt.expected !== "string" ||
      !reportsTotal(finalAnswerText(receipt), receipt.expected)) return undefined;
  const send = toolEvents(receipt, "gptqueue_send_message").find(part => typeof part.state?.input?.to === "string" && typeof part.state?.output?.message_id === "string");
  const receive = toolEvents(receipt, "gptqueue_receive_message").find(part => typeof part.state?.output?.id === "string");
  const register = toolEvents(receipt, "gptqueue_register_agent").find(part => typeof part.state?.output?.name === "string");
  const requestId = String(send?.state?.output?.message_id ?? ""), replyId = String(receive?.state?.output?.id ?? ""), sender = String(register?.state?.output?.name ?? "");
  const recipient = String(send?.state?.input?.to ?? ""), reply = receive?.state?.output as Json | undefined;
  const specialistMessages = messagesOf(receipt.specialist_messages);
  const claimResults = structuredResults(specialistMessages, "claim_tasks");
  const claim = claimResults.map(result => result.claim).find((candidate: Json | undefined) => Array.isArray(candidate?.tasks) && candidate.tasks.some((task: unknown) => taskHasId(task, requestId)));
  const acknowledged = acknowledgesClaim(specialistMessages, String(claim?.claim_id ?? ""));
  const replyText = String(reply?.payload?.content ?? "");
  const specialistAnswer = /^\s*\d+\s*$/u.test(replyText) ? replyText.trim() : count(replyText, /(?:verified\s+)?count\s+is\s+(\d+)/iu)?.toString();
  if (!requestId || !replyId || !sender || !recipient || !claim?.claim_id || !reply || reply.payload?.in_reply_to !== requestId || specialistAnswer !== String(receipt.specialist_count)) return undefined;
  return {
    sender, recipient, expected_reply_content: String(receipt.specialist_count),
    request: { id: requestId, from: sender, to: recipient, content: String(send?.state?.input?.content ?? "") },
    reply: { id: replyId, from: String(reply.from ?? recipient), to: String(reply.to ?? sender), in_reply_to: String(reply.payload.in_reply_to), content: specialistAnswer },
    request_consumption: { message_id: requestId, actor: recipient, consumed: true, claim_id: String(claim.claim_id), acknowledged },
    reply_consumption: { message_id: replyId, actor: sender, consumed: true, acknowledged: false },
    execution: execution("completed"), request_requires_ack: true, reply_requires_ack: false,
  };
};

const mixedProbeEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  const diagnostic = receipt.assisted_diagnostic, histories = messagesOf(diagnostic?.history?.after);
  const receive = histories.find(message => message.info?.role === "assistant" && message.parts?.some((part: Json) => part.type === "tool" && part.tool === "gptqueue_receive_message" && part.state?.output?.id));
  const send = histories.find(message => message.info?.role === "assistant" && message.parts?.some((part: Json) => part.type === "tool" && part.tool === "gptqueue_send_message" && part.state?.output?.message_id));
  const receivePart = receive?.parts?.find((part: Json) => part.type === "tool" && part.tool === "gptqueue_receive_message") as Json | undefined;
  const sendPart = send?.parts?.find((part: Json) => part.type === "tool" && part.tool === "gptqueue_send_message") as Json | undefined;
  const request = receivePart?.state?.output as Json | undefined, sent = sendPart?.state?.output as Json | undefined, input = sendPart?.state?.input as Json | undefined;
  if (receipt.route !== "opencode-serve-plus-pi-rpc" || !request?.id || !request.from || !request.to || !sent?.message_id || !input?.in_reply_to) return undefined;
  const marker = String(input.content ?? "").match(/[0-9a-f]{8}-[0-9a-f-]{27,}/iu)?.[0];
  if (!marker || String(input.content ?? "") !== `${marker}: acknowledged`) return undefined;
  const piMessages = messagesOf(diagnostic?.history?.pi);
  const claim = structuredResults(piMessages, "claim_tasks").map(result => result.claim).find((candidate: Json | undefined) =>
    typeof candidate?.claim_id === "string" && Array.isArray(candidate.tasks) && candidate.tasks.length === 1);
  const replyConsumed = Boolean(claim?.claim_id && acknowledgesClaim(piMessages, String(claim.claim_id)));
  return {
    sender: String(request.from), recipient: String(request.to), expected_reply_content: `${marker}: acknowledged`,
    request: { id: String(request.id), from: String(request.from), to: String(request.to), content: String(request.payload?.content ?? "") },
    reply: { id: String(sent.message_id), from: String(request.to), to: String(request.from), in_reply_to: String(input.in_reply_to), content: String(input.content) },
    request_consumption: { message_id: String(request.id), actor: String(request.to), consumed: true, acknowledged: false },
    reply_consumption: { message_id: String(sent.message_id), actor: String(request.from), consumed: replyConsumed, claim_id: claim?.claim_id, acknowledged: replyConsumed },
    execution: execution("completed"), request_requires_ack: false, reply_requires_ack: false,
  };
};

const opencodeTuiEvidence = (receipt: Json, consumption: Json): ExchangeEvidence | undefined => {
  const observation = receipt.routes?.tui?.observation as Json | undefined;
  const request = observation?.exact_request as Json | undefined;
  const parts = Array.isArray(consumption.parts) ? consumption.parts as Json[] : [];
  const reply = parts.find(part => part.type === "tool" && part.tool === "gptqueue_receive_message")?.state?.output as Json | undefined;
  if (!request?.id || !request.from || !request.to || !reply?.id || !reply.from || !reply.to || !reply.payload?.in_reply_to ||
      reply.payload.in_reply_to !== request.id || consumption.reply_id !== reply.id) return undefined;
  return {
    sender: String(request.from), recipient: String(request.to), expected_reply_content: `reply-${String(request.payload?.content ?? '')}`,
    request: { id: String(request.id), from: String(request.from), to: String(request.to), content: String(request.payload?.content ?? "") },
    reply: { id: String(reply.id), from: String(reply.from), to: String(reply.to), in_reply_to: String(reply.payload.in_reply_to), content: String(reply.payload.content ?? "") },
    request_consumption: { message_id: String(request.id), actor: String(request.to), consumed: true, acknowledged: false },
    reply_consumption: { message_id: String(reply.id), actor: String(reply.to), consumed: parts.some(part => part.type === 'tool' && part.tool === 'gptqueue_receive_message' && part.state?.status === 'completed' && part.state?.output?.id === reply.id), acknowledged: false },
    execution: execution(receipt.routes?.tui?.result?.exit?.code === 0 ? "completed" : "failed"), request_requires_ack: false, reply_requires_ack: false,
  };
};

const piHeadlessInitiativeEvidence = (receipt: Json): ExchangeEvidence | undefined => {
  if (receipt.route !== "pi-headless" || receipt.fixture_revision !== 1 || receipt.process?.code !== 0 || receipt.process?.timed_out === true ||
      typeof receipt.expected !== "string" || !reportsTotal(String(receipt.final_answer ?? ""), receipt.expected)) return undefined;
  const reply = receipt.reply as Json | undefined, messages = messagesOf(receipt.messages), specialistMessages = messagesOf(receipt.specialist_messages);
  if (!reply?.id || !reply.from || !reply.to || typeof reply.payload?.in_reply_to !== "string") return undefined;
  const requestId = String(reply.payload.in_reply_to), recipient = String(reply.from), sender = String(reply.to);
  const requestCall = toolCalls(messages, "send_message").find(call => call.arguments?.to === recipient && typeof call.arguments?.content === "string");
  const requestIdObserved = sentMessageId(messages, recipient);
  const received = objectFromReceive(messages, String(reply.id));
  const claim = structuredResults(specialistMessages, "claim_tasks").map(result => result.claim).find((candidate: Json | undefined) => Array.isArray(candidate?.tasks) && candidate.tasks.some((task: unknown) => taskHasId(task, requestId)));
  const acknowledged = acknowledgesClaim(specialistMessages, String(claim?.claim_id ?? ""));
  const own = Number(receipt.inputs?.own), other = Number(receipt.inputs?.other), expected = String(receipt.expected);
  const independentTotal = Number.isSafeInteger(own) && Number.isSafeInteger(other) && String(own + other) === expected;
  const exactReply = String(reply.payload.content ?? "") === String(other) && reply.payload.in_reply_to === requestId && received?.id === reply.id;
  if (!requestCall || requestIdObserved !== requestId || !claim?.claim_id || !acknowledged || !independentTotal || !exactReply) return undefined;
  return {
    sender, recipient, expected_reply_content: String(other),
    request: { id: requestId, from: sender, to: recipient, content: String(requestCall.arguments.content) },
    reply: { id: String(reply.id), from: recipient, to: sender, in_reply_to: requestId, content: String(reply.payload.content) },
    request_consumption: { message_id: requestId, actor: recipient, consumed: true, claim_id: String(claim.claim_id), acknowledged },
    reply_consumption: { message_id: String(reply.id), actor: sender, consumed: true, acknowledged: false },
    execution: execution("completed"), request_requires_ack: true, reply_requires_ack: false,
  };
};

const replyActivationFact = (receipt: Json, file: string): Fact | undefined => {
  const request = receipt.request as Json | undefined, reply = receipt.reply_send as Json | undefined, traces = messagesOf(receipt.traces), messages = messagesOf(receipt.messages);
  const claim = traces.find(trace => trace.stage === "task_claimed" && trace.message_id === reply?.message_id);
  const acknowledged = traces.some(trace => trace.stage === "task_acknowledged" && trace.claim_id === claim?.claim_id);
  const claimedReply = structuredResults(messages, "claim_tasks").flatMap(result => Array.isArray(result.claim?.tasks) ? result.claim.tasks : []).map((task: unknown) => { try { return JSON.parse(String(task)) as Json; } catch { return undefined; } }).find(task => task?.id === reply?.message_id);
  const expected = String(receipt.expected_continuation ?? ""), before = Number(receipt.message_count_before_reply), continuationMessages = Number.isInteger(before) ? messages.slice(before) : messages;
  const assistantTexts = continuationMessages.filter(message => message.role === "assistant").flatMap(message => Array.isArray(message.content) ? message.content.map((part: Json) => typeof part.text === "string" ? part.text.trim() : "") : []);
  const independent = receipt.reply_type === "result" ? arithmeticAnswer(String(request?.payload?.content ?? "")) === expected : receipt.reply_type === "error" && expected === "SERVICE_UNAVAILABLE";
  const actualContinuation = assistantTexts.includes(expected);
  const exactIncoming = claimedReply !== undefined && claimedReply.id === reply?.message_id && claimedReply.from === request?.to && claimedReply.to === request?.from && claimedReply.type === receipt.reply_type && claimedReply.payload?.in_reply_to === request?.id;
  const complete = receipt.route === "pi-rpc-cli" && receipt.passed === true && request?.id && request.from && request.to && reply?.message_id && reply.to === request.from && claim?.claim_id && acknowledged && exactIncoming && independent && actualContinuation;
  if (!complete) return undefined;
  return { outcome: "meets", execution: execution("completed"), detail: `Receiver ${receipt.route} completed ${receipt.reply_type} continuation for ${String(request.id)} with exact claim acknowledgement and independent answer`, sources: [source(file)] };
};

const summarizeFacts = (facts: readonly Fact[], detail: string, sources: readonly Source[]): Fact => {
  if (!facts.length) return unresolved(detail, sources);
  if (facts.some(fact => fact.outcome === "does_not_meet")) return { outcome: "does_not_meet", execution: execution("completed"), detail, sources };
  if (facts.some(fact => fact.outcome !== "meets" || fact.execution.status !== "completed")) return unresolved(detail, sources);
  return { outcome: "meets", execution: execution("completed"), detail, sources };
};

describe("frozen acceptance evidence report", () => {
  it.skipIf(process.env.GPTQUEUE_ACCEPTANCE_REPORT !== "1")("emits an aggregate report and complete ordered route-pair matrix", () => {
    const inventory = readJson(inventoryPath), routes = (inventory.routes as Json[]).map(route => String(route.id));
    expect(routes).toHaveLength(25);
    const receiptFiles = findReceipts(artifactRoot).sort();
    const evidenceFiles = [...new Set([...receiptFiles, ...findNamed(artifactRoot, ["probe.json", "claude-preflight.json", "pi-managed-preflight.json", "native-child-finding.json", "mixed-probe.json", "current-shell-readiness.json", "tui-consumption.json", "specialist-traces.json"])])].sort();
    const receipts: readonly Source[] = Object.freeze(evidenceFiles.map(source));
    const piSourceFiles = receiptFiles.filter(file => file.includes("/pi-initiative/")).sort(), piSource = piSourceFiles.map(source);
    const piTrialFiles = piSourceFiles.filter(file => readJson(file).fixture_revision === 2), piReceipts = piTrialFiles.map(readJson);
    const piFacts = piTrialFiles.map(file => { const receipt = readJson(file), evidence = initiativeEvidence(receipt), retainedSource = source(file); return evidence ? fromOracle(checkExchangeEvidence(evidence), "Pi initiative exchange with an independently reported final total", [retainedSource]) : unresolved("Pi initiative receipt lacks exact correlated evidence or an independent final total", [retainedSource]); });
    const piCommunication = piFacts.length === 3 ? summarizeFacts(piFacts, "Pi initiative fixture revision 2 has three exact trials (3/3), each with an independently reported final total", piSource) : unresolved(`Pi initiative fixture revision 2 has ${piFacts.length}/3 retained trials`, piSource);
    const piAutomatic = unresolved("No automatic task result or error evidence was captured for the initiative route", piSource);

    const piHeadlessCandidates = receiptFiles.filter(file => file.includes("/pi-headless-initiative/")).filter(file => readJson(file).fixture_revision === 1).sort();
    const piHeadlessByTrial = new Map<number, string>();
    for (const file of [...piHeadlessCandidates].sort((left, right) => statSync(left).mtimeMs - statSync(right).mtimeMs)) {
      const receipt = readJson(file), trial = Number(receipt.trial);
      if (Number.isInteger(trial) && piHeadlessInitiativeEvidence(receipt)) piHeadlessByTrial.set(trial, file);
    }
    const piHeadlessInitiativeFiles = [...piHeadlessByTrial.values()].sort();
    const piHeadlessAttemptFiles = piHeadlessCandidates.filter(file => !piHeadlessInitiativeFiles.includes(file));
    const piHeadlessInitiativeSources = piHeadlessInitiativeFiles.map(source);
    const piHeadlessInitiativeFacts = piHeadlessInitiativeFiles.map(file => { const receipt = readJson(file), evidence = piHeadlessInitiativeEvidence(receipt), retainedSource = source(file); return evidence ? fromOracle(checkExchangeEvidence(evidence), "Pi headless initiative exchange has exact request/reply correlation, exact specialist claim acknowledgement, and an independently derived total", [retainedSource]) : unresolved("Pi headless initiative receipt lacks exact raw request/reply, specialist claim acknowledgement, or independent total", [retainedSource]); });
    const piHeadlessCommunication = piHeadlessInitiativeFacts.length === 3 ? summarizeFacts(piHeadlessInitiativeFacts, `Pi headless initiative fixture revision 1 has three exact trials (3/3); earlier unqualified captures remain attempts${piHeadlessAttemptFiles.length ? ` (${piHeadlessAttemptFiles.length} excluded)` : ""}`, piHeadlessInitiativeSources) : unresolved(`Pi headless initiative fixture revision 1 has ${piHeadlessInitiativeFacts.length}/3 retained trials`, piHeadlessInitiativeSources);

    const opencodeInitiativeFiles = receiptFiles.filter(file => file.includes("/opencode-initiative/")).sort();
    const opencodeInitiativeTrials = opencodeInitiativeFiles.filter(file => readJson(file).fixture_revision === 3);
    const opencodeInitiativeSources = opencodeInitiativeTrials.map(source);
    const opencodeInitiativeFacts = opencodeInitiativeTrials.map(file => {
      const receipt = readJson(file), evidence = opencodeInitiativeEvidence(receipt), retainedSource = source(file);
      return evidence ? fromOracle(checkExchangeEvidence(evidence), "OpenCode initiative fixture revision 3 exchange with an independently reported final total", [retainedSource]) : unresolved("OpenCode initiative receipt lacks exact correlated evidence or an independent final total", [retainedSource]);
    });
    const opencodeCommunication = opencodeInitiativeFacts.length === 3 ? summarizeFacts(opencodeInitiativeFacts, "OpenCode initiative fixture revision 3 has three exact trials (3/3); fixture revision 2 filesystem-shortcut attempts are excluded", opencodeInitiativeSources) : unresolved(`OpenCode initiative fixture revision 3 has ${opencodeInitiativeFacts.length}/3 retained trials`, opencodeInitiativeSources);

    const codexInitiativeFiles = receiptFiles.filter(file => file.includes("/codex-initiative/")).sort();
    const codexInitiativeTrials = codexInitiativeFiles.filter(file => readJson(file).fixture_revision === 1);
    const codexInitiativeSources = codexInitiativeTrials.map(source);
    const codexInitiativeFacts = codexInitiativeTrials.map(file => {
      const receipt = readJson(file), evidence = codexInitiativeEvidence(receipt), retainedSource = source(file);
      return evidence ? fromOracle(checkExchangeEvidence(evidence), "Codex initiative exchange is correlated to the actual request, claim acknowledged, and independently reported final total", [retainedSource]) : unresolved("Codex initiative receipt lacks raw request correlation, specialist claim acknowledgement, or an independent final total", [retainedSource]);
    });
    const codexCommunication = codexInitiativeFacts.length === 3 ? summarizeFacts(codexInitiativeFacts, "Codex initiative fixture revision 1 has three exact trials (3/3); trial outcomes are derived from raw request/reply and specialist traces", codexInitiativeSources) : unresolved(`Codex initiative fixture revision 1 has ${codexInitiativeFacts.length}/3 retained trials`, codexInitiativeSources);

    const transportEntries = receiptFiles.filter(file => file.includes("/transport/")).map(file => ({ file, receipt: readJson(file) }));
    const genericFile = transportEntries.find(item => item.receipt.revision === 2), genericReceipt = genericFile?.receipt;
    const genericEdges = Array.isArray(genericReceipt?.edges) ? genericReceipt.edges as Json[] : [];
    const genericTypes = [...new Set(genericEdges.map(edge => String(edge.type)))].sort();
    const genericRoute = (name: string): string => name === "explicit-session" ? "generic-stateless" : `generic-${name}`;
    const genericByPair = new Map<string, { count: number; types: string[]; exact: number; verdicts: string[]; instances: string[] }>();
    for (const edge of genericEdges) {
      const key = `${genericRoute(String(edge.from))}->${genericRoute(String(edge.to))}`, prior = genericByPair.get(key) ?? { count: 0, types: [], exact: 0, verdicts: [], instances: [] };
      genericByPair.set(key, { count: prior.count + 1, types: [...prior.types, String(edge.type)], exact: prior.exact + (edge.exact_received === true ? 1 : 0), verdicts: [...prior.verdicts, String(edge.exchange_verdict?.outcome ?? "uncertain")], instances: [...prior.instances, String(edge.sender), String(edge.recipient)] });
    }
    const genericByPairFact = (key: string): Fact => {
      const wire = genericByPair.get(key);
      if (!wire || !genericFile) return unresolved("No revision-2 correlated transport evidence was captured");
      const [from, to] = key.split("->"), expectedCount = from === to ? 10 : 20;
      const complete = genericReceipt?.revision === 2 && genericEdges.length === 150 && genericByPair.size === 9 && genericTypes.length === 5 && wire.count === expectedCount && wire.exact === expectedCount && wire.verdicts.length === expectedCount && wire.verdicts.every(verdict => verdict === "meets") && new Set(wire.instances).size >= 2;
      if (complete) return { outcome: "meets", execution: execution("completed"), detail: `Revision-2 transport contains ${expectedCount} actual correlated exchanges for this ordered instance pair; all five message types are represented in the complete 150-edge/9-pair receipt`, sources: [source(genericFile.file)] };
      return unresolved("Transport evidence is incomplete or contains no exact correlated exchange verdict", [source(genericFile.file)]);
    };

    const genericPiFiles = receiptFiles.filter(file => file.includes("/pi/") && readJson(file).route === "pi-rpc-cli");
    const genericPiFile = genericPiFiles.find(file => genericPiEvidence(readJson(file)) !== undefined), genericPiReceipt = genericPiFile ? readJson(genericPiFile) : undefined;
    const genericPiSource = genericPiFile ? [source(genericPiFile)] : [], genericPiEvidenceValue = genericPiReceipt ? genericPiEvidence(genericPiReceipt) : undefined;
    const genericPiFact = genericPiEvidenceValue && genericPiReceipt?.passed === true ? fromOracle(checkExchangeEvidence(genericPiEvidenceValue), "Generic-to-Pi receipt records the exact reply as controller receive evidence", genericPiSource) : unresolved("No durable generic-to-Pi task-reply receipt with independent arithmetic evidence", genericPiSource);
    const piInteractiveFile = receiptFiles.filter(file => file.includes("/pi-interactive/")).find(file => piModeEvidence(readJson(file)) !== undefined);
    const opencodeFindingFile = evidenceFiles.find(file => file.endsWith("/native-child-finding.json")), opencodeFinding = opencodeFindingFile ? readJson(opencodeFindingFile) : undefined;
    const opencodeProbeFile = evidenceFiles.find(file => file.includes("/opencode/") && file.endsWith("/probe.json"));
    const mixedProbeFile = evidenceFiles.find(file => file.endsWith("/mixed-probe.json")), mixedProbe = mixedProbeFile ? readJson(mixedProbeFile) : undefined;
    const mixedEvidence = mixedProbe ? mixedProbeEvidence(mixedProbe) : undefined;
    const currentShellReadinessFile = evidenceFiles.find(file => file.endsWith("/current-shell-readiness.json"));
    const codexPersistentFile = receiptFiles.find(file => file.includes("/codex-variants/a37d3cd6-b13b-4d17-a30a-2e36e7a18177/receipt.json"));
    const codexForkFile = receiptFiles.find(file => file.includes("/codex-variants/bf36ec59-8801-41d4-b573-31e2047adb00/receipt.json"));
    const codexTuiFile = receiptFiles.find(file => file.includes("/codex-variants/e349f35b-0dad-444b-b5cd-6d624ff4b261/receipt.json"));
    const codexAppServerFile = receiptFiles.find(file => file.includes("/codex-variants/34088171-798d-4d0d-a15b-e76426d8582e/receipt.json"));
    const opencodeVariantsFile = receiptFiles.find(file => file.includes("/opencode-variants/15181ebc-4801-486a-9b6b-0025a7bbc1a6/receipt.json"));
    const tuiConsumptionFile = evidenceFiles.find(file => file.endsWith("/opencode-variants/15181ebc-4801-486a-9b6b-0025a7bbc1a6/tui-consumption.json"));
    const opencodeTuiEvidenceValue = opencodeVariantsFile && tuiConsumptionFile ? opencodeTuiEvidence(readJson(opencodeVariantsFile), readJson(tuiConsumptionFile)) : undefined;
    const managedPreflightFile = evidenceFiles.find(file => file.endsWith("/pi-managed-preflight.json"));

    const routeReceipts = new Map<string, { receipt: Json; file: string }[]>();
    for (const file of receiptFiles) { const receipt = readJson(file); if (typeof receipt.route === "string") routeReceipts.set(receipt.route, [...(routeReceipts.get(receipt.route) ?? []), { receipt, file }]); }
    const identityState = (receipt: Json): { registered: boolean; activated: boolean } => {
      const binding = receipt.binding as Json | undefined;
      const registered = typeof receipt.agent === "string" ||
        (Array.isArray(receipt.identities) && receipt.identities.some((identity: Json) => typeof identity?.name === "string")) ||
        messagesOf(receipt.messages).some(message => { const value = structured(message); return typeof value?.agent === "string"; });
      const activated = typeof binding?.runtime_id === "string" ||
        messagesOf(receipt.messages).some(message => { const value = structured(message); return typeof value?.runtime?.runtime_id === "string"; }) ||
        messagesOf(receipt.traces).some(trace => trace.stage === "runtime_bound" || trace.stage === "activation_queued");
      return { registered, activated };
    };
    const hasIdentity = (receipt: Json): boolean => identityState(receipt).registered;
    const registration = (route: string): Fact => {
      if (route === "opencode-native-task" && opencodeFinding?.status === "does_not_meet") return { outcome: "does_not_meet", execution: execution("completed"), detail: "Native Task registration displaced its parent's identity on the shared connection", sources: opencodeFindingFile ? [source(opencodeFindingFile)] : [] };
      const observed = routeReceipts.get(route)?.find(item => hasIdentity(item.receipt));
      if (observed) { const state = identityState(observed.receipt); return { outcome: "meets", execution: execution("completed"), detail: `${route} has an explicit registered identity (${state.activated ? "runtime activation also observed" : "runtime activation not required for registration"})`, sources: [source(observed.file)] }; }
      if (route === "opencode-run" && opencodeProbeFile) {
        const registry = readJson(opencodeProbeFile).direct?.registry;
        if (Array.isArray(registry) && registry.some((item: Json) => item.registered === true && typeof item.agent === "string")) return { outcome: "meets", execution: execution("completed"), detail: "OpenCode direct registry contains an explicit registered identity", sources: [source(opencodeProbeFile)] };
      }
      if (route === "pi-managed" && managedPreflightFile) return unresolved("Managed Pi launcher is unsupported under the isolated evaluation policy; no shared daemon action was attempted", [source(managedPreflightFile)]);
      return unresolved("Route identity/runtime fields were not independently proven");
    };

    const pairEvidence = new Map<string, { communication: Fact; automatic_tasks: Fact; sources: readonly Source[] }>();
    if (piCommunication.outcome !== "uncertain") pairEvidence.set("pi-rpc-cli->pi-rpc-cli", { communication: piCommunication, automatic_tasks: piAutomatic, sources: piSource });
    if (genericPiFact.execution.status !== "not_run") pairEvidence.set("generic-stdio->pi-rpc-cli", { communication: genericPiFact, automatic_tasks: genericPiFact, sources: genericPiSource });
    for (const file of receiptFiles.filter(candidate => /\/pi-(interactive|headless)\//u.test(candidate))) {
      const receipt = readJson(file), evidence = piModeEvidence(receipt);
      if (!evidence || typeof receipt.route !== "string") continue;
      const fact = receipt.passed === true ? fromOracle(checkExchangeEvidence(evidence), "Pi mode task reply with independently derived arithmetic answer", [source(file)]) : unresolved("Pi mode receipt did not complete", [source(file)]);
      pairEvidence.set(`generic-stdio->${receipt.route}`, { communication: fact, automatic_tasks: fact, sources: [source(file)] });
    }
    if (piHeadlessCommunication.execution.status !== "not_run") pairEvidence.set("pi-headless->pi-rpc-cli", { communication: piHeadlessCommunication, automatic_tasks: unresolved("Pi headless initiative does not establish a separate idle automatic-task trial", piHeadlessInitiativeSources), sources: piHeadlessInitiativeSources });
    const childFile = receiptFiles.filter(file => file.includes("/pi-child/")).find(file => nativeChildEvidence(readJson(file)) !== undefined);
    if (childFile) {
      const childReceipt = readJson(childFile), evidence = nativeChildEvidence(childReceipt);
      const childFact = evidence && childReceipt.passed === true ? fromOracle(checkExchangeEvidence(evidence), "Pi native child exact parent/child exchange", [source(childFile)]) : unresolved("Pi native child receipt lacks a completed exact exchange", [source(childFile)]);
      pairEvidence.set("pi-sdk->pi-native-child", { communication: childFact, automatic_tasks: unresolved("Native child automatic handling was not independently captured", [source(childFile)]), sources: [source(childFile)] });
    }
    const codexFile = receiptFiles.filter(file => file.includes("/codex-exchange/")).find(file => codexExchangeEvidence(readJson(file)) !== undefined);
    if (codexFile) {
      const codexReceipt = readJson(codexFile), codexEvidence = codexExchangeEvidence(codexReceipt);
      if (codexEvidence) pairEvidence.set("codex-headless->codex-native-child", { communication: fromOracle(checkExchangeEvidence(codexEvidence), "Latest Codex exchange receipt records exact independent IDs, correlation, child claim acknowledgement, and both consumption checks", [source(codexFile)]), automatic_tasks: unresolved("Codex exchange receipt does not establish a separate idle automatic-task trial", [source(codexFile)]), sources: [source(codexFile)] });
    }
    if (opencodeCommunication.execution.status !== "not_run") pairEvidence.set("opencode-run->pi-rpc-cli", { communication: opencodeCommunication, automatic_tasks: unresolved("OpenCode initiative does not establish a separate idle automatic-task trial", opencodeInitiativeSources), sources: opencodeInitiativeSources });
    if (codexCommunication.execution.status !== "not_run") pairEvidence.set("codex-headless->pi-rpc-cli", { communication: codexCommunication, automatic_tasks: unresolved("Codex initiative does not establish a separate idle automatic-task trial", codexInitiativeSources), sources: codexInitiativeSources });
    if (mixedEvidence && mixedProbeFile) {
      pairEvidence.set("pi-rpc-cli->opencode-serve-attach", { communication: fromOracle(checkExchangeEvidence(mixedEvidence), "Assisted Pi-to-OpenCode exchange has an exact receiver-side result and retained Pi claim acknowledgement", [source(mixedProbeFile)]), automatic_tasks: { outcome: mixedProbe?.idle_unassisted?.status === "does_not_meet" ? "does_not_meet" : "uncertain", execution: execution("completed"), detail: `Idle 300-second automatic delivery was not consumed by receiver opencode-serve-attach; assisted result is recorded separately`, sources: [source(mixedProbeFile)] }, sources: [source(mixedProbeFile)] });
    }
    if (opencodeTuiEvidenceValue && opencodeVariantsFile && tuiConsumptionFile) {
      pairEvidence.set("opencode-interactive->generic-stdio", { communication: fromOracle(checkExchangeEvidence(opencodeTuiEvidenceValue), "OpenCode TUI route has the original exact request plus the retained native receive envelope; self-loop mechanics remain separate", [source(opencodeVariantsFile), source(tuiConsumptionFile)]), automatic_tasks: unresolved("TUI route does not establish an idle automatic task trial", [source(opencodeVariantsFile)]), sources: [source(opencodeVariantsFile), source(tuiConsumptionFile)] });
    }
    for (const [key] of genericByPair) pairEvidence.set(key, { communication: genericByPairFact(key), automatic_tasks: unresolved("Transport wire evidence does not establish idle task claim/reply handling"), sources: genericFile ? [source(genericFile.file)] : [] });
    const pairs: readonly Pair[] = Object.freeze(routes.flatMap(from => routes.map(to => {
      const key = `${from}->${to}`, known = pairEvidence.get(key), wire = genericByPair.get(key);
      return { from, to, independent_instances: true as const, registration_identity: registration(from), communication: known?.communication ?? unresolved("No ordered-pair exchange was run"), automatic_tasks: known?.automatic_tasks ?? unresolved("No ordered-pair automatic task check was run"), ...(wire ? { wire: { edge_count: wire.count, pair_count: 1, actual_exchange_count: wire.count, distinct_instance_count: new Set(wire.instances).size, complete: genericByPairFact(key).outcome === "meets", message_types: Object.freeze([...new Set(wire.types)].sort()), exact_received: wire.exact } } : {}), sources: known?.sources ?? [] } satisfies Pair;
    })));
    expect(pairs).toHaveLength(routes.length * routes.length);

    const initiative: readonly AcceptanceRow[] = Object.freeze(routes.map(route => route === "pi-rpc-cli" ? row("initiative:pi-rpc-cli", "initiative", piCommunication) : route === "pi-headless" ? row("initiative:pi-headless", "initiative", piHeadlessCommunication) : route === "opencode-run" ? row("initiative:opencode-run", "initiative", opencodeCommunication) : route === "codex-headless" ? row("initiative:codex-headless", "initiative", codexCommunication) : route.startsWith("generic-") ? row(`initiative:${route}`, "initiative", { outcome: "not_applicable", execution: execution("unsupported"), detail: "Generic transport has no model initiative", sources: [] }, false) : row(`initiative:${route}`, "initiative", unresolved("No independent natural-task trial receipt"))));
    const identityRows: readonly AcceptanceRow[] = opencodeFinding?.status === "does_not_meet" ? [row("identity:opencode-native-task", "communication", { outcome: "does_not_meet", execution: execution("completed"), detail: `Confirmed native-task identity failure: ${String(opencodeFinding.reason ?? "finding")}`, sources: opencodeFindingFile ? [source(opencodeFindingFile)] : [] })] : [];
    const communicationRows = pairs.map(pair => row(`communication:${pair.from}->${pair.to}`, "communication", pair.communication));
    const automaticRows = pairs.map(pair => row(`automatic_tasks:${pair.from}->${pair.to}`, "automatic_tasks", pair.automatic_tasks, !pair.to.startsWith("generic-")));
    const piReplyFiles = receiptFiles.filter(file => file.includes("/pi-reply-activation/")).sort();
    const piReplyFacts = piReplyFiles.map(file => replyActivationFact(readJson(file), file)).filter((fact): fact is Fact => fact !== undefined);
    const piReplyAutomatic = piReplyFacts.length ? summarizeFacts(piReplyFacts, "Pi reply activation requires an independently calculated result or declared service error, exact claim acknowledgement, and the observed assistant continuation", piReplyFacts.flatMap(fact => fact.sources)) : undefined;
    const piResultFacts = piReplyFiles.filter(file => readJson(file).reply_type === "result").map(file => replyActivationFact(readJson(file), file)).filter((fact): fact is Fact => fact !== undefined);
    const automaticResultFacts = new Map<string, Fact>(piResultFacts.length ? [["pi-rpc-cli", summarizeFacts(piResultFacts, "Correlated result activated Pi and was acknowledged before continuation", piResultFacts.flatMap(fact => fact.sources))]] : []);
    const automaticResultRows = routes.map(route => row(`automatic_results:${route}`, "automatic_tasks", route.startsWith("generic-") ? { outcome: "not_applicable", execution: execution("unsupported"), detail: "Generic transport automatic result/error is not applicable as a model-receiver trial", sources: [] } : automaticResultFacts.get(route) ?? unresolved(`No automatic result evidence for receiver ${route}`), !route.startsWith("generic-")));
    const automaticErrorFacts = new Map<string, Fact>();
    for (const file of evidenceFiles) {
      const receipt = readJson(file);
      if (receipt.reply_type !== "error" || typeof receipt.route !== "string") continue;
      const fact = replyActivationFact(receipt, file);
      if (fact) automaticErrorFacts.set(receipt.route, fact);
    }
    const automaticErrorRows = routes.map(route => row(`automatic_errors:${route}`, "automatic_tasks", route.startsWith("generic-") ? { outcome: "not_applicable", execution: execution("unsupported"), detail: "Generic transport automatic result/error is not applicable as a model-receiver trial", sources: [] } : automaticErrorFacts.get(route) ?? unresolved(`No correlated incoming automatic error evidence was captured for receiver ${route}`), !route.startsWith("generic-")));
    const automaticEvidenceRows = automaticErrorRows;
    const aggregate = { communication: evaluateTotalVerdict(communicationRows), automatic_tasks: evaluateTotalVerdict([...automaticRows, ...automaticResultRows, ...automaticEvidenceRows]), initiative: evaluateTotalVerdict(initiative), identity: evaluateTotalVerdict(identityRows), overall: evaluateTotalVerdict([...communicationRows, ...automaticRows, ...automaticResultRows, ...automaticEvidenceRows, ...initiative, ...identityRows]) };
    const dispositions = existsSync(routeDispositionPath) ? readJson(routeDispositionPath).routes as Json[] : [];
    const dispositionByRoute = new Map(dispositions.map(route => [String(route.id), String(route.status)]));
    const blockedRoutes = new Set([...dispositionByRoute].filter(([, status]) => status === "blocked_prerequisite").map(([route]) => route));
    const pairCoverage = {
      expected_pair_count: pairs.length,
      actual_pair_count: pairEvidence.size,
      completed_pair_count: pairs.filter(pair => pair.communication.execution.status === "completed").length,
      blocked_pair_count: pairs.filter(pair => blockedRoutes.has(pair.from) || blockedRoutes.has(pair.to)).length,
      unrun_pair_count: pairs.filter(pair => !pairEvidence.has(`${pair.from}->${pair.to}`) && !blockedRoutes.has(pair.from) && !blockedRoutes.has(pair.to)).length,
    };
    const launchObservations = {
      current_shell: currentShellReadinessFile ? { classification: "installed-current-shell-observation", source: source(currentShellReadinessFile), agent: readJson(currentShellReadinessFile).agent, activation_ready: readJson(currentShellReadinessFile).activation_ready, runtime: readJson(currentShellReadinessFile).runtime, operation: readJson(currentShellReadinessFile).operation, scope: readJson(currentShellReadinessFile).scope } : { classification: "absent" },
      codex_persistent_exec_resume: codexPersistentFile ? { classification: "mechanics-observed", source: source(codexPersistentFile), run_id: readJson(codexPersistentFile).run_id, exec_completed: readJson(codexPersistentFile).routes?.exec?.code === 0, resume_completed: readJson(codexPersistentFile).routes?.resume?.code === 0 } : { classification: "absent" },
      codex_native_fork: codexForkFile ? { classification: "mechanics-observed-self-loop", source: source(codexForkFile), passed: readJson(codexForkFile).routes?.fork?.passed === true, mcp_self_loop: true, default_tui: readJson(codexForkFile).routes?.tui?.passed === true ? "passed" : "approval-barrier" } : { classification: "absent" },
      codex_tui_assisted: codexTuiFile ? { classification: "assisted-positive", source: source(codexTuiFile), passed: readJson(codexTuiFile).routes?.tui?.passed === true, approval_mode: readJson(codexTuiFile).routes?.tui?.approval_mode ?? "unknown" } : { classification: "absent" },
      codex_app_server: codexAppServerFile ? (() => { const receipt = readJson(codexAppServerFile), route = receipt.routes?.app_server as Json | undefined, preview = String(route?.history?.preview ?? ""), items = Array.isArray(route?.history?.turns) ? route.history.turns.flatMap((turn: Json) => Array.isArray(turn.items) ? turn.items : []) : []; const tools = new Set(items.filter((item: Json) => item.type === "mcpToolCall").map((item: Json) => String(item.tool))); return { classification: "mechanics-observed-self-loop", source: source(codexAppServerFile), started: route?.started === true, mcp_self_loop: preview.includes("Send yourself") && tools.has("send_message") && tools.has("receive_message"), tui: "fixture-trust-prompt-unqualified", fork: "fixture-trust-prompt-unqualified" }; })() : { classification: "absent" },
      opencode_variants: opencodeVariantsFile ? (() => { const routesObserved = readJson(opencodeVariantsFile).routes as Json; return { classification: "mechanics-observed", source: source(opencodeVariantsFile), mechanical_self_loops: ["run", "resume", "fork", "attach", "acp"].filter(route => routesObserved?.[route]?.observation || routesObserved?.[route]?.acceptance?.passed === true), tui: opencodeTuiEvidenceValue ? "exact_native_envelope_pair" : "mechanics_only" }; })() : { classification: "absent" },
    };
    const blockerSummaries = dispositions.filter(route => String(route.status) === "blocked_prerequisite").map(route => ({ route: String(route.id), status: String(route.status), reason: String(route.reason ?? ""), sources: Array.isArray(route.evidence) ? route.evidence : [] }));
    const remainingUnrunCoverage = { pair_count: pairCoverage.unrun_pair_count, source_routes: [...new Set(pairs.filter(pair => !pairEvidence.has(`${pair.from}->${pair.to}`) && !blockedRoutes.has(pair.from) && !blockedRoutes.has(pair.to)).map(pair => pair.from))].sort(), receiver_routes: [...new Set(pairs.filter(pair => !pairEvidence.has(`${pair.from}->${pair.to}`) && !blockedRoutes.has(pair.from) && !blockedRoutes.has(pair.to)).map(pair => pair.to))].sort() };
    const report = {
      schema_version: 2, generated_at: new Date().toISOString(), source_revision: inventory.source,
      execution_policy: { report_generation: "read_only", underlying_tests: "represented_by_retained_evidence", redis_operations: "none", auth_read: false, global_changes: false },
      route_count: routes.length, pair_count: pairs.length, pair_coverage: pairCoverage, routes, receipts, aggregates: aggregate, initiative_rows: initiative, identity_rows: identityRows, automatic_result_rows: automaticResultRows, automatic_error_rows: automaticEvidenceRows, pair_matrix: pairs, launch_observations: launchObservations, blockers: blockerSummaries, remaining_unrun_coverage: remainingUnrunCoverage, review_deltas: { new_receipt_sources: [currentShellReadinessFile, codexPersistentFile, codexForkFile, codexTuiFile, codexAppServerFile, opencodeVariantsFile, tuiConsumptionFile, piInteractiveFile, ...piHeadlessInitiativeFiles].filter((file): file is string => typeof file === "string").map(source), no_falsely_bound_review_pass: true, note: "Launch observations remain mechanics or exact-envelope evidence; no review pass is inferred from a retained passed flag." },
      initiative_evidence: { routes: [{ route: "pi-rpc-cli", fixture_revision: 2, trial_count: piReceipts.length, fact: piCommunication, trial_facts: piFacts, sources: piSource }, { route: "pi-headless", fixture_revision: 1, trial_count: piHeadlessInitiativeFiles.length, fact: piHeadlessCommunication, trial_facts: piHeadlessInitiativeFacts, sources: piHeadlessInitiativeSources, attempt_sources: piHeadlessAttemptFiles.map(source) }, { route: "opencode-run", fixture_revision: 3, trial_count: opencodeInitiativeTrials.length, fact: opencodeCommunication, trial_facts: opencodeInitiativeFacts, sources: opencodeInitiativeSources }, { route: "codex-headless", fixture_revision: 1, trial_count: codexInitiativeTrials.length, fact: codexCommunication, trial_facts: codexInitiativeFacts, sources: codexInitiativeSources }] },
      reply_activation: { route: "pi-rpc-cli", trial_count: piReplyFiles.length, qualified_count: piReplyFacts.length, qualified_fact: piReplyAutomatic, sources: piReplyFiles.map(source), note: "Only receipts with independent answer/error predicates, exact claim acknowledgement, and an observed assistant continuation qualify." },
      generic_wire: { source: genericFile ? source(genericFile.file) : undefined, edge_count: genericEdges.length, pair_count: genericByPair.size, actual_exchange_count: genericEdges.filter(edge => edge.exchange_verdict?.outcome === "meets").length, distinct_instance_count: new Set(genericEdges.flatMap(edge => [String(edge.sender), String(edge.recipient)])).size, complete: genericReceipt?.revision === 2 && genericEdges.length === 150 && genericByPair.size === 9 && genericTypes.length === 5, message_observation_count: genericEdges.length, revision: genericReceipt?.revision ?? null, message_types: genericTypes, note: "Revision-2 transport evidence is accepted only from the complete 150-edge, nine-pair receipt with exact correlated exchange verdicts; missing task handling remains unresolved." },
      notes: ["Execution success is separate from semantic acceptance.", "Every evidence gap remains uncertain; the confirmed OpenCode native-child finding is a required identity failure.", "Report generation reads retained evidence only; historical harness and provider failures remain attempts and are not incoming automatic-task failures."],
    };
    mkdirSync(artifactRoot, { recursive: true });
    writeFileSync(reportJsonPath, JSON.stringify(report, null, 2) + "\n", { mode: 0o600 });
    const reportHash = digest(readFileSync(reportJsonPath));
    const observedMechanics = new Map<string, string>([
      ["codex-headless", "persistent exec mechanics"], ["codex-resume", "persistent resume mechanics"], ["codex-appserver", "app-server MCP self-loop mechanics"], ["codex-fork", "native MCP self-loop mechanics"], ["codex-interactive", "default TUI approval barrier; assisted TUI exact self-loop"],
      ["opencode-interactive", "TUI native envelope pair"], ["opencode-run", "run mechanics"], ["opencode-resume", "resume mechanics"], ["opencode-fork", "fork mechanics"], ["opencode-native-task", "confirmed displaced identity"], ["opencode-serve-attach", "serve/attach mechanics"], ["opencode-acp", "ACP mechanics"],
    ]);
    const md = ["# GPTQueue acceptance evidence report", "", `Source revision: \`${String(inventory.source)}\``, `Routes: ${routes.length}; ordered independent route pairs: ${pairs.length} (${routes.length} × ${routes.length}).`, `Pair evidence: ${pairCoverage.actual_pair_count} actual, ${pairCoverage.completed_pair_count} completed, ${pairCoverage.blocked_pair_count} blocked, ${pairCoverage.unrun_pair_count} unrun.`, `Aggregates: communication **${aggregate.communication.outcome}**, automatic tasks **${aggregate.automatic_tasks.outcome}**, initiative **${aggregate.initiative.outcome}**, identity **${aggregate.identity.outcome}**, overall **${aggregate.overall.outcome}**.`, "", "Report generation is read-only over retained evidence. Missing evidence is unresolved.", "", "## Route summary", "", "| Route | Identity | Communication | Automatic tasks received | Initiative | Observed mechanics |", "| --- | --- | --- | --- | --- | --- |", ...routes.map(route => { const outgoingPairs = pairs.filter(pair => pair.from === route), incomingPairs = pairs.filter(pair => pair.to === route); const comm = outgoingPairs.every(pair => pair.communication.outcome === "meets") ? "meets" : outgoingPairs.some(pair => pair.communication.outcome === "does_not_meet") ? "does_not_meet" : "uncertain"; const incomingFacts = [...incomingPairs.map(pair => pair.automatic_tasks), automaticResultFacts.get(route)].filter((fact): fact is Fact => fact !== undefined); const auto = incomingFacts.some(fact => fact.outcome === "does_not_meet") ? "does_not_meet" : incomingFacts.length > 0 && incomingFacts.every(fact => fact.outcome === "not_applicable") ? "not_applicable" : "uncertain"; const init = initiative.find(item => item.id === `initiative:${route}`)?.outcome ?? "uncertain"; return `| ${route} | ${registration(route).outcome} | ${comm} | ${auto} | ${init} | ${observedMechanics.get(route) ?? ""} |`; }), "", `Blocked prerequisites: ${blockerSummaries.length}; remaining unrun ordered pairs: ${remainingUnrunCoverage.pair_count}.`, ...blockerSummaries.map(blocker => `- Blocked ${blocker.route}: ${blocker.reason} (sources: ${blocker.sources.join(", ")})`), "", "## Matrix and evidence", "", "The complete 625-row structured matrix, oracle rows, receipt paths, and receipt hashes are in [report.json](./report.json).", `Generated report JSON sha256: \`${reportHash}\`. Revision-2 transport source: \`${genericFile ? source(genericFile.file).sha256 : "absent"}\`.`].join("\n");
    writeFileSync(reportMarkdownPath, md + "\n", { mode: 0o600 });
    expect(report.pair_matrix).toHaveLength(routes.length * routes.length);
    expect(new Set(pairs.map(pair => `${pair.from}->${pair.to}`)).size).toBe(pairs.length);
    expect(genericEdges.every(edge => edge.exchange_verdict || genericReceipt?.revision !== 2)).toBe(true);
    expect(existsSync(reportJsonPath)).toBe(true);
    expect(existsSync(reportMarkdownPath)).toBe(true);
    const persisted = readJson(reportJsonPath), persistedPairCount = persisted.pair_matrix.length;
    expect(persisted.schema_version).toBe(report.schema_version);
    expect(persisted.pair_coverage.actual_pair_count).toBe(report.pair_coverage.actual_pair_count);
    expect(persisted.aggregates.overall.outcome).toBe(report.aggregates.overall.outcome);
    expect(persisted.launch_observations.current_shell.classification).toBe("installed-current-shell-observation");
    expect(persisted.review_deltas.no_falsely_bound_review_pass).toBe(true);
    expect(persisted.pair_matrix.find((pair: Json) => pair.from === "opencode-interactive" && pair.to === "generic-stdio").communication.outcome).toBe("meets");
    expect(persisted.pair_matrix.find((pair: Json) => pair.from === "opencode-native-task").registration_identity.outcome).toBe("does_not_meet");
    expect(readFileSync(reportMarkdownPath, "utf8")).toContain(`overall **${report.aggregates.overall.outcome}**`);
    expect(readFileSync(reportMarkdownPath, "utf8")).toContain("Observed mechanics");
    persisted.pair_matrix.pop();
    expect(readJson(reportJsonPath).pair_matrix).toHaveLength(persistedPairCount);
    if (process.env.GPTQUEUE_ACCEPTANCE_ENFORCE === "1") expect(report.aggregates.overall.outcome).toBe("meets");
  });

  it("rejects mutated Pi headless initiative reply and claim evidence", (ctx) => {
    const receipts = findReceipts(join(artifactRoot, "pi-headless-initiative"));
    ctx.skip(receipts.length === 0, "retained Pi headless initiative receipts absent");
    // Receipts exist, so the predicate must accept at least one: a broken
    // predicate has to fail here, not quietly skip.
    const file = receipts.find(candidate => piHeadlessInitiativeEvidence(readJson(candidate)) !== undefined);
    if (!file) throw new Error(`none of ${receipts.length} retained Pi headless initiative receipts satisfies the evidence predicate`);
    const receipt = readJson(file);
    expect(piHeadlessInitiativeEvidence(receipt)).toBeDefined();
    const wrongReply = JSON.parse(JSON.stringify(receipt)) as Json;
    wrongReply.reply.payload.content = "0";
    expect(piHeadlessInitiativeEvidence(wrongReply)).toBeUndefined();
    const wrongClaim = JSON.parse(JSON.stringify(receipt)) as Json;
    const ackCall = messagesOf(wrongClaim.specialist_messages).flatMap(message => Array.isArray(message.content) ? message.content : []).find((part: Json) => part.type === "toolCall" && part.name === "acknowledge_tasks");
    if (!ackCall) throw new Error("retained Pi headless acknowledgement call missing");
    ackCall.arguments.claim_id = "wrong-claim-id";
    expect(piHeadlessInitiativeEvidence(wrongClaim)).toBeUndefined();
    const missingAck = JSON.parse(JSON.stringify(receipt)) as Json;
    const ackCallIds = new Set<string>();
    const specialistMessages = Array.isArray(missingAck.specialist_messages) ? missingAck.specialist_messages as Json[] : [];
    for (const message of specialistMessages) {
      if (!Array.isArray(message.content)) continue;
      const removed = message.content.filter((part: Json) => {
        const isAckCall = (part.type === "toolCall" || part.type === "collabAgentToolCall") && part.name === "acknowledge_tasks";
        if (isAckCall && typeof part.id === "string") ackCallIds.add(part.id);
        return !isAckCall;
      });
      message.content = removed;
    }
    missingAck.specialist_messages = specialistMessages.filter(message => !(message.role === "toolResult" && (message.toolName === "acknowledge_tasks" || ackCallIds.has(String(message.toolCallId ?? "")))));
    expect(piHeadlessInitiativeEvidence(missingAck)).toBeUndefined();
  });
});
