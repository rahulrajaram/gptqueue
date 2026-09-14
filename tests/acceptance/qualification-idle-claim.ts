import type { ExchangeEvidence, MessageEvidence } from "./oracle.js";
import type { NativeTrace } from "./qualification-evidence.js";
import type { ParticipantIdentity } from "./qualification-types.js";

type Json = Record<string, unknown>;

export type IdleClaimExchangeInput = Readonly<{
  peer: ParticipantIdentity;
  model: ParticipantIdentity;
  genericTraces: readonly NativeTrace[];
  nativeTraces: readonly NativeTrace[];
  postStimulusCallIds: readonly string[];
  nonce: string;
  requestContent: string;
  expectedReplyContent: string;
}>;

const object = (value: unknown): Json | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;

const parse = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value) as unknown; } catch { return value; }
};

const result = (trace: NativeTrace): Json | undefined => object(trace.output);
const input = (trace: NativeTrace): Json | undefined => object(trace.input);
const payloadOf = (value: Json): unknown => value.payload ?? value;
const payload = (value: Json): Json | undefined => object(payloadOf(value));

const sameIdentity = (left: ParticipantIdentity, right: ParticipantIdentity): boolean =>
  left.participantId === right.participantId && left.route === right.route &&
  left.hostRuntimeId === right.hostRuntimeId && left.agent === right.agent &&
  left.cwdHash === right.cwdHash && left.profileHash === right.profileHash &&
  left.epochHash === right.epochHash;

const exactTrace = (trace: NativeTrace | undefined, actor: ParticipantIdentity, name: string): NativeTrace => {
  if (!trace || trace.name !== name || trace.successful !== true || trace.runtimeBound !== true ||
      trace.runtimeId !== actor.hostRuntimeId || !sameIdentity(trace.actor, actor)) {
    throw new Error(`missing exact successful runtime-bound ${name} trace`);
  }
  return trace;
};

const one = (traces: readonly NativeTrace[], predicate: (trace: NativeTrace) => boolean, detail: string): NativeTrace => {
  const matches = traces.filter(predicate);
  if (matches.length !== 1) throw new Error(`${detail}: expected one exact trace, found ${matches.length}`);
  return matches[0]!;
};

const oneStable = (traces: readonly NativeTrace[], predicate: (trace: NativeTrace) => boolean, key: (trace: NativeTrace) => string, detail: string): NativeTrace => {
  const matches = traces.filter(predicate);
  const distinct = new Map(matches.map((trace) => [key(trace), trace]));
  if (distinct.size !== 1) throw new Error(`${detail}: expected one exact identity, found ${distinct.size}`);
  return matches[0]!;
};

const eligible = (trace: NativeTrace, actor: ParticipantIdentity, name: string): boolean =>
  trace.name === name && trace.successful === true && trace.runtimeBound === true &&
  trace.runtimeId === actor.hostRuntimeId && sameIdentity(trace.actor, actor);

const envelope = (value: unknown): Json => {
  const message = object(parse(value));
  if (!message || typeof message.id !== "string" || message.id.length === 0 ||
      typeof message.from !== "string" || typeof message.to !== "string" ||
      !["task", "result"].includes(String(message.type))) {
    throw new Error("message envelope is not exact");
  }
  return message;
};

const messageContent = (message: Json): string => {
  const value = payload(message)?.content;
  if (typeof value !== "string") throw new Error("message payload has no exact content");
  return value;
};

const claimFor = (traces: readonly NativeTrace[], messageId: string, actor: ParticipantIdentity): NativeTrace =>
  oneStable(traces, (trace) => {
    if (trace.name !== "claim_tasks" || trace.successful !== true || trace.runtimeBound !== true ||
        trace.runtimeId !== actor.hostRuntimeId || !sameIdentity(trace.actor, actor)) return false;
    const output = result(trace), claim = object(output?.claim);
    if (output?.status !== "ok" || output.claimed !== true || claim?.actor_id !== actor.agent ||
        typeof claim.claim_id !== "string" || claim.claim_id.length === 0 || !Array.isArray(claim.tasks)) return false;
    return claim.tasks.some((task) => {
      try { return envelope(task).id === messageId; } catch { return false; }
    });
  }, (trace) => String(object(result(trace)?.claim)?.claim_id), `native claim for ${messageId}`);

const claimedMessage = (trace: NativeTrace, messageId: string): Json => {
  const claim = object(result(trace)?.claim);
  const tasks = Array.isArray(claim?.tasks) ? claim.tasks : [];
  const task = tasks.map(parse).find((candidate) => {
    try { return envelope(candidate).id === messageId; } catch { return false; }
  });
  if (task === undefined) throw new Error(`claim does not contain ${messageId}`);
  return envelope(task);
};

export const collectIdleClaimExchange = (inputValue: IdleClaimExchangeInput): ExchangeEvidence => {
  if (inputValue.nonce.length === 0) throw new Error("nonce must be non-empty");
  if (inputValue.peer.participantId === inputValue.model.participantId || inputValue.peer.agent === inputValue.model.agent || sameIdentity(inputValue.peer, inputValue.model)) throw new Error("peer and model identities must be distinct");
  const postStimulus = new Set(inputValue.postStimulusCallIds);
  const generic = inputValue.genericTraces;
  const native = inputValue.nativeTraces;

  const genericSend = exactTrace(oneStable(generic, (trace) => {
    const request = input(trace), output = result(trace);
    return eligible(trace, inputValue.peer, "send_message") && request?.to === inputValue.model.agent &&
      request.type === "task" && request.content === inputValue.requestContent &&
      typeof request.content === "string" && request.content.includes(inputValue.nonce) &&
      output?.status === "sent" && typeof output.message_id === "string" && output.message_id.length > 0 &&
      output.to === inputValue.model.agent;
  }, (trace) => String(result(trace)?.message_id), "generic request send"), inputValue.peer, "send_message");
  const requestId = String(result(genericSend)?.message_id);

  const claim = claimFor(native, requestId, inputValue.model);
  if (!postStimulus.has(claim.sourceId)) throw new Error("request claim was outside the post-stimulus call set");
  const request = claimedMessage(claim, requestId);
  const requestPayload = messageContent(request);
  if (request.from !== inputValue.peer.agent || request.to !== inputValue.model.agent || request.type !== "task" ||
      requestPayload !== inputValue.requestContent || !requestPayload.includes(inputValue.nonce)) {
    throw new Error("claimed request envelope failed exact identity/content checks");
  }
  const claimId = String(object(result(claim)?.claim)?.claim_id);

  const nativeSend = exactTrace(oneStable(native, (trace) => {
    const requestValue = input(trace), output = result(trace);
    return eligible(trace, inputValue.model, "send_message") && postStimulus.has(trace.sourceId) &&
      requestValue?.to === inputValue.peer.agent && requestValue.type === "result" &&
      requestValue.content === inputValue.expectedReplyContent && requestValue.in_reply_to === requestId &&
      output?.status === "sent" && output.to === inputValue.peer.agent &&
      typeof output.message_id === "string" && output.message_id.length > 0 && output.message_id !== requestId;
  }, (trace) => String(result(trace)?.message_id), "native correlated reply send"), inputValue.model, "send_message");
  const replyId = String(result(nativeSend)?.message_id);

  const acknowledgement = exactTrace(oneStable(native, (trace) => {
    const requestValue = input(trace), output = result(trace);
    return eligible(trace, inputValue.model, "acknowledge_tasks") && postStimulus.has(trace.sourceId) &&
      requestValue?.claim_id === claimId && output?.status === "ok" && Number(output.acknowledged) > 0;
  }, (trace) => String(input(trace)?.claim_id), "native request acknowledgement"), inputValue.model, "acknowledge_tasks");

  const genericReceive = exactTrace(oneStable(generic, (trace) => {
    const output = result(trace), message = object(output?.message);
    if (!eligible(trace, inputValue.peer, "receive_message") || output?.status !== "message" || !message) return false;
    try {
      return message.id === replyId && message.from === inputValue.model.agent && message.to === inputValue.peer.agent &&
        message.type === "result" && payload(message)?.in_reply_to === requestId && messageContent(message) === inputValue.expectedReplyContent;
    } catch { return false; }
  }, (trace) => String(object(result(trace)?.message)?.id), "generic reply receive"), inputValue.peer, "receive_message");
  void acknowledgement;
  void genericReceive;

  const requestEvidence: MessageEvidence = { id: requestId, from: request.from as string, to: request.to as string, content: requestPayload };
  const replyEvidence: MessageEvidence = { id: replyId, from: inputValue.model.agent, to: inputValue.peer.agent, in_reply_to: requestId, content: inputValue.expectedReplyContent };
  return Object.freeze({
    sender: inputValue.peer.agent,
    recipient: inputValue.model.agent,
    request: Object.freeze(requestEvidence),
    reply: Object.freeze(replyEvidence),
    expected_reply_content: inputValue.expectedReplyContent,
    prior_message_ids: Object.freeze([]),
    request_consumption: Object.freeze({ message_id: requestId, actor: inputValue.model.agent, consumed: true, claim_id: claimId, acknowledged: true }),
    reply_consumption: Object.freeze({ message_id: replyId, actor: inputValue.peer.agent, consumed: true, acknowledged: false }),
    request_requires_ack: true,
    reply_requires_ack: false,
    execution: Object.freeze({ status: "completed" as const }),
  });
};
