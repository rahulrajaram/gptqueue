import type { NativeTrace } from "./qualification-evidence.js";
import type { ParticipantIdentity } from "./qualification-types.js";

type Json = Record<string, unknown>;
type ReplyType = "result" | "error";

export type IdleReplyContinuationInput = Readonly<{
  model: ParticipantIdentity;
  peer: ParticipantIdentity;
  baselineTurnIds: readonly string[];
  originatingTurnId: string;
  nativeHistory: unknown;
  genericTraces: readonly NativeTrace[];
  nativeTraces: readonly NativeTrace[];
  requestContent: string;
  replyContent: string;
  continuation: string;
  replyType: ReplyType;
  nonce: string;
}>;

export type IdleReplyContinuationResult = Readonly<{
  requestId: string;
  replyId: string;
  claimId: string;
  continuationTurnId: string;
  assistantContent: string;
  replyType: ReplyType;
}>;

const object = (value: unknown): Json | undefined => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const parse = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value) as unknown; } catch { return value; }
};
const inputOf = (trace: NativeTrace): Json => trace.input ?? {};
const outputOf = (trace: NativeTrace): Json => trace.output ?? {};
const sameIdentity = (left: ParticipantIdentity, right: ParticipantIdentity): boolean =>
  left.participantId === right.participantId && left.route === right.route && left.hostRuntimeId === right.hostRuntimeId &&
  left.agent === right.agent && left.cwdHash === right.cwdHash && left.profileHash === right.profileHash && left.epochHash === right.epochHash;
const eligible = (trace: NativeTrace, actor: ParticipantIdentity, name: string): boolean =>
  trace.name === name && trace.successful === true && trace.runtimeBound === true && trace.runtimeId === actor.hostRuntimeId && sameIdentity(trace.actor, actor);
const payloadOf = (value: Json): Json | undefined => object(value.payload) ?? value;
const contentOf = (value: Json): string | undefined => {
  const payload = payloadOf(value);
  return typeof payload?.content === "string" ? payload.content : undefined;
};
const claimTask = (trace: NativeTrace, messageId: string): Json | undefined => {
  const claim = object(outputOf(trace).claim);
  if (outputOf(trace).status !== "ok" || outputOf(trace).claimed !== true || claim?.actor_id !== trace.actor.agent || typeof claim.claim_id !== "string" || !Array.isArray(claim.tasks)) return undefined;
  return claim.tasks.map(parse).map(object).find(task => task?.id === messageId);
};
const historyTurns = (history: unknown): readonly Json[] => {
  const root = object(history), turns = root?.turns;
  if (!Array.isArray(turns)) throw new Error("native history has no turns");
  if (typeof root?.id !== "string" || root.id.length === 0) throw new Error("native history has no exact root ID");
  if (turns.some(turn => typeof object(turn)?.id !== "string" || String(object(turn)?.id).length === 0) || new Set(turns.map(turn => object(turn)?.id)).size !== turns.length) throw new Error("native turn IDs must be nonempty and unique");
  return turns.map(object).filter((turn): turn is Json => turn !== undefined);
};
const requiredTurn = (turns: readonly Json[], id: string): Json => {
  const turn = turns.find(candidate => candidate.id === id);
  if (!turn || typeof turn.id !== "string") throw new Error(`native turn ${id} is missing`);
  return turn;
};
const turnStatus = (turn: Json): string => String(turn.status ?? "").toLowerCase();
const itemIds = (turn: Json): Set<string> => new Set((Array.isArray(turn.items) ? turn.items : []).map(object).filter((item): item is Json => item?.type === "mcpToolCall" && typeof item.id === "string").map(item => String(item.id)));
const assistantMessages = (turn: Json): readonly Readonly<{ text: string; phase?: string }>[] => (Array.isArray(turn.items) ? turn.items : []).map(object).filter((item): item is Json => item?.type === "agentMessage" || item?.type === "AgentMessage").flatMap(item => {
  const phase = typeof item.phase === "string" ? item.phase : undefined;
  if (typeof item.text === "string") return [{ text: item.text, ...(phase === undefined ? {} : { phase }) }];
  if (typeof item.content === "string") return [{ text: item.content, ...(phase === undefined ? {} : { phase }) }];
  if (!Array.isArray(item.content)) return [];
  return item.content.map(object).filter((part): part is Json => typeof part?.text === "string").map(part => ({ text: String(part.text), ...(phase === undefined ? {} : { phase }) }));
});
const postOriginTurn = (turns: readonly Json[], originatingTurnId: string, baselineTurnIds: readonly string[]): Json => {
  const originIndex = turns.findIndex(turn => turn.id === originatingTurnId);
  if (originIndex < 0) throw new Error("originating turn is missing");
  const fresh = turns.slice(originIndex + 1).filter(turn => typeof turn.id === "string" && !baselineTurnIds.includes(String(turn.id)));
  const completed = fresh.filter(turn => ["completed", "succeeded"].includes(turnStatus(turn)));
  if (completed.length !== 1) throw new Error(`expected one fresh completed continuation turn, found ${completed.length}`);
  return completed[0]!;
};

export const collectIdleReplyContinuation = (input: IdleReplyContinuationInput): IdleReplyContinuationResult => {
  if (input.nonce.length === 0 || input.requestContent.length === 0 || input.replyContent.length === 0 || input.continuation.length === 0) throw new Error("continuation inputs must be non-empty");
  if (sameIdentity(input.model, input.peer) || input.model.agent === input.peer.agent || input.model.participantId === input.peer.participantId) throw new Error("model and peer identities must be distinct");
  const root = object(input.nativeHistory), turns = historyTurns(input.nativeHistory);
  if (root?.id !== input.model.hostRuntimeId) throw new Error("native history root is bound to a different runtime");
  const origin = requiredTurn(turns, input.originatingTurnId);
  if (input.baselineTurnIds.includes(input.originatingTurnId) || !["completed", "succeeded"].includes(turnStatus(origin))) throw new Error("originating turn is not a fresh completed turn");
  const originItems = itemIds(origin);
  const originSends = input.nativeTraces.filter(trace => eligible(trace, input.model, "send_message") && originItems.has(trace.sourceId) && inputOf(trace).to === input.peer.agent && inputOf(trace).type === "task" && inputOf(trace).content === input.requestContent && input.requestContent.includes(input.nonce) && outputOf(trace).status === "sent" && typeof outputOf(trace).message_id === "string");
  const requestIds = new Set(originSends.map(trace => String(outputOf(trace).message_id)));
  if (originSends.length !== 1 || requestIds.size !== 1 || [...requestIds][0]!.length === 0) throw new Error("native history has no unique exact originating task send");
  const requestId = String(outputOf(originSends[0]!).message_id);
  if (outputOf(originSends[0]!).to !== input.peer.agent) throw new Error("originating task send returned another recipient");

  const genericReceive = input.genericTraces.filter(trace => eligible(trace, input.peer, "receive_message") && outputOf(trace).status === "message").find(trace => {
    const message = object(outputOf(trace).message);
    return message?.id === requestId && message.from === input.model.agent && message.to === input.peer.agent && message.type === "task" && contentOf(message) === input.requestContent && contentOf(message)?.includes(input.nonce) === true;
  });
  if (!genericReceive) throw new Error("peer did not receive the originating task");
  const genericReplies = input.genericTraces.filter(trace => eligible(trace, input.peer, "send_message") && inputOf(trace).to === input.model.agent && inputOf(trace).type === input.replyType && inputOf(trace).content === input.replyContent && inputOf(trace).in_reply_to === requestId && outputOf(trace).status === "sent" && typeof outputOf(trace).message_id === "string");
  const replyIds = new Set(genericReplies.map(trace => String(outputOf(trace).message_id)));
  if (genericReplies.length !== 1 || replyIds.size !== 1 || [...replyIds][0]!.length === 0 || outputOf(genericReplies[0]!).to !== input.model.agent) throw new Error("peer has no unique exact correlated reply");
  const replyId = String(outputOf(genericReplies[0]!).message_id);
  if (replyId === requestId) throw new Error("reply must have a fresh distinct message ID");
  if (input.genericTraces.indexOf(genericReceive) >= input.genericTraces.indexOf(genericReplies[0]!)) throw new Error("peer reply precedes task consumption");
  const continuationTurn = postOriginTurn(turns, input.originatingTurnId, input.baselineTurnIds);
  const continuationItems = itemIds(continuationTurn);
  const claimTraces = input.nativeTraces.filter(trace => eligible(trace, input.model, "claim_tasks") && continuationItems.has(trace.sourceId) && claimTask(trace, replyId) !== undefined);
  const claimIds = new Set(claimTraces.map(trace => String(object(outputOf(trace).claim)?.claim_id)));
  if (claimTraces.length !== 1 || claimIds.size !== 1 || [...claimIds][0]!.length === 0) throw new Error("continuation has no unique exact reply claim");
  const claim = claimTraces[0]!;
  const claimId = String(object(outputOf(claim).claim)?.claim_id);
  const claimed = claimTask(claim, replyId)!;
  if (claimed.from !== input.peer.agent || claimed.to !== input.model.agent || claimed.type !== input.replyType || contentOf(claimed) !== input.replyContent || payloadOf(claimed)?.in_reply_to !== requestId) throw new Error("claimed reply failed exact actor/type/content/correlation checks");
  const acks = input.nativeTraces.filter(trace => eligible(trace, input.model, "acknowledge_tasks") && continuationItems.has(trace.sourceId) && inputOf(trace).claim_id === claimId && outputOf(trace).status === "ok" && Number(outputOf(trace).acknowledged) > 0);
  if (acks.length !== 1) throw new Error("continuation has no unique positive acknowledgement");
  const orderedItems = (continuationTurn.items as Json[]);
  if (orderedItems.findIndex(item => item.id === claim.sourceId) >= orderedItems.findIndex(item => item.id === acks[0]!.sourceId)) throw new Error("acknowledgement precedes the reply claim");
  const lastAnswerIndex = orderedItems.reduce((last, item, index) => (item.type === "agentMessage" || item.type === "AgentMessage") && (item.phase === "final_answer" || item.phase === undefined) ? index : last, -1);
  if (lastAnswerIndex <= orderedItems.findIndex(item => item.id === claim.sourceId)) throw new Error("final answer precedes reading the reply");
  const sendsAfterOrigin = input.nativeTraces.filter(trace => eligible(trace, input.model, "send_message") && !originItems.has(trace.sourceId) && (inputOf(trace).type === "result" || inputOf(trace).type === "error") && (inputOf(trace).in_reply_to === replyId || inputOf(trace).in_reply_to === requestId));
  if (sendsAfterOrigin.length > 0) throw new Error("native continuation automatically sent a reply");
  const messages = assistantMessages(continuationTurn);
  const finalMessages = messages.filter(message => message.phase === "final_answer");
  if (messages.some(message => message.phase !== undefined)) {
    if (finalMessages.at(-1)?.text !== input.continuation) throw new Error("continuation turn final answer is not exact");
  } else if (messages.at(-1)?.text !== input.continuation) throw new Error("continuation turn has no exact expected assistant message");
  return Object.freeze({ requestId, replyId, claimId, continuationTurnId: String(continuationTurn.id), assistantContent: input.continuation, replyType: input.replyType });
};
