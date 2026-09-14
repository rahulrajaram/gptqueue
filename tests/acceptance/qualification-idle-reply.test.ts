import { describe, expect, it } from "vitest";
import { extractGenericTraces, extractNativeTraces } from "./qualification-evidence.js";
import { collectIdleReplyContinuation, type IdleReplyContinuationInput } from "./qualification-idle-reply.js";
import type { ParticipantIdentity } from "./qualification-types.js";

const identity = (agent: string, route: "generic-stdio" | "codex-appserver"): ParticipantIdentity => ({
  participantId: `${agent}-participant`, route, hostRuntimeId: `${agent}-runtime`, agent,
  cwdHash: `${agent}-cwd`, profileHash: `${agent}-profile`, epochHash: `${agent}-epoch`,
});
const model = identity("model", "codex-appserver");
const peer = identity("peer", "generic-stdio");
const ref = { path: "synthetic-history.json", sha256: "synthetic", sourceRevision: "synthetic", oracleRevision: "synthetic" };
type Json = Record<string, unknown>;

const fixture = (replyType: "result" | "error" = "result"): IdleReplyContinuationInput => {
  const nonce = `reply-${replyType}-nonce`;
  const requestContent = `${nonce}: perform the hidden operation`;
  const replyContent = replyType === "result" ? `${nonce}: value=17` : `${nonce}: code=E17 detail=hidden`;
  const continuation = replyType === "result" ? `${nonce}: continued value is 34` : `${nonce}: recover from E17`;
  const request = { id: "task-id", from: model.agent, to: peer.agent, type: "task", payload: { content: requestContent } };
  const reply = { id: "reply-id", from: peer.agent, to: model.agent, type: replyType, payload: { content: replyContent, in_reply_to: request.id } };
  const call = (id: string, tool: string, args: Json, result: Json) => ({ type: "mcpToolCall", id, tool, server: "gptqueue-shared", status: "completed", arguments: JSON.stringify(args), result: { structuredContent: result } });
  const history = { id: model.hostRuntimeId, turns: [
    { id: "setup", status: "completed", items: [call("runtime", "get_runtime_status", {}, { status: "ok", agent: model.agent, runtime: { runtime_id: model.hostRuntimeId } })] },
    { id: "origin", status: "completed", items: [call("origin-send", "send_message", { to: peer.agent, type: "task", content: requestContent }, { status: "sent", to: peer.agent, message_id: request.id })] },
    { id: "continuation", status: "completed", items: [
      call("reply-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { actor_id: model.agent, claim_id: "claim-id", tasks: [JSON.stringify(reply)] } }),
      call("reply-ack", "acknowledge_tasks", { claim_id: "claim-id" }, { status: "ok", acknowledged: 1 }),
      { type: "agentMessage", id: "answer", phase: "final_answer", content: [{ type: "text", text: continuation }] },
    ] },
  ] };
  const genericRecords = [
    { sourceId: "peer-receive-task", name: "receive_message", request: { timeout: 5 }, response: { isError: false, result: { status: "message", message: request } } },
    { sourceId: "peer-send-reply", name: "send_message", request: { to: model.agent, type: replyType, content: replyContent, in_reply_to: request.id }, response: { isError: false, result: { status: "sent", to: model.agent, message_id: reply.id } } },
  ];
  return { model, peer, baselineTurnIds: ["setup"], originatingTurnId: "origin", nativeHistory: history, nativeTraces: extractNativeTraces(history, model, ref), genericTraces: extractGenericTraces(genericRecords, peer, ref), requestContent, replyContent, continuation, replyType, nonce };
};

describe("idle correlated result/error continuation collector", () => {
  it.each(["result", "error"] as const)("accepts a fresh %s continuation with claim, ACK, and assistant answer", replyType => {
    const result = collectIdleReplyContinuation(fixture(replyType));
    expect(result).toMatchObject({ requestId: "task-id", replyId: "reply-id", claimId: "claim-id", continuationTurnId: "continuation", replyType });
  });

  const invalid = (name: string, alter: (value: IdleReplyContinuationInput) => IdleReplyContinuationInput) => it(`rejects ${name}`, () => {
    expect(() => collectIdleReplyContinuation(alter(fixture()))).toThrow();
  });
  invalid("wrong correlation", value => ({ ...value, genericTraces: value.genericTraces.map(trace => trace.name === "send_message" ? { ...trace, input: { ...(trace.input ?? {}), in_reply_to: "other-task" } } : trace) }));
  invalid("wrong reply type", value => ({ ...value, replyType: "error", genericTraces: value.genericTraces.map(trace => trace.name === "send_message" ? { ...trace, input: { ...(trace.input ?? {}), type: "error" } } : trace) }));
  invalid("wrong runtime binding", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, runtimeId: "other-runtime" } : trace) }));
  invalid("wrong actor", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, actor: peer } : trace) }));
  invalid("wrong claimed payload", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, output: { ...(trace.output ?? {}), claim: { actor_id: model.agent, claim_id: "claim-id", tasks: [JSON.stringify({ id: "reply-id", from: peer.agent, to: model.agent, type: "result", payload: { content: "wrong", in_reply_to: "task-id" } })] } } } : trace) }));
  invalid("missing ACK", value => ({ ...value, nativeTraces: value.nativeTraces.filter(trace => trace.name !== "acknowledge_tasks") }));
  invalid("zero ACK", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "acknowledge_tasks" ? { ...trace, output: { status: "ok", acknowledged: 0 } } : trace) }));
  invalid("wrong ACK claim", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "acknowledge_tasks" ? { ...trace, input: { claim_id: "other-claim" } } : trace) }));
  invalid("in-progress continuation", value => ({ ...value, nativeHistory: { ...(value.nativeHistory as Json), turns: ((value.nativeHistory as Json).turns as Json[]).map(turn => turn.id === "continuation" ? { ...turn, status: "inProgress" } : turn) } }));
  invalid("old originating turn", value => ({ ...value, originatingTurnId: "setup" }));
  invalid("no generic receive", value => ({ ...value, genericTraces: value.genericTraces.filter(trace => trace.name !== "receive_message") }));
  invalid("no native originating send", value => ({ ...value, nativeTraces: value.nativeTraces.filter(trace => trace.sourceId !== "origin-send") }));
  invalid("automatic native reply loop", value => ({ ...value, nativeTraces: [...value.nativeTraces, { ...value.nativeTraces.find(trace => trace.name === "send_message")!, sourceId: "loop", input: { to: peer.agent, type: "result", content: "loop", in_reply_to: "reply-id" }, output: { status: "sent", to: peer.agent, message_id: "loop-id" } }] }));
  invalid("wrong final answer", value => ({ ...value, continuation: "a fabricated answer" }));
  invalid("foreign native history root", value => ({ ...value, nativeHistory: { ...(value.nativeHistory as Json), id: "foreign-runtime" } }));
  invalid("empty native request ID", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.sourceId === "origin-send" ? { ...trace, output: { ...trace.output, message_id: "" } } : trace) }));
  invalid("wrong originating send recipient", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.sourceId === "origin-send" ? { ...trace, output: { ...trace.output, to: "foreign-peer" } } : trace) }));
  invalid("empty reply claim ID", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, output: { ...trace.output, claim: { ...(trace.output?.claim as Json), claim_id: "" } } } : trace) }));
  invalid("non-MCP claim item", value => ({ ...value, nativeHistory: { ...(value.nativeHistory as Json), turns: ((value.nativeHistory as Json).turns as Json[]).map(turn => turn.id === "continuation" ? { ...turn, items: (turn.items as Json[]).map(item => item.id === "reply-claim" ? { ...item, type: "agentMessage" } : item) } : turn) } }));
  invalid("reply before peer consumption", value => ({ ...value, genericTraces: [...value.genericTraces].reverse() }));
  invalid("ACK before native claim", value => ({ ...value, nativeHistory: { ...(value.nativeHistory as Json), turns: ((value.nativeHistory as Json).turns as Json[]).map(turn => turn.id === "continuation" ? { ...turn, items: [...turn.items as Json[]].reverse() } : turn) } }));
  invalid("answer before reading the reply", value => ({ ...value, nativeHistory: { ...(value.nativeHistory as Json), turns: ((value.nativeHistory as Json).turns as Json[]).map(turn => turn.id === "continuation" ? { ...turn, items: [(turn.items as Json[])[2]!, (turn.items as Json[])[0]!, (turn.items as Json[])[1]!] } : turn) } }));
  it("rejects reusing the request ID for its reply", () => {
    const value = fixture();
    const reused = { ...value, genericTraces: value.genericTraces.map(trace => trace.name === "send_message" ? { ...trace, output: { ...trace.output, message_id: "task-id" } } : trace) };
    expect(() => collectIdleReplyContinuation(reused)).toThrow(/fresh distinct message ID/);
  });
  invalid("commentary followed by wrong final answer", value => ({ ...value, nativeHistory: { ...(value.nativeHistory as Json), turns: ((value.nativeHistory as Json).turns as Json[]).map(turn => turn.id === "continuation" ? { ...turn, items: [...turn.items as Json[], { type: "agentMessage", id: "wrong-final", phase: "final_answer", text: "wrong final" }] } : turn) } }));
  invalid("commentary-only continuation", value => ({ ...value, nativeHistory: { ...(value.nativeHistory as Json), turns: ((value.nativeHistory as Json).turns as Json[]).map(turn => turn.id === "continuation" ? { ...turn, items: (turn.items as Json[]).map(item => item.id === "answer" ? { ...item, phase: "commentary" } : item) } : turn) } }));
});
