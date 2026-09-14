import { describe, expect, it } from "vitest";
import { checkExchangeEvidence } from "./oracle.js";
import { collectIdleClaimExchange } from "./qualification-idle-claim.js";
import type { NativeTrace } from "./qualification-evidence.js";
import type { ParticipantIdentity } from "./qualification-types.js";

const identity = (agent: string, route: "generic-stdio" | "codex-appserver"): ParticipantIdentity => ({
  participantId: `${agent}-participant`, route, hostRuntimeId: `${agent}-runtime`, agent,
  cwdHash: `${agent}-cwd`, profileHash: `${agent}-profile`, epochHash: `${agent}-epoch`,
});
const peer = identity("peer", "generic-stdio");
const model = identity("model", "codex-appserver");
const nonce = "idle-claim-nonce";
const requestContent = `${nonce}: calculate 17+25`;
const expectedReplyContent = `${nonce}: 42`;
const request = { id: "request-id", from: peer.agent, to: model.agent, type: "task", payload: { content: requestContent } };
const reply = { id: "reply-id", from: model.agent, to: peer.agent, type: "result", payload: { content: expectedReplyContent, in_reply_to: request.id } };
const trace = (actor: ParticipantIdentity, sourceId: string, name: string, input: Record<string, unknown>, output: Record<string, unknown>): NativeTrace => ({
  sourceId, name, inputHash: sourceId, outputHash: sourceId, actor, runtimeId: actor.hostRuntimeId,
  rawHistoryRef: { path: `${actor.agent}.json`, sha256: "sha", sourceRevision: "source", oracleRevision: "oracle" },
  input, output, successful: true, runtimeBound: true,
});

const fixture = () => ({
  peer, model, nonce, requestContent, expectedReplyContent,
  postStimulusCallIds: ["model-claim", "model-send", "model-ack"],
  genericTraces: [
    trace(peer, "peer-send", "send_message", { to: model.agent, type: "task", content: requestContent }, { status: "sent", to: model.agent, message_id: request.id }),
    trace(peer, "peer-receive", "receive_message", { timeout: 5 }, { status: "message", message: reply }),
  ],
  nativeTraces: [
    trace(model, "model-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { actor_id: model.agent, claim_id: "claim-id", tasks: [JSON.stringify(request)] } }),
    trace(model, "model-send", "send_message", { to: peer.agent, type: "result", content: expectedReplyContent, in_reply_to: request.id }, { status: "sent", to: peer.agent, message_id: reply.id }),
    trace(model, "model-ack", "acknowledge_tasks", { claim_id: "claim-id" }, { status: "ok", acknowledged: 1 }),
  ],
});

describe("idle claim mixed exchange collector", () => {
  it("joins exact generic send, native claim/send/ack, and actual generic receive", () => {
    const evidence = collectIdleClaimExchange(fixture());
    expect(checkExchangeEvidence(evidence).outcome).toBe("meets");
    expect(evidence.request_requires_ack).toBe(true);
    expect(evidence.reply_requires_ack).toBe(false);
    expect(evidence.request_consumption).toMatchObject({ message_id: request.id, claim_id: "claim-id", acknowledged: true });
    expect(evidence.reply_consumption).toMatchObject({ message_id: reply.id, actor: peer.agent, consumed: true, acknowledged: false });
  });

  it("ignores failed noise and collapses identical successful retries", () => {
    const value = fixture();
    value.genericTraces.push({ ...value.genericTraces[0]!, sourceId: "peer-send-retry" });
    value.nativeTraces.push({ ...value.nativeTraces[0]!, sourceId: "model-claim-retry" });
    value.nativeTraces.push({ ...value.nativeTraces[1]!, sourceId: "model-send-retry" });
    value.nativeTraces.push({ ...value.nativeTraces[2]!, sourceId: "model-ack-retry" });
    value.genericTraces.push({ ...value.genericTraces[0]!, sourceId: "failed-noise", successful: false, runtimeBound: false, actor: model });
    expect(checkExchangeEvidence(collectIdleClaimExchange(value)).outcome).toBe("meets");
  });

  it.each([
    ["missing claim ack", (value: ReturnType<typeof fixture>) => { value.nativeTraces = value.nativeTraces.filter((item) => item.sourceId !== "model-ack"); }],
    ["zero claim ack", (value: ReturnType<typeof fixture>) => { value.nativeTraces = value.nativeTraces.map((item) => item.sourceId === "model-ack" ? { ...item, output: { status: "ok", acknowledged: 0 } } : item); }],
    ["wrong claim ack", (value: ReturnType<typeof fixture>) => { value.nativeTraces = value.nativeTraces.map((item) => item.sourceId === "model-ack" ? { ...item, input: { claim_id: "other-claim" } } : item); }],
    ["wrong reply correlation", (value: ReturnType<typeof fixture>) => { value.nativeTraces = value.nativeTraces.map((item) => item.sourceId === "model-send" ? { ...item, input: { ...item.input, in_reply_to: "wrong-request" } } : item); }],
    ["wrong model runtime", (value: ReturnType<typeof fixture>) => { value.nativeTraces = value.nativeTraces.map((item) => item.sourceId === "model-claim" ? { ...item, runtimeId: "other-runtime" } : item); }],
    ["native call outside post-stimulus set", (value: ReturnType<typeof fixture>) => { value.postStimulusCallIds = ["model-claim", "model-send"]; }],
    ["generic receipt without receive", (value: ReturnType<typeof fixture>) => { value.genericTraces = value.genericTraces.filter((item) => item.name !== "receive_message"); }],
    ["wrong payload", (value: ReturnType<typeof fixture>) => { value.nativeTraces = value.nativeTraces.map((item) => item.sourceId === "model-claim" ? { ...item, output: { ...(item.output ?? {}), claim: { actor_id: model.agent, claim_id: "claim-id", tasks: [JSON.stringify({ ...request, payload: { content: "wrong" } })] } } } : item); }],
    ["ambiguous message IDs", (value: ReturnType<typeof fixture>) => { value.genericTraces.push({ ...value.genericTraces[0]!, sourceId: "peer-send-other", output: { status: "sent", to: model.agent, message_id: "other-request" } }); }],
    ["ambiguous claim IDs", (value: ReturnType<typeof fixture>) => { value.nativeTraces.push({ ...value.nativeTraces[0]!, sourceId: "model-claim-other", output: { status: "ok", claimed: true, claim: { actor_id: model.agent, claim_id: "other-claim", tasks: [JSON.stringify(request)] } } }); }],
    ["empty nonce", (value: ReturnType<typeof fixture>) => { value.nonce = ""; }],
    ["same participant identity", (value: ReturnType<typeof fixture>) => { value.model = { ...value.model, participantId: value.peer.participantId }; }],
    ["same agent identity", (value: ReturnType<typeof fixture>) => { value.model = { ...value.model, agent: value.peer.agent }; }],
  ] as const)("rejects %s", (_name, mutate) => {
    const value = fixture();
    mutate(value);
    expect(() => collectIdleClaimExchange(value)).toThrow();
  });
});
