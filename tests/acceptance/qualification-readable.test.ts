import { describe, expect, it } from "vitest";
import { collectReadableDelivery, type ReadableDeliveryInput } from "./qualification-readable.js";
import type { NativeTrace } from "./qualification-evidence.js";
import type { ParticipantIdentity } from "./qualification-types.js";

const identity = (agent: string, route: "generic-stdio" | "codex-appserver"): ParticipantIdentity => ({
  participantId: `${agent}-participant`, route, hostRuntimeId: `${agent}-runtime`, agent,
  cwdHash: `${agent}-cwd`, profileHash: `${agent}-profile`, epochHash: `${agent}-epoch`,
});
const model = identity("model", "codex-appserver");
const peer = identity("peer", "generic-stdio");
const base = (messageType: "ping" | "status" = "ping"): ReadableDeliveryInput => {
  const content = `${messageType}-probe-content`;
  const trace = (sourceId: string, actor: ParticipantIdentity, name: string, input: Record<string, unknown>, output: Record<string, unknown>, successful = true): NativeTrace => ({
    sourceId, name, input, output, actor, runtimeId: actor.hostRuntimeId, inputHash: sourceId, outputHash: sourceId,
    rawHistoryRef: { path: "synthetic", sha256: "synthetic", sourceRevision: "synthetic", oracleRevision: "synthetic" }, successful, runtimeBound: successful,
  });
  return {
    peer, model, genericTraces: [trace("send", peer, "send_message", { to: model.agent, type: messageType, content }, { status: "sent", to: model.agent, message_id: "message-id", deduplicated: false })],
    nativeTraces: [trace("runtime-model", model, "get_runtime_status", {}, { status: "ok", runtime: { runtime_id: model.hostRuntimeId } }), trace("claim", model, "claim_tasks", {}, { status: "ok", claimed: true, claim: { actor_id: model.agent, claim_id: "claim-id", tasks: [{ id: "message-id", from: peer.agent, to: model.agent, type: messageType, payload: { content } }] } })],
    postStimulusCallIds: ["claim"], messageType, content,
  };
};

describe("readable ping/status delivery", () => {
  it.each(["ping", "status"] as const)("accepts exact %s delivery", messageType => {
    expect(collectReadableDelivery(base(messageType))).toEqual({ messageId: "message-id", claimId: "claim-id", claimSourceId: "claim" });
  });
  const invalid = (name: string, alter: (input: ReadableDeliveryInput) => ReadableDeliveryInput) => it(`rejects ${name}`, () => expect(() => collectReadableDelivery(alter(base()))).toThrow());
  invalid("near-match content", value => ({ ...value, content: "different-content" }));
  invalid("wrong message type", value => ({ ...value, messageType: "status", genericTraces: value.genericTraces.map(trace => trace.name === "send_message" ? { ...trace, input: { ...trace.input, type: "status" } } : trace) }));
  invalid("wrong actor identity", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, actor: peer } : trace) }));
  invalid("wrong runtime binding", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, runtimeId: "foreign-runtime" } : trace) }));
  invalid("baseline-only claim", value => ({ ...value, postStimulusCallIds: ["runtime-model"] }));
  invalid("ambiguous exact sends", value => ({ ...value, genericTraces: [...value.genericTraces, value.genericTraces[0]!] }));
  invalid("wrong claimed envelope", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, output: { ...trace.output, claim: { ...(trace.output?.claim as Record<string, unknown>), tasks: [{ id: "message-id", from: peer.agent, to: model.agent, type: "ping", payload: { content: "near-match" } }] } } } : trace) }));
  invalid("failed send instead of proof", value => ({ ...value, genericTraces: value.genericTraces.map(trace => ({ ...trace, successful: false, output: { status: "error" } })) }));
  invalid("unbound native claim", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => ({ ...trace, runtimeBound: false })) }));
  invalid("deduplicated send", value => ({ ...value, genericTraces: value.genericTraces.map(trace => ({ ...trace, output: { ...trace.output, deduplicated: true } })) }));
  invalid("empty message ID", value => ({ ...value, genericTraces: value.genericTraces.map(trace => ({ ...trace, output: { ...trace.output, message_id: "" } })) }));
  invalid("failed claim", value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, successful: false } : trace) }));
  invalid("duplicate exact claims", value => ({ ...value, nativeTraces: [...value.nativeTraces, value.nativeTraces[1]!] }));
  invalid("same sender and recipient", value => ({ ...value, peer: model }));
  for (const [field, replacement] of [["actor_id", "foreign"], ["claim_id", ""]] as const) {
    invalid(`claim ${field}`, value => ({ ...value, nativeTraces: value.nativeTraces.map(trace => trace.name === "claim_tasks" ? { ...trace, output: { ...trace.output, claim: { ...(trace.output?.claim as Record<string, unknown>), [field]: replacement } } } : trace) }));
  }
  it("preserves a valid chain alongside an unrelated failed call", () => {
    const value = base();
    expect(collectReadableDelivery({ ...value, genericTraces: [...value.genericTraces, { ...value.genericTraces[0]!, sourceId: "failed", successful: false, runtimeBound: false, output: { status: "error" } }] }).messageId).toBe("message-id");
  });
});
