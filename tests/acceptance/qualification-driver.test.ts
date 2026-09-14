import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { runGenericPair, runMixedPair, runModelPair, type TypedEvidenceWriter } from "./qualification-driver.js";
import { CohortLeasePool, type CohortMember } from "./qualification-scheduler.js";
import type { GenericCallRecord, GenericParticipant, ModelParticipant, RawEvidenceRef } from "./qualification-types.js";
import { checkExchangeEvidence } from "./oracle.js";

const writer = (tamper = false): TypedEvidenceWriter => ({
  writeHistory: async ({ actor, serialized }, _signal): Promise<RawEvidenceRef> => ({ path: `${actor.agent}-history.json`, sha256: tamper ? "wrong" : createHash("sha256").update(serialized).digest("hex"), sourceRevision: "source", oracleRevision: "oracle" }),
});
const fake = (route: "generic-stdio", agent: string, actions: readonly Readonly<{ name: string; args: Readonly<Record<string, unknown>>; result: unknown }>[]): GenericParticipant => {
  const identity = { participantId: agent, route, hostRuntimeId: `runtime-${agent}`, agent, cwdHash: `${agent}-cwd`, profileHash: `${agent}-profile`, epochHash: `${agent}-epoch` };
  const records: GenericCallRecord[] = [];
  let index = 0;
  return { kind: "generic", identity, call: async (name, args) => {
    const action = actions[index++];
    if (!action) throw new Error(`unexpected call ${name}`);
    expect(name).toBe(action.name);
    expect(args).toEqual(action.args);
    const response = { isError: false, result: action.result };
    records.push({ sourceId: `${agent}-${index}`, name, request: args, response });
    return action.result;
  }, status: async () => ({ kind: "idle", runtimeId: `runtime-${agent}` }), history: async () => Object.freeze([...records]), close: async () => undefined };
};
const participants = (evidenceWriter: TypedEvidenceWriter): { pool: CohortLeasePool; plan: Parameters<typeof runGenericPair>[1] } => {
  const request = { id: "request-1", from: "sender", to: "receiver", type: "task", payload: { content: "qualification nonce-1: calculate 2+3" } };
  const reply = { id: "reply-1", from: "receiver", to: "sender", type: "result", payload: { content: "answer nonce-1: 5", in_reply_to: "request-1" } };
  const sender = fake("generic-stdio", "sender", [
    { name: "send_message", args: { to: "receiver", type: "task", content: "qualification nonce-1: calculate 2+3", idempotency_key: "nonce-1:request" }, result: { status: "sent", message_id: "request-1", to: "receiver" } },
    { name: "receive_message", args: { timeout: 5 }, result: { status: "message", message: reply } },
  ]);
  const receiver = fake("generic-stdio", "receiver", [
    { name: "receive_message", args: { timeout: 5 }, result: { status: "message", message: request } },
    { name: "send_message", args: { to: "sender", type: "result", content: "answer nonce-1: 5", in_reply_to: "request-1", idempotency_key: "nonce-1:reply" }, result: { status: "sent", message_id: "reply-1", to: "sender" } },
  ]);
  const members: readonly CohortMember[] = [{ participant: sender, identity: sender.identity }, { participant: receiver, identity: receiver.identity }];
  return { pool: new CohortLeasePool(members), plan: { pair: { pairId: "sender->receiver", sender: "generic-stdio", receiver: "generic-stdio", nonce: "nonce-1" }, left: 2, right: 3, evidenceWriter } };
};
const nativeRow = (id: string, tool: string, args: unknown, structuredContent: unknown) => ({ type: "mcpToolCall", id, tool, status: "completed", arguments: JSON.stringify(args), result: { structuredContent } });
const modelParticipants = (legacySide?: "sender" | "receiver"): { pool: CohortLeasePool; plan: Parameters<typeof runModelPair>[1]; prompts: string[] } => {
  const sender = { participantId: "codex-sender", route: "codex-appserver", hostRuntimeId: "codex-runtime", agent: "codex-agent", cwdHash: "c1", profileHash: "p1", epochHash: "e1" } as const;
  const receiver = { participantId: "pi-receiver", route: "pi-rpc-cli", hostRuntimeId: "pi-runtime", agent: "pi-agent", cwdHash: "c2", profileHash: "p2", epochHash: "e2" } as const;
  const requestContent = "qualification model-n: calculate 17+25";
  const expectedReplyContent = "answer model-n: 42";
  const request = { id: "request-model", from: sender.agent, to: receiver.agent, type: "task", payload: { content: requestContent } };
  const reply = { id: "reply-model", from: receiver.agent, to: sender.agent, type: "result", payload: { content: expectedReplyContent, in_reply_to: request.id } };
  const senderHistory = { turns: [{ items: [
    nativeRow("s-runtime", "get_runtime_status", {}, { status: "ok", agent: sender.agent, runtime: { runtime_id: sender.hostRuntimeId } }),
    nativeRow("s-send", "send_message", { to: receiver.agent, type: "task", content: requestContent, idempotency_key: "model-n:request" }, { status: "sent", message_id: request.id, to: receiver.agent }),
    nativeRow("s-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { claim_id: "claim-reply", actor_id: sender.agent, tasks: [JSON.stringify(reply)] } }),
    nativeRow("s-ack", "acknowledge_tasks", { claim_id: "claim-reply" }, { status: "ok", acknowledged: 1 }),
  ] }] };
  const receiverHistory = { turns: [{ items: [
    nativeRow("r-runtime", "get_runtime_status", {}, { status: "ok", agent: receiver.agent, runtime: { runtime_id: receiver.hostRuntimeId } }),
    nativeRow("r-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { claim_id: "claim-request", actor_id: receiver.agent, tasks: [JSON.stringify(request)] } }),
    nativeRow("r-send", "send_message", { to: sender.agent, type: "result", content: expectedReplyContent, in_reply_to: request.id, idempotency_key: "model-n:reply" }, { status: "sent", message_id: reply.id, to: sender.agent }),
    nativeRow("r-ack", "acknowledge_tasks", { claim_id: "claim-request" }, { status: "ok", acknowledged: 1 }),
  ] }] };
  const prompts: string[] = [];
  const participant = (identity: typeof sender | typeof receiver, history: unknown): ModelParticipant => ({ kind: "model", identity, prompt: async (text) => { prompts.push(text); }, status: async () => ({ kind: "idle", runtimeId: identity.hostRuntimeId }), history: async () => history, close: async () => undefined });
  const legacyHistory = (history: typeof senderHistory, message: typeof request | typeof reply) => ({ turns: [{ items: [
    ...history.turns[0]!.items.filter(item => item.tool !== "claim_tasks" && item.tool !== "acknowledge_tasks"),
    nativeRow("legacy-consumption", "receive_message", {}, { status: "message", message }),
  ] }] });
  const senderParticipant = participant(sender, legacySide === "sender" ? legacyHistory(senderHistory, reply) : senderHistory);
  const receiverParticipant = participant(receiver, legacySide === "receiver" ? legacyHistory(receiverHistory, request) : receiverHistory);
  const members: readonly CohortMember[] = [{ participant: senderParticipant, identity: senderParticipant.identity }, { participant: receiverParticipant, identity: receiverParticipant.identity }];
  const evidenceWriter: TypedEvidenceWriter = { writeHistory: async ({ actor, serialized }) => ({ path: `${actor.agent}.json`, sha256: createHash("sha256").update(serialized).digest("hex"), sourceRevision: "source", oracleRevision: "oracle" }) };
  return { pool: new CohortLeasePool(members), plan: { pair: { pairId: "codex->pi", sender: "codex-appserver", receiver: "pi-rpc-cli", nonce: "model-n" }, left: 17, right: 25, evidenceWriter }, prompts };
};
const legacyModelParticipants = (): { pool: CohortLeasePool; plan: Parameters<typeof runModelPair>[1]; prompts: string[] } => {
  const sender = { participantId: "legacy-sender", route: "codex-appserver", hostRuntimeId: "legacy-sender-runtime", agent: "legacy-sender-agent", cwdHash: "lc1", profileHash: "lp1", epochHash: "le1" } as const;
  const receiver = { participantId: "legacy-receiver", route: "pi-rpc-cli", hostRuntimeId: "legacy-receiver-runtime", agent: "legacy-receiver-agent", cwdHash: "lc2", profileHash: "lp2", epochHash: "le2" } as const;
  const requestContent = "qualification legacy-n: calculate 18+24";
  const expectedReplyContent = "answer legacy-n: 42";
  const request = { id: "legacy-request", from: sender.agent, to: receiver.agent, type: "task", payload: { content: requestContent } };
  const reply = { id: "legacy-reply", from: receiver.agent, to: sender.agent, type: "result", payload: { content: expectedReplyContent, in_reply_to: request.id } };
  const senderHistory = { turns: [{ items: [
    nativeRow("ls-runtime", "get_runtime_status", {}, { status: "ok", agent: sender.agent, runtime: { runtime_id: sender.hostRuntimeId } }),
    nativeRow("ls-send", "send_message", { to: receiver.agent, type: "task", content: requestContent, idempotency_key: "legacy-n:request" }, { status: "sent", message_id: request.id, to: receiver.agent }),
    nativeRow("ls-receive", "receive_message", { timeout: 5 }, { status: "message", message: reply }),
  ] }] };
  const receiverHistory = { turns: [{ items: [
    nativeRow("lr-runtime", "get_runtime_status", {}, { status: "ok", agent: receiver.agent, runtime: { runtime_id: receiver.hostRuntimeId } }),
    nativeRow("lr-receive", "receive_message", { timeout: 5 }, { status: "message", message: request }),
    nativeRow("lr-send", "send_message", { to: sender.agent, type: "result", content: expectedReplyContent, in_reply_to: request.id, idempotency_key: "legacy-n:reply" }, { status: "sent", message_id: reply.id, to: sender.agent }),
  ] }] };
  const prompts: string[] = [];
  const participant = (identity: typeof sender | typeof receiver, history: unknown): ModelParticipant => ({ kind: "model", identity, prompt: async (text, signal) => { prompts.push(text); await new Promise<void>((resolve) => { if (signal.aborted) resolve(); else signal.addEventListener("abort", () => resolve(), { once: true }); }); }, status: async () => ({ kind: "idle", runtimeId: identity.hostRuntimeId }), history: async () => history, close: async () => undefined });
  const senderParticipant = participant(sender, senderHistory), receiverParticipant = participant(receiver, receiverHistory);
  const members: readonly CohortMember[] = [{ participant: senderParticipant, identity: senderParticipant.identity }, { participant: receiverParticipant, identity: receiverParticipant.identity }];
  const evidenceWriter: TypedEvidenceWriter = { writeHistory: async ({ actor, serialized }) => ({ path: `${actor.agent}-legacy.json`, sha256: createHash("sha256").update(serialized).digest("hex"), sourceRevision: "source", oracleRevision: "oracle" }) };
  return { pool: new CohortLeasePool(members), plan: { pair: { pairId: "legacy-codex->pi", sender: "codex-appserver", receiver: "pi-rpc-cli", nonce: "legacy-n" }, left: 18, right: 24, evidenceWriter, consumption: "legacy_receive" }, prompts };
};

describe("qualification generic pair driver", () => {
  it("uses the received envelope ID in the reply call and persists real histories", async () => {
    const { pool, plan } = participants(writer());
    const result = await runGenericPair(pool, plan, new AbortController().signal);
    expect(checkExchangeEvidence(result.exchange!).outcome).toBe("meets");
    expect(result.exchange?.reply?.in_reply_to).toBe("request-1");
    expect(result.raw).toHaveLength(2);
  });

  it("rejects a writer reference whose digest does not match the serialized history", async () => {
    const { pool, plan } = participants(writer(true));
    await expect(runGenericPair(pool, plan, new AbortController().signal)).rejects.toThrow(/does not hash/);
  });

  it("drives one cross-model exchange only through prompt, status, and native history", async () => {
    const { pool, plan, prompts } = modelParticipants();
    const result = await runModelPair(pool, plan, AbortSignal.timeout(5_000));
    expect(checkExchangeEvidence(result.exchange!).outcome).toBe("meets");
    expect(result.exchange?.request_consumption?.acknowledged).toBe(true);
    expect(result.exchange?.reply_consumption?.acknowledged).toBe(true);
    expect(prompts).toHaveLength(2);
    expect(prompts.some((prompt) => prompt.includes("claim_tasks") && prompt.includes("pi-agent"))).toBe(true);
    expect(prompts.some((prompt) => prompt.includes("send_message") && prompt.includes("pi-agent"))).toBe(true);
    const receiverPrompt = prompts.find((prompt) => prompt.includes("assisted receiver"));
    expect(receiverPrompt).toContain("<computed decimal>");
    expect(receiverPrompt).toContain("stop immediately");
    expect(receiverPrompt).not.toContain("answer model-n: 42");
  });

  it.each(["sender", "receiver"] as const)("uses the declared per-side protocol when %s consumes through receive_message", async legacySide => {
    const { pool, plan, prompts } = modelParticipants(legacySide);
    const consumption = { request: legacySide === "receiver" ? "legacy_receive" : "claim_ack", reply: legacySide === "sender" ? "legacy_receive" : "claim_ack" } as const;
    const result = await runModelPair(pool, { ...plan, consumption }, AbortSignal.timeout(5_000));
    expect(checkExchangeEvidence(result.exchange!).outcome).toBe("meets");
    expect(result.exchange?.request_requires_ack).toBe(legacySide !== "receiver");
    expect(result.exchange?.reply_requires_ack).toBe(legacySide !== "sender");
    const receiverPrompt = prompts.find(prompt => prompt.includes("assisted receiver"))!;
    const senderPrompt = prompts.find(prompt => !prompt.includes("assisted receiver"))!;
    expect(receiverPrompt).toContain(legacySide === "receiver" ? "receive_message" : "claim_tasks");
    expect(senderPrompt).toContain(legacySide === "sender" ? "receive_message" : "claim_tasks");
  });

  it("returns a complete legacy receive exchange before either prompt reports completion", async () => {
    const { pool, plan, prompts } = legacyModelParticipants();
    const result = await runModelPair(pool, plan, AbortSignal.timeout(5_000));
    expect(checkExchangeEvidence(result.exchange!).outcome).toBe("meets");
    expect(result.exchange?.request_requires_ack).toBe(false);
    expect(result.exchange?.reply_requires_ack).toBe(false);
    expect(prompts).toHaveLength(2);
    expect(prompts.every((prompt) => prompt.includes("receive_message"))).toBe(true);
    expect(prompts.every((prompt) => prompt.includes("timeout: 60"))).toBe(true);
    expect(prompts.some((prompt) => prompt.includes("claim_tasks"))).toBe(false);
  });

  it("drives a generic sender and model receiver through legacy native receive with unknown model status", async () => {
    const genericIdentity = { participantId: "mixed-generic", route: "generic-stdio", hostRuntimeId: "mixed-generic-runtime", agent: "mixed-generic", cwdHash: "gc", profileHash: "gp", epochHash: "ge" } as const;
    const modelIdentity = { participantId: "mixed-model", route: "codex-appserver", hostRuntimeId: "mixed-model-runtime", agent: "mixed-model", cwdHash: "mc", profileHash: "mp", epochHash: "me" } as const;
    const requestContent = "qualification mixed-n: calculate 17+25";
    const replyContent = "answer mixed-n: 42";
    const request = { id: "mixed-request", from: genericIdentity.agent, to: modelIdentity.agent, type: "task", payload: { content: requestContent } };
    const reply = { id: "mixed-reply", from: modelIdentity.agent, to: genericIdentity.agent, type: "result", payload: { content: replyContent, in_reply_to: request.id } };
    const genericRecords: GenericCallRecord[] = [];
    const genericActions = [
      { name: "send_message", args: { to: modelIdentity.agent, type: "task", content: requestContent, idempotency_key: "mixed-n:request" }, result: { status: "sent", message_id: request.id, to: modelIdentity.agent } },
      { name: "receive_message", args: { timeout: 5 }, result: { status: "message", message: reply } },
    ] as const;
    let actionIndex = 0;
    const generic: GenericParticipant = { kind: "generic", identity: genericIdentity, call: async (name, args) => { const action = genericActions[actionIndex++]; expect(action).toBeDefined(); expect(name).toBe(action!.name); expect(args).toEqual(action!.args); genericRecords.push({ sourceId: `mixed-generic-${actionIndex}`, name, request: args, response: { isError: false, result: action!.result } }); return action!.result; }, status: async () => ({ kind: "idle", runtimeId: genericIdentity.hostRuntimeId }), history: async () => genericRecords, close: async () => undefined };
    const modelHistory = { turns: [{ items: [
      nativeRow("mixed-runtime", "get_runtime_status", {}, { status: "ok", agent: modelIdentity.agent, runtime: { runtime_id: modelIdentity.hostRuntimeId } }),
      nativeRow("mixed-receive", "receive_message", { timeout: 5 }, { status: "message", message: request }),
      nativeRow("mixed-send", "send_message", { to: genericIdentity.agent, type: "result", content: replyContent, in_reply_to: request.id }, { status: "sent", message_id: reply.id, to: genericIdentity.agent }),
    ] }] };
    const prompts: string[] = [];
    const model: ModelParticipant = { kind: "model", identity: modelIdentity, prompt: async (text) => { prompts.push(text); }, status: async () => ({ kind: "unknown", detail: "headless status unavailable" }), history: async () => modelHistory, close: async () => undefined };
    const pool = new CohortLeasePool([{ participant: generic, identity: generic.identity }, { participant: model, identity: model.identity }]);
    const evidenceWriter: TypedEvidenceWriter = { writeHistory: async ({ actor, serialized }) => ({ path: `${actor.agent}-mixed.json`, sha256: createHash("sha256").update(serialized).digest("hex"), sourceRevision: "source", oracleRevision: "oracle" }) };
    const result = await runMixedPair(pool, { pair: { pairId: "mixed-generic->model", sender: "generic-stdio", receiver: "codex-appserver", nonce: "mixed-n" }, left: 17, right: 25, evidenceWriter }, AbortSignal.timeout(5_000));
    expect(checkExchangeEvidence(result.exchange!).outcome).toBe("meets");
    expect(result.exchange?.request_requires_ack).toBe(false);
    expect(prompts).toHaveLength(1);
    expect(prompts[0]).toContain("receive_message");
    expect(prompts[0]).toContain("<computed decimal>");
    expect(prompts[0]).not.toContain(replyContent);
  });

  it("drives a model sender and generic receiver with the same exact legacy join", async () => {
    const modelIdentity = { participantId: "mixed-model-sender", route: "codex-appserver", hostRuntimeId: "mixed-model-sender-runtime", agent: "mixed-model-sender", cwdHash: "mc", profileHash: "mp", epochHash: "me" } as const;
    const genericIdentity = { participantId: "mixed-generic-receiver", route: "generic-stdio", hostRuntimeId: "mixed-generic-receiver-runtime", agent: "mixed-generic-receiver", cwdHash: "gc", profileHash: "gp", epochHash: "ge" } as const;
    const requestContent = "qualification mixed-reverse: calculate 19+23";
    const replyContent = "answer mixed-reverse: 42";
    const request = { id: "mixed-reverse-request", from: modelIdentity.agent, to: genericIdentity.agent, type: "task", payload: { content: requestContent } };
    const reply = { id: "mixed-reverse-reply", from: genericIdentity.agent, to: modelIdentity.agent, type: "result", payload: { content: replyContent, in_reply_to: request.id } };
    const genericRecords: GenericCallRecord[] = [];
    const actions = [
      { name: "receive_message", args: { timeout: 5 }, result: { status: "error", error: { code: "timeout" } } },
      { name: "receive_message", args: { timeout: 5 }, result: { status: "message", message: request } },
      { name: "send_message", args: { to: modelIdentity.agent, type: "result", content: replyContent, in_reply_to: request.id, idempotency_key: "mixed-reverse:reply" }, result: { status: "sent", message_id: reply.id, to: modelIdentity.agent } },
    ] as const;
    let index = 0;
    const generic: GenericParticipant = { kind: "generic", identity: genericIdentity, call: async (name, args) => { const action = actions[index++]; expect(action).toBeDefined(); expect(name).toBe(action!.name); expect(args).toEqual(action!.args); genericRecords.push({ sourceId: `mixed-reverse-generic-${index}`, name, request: args, response: { isError: false, result: action!.result } }); return action!.result; }, status: async () => ({ kind: "idle", runtimeId: genericIdentity.hostRuntimeId }), history: async () => genericRecords, close: async () => undefined };
    const modelHistory = { turns: [{ items: [
      nativeRow("mixed-reverse-runtime", "get_runtime_status", {}, { status: "ok", agent: modelIdentity.agent, runtime: { runtime_id: modelIdentity.hostRuntimeId } }),
      nativeRow("mixed-reverse-send", "send_message", { to: genericIdentity.agent, type: "task", content: requestContent }, { status: "sent", message_id: request.id, to: genericIdentity.agent }),
      nativeRow("mixed-reverse-receive", "receive_message", { timeout: 5 }, { status: "message", message: reply }),
    ] }] };
    const model: ModelParticipant = { kind: "model", identity: modelIdentity, prompt: async () => undefined, status: async () => ({ kind: "unknown", detail: "ACP status unavailable" }), history: async () => modelHistory, close: async () => undefined };
    const pool = new CohortLeasePool([{ participant: model, identity: model.identity }, { participant: generic, identity: generic.identity }]);
    const evidenceWriter: TypedEvidenceWriter = { writeHistory: async ({ actor, serialized }) => ({ path: `${actor.agent}-mixed-reverse.json`, sha256: createHash("sha256").update(serialized).digest("hex"), sourceRevision: "source", oracleRevision: "oracle" }) };
    const result = await runMixedPair(pool, { pair: { pairId: "mixed-model->generic", sender: "codex-appserver", receiver: "generic-stdio", nonce: "mixed-reverse" }, left: 19, right: 23, evidenceWriter }, AbortSignal.timeout(5_000));
    expect(checkExchangeEvidence(result.exchange!).outcome).toBe("meets");
    expect(result.exchange?.request?.content).toBe(requestContent);
  });
});
