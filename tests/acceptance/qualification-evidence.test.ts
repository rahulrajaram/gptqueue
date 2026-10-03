import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { collectGenericExchange, collectNativeExchange, extractGenericTraces, extractNativeTraces, extractOpenCodeAcpTraces, extractOpenCodeTraces, type GenericActorHistory } from "./qualification-evidence.js";
import type { GenericCallRecord, ParticipantIdentity, RawEvidenceRef } from "./qualification-types.js";
import { requireRetained } from "./retained-evidence.js";

const sender: ParticipantIdentity = { participantId: "a", route: "generic-stdio", hostRuntimeId: "runtime-a", agent: "a", cwdHash: "c-a", profileHash: "p-a", epochHash: "e-a" };
const receiver: ParticipantIdentity = { participantId: "b", route: "generic-stdio", hostRuntimeId: "runtime-b", agent: "b", cwdHash: "c-b", profileHash: "p-b", epochHash: "e-b" };
const raw: RawEvidenceRef = { path: "history.json", sha256: "h", sourceRevision: "s", oracleRevision: "o" };
const envelope = (id: string, from: string, to: string, type: string, content: string, in_reply_to?: string) => ({ id, from, to, type, payload: { content, ...(in_reply_to ? { in_reply_to } : {}) } });
const response = (result: unknown, isError = false): Readonly<{ isError: boolean; result: unknown }> => ({ isError, result });
const records = (): Readonly<{ sender: readonly GenericCallRecord[]; receiver: readonly GenericCallRecord[]; request: ReturnType<typeof envelope>; reply: ReturnType<typeof envelope> }> => {
  const request = envelope("request", "a", "b", "task", "qualification n: calculate 2+3");
  const reply = envelope("reply", "b", "a", "result", "answer n: 5", "request");
  return {
    request, reply,
    sender: [
      { sourceId: "sender-send", name: "send_message", request: { to: "b", type: "task", content: "qualification n: calculate 2+3", idempotency_key: "n:request" }, response: response({ status: "sent", message_id: "request", to: "b" }) },
      { sourceId: "sender-receive", name: "receive_message", request: { timeout: 5 }, response: response({ status: "message", message: reply }) },
    ],
    receiver: [
      { sourceId: "receiver-receive", name: "receive_message", request: { timeout: 5 }, response: response({ status: "message", message: request }) },
      { sourceId: "receiver-send", name: "send_message", request: { to: "a", type: "result", content: "answer n: 5", in_reply_to: "request", idempotency_key: "n:reply" }, response: response({ status: "sent", message_id: "reply", to: "a" }) },
    ],
  };
};
const collectedInput = () => {
  const fixture = records();
  const senderHistory: GenericActorHistory = { actor: sender, records: fixture.sender };
  const receiverHistory: GenericActorHistory = { actor: receiver, records: fixture.receiver };
  return { fixture, senderHistory, receiverHistory, traces: [...extractGenericTraces(fixture.sender, sender, raw), ...extractGenericTraces(fixture.receiver, receiver, raw)] };
};

describe("qualification evidence collector", () => {
  it("accepts retained sanitized generic history as an offline source fixture", (ctx) => {
    const path = join(process.cwd(), ".gptqueue/repair-qualification/20260912/generic-qualification/52c5a060-62d3-4675-af88-d8194b7ac381/history.json");
    requireRetained(ctx, path);
    const serialized = readFileSync(path, "utf8");
    const fixture = JSON.parse(serialized) as readonly Readonly<{ participant: ParticipantIdentity; calls: readonly GenericCallRecord[] }>[];
    const participant = fixture[0];
    expect(participant?.calls.length).toBeGreaterThan(0);
    const rawFixture: RawEvidenceRef = { path, sha256: createHash("sha256").update(serialized).digest("hex"), sourceRevision: "retained-fixture", oracleRevision: "oracle" };
    const traces = extractGenericTraces(participant!.calls, participant!.participant, rawFixture);
    expect(traces.length).toBe(participant!.calls.length);
    expect(traces.every((trace) => trace.rawHistoryRef.sha256 === rawFixture.sha256 && trace.sourceId.length > 0)).toBe(true);
  });

  it("extracts native MCP calls only when the native source ID is present", () => {
    const runtimeResult = { structuredContent: { status: "ok", agent: "a", runtime: { runtime_id: "runtime-a" } } };
    const traces = extractNativeTraces({ turns: [{ items: [{ type: "mcpToolCall", id: "runtime-1", tool: "get_runtime_status", arguments: "{}", result: runtimeResult }, { type: "mcpToolCall", id: "call-1", tool: "send_message", arguments: "{\"type\":\"task\"}", result: { ok: true } }] }] }, sender, raw);
    expect(traces).toHaveLength(2); expect(traces[1]?.sourceId).toBe("call-1"); expect(traces[1]?.runtimeId).toBe("runtime-a");
    expect(() => extractNativeTraces({ turns: [{ items: [{ type: "mcpToolCall", tool: "send_message", result: {} }] }] }, sender, raw)).toThrow(/source ID/);
  });

  it("unwraps Pi native-child JSONL messages only with one exact session header", () => {
    const actor: ParticipantIdentity = { ...sender, route: "pi-native-child", hostRuntimeId: "child-session", agent: "child-agent" };
    const row = (id: string, message: Record<string, unknown>) => ({ type: "message", id, message });
    const history = [
      { type: "session", id: actor.hostRuntimeId, version: 3 },
      row("status-call", { role: "assistant", content: [{ type: "toolCall", id: "status-call", name: "get_runtime_status", arguments: "{}" }] }),
      row("status-result", { role: "toolResult", toolCallId: "status-call", toolName: "get_runtime_status", details: { structuredContent: { status: "ok", agent: actor.agent, runtime: { runtime_id: actor.hostRuntimeId } } } }),
      row("failed-call", { role: "assistant", content: [{ type: "toolCall", id: "failed-call", name: "send_message", arguments: "{}" }] }),
      row("failed-result", { role: "toolResult", toolCallId: "failed-call", toolName: "send_message", isError: true, details: { structuredContent: { status: "error" } } }),
    ];
    const traces = extractNativeTraces(history, actor, raw);
    expect(traces.map((trace) => trace.sourceId)).toEqual(["status-call", "failed-call"]);
    expect(traces[0]).toMatchObject({ successful: true, runtimeBound: true, rawHistoryRef: raw });
    expect(traces[1]).toMatchObject({ successful: false, runtimeBound: false });
    expect(() => extractNativeTraces(history.slice(1), actor, raw)).toThrow(/exactly one matching session header/);
    expect(() => extractNativeTraces([{ type: "session", id: "wrong" }, ...history.slice(1)], actor, raw)).toThrow(/exactly one matching session header/);
    expect(() => extractNativeTraces([history[0], ...history], actor, raw)).toThrow(/exactly one matching session header/);
  });

  it("does not unwrap nested calls from non-message Pi JSONL records", () => {
    const actor: ParticipantIdentity = { ...sender, route: "pi-native-child", hostRuntimeId: "child-session-nested", agent: "child-agent" };
    const history = [
      { type: "session", id: actor.hostRuntimeId, version: 3 },
      { type: "metadata", id: "metadata-1", message: { role: "assistant", content: [{ type: "toolCall", id: "forged-call", name: "get_runtime_status", arguments: "{}" }] } },
    ];
    expect(extractNativeTraces(history, actor, raw)).toEqual([]);
  });

  it("parses headless Codex JSONL with thread identity independent of activation readiness", () => {
    const actor: ParticipantIdentity = { ...sender, route: "codex-headless", participantId: "headless", agent: "headless-agent", hostRuntimeId: "headless-thread" };
    const history = [
      { type: "thread.started", thread_id: actor.hostRuntimeId },
      { type: "item.completed", item: { type: "mcp_tool_call", id: "status", server: "gptqueue-shared", tool: "get_runtime_status", arguments: "{}", status: "completed", result: { structured_content: { status: "ok", agent: actor.agent, activation_ready: false, runtime: null } } } },
      { type: "item.completed", item: { type: "mcp_tool_call", id: "send", server: "gptqueue-shared", tool: "send_message", arguments: JSON.stringify({ to: "peer", type: "ping", content: "hello" }), status: "completed", result: { structured_content: { status: "sent", message_id: "message" } } } },
    ];
    const traces = extractNativeTraces(history, actor, raw);
    expect(traces).toHaveLength(2);
    expect(traces[1]).toMatchObject({ sourceId: "send", successful: true, runtimeBound: true, rawHistoryRef: raw });
    expect(traces[0]?.output).toMatchObject({ activation_ready: false, runtime: null });
  });

  it("retains headless failed or wrong-server observations without treating them as proof", () => {
    const actor: ParticipantIdentity = { ...sender, route: "codex-headless", agent: "headless-agent", hostRuntimeId: "headless-thread" };
    const history = [
      { type: "thread.started", thread_id: actor.hostRuntimeId },
      { type: "item.completed", item: { type: "mcp_tool_call", id: "status", server: "gptqueue-shared", tool: "get_runtime_status", arguments: "{}", status: "completed", result: { structured_content: { status: "ok", agent: actor.agent, activation_ready: false, runtime: null } } } },
      { type: "item.completed", item: { type: "mcp_tool_call", id: "failed", server: "wrong-server", tool: "send_message", arguments: "{}", status: "failed", result: { structured_content: { status: "error" } } } },
    ];
    expect(extractNativeTraces(history, actor, raw).find(trace => trace.sourceId === "failed")).toMatchObject({ successful: false, runtimeBound: false });
  });

  it.each([
    ["missing thread", (history: unknown[]) => history.slice(1)],
    ["duplicate thread", (history: unknown[]) => [history[0], ...history]],
    ["foreign thread", (history: unknown[]) => [{ type: "thread.started", thread_id: "other" }, ...history.slice(1)]],
    ["wrong status agent", (history: unknown[]) => history.map(row => JSON.stringify(row).includes("get_runtime_status") ? ({ ...(row as Record<string, unknown>), item: { ...((row as Record<string, unknown>).item as Record<string, unknown>), result: { structured_content: { status: "ok", agent: "other", activation_ready: false, runtime: null } } } }) : row)],
  ] as const)("rejects %s headless identity", (_name, alter) => {
    const actor: ParticipantIdentity = { ...sender, route: "codex-headless", agent: "headless-agent", hostRuntimeId: "headless-thread" };
    const history: unknown[] = [{ type: "thread.started", thread_id: actor.hostRuntimeId }, { type: "item.completed", item: { type: "mcp_tool_call", id: "status", server: "gptqueue-shared", tool: "get_runtime_status", arguments: "{}", status: "completed", result: { structured_content: { status: "ok", agent: actor.agent, activation_ready: false, runtime: null } } } }];
    expect(() => extractNativeTraces(alter(history), actor, raw)).toThrow();
  });

  it.each([
    { status: "failed" },
    { server: "foreign-server" },
    { error: { message: "rejected" } },
    { isError: true },
  ])("does not bind headless identity from a rejected runtime call: %j", override => {
    const actor: ParticipantIdentity = { ...sender, route: "codex-headless" };
    const history = [{ type: "thread.started", thread_id: actor.hostRuntimeId }, {
      type: "item.completed", item: { type: "mcp_tool_call", id: "status", server: "gptqueue-shared", tool: "get_runtime_status", status: "completed", result: { structured_content: { status: "ok", agent: actor.agent, runtime: null } }, ...override },
    }];
    expect(() => extractNativeTraces(history, actor, raw)).toThrow(/binding/);
  });

  it("reads the actual retained headless control exchange without inventing activation readiness", (ctx) => {
    const path = join(process.cwd(), ".gptqueue/repair-qualification/20260912/codex-headless/2ee00dd7-702c-45c9-a5c9-ff7022ecfef9/receipt-sanitized.json");
    requireRetained(ctx, path);
    const receipt = JSON.parse(readFileSync(path, "utf8")) as { identity: ParticipantIdentity; phases: { label: string; value: unknown }[] };
    const traces = extractNativeTraces(receipt.phases.find(phase => phase.label === "final-history")!.value, receipt.identity, raw);
    expect(traces.map(trace => trace.name)).toEqual(["get_runtime_status", "receive_message", "send_message"]);
    expect(traces.every(trace => trace.successful && trace.runtimeBound)).toBe(true);
    expect(traces[0]?.output).toMatchObject({ activation_ready: false, runtime: null });
    expect(traces[2]?.input).toMatchObject({ type: "result", in_reply_to: "a93dc981-ede7-4d81-b4b7-16b56342e5ba", content: "HEADLESS_CONTROL_READY" });
  });

  it("normalizes retained Codex and Pi native histories with exact runtime binding", (ctx) => {
    const codexPath = join(process.cwd(), ".gptqueue/repair-qualification/20260912/qualification-codex-appserver/3dcef4d3-5220-43f2-8c11-c9b5b836540f/history-sender-sanitized.json");
    requireRetained(ctx, codexPath, join(process.cwd(), ".gptqueue/repair-qualification/20260912/qualification-pi-rpc/feb7ee51-3bd8-4a03-9b44-e25a7940f35a/history-sender-sanitized.json"));
    const codexHistory = JSON.parse(readFileSync(codexPath, "utf8")) as unknown;
    const codexActor: ParticipantIdentity = { ...sender, agent: "gptqueue-shell-codex-70fed238cff30280-76bea006-dac0-4c63-b2d0-8bb4409baeb7", hostRuntimeId: "01a0976a-e880-7270-b0ee-2b5c19d1f949" };
    const codexTraces = extractNativeTraces(codexHistory, codexActor, raw);
    expect(codexTraces.some((trace) => trace.name === "get_runtime_status")).toBe(true);
    const piPath = join(process.cwd(), ".gptqueue/repair-qualification/20260912/qualification-pi-rpc/feb7ee51-3bd8-4a03-9b44-e25a7940f35a/history-sender-sanitized.json");
    const piHistory = JSON.parse(readFileSync(piPath, "utf8")) as unknown;
    const piActor: ParticipantIdentity = { ...sender, agent: "gptqueue-shell-pi-0a367b92cf0b037d-67d2e0ed-0492-41f0-ae36-2b7ef0ededde", hostRuntimeId: "01a0976e-8ede-76c0-ad0a-921ea137effc" };
    const piTraces = extractNativeTraces(piHistory, piActor, raw);
    expect(piTraces.some((trace) => trace.name === "get_runtime_status")).toBe(true);
    expect(piTraces.every((trace) => trace.successful === true && trace.runtimeId === piActor.hostRuntimeId)).toBe(true);
  });

  it("normalizes Pi structured tool results around bare message content for legacy receive", () => {
    const piSender: ParticipantIdentity = { ...sender, route: "pi-rpc-cli", participantId: "pi-structured-sender", agent: "pi-structured-sender", hostRuntimeId: "pi-structured-runtime-sender" };
    const piReceiver: ParticipantIdentity = { ...receiver, route: "pi-rpc-cli", participantId: "pi-structured-receiver", agent: "pi-structured-receiver", hostRuntimeId: "pi-structured-runtime-receiver" };
    const requestContent = "qualification pi-structured: calculate 18+24";
    const expectedReplyContent = "answer pi-structured: 42";
    const request = { id: "pi-structured-request", from: piSender.agent, to: piReceiver.agent, type: "task", payload: { content: requestContent } };
    const reply = { id: "pi-structured-reply", from: piReceiver.agent, to: piSender.agent, type: "result", payload: { content: expectedReplyContent, in_reply_to: request.id } };
    const row = (id: string, name: string, args: unknown, contentText: unknown, structuredContent: unknown, isError = false) => [
      { role: "assistant", content: [{ type: "toolCall", id, name, arguments: JSON.stringify(args) }] },
      { role: "toolResult", toolCallId: id, toolName: name, content: [{ type: "text", text: contentText }], details: { structuredContent }, isError },
    ];
    const runtime = (actor: ParticipantIdentity, id: string) => ({ status: "ok", agent: actor.agent, runtime: { runtime_id: actor.hostRuntimeId } });
    const history = (actor: ParticipantIdentity, peer: ParticipantIdentity, role: "sender" | "receiver") => [
      ...row(`${role}-runtime`, "get_runtime_status", {}, runtime(actor, `${role}-runtime`), runtime(actor, `${role}-runtime`)),
      ...(role === "sender" ? [
        ...row("sender-send", "send_message", { to: peer.agent, type: "task", content: requestContent, idempotency_key: "pi-structured:request" }, { status: "sent", message_id: request.id, to: peer.agent }, { status: "sent", message_id: request.id, to: peer.agent }),
        ...row("sender-receive", "receive_message", { timeout: 60 }, reply, { status: "message", message: reply }),
      ] : [
        ...row("receiver-receive", "receive_message", { timeout: 60 }, request, { status: "message", message: request }),
        ...row("receiver-send", "send_message", { to: peer.agent, type: "result", content: expectedReplyContent, in_reply_to: request.id }, { status: "sent", message_id: reply.id, to: peer.agent }, { status: "sent", message_id: reply.id, to: peer.agent }),
      ]),
    ];
    const senderTraces = extractNativeTraces(history(piSender, piReceiver, "sender"), piSender, raw);
    const receiverTraces = extractNativeTraces(history(piReceiver, piSender, "receiver"), piReceiver, raw);
    expect(senderTraces.find((trace) => trace.name === "receive_message")?.output).toMatchObject({ status: "message", message: reply });
    const collected = collectNativeExchange({ sender: { actor: piSender, traces: senderTraces }, receiver: { actor: piReceiver, traces: receiverTraces }, nonce: "pi-structured", requestContent, expectedReplyContent, consumption: "legacy_receive" });
    expect(collected.evidence.reply?.id).toBe(reply.id);

    const errored = history(piSender, piReceiver, "sender").map((entry) => entry && typeof entry === "object" && "toolCallId" in entry && entry.toolCallId === "sender-receive" ? { ...entry, isError: true, details: { structuredContent: { status: "message", message: reply } } } : entry);
    const erroredTrace = extractNativeTraces(errored, piSender, raw).find((trace) => trace.sourceId === "sender-receive");
    expect(erroredTrace).toMatchObject({ successful: false, runtimeBound: false });
    const detailsErrored = history(piSender, piReceiver, "sender").map((entry) => entry && typeof entry === "object" && "toolCallId" in entry && entry.toolCallId === "sender-receive" ? { ...entry, details: { isError: true, structuredContent: { status: "message", message: reply } } } : entry);
    expect(extractNativeTraces(detailsErrored, piSender, raw).find((trace) => trace.sourceId === "sender-receive")).toMatchObject({ successful: false, runtimeBound: false });
    const structuredErrored = history(piSender, piReceiver, "sender").map((entry) => entry && typeof entry === "object" && "toolCallId" in entry && entry.toolCallId === "sender-receive" ? { ...entry, details: { structuredContent: { status: "error", error: { code: "delivery_failed" } } } } : entry);
    expect(extractNativeTraces(structuredErrored, piSender, raw).find((trace) => trace.sourceId === "sender-receive")).toMatchObject({ successful: false, runtimeBound: false });
    const wrongCallIdentity = history(piSender, piReceiver, "sender").map((entry) => entry && typeof entry === "object" && "toolCallId" in entry && entry.toolCallId === "sender-receive" ? { ...entry, toolCallId: "different-call" } : entry);
    expect(extractNativeTraces(wrongCallIdentity, piSender, raw).find((trace) => trace.sourceId === "sender-receive")).toMatchObject({ successful: false });
    const wrongToolName = history(piSender, piReceiver, "sender").map((entry) => entry && typeof entry === "object" && "toolCallId" in entry && entry.toolCallId === "sender-receive" ? { ...entry, toolName: "send_message" } : entry);
    expect(extractNativeTraces(wrongToolName, piSender, raw).find((trace) => trace.sourceId === "sender-receive")).toMatchObject({ successful: false, runtimeBound: false });
    expect(() => extractNativeTraces(history({ ...piSender, agent: "wrong-pi-actor" }, piReceiver, "sender"), piSender, raw)).toThrow(/runtime binding/);
  });

  it("does not treat nested user or tool payloads as native tool calls", () => {
    const history = { turns: [{ items: [{ type: "mcpToolCall", id: "runtime-only", tool: "get_runtime_status", server: "gptqueue-shared", status: "completed", arguments: "{}", result: { structuredContent: { status: "ok", agent: sender.agent, runtime: { runtime_id: sender.hostRuntimeId }, nested: { type: "mcpToolCall", id: "forged", tool: "send_message", result: { status: "sent" } } } } }, { type: "agentMessage", content: [{ type: "text", text: JSON.stringify({ type: "mcpToolCall", id: "forged-2", tool: "send_message" }) }] }] }] };
    const traces = extractNativeTraces(history, sender, raw);
    expect(traces.map((trace) => trace.sourceId)).toEqual(["runtime-only"]);
    const mismatchedRuntime = { turns: [{ items: [{ type: "mcpToolCall", id: "runtime-mismatch", tool: "get_runtime_status", status: "completed", arguments: "{}", result: { structuredContent: { status: "ok", agent: "other", runtime: { runtime_id: sender.hostRuntimeId } } } }] }] };
    expect(() => extractNativeTraces(mismatchedRuntime, sender, raw)).toThrow(/runtime binding/);
    const beforeRuntimeBinding = { turns: [{ items: [{ type: "mcpToolCall", id: "pre-bind", tool: "send_message", status: "completed", arguments: "{}", result: { structuredContent: { status: "sent", message_id: "m" } } }] }] };
    expect(extractNativeTraces(beforeRuntimeBinding, sender, raw)[0]?.runtimeBound).toBe(false);
  });

  it("joins native task/reply claims to their exact acknowledgements", () => {
    const requestContent = "qualification native-n: calculate 17+25";
    const expectedReplyContent = "answer native-n: 42";
    const request = { id: "request-native", from: sender.agent, to: receiver.agent, type: "task", payload: { content: requestContent } };
    const reply = { id: "reply-native", from: receiver.agent, to: sender.agent, type: "result", payload: { content: expectedReplyContent, in_reply_to: request.id } };
    const row = (id: string, tool: string, args: unknown, structuredContent: unknown) => ({ type: "mcpToolCall", id, tool, status: "completed", arguments: JSON.stringify(args), result: { structuredContent } });
    const senderHistory = { turns: [{ items: [
      row("s-runtime", "get_runtime_status", {}, { status: "ok", agent: sender.agent, runtime: { runtime_id: sender.hostRuntimeId } }),
      row("s-send", "send_message", { to: receiver.agent, type: "task", content: requestContent, idempotency_key: "native-n:request" }, { status: "sent", message_id: request.id, to: receiver.agent }),
      row("s-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { claim_id: "claim-reply", actor_id: sender.agent, tasks: [JSON.stringify(reply)] } }),
      row("s-ack", "acknowledge_tasks", { claim_id: "claim-reply" }, { status: "ok", acknowledged: 1 }),
    ] }] };
    const receiverHistory = { turns: [{ items: [
      row("r-runtime", "get_runtime_status", {}, { status: "ok", agent: receiver.agent, runtime: { runtime_id: receiver.hostRuntimeId } }),
      row("r-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { claim_id: "claim-request", actor_id: receiver.agent, tasks: [JSON.stringify(request)] } }),
      row("r-send", "send_message", { to: sender.agent, type: "result", content: expectedReplyContent, in_reply_to: request.id, idempotency_key: "native-n:reply" }, { status: "sent", message_id: reply.id, to: sender.agent }),
      row("r-ack", "acknowledge_tasks", { claim_id: "claim-request" }, { status: "ok", acknowledged: 1 }),
    ] }] };
    const senderTraces = extractNativeTraces(senderHistory, sender, raw);
    const receiverTraces = extractNativeTraces(receiverHistory, receiver, raw);
    const collected = collectNativeExchange({ sender: { actor: sender, traces: senderTraces }, receiver: { actor: receiver, traces: receiverTraces }, nonce: "native-n", requestContent, expectedReplyContent });
    expect(collected.evidence.request_consumption?.claim_id).toBe("claim-request");
    expect(collected.evidence.reply_consumption?.claim_id).toBe("claim-reply");
    expect(collected.evidence.request_requires_ack).toBe(true);
  });

  it.each([
    ["claim request and legacy reply", { request: "claim_ack", reply: "legacy_receive" }],
    ["legacy request and claim reply", { request: "legacy_receive", reply: "claim_ack" }],
  ] as const)("collects mixed native consumption: %s", (_name, consumption) => {
    const requestContent = "qualification mixed-n: calculate 17+25";
    const expectedReplyContent = "answer mixed-n: 42";
    const request = { id: "request-mixed", from: sender.agent, to: receiver.agent, type: "task", payload: { content: requestContent } };
    const reply = { id: "reply-mixed", from: receiver.agent, to: sender.agent, type: "result", payload: { content: expectedReplyContent, in_reply_to: request.id } };
    const row = (id: string, tool: string, args: unknown, structuredContent: unknown) => ({ type: "mcpToolCall", id, tool, status: "completed", arguments: JSON.stringify(args), result: { structuredContent } });
    const receiverConsume = consumption.request === "claim_ack"
      ? [row("r-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { claim_id: "claim-request-mixed", actor_id: receiver.agent, tasks: [JSON.stringify(request)] } }), row("r-ack", "acknowledge_tasks", { claim_id: "claim-request-mixed" }, { status: "ok", acknowledged: 1 })]
      : [row("r-receive", "receive_message", { timeout: 5 }, { status: "message", message: request })];
    const senderConsume = consumption.reply === "claim_ack"
      ? [row("s-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { claim_id: "claim-reply-mixed", actor_id: sender.agent, tasks: [JSON.stringify(reply)] } }), row("s-ack", "acknowledge_tasks", { claim_id: "claim-reply-mixed" }, { status: "ok", acknowledged: 1 })]
      : [row("s-receive", "receive_message", { timeout: 5 }, { status: "message", message: reply })];
    const senderTraces = extractNativeTraces({ turns: [{ items: [row("s-runtime-mixed", "get_runtime_status", {}, { status: "ok", agent: sender.agent, runtime: { runtime_id: sender.hostRuntimeId } }), row("s-send-mixed", "send_message", { to: receiver.agent, type: "task", content: requestContent, idempotency_key: "mixed-n:request" }, { status: "sent", message_id: request.id, to: receiver.agent }), ...senderConsume] }] }, sender, raw);
    const receiverTraces = extractNativeTraces({ turns: [{ items: [row("r-runtime-mixed", "get_runtime_status", {}, { status: "ok", agent: receiver.agent, runtime: { runtime_id: receiver.hostRuntimeId } }), ...receiverConsume, row("r-send-mixed", "send_message", { to: sender.agent, type: "result", content: expectedReplyContent, in_reply_to: request.id, idempotency_key: "mixed-n:reply" }, { status: "sent", message_id: reply.id, to: sender.agent })] }] }, receiver, raw);
    const collected = collectNativeExchange({ sender: { actor: sender, traces: senderTraces }, receiver: { actor: receiver, traces: receiverTraces }, nonce: "mixed-n", requestContent, expectedReplyContent, consumption });
    expect(collected.evidence.request_requires_ack).toBe(consumption.request === "claim_ack");
    expect(collected.evidence.reply_requires_ack).toBe(consumption.reply === "claim_ack");
    expect(collected.evidence.request_consumption?.acknowledged).toBe(consumption.request === "claim_ack");
    expect(collected.evidence.reply_consumption?.acknowledged).toBe(consumption.reply === "claim_ack");
  });

  it.each([{}, { request: "unsupported", reply: "legacy_receive" }, { request: "claim_ack" }])("rejects malformed consumption declarations %j", consumption => {
    expect(() => collectNativeExchange({ sender: { actor: sender, traces: [] }, receiver: { actor: receiver, traces: [] }, nonce: "n", requestContent: "request n", expectedReplyContent: "reply n", consumption: consumption as Parameters<typeof collectNativeExchange>[0]["consumption"] })).toThrow(/invalid native consumption contract/);
  });

  it("rejects missing ACK, wrong correlation, and runtime binding under an explicit mixed contract", () => {
    const requestContent = "qualification mixed-reject: calculate 17+25";
    const expectedReplyContent = "answer mixed-reject: 42";
    const request = { id: "request-mixed-reject", from: sender.agent, to: receiver.agent, type: "task", payload: { content: requestContent } };
    const reply = { id: "reply-mixed-reject", from: receiver.agent, to: sender.agent, type: "result", payload: { content: expectedReplyContent, in_reply_to: request.id } };
    const row = (id: string, tool: string, args: unknown, structuredContent: unknown) => ({ type: "mcpToolCall", id, tool, status: "completed", arguments: JSON.stringify(args), result: { structuredContent } });
    const makeHistories = (replyCorrelation = request.id, senderAgent = sender.agent) => ({
      sender: { turns: [{ items: [row("sr-runtime", "get_runtime_status", {}, { status: "ok", agent: senderAgent, runtime: { runtime_id: sender.hostRuntimeId } }), row("sr-send", "send_message", { to: receiver.agent, type: "task", content: requestContent }, { status: "sent", message_id: request.id, to: receiver.agent }), row("sr-receive", "receive_message", {}, { status: "message", message: reply })] }] },
      receiver: { turns: [{ items: [row("rr-runtime", "get_runtime_status", {}, { status: "ok", agent: receiver.agent, runtime: { runtime_id: receiver.hostRuntimeId } }), row("rr-claim", "claim_tasks", {}, { status: "ok", claimed: true, claim: { claim_id: "claim-reject", actor_id: receiver.agent, tasks: [JSON.stringify(request)] } }), row("rr-send", "send_message", { to: sender.agent, type: "result", content: expectedReplyContent, in_reply_to: replyCorrelation }, { status: "sent", message_id: reply.id, to: sender.agent })] }] },
    });
    const valid = makeHistories();
    const validSender = extractNativeTraces(valid.sender, sender, raw);
    const validReceiver = extractNativeTraces(valid.receiver, receiver, raw);
    expect(() => collectNativeExchange({ sender: { actor: sender, traces: validSender }, receiver: { actor: receiver, traces: validReceiver }, nonce: "mixed-reject", requestContent, expectedReplyContent, consumption: { request: "claim_ack", reply: "legacy_receive" } })).toThrow(/acknowledgement/);
    const wrongAck = makeHistories();
    wrongAck.receiver.turns[0]!.items.push(row("wrong-ack", "acknowledge_tasks", { claim_id: "another-claim" }, { status: "ok", acknowledged: 1 }));
    expect(() => collectNativeExchange({ sender: { actor: sender, traces: validSender }, receiver: { actor: receiver, traces: extractNativeTraces(wrongAck.receiver, receiver, raw) }, nonce: "mixed-reject", requestContent, expectedReplyContent, consumption: { request: "claim_ack", reply: "legacy_receive" } })).toThrow(/acknowledgement/);
    const wrongCorrelation = makeHistories("wrong-request");
    expect(() => collectNativeExchange({ sender: { actor: sender, traces: extractNativeTraces(wrongCorrelation.sender, sender, raw) }, receiver: { actor: receiver, traces: extractNativeTraces(wrongCorrelation.receiver, receiver, raw) }, nonce: "mixed-reject", requestContent, expectedReplyContent, consumption: { request: "claim_ack", reply: "legacy_receive" } })).toThrow(/correlated reply/);
    const wrongRuntime = makeHistories("request-mixed-reject", "wrong-agent");
    expect(() => collectNativeExchange({ sender: { actor: sender, traces: extractNativeTraces(wrongRuntime.sender, sender, raw) }, receiver: { actor: receiver, traces: extractNativeTraces(wrongRuntime.receiver, receiver, raw) }, nonce: "mixed-reject", requestContent, expectedReplyContent, consumption: { request: "claim_ack", reply: "legacy_receive" } })).toThrow(/runtime binding/);
  });

  it("retains failed native calls without allowing them to qualify or bind", () => {
    const row = (id: string, tool: string, result: unknown, status = "completed") => ({ type: "mcpToolCall", id, tool, status, arguments: "{}", result });
    const failedSendHistory = { turns: [{ items: [row("runtime", "get_runtime_status", { structuredContent: { status: "ok", agent: sender.agent, runtime: { runtime_id: sender.hostRuntimeId } } }), row("failed-send", "send_message", { isError: true, structuredContent: { status: "error", error: { code: "unknown_recipient" } } }, "failed")] }] };
    const failedSend = extractNativeTraces(failedSendHistory, sender, raw);
    expect(failedSend.find((trace) => trace.sourceId === "failed-send")).toMatchObject({ successful: false, runtimeBound: false });
    expect(() => collectNativeExchange({ sender: { actor: sender, traces: failedSend }, receiver: { actor: receiver, traces: [] }, nonce: "n", requestContent: "qualification n: calculate 2+3", expectedReplyContent: "answer n: 5" })).toThrow(/fresh task send/);

    const failedRuntimeHistory = { turns: [{ items: [row("failed-runtime", "get_runtime_status", { isError: true, structuredContent: { status: "error" } }), row("send", "send_message", { structuredContent: { status: "sent", message_id: "request", to: receiver.agent } })] }] };
    const failedRuntime = extractNativeTraces(failedRuntimeHistory, sender, raw);
    expect(failedRuntime.find((trace) => trace.sourceId === "failed-runtime")).toMatchObject({ successful: false, runtimeBound: false });
    expect(failedRuntime.find((trace) => trace.sourceId === "send")).toMatchObject({ successful: true, runtimeBound: false });
    expect(() => collectNativeExchange({ sender: { actor: sender, traces: failedRuntime }, receiver: { actor: receiver, traces: [] }, nonce: "n", requestContent: "qualification n: calculate 2+3", expectedReplyContent: "answer n: 5" })).toThrow(/actor\/runtime binding/);
  });

  it("re-adjudicates the retained recovered exchange after a failed wrong-recipient attempt", (ctx) => {
    const base = join(process.cwd(), ".gptqueue/repair-qualification/20260912/qualification-cross-model/8a903b8f-0638-46d6-b44a-269dfccad35a");
    requireRetained(ctx, join(base, "history-sender-failure-sanitized.json"), join(base, "history-receiver-failure-sanitized.json"));
    const codexHistory = JSON.parse(readFileSync(join(base, "history-sender-failure-sanitized.json"), "utf8")) as unknown;
    const piHistory = JSON.parse(readFileSync(join(base, "history-receiver-failure-sanitized.json"), "utf8")) as unknown;
    const codexActor: ParticipantIdentity = { ...sender, route: "codex-appserver", participantId: "cross-codex", agent: "gptqueue-shell-codex-77a8d8056fe5fb7b-fd934557-0b01-4c10-9592-290bddc88bce", hostRuntimeId: "01a09799-fd52-7370-b67f-8379aea29dd3" };
    const piActor: ParticipantIdentity = { ...receiver, route: "pi-rpc-cli", participantId: "cross-pi", agent: "gptqueue-shell-pi-77a8d8056fe5fb7b-e10086d8-4def-413a-8e77-ebccf6fb0a55", hostRuntimeId: "01a0979a-6a30-73aa-9fec-404401c179fa" };
    const nonce = "8a903b8f-0638-46d6-b44a-269dfccad35a";
    const requestContent = `qualification cross-${nonce}: calculate 17+25`;
    const expectedReplyContent = `answer cross-${nonce}: 42`;
    const senderTraces = extractNativeTraces(codexHistory, codexActor, raw);
    const receiverTraces = extractNativeTraces(piHistory, piActor, raw);
    expect(senderTraces.some((trace) => trace.successful === false && trace.name === "send_message")).toBe(true);
    const collected = collectNativeExchange({ sender: { actor: codexActor, traces: senderTraces }, receiver: { actor: piActor, traces: receiverTraces }, nonce, requestContent, expectedReplyContent });
    expect(collected.evidence.execution?.status).toBe("completed");
    expect(collected.evidence.request?.id).toBe("786a39a3-2b42-4d5e-8c49-d8dcc0baf679");
    expect(collected.evidence.reply?.id).toBe("c32adcbd-9856-4b16-b0a6-16026f35c1ee");
    expect(collected.traces.some((trace) => trace.successful === false && trace.sourceId === "exec-226a6cff-8e7c-408e-af86-8d9a20b16c6a")).toBe(true);
    const wrongActor: ParticipantIdentity = { ...piActor, agent: "wrong-successful-actor" };
    expect(() => collectNativeExchange({ sender: { actor: codexActor, traces: senderTraces }, receiver: { actor: wrongActor, traces: receiverTraces }, nonce, requestContent, expectedReplyContent })).toThrow(/actor\/runtime binding/);
  });

  it("normalizes retained OpenCode SDK parent and child streams without crossing the fork boundary", (ctx) => {
    const path = join(process.cwd(), ".gptqueue/repair-qualification/20260912/opencode-repair/1134a06c-8ad6-49ec-8797-195c8e1a1d67/receipt-sanitized.json");
    requireRetained(ctx, path);
    const receipt = JSON.parse(readFileSync(path, "utf8")) as Readonly<{ native_tool_history: Readonly<{ parent: readonly unknown[]; child: readonly unknown[] }> }>;
    const parentActor: ParticipantIdentity = { ...sender, route: "opencode-run", participantId: "opencode-parent", agent: "gptqueue-opencode-ses_f68db9944ffeMp3B2x61QcfhAH", hostRuntimeId: "ses_f68db9944ffeMp3B2x61QcfhAH" };
    const childActor: ParticipantIdentity = { ...receiver, route: "opencode-native-task", participantId: "opencode-child", agent: "gptqueue-opencode-ses_f68db6a35ffegDFtJfLBOMahE2", hostRuntimeId: "ses_f68db6a35ffegDFtJfLBOMahE2" };
    const parentTraces = extractOpenCodeTraces(receipt.native_tool_history.parent, parentActor, raw);
    const childTraces = extractOpenCodeTraces(receipt.native_tool_history.child, childActor, raw);
    expect(parentTraces.some((trace) => trace.name === "send_message")).toBe(true);
    expect(parentTraces.every((trace) => trace.runtimeBound === false)).toBe(true);
    expect(childTraces.some((trace) => trace.name === "get_runtime_status" && trace.runtimeBound === true)).toBe(true);
    expect(childTraces.some((trace) => trace.name === "send_message" && trace.successful === true)).toBe(true);
    const childOnly = extractOpenCodeTraces({ native_tool_history: { child: receipt.native_tool_history.child } }, childActor, raw);
    expect(childOnly.map((trace) => trace.sourceId)).toEqual(childTraces.map((trace) => trace.sourceId));
    const combinedForChild = extractOpenCodeTraces({ native_tool_history: { parent: receipt.native_tool_history.parent, child: receipt.native_tool_history.child } }, childActor, raw);
    expect(combinedForChild.map((trace) => trace.sourceId)).toEqual(childTraces.map((trace) => trace.sourceId));
    expect(combinedForChild.some((trace) => trace.name === "get_runtime_status" && trace.runtimeBound === true)).toBe(true);
  });

  it("normalizes retained OpenCode ACP tool update notifications", (ctx) => {
    const path = join(process.cwd(), ".gptqueue/repair-qualification/20260912/opencode-repair/opencode-acp-conformance-2026-09-12T21-22-29-442Z-1b57818b-c19d-4705-8332-0e253f86686e/receipt.json");
    requireRetained(ctx, path);
    const receipt = JSON.parse(readFileSync(path, "utf8")) as Readonly<{ evidence: Readonly<{ history: readonly unknown[] }>; identity: ParticipantIdentity }>;
    const traces = extractOpenCodeAcpTraces(receipt.evidence.history, receipt.identity, raw);
    expect(traces).toHaveLength(1);
    expect(traces[0]).toMatchObject({ name: "get_runtime_status", successful: true, runtimeBound: true });
  });

  it("collects an exact OpenCode legacy receive exchange without inventing acknowledgements", () => {
    const requestContent = "qualification opencode-legacy: calculate 53+41";
    const expectedReplyContent = "answer opencode-legacy: 94";
    const request = { id: "request-opencode", from: sender.agent, to: receiver.agent, type: "task", payload: { content: requestContent } };
    const reply = { id: "reply-opencode", from: receiver.agent, to: sender.agent, type: "result", payload: { content: expectedReplyContent, in_reply_to: request.id } };
    const staleRequest = { id: "stale-request-opencode", from: sender.agent, to: receiver.agent, type: "task", payload: { content: requestContent } };
    const row = (id: string, tool: string, input: unknown, output: unknown) => ({ parts: [{ type: "tool", callID: id, tool: `gptqueue_${tool}`, state: { status: "completed", input, output } }] });
    const senderHistory = [
      row("os-runtime", "get_runtime_status", {}, { status: "ok", agent: sender.agent, runtime: { runtime_id: sender.hostRuntimeId } }),
      row("os-send", "send_message", { to: receiver.agent, type: "task", content: requestContent }, { status: "sent", message_id: request.id, to: receiver.agent }),
      row("os-receive", "receive_message", { timeout: 5 }, { status: "message", message: reply }),
    ];
    const receiverHistory = [
      row("or-runtime", "get_runtime_status", {}, { status: "ok", agent: receiver.agent, runtime: { runtime_id: receiver.hostRuntimeId } }),
      row("or-stale-receive", "receive_message", { timeout: 5 }, { status: "message", message: staleRequest }),
      row("or-receive", "receive_message", { timeout: 5 }, { status: "message", message: request }),
      row("or-send", "send_message", { to: sender.agent, type: "result", content: expectedReplyContent, in_reply_to: request.id }, { status: "sent", message_id: reply.id, to: sender.agent }),
    ];
    const collected = collectNativeExchange({ sender: { actor: sender, traces: extractOpenCodeTraces(senderHistory, sender, raw) }, receiver: { actor: receiver, traces: extractOpenCodeTraces(receiverHistory, receiver, raw) }, nonce: "opencode-legacy", requestContent, expectedReplyContent, consumption: "legacy_receive" });
    expect(collected.evidence.request?.id).toBe(request.id);
    expect(collected.evidence.reply?.id).toBe(reply.id);
    expect(collected.evidence.request_requires_ack).toBe(false);
    expect(collected.evidence.reply_requires_ack).toBe(false);
  });

  it("requires exact successful records, routes, nonce, IDs, and reverse correlation", () => {
    const { fixture, senderHistory, receiverHistory, traces } = collectedInput();
    const result = collectGenericExchange({ sender, receiver, nonce: "n", requestContent: "qualification n: calculate 2+3", expectedReplyContent: "answer n: 5", sent: { status: "sent", message_id: "request", to: "b" }, received: fixture.request, replied: { status: "sent", message_id: "reply", to: "a" }, returned: fixture.reply, traces, histories: { sender: senderHistory, receiver: receiverHistory } });
    expect(result.evidence.request?.id).toBe("request");
    expect(result.evidence.reply?.in_reply_to).toBe("request");
    expect(() => collectGenericExchange({ sender, receiver, nonce: "n", requestContent: "qualification n: calculate 2+3", expectedReplyContent: "answer n: 5", sent: { status: "sent", message_id: "wrong", to: "b" }, received: fixture.request, replied: { status: "sent", message_id: "reply", to: "a" }, returned: fixture.reply, traces, histories: { sender: senderHistory, receiver: receiverHistory } })).toThrow(/exact identity/);
    expect(() => collectGenericExchange({ sender, receiver, nonce: "n", requestContent: "qualification n: calculate 2+3", expectedReplyContent: "answer n: 5", sent: { status: "sent", message_id: "request", to: "b" }, received: fixture.request, replied: { status: "sent", message_id: "reply", to: "a" }, returned: fixture.reply, traces: [], histories: { sender: senderHistory, receiver: receiverHistory } })).toThrow(/trace/);
  });

  it("rejects wrong actor, wrong tool, failed, and stale history records", () => {
    const { fixture, senderHistory, receiverHistory, traces } = collectedInput();
    const wrongActor = { ...receiverHistory, actor: { ...receiver, agent: "other" } };
    expect(() => collectGenericExchange({ sender, receiver, nonce: "n", requestContent: "qualification n: calculate 2+3", expectedReplyContent: "answer n: 5", sent: { status: "sent", message_id: "request", to: "b" }, received: fixture.request, replied: { status: "sent", message_id: "reply", to: "a" }, returned: fixture.reply, traces, histories: { sender: senderHistory, receiver: wrongActor } })).toThrow(/actor binding/);
    const wrongTool: GenericCallRecord = { ...fixture.receiver[1]!, name: "not_a_tool" };
    expect(() => collectGenericExchange({ sender, receiver, nonce: "n", requestContent: "qualification n: calculate 2+3", expectedReplyContent: "answer n: 5", sent: { status: "sent", message_id: "request", to: "b" }, received: fixture.request, replied: { status: "sent", message_id: "reply", to: "a" }, returned: fixture.reply, traces, histories: { sender: senderHistory, receiver: { actor: receiver, records: [fixture.receiver[0]!, wrongTool] } } })).toThrow(/wrong tool/);
    const failed: GenericCallRecord = { ...fixture.sender[0]!, response: response(undefined, true) };
    expect(() => extractGenericTraces([failed], sender, raw)).toThrow(/failed/);
    const stale: GenericCallRecord = { ...fixture.receiver[0]!, response: response({ status: "message", message: envelope("stale", "a", "b", "task", "qualification n: calculate 2+3") }) };
    expect(() => collectGenericExchange({ sender, receiver, nonce: "n", requestContent: "qualification n: calculate 2+3", expectedReplyContent: "answer n: 5", sent: { status: "sent", message_id: "request", to: "b" }, received: fixture.request, replied: { status: "sent", message_id: "reply", to: "a" }, returned: fixture.reply, traces, histories: { sender: senderHistory, receiver: { actor: receiver, records: [stale, fixture.receiver[1]!] } } })).toThrow(/stale/);
  });
});
