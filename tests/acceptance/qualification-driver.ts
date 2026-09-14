import { createHash } from "node:crypto";
import type { GenericCallRecord, GenericParticipant, ModelParticipant, PairLeasePool, PairSpec, Participant, ParticipantIdentity, RawEvidenceRef, TrialEvidence } from "./qualification-types.js";
import { withPairLease } from "./qualification-scheduler.js";
import { collectGenericExchange, collectNativeExchange, extractGenericTraces, extractNativeTraces, extractOpenCodeAcpTraces, extractOpenCodeTraces, type GenericActorHistory, type NativeExchangeParticipant } from "./qualification-evidence.js";
import { sanitizeEvidence } from "./public-evidence.js";

export type EvidenceArtifact = Readonly<{ actor: ParticipantIdentity; records?: readonly GenericCallRecord[]; history?: unknown; serialized: string }>;
export type TypedEvidenceWriter = Readonly<{
  writeHistory: (artifact: EvidenceArtifact, signal: AbortSignal) => Promise<RawEvidenceRef>;
}>;
export type ArithmeticPairPlan = Readonly<{ pair: PairSpec; left: number; right: number; evidenceWriter: TypedEvidenceWriter }>;

const asGeneric = (value: unknown): GenericParticipant => {
  if (!value || typeof value !== "object" || (value as { kind?: unknown }).kind !== "generic") throw new Error("generic pair driver cannot call a model participant");
  return value as GenericParticipant;
};
const resultMessage = (value: unknown, expectedStatus: "sent" | "message"): Record<string, unknown> => {
  if (!value || typeof value !== "object") throw new Error("MCP call returned no result");
  const result = value as Record<string, unknown>;
  if (result.status !== expectedStatus) throw new Error(`MCP call returned unexpected status ${String(result.status)}`);
  return result;
};
const serialize = (value: unknown): string => `${JSON.stringify(value) ?? "null"}\n`;
const digest = (serialized: string): string => createHash("sha256").update(serialized).digest("hex");
const writeHistory = async (writer: TypedEvidenceWriter, actor: ParticipantIdentity, records: readonly GenericCallRecord[], signal: AbortSignal): Promise<RawEvidenceRef> => {
  const artifact: EvidenceArtifact = Object.freeze({ actor, records: Object.freeze([...records]), history: Object.freeze([...records]), serialized: serialize(records) });
  const ref = await writer.writeHistory(artifact, signal);
  if (!ref || typeof ref.path !== "string" || ref.path.length === 0 || typeof ref.sha256 !== "string" || ref.sha256 !== digest(artifact.serialized) || typeof ref.sourceRevision !== "string" || ref.sourceRevision.length === 0 || typeof ref.oracleRevision !== "string" || ref.oracleRevision.length === 0) throw new Error("evidence writer returned a reference that does not hash the serialized history");
  return Object.freeze({ ...ref });
};
const writeNativeHistory = async (writer: TypedEvidenceWriter, actor: ParticipantIdentity, history: unknown, signal: AbortSignal): Promise<RawEvidenceRef> => {
  const sanitizedHistory = sanitizeEvidence(history, { parseEmbeddedJson: true, redactSessionObjectIds: true });
  const artifact: EvidenceArtifact = Object.freeze({ actor, history: sanitizedHistory, serialized: serialize(sanitizedHistory) });
  const ref = await writer.writeHistory(artifact, signal);
  if (!ref || typeof ref.path !== "string" || ref.path.length === 0 || typeof ref.sha256 !== "string" || ref.sha256 !== digest(artifact.serialized) || typeof ref.sourceRevision !== "string" || ref.sourceRevision.length === 0 || typeof ref.oracleRevision !== "string" || ref.oracleRevision.length === 0) throw new Error("evidence writer returned a reference that does not hash the serialized native history");
  return Object.freeze({ ...ref });
};

/** One controller-computed generic exchange. Model routes must use a model adapter that observes native calls. */
export const runGenericPair = async (pool: PairLeasePool, plan: ArithmeticPairPlan, signal: AbortSignal): Promise<TrialEvidence> =>
  withPairLease(pool, plan.pair, signal, async ({ lease, sender: rawSender, receiver: rawReceiver }) => {
    const sender = asGeneric(rawSender), receiver = asGeneric(rawReceiver);
    const requestContent = `qualification ${plan.pair.nonce}: calculate ${plan.left}+${plan.right}`;
    const expectedReplyContent = `answer ${plan.pair.nonce}: ${plan.left + plan.right}`;
    const sent = resultMessage(await sender.call("send_message", { to: receiver.identity.agent, type: "task", content: requestContent, idempotency_key: `${plan.pair.nonce}:request` }, signal), "sent");
    const received = resultMessage(await receiver.call("receive_message", { timeout: 5 }, signal), "message");
    const receivedMessage = received.message;
    if (!receivedMessage || typeof receivedMessage !== "object" || typeof (receivedMessage as Record<string, unknown>).id !== "string") throw new Error("receive response has no exact message ID");
    const receivedEnvelope = receivedMessage as Record<string, unknown>;
    const replied = resultMessage(await receiver.call("send_message", { to: sender.identity.agent, type: "result", content: expectedReplyContent, in_reply_to: receivedEnvelope.id, idempotency_key: `${plan.pair.nonce}:reply` }, signal), "sent");
    const returned = resultMessage(await sender.call("receive_message", { timeout: 5 }, signal), "message");
    const returnedMessage = returned.message;
    const senderRecords = await sender.history(signal), receiverRecords = await receiver.history(signal);
    const senderRef = await writeHistory(plan.evidenceWriter, sender.identity, senderRecords, signal);
    const receiverRef = await writeHistory(plan.evidenceWriter, receiver.identity, receiverRecords, signal);
    if (senderRef.path === receiverRef.path) throw new Error("evidence writer reused one artifact path for both actor histories");
    const senderHistory: GenericActorHistory = Object.freeze({ actor: sender.identity, records: senderRecords });
    const receiverHistory: GenericActorHistory = Object.freeze({ actor: receiver.identity, records: receiverRecords });
    const collected = collectGenericExchange({ sender: sender.identity, receiver: receiver.identity, nonce: plan.pair.nonce, requestContent, expectedReplyContent, sent, received: receivedMessage, replied, returned: returnedMessage, traces: [...extractGenericTraces(senderRecords, sender.identity, senderRef), ...extractGenericTraces(receiverRecords, receiver.identity, receiverRef)], histories: { sender: senderHistory, receiver: receiverHistory }, priorMessageIds: [] });
    return Object.freeze({ trialId: plan.pair.pairId, pair: plan.pair, kind: "communication", execution: { status: "completed" as const }, lease, exchange: collected.evidence, raw: Object.freeze([senderRef, receiverRef]) });
  });

export type MixedPairPlan = Readonly<{ pair: PairSpec; left: number; right: number; evidenceWriter: TypedEvidenceWriter }>;
const nativeModelTraces = (participant: ModelParticipant, history: unknown, raw: RawEvidenceRef): readonly import("./qualification-evidence.js").NativeTrace[] => participant.identity.route === "opencode-acp"
  ? extractOpenCodeAcpTraces(history, participant.identity, raw)
  : participant.identity.route.startsWith("opencode-")
    ? extractOpenCodeTraces(history, participant.identity, raw)
    : extractNativeTraces(history, participant.identity, raw);

/** Run one generic/model pair through generic calls on one side and native prompt/history on the other. */
export const runMixedPair = async (pool: PairLeasePool, plan: MixedPairPlan, signal: AbortSignal): Promise<TrialEvidence> =>
  withPairLease(pool, plan.pair, signal, async ({ lease, sender: rawSender, receiver: rawReceiver }) => {
    if (rawSender.kind === rawReceiver.kind) throw new Error("mixed pair requires exactly one generic and one model participant");
    const model = (rawSender.kind === "model" ? rawSender : rawReceiver) as ModelParticipant;
    const generic = (rawSender.kind === "generic" ? rawSender : rawReceiver) as GenericParticipant;
    const modelRole = rawSender.kind === "model" ? "sender" : "receiver";
    const bounded = deadlineSignal(signal, 180_000);
    const operationSignal = bounded.signal;
    const requestContent = `qualification ${plan.pair.nonce}: calculate ${plan.left}+${plan.right}`;
    const expectedReplyContent = `answer ${plan.pair.nonce}: ${plan.left + plan.right}`;
    const modelStatus = await model.status(operationSignal);
    if (modelStatus.kind === "busy" || modelStatus.kind === "terminated") throw new Error("mixed pair cannot prompt a busy or terminated participant");
    let latestModel: unknown;
    let latestGeneric: readonly GenericCallRecord[] | undefined;
    let refs: readonly RawEvidenceRef[] = Object.freeze([]);
    const peerController = new AbortController();
    const peerSignal = AbortSignal.any([operationSignal, peerController.signal]);
    try {
      let promptFailure: unknown;
      const prompt = model.prompt(legacyModelPrompt(modelRole, plan.pair, rawSender.identity, rawReceiver.identity, requestContent, expectedReplyContent), peerSignal);
      void prompt.catch((error) => { promptFailure = error; peerController.abort(error); });
      if (rawSender.kind === "generic") {
        await generic.call("send_message", { to: rawReceiver.identity.agent, type: "task", content: requestContent, idempotency_key: `${plan.pair.nonce}:request` }, peerSignal);
        await receiveGenericUntilMessage(generic, peerSignal);
      } else {
        const received = receiveGenericUntilMessage(generic, peerSignal);
        const receivedResult = await received;
        const receivedMessage = receivedResult.message;
        if (!receivedMessage || typeof receivedMessage !== "object" || typeof (receivedMessage as Record<string, unknown>).id !== "string") throw new Error("mixed receive response has no exact message ID");
        await generic.call("send_message", { to: rawSender.identity.agent, type: "result", content: expectedReplyContent, in_reply_to: (receivedMessage as Record<string, unknown>).id, idempotency_key: `${plan.pair.nonce}:reply` }, peerSignal);
      }
      let exchange: ReturnType<typeof collectNativeExchange> | undefined;
      let collectorError: unknown;
      const deadline = Date.now() + 180_000;
      while (Date.now() < deadline) {
        [latestModel, latestGeneric] = await Promise.all([model.history(operationSignal), generic.history(operationSignal)]);
        const modelRef = await writeNativeHistory(plan.evidenceWriter, model.identity, latestModel, operationSignal);
        const genericRef = await writeHistory(plan.evidenceWriter, generic.identity, latestGeneric, operationSignal);
        if (modelRef.path === genericRef.path) throw new Error("evidence writer reused one artifact path for mixed histories");
        refs = Object.freeze([modelRef, genericRef]);
        try {
          const modelTraces = nativeModelTraces(model, latestModel, modelRef);
          const genericTraces = extractGenericTraces(latestGeneric, generic.identity, genericRef, true);
          const senderTraces = rawSender.kind === "model" ? modelTraces : genericTraces;
          const receiverTraces = rawReceiver.kind === "model" ? modelTraces : genericTraces;
          exchange = collectNativeExchange({ sender: { actor: rawSender.identity, traces: senderTraces } satisfies NativeExchangeParticipant, receiver: { actor: rawReceiver.identity, traces: receiverTraces } satisfies NativeExchangeParticipant, nonce: plan.pair.nonce, requestContent, expectedReplyContent, consumption: "legacy_receive" });
          break;
        } catch (error) {
          collectorError = error;
          if (promptFailure !== undefined) throw promptFailure;
          await wait(500, operationSignal);
        }
      }
      if (!exchange) {
        await prompt;
        const detail = collectorError instanceof Error ? ` Last collector error: ${collectorError.message}` : "";
        throw new Error(`mixed pair did not produce an exact native exchange within 180 seconds.${detail}`);
      }
      const lifecycle = await settlePrompt(prompt, peerController);
      return Object.freeze({ trialId: plan.pair.pairId, pair: plan.pair, kind: "communication", execution: { status: "completed" as const, detail: `communication proof captured; prompt lifecycle ${lifecycle}` }, lease, exchange: exchange.evidence, raw: refs });
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      if (refs.length === 0) {
        try {
          const captureSignal = AbortSignal.timeout(10_000);
          latestModel = latestModel ?? await model.history(captureSignal);
          latestGeneric = latestGeneric ?? await generic.history(captureSignal);
          const modelRef = await writeNativeHistory(plan.evidenceWriter, model.identity, latestModel, captureSignal);
          const genericRef = await writeHistory(plan.evidenceWriter, generic.identity, latestGeneric, captureSignal);
          if (modelRef.path === genericRef.path) throw new Error("evidence writer reused one artifact path for mixed histories");
          refs = Object.freeze([modelRef, genericRef]);
        } catch { /* preserve the primary failure */ }
      }
      Object.assign(failure, { raw: refs, histories: { model: latestModel, generic: latestGeneric } });
      throw failure;
    } finally { peerController.abort(new Error("mixed pair operation finished")); bounded.cancel(); }
  });

export type ModelPairPlan = Readonly<{ pair: PairSpec; left: number; right: number; evidenceWriter: TypedEvidenceWriter; consumption?: Parameters<typeof collectNativeExchange>[0]["consumption"] }>;
const asModel = (value: unknown): ModelParticipant => {
  if (!value || typeof value !== "object" || (value as { kind?: unknown }).kind !== "model") throw new Error("model pair driver requires model participants");
  return value as ModelParticipant;
};
const wait = (ms: number, signal: AbortSignal): Promise<void> => new Promise((resolveWait, reject) => {
  if (signal.aborted) { reject(signal.reason ?? new Error("operation aborted")); return; }
  const timer = setTimeout(resolveWait, ms);
  signal.addEventListener("abort", () => { clearTimeout(timer); reject(signal.reason ?? new Error("operation aborted")); }, { once: true });
});
const receiveGenericUntilMessage = async (participant: GenericParticipant, signal: AbortSignal): Promise<Record<string, unknown>> => {
  while (true) {
    try {
      const value = await participant.call("receive_message", { timeout: 5 }, signal);
      if (value && typeof value === "object" && (value as Record<string, unknown>).status === "message") return value as Record<string, unknown>;
    } catch (error) {
      if (signal.aborted) throw error;
    }
    await wait(100, signal);
  }
};
const modelPrompt = (role: "sender" | "receiver", pair: PairSpec, sender: ParticipantIdentity, receiver: ParticipantIdentity, requestContent: string, expectedReplyContent: string): string => role === "receiver"
  ? `You are the assisted receiver for qualification ${pair.nonce}. Repeatedly use the native claim_tasks tool until you claim a task from ${sender.agent}. For the exact task content ${requestContent}, compute the decimal answer yourself, send exactly one result to ${sender.agent} using the answer template answer ${pair.nonce}: <computed decimal>, type result, and in_reply_to equal to the exact id field of the claimed task message (its UUID; never the task content, nonce text, or any other value). Then acknowledge the exact claim with acknowledge_tasks. After acknowledge_tasks succeeds, stop immediately: do not claim again or send another message. Do not answer by prompt text; the qualified communication must be caused by the claimed task.`
  : `Send exactly one fresh task to ${receiver.agent} using the native send_message tool: type task, content ${requestContent}, and a fresh idempotency key. Then repeatedly claim_tasks until you claim the correlated result, verify its in_reply_to names your task ID and its content is ${expectedReplyContent}, acknowledge that exact claim, and report completion.`;
const legacyModelPrompt = (role: "sender" | "receiver", pair: PairSpec, sender: ParticipantIdentity, receiver: ParticipantIdentity, requestContent: string, expectedReplyContent: string): string => role === "receiver"
  ? `You are the assisted receiver for qualification ${pair.nonce}. Repeatedly use the native receive_message tool with timeout: 60 until you receive a task from ${sender.agent}. For the exact task content ${requestContent}, compute the decimal answer yourself, send exactly one result to ${sender.agent} using the answer template answer ${pair.nonce}: <computed decimal>, type result, and in_reply_to equal to the exact id field of the received task message (its UUID; never the task content, nonce text, or any other value). The reply content must be exactly answer ${pair.nonce}: <computed decimal> with no other words or arithmetic shown. Stop immediately after the result send succeeds. Do not answer by prompt text; the qualified communication must be caused by the received task.`
  : `Send exactly one fresh task to ${receiver.agent} using the native send_message tool: type task, content ${requestContent}, and a fresh idempotency key. Then repeatedly use the native receive_message tool with timeout: 60 until you receive the correlated result, verify its in_reply_to names your task ID and its content is ${expectedReplyContent}, and report completion.`;
const settlePrompt = async (prompts: Promise<unknown>, controller: AbortController, milliseconds = 2_000): Promise<"settled" | "timed_out"> => {
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const lifecycle = await Promise.race<"settled" | "timed_out">([
      prompts.then(() => "settled" as const, () => "settled" as const),
      new Promise<"timed_out">((resolve) => { timer = setTimeout(() => resolve("timed_out"), milliseconds); }),
    ]);
    if (lifecycle === "timed_out") controller.abort(new Error("exchange proof captured; prompt lifecycle exceeded grace period"));
    return lifecycle;
  } finally { if (timer) clearTimeout(timer); }
};
const deadlineSignal = (parent: AbortSignal, milliseconds: number): Readonly<{ signal: AbortSignal; cancel: () => void }> => {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(new Error(`model pair exceeded ${milliseconds}ms deadline`)), milliseconds);
  const abort = () => controller.abort(parent.reason ?? new Error("operation aborted"));
  if (parent.aborted) abort(); else parent.addEventListener("abort", abort, { once: true });
  return Object.freeze({ signal: controller.signal, cancel: () => { clearTimeout(timer); parent.removeEventListener("abort", abort); } });
};

/** Run one model-backed exchange through prompt/history/status only; no model call API is used. */
export const runModelPair = async (pool: PairLeasePool, plan: ModelPairPlan, signal: AbortSignal): Promise<TrialEvidence> =>
  withPairLease(pool, plan.pair, signal, async ({ lease, sender: rawSender, receiver: rawReceiver }) => {
    const sender = asModel(rawSender), receiver = asModel(rawReceiver);
    const bounded = deadlineSignal(signal, 180_000);
    const operationSignal = bounded.signal;
    const promptController = new AbortController();
    const promptSignal = AbortSignal.any([operationSignal, promptController.signal]);
    const requestContent = `qualification ${plan.pair.nonce}: calculate ${plan.left}+${plan.right}`;
    const expectedReplyContent = `answer ${plan.pair.nonce}: ${plan.left + plan.right}`;
    const consumption = plan.consumption ?? "claim_ack";
    const requestConsumption = typeof consumption === "string" ? consumption : consumption.request;
    const replyConsumption = typeof consumption === "string" ? consumption : consumption.reply;
    const initial = await Promise.all([sender.status(operationSignal), receiver.status(operationSignal)]);
    if (initial.some((status) => status.kind === "busy" || status.kind === "terminated")) throw new Error("model pair cannot prompt a busy or terminated participant");
    const prompts = Promise.all([
      receiver.prompt(requestConsumption === "legacy_receive" ? legacyModelPrompt("receiver", plan.pair, sender.identity, receiver.identity, requestContent, expectedReplyContent) : modelPrompt("receiver", plan.pair, sender.identity, receiver.identity, requestContent, expectedReplyContent), promptSignal),
      sender.prompt(replyConsumption === "legacy_receive" ? legacyModelPrompt("sender", plan.pair, sender.identity, receiver.identity, requestContent, expectedReplyContent) : modelPrompt("sender", plan.pair, sender.identity, receiver.identity, requestContent, expectedReplyContent), promptSignal),
    ]);
    let promptFailure: unknown;
    void prompts.catch((error) => { promptFailure = error; promptController.abort(error); });
    const deadline = Date.now() + 180_000;
    let latestSender: unknown;
    let latestReceiver: unknown;
    let exchange: ReturnType<typeof collectNativeExchange> | undefined;
    let refs: readonly RawEvidenceRef[] = Object.freeze([]);
      let collectorError: unknown;
    try {
      while (Date.now() < deadline) {
        [latestSender, latestReceiver] = await Promise.all([sender.history(promptSignal), receiver.history(promptSignal)]);
        const senderRef = await writeNativeHistory(plan.evidenceWriter, sender.identity, latestSender, promptSignal);
        const receiverRef = await writeNativeHistory(plan.evidenceWriter, receiver.identity, latestReceiver, promptSignal);
        if (senderRef.path === receiverRef.path) throw new Error("evidence writer reused one artifact path for both native histories");
        refs = Object.freeze([senderRef, receiverRef]);
        try {
          const senderTraces = nativeModelTraces(sender, latestSender, senderRef);
          const receiverTraces = nativeModelTraces(receiver, latestReceiver, receiverRef);
          exchange = collectNativeExchange({ sender: { actor: sender.identity, traces: senderTraces } satisfies NativeExchangeParticipant, receiver: { actor: receiver.identity, traces: receiverTraces } satisfies NativeExchangeParticipant, nonce: plan.pair.nonce, requestContent, expectedReplyContent, consumption });
          break;
        } catch (error) {
          collectorError = error;
          if (promptFailure !== undefined) throw promptFailure;
          await wait(500, promptSignal);
        }
      }
      if (!exchange) {
        await prompts;
        const detail = collectorError instanceof Error ? ` Last collector error: ${collectorError.message}` : "";
        throw new Error(`model pair did not produce an exact native exchange within 180 seconds.${detail}`);
      }
      const lifecycle = await settlePrompt(prompts, promptController);
      return Object.freeze({ trialId: plan.pair.pairId, pair: plan.pair, kind: "communication", execution: { status: "completed" as const, detail: `communication proof captured; prompt lifecycle ${lifecycle}` }, lease, exchange: exchange.evidence, raw: refs });
    } catch (error) {
      const failure = error instanceof Error ? error : new Error(String(error));
      if (collectorError !== undefined) Object.assign(failure, { cause: collectorError });
      if (refs.length === 0) {
        try {
          const captureSignal = AbortSignal.timeout(10_000);
          latestSender = latestSender ?? await sender.history(captureSignal);
          latestReceiver = latestReceiver ?? await receiver.history(captureSignal);
          const senderRef = await writeNativeHistory(plan.evidenceWriter, sender.identity, latestSender, captureSignal);
          const receiverRef = await writeNativeHistory(plan.evidenceWriter, receiver.identity, latestReceiver, captureSignal);
          if (senderRef.path === receiverRef.path) throw new Error("evidence writer reused one artifact path for both native histories");
          refs = Object.freeze([senderRef, receiverRef]);
        } catch { /* preserve the primary failure */ }
      }
      Object.assign(failure, { raw: refs, histories: { sender: latestSender, receiver: latestReceiver } });
      throw failure;
    } finally { promptController.abort(new Error("model pair operation finished")); bounded.cancel(); }
  });
