import { createHash, randomUUID } from "node:crypto";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it, vi } from "vitest";
import { createPiNativeChildAdapter, enterPiNativeChildProfileScope, getPiNativeChildCleanupEvidence, type PiNativeChildParticipant } from "./qualification-pi-child.js";
import { startOwnedRedis, type OwnedRedis } from "./owned-redis.js";
import type { Participant } from "./qualification-types.js";
import { sanitizeEvidence } from "./public-evidence.js";
import { nodePrefixPath } from "./local-tools.js";

const enabled = process.env.GPTQUEUE_QUALIFICATION_PI_NATIVE_CHILD === "1";
const repo = resolve(import.meta.dirname, "../..");
const artifactRoot = join(repo, ".gptqueue/repair-qualification/20260912/qualification-pi-native-child");
const sourceFiles = [
  "tests/acceptance/qualification-pi-child.ts", "tests/acceptance/qualification-pi-child-conformance.test.ts", "tests/acceptance/qualification-types.ts", "tests/acceptance/owned-redis.ts", "src/registered-shell/pi-extension.ts", "dist/registered-shell/pi-extension.js",
] as const;
vi.setConfig({ testTimeout: 900_000, hookTimeout: 60_000 });
type Json = Record<string, unknown>;
const digest = async (file: string): Promise<string> => createHash("sha256").update(await readFile(join(repo, file))).digest("hex");
const identity = (participant: Participant): Json => ({ participant_id: participant.identity.participantId, route: participant.identity.route, host_runtime_id: participant.identity.hostRuntimeId, agent: participant.identity.agent, cwd_hash: participant.identity.cwdHash, profile_hash: participant.identity.profileHash, epoch_hash: participant.identity.epochHash, provenance: "provenance" in participant ? participant.provenance : undefined });
const object = (value: unknown): Json | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const parse = (value: unknown): unknown => {
  if (typeof value !== "string") return value;
  try { return JSON.parse(value); } catch { return value; }
};
const nestedObjects = (value: unknown): readonly Json[] => {
  if (typeof value === "string") {
    const parsed = parse(value);
    return parsed === value ? [] : nestedObjects(parsed);
  }
  if (Array.isArray(value)) return value.flatMap(nestedObjects);
  const current = object(value);
  return current ? [current, ...Object.values(current).flatMap(nestedObjects)] : [];
};
const piMessages = (history: unknown): readonly Json[] => Array.isArray(history)
  ? history.map(value => object(object(value)?.message) ?? object(value)).filter((value): value is Json => value !== undefined)
  : [];
const toolCalls = (messages: readonly Json[], name: string): readonly Json[] => messages.filter(message => message.role === "assistant" && Array.isArray(message.content))
  .flatMap(message => (message.content as unknown[]).map(object).filter((value): value is Json => value?.type === "toolCall" && value.name === name && typeof value.id === "string"));
const callResult = (messages: readonly Json[], call: Json): readonly Json[] => messages
  .filter(message => message.role === "toolResult" && message.toolCallId === call.id && message.toolName === call.name)
  .flatMap(nestedObjects);
const callInput = (call: Json): Json => object(parse(call.arguments)) ?? {};
const successfulCall = (messages: readonly Json[], name: string, predicate: (input: Json, output: readonly Json[]) => boolean): Json | undefined => toolCalls(messages, name)
  .map(call => ({ call, output: callResult(messages, call) }))
  .find(({ call, output }) => predicate(callInput(call), output))?.call;
const sentMessageId = (messages: readonly Json[], call: Json): string | undefined => callResult(messages, call)
  .find(output => output.status === "sent" && typeof output.message_id === "string")?.message_id as string | undefined;
const hasLegacyReceive = (messages: readonly Json[], messageId: string): boolean => toolCalls(messages, "receive_message").some(call => callResult(messages, call)
  .some(output => output.status === "message" && object(output.message)?.id === messageId));

describe("Pi native-child history normalization", () => {
  it("handles text leaves, encoded objects, nested arrays, and primitive encoded text", () => {
    expect(nestedObjects("ok")).toEqual([]);
    expect(nestedObjects(JSON.stringify({ status: "ok", nested: { value: 1 } }))).toEqual([
      { status: "ok", nested: { value: 1 } },
      { value: 1 },
    ]);
    expect(nestedObjects([{ first: { value: 1 } }, [{ second: true }]])).toEqual([
      { first: { value: 1 } }, { value: 1 }, { second: true },
    ]);
    expect(nestedObjects(JSON.stringify("encoded text"))).toEqual([]);
  });
});

const assertNativePeerExchange = (histories: readonly unknown[], sender: Participant, receiver: Participant, marker: string): void => {
  const senderMessages = piMessages(histories[0]);
  const receiverMessages = piMessages(histories[1]);
  const requestContent = `Reply with exactly ${marker}`;
  const senderSend = successfulCall(senderMessages, "send_message", (input, output) => input.to === receiver.identity.agent && input.type === "task" && input.content === requestContent && output.some(value => value.status === "sent" && value.to === receiver.identity.agent));
  if (!senderSend) throw new Error("native sender has no exact peer task send");
  const requestId = sentMessageId(senderMessages, senderSend);
  if (!requestId) throw new Error("native peer task send has no exact message_id");
  if (!hasLegacyReceive(receiverMessages, requestId)) throw new Error("native receiver did not consume the exact peer task");
  const receiverSend = successfulCall(receiverMessages, "send_message", (input, output) => input.to === sender.identity.agent && input.type === "result" && input.content === marker && input.in_reply_to === requestId && output.some(value => value.status === "sent" && value.to === sender.identity.agent));
  if (!receiverSend) throw new Error("native receiver has no exact correlated peer result");
  const replyId = sentMessageId(receiverMessages, receiverSend);
  if (!replyId) throw new Error("native peer result has no exact message_id");
  if (!hasLegacyReceive(senderMessages, replyId)) throw new Error("native sender did not consume the exact peer result");
  if (!toolCalls(senderMessages, "send_message").some(call => callInput(call).content === requestContent && callInput(call).to === receiver.identity.agent)) throw new Error("native sender peer call arguments were not exact");
};

describe.skipIf(!enabled)("Pi native-child qualification", () => {
  it("proves two actual Agent child sessions have independent lineage and GPTQueue replies", async () => {
    const provider = process.env.GPTQUEUE_PI_PROVIDER;
    const model = process.env.GPTQUEUE_PI_MODEL;
    if (!provider || !model) throw new Error("GPTQUEUE_PI_PROVIDER and GPTQUEUE_PI_MODEL are required");
    const runId = randomUUID();
    const artifactDir = join(artifactRoot, runId);
    await mkdir(artifactDir, { recursive: true, mode: 0o700 });
    const receipt: Json = { schema_version: 1, run_id: runId, route: "pi-native-child", passed: false, execution: { status: "running" }, provider, model, source_hashes: {}, phases: [], cleanup: [] };
    const persist = async (label: string, value: unknown): Promise<void> => {
      const phase = { label, at: new Date().toISOString(), value: sanitizeEvidence(value, { parseEmbeddedJson: true }) };
      (receipt.phases as Json[]).push(phase);
      await writeFile(join(artifactDir, `${String((receipt.phases as Json[]).length).padStart(4, "0")}-${label}.json`), `${JSON.stringify(phase, null, 2)}\n`, { mode: 0o600 });
      await writeFile(join(artifactDir, "receipt-sanitized.json"), `${JSON.stringify(sanitizeEvidence(receipt, { parseEmbeddedJson: true }), null, 2)}\n`, { mode: 0o600 });
    };
    let redis: OwnedRedis | undefined;
    let workspace: string | undefined;
    let sender: Participant | undefined;
    let receiver: Participant | undefined;
    let failure: unknown;
    let releaseProfile: (() => void) | undefined;
    try {
      receipt.source_hashes = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(file)])));
      await persist("source-hashes", receipt.source_hashes);
      redis = await startOwnedRedis();
      workspace = join(artifactDir, "workspace");
      const profile = join(artifactDir, "profile");
      await mkdir(profile, { recursive: true, mode: 0o700 });
      const sdk = await import(pathToFileURL(join(nodePrefixPath("lib/node_modules/@earendil-works/pi-coding-agent/dist"), "index.js")).href) as any;
      const modelRuntime = await sdk.ModelRuntime.create({ allowModelNetwork: false });
      const modelValue = modelRuntime.getModel(provider, model);
      expect(modelValue).toBeTruthy();
      expect(modelRuntime.hasConfiguredAuth(provider)).toBe(true);
      releaseProfile = enterPiNativeChildProfileScope(profile);
      await persist("profile-scope", { pi_coding_agent_dir: resolve(profile), selection: "owned process-lifetime profile", model_runtime: "preloaded before environment scope" });
      const adapter = createPiNativeChildAdapter({ workspaceRoot: workspace, provider, model, profileRoot: profile, modelRuntime });
      const signal = AbortSignal.timeout(840_000);
      const availability = await adapter.preflight(signal);
      await persist("preflight", availability);
      expect(availability.kind).toBe("available");
      sender = await adapter.launch({ role: "sender", pairId: "pi-native-child-conformance", nonce: `${runId}-sender`, redisUrl: redis.url }, signal);
      receiver = await adapter.launch({ role: "receiver", pairId: "pi-native-child-conformance", nonce: `${runId}-receiver`, redisUrl: redis.url }, signal);
      receipt.identities = { sender: identity(sender), receiver: identity(receiver) };
      await persist("launched", receipt.identities);
      expect(sender.identity.hostRuntimeId).not.toBe(receiver.identity.hostRuntimeId);
      expect(sender.identity.agent).not.toBe(receiver.identity.agent);
      expect((await Promise.all([sender.status(signal), receiver.status(signal)])).every(status => status.kind === "unknown")).toBe(true);
      if (sender.kind !== "model" || receiver.kind !== "model") throw new Error("Pi native-child route did not launch model participants");
      const peerMarker = `NATIVE_CHILD_PEER_${runId.replaceAll("-", "_").toUpperCase()}`;
      const senderReply = await sender.prompt([
        "Call get_runtime_status exactly once.",
        `Send one task to ${receiver.identity.agent} with content "Reply with exactly ${peerMarker}".`,
        "Wait for the correlated result, then reply to the controller with NATIVE_CHILD_SENDER_DONE.",
      ].join(" "), signal);
      expect(String(senderReply)).toContain("NATIVE_CHILD_SENDER_DONE");
      const histories = await Promise.all([sender.history(signal), receiver.history(signal)]);
      await persist("history", histories);
      assertNativePeerExchange(histories, sender, receiver, peerMarker);
      receipt.execution = { status: "completed" };
      receipt.passed = true;
    } catch (error) {
      failure = error;
      receipt.error = String(error);
      receipt.execution = { status: "failed", detail: String(error) };
      const failureSignal = AbortSignal.timeout(10_000);
      const failureHistories = await Promise.all([
        sender?.history(failureSignal).catch(historyError => ({ history_error: String(historyError) })),
        receiver?.history(failureSignal).catch(historyError => ({ history_error: String(historyError) })),
      ]);
      await persist("failure-history", failureHistories).catch(() => undefined);
      await persist("failure", { error: String(error) }).catch(() => undefined);
    } finally {
      const cleanup: Json[] = [];
      const actions: readonly [string, (() => Promise<unknown>) | undefined][] = [
        ["sender.close", sender ? () => sender!.close() : undefined],
        ["receiver.close", receiver ? () => receiver!.close() : undefined],
        ["redis.close", redis ? () => redis!.close() : undefined],
        ["workspace.remove", workspace ? async () => rm(workspace!, { recursive: true, force: true }) : undefined],
      ];
      for (const [name, action] of actions) {
        if (!action) { cleanup.push({ name, status: "skipped", reason: "not_started" }); continue; }
        try {
          const observation = await action();
          const participant = name === "sender.close" ? sender : name === "receiver.close" ? receiver : undefined;
          cleanup.push({ name, status: "fulfilled", observation: observation ?? (participant ? getPiNativeChildCleanupEvidence(participant as PiNativeChildParticipant) : undefined) });
        } catch (error) {
          const participant = name === "sender.close" ? sender : name === "receiver.close" ? receiver : undefined;
          cleanup.push({ name, status: "rejected", error: String(error), cleanup_evidence: participant ? getPiNativeChildCleanupEvidence(participant as PiNativeChildParticipant) : undefined });
        }
      }
      receipt.cleanup = cleanup;
      receipt.profile_scope_restored = false;
      receipt.source_hashes_after = Object.fromEntries(await Promise.all(sourceFiles.map(async file => [file, await digest(file)])));
      receipt.source_hashes_match = JSON.stringify(receipt.source_hashes) === JSON.stringify(receipt.source_hashes_after);
      if (receipt.source_hashes_match !== true || cleanup.some(item => item.status === "rejected")) { receipt.passed = false; failure ??= new Error("Pi native-child cleanup/source verification failed"); }
      if (!failure && !cleanup.some(item => item.status === "rejected")) {
        releaseProfile?.();
        releaseProfile = undefined;
        receipt.profile_scope_restored = true;
      }
      receipt.ended_at = new Date().toISOString();
      await persist("cleanup", cleanup).catch(() => undefined);
    }
    if (failure) throw failure;
    expect(receipt.passed).toBe(true);
  });
});
