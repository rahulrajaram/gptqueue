import { describe, expect, it } from "vitest";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";

type ObjectRecord = Record<string, unknown>;
const root = join(process.cwd(), ".gptqueue/acceptance/20260912-evaluation/opencode-variants/15181ebc-4801-486a-9b6b-0025a7bbc1a6");
const receiptPath = join(root, "receipt.json");
const tuiConsumptionPath = join(root, "tui-consumption.json");
const asRecord = (value: unknown): ObjectRecord | undefined => value && typeof value === "object" && !Array.isArray(value) ? value as ObjectRecord : undefined;
const readRecord = (path: string): ObjectRecord => {
  const parsed: unknown = JSON.parse(readFileSync(path, "utf8"));
  const record = asRecord(parsed);
  if (!record) throw new Error(`expected JSON object in ${path}`);
  return record;
};

describe.skipIf(!existsSync(receiptPath) || !existsSync(tuiConsumptionPath))("OpenCode native terminal evidence proofs", () => {
  it("proves the TUI received the independent peer reply envelope", () => {
    const receipt = readRecord(receiptPath);
    const routes = asRecord(receipt.routes);
    const tui = asRecord(routes?.tui);
    const tuiResult = asRecord(tui?.result);
    const request = asRecord(tuiResult?.exact_request);
    const sentReply = asRecord(tuiResult?.exact_reply);
    const tuiAgent = tuiResult?.agent;
    const peer = asRecord(tui?.observation)?.exact_reference_peer;
    const nonce = tuiResult?.nonce;
    const derivative = readRecord(tuiConsumptionPath);
    const parts = Array.isArray(derivative.parts) ? derivative.parts : [];
    const receivePart = parts
      .map(asRecord)
      .find((part) => part?.type === "tool" && part.tool === "gptqueue_receive_message");
    const state = asRecord(receivePart?.state);
    const received = asRecord(state?.output);
    const receivedPayload = asRecord(received?.payload);
    const requestId = request?.id;
    const replyId = sentReply?.message_id;
    expect(derivative.source).toContain("installed OpenCode native SQLite");
    expect(request?.from).toBe(tuiAgent);
    expect(request?.to).toBe(peer);
    expect(asRecord(request?.payload)?.content).toBe(nonce);
    expect(sentReply?.to).toBe(tuiAgent);
    expect(replyId).toBe(received?.id);
    expect(received?.from).toBe(peer);
    expect(received?.to).toBe(tuiAgent);
    expect(receivedPayload?.in_reply_to).toBe(requestId);
    expect(receivedPayload?.content).toBe(`reply-${String(nonce)}`);
    expect(state?.status).toBe("completed");
  });

  it("proves ACP completed send and receive notifications correlate by message id", () => {
    const receipt = readRecord(receiptPath);
    const acp = asRecord(asRecord(receipt.routes)?.acp);
    const result = asRecord(acp?.result);
    const notifications = Array.isArray(result?.notifications) ? result.notifications : [];
    const updates = notifications
      .map(asRecord)
      .map((message) => asRecord(message?.params))
      .map((params) => asRecord(params?.update))
      .filter((update): update is ObjectRecord => update?.sessionUpdate === "tool_call_update" && update.status === "completed");
    const toolTitles = new Map<string, string>();
    for (const notification of notifications) {
      const update = asRecord(asRecord(asRecord(notification)?.params)?.update);
      if (update?.sessionUpdate === "tool_call" && typeof update.toolCallId === "string" && typeof update.title === "string") toolTitles.set(update.toolCallId, update.title);
    }
    const titleOf = (update: ObjectRecord): string => typeof update.toolCallId === "string" ? toolTitles.get(update.toolCallId) ?? String(update.title ?? "") : String(update.title ?? "");
    const output = (update: ObjectRecord): ObjectRecord | undefined => asRecord(asRecord(update.rawOutput)?.output) ?? asRecord(update.output);
    const sendUpdate = updates.find((update) => titleOf(update).endsWith("send_message"));
    const receiveUpdate = updates.find((update) => titleOf(update).endsWith("receive_message"));
    const send = sendUpdate ? output(sendUpdate) : undefined;
    const receive = receiveUpdate ? output(receiveUpdate) : undefined;
    const receivePayload = asRecord(receive?.payload);
    expect(send?.status).toBe("sent");
    expect(typeof send?.message_id).toBe("string");
    expect(receive?.id).toBe(send?.message_id);
    expect(receive?.from).toBe(result?.agent);
    expect(receive?.to).toBe(result?.agent);
    expect(receivePayload?.content).toBe(result?.nonce);
  });
});
