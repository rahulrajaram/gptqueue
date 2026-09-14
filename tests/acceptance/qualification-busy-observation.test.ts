import { describe, expect, it } from "vitest";
import { assertBusyDeliveryObservation, type BusyDeliveryObservation } from "./qualification-busy-task.js";

const actor = { participantId: "model", route: "codex-appserver" as const, hostRuntimeId: "runtime-1", agent: "model", cwdHash: "cwd", profileHash: "profile", epochHash: "epoch" };
const history = (...turns: Record<string, unknown>[]): Record<string, unknown> => ({ id: actor.hostRuntimeId, turns });
const turn = (id: string, status: string, text?: string): Record<string, unknown> => ({ id, status, items: text === undefined ? [] : [{ type: "agentMessage", phase: "final_answer", text }] });
const fixture = (): BusyDeliveryObservation => ({
  actor,
  baselineTurnIds: ["setup"],
  before: { observedAt: 10, runtimeId: actor.hostRuntimeId, history: history(turn("setup", "completed"), turn("busy", "inProgress")) },
  after: { observedAt: 30, runtimeId: actor.hostRuntimeId, history: history(turn("setup", "completed"), turn("busy", "inProgress")) },
  beforeStatus: { kind: "busy", runtimeId: actor.hostRuntimeId }, afterStatus: { kind: "busy", runtimeId: actor.hostRuntimeId }, sendStartedAt: 20, sendCompletedAt: 22, sentMessageId: "message-1",
  controllerLedger: [{ kind: "busy_prompt_started", at: 9 }, { kind: "generic_send", at: 22, message_id: "message-1" }],
  acceptedPrompt: { turn: { id: "busy" } },
  final: { observedAt: 50, runtimeId: actor.hostRuntimeId, history: history(turn("setup", "completed"), turn("busy", "completed", "busy answer: 42"), turn("later", "completed", "later answer")) },
  laterActivationTurnId: "later", expectedBusyFinalAnswer: "busy answer: 42",
});

describe("busy delivery observation oracle", () => {
  it("accepts a genuinely overlapping busy delivery and later activation", () => expect(assertBusyDeliveryObservation(fixture())).toBe("busy"));
  it.each([
    ["wrong runtime", (v: BusyDeliveryObservation) => ({ ...v, after: { ...v.after, runtimeId: "other" } })],
    ["wrong history root", (v: BusyDeliveryObservation) => ({ ...v, final: { ...v.final, history: { id: "other", turns: [turn("setup", "completed"), turn("busy", "completed", "busy answer: 42"), turn("later", "completed", "later answer")] } } })],
    ["duplicate busy turns", (v: BusyDeliveryObservation) => ({ ...v, after: { ...v.after, history: history(turn("setup", "completed"), turn("busy", "inProgress"), turn("other", "inProgress")) } })],
    ["already completed after send", (v: BusyDeliveryObservation) => ({ ...v, after: { ...v.after, history: history(turn("setup", "completed"), turn("busy", "completed")) } })],
    ["wrong turn acceptance", (v: BusyDeliveryObservation) => ({ ...v, acceptedPrompt: { turn: { id: "other" } } })],
    ["wrong status", (v: BusyDeliveryObservation) => ({ ...v, afterStatus: { kind: "idle", runtimeId: actor.hostRuntimeId } as const })],
    ["missing generic send", (v: BusyDeliveryObservation) => ({ ...v, controllerLedger: v.controllerLedger.filter(event => event.kind !== "generic_send") })],
    ["wrong sent message ID", (v: BusyDeliveryObservation) => ({ ...v, sentMessageId: "message-2" })],
    ["duplicate generic sends", (v: BusyDeliveryObservation) => ({ ...v, controllerLedger: [...v.controllerLedger, { kind: "generic_send", at: 21, message_id: "message-1" }] })],
    ["late generic send", (v: BusyDeliveryObservation) => ({ ...v, controllerLedger: v.controllerLedger.map(event => event.kind === "generic_send" ? { ...event, at: 40 } : event) })],
    ["early generic send", (v: BusyDeliveryObservation) => ({ ...v, controllerLedger: v.controllerLedger.map(event => event.kind === "generic_send" ? { ...event, at: 19 } : event) })],
    ["generic send without message ID", (v: BusyDeliveryObservation) => ({ ...v, controllerLedger: v.controllerLedger.map(event => event.kind === "generic_send" ? { ...event, message_id: undefined } : event) })],
    ["send starts before bracket", (v: BusyDeliveryObservation) => ({ ...v, sendStartedAt: 5 })],
    ["send completes after bracket", (v: BusyDeliveryObservation) => ({ ...v, sendCompletedAt: 35 })],
    ["late controller prompt", (v: BusyDeliveryObservation) => ({ ...v, controllerLedger: [...v.controllerLedger, { kind: "controller_prompt", at: 20 }] })],
    ["late busy prompt start", (v: BusyDeliveryObservation) => ({ ...v, controllerLedger: [...v.controllerLedger, { kind: "busy_prompt_started", at: 21 }] })],
    ["final observation out of order", (v: BusyDeliveryObservation) => ({ ...v, final: { ...v.final, observedAt: 19 } })],
    ["duplicate history turn ID", (v: BusyDeliveryObservation) => ({ ...v, after: { ...v.after, history: history(turn("setup", "completed"), turn("busy", "inProgress"), turn("busy", "inProgress")) } })],
    ["missing terminal", (v: BusyDeliveryObservation) => ({ ...v, final: { ...v.final, history: history(turn("setup", "completed"), turn("later", "completed", "later answer")) } })],
    ["wrong final", (v: BusyDeliveryObservation) => ({ ...v, expectedBusyFinalAnswer: "wrong" })],
  ])("rejects %s", (_name, mutate) => expect(() => assertBusyDeliveryObservation(mutate(fixture()))).toThrow());
  it("rejects idle before-send and a baseline-reused after-send turn", () => {
    expect(() => assertBusyDeliveryObservation({ ...fixture(), beforeStatus: { kind: "idle", runtimeId: actor.hostRuntimeId } })).toThrow(/status/);
    expect(() => assertBusyDeliveryObservation({ ...fixture(), after: { ...fixture().after, history: history(turn("setup", "inProgress")) } })).toThrow();
  });
});
