import type { ParticipantIdentity, RuntimeStatus } from "./qualification-types.js";

export type BusyHistoryObservation = Readonly<{
  observedAt: number;
  runtimeId: string;
  history: unknown;
}>;

export type BusyControllerEvent = Readonly<{
  kind: string;
  at: number;
  turnId?: string;
  message_id?: string;
}>;

export type BusyPromptAcceptance = Readonly<{
  turn?: Readonly<{ id?: unknown }>;
  turnId?: unknown;
}>;

export type BusyDeliveryObservation = Readonly<{
  actor: ParticipantIdentity;
  baselineTurnIds: readonly string[];
  before: BusyHistoryObservation;
  after: BusyHistoryObservation;
  beforeStatus?: RuntimeStatus;
  afterStatus?: RuntimeStatus;
  sendStartedAt: number;
  sendCompletedAt: number;
  sentMessageId: string;
  controllerLedger: readonly BusyControllerEvent[];
  acceptedPrompt: BusyPromptAcceptance;
  final: BusyHistoryObservation;
  laterActivationTurnId: string;
  expectedBusyFinalAnswer: string;
}>;

type Json = Record<string, unknown>;
type Turn = Json & { id?: unknown; status?: unknown; items?: unknown };

const object = (value: unknown): Json | undefined => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const rootRuntimeId = (history: unknown): string => {
  const id = object(history)?.id;
  if (typeof id !== "string" || id.length === 0) throw new Error("native history has no exact runtime root ID");
  return id;
};
const turnsOf = (history: unknown): readonly Turn[] => {
  const turns = object(history)?.turns;
  if (!Array.isArray(turns)) throw new Error("native history has no turns");
  return turns.map(value => {
    const turn = object(value);
    if (!turn) throw new Error("native history contains a malformed turn");
    if (typeof turn.id !== "string" || turn.id.length === 0) throw new Error("native turn has no exact ID");
    return turn as Turn;
  });
};
const turnId = (turn: Turn): string => String(turn.id);
const statusOf = (turn: Turn): string => String(turn.status ?? "").toLowerCase();
const acceptedTurnId = (accepted: BusyPromptAcceptance): string => {
  const nested = accepted.turn?.id;
  const value = typeof nested === "string" ? nested : accepted.turnId;
  if (typeof value !== "string" || value.length === 0) throw new Error("model.prompt returned no exact accepted turn ID");
  return value;
};
const finalTexts = (turn: Turn): readonly string[] => {
  if (!Array.isArray(turn.items)) return [];
  return turn.items.flatMap(item => {
    const row = object(item);
    return row?.type === "agentMessage" && row.phase === "final_answer" && typeof row.text === "string" ? [row.text] : [];
  });
};

/**
 * Proves a task was delivered while the native model was already busy.
 * Every input is an observation captured at a separate controller boundary;
 * this function does not synthesize a busy state from the expected answer.
 */
export const assertBusyDeliveryObservation = (input: BusyDeliveryObservation): string => {
  const { actor, baselineTurnIds } = input;
  if (!actor.hostRuntimeId) throw new Error("busy task actor has no host runtime ID");
  if (!Number.isFinite(input.sendStartedAt) || !Number.isFinite(input.sendCompletedAt) || typeof input.sentMessageId !== "string" || input.sentMessageId.length === 0) throw new Error("busy task send boundary is not exact");
  const times = [input.before.observedAt, input.sendStartedAt, input.sendCompletedAt, input.after.observedAt, input.final.observedAt];
  if (times.some(value => !Number.isFinite(value)) || input.before.observedAt > input.sendStartedAt || input.sendStartedAt > input.sendCompletedAt || input.sendCompletedAt > input.after.observedAt || input.after.observedAt > input.final.observedAt) throw new Error("native observations and send boundary are not ordered");
  if (input.before.runtimeId !== actor.hostRuntimeId || input.after.runtimeId !== actor.hostRuntimeId || input.final.runtimeId !== actor.hostRuntimeId) throw new Error("native history observation has the wrong runtime");
  for (const history of [input.before.history, input.after.history, input.final.history]) if (rootRuntimeId(history) !== actor.hostRuntimeId) throw new Error("native history root has the wrong runtime");

  const baseline = new Set(baselineTurnIds);
  if ([...baselineTurnIds].some(id => id.length === 0) || baseline.size !== baselineTurnIds.length) throw new Error("baseline turn IDs must be unique and nonempty");
  const beforeTurns = turnsOf(input.before.history), afterTurns = turnsOf(input.after.history), finalTurns = turnsOf(input.final.history);
  for (const turns of [beforeTurns, afterTurns, finalTurns]) {
    const ids = turns.map(turnId);
    if (new Set(ids).size !== ids.length) throw new Error("native history contains duplicate turn IDs");
  }
  const freshBefore = beforeTurns.filter(turn => !baseline.has(turnId(turn)));
  const freshAfter = afterTurns.filter(turn => !baseline.has(turnId(turn)));
  if (freshBefore.length !== 1) throw new Error("before-send history does not contain exactly one new task turn");
  const busyTurns = freshAfter.filter(turn => statusOf(turn) === "inprogress");
  if (busyTurns.length !== 1 || freshAfter.length !== 1 || statusOf(freshBefore[0]!) !== "inprogress" || turnId(freshBefore[0]!) !== turnId(busyTurns[0]!)) throw new Error("before and after histories do not contain the same unique in-progress busy turn");
  const busyTurn = busyTurns[0]!;
  const busyId = turnId(busyTurn);

  for (const status of [input.beforeStatus, input.afterStatus]) {
    if (status !== undefined && (status.kind !== "busy" || status.runtimeId !== actor.hostRuntimeId)) throw new Error("status observation does not prove the exact runtime was busy");
  }
  if (!input.controllerLedger.some(event => event.kind === "busy_prompt_started" && Number.isFinite(event.at) && event.at < input.sendStartedAt)) throw new Error("controller ledger lacks a started busy prompt before the task boundary");
  if (input.controllerLedger.some(event => (event.kind === "controller_prompt" || event.kind === "busy_prompt_started") && event.at >= input.sendStartedAt)) throw new Error("controller prompt or busy prompt start occurred at or after the task boundary");
  const sends = input.controllerLedger.filter(event => event.kind === "generic_send" && event.message_id === input.sentMessageId);
  const bracketedSends = sends.filter(event => Number.isFinite(event.at) && event.at >= input.sendStartedAt && event.at <= input.sendCompletedAt);
  if (sends.length !== 1 || bracketedSends.length !== 1 || bracketedSends[0]!.at !== input.sendCompletedAt) throw new Error("controller ledger lacks exactly one causally bracketed generic send for the exact message ID");

  const acceptedId = acceptedTurnId(input.acceptedPrompt);
  const finalBusy = finalTurns.find(turn => turnId(turn) === acceptedId);
  if (!finalBusy || acceptedId !== busyId) throw new Error("accepted prompt turn does not match the observed busy turn");
  if (!["completed", "succeeded"].includes(statusOf(finalBusy))) throw new Error(`busy turn ${busyId} did not complete successfully`);
  const busyFinalTexts = finalTexts(finalBusy);
  if (busyFinalTexts.at(-1) !== input.expectedBusyFinalAnswer) throw new Error("busy turn has the wrong native final answer");
  const laterIndex = finalTurns.findIndex(turn => turnId(turn) === input.laterActivationTurnId);
  const later = laterIndex >= 0 ? finalTurns[laterIndex] : undefined;
  if (!later || input.laterActivationTurnId === busyId || baseline.has(input.laterActivationTurnId) || beforeTurns.some(turn => turnId(turn) === input.laterActivationTurnId) || afterTurns.some(turn => turnId(turn) === input.laterActivationTurnId) || laterIndex <= finalTurns.findIndex(turn => turnId(turn) === busyId)) throw new Error("later activation turn is missing, early, or reuses an existing turn");
  if (!["completed", "succeeded"].includes(statusOf(later))) throw new Error("later activation turn did not complete successfully");
  return busyId;
};
