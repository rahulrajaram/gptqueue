import { createHash } from "node:crypto";
import type { ParticipantIdentity } from "./qualification-types.js";
import type { NativeTrace } from "./qualification-evidence.js";

export type IdleObservation = Readonly<{ boundary: number; nativeTurnIds: readonly string[]; controllerEvents: readonly Readonly<{ kind: string; at: number }>[] }>;
export const assertIdleObservation = (input: IdleObservation, baselineTurnIds: readonly string[]): void => {
  const fresh = input.nativeTurnIds.filter(id => !baselineTurnIds.includes(id));
  if ([...input.nativeTurnIds, ...baselineTurnIds].some(id => id.length === 0)) throw new Error("native turn IDs must be nonempty");
  if (fresh.length === 0) throw new Error("idle task produced no new native turn");
  if (new Set(fresh).size !== fresh.length) throw new Error("idle task produced duplicate native turn IDs");
  if (input.controllerEvents.some(event => event.at >= input.boundary && event.kind === "controller_prompt")) throw new Error("controller prompt occurred after stimulus boundary");
};
export const sourceDigest = (value: string): string => createHash("sha256").update(value).digest("hex");
export const exactActor = (trace: NativeTrace, actor: ParticipantIdentity): boolean => trace.successful === true && trace.runtimeBound === true && trace.runtimeId === actor.hostRuntimeId && trace.actor.agent === actor.agent;

/** A terminal setup turn can be interrupted or failed; neither proves readiness. */
export const assertSetupTurnCompleted = (history: unknown, started: unknown): string => {
  const turnId = (started as { turn?: { id?: unknown } })?.turn?.id;
  if (typeof turnId !== "string" || turnId.length === 0) throw new Error("setup prompt returned no exact accepted turn ID");
  const turns = (history as { turns?: unknown })?.turns;
  const turn = Array.isArray(turns) ? turns.find(value => value?.id === turnId) : undefined;
  const status = String(turn?.status ?? "missing").toLowerCase();
  if (!["completed", "succeeded"].includes(status)) throw new Error(`setup turn ${turnId} terminated without readiness: ${status}`);
  return turnId;
};
