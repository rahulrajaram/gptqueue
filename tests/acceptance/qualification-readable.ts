import type { NativeTrace } from "./qualification-evidence.js";
import type { ParticipantIdentity } from "./qualification-types.js";

type Json = Record<string, unknown>;
export type ReadableDeliveryInput = Readonly<{
  peer: ParticipantIdentity; model: ParticipantIdentity; genericTraces: readonly NativeTrace[]; nativeTraces: readonly NativeTrace[];
  postStimulusCallIds: readonly string[]; messageType: "ping" | "status"; content: string;
}>;
export type ReadableDeliveryResult = Readonly<{ messageId: string; claimId: string; claimSourceId: string }>;
const object = (value: unknown): Json | undefined => value !== null && typeof value === "object" && !Array.isArray(value) ? value as Json : undefined;
const same = (a: ParticipantIdentity, b: ParticipantIdentity): boolean => a.participantId === b.participantId && a.route === b.route && a.hostRuntimeId === b.hostRuntimeId && a.agent === b.agent && a.cwdHash === b.cwdHash && a.profileHash === b.profileHash && a.epochHash === b.epochHash;
const eligible = (trace: NativeTrace, actor: ParticipantIdentity): boolean => trace.successful === true && trace.runtimeBound === true && trace.runtimeId === actor.hostRuntimeId && same(trace.actor, actor);
const output = (trace: NativeTrace): Json => trace.output ?? {};
const input = (trace: NativeTrace): Json => trace.input ?? {};

export const collectReadableDelivery = (value: ReadableDeliveryInput): ReadableDeliveryResult => {
  if (value.content.length === 0 || value.peer.agent === value.model.agent || same(value.peer, value.model)) throw new Error("invalid readable delivery identities or content");
  const sends = value.genericTraces.filter(trace => eligible(trace, value.peer) && trace.name === "send_message" && input(trace).to === value.model.agent && input(trace).type === value.messageType && input(trace).content === value.content && output(trace).status === "sent" && output(trace).to === value.model.agent && typeof output(trace).message_id === "string" && String(output(trace).message_id).length > 0 && (output(trace).deduplicated === undefined || output(trace).deduplicated === false));
  const ids = new Set(sends.map(trace => String(output(trace).message_id)));
  if (sends.length !== 1 || ids.size !== 1) throw new Error("expected one exact generic send");
  const messageId = String(output(sends[0]!).message_id);
  const claims = value.nativeTraces.filter(trace => eligible(trace, value.model) && trace.name === "claim_tasks" && value.postStimulusCallIds.includes(trace.sourceId) && output(trace).status === "ok" && output(trace).claimed === true).filter(trace => {
    const claim = object(output(trace).claim); if (!claim || claim.actor_id !== value.model.agent || typeof claim.claim_id !== "string" || claim.claim_id.length === 0 || !Array.isArray(claim.tasks)) return false;
    return claim.tasks.some(task => { const envelope = object(typeof task === "string" ? (() => { try { return JSON.parse(task) as unknown; } catch { return undefined; } })() : task); const payload = object(envelope?.payload); return envelope?.id === messageId && envelope.from === value.peer.agent && envelope.to === value.model.agent && envelope.type === value.messageType && payload?.content === value.content; });
  });
  const claimIds = new Set(claims.map(trace => String(object(output(trace).claim)?.claim_id)));
  if (claims.length !== 1 || claimIds.size !== 1) throw new Error("expected one exact native claim");
  return Object.freeze({ messageId, claimId: [...claimIds][0]!, claimSourceId: claims[0]!.sourceId });
};
