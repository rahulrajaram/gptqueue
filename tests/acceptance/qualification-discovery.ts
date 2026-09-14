import type { NativeTrace } from "./qualification-evidence.js";
import type { ParticipantIdentity, RawEvidenceRef } from "./qualification-types.js";

type PublicPeerRecord = Readonly<Record<string, unknown>>;

export type PeerDiscoveryEvidence = Readonly<{
  actor: ParticipantIdentity;
  peer: ParticipantIdentity;
  sourceId: string;
  rawHistoryRef: RawEvidenceRef;
  observedPeer: PublicPeerRecord;
}>;

const sameRuntimeIdentity = (left: ParticipantIdentity, right: ParticipantIdentity): boolean =>
  left.participantId === right.participantId &&
  left.route === right.route &&
  left.agent === right.agent &&
  left.hostRuntimeId === right.hostRuntimeId;

const object = (value: unknown): Record<string, unknown> | undefined =>
  value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : undefined;

const peerRecord = (value: unknown, peer: ParticipantIdentity): PublicPeerRecord | undefined => {
  const record = object(value);
  return record?.name === peer.agent && record.online === true
    ? Object.freeze({ ...record })
    : undefined;
};

/**
 * Join a pair's exact actor identity to a successful native list_agents result.
 * Registration markers, queue status, and names embedded in other payloads do
 * not establish discovery evidence.
 */
export const collectPeerDiscovery = (
  actor: ParticipantIdentity,
  peer: ParticipantIdentity,
  traces: readonly NativeTrace[],
): PeerDiscoveryEvidence => {
  if (actor.participantId === peer.participantId || actor.agent === peer.agent) {
    throw new Error("peer discovery requires distinct actors");
  }

  for (const trace of traces) {
    if (
      trace.name !== "list_agents" ||
      trace.successful !== true ||
      trace.runtimeBound !== true ||
      trace.runtimeId !== actor.hostRuntimeId ||
      !sameRuntimeIdentity(trace.actor, actor)
    ) continue;

    const output = object(trace.output);
    if (output?.status !== "ok" || !Array.isArray(output.agents)) continue;
    const observedPeer = output.agents.map((entry) => peerRecord(entry, peer)).find((entry): entry is PublicPeerRecord => entry !== undefined);
    if (observedPeer) {
      return Object.freeze({ actor, peer, sourceId: trace.sourceId, rawHistoryRef: trace.rawHistoryRef, observedPeer });
    }
  }

  throw new Error(`peer discovery missing for ${actor.agent} -> ${peer.agent}`);
};
