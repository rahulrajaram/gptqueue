import { describe, expect, it } from "vitest";
import { collectPeerDiscovery } from "./qualification-discovery.js";
import type { NativeTrace } from "./qualification-evidence.js";
import type { ParticipantIdentity, RawEvidenceRef } from "./qualification-types.js";

const actor: ParticipantIdentity = {
  participantId: "actor-participant",
  route: "generic-stdio",
  hostRuntimeId: "actor-runtime",
  agent: "actor-agent",
  cwdHash: "actor-cwd",
  profileHash: "actor-profile",
  epochHash: "actor-epoch",
};
const peer: ParticipantIdentity = {
  participantId: "peer-participant",
  route: "generic-http",
  hostRuntimeId: "peer-runtime",
  agent: "peer-agent",
  cwdHash: "peer-cwd",
  profileHash: "peer-profile",
  epochHash: "peer-epoch",
};
const rawHistoryRef: RawEvidenceRef = {
  path: "history.json",
  sha256: "history-sha256",
  sourceRevision: "source-revision",
  oracleRevision: "oracle-revision",
};

const makeTrace = (overrides: Readonly<Partial<NativeTrace>> = {}): NativeTrace => ({
  sourceId: "list-agents-call",
  name: "list_agents",
  inputHash: "input-hash",
  outputHash: "output-hash",
  actor,
  runtimeId: actor.hostRuntimeId,
  rawHistoryRef,
  input: {},
  output: {
    status: "ok",
    agents: [{ name: peer.agent, online: true, client: null, uuid: null, working_directory: null }],
  },
  successful: true,
  runtimeBound: true,
  ...overrides,
});

describe("peer discovery evidence", () => {
  it("requires an exact runtime-bound list_agents result and preserves its peer record", () => {
    const trace = makeTrace();
    const evidence = collectPeerDiscovery(actor, peer, [trace]);

    expect(evidence.actor).toBe(actor);
    expect(evidence.peer).toBe(peer);
    expect(evidence.sourceId).toBe(trace.sourceId);
    expect(evidence.rawHistoryRef).toBe(rawHistoryRef);
    expect(evidence.observedPeer).toEqual({ name: peer.agent, online: true, client: null, uuid: null, working_directory: null });
  });

  it("accepts distinct participants sharing a host runtime", () => {
    const sharedHostPeer = { ...peer, hostRuntimeId: actor.hostRuntimeId };
    const evidence = collectPeerDiscovery(actor, sharedHostPeer, [makeTrace({
      output: { status: "ok", agents: [{ name: sharedHostPeer.agent, online: true }] },
    })]);
    expect(evidence.peer).toBe(sharedHostPeer);
  });

  it.each([
    ["wrong actor", { actor: { ...actor, participantId: "other-participant" } }],
    ["wrong route", { actor: { ...actor, route: "generic-http" } }],
    ["wrong runtime", { runtimeId: "other-runtime" }],
    ["wrong peer", { output: { status: "ok", agents: [{ name: "other-peer", online: true }] } }],
    ["offline peer", { output: { status: "ok", agents: [{ name: peer.agent, online: false }] } }],
    ["failed call", { successful: false }],
    ["unbound call", { runtimeBound: false }],
    ["wrong tool", { name: "get_queue_status" }],
  ] as const)("rejects %s", (_reason, overrides) => {
    expect(() => collectPeerDiscovery(actor, peer, [makeTrace(overrides)])).toThrow(/peer discovery missing/);
  });

  it("rejects seeded names without a successful list_agents result", () => {
    const trace = makeTrace({
      name: "register_agent",
      input: { peer: peer.agent },
      output: { status: "registered", agent: peer.agent },
    });
    expect(() => collectPeerDiscovery(actor, peer, [trace])).toThrow(/peer discovery missing/);
  });

  it("requires distinct actors", () => {
    expect(() => collectPeerDiscovery(actor, { ...peer, participantId: actor.participantId }, [makeTrace()])).toThrow(/distinct actors/);
  });
});
