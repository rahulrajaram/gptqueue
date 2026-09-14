import type { AcceptanceDimension, AcceptanceRow, ExchangeEvidence, ExecutionEvidence } from "./oracle.js";

export const routeIds = [
  "codex-appserver", "codex-interactive", "codex-headless", "codex-native-child", "codex-fork", "codex-resume",
  "pi-interactive", "pi-headless", "pi-rpc-cli", "pi-native-child", "pi-managed", "pi-sdk",
  "opencode-interactive", "opencode-run", "opencode-native-task", "opencode-fork", "opencode-resume",
  "opencode-serve-attach", "opencode-acp", "claude-cli", "claude-native-child", "gemini-cli",
  "generic-stdio", "generic-http", "generic-stateless",
] as const;
export type RouteId = (typeof routeIds)[number];
export type HostFamily = "codex" | "pi" | "opencode" | "claude" | "gemini" | "generic";
export type Availability =
  | Readonly<{ kind: "available" }>
  | Readonly<{ kind: "blocked_prerequisite"; detail: string }>
  | Readonly<{ kind: "setup_gap"; detail: string }>;
export type RuntimeStatus =
  | Readonly<{ kind: "idle"; runtimeId: string }>
  | Readonly<{ kind: "busy"; runtimeId: string }>
  | Readonly<{ kind: "terminated"; runtimeId?: string; detail?: string }>
  | Readonly<{ kind: "unknown"; detail: string }>;
export type TrialKind = "communication" | "task" | "result" | "error" | "busy" | "ping" | "status" | "initiative" | "unnamed_registration";

export type RouteSpec = Readonly<{ id: RouteId; host: HostFamily; modelBacked: boolean; availability: Availability }>;
export type ParticipantIdentity = Readonly<{
  participantId: string; route: RouteId; hostRuntimeId: string; agent: string;
  cwdHash: string; profileHash: string; epochHash: string;
}>;
export type ModelParticipant = Readonly<{
  kind: "model"; identity: ParticipantIdentity;
  prompt: (text: string, signal: AbortSignal) => Promise<unknown>;
  status: (signal: AbortSignal) => Promise<RuntimeStatus>;
  history: (signal: AbortSignal) => Promise<unknown>;
  close: () => Promise<void>;
}>;
export type GenericParticipant = Readonly<{
  kind: "generic"; identity: ParticipantIdentity;
  call: (name: string, args: Readonly<Record<string, unknown>>, signal: AbortSignal) => Promise<unknown>;
  status: (signal: AbortSignal) => Promise<RuntimeStatus>;
  history: (signal: AbortSignal) => Promise<readonly GenericCallRecord[]>;
  close: () => Promise<void>;
}>;
export type GenericCallRecord = Readonly<{
  sourceId: string; name: string; request: Readonly<Record<string, unknown>>; response: unknown;
}>;
export type Participant = ModelParticipant | GenericParticipant;
export type RouteAdapter = Readonly<{
  spec: RouteSpec;
  preflight: (signal: AbortSignal) => Promise<Availability>;
  launch: (input: Readonly<{ role: "sender" | "receiver"; pairId: string; nonce: string; redisUrl: string }>, signal: AbortSignal) => Promise<Participant>;
}>;

export type PairTemplate = Readonly<{ pairId: string; sender: RouteId; receiver: RouteId }>;
export type PairSpec = PairTemplate & Readonly<{ nonce: string }>;
export type PairLease = Readonly<{
  pair: PairSpec; sender: ParticipantIdentity; receiver: ParticipantIdentity; leaseHash: string;
}>;
export type RawEvidenceRef = Readonly<{ path: string; sha256: string; sourceRevision: string; oracleRevision: string }>;
export type TrialEvidence = Readonly<{
  trialId: string; pair: PairSpec; kind: TrialKind; execution: ExecutionEvidence;
  lease: PairLease; exchange?: ExchangeEvidence; raw: readonly RawEvidenceRef[];
}>;
export type QualificationRow = Readonly<AcceptanceRow & { route: RouteId; pairId?: string; trialKind: TrialKind; raw: readonly RawEvidenceRef[] }>;
export type PairLeasePool = Readonly<{
  acquirePair: (pair: PairSpec, signal: AbortSignal) => Promise<Readonly<{ lease: PairLease; sender: Participant; receiver: Participant; release: () => Promise<void> }>>;
}>;

export const isModelParticipant = (participant: Participant): participant is ModelParticipant => participant.kind === "model";
export const acceptanceDimensions: readonly AcceptanceDimension[] = ["communication", "automatic_tasks", "initiative"];
