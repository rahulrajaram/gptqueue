/**
 * Agent tracking: domain vocabulary for seats.
 *
 * A seat is a durable role (a job card plus an authorization policy) that
 * outlives any one runtime session. A session never owns a seat; it holds
 * one for a tenure. Each tenure has a monotonic epoch, so a successor fences
 * out every write its predecessor might still attempt.
 *
 * The field shapes borrow sw4rm vocabulary (AgentDescriptor for the card),
 * but nothing here speaks the sw4rm wire protocol. This directory imports
 * only `node:` builtins and its own files, so it can be split out later.
 */

/** Static seat definition, admitted from the operator-owned seat file. */
export interface SeatDeclaration {
  /** Stable identity, `[A-Za-z0-9._-]{1,64}` (sw4rm AgentDescriptor.agent_id). */
  readonly seat_id: string;
  /** Short display name (sw4rm AgentDescriptor.name). */
  readonly name: string;
  /** The job card: at most 200 words (sw4rm AgentDescriptor.description). */
  readonly description: string;
  /** Opaque capability claims (sw4rm AgentDescriptor.capabilities). */
  readonly capabilities: readonly string[];
  /** Who may hold the seat: AT9 refuses any other launcher. */
  readonly policy: SeatPolicy;
}

export interface SeatPolicy {
  /** Runtime names allowed to hold the seat, e.g. "claude", "codex", "pi". */
  readonly runtimes: readonly string[];
  /** Absolute directories; the holder's cwd must equal or sit under one. */
  readonly cwd_roots: readonly string[];
}

/** The holding process. The process, not the session id, is the tenure unit. */
export interface HolderIdentity {
  readonly runtime: string;
  /** Native runtime session id (Claude session_id, Codex thread id, ...). */
  readonly session_id: string;
  /** Hostname, for messages and as a guard against cloned machine ids. */
  readonly host: string;
  /** Stable machine identity (/etc/machine-id); null when unreadable. */
  readonly machine_id: string | null;
  /** Kernel boot identity (/proc/sys/kernel/random/boot_id), random per boot; null when unreadable. */
  readonly boot_id: string | null;
  /** PID-namespace identity; null when unreadable, which makes the pid unobservable. */
  readonly pidns: string | null;
  readonly pid: number;
  /** Process start time (/proc/<pid>/stat field 22), so a recycled pid never matches. */
  readonly pid_start: string;
}

export type TenureState = "vacant" | "held";

/** One seat's current tenure. A vacant seat keeps the last epoch it issued. */
export interface SeatTenure {
  readonly seat_id: string;
  readonly state: TenureState;
  /** 0 before the first bind; incremented by every new tenure. */
  readonly epoch: number;
  readonly holder?: HolderIdentity; // set iff held
  readonly bound_at?: string; // ISO, set iff held
  readonly lease_expires_at?: string; // ISO, set iff held
}

/** How a tenure began. */
export type BindReason =
  | "vacant" // the seat was vacant
  | "takeover"; // the predecessor was vacated in the same step

/** Why a tenure ended. Heartbeat silence is deliberately not a reason. */
export type VacateReason =
  | "released" // the holder released it
  | "holder_exit_proven" // same-host evidence that the holder process is gone
  | "lease_expired"; // liveness unobservable and the lease ran out

/** Append-only tenure history (AT5). Entries are never edited or removed. */
export type TenureHistoryEntry =
  | Readonly<{
      kind: "bind";
      seat_id: string;
      epoch: number;
      reason: BindReason;
      runtime: string;
      session_id: string;
      at: string;
    }>
  | Readonly<{
      kind: "rebind"; // same process, new native session id (e.g. Claude /clear)
      seat_id: string;
      epoch: number;
      runtime: string;
      session_id: string;
      previous_session_id: string;
      at: string;
    }>
  | Readonly<{
      kind: "vacate";
      seat_id: string;
      epoch: number;
      reason: VacateReason;
      runtime: string;
      session_id: string;
      at: string;
    }>;

/**
 * Liveness of the current holder as observed from the caller's host.
 * `unknown` covers another host or PID namespace, and unreadable process data.
 */
export type Liveness = "alive" | "exited" | "unknown";

/** Stable refusal codes. Each maps to a sw4rm ErrorCode (see sw4rm.ts). */
export type SeatErrorCode =
  // seat file (operator-owned declarations)
  | "seat_file_unavailable"
  | "seat_file_invalid"
  // authorization (AT9)
  | "unknown_seat"
  | "runtime_not_allowed"
  | "cwd_not_allowed"
  | "holder_unidentified"
  // tenure (AT2, AT4)
  | "seat_held"
  | "stale_epoch"
  | "not_holder"
  | "invalid_request"
  // store
  | "store_corrupt"
  | "store_unavailable"
  | "contention";

export type SeatError = Readonly<{ code: SeatErrorCode; message: string }>;

export type Result<T> =
  | Readonly<{ ok: true; value: T }>
  | Readonly<{ ok: false; error: SeatError }>;
