/**
 * sw4rm vocabulary mapping (AT11). Hand-written so the runtime library stays
 * dependency-free; tests/agent-tracking/at11-sw4rm.test.ts checks every value
 * against @sw4rm/js-sdk.
 */
import type { SeatDeclaration, SeatErrorCode } from "./types.js";

/** The sw4rm source this mapping was written against. */
export const SW4RM_SOURCE = Object.freeze({
  repository: "github.com/rahulrajaram/sw4rm",
  commit: "976d560273048374c69c7b97edff1335aecb28f2",
  sdk: "@sw4rm/js-sdk@0.6.0",
});

/** sw4rm.common.ErrorCode (protos/common.proto). */
export const SW4RM_ERROR_CODE = Object.freeze({
  ERROR_CODE_UNSPECIFIED: 0,
  BUFFER_FULL: 1,
  NO_ROUTE: 2,
  ACK_TIMEOUT: 3,
  AGENT_UNAVAILABLE: 4,
  AGENT_SHUTDOWN: 5,
  VALIDATION_ERROR: 6,
  PERMISSION_DENIED: 7,
  UNSUPPORTED_MESSAGE_TYPE: 8,
  OVERSIZE_PAYLOAD: 9,
  TOOL_TIMEOUT: 10,
  PARTIAL_DELIVERY: 11,
  FORCED_PREEMPTION: 12,
  TTL_EXPIRED: 13,
  DUPLICATE_DETECTED: 14,
  ALREADY_IN_PROGRESS: 15,
  OVERLOADED: 16,
  REDIRECT: 20,
  INTERNAL_ERROR: 99,
} as const);

export type Sw4rmErrorCodeName = keyof typeof SW4RM_ERROR_CODE;

/** Every seat refusal has exactly one sw4rm error code. */
export const SEAT_ERROR_TO_SW4RM: Readonly<Record<SeatErrorCode, Sw4rmErrorCodeName>> = Object.freeze({
  seat_file_unavailable: "INTERNAL_ERROR",
  seat_file_invalid: "VALIDATION_ERROR",
  unknown_seat: "PERMISSION_DENIED",
  runtime_not_allowed: "PERMISSION_DENIED",
  cwd_not_allowed: "PERMISSION_DENIED",
  holder_unidentified: "VALIDATION_ERROR",
  seat_held: "ALREADY_IN_PROGRESS",
  stale_epoch: "FORCED_PREEMPTION",
  not_holder: "PERMISSION_DENIED",
  invalid_request: "VALIDATION_ERROR",
  store_corrupt: "INTERNAL_ERROR",
  store_unavailable: "AGENT_UNAVAILABLE",
  contention: "OVERLOADED",
});

/** The sw4rm AgentDescriptor fields a seat card fills. */
export interface Sw4rmAgentDescriptorCore {
  readonly agent_id: string;
  readonly name: string;
  readonly description: string;
  readonly capabilities: readonly string[];
}

export const toAgentDescriptor = (seat: SeatDeclaration): Sw4rmAgentDescriptorCore =>
  Object.freeze({
    agent_id: seat.seat_id,
    name: seat.name,
    description: seat.description,
    capabilities: Object.freeze([...seat.capabilities]),
  });
