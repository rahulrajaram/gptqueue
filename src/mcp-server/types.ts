// The message type is part of the transport-agnostic core; re-exported here
// for existing importers.
export type { QueueMessage } from "../core/types.js";

export const HEARTBEAT_TTL = 30;
export const HEARTBEAT_INTERVAL = 10;
