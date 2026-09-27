export interface QueueMessage {
  id: string;
  from: string;
  to: string;
  timestamp: string;
  type: "task" | "result" | "status" | "error" | "ping";
  payload: {
    content: string;
    metadata?: Record<string, unknown>;
    in_reply_to?: string;
  };
}

export const HEARTBEAT_TTL = 30;
export const HEARTBEAT_INTERVAL = 10;
