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

export interface AgentRegistration {
  name: string;
  role: "publisher" | "consumer" | "both";
  description?: string;
  registered_at: string;
  pid: number;
}

export const REDIS_KEYS = {
  registry: "gptq:registry",
  queue: (agent: string) => `gptq:q:${agent}`,
  meta: (agent: string) => `gptq:meta:${agent}`,
  heartbeat: (agent: string) => `gptq:heartbeat:${agent}`,
} as const;

export const DEFAULT_QUEUE_BOUND = 10;
export const HEARTBEAT_TTL = 30;
export const HEARTBEAT_INTERVAL = 10;
// test
