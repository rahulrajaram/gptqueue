import type { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import type { RuntimeIdentity } from "./runtime.js";
import { SESSION_KEYS } from "../core/keys.js";
import { GptQueueError } from "../mcp-server/errors.js";

export type ActivationRecord = Readonly<{
  operation_id: string;
  message_ids: readonly string[];
  attempt: number;
  state: "pending" | "submitting" | "accepted" | "ambiguous" | "exhausted";
  created_at: string;
  /** When the record entered its current state; deadlines are measured from it. */
  state_since?: string;
  /** First attempt of the current bounded retry round (exhausted cooldown re-arms a new round). */
  round_start?: number;
}>;

/** Outcome of renewing the activation lease. */
export type LeaseRenewal = "held" | "reacquired" | "lost";

const operationKey = SESSION_KEYS.activation;

const ACTIVATION_STATES: ReadonlySet<unknown> = new Set(["pending", "submitting", "accepted", "ambiguous", "exhausted"]);
const isActivationRecord = (value: unknown): value is ActivationRecord => {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  return typeof record.operation_id === "string" && typeof record.created_at === "string" &&
    typeof record.attempt === "number" && ACTIVATION_STATES.has(record.state) &&
    (record.state_since === undefined || typeof record.state_since === "string") &&
    (record.round_start === undefined || typeof record.round_start === "number") &&
    Array.isArray(record.message_ids) && record.message_ids.every((id) => typeof id === "string");
};

/** Fences every state write against the current runtime owner. */
export class ActivationStore {
  readonly token = randomUUID();
  private owner: string | undefined;

  constructor(private readonly redis: Redis, readonly agent: string) {}

  async attach(binding: RuntimeIdentity): Promise<void> {
    const owner = JSON.stringify({ ...binding, token: this.token });
    if (await this.redis.set(SESSION_KEYS.runtimeBinding(this.agent), owner, "EX", 30, "NX") !== "OK") {
      // Typed (I-2): callers classify the conflict by code, not by this wording.
      throw new GptQueueError("RUNTIME_BINDING_HELD", "Runtime inbox already has a live dispatcher",
        { agent: this.agent });
    }
    this.owner = owner;
  }

  async refresh(): Promise<boolean> {
    return await this.renew() !== "lost";
  }

  /**
   * Renews the lease. A lease that lapsed (for example during one long native
   * call) is re-acquired when no other owner took it; only a live foreign
   * owner fences this dispatcher.
   */
  async renew(): Promise<LeaseRenewal> {
    if (!this.owner) return "lost";
    const result = await this.redis.eval(
      "local v = redis.call('GET',KEYS[1]) " +
      "if v == ARGV[1] then redis.call('EXPIRE',KEYS[1],30) return 1 end " +
      "if not v then redis.call('SET',KEYS[1],ARGV[1],'EX',30) return 2 end return 0",
      1, SESSION_KEYS.runtimeBinding(this.agent), this.owner,
    );
    return result === 1 ? "held" : result === 2 ? "reacquired" : "lost";
  }

  async current(): Promise<ActivationRecord | null> {
    const raw = await this.redis.get(operationKey(this.agent));
    if (!raw) return null;
    // A corrupt record reads as absent so the dispatcher starts a fresh one
    // instead of failing every tick until the record's TTL lapses.
    try {
      const parsed: unknown = JSON.parse(raw);
      return isActivationRecord(parsed) ? parsed : null;
    } catch {
      return null;
    }
  }

  async save(record: ActivationRecord | null): Promise<boolean> {
    if (!this.owner) return false;
    return await this.redis.eval(
      "if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end " +
      "if ARGV[2] == '' then redis.call('DEL',KEYS[2]) else redis.call('SET',KEYS[2],ARGV[2],'EX',604800) end return 1",
      2, SESSION_KEYS.runtimeBinding(this.agent), operationKey(this.agent), this.owner, record ? JSON.stringify(record) : "",
    ) === 1;
  }

  async detach(): Promise<void> {
    if (!this.owner) return;
    await this.redis.eval(
      "if redis.call('GET',KEYS[1]) == ARGV[1] then return redis.call('DEL',KEYS[1]) end return 0",
      1, SESSION_KEYS.runtimeBinding(this.agent), this.owner,
    );
    this.owner = undefined;
  }
}
