import type { Redis } from "ioredis";
import { randomUUID } from "node:crypto";
import type { RuntimeIdentity } from "./runtime.js";
import { SESSION_KEYS } from "../core/keys.js";

export type ActivationRecord = Readonly<{
  operation_id: string;
  message_ids: readonly string[];
  attempt: number;
  state: "pending" | "submitting" | "accepted" | "ambiguous" | "exhausted";
  created_at: string;
}>;

const operationKey = SESSION_KEYS.activation;

const ACTIVATION_STATES: ReadonlySet<unknown> = new Set(["pending", "submitting", "accepted", "ambiguous", "exhausted"]);
const isActivationRecord = (value: unknown): value is ActivationRecord => {
  if (typeof value !== "object" || value === null) return false;
  const record = value as Record<string, unknown>;
  return typeof record.operation_id === "string" && typeof record.created_at === "string" &&
    typeof record.attempt === "number" && ACTIVATION_STATES.has(record.state) &&
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
      throw new Error("Runtime inbox already has a live dispatcher");
    }
    this.owner = owner;
  }

  async refresh(): Promise<boolean> {
    if (!this.owner) return false;
    return await this.redis.eval(
      "if redis.call('GET',KEYS[1]) ~= ARGV[1] then return 0 end redis.call('EXPIRE',KEYS[1],30) return 1",
      1, SESSION_KEYS.runtimeBinding(this.agent), this.owner,
    ) === 1;
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
