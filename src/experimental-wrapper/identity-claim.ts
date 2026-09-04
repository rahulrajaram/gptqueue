import { createHash, randomUUID } from "node:crypto";
import { Redis } from "ioredis";
import { SESSION_KEYS } from "../core/keys.js";

const CLAIM_KEY_PREFIX = "gptq:experimental-wrapper-claim:";

const ACQUIRE_CLAIM_SCRIPT = `
if redis.call("EXISTS", KEYS[2]) == 1 then
  return 0
end
if ARGV[3] == "fresh" and redis.call("HEXISTS", KEYS[1], ARGV[1]) == 1 then
  return -1
end
if redis.call("SCARD", KEYS[3]) > 0 or redis.call("EXISTS", KEYS[4]) == 1 then
  return -2
end
if redis.call("SET", KEYS[2], ARGV[2], "NX") then
  return 1
end
return 0
`;

const RELEASE_CLAIM_SCRIPT = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`;

const VERIFY_SESSION_SCRIPT = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
if redis.call("SCARD", KEYS[2]) ~= 1 then
  return -1
end
if redis.call("SISMEMBER", KEYS[2], ARGV[2]) ~= 1 then
  return -1
end
return 1
`;

const UNREGISTER_EXCLUSIVE_SESSION_SCRIPT = `
if redis.call("GET", KEYS[1]) ~= ARGV[1] then
  return 0
end
if redis.call("HGET", KEYS[3], "agent_name") ~= ARGV[3] then
  return -1
end

local exclusive = redis.call("SCARD", KEYS[2]) == 1
  and redis.call("SISMEMBER", KEYS[2], ARGV[2]) == 1

redis.call("DEL", KEYS[3], KEYS[4])
redis.call("SREM", KEYS[2], ARGV[2])

if not exclusive then
  return 2
end

redis.call("HDEL", KEYS[5], ARGV[3])
redis.call("DEL", KEYS[6], KEYS[7], KEYS[8])
return 1
`;

export type ExclusiveUnregisterOutcome =
  | "unregistered"
  | "session_closed_only";

export interface WrapperIdentityClaim {
  readonly assertExclusiveSession: (sessionId: string) => Promise<void>;
  readonly unregisterExclusiveSession: (
    sessionId: string
  ) => Promise<ExclusiveUnregisterOutcome>;
  readonly release: () => Promise<void>;
  readonly abandon: () => void;
}

const claimKeyFor = (agent: string): string =>
  `${CLAIM_KEY_PREFIX}${createHash("sha256").update(agent).digest("hex")}`;

const closeRedis = async (redis: Redis): Promise<void> => {
  try {
    await redis.quit();
  } catch (error) {
    redis.disconnect();
    throw error;
  }
};

/**
 * Claim a wrapper name before registering it.
 *
 * The claim intentionally has no TTL: losing exclusivity is more dangerous
 * than leaving a fail-closed key after SIGKILL. A normally exiting wrapper
 * compare-deletes its own token. Destructive runs additionally require a fresh
 * registry name; non-destructive runs may reuse an offline identity but refuse
 * any name with an active session or heartbeat.
 */
export async function acquireWrapperIdentityClaim(
  redisUrl: string,
  agent: string,
  requireFresh: boolean
): Promise<WrapperIdentityClaim> {
  const redis = new Redis(redisUrl, { maxRetriesPerRequest: 3 });
  const claimKey = claimKeyFor(agent);
  const token = randomUUID();

  try {
    const acquired = Number(
      await redis.eval(
        ACQUIRE_CLAIM_SCRIPT,
        4,
        SESSION_KEYS.registry,
        claimKey,
        SESSION_KEYS.agentSessions(agent),
        SESSION_KEYS.heartbeat(agent),
        agent,
        token,
        requireFresh ? "fresh" : "reusable"
      )
    );
    if (acquired === -1) {
      throw new Error(
        `Refusing destructive cleanup for pre-existing agent: ${agent}`
      );
    }
    if (acquired !== 1) {
      if (acquired === -2) {
        throw new Error(
          `Refusing concurrent wrapper ownership of active agent: ${agent}`
        );
      }
      throw new Error(
        `Another experimental wrapper already owns the agent name: ${agent}`
      );
    }
  } catch (error) {
    redis.disconnect();
    throw error;
  }

  let released = false;
  let redisClosed = false;
  const assertExclusiveSession = async (sessionId: string): Promise<void> => {
    const verified = Number(
      await redis.eval(
        VERIFY_SESSION_SCRIPT,
        2,
        claimKey,
        SESSION_KEYS.agentSessions(agent),
        token,
        sessionId
      )
    );
    if (verified !== 1) {
      throw new Error(
        `Agent identity lost exclusive session ownership before launch or cleanup: ${agent}`
      );
    }
  };

  const unregisterExclusiveSession = async (
    sessionId: string
  ): Promise<ExclusiveUnregisterOutcome> => {
    const outcome = Number(
      await redis.eval(
        UNREGISTER_EXCLUSIVE_SESSION_SCRIPT,
        8,
        claimKey,
        SESSION_KEYS.agentSessions(agent),
        SESSION_KEYS.session(sessionId),
        SESSION_KEYS.lease(sessionId),
        SESSION_KEYS.registry,
        SESSION_KEYS.queue(agent),
        SESSION_KEYS.mailboxMeta(agent),
        SESSION_KEYS.heartbeat(agent),
        token,
        sessionId,
        agent
      )
    );
    switch (outcome) {
      case 1:
        return "unregistered";
      case 2:
        return "session_closed_only";
      case 0:
        throw new Error(`Wrapper identity claim was lost for agent: ${agent}`);
      case -1:
        throw new Error(
          `Registered session no longer belongs to wrapper agent: ${agent}`
        );
      default:
        throw new Error(
          `Unexpected atomic unregister result ${outcome} for agent: ${agent}`
        );
    }
  };

  const release = async (): Promise<void> => {
    if (redisClosed && !released) {
      throw new Error(`Wrapper identity claim was abandoned for agent: ${agent}`);
    }
    if (!released) {
      const deleted = Number(
        await redis.eval(RELEASE_CLAIM_SCRIPT, 1, claimKey, token)
      );
      if (deleted !== 1) {
        throw new Error(`Wrapper identity claim was lost for agent: ${agent}`);
      }
      released = true;
    }
    if (!redisClosed) {
      await closeRedis(redis);
      redisClosed = true;
    }
  };

  const abandon = (): void => {
    if (redisClosed) return;
    redis.disconnect();
    redisClosed = true;
  };

  return Object.freeze({
    assertExclusiveSession,
    unregisterExclusiveSession,
    release,
    abandon,
  });
}
