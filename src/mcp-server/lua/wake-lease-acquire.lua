-- Atomic, coalescing acquire of a per-actor wake lease.
--
-- Full wake lease semantics live in TypeScript (src/core/wake-lease.ts). This
-- script performs NO business logic: if a lease already exists it returns the
-- existing JSON with coalesced=1 (a concurrent controller already owns the
-- activation in flight), otherwise it writes a new lease with the requested
-- TTL and returns it with coalesced=0. Redis key TTL is the expiry mechanism;
-- no compare-on-read is needed.
--
-- KEYS[1] = wake lease string key (per actor, with TTL)
-- ARGV[1] = actor_id
-- ARGV[2] = issued_by_session
-- ARGV[3] = new lease_id
-- ARGV[4] = issued_at (ISO)
-- ARGV[5] = expires_at (ISO)
-- ARGV[6] = TTL (whole seconds, EX)
--
-- Returns { stored_json, coalesced }.
local key = KEYS[1]
local actorId = ARGV[1]
local issuedBy = ARGV[2]
local leaseId = ARGV[3]
local issuedAt = ARGV[4]
local expiresAt = ARGV[5]
local ttl = tonumber(ARGV[6])

local existing = redis.call('GET', key)
if existing then
  return { existing, 1 }
end

local newLease = cjson.encode({
  lease_id = leaseId,
  actor_id = actorId,
  issued_by_session = issuedBy,
  issued_at = issuedAt,
  expires_at = expiresAt
})
redis.call('SET', key, newLease, 'EX', ttl)
return { newLease, 0 }