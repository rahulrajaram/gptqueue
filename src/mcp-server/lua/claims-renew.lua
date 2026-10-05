-- Atomic renewal of a durable task claim's expiry.
--
-- Full renewal semantics live in TypeScript (src/core/task-claim-store.ts). This
-- script performs NO business logic beyond the atomicity boundary: it looks up
-- the claim, requires it to exist and be owned by the caller, requires it not
-- already be expired (renewal cannot resurrect a claim), then extends its
-- expiry -- capped by the claim's lifetime budget from its claimed_at instant --
-- and persists the new expiry atomically in both the hash and the index zset.
-- date parsing/formatting (claims-lib.lua, composed in front) are done deterministically in-script (never via
-- os.time, which is clock/timezone dependent), so a renewal always round-trips
-- to the same epoch-ms returned as the zset score.
--
-- KEYS[1] = claims hash (gptq:claims)
-- KEYS[2] = claims index zset (gptq:claims-index:<actor_id>)
-- ARGV[1] = claim_id
-- ARGV[2] = actor_id expected on the stored claim
-- ARGV[3] = session_id expected on the stored claim
-- ARGV[4] = now epoch ms from a caller-simulated clock, or '' (the default)
--           to decide on Redis server TIME; see claims-lib.lua
-- ARGV[5] = ttl_seconds (int, pre-validated by the caller) -> ttl_ms = * 1000
-- ARGV[6] = budget_seconds (int) -> budget_ms = * 1000
--
-- new_expires_ms = min(now + ttl_ms, claimed_at_ms + budget_ms). If that is not
-- strictly after now, the renewal would not extend the claim and is refused.
--
-- Returns { 1, new_expires_at_iso } on a renewed claim,
--         { 2, '' }            when the claim does not exist (or is corrupt),
--         { 3, session_id }    when the caller is not the claim owner,
--         { 4, '' }            when the claim is already expired at now,
--         { 5, '' }            when the lifetime budget is exhausted.
local claims = KEYS[1]
local index = KEYS[2]
local claimId = ARGV[1]
local actorId = ARGV[2]
local sessionId = ARGV[3]
local nowMs = tonumber(ARGV[4]) or server_ms()
local ttlSeconds = tonumber(ARGV[5])
local budgetSeconds = tonumber(ARGV[6])

local raw = redis.call('HGET', claims, claimId)
if raw == false then
  return { 2, '' }
end

local claim
local decoded = pcall(function() claim = cjson.decode(raw) end)
if not decoded then return { 2, '' } end
if type(claim) ~= 'table'
   or type(claim.claim_id) ~= 'string'
   or type(claim.actor_id) ~= 'string'
   or type(claim.session_id) ~= 'string'
   or type(claim.claimed_at) ~= 'string'
   or type(claim.expires_at) ~= 'string' then
  return { 2, '' }
end

if claim.actor_id ~= actorId or claim.session_id ~= sessionId then
  return { 3, claim.session_id }
end

local expiresMs = ms_from_iso(claim.expires_at)
local claimedMs = ms_from_iso(claim.claimed_at)
if type(expiresMs) ~= 'number' or type(claimedMs) ~= 'number' then
  return { 2, '' }
end

-- Renewal cannot resurrect an already-expired claim.
if expiresMs <= nowMs then
  return { 4, '' }
end

local ttlMs = ttlSeconds * 1000
local budgetMs = budgetSeconds * 1000
local newExpiresMs = nowMs + ttlMs
local capExpiresMs = claimedMs + budgetMs
if newExpiresMs > capExpiresMs then newExpiresMs = capExpiresMs end

-- Budget exhausted: no forward movement is possible, so refuse.
if newExpiresMs <= nowMs then
  return { 5, '' }
end

claim.expires_at = iso_from_ms(newExpiresMs)
redis.call('HSET', claims, claimId, cjson.encode(claim))
-- ZADD XX updates only an existing member, so the index cannot silently
-- recreate a claim the hash no longer references; the score mirrors the new
-- expiry epoch-ms exactly.
redis.call('ZADD', index, 'XX', newExpiresMs, claimId)
return { 1, claim.expires_at }