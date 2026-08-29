-- Atomic renewal of a durable task claim's expiry.
--
-- Full renewal semantics live in TypeScript (src/core/task-claim-store.ts). This
-- script performs NO business logic beyond the atomicity boundary: it looks up
-- the claim, requires it to exist and be owned by the caller, requires it not
-- already be expired (renewal cannot resurrect a claim), then extends its
-- expiry -- capped by the claim's lifetime budget from its claimed_at instant --
-- and persists the new expiry atomically in both the hash and the index zset.
-- date parsing/formatting are done deterministically in-script (never via
-- os.time, which is clock/timezone dependent), so a renewal always round-trips
-- to the same epoch-ms returned as the zset score.
--
-- KEYS[1] = claims hash (gptq:claims)
-- KEYS[2] = claims index zset (gptq:claims-index:<actor_id>)
-- ARGV[1] = claim_id
-- ARGV[2] = actor_id expected on the stored claim
-- ARGV[3] = session_id expected on the stored claim
-- ARGV[4] = now epoch ms
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
local nowMs = tonumber(ARGV[4])
local ttlSeconds = tonumber(ARGV[5])
local budgetSeconds = tonumber(ARGV[6])

-- Howard Hinnant civil-from-days / days-from-civil; pure arithmetic so Lua's
-- lack of a reliable os.time never leaks host clock/timezone into the store.
local function days_from_civil(y, m, d)
  local yy = y
  if m <= 2 then yy = yy - 1 end
  local era = math.floor(yy / 400)
  local yoe = yy - era * 400
  local mp = m + (m > 2 and -3 or 9)
  local doy = math.floor((153 * mp + 2) / 5) + d - 1
  local doe = yoe * 365 + math.floor(yoe / 4) - math.floor(yoe / 100) + doy
  return era * 146097 + doe - 719468
end

local function ms_from_iso(s)
  local y, mo, d, h, mi, se, ms =
    string.match(s, '^(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)%.(%d+)Z$')
  if not y then return nil end
  local days = days_from_civil(tonumber(y), tonumber(mo), tonumber(d))
  local secs = days * 86400 + tonumber(h) * 3600 + tonumber(mi) * 60 + tonumber(se)
  local msv = tonumber(ms)
  local mslen = #ms
  while mslen < 3 do msv = msv * 10; mslen = mslen + 1 end
  return secs * 1000 + msv
end

local function iso_from_ms(ms)
  local totalSecs = math.floor(ms / 1000)
  local msec = ms - totalSecs * 1000
  local days = math.floor(totalSecs / 86400)
  local rem = totalSecs % 86400
  local hour = math.floor(rem / 3600)
  local minute = math.floor((rem % 3600) / 60)
  local sec = rem % 60
  local z = days + 719468
  local era = math.floor(z / 146097)
  local doe = z - era * 146097
  local yoe = math.floor((doe - math.floor(doe / 1460) + math.floor(doe / 36524)
    - math.floor(doe / 146096)) / 365)
  local y = yoe + era * 400
  local doy = doe - (365 * yoe + math.floor(yoe / 4) - math.floor(yoe / 100))
  local mp = math.floor((5 * doy + 2) / 153)
  local d = doy - math.floor((153 * mp + 2) / 5) + 1
  local m = mp + (mp < 10 and 3 or -9)
  local yfinal = y
  if m <= 2 then yfinal = y + 1 end
  return string.format('%04d-%02d-%02dT%02d:%02d:%02d.%03dZ',
    yfinal, m, d, hour, minute, sec, msec)
end

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