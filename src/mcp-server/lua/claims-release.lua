-- Atomic user-initiated release of one of the caller's own durable task claims.
--
-- Unlike claims-ack.lua (which confirms delivery and drops the tasks), this
-- script puts the claim's tasks back on the caller's inbox so they can be
-- claimed again, and removes the claim. The release is only valid for the
-- claiming runtime session (actor_id + session_id must match the stored
-- claim), so a caller can never release another session's claim.
--
-- Recovery counters are INCR'd per task exactly as claims-recover.lua does, so
-- a message that keeps getting released cannot loop forever; once a message's
-- counter exceeds the cap it is dead-lettered instead of re-queued. The inbox
-- and DLQ must be lists (or absent) and the counters must be incrementable
-- before the first write, so a claim is never deleted while its tasks have no
-- durable destination.
--
-- KEYS[1] = claims hash (gptq:claims)
-- KEYS[2] = claims index zset (gptq:claims-index:<actor_id>)
-- KEYS[3] = inbox list (gptq:q:<actor_id>)
-- KEYS[4] = DLQ list (gptq:dlq:<actor_id>)
-- ARGV[1] = claim_id
-- ARGV[2] = actor_id expected on the stored claim
-- ARGV[3] = session_id expected on the stored claim
-- ARGV[4] = recover_cap (int >= 1): a message released more than cap times is dead-lettered
-- ARGV[5] = dlq_max_length (int >= 1): LTRIM bound on the DLQ list (keep newest)
-- ARGV[6] = counter_ttl_seconds (int >= 1): EX placed on a fresh recovery counter
--
-- Returns { 1, released, deadlettered } on success,
--         { 2, '' } when the claim does not exist (or is corrupt),
--         { 3, session_id } when the caller is not the claim owner,
--         { -1, 'inbox' | 'dlq' | 'counter' } when a key holds the wrong type.
local claims = KEYS[1]
local index = KEYS[2]
local inbox = KEYS[3]
local dlq = KEYS[4]
local claimId = ARGV[1]
local actorId = ARGV[2]
local sessionId = ARGV[3]
local recoverCap = tonumber(ARGV[4])
local dlqMaxLength = tonumber(ARGV[5])
local counterTtl = tonumber(ARGV[6])

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
   or type(claim.tasks) ~= 'table' then
  return { 2, '' }
end

if claim.actor_id ~= actorId or claim.session_id ~= sessionId then
  return { 3, claim.session_id }
end

local function holds(key, kind)
  local t = redis.call('TYPE', key).ok
  return t == 'none' or t == kind
end

local function incrementable(value)
  if value == '0' then return true end
  local digits = string.match(value, '^%-?([1-9]%d*)$')
  return digits ~= nil and #digits <= 18
end

-- Pass 1: validate every destination before the first write.
if not holds(inbox, 'list') then return { -1, 'inbox' } end
if not holds(dlq, 'list') then return { -1, 'dlq' } end
local plan = {}
for _, task in ipairs(claim.tasks) do
  local msg
  local msgDecoded = pcall(function() msg = cjson.decode(task) end)
  local counterKey
  if msgDecoded and type(msg) == 'table' and type(msg.id) == 'string' and #msg.id > 0 then
    counterKey = 'gptq:rc:' .. actorId .. ':' .. msg.id
    if not holds(counterKey, 'string') then return { -1, 'counter' } end
    local current = redis.call('GET', counterKey)
    if current and not incrementable(current) then return { -1, 'counter' } end
  end
  plan[#plan + 1] = { task = task, counterKey = counterKey }
end

-- Pass 2: write.
redis.call('HDEL', claims, claimId)
redis.call('ZREM', index, claimId)
local released = 0
local deadlettered = 0
for _, item in ipairs(plan) do
  if not item.counterKey then
    redis.call('RPUSH', inbox, item.task)
    released = released + 1
  else
    local count = redis.call('INCR', item.counterKey)
    if count == 1 then
      redis.call('EXPIRE', item.counterKey, counterTtl)
    end
    if count > recoverCap then
      local length = redis.call('LPUSH', dlq, item.task)
      if length > dlqMaxLength then
        redis.call('LTRIM', dlq, 0, dlqMaxLength - 1)
      end
      redis.call('DEL', item.counterKey)
      deadlettered = deadlettered + 1
    else
      redis.call('RPUSH', inbox, item.task)
      released = released + 1
    end
  end
end
return { 1, released, deadlettered }
