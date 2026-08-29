-- Atomic conditional acknowledgement of a durable task claim.
--
-- Full ack semantics live in TypeScript (src/core/task-claim-store.ts). This
-- script performs NO business logic beyond the ownership + presence check:
-- it looks up the claim, requires it to exist, requires the stored
-- actor_id/session_id to match the caller (only the claiming runtime session
-- may acknowledge its own claim), then removes the claim from the hash and the
-- index and returns how many tasks were acknowledged.
--
-- KEYS[1] = claims hash (gptq:claims)
-- KEYS[2] = claims index zset (gptq:claims-index:<actor_id>)
-- ARGV[1] = claim_id
-- ARGV[2] = actor_id expected on the stored claim
-- ARGV[3] = session_id expected on the stored claim
--
-- On success, besides removing the claim, this also DELetes each task's sidecar
-- recovery counter (gptq:rc:<actor_id>:<message_id>) so an acknowledged message
-- starts a fresh recovery budget if it is ever re-delivered. Each task's
-- envelope is decoded to its message id; a legacy / undecodable envelope (no
-- stable id) simply skips that counter cleanup rather than failing the ack.
--
-- Returns { 1, task_count } on success,
--         { 2, '' }        when the claim does not exist (or is corrupt),
--         { 3, session_id } when the caller is not the claim owner.
local claims = KEYS[1]
local index = KEYS[2]
local claimId = ARGV[1]
local actorId = ARGV[2]
local sessionId = ARGV[3]

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

redis.call('HDEL', claims, claimId)
redis.call('ZREM', index, claimId)
for _, task in ipairs(claim.tasks) do
  local msg
  local msgDecoded = pcall(function() msg = cjson.decode(task) end)
  if msgDecoded and type(msg) == 'table'
     and type(msg.id) == 'string' and #msg.id > 0 then
    redis.call('DEL', 'gptq:rc:' .. actorId .. ':' .. msg.id)
  end
end
return { 1, #claim.tasks }