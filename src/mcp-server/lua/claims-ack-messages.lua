-- Atomic SUBSET acknowledgement of a durable task claim (acknowledge_messages).
--
-- Acknowledges exactly the selected messages of one claim and keeps the rest
-- claimed, with the claim's claimed_at, expires_at and index score unchanged.
-- Ownership is exactly claims-ack.lua's rule: the stored actor_id/session_id
-- must match the caller; expiry is NOT checked (a late ack of an expired but
-- not yet recovered claim succeeds, as a whole-claim ack does today).
--
-- Every check runs before the first write; every refusal writes nothing.
--
-- KEYS[1] = claims hash (gptq:claims)
-- KEYS[2] = claims index zset (gptq:claims-index:<actor_id>)
-- ARGV[1] = claim_id
-- ARGV[2] = actor_id expected on the stored claim
-- ARGV[3] = session_id expected on the stored claim
-- ARGV[4..] = selected message ids (1..16, validated by the caller too)
--
-- Returns { 1, acknowledged, remaining, claim_deleted (0|1) } on success;
--         { 2, '' }            unknown claim (absent, or corrupt like claims-ack);
--         { 3, '' }            caller is not the claim owner;
--         { 4, message_id }    duplicate id in the selector;
--         { 5, message_id }    selected id not in the claim;
--         { 6, message_id }    the claim holds two envelopes with this id;
--         { 7, message_id }    a selected id is absent but the claim holds an
--                              envelope whose id cannot be read (it might be it);
--         { 8, '' }            empty selector;
--         { -1, part }         a key holds the wrong type, or the claim is not
--                              a member of the actor's index (store corruption).
local claims = KEYS[1]
local index = KEYS[2]
local claimId = ARGV[1]
local actorId = ARGV[2]
local sessionId = ARGV[3]

local function holds(key, kind)
  local t = redis.call('TYPE', key).ok
  return t == 'none' or t == kind
end

if #ARGV < 4 then return { 8, '' } end
if not holds(claims, 'hash') then return { -1, 'claims' } end
if not holds(index, 'zset') then return { -1, 'index' } end

local raw = redis.call('HGET', claims, claimId)
if raw == false then return { 2, '' } end

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
  return { 3, '' }
end

-- Envelopes must be strings (raw payloads); anything else is corruption.
local ids = {}
local seen = {}
local unreadable = false
for i, task in ipairs(claim.tasks) do
  if type(task) ~= 'string' then return { -1, 'tasks' } end
  local msg
  local ok = pcall(function() msg = cjson.decode(task) end)
  if ok and type(msg) == 'table' and type(msg.id) == 'string' and #msg.id > 0 then
    if seen[msg.id] then return { 6, msg.id } end
    seen[msg.id] = i
    ids[i] = msg.id
  else
    unreadable = true
  end
end

local selected = {}
for i = 4, #ARGV do
  local id = ARGV[i]
  if selected[id] then return { 4, id } end
  selected[id] = true
end
for i = 4, #ARGV do
  local id = ARGV[i]
  if not seen[id] then
    if unreadable then return { 7, id } end
    return { 5, id }
  end
end

if redis.call('ZSCORE', index, claimId) == false then return { -1, 'index_member' } end

-- Mutation: every key above is validated.
local remainder = {}
local acknowledged = 0
for i, task in ipairs(claim.tasks) do
  local id = ids[i]
  if id ~= nil and selected[id] then
    acknowledged = acknowledged + 1
    redis.call('DEL', 'gptq:rc:' .. actorId .. ':' .. id)
  else
    remainder[#remainder + 1] = task
  end
end

if #remainder == 0 then
  redis.call('HDEL', claims, claimId)
  redis.call('ZREM', index, claimId)
  return { 1, acknowledged, 0, 1 }
end

claim.tasks = remainder
redis.call('HSET', claims, claimId, cjson.encode(claim))
return { 1, acknowledged, #remainder, 0 }
