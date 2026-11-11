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
-- Full recovery parity (F2/F3, manager msg 69080475):
--   * F2 wake: a re-queued task emits the same header-only inbox event a fresh
--     send would (wake_plan/wake_emit in claims-lib.lua), so an idle runtime
--     blocked in XREAD wakes at once instead of waiting for a rescan. A task
--     whose outstanding key holds the wrong type (wake_plan returns false) is
--     refused before any write, exactly as recovery does.
--   * F3 DLQ accounting/notice: when the LTRIM bound discards oldest DLQ
--     entries, the number discarded is added to KEYS[6] (gptq:dlq-dropped),
--     which dlq_status reports; and a non-system dead-lettered message id is
--     added to the notice-pending set (with the overflow counter) so the
--     caller's release path can raise a dead-letter notice, as recovery does.
--
-- KEYS[1] = claims hash (gptq:claims)
-- KEYS[2] = claims index zset (gptq:claims-index:<actor_id>)
-- KEYS[3] = inbox list (gptq:q:<actor_id>)
-- KEYS[4] = DLQ list (gptq:dlq:<actor_id>)
-- KEYS[5] = inbox event stream (gptq:inbox-events:<actor_id>)
-- KEYS[6] = DLQ drop counter (gptq:dlq-dropped:<actor_id>)
-- KEYS[7] = DLQ notice-pending set (gptq:dlq-notice-pending:<actor_id>)
-- KEYS[8] = DLQ notice-overflow counter (gptq:dlq-notice-overflow:<actor_id>)
-- KEYS[9] = DLQ notice window (gptq:dlq-notice-window:<actor_id>)
-- ARGV[1] = claim_id
-- ARGV[2] = actor_id expected on the stored claim
-- ARGV[3] = session_id expected on the stored claim
-- ARGV[4] = recover_cap (int >= 1): a message released more than cap times is dead-lettered
-- ARGV[5] = dlq_max_length (int >= 1): LTRIM bound on the DLQ list (keep newest)
-- ARGV[6] = counter_ttl_seconds (int >= 1): EX placed on a fresh recovery counter
-- ARGV[7] = system sender: a task authored by it dead-letters without a notice
--
-- Returns { 1, released, deadlettered, dlq_dropped } on success,
--         { 2, '' } when the claim does not exist (or is corrupt),
--         { 3, session_id } when the caller is not the claim owner,
--         { -1, role } when a key holds the wrong type, where role is one of
--         'index' | 'inbox' | 'dlq' | 'counter' | 'events' | 'dropped' | 'outstanding' |
--         'notice_pending' | 'notice_overflow' | 'notice_window'.
local claims = KEYS[1]
local index = KEYS[2]
local inbox = KEYS[3]
local dlq = KEYS[4]
local events = KEYS[5]
local dropped = KEYS[6]
local noticePending = KEYS[7]
local noticeOverflow = KEYS[8]
local noticeWindow = KEYS[9]
local claimId = ARGV[1]
local actorId = ARGV[2]
local sessionId = ARGV[3]
local recoverCap = tonumber(ARGV[4])
local dlqMaxLength = tonumber(ARGV[5])
local counterTtl = tonumber(ARGV[6])
local system = ARGV[7] or 'gptqueue-system'

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
if not holds(index, 'zset') then return { -1, 'index' } end
if not holds(inbox, 'list') then return { -1, 'inbox' } end
if not holds(dlq, 'list') then return { -1, 'dlq' } end
if not holds(events, 'stream') then return { -1, 'events' } end
local plan = {}
local noticeIncrements = 0
for _, task in ipairs(claim.tasks) do
  local msg
  local msgDecoded = pcall(function() msg = cjson.decode(task) end)
  local msgId
  if msgDecoded and type(msg) == 'table' then msgId = msg.id end
  local counterKey
  if type(msgId) == 'string' and #msgId > 0 then
    counterKey = 'gptq:rc:' .. actorId .. ':' .. msgId
    if not holds(counterKey, 'string') then return { -1, 'counter' } end
    local current = redis.call('GET', counterKey)
    if current and not incrementable(current) then return { -1, 'counter' } end
  end
  local wake = wake_plan(task)
  if wake == false then return { -1, 'outstanding' } end
  -- System-authored tasks (including notices) still dead-letter normally,
  -- but never generate another notice: no recursive system traffic.
  local noticeId
  if counterKey and type(msg) == 'table' and msg.from ~= system then noticeId = msgId end
  if noticeId then noticeIncrements = noticeIncrements + 1 end
  plan[#plan + 1] = { task = task, counterKey = counterKey, wake = wake, noticeId = noticeId }
end
if not holds(dropped, 'string') then return { -1, 'dropped' } end
local droppedSoFar = redis.call('GET', dropped)
if droppedSoFar and not incrementable(droppedSoFar) then return { -1, 'dropped' } end
if not dn_holds(noticePending, 'set') then return { -1, 'notice_pending' } end
local overflow = dn_count(noticeOverflow)
if overflow == false or overflow > DN_MAX_COUNT - noticeIncrements then return { -1, 'notice_overflow' } end
if dn_count(noticeWindow) == false then return { -1, 'notice_window' } end
local pendingCount = redis.call('SCARD', noticePending)

-- Pass 2: write.
redis.call('HDEL', claims, claimId)
redis.call('ZREM', index, claimId)
local released = 0
local deadlettered = 0
local dlqDropped = 0
local function requeue(item)
  redis.call('RPUSH', inbox, item.task)
  if item.wake then wake_emit(events, item.wake) end
  released = released + 1
end
for _, item in ipairs(plan) do
  if not item.counterKey then
    -- Legacy / undecodable envelope: no per-message counter can be tracked.
    requeue(item)
  else
    local count = redis.call('INCR', item.counterKey)
    if count == 1 then
      redis.call('EXPIRE', item.counterKey, counterTtl)
    end
    if count > recoverCap then
      local length = redis.call('LPUSH', dlq, item.task)
      if length > dlqMaxLength then
        redis.call('LTRIM', dlq, 0, dlqMaxLength - 1)
        dlqDropped = dlqDropped + (length - dlqMaxLength)
      end
      redis.call('DEL', item.counterKey)
      if item.noticeId then
        if pendingCount < 256 then
          pendingCount = pendingCount + redis.call('SADD', noticePending, item.noticeId)
        elseif redis.call('SISMEMBER', noticePending, item.noticeId) == 0 then
          redis.call('INCR', noticeOverflow)
        end
      end
      deadlettered = deadlettered + 1
    else
      requeue(item)
    end
  end
end
if dlqDropped > 0 then redis.call('INCRBY', dropped, dlqDropped) end
return { 1, released, deadlettered, dlqDropped }
