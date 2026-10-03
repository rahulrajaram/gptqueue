-- Recover expired durable task claims for one actor back into its inbox.
--
-- Full recovery semantics (and the documented re-enter-at-the-tail ordering
-- caveat) live in TypeScript (src/core/task-claim-store.ts). This script
-- performs the durable move: it finds every outstanding claim in the actor's
-- index whose score (expires_at epoch ms) has passed, reads the claim's tasks,
-- and for each task either RPUSHes it back onto the actor's inbox list (in the
-- original pop order) or, once a per-message recovery counter exceeds the cap,
-- quarantines it to the actor's dead-letter queue (DLQ) instead. It then
-- removes the claim from the hash and the index. Non-atomic per-message
-- ordering across claims is acceptable; the per-claim task order is preserved
-- by the RPUSH sequence below.
--
-- Recovery counters (gptq:rc:<actor_id>:<message_id>) are INCR'd per task as
-- it is about to be re-queued. When the resulting count EXCEEDS the cap, the
-- raw task payload is LPUSHed onto the DLQ (gptq:dlq:<actor_id>) instead of
-- RPUSHed to the inbox, the DLQ is LTRIM'd to its bound (newest retained), and
-- the counter key is DELeted so a future requeue starts a fresh budget. The
-- counter key gets a long EX on first INCR to bound orphans. A task whose
-- envelope cannot be decoded to a message id cannot be tracked per-message, so
-- it is simply re-queued to the inbox without counter accounting (legacy
-- payloads survive; they are never counter-quarantined on their own).
--
-- Every fallible check runs before the first write (FIX1): the inbox and the
-- DLQ must be lists (or absent) and every recovery counter a canonical
-- integer string INCR can still increment (or absent). Otherwise the script returns { -1, <role> } and changes
-- nothing, so an expired claim is never deleted while its tasks have no
-- durable destination. A rename recovers through this script, so its
-- rollback relies on that.
--
-- KEYS[1] = claims index zset (gptq:claims-index:<actor_id>)
-- KEYS[2] = claims hash (gptq:claims)
-- KEYS[3] = inbox list (gptq:q:<actor_id>)
-- KEYS[4] = DLQ list (gptq:dlq:<actor_id>)
-- ARGV[1] = now epoch ms cutoff; claims with score < now are expired
-- ARGV[2] = recover_cap (int >= 1): a message recovered more than cap times is dead-lettered
-- ARGV[3] = dlq_max_length (int >= 1): LTRIM bound on the DLQ list (keep newest)
-- ARGV[4] = counter_ttl_seconds (int >= 1): EX placed on a fresh recovery counter
--
-- Returns { recovered_tasks, deadlettered_tasks }, or { -1, 'inbox' | 'dlq' |
-- 'counter' } when that destination holds the wrong type.
local index = KEYS[1]
local claims = KEYS[2]
local inbox = KEYS[3]
local dlq = KEYS[4]
local now = tonumber(ARGV[1])
local recoverCap = tonumber(ARGV[2])
local dlqMaxLength = tonumber(ARGV[3])
local counterTtl = tonumber(ARGV[4])

-- Scores are integer epoch ms; "expired" means expires_at_ms < now, so the
-- cutoff excludes a claim whose score exactly equals the present instant.
local expired = redis.call('ZRANGEBYSCORE', index, '-inf', now - 1)
if #expired == 0 then
  return { 0, 0 }
end

local function holds(key, kind)
  local t = redis.call('TYPE', key).ok
  return t == 'none' or t == kind
end

-- INCR accepts only a canonical decimal int64 ('0', or an optional '-' and
-- no leading zero: not '+5', '05', ' 5' or '-0'), and fails on one at its
-- maximum. At most 18 digits keeps |value| < 10^18, far from either end of
-- the int64 range, so INCR cannot fail after the claim is deleted (L1).
local function incrementable(value)
  if value == '0' then return true end
  local digits = string.match(value, '^%-?([1-9]%d*)$')
  return digits ~= nil and #digits <= 18
end

-- Pass 1 (read-only): decode every expired claim and validate each
-- destination its tasks could reach. An entry's counterKey is nil for a
-- legacy envelope, or a claim without an actor id, which is re-queued
-- without counter accounting.
local plan = {}
for _, claimId in ipairs(expired) do
  local entry = { id = claimId, tasks = {} }
  local raw = redis.call('HGET', claims, claimId)
  if raw then
    local claim
    local decoded = pcall(function() claim = cjson.decode(raw) end)
    if decoded and type(claim) == 'table' and type(claim.tasks) == 'table' then
      local actorId = claim.actor_id
      for _, task in ipairs(claim.tasks) do
        local msg
        local msgDecoded = pcall(function() msg = cjson.decode(task) end)
        local msgId
        if msgDecoded and type(msg) == 'table' then
          msgId = msg.id
        end
        local counterKey
        if type(msgId) == 'string' and #msgId > 0 and type(actorId) == 'string' then
          counterKey = 'gptq:rc:' .. actorId .. ':' .. msgId
          if not holds(counterKey, 'string') then return { -1, 'counter' } end
          local current = redis.call('GET', counterKey)
          if current and not incrementable(current) then return { -1, 'counter' } end
        end
        entry.tasks[#entry.tasks + 1] = { task = task, counterKey = counterKey }
      end
    end
  end
  plan[#plan + 1] = entry
end
if not holds(inbox, 'list') then return { -1, 'inbox' } end
if not holds(dlq, 'list') then return { -1, 'dlq' } end

-- Pass 2: every write below targets a validated key.
local recovered = 0
local deadlettered = 0
for _, entry in ipairs(plan) do
  -- Remove the claim regardless of readability so the index/hash cannot leak
  -- entries; only recoverable tasks are re-queued.
  redis.call('HDEL', claims, entry.id)
  redis.call('ZREM', index, entry.id)
  for _, item in ipairs(entry.tasks) do
    local task = item.task
    local counterKey = item.counterKey
    if not counterKey then
      -- Legacy / undecodable envelope, or a corrupt claim record without an
      -- actor id: no per-message counter can be tracked. Re-queue directly.
      redis.call('RPUSH', inbox, task)
      recovered = recovered + 1
    else
      local count = redis.call('INCR', counterKey)
      if count == 1 then
        redis.call('EXPIRE', counterKey, counterTtl)
      end
      if count > recoverCap then
        redis.call('LPUSH', dlq, task)
        redis.call('LTRIM', dlq, 0, dlqMaxLength - 1)
        redis.call('DEL', counterKey)
        deadlettered = deadlettered + 1
      else
        redis.call('RPUSH', inbox, task)
        recovered = recovered + 1
      end
    end
  end
end
return { recovered, deadlettered }
