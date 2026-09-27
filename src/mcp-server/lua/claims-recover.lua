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
-- KEYS[1] = claims index zset (gptq:claims-index:<actor_id>)
-- KEYS[2] = claims hash (gptq:claims)
-- KEYS[3] = inbox list (gptq:q:<actor_id>)
-- KEYS[4] = DLQ list (gptq:dlq:<actor_id>)
-- ARGV[1] = now epoch ms cutoff; claims with score < now are expired
-- ARGV[2] = recover_cap (int >= 1): a message recovered more than cap times is dead-lettered
-- ARGV[3] = dlq_max_length (int >= 1): LTRIM bound on the DLQ list (keep newest)
-- ARGV[4] = counter_ttl_seconds (int >= 1): EX placed on a fresh recovery counter
--
-- Returns { recovered_tasks, deadlettered_tasks }.
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
local recovered = 0
local deadlettered = 0
for _, claimId in ipairs(expired) do
  local raw = redis.call('HGET', claims, claimId)
  -- Remove the claim regardless of readability so the index/hash cannot leak
  -- entries; only recoverable tasks are re-queued.
  redis.call('HDEL', claims, claimId)
  redis.call('ZREM', index, claimId)
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
        -- A claim record without an actor id (corrupt) cannot key a counter;
        -- re-queue rather than erroring after the claim was already removed.
        if type(msgId) ~= 'string' or #msgId == 0 or type(actorId) ~= 'string' then
          -- Legacy / undecodable envelope: no message id, so no per-message
          -- counter can be tracked. Re-queue directly to the inbox.
          redis.call('RPUSH', inbox, task)
          recovered = recovered + 1
        else
          local counterKey = 'gptq:rc:' .. actorId .. ':' .. msgId
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
  end
end
return { recovered, deadlettered }