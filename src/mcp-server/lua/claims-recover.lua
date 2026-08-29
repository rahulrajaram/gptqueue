-- Recover expired durable task claims for one actor back into its inbox.
--
-- Full recovery semantics (and the documented re-enter-at-the-tail ordering
-- caveat) live in TypeScript (src/core/task-claim-store.ts). This script
-- performs the durable move: it finds every outstanding claim in the actor's
-- index whose score (expires_at epoch ms) has passed, reads the claim's tasks,
-- RPUSHes them back onto the actor's inbox list in their original pop order,
-- then removes the claim from the hash and the index. Non-atomic per-message
-- ordering across claims is acceptable; the per-claim task order is preserved
-- by the RPUSH sequence below.
--
-- KEYS[1] = claims index zset (gptq:claims-index:<actor_id>)
-- KEYS[2] = claims hash (gptq:claims)
-- KEYS[3] = inbox list (gptq:q:<actor_id>)
-- ARGV[1] = now epoch ms cutoff; claims with score < now are expired
--
-- Returns the number of tasks recovered (RPUSHed back to the inbox).
local index = KEYS[1]
local claims = KEYS[2]
local inbox = KEYS[3]
local now = tonumber(ARGV[1])

-- Scores are integer epoch ms; "expired" means expires_at_ms < now, so the
-- cutoff excludes a claim whose score exactly equals the present instant.
local expired = redis.call('ZRANGEBYSCORE', index, '-inf', now - 1)
local recovered = 0
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
      for _, task in ipairs(claim.tasks) do
        redis.call('RPUSH', inbox, task)
        recovered = recovered + 1
      end
    end
  end
end
return recovered