-- Atomically delete an actor's durable claim state when its identity is
-- unregistered: every outstanding claim (hash field + index), the dead-letter
-- queue, and the inbox event and trace streams. Without this, a later agent
-- registering the same name inherits stranded claims and a stale DLQ.
--
-- KEYS[1] = claims hash (gptq:claims)
-- KEYS[2] = claims index zset (gptq:claims-index:<actor_id>)
-- KEYS[3] = DLQ list (gptq:dlq:<actor_id>)
-- KEYS[4] = inbox event stream (gptq:inbox-events:<actor_id>)
-- KEYS[5] = inbox trace stream (gptq:inbox-trace:<actor_id>)
--
-- Returns the number of claims removed.
local claims = redis.call('ZRANGE', KEYS[2], 0, -1)
for _, claimId in ipairs(claims) do
  redis.call('HDEL', KEYS[1], claimId)
end
redis.call('DEL', KEYS[2], KEYS[3], KEYS[4], KEYS[5])
return #claims
