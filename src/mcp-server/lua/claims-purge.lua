-- Lua FUNCTION (not a standalone script) that deletes an actor's durable
-- claim state when its identity is unregistered: every outstanding claim
-- (hash field + index), the dead-letter queue, the inbox event and trace
-- streams, and the per-message recovery counters of the messages that state
-- holds. Without this, a later agent registering the same name inherits
-- stranded claims and a stale DLQ. Callers compose it into a script (see
-- purgeActorClaimsLua in src/core/task-claim-store.ts) so retiring an
-- identity and purging its claims is one atomic step (review finding RF5).
--
-- Counters are deleted by exact key, never by prefix scan: plain names may
-- contain ':', so a scan of "a"'s prefix would also match agent "a:b" (RF7).
-- The keys are built from the message ids found in the purged claims and
-- the DLQ; they must match CLAIM_KEYS.recoverCount in src/core/keys.ts, as
-- in claims-recover.lua. Two kinds of counter are left to expire with their
-- TTL (RECOVER_COUNTER_TTL_SECONDS) instead:
--   * counters of already-acknowledged messages, which nothing references;
--   * counters of messages still queued in the inbox (FIX8). Collecting
--     them meant decoding every queued payload inside this blocking script,
--     and recovery can grow an inbox past its send bound. The caller deletes
--     the inbox, and a stale counter only shortens the recovery budget of a
--     later message with the same id under the same name.
--
-- Callers run this after deleting the session and registry entry, and Redis
-- does not roll back a script that errors, so nothing here may raise on a
-- corrupt key (FIX3): a key that does not hold its expected type is skipped
-- when collecting counter ids and then deleted regardless of its type.
--
-- claims  = claims hash (gptq:claims)
-- index   = claims index zset (gptq:claims-index:<actor_id>)
-- dlq     = DLQ list (gptq:dlq:<actor_id>)
-- events  = inbox event stream (gptq:inbox-events:<actor_id>)
-- trace   = inbox trace stream (gptq:inbox-trace:<actor_id>)
-- actor   = the actor id (counter key component)
--
-- Returns the number of claims removed.
local function purge_actor_claims(claims, index, dlq, events, trace, actor)
  local counters = {}
  local function track(payload)
    local ok, msg = pcall(cjson.decode, payload)
    if ok and type(msg) == 'table' and type(msg.id) == 'string' and #msg.id > 0 then
      counters['gptq:rc:' .. actor .. ':' .. msg.id] = true
    end
  end
  local function holds(key, kind)
    return redis.call('TYPE', key).ok == kind
  end
  local claimIds = {}
  if holds(index, 'zset') then claimIds = redis.call('ZRANGE', index, 0, -1) end
  if #claimIds > 0 and holds(claims, 'hash') then
    for _, claimId in ipairs(claimIds) do
      local raw = redis.call('HGET', claims, claimId)
      if raw then
        local ok, claim = pcall(cjson.decode, raw)
        if ok and type(claim) == 'table' and type(claim.tasks) == 'table' then
          for _, task in ipairs(claim.tasks) do
            if type(task) == 'string' then track(task) end
          end
        end
      end
      redis.call('HDEL', claims, claimId)
    end
  end
  if holds(dlq, 'list') then
    for _, payload in ipairs(redis.call('LRANGE', dlq, 0, -1)) do track(payload) end
  end
  for key in pairs(counters) do redis.call('DEL', key) end
  redis.call('DEL', index, dlq, events, trace)
  return #claimIds
end
