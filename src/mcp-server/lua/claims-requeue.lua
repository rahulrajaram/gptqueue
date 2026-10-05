-- Atomically move one dead-lettered task back to the actor's inbox tail.
--
-- Full requeue semantics live in TypeScript (src/core/task-claim-store.ts).
-- This script is the atomicity boundary: it finds the first DLQ entry (newest
-- first) whose decoded envelope id equals the requested message id, RPUSHes
-- the raw payload onto the inbox, then LREMs exactly that entry and DELs the
-- message's recovery counter so it gets a fresh budget. The destination write
-- runs first because Lua scripts do not roll back earlier writes when a later
-- redis.call errors: a WRONGTYPE inbox must abort before the DLQ is touched.
-- A crash can no longer strand a message between the DLQ removal and the
-- inbox push. Entries that
-- do not decode to a table with a string id are skipped, as before.
--
-- KEYS[1] = DLQ list (gptq:dlq:<actor_id>)
-- KEYS[2] = inbox list (gptq:q:<actor_id>)
-- KEYS[3] = recovery counter (gptq:rc:<actor_id>:<message_id>)
-- KEYS[4] = inbox event stream (gptq:inbox-events:<actor_id>)
-- KEYS[5] = mailbox metadata hash (gptq:meta:<actor_id>)
-- ARGV[1] = message_id
--
-- Like recovery, a requeue is not bounded by the send admission bound (the
-- operator explicitly asked for this work back); it emits the same inbox
-- event a fresh send would (wake_plan in claims-lib.lua, composed in front)
-- and sets current_size to the inbox's true length so any overshoot shows.
-- Every type check runs before the first write.
--
-- Returns 1 when requeued, 0 when no matching DLQ entry exists.
for _, payload in ipairs(redis.call('LRANGE', KEYS[1], 0, -1)) do
  local ok, msg = pcall(cjson.decode, payload)
  if ok and type(msg) == 'table' and type(msg.id) == 'string' and msg.id == ARGV[1] then
    for i, kind in ipairs({ 'list', 'list', 'string', 'stream', 'hash' }) do
      local found = redis.call('TYPE', KEYS[i]).ok
      if found ~= 'none' and found ~= kind then
        return redis.error_reply('WRONGTYPE requeue key ' .. KEYS[i] .. ' holds ' .. found)
      end
    end
    local wake = wake_plan(payload)
    if wake == false then return redis.error_reply('WRONGTYPE outstanding-request key for ' .. ARGV[1]) end
    redis.call('RPUSH', KEYS[2], payload)
    if wake then wake_emit(KEYS[4], wake) end
    redis.call('HSET', KEYS[5], 'current_size', redis.call('LLEN', KEYS[2]))
    redis.call('LREM', KEYS[1], 1, payload)
    redis.call('DEL', KEYS[3])
    return 1
  end
end
return 0
