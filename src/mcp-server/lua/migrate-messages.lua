-- Atomically transfer every message from the source mailbox to the tail of
-- the destination mailbox, preserving FIFO order, or leave the source
-- untouched. Replaces the legacy LPOP/RPUSH loop, which could permanently
-- lose a message if the process died between an LPOP and its RPUSH (review
-- finding F2). Redis executes the whole script atomically: no reader can
-- observe a partial transfer, and any failure leaves the source exactly as
-- it was — either every element has moved or none has.
--
-- D3: a same-key call (from == to) would LRANGE, RPUSH (doubling), then DEL
-- the only copy — silent total mailbox loss. Refuse it as a no-op.
--
-- Optional KEYS[3] is the source actor's claims index. When it holds ANY
-- claim the transfer is refused (returns -1): claimed batches are owned by
-- the source identity and would be stranded under a name nothing recovers.
-- Expired claims block too (RF1): the caller recovers them back onto the
-- source inbox first, so one still indexed here either is live or expired
-- after that recovery, and either way its tasks are not in the inbox.
--
-- Optional last two KEYS, with ARGV[1] = the DLQ length bound, are the
-- source and destination dead-letter queues (FIX5). A rename's recovery can
-- quarantine a capped task into the source DLQ, which the new name could
-- never list or requeue. The whole source DLQ moves under the same guard and
-- in the same atomic step, to the destination's head (DLQs are newest
-- first, and the source's entries include that just-quarantined task) in its
-- own order. The destination is then trimmed to its bound, keeping the
-- newest entries, as recovery does; DLQ lists carry no TTL. Both DLQs are
-- type-checked before anything moves.
--
-- KEYS: 1 source inbox, 2 destination inbox, [3 source claims index],
--       [source DLQ, destination DLQ]
local src = KEYS[1]
local dst = KEYS[2]
local claimsIndex, dlqSrc, dlqDst
if #KEYS == 3 or #KEYS == 5 then claimsIndex = KEYS[3] end
if #KEYS >= 4 then dlqSrc, dlqDst = KEYS[#KEYS - 1], KEYS[#KEYS] end
if src == dst then
  return 0
end
if claimsIndex and redis.call('ZCARD', claimsIndex) > 0 then
  return -1
end
if dlqSrc then
  for _, key in ipairs({ dlqSrc, dlqDst }) do
    local t = redis.call('TYPE', key).ok
    if t ~= 'none' and t ~= 'list' then
      return redis.error_reply('dead-letter queue key has wrong type')
    end
  end
end
-- RPUSH/LPUSH in bounded chunks: Lua's unpack is host-stack-limited (~8000
-- elements), so a large mailbox must not overflow it. Chunking is purely a
-- host-limit concern; the script remains atomic throughout.
local chunk = 256
local msgs = redis.call('LRANGE', src, 0, -1)
if #msgs > 0 then
  for i = 1, #msgs, chunk do
    local part = {}
    for j = i, math.min(i + chunk - 1, #msgs) do
      part[#part + 1] = msgs[j]
    end
    redis.call('RPUSH', dst, unpack(part))
  end
  redis.call('DEL', src)
end
if dlqSrc and dlqSrc ~= dlqDst then
  local dead = redis.call('LRANGE', dlqSrc, 0, -1)
  if #dead > 0 then
    -- LPUSH from the source's tail so its head lands at the destination's head.
    for i = #dead, 1, -chunk do
      local part = {}
      for j = i, math.max(i - chunk + 1, 1), -1 do
        part[#part + 1] = dead[j]
      end
      redis.call('LPUSH', dlqDst, unpack(part))
    end
    redis.call('LTRIM', dlqDst, 0, tonumber(ARGV[1]) - 1)
    redis.call('DEL', dlqSrc)
  end
end
return #msgs
