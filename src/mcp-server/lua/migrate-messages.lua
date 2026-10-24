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
for _, key in ipairs({src, dst}) do
  if not dn_holds(key, 'list') then return redis.error_reply('mailbox key has wrong type') end
end
-- R5: exact actor keys, merged in this same transaction, including a rename
-- with no queued messages. Preserve the longest coalescing window.
local sourceActor = string.sub(src, #'gptq:q:' + 1)
local destActor = string.sub(dst, #'gptq:q:' + 1)
local function noticeKeys(actor)
  return { 'gptq:dlq-notice-pending:' .. actor, 'gptq:dlq-notice-overflow:' .. actor,
    'gptq:dlq-notice-window:' .. actor, 'gptq:dlq-notice-collisions:' .. actor }
end
local ns, nd = noticeKeys(sourceActor), noticeKeys(destActor)
for _, keys in ipairs({ns, nd}) do
  if not dn_holds(keys[1], 'set') then return redis.error_reply('notice pending key has wrong type') end
  for i = 2, 4 do
    if dn_count(keys[i]) == false then return redis.error_reply('notice counter is corrupt') end
  end
end
local pending = redis.call('SMEMBERS', nd[1])
if #pending > 256 then return redis.error_reply('notice pending exceeds cap') end
local present = {}
for _, id in ipairs(pending) do present[id] = true end
local incoming = redis.call('SMEMBERS', ns[1])
table.sort(incoming)
local extra = 0
for _, id in ipairs(incoming) do
  if not present[id] then
    if #pending < 256 then
      pending[#pending + 1] = id
      present[id] = true
    else
      extra = extra + 1
    end
  end
end
local sourceOverflow, destOverflow = dn_count(ns[2]), dn_count(nd[2])
if sourceOverflow > DN_MAX_COUNT - destOverflow or sourceOverflow + destOverflow > DN_MAX_COUNT - extra then
  return redis.error_reply('notice overflow exceeds integer capacity')
end
local mergedOverflow = sourceOverflow + destOverflow + extra
local sourceDiagnostic, destDiagnostic = dn_count(ns[4]), dn_count(nd[4])
if sourceDiagnostic > DN_MAX_COUNT - destDiagnostic then return redis.error_reply('notice diagnostic exceeds integer capacity') end
local sourceWindow, destWindow = redis.call('PTTL', ns[3]), redis.call('PTTL', nd[3])
local windowTtl = math.max(sourceWindow, destWindow)
if sourceWindow == -1 or destWindow == -1 then windowTtl = -1 end
if dlqSrc then
  local bound = dn_number(ARGV[1])
  if bound == false or bound < 1 then return redis.error_reply('invalid DLQ bound') end
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
-- All merge destinations and numeric additions have been checked.
for _, id in ipairs(pending) do redis.call('SADD', nd[1], id) end
if mergedOverflow > 0 then redis.call('SET', nd[2], string.format('%.0f', mergedOverflow)) end
if sourceDiagnostic + destDiagnostic > 0 then
  redis.call('SET', nd[4], string.format('%.0f', sourceDiagnostic + destDiagnostic))
end
if windowTtl == -1 then redis.call('SET', nd[3], '1')
elseif windowTtl > 0 then redis.call('SET', nd[3], '1', 'PX', windowTtl) end
redis.call('DEL', unpack(ns))
return #msgs
