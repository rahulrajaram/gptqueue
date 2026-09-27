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
-- Optional KEYS[3] is the source actor's claims index (scores are expiry
-- times in epoch ms) with ARGV[1] = now in epoch ms. When it holds an
-- unexpired claim the transfer is refused (returns -1): claimed batches are
-- owned by the source identity and would be stranded under a name nothing
-- recovers. Already-expired entries do not block; recovery re-queues them.
local src = KEYS[1]
local dst = KEYS[2]
if src == dst then
  return 0
end
if KEYS[3] and redis.call('ZCOUNT', KEYS[3], ARGV[1], '+inf') > 0 then
  return -1
end
local msgs = redis.call('LRANGE', src, 0, -1)
if #msgs == 0 then
  return 0
end
-- RPUSH in bounded chunks: Lua's unpack is host-stack-limited (~8000
-- elements), so a large mailbox must not overflow it. Chunking is purely a
-- host-limit concern; the script remains atomic throughout.
local chunk = 256
for i = 1, #msgs, chunk do
  local part = {}
  for j = i, math.min(i + chunk - 1, #msgs) do
    part[#part + 1] = msgs[j]
  end
  redis.call('RPUSH', dst, unpack(part))
end
redis.call('DEL', src)
return #msgs
