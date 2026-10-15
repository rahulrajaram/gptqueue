-- Atomic bounded push plus an eligible header-only inbox event and its
-- correlation record. KEYS[6] task ledger of this message, KEYS[7] answered
-- marker of its in_reply_to; ARGV[9] subject, ARGV[10] ISO now.
-- KEYS[5] (COR-F2): the recipient's forwarding tombstone. A retired or
-- adopted provisional name answers {-1, target|'retired'} so the caller
-- forwards or refuses, instead of pushing into a queue nothing will claim.
local forwarded = redis.call('GET', KEYS[5])
if forwarded then return {-1, forwarded} end
local current = redis.call('LLEN', KEYS[1])
if current >= tonumber(ARGV[2]) then return {0, ''} end
if redis.call('TYPE', KEYS[2]).ok ~= 'none' and redis.call('TYPE', KEYS[2]).ok ~= 'hash' then return redis.error_reply('mailbox metadata key has wrong type') end
if redis.call('TYPE', KEYS[3]).ok ~= 'none' and redis.call('TYPE', KEYS[3]).ok ~= 'stream' then return redis.error_reply('inbox event key has wrong type') end
if redis.call('TYPE', KEYS[4]).ok ~= 'none' and redis.call('TYPE', KEYS[4]).ok ~= 'string' then return redis.error_reply('outstanding key has wrong type') end
local envelope = cjson.decode(ARGV[1])
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'current_size', current + 1)
local owned = ARGV[5] == '1' and ARGV[4] ~= 'task' and redis.call('GET', KEYS[4]) == ARGV[8]
local notify = ARGV[4] == 'task' or owned
if notify then
  redis.call('XADD', KEYS[3], 'MAXLEN', '~', 1024, '*', 'message_id', ARGV[3], 'type', ARGV[4], 'timestamp', envelope.timestamp, 'from', envelope.from, 'to', envelope.to, 'in_reply_to', envelope.payload.in_reply_to or '')
end
-- S8: correlation records commit with the delivery. A task records its ledger
-- (subject computed in TS); a result/error from the request's owner (the same
-- ownership test that gates its wake event) marks the request answered.
if ARGV[4] == 'task' then
  redis.call('SET', KEYS[6], cjson.encode({from = envelope.from, to = envelope.to, subject = ARGV[9], sent_at = envelope.timestamp}), 'EX', 604800)
elseif (ARGV[4] == 'result' or ARGV[4] == 'error') and owned then
  redis.call('SET', KEYS[7], ARGV[10], 'EX', 604800)
end
if ARGV[6] == '1' then redis.call('SET', KEYS[4], ARGV[7], 'EX', 604800) end
return {1, ''}
