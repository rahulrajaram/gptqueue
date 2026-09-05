-- Atomic bounded push, retry deduplication, and eligible inbox event.
local existing = redis.call('GET', KEYS[3])
if existing then return {2, existing} end
local current = redis.call('LLEN', KEYS[1])
if current >= tonumber(ARGV[2]) then return {0, ARGV[3]} end
if redis.call('TYPE', KEYS[2]).ok ~= 'none' and redis.call('TYPE', KEYS[2]).ok ~= 'hash' then return redis.error_reply('mailbox metadata key has wrong type') end
if redis.call('TYPE', KEYS[4]).ok ~= 'none' and redis.call('TYPE', KEYS[4]).ok ~= 'stream' then return redis.error_reply('inbox event key has wrong type') end
if redis.call('TYPE', KEYS[5]).ok ~= 'none' and redis.call('TYPE', KEYS[5]).ok ~= 'string' then return redis.error_reply('outstanding key has wrong type') end
local envelope = cjson.decode(ARGV[1])
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'current_size', current + 1)
redis.call('SET', KEYS[3], ARGV[3], 'EX', tonumber(ARGV[4]))
local notify = ARGV[6] == 'task' or (ARGV[5] == '1' and redis.call('GET', KEYS[5]) == ARGV[9])
if notify then
  redis.call('XADD', KEYS[4], 'MAXLEN', '~', 1024, '*', 'message_id', ARGV[3], 'type', ARGV[6], 'timestamp', envelope.timestamp, 'from', envelope.from, 'to', envelope.to, 'in_reply_to', envelope.payload.in_reply_to or '')
end
if ARGV[7] == '1' then redis.call('SET', KEYS[5], ARGV[8], 'EX', 604800) end
return {1, ARGV[3]}
