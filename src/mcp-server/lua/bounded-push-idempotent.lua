-- Atomic bounded push, retry deduplication, eligible inbox event and its
-- correlation record (a replay returns before any write). KEYS[7] task ledger,
-- KEYS[8] answered marker; ARGV[10] subject, ARGV[11] ISO now, ARGV[12] the
-- outstanding-key prefix. Admission is the waking rule in
-- waking-admission-lib.lua (composed in front), checked after the replay
-- check; a refusal writes nothing (no dedupe record) and answers
-- {0, id, reason}.
local existing = redis.call('GET', KEYS[3])
if existing then return {2, existing} end
-- KEYS[6] (COR-F2): the recipient's forwarding tombstone; see bounded-push.lua.
local forwarded = redis.call('GET', KEYS[6])
if forwarded then return {-1, forwarded} end
local current = redis.call('LLEN', KEYS[1])
local refusal = adm_refusal(KEYS[1], current, tonumber(ARGV[2]), ARGV[6] == 'task' or (ARGV[5] == '1' and adm_owner(KEYS[5]) == ARGV[9]), ARGV[8], ARGV[12])
if refusal then return {0, ARGV[3], refusal} end
if redis.call('TYPE', KEYS[2]).ok ~= 'none' and redis.call('TYPE', KEYS[2]).ok ~= 'hash' then return redis.error_reply('mailbox metadata key has wrong type') end
if redis.call('TYPE', KEYS[4]).ok ~= 'none' and redis.call('TYPE', KEYS[4]).ok ~= 'stream' then return redis.error_reply('inbox event key has wrong type') end
if redis.call('TYPE', KEYS[5]).ok ~= 'none' and redis.call('TYPE', KEYS[5]).ok ~= 'string' then return redis.error_reply('outstanding key has wrong type') end
local envelope = cjson.decode(ARGV[1])
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'current_size', current + 1)
redis.call('SET', KEYS[3], ARGV[3], 'EX', tonumber(ARGV[4]))
local owned = ARGV[5] == '1' and ARGV[6] ~= 'task' and redis.call('GET', KEYS[5]) == ARGV[9]
local notify = ARGV[6] == 'task' or owned
if notify then
  redis.call('XADD', KEYS[4], 'MAXLEN', '~', 1024, '*', 'message_id', ARGV[3], 'type', ARGV[6], 'timestamp', envelope.timestamp, 'from', envelope.from, 'to', envelope.to, 'in_reply_to', envelope.payload.in_reply_to or '')
end
-- S8: correlation records commit with the delivery. A task records its ledger
-- (subject computed in TS); a result/error from the request's owner (the same
-- ownership test that gates its wake event) marks the request answered.
if ARGV[6] == 'task' then
  redis.call('SET', KEYS[7], cjson.encode({from = envelope.from, to = envelope.to, subject = ARGV[10], sent_at = envelope.timestamp}), 'EX', 604800)
elseif (ARGV[6] == 'result' or ARGV[6] == 'error') and owned then
  redis.call('SET', KEYS[8], ARGV[11], 'EX', 604800)
end
if ARGV[7] == '1' then redis.call('SET', KEYS[5], ARGV[8], 'EX', 604800) end
return {1, ARGV[3]}
