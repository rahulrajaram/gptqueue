-- Atomic bounded push with retry deduplication.
local existing = redis.call('GET', KEYS[3])
if existing then return {2, existing} end
local current = redis.call('LLEN', KEYS[1])
if current >= tonumber(ARGV[2]) then return {0, ARGV[3]} end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'current_size', current + 1)
redis.call('SET', KEYS[3], ARGV[3], 'EX', tonumber(ARGV[4]))
return {1, ARGV[3]}
