-- Atomic: check LLEN, RPUSH only if under limit, else return 0
local current = redis.call('LLEN', KEYS[1])
if current >= tonumber(ARGV[2]) then return 0 end
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], 'current_size', current + 1)
return 1
