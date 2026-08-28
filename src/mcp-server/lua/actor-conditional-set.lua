-- Atomic conditional HSET for a durable actor directory record.
--
-- The full actor directory semantics live in TypeScript
-- (src/core/actor-directory.ts). This script performs NO business logic: it
-- only compares the stored JSON's `.registered_by` session id against an
-- expected owner passed via ARGV, then writes the new value if the field is
-- absent (first registration) or the stored owner matches. A durable actor's
-- profile is owned by the session that registered it; the same session may
-- update it. If the owner does not match (a concurrent/foreign writer won),
-- it returns 0 and the caller re-reads and derives the resulting domain
-- error.
--
-- KEYS[1] = actor profiles hash key
-- ARGV[1] = field (actor_id)
-- ARGV[2] = new serialized JSON value
-- ARGV[3] = expected owner `.registered_by`
--
-- Returns 1 when the write succeeded, 0 when the precondition failed.
local key = KEYS[1]
local field = ARGV[1]
local newValue = ARGV[2]
local expectedOwner = ARGV[3]

local current = redis.call('HGET', key, field)
if current == false then
  redis.call('HSET', key, field, newValue)
  return 1
end

local record
local decoded = pcall(function() record = cjson.decode(current) end)
if not decoded then return 0 end
if type(record) ~= 'table' or record.registered_by ~= expectedOwner then return 0 end

redis.call('HSET', key, field, newValue)
return 1