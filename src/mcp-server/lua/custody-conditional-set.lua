-- Atomic conditional HSET for a worktree custody record.
--
-- The full custody semantics live in TypeScript (src/core/custody-store.ts).
-- This script performs NO business logic: it only compares the stored JSON's
-- `.state` string field (and, for release, `.custodian.session_id`) against an
-- expected precondition passed via ARGV, then writes the new value if it
-- matches. If it does not match (a concurrent writer won), it returns 0 and
-- the caller re-reads and derives the resulting domain error.
--
-- KEYS[1] = custody records hash key
-- ARGV[1] = field (worktree_path)
-- ARGV[2] = new serialized JSON value
-- ARGV[3] = expected precondition `.state`, or "ABSENT" for field absence
-- ARGV[4] = (release only) expected custodian `.session_id`; "" disables the check
--
-- Returns 1 when the write succeeded, 0 when the precondition failed.
local key = KEYS[1]
local field = ARGV[1]
local newValue = ARGV[2]
local expectedState = ARGV[3]
local expectedCustodianSession = ARGV[4]

local current = redis.call('HGET', key, field)
if current == false then
  if expectedState == 'ABSENT' then
    redis.call('HSET', key, field, newValue)
    return 1
  end
  return 0
end

local record
local decoded = pcall(function() record = cjson.decode(current) end)
if not decoded then return 0 end
if type(record) ~= 'table' or record.state ~= expectedState then return 0 end

if expectedCustodianSession ~= '' then
  local custodian = record.custodian
  if type(custodian) ~= 'table' or custodian.session_id ~= expectedCustodianSession then
    return 0
  end
end

redis.call('HSET', key, field, newValue)
return 1