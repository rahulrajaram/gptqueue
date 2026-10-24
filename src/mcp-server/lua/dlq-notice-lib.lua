-- Shared read-only validation for durable DLQ notice state. Counts stay in
-- Lua/JavaScript's exact integer range; validate headroom before any writes.
local DN_MAX_COUNT = 9007199254740991
local function dn_holds(key, kind)
  local t = redis.call('TYPE', key).ok
  return t == 'none' or t == kind
end
local function dn_number(raw)
  if not raw then return 0 end
  if raw ~= '0' and not string.match(raw, '^[1-9]%d*$') then return false end
  local n = tonumber(raw)
  if not n or n > DN_MAX_COUNT then return false end
  return n
end
local function dn_count(key)
  if not dn_holds(key, 'string') then return false end
  return dn_number(redis.call('GET', key))
end
