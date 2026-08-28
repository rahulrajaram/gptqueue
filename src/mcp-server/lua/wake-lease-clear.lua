-- Atomic conditional delete of a per-actor wake lease.
--
-- Deletes the lease only when the stored JSON's `.lease_id` matches the lease
-- the caller holds. A mismatch (a newer controller already replaced the lease,
-- or this is an idempotent runtime_ready race) returns 0 and leaves the lease
-- untouched; a missing lease returns 0 as well.
--
-- KEYS[1] = wake lease string key (per actor, with TTL)
-- ARGV[1] = lease_id the caller expects
--
-- Returns 1 when deleted, 0 when the precondition failed or the key is absent.
local key = KEYS[1]
local expectedLeaseId = ARGV[1]

local current = redis.call('GET', key)
if current == false then return 0 end

local lease
local decoded = pcall(function() lease = cjson.decode(current) end)
if not decoded then return 0 end
if type(lease) ~= 'table' or lease.lease_id ~= expectedLeaseId then return 0 end

redis.call('DEL', key)
return 1