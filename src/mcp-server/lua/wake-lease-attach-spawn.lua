-- Best-effort, conditional attach of spawn evidence to a wake lease.
--
-- Full semantics live in TypeScript (src/core/wake-lease.ts): this is pure
-- bookkeeping so an observer (e.g. actor_status) can surface which pid the
-- activation was spawned with. It updates only when the stored lease's
-- `.lease_id` matches the caller's, and preserves the lease TTL (KEEPTTL) so
-- attaching evidence never turns a transient activation lease permanent. A
-- mismatch or a missing/expired lease returns 0 (attached:false) — NOT an
-- error — so a best-effort spawn report is always idempotent.
--
-- KEYS[1] = wake lease string key (per actor, with TTL)
-- ARGV[1] = lease_id the caller expects
-- ARGV[2] = pid (integer)
-- ARGV[3] = spawned_at (ISO)
--
-- Returns 1 when attached, 0 when the precondition failed or the key is absent.
local key = KEYS[1]
local expectedLeaseId = ARGV[1]
local pid = tonumber(ARGV[2])
local spawnedAt = ARGV[3]

local current = redis.call('GET', key)
if current == false then return 0 end

local lease
local decoded = pcall(function() lease = cjson.decode(current) end)
if not decoded then return 0 end
if type(lease) ~= 'table' or lease.lease_id ~= expectedLeaseId then return 0 end

lease.spawned_pid = pid
lease.spawned_at = spawnedAt
redis.call('SET', key, cjson.encode(lease), 'KEEPTTL')
return 1