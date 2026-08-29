-- Atomic, bounded batch-claim of durable inbox messages.
--
-- Full claim semantics live in TypeScript (src/core/task-claim-store.ts). This
-- script performs NO business logic beyond the atomicity boundary: it LPOPs up
-- to max_batch messages off the actor's inbox, and if at least one message was
-- popped it materializes the claim record, persists it in the claims hash,
-- indexes it in the per-actor zset, and returns the stored JSON. If the inbox
-- was empty it returns a sentinel so the caller maps it to an empty batch
-- (claim:null) rather than an error.
--
-- KEYS[1] = inbox list (gptq:q:<actor_id>)
-- KEYS[2] = claims hash (gptq:claims)
-- KEYS[3] = claims index zset (gptq:claims-index:<actor_id>)
-- ARGV[1] = claim_id (caller-supplied uuid)
-- ARGV[2] = actor_id
-- ARGV[3] = session_id (owning runtime session)
-- ARGV[4] = claimed_at (ISO)
-- ARGV[5] = expires_at (ISO)
-- ARGV[6] = expires_at epoch ms (zset score)
-- ARGV[7] = max_batch (int, already validated by the caller)
-- ARGV[8] = max_concurrent_claims (int >= 1) when the caller enforces an
--           outstanding-claims ceiling, or 0 as the "unlimited" sentinel.
--
-- Returns { 1, stored_json } on a claim,
--         { 0, '' }          when the inbox is empty,
--         { 2, '' }          when the actor's outstanding-claim count is at
--                             (or past) the max_concurrent_claims ceiling.
local inbox = KEYS[1]
local claims = KEYS[2]
local index = KEYS[3]

local batch = tonumber(ARGV[7])
local maxConcurrent = tonumber(ARGV[8])

-- Enforce the outstanding-claims ceiling BEFORE popping. The actor's index
-- zset is accurate at this instant because lazy recovery (claims-recover.lua)
-- has already run and removed expired claims, so ZCARD is the true count of
-- live unacked claims. At/over the ceiling we refuse to pop (and thus refuse
-- to materialize another claim) so the ordering and validation semantics for
-- the ceiling stay atomic with the pop.
if maxConcurrent and maxConcurrent > 0 then
  local outstanding = redis.call('ZCARD', index)
  if outstanding >= maxConcurrent then
    return { 2, '' }
  end
end
local tasks = {}
for i = 1, batch do
  local msg = redis.call('LPOP', inbox)
  if msg == false then break end
  tasks[#tasks + 1] = msg
end

if #tasks == 0 then
  return { 0, '' }
end

local claim = {
  claim_id = ARGV[1],
  actor_id = ARGV[2],
  session_id = ARGV[3],
  claimed_at = ARGV[4],
  expires_at = ARGV[5],
  tasks = tasks,
}
redis.call('HSET', claims, ARGV[1], cjson.encode(claim))
redis.call('ZADD', index, tonumber(ARGV[6]), ARGV[1])
return { 1, cjson.encode(claim) }