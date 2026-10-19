-- Waking admission (composed in front of bounded-push.lua and
-- bounded-push-idempotent.lua by src/core/mailbox-store.ts, and therefore of
-- the watchdog send script, which is composed from the idempotent one).
--
-- max_size is an ordinary-send quota for each class of queued mail:
--   * waking mail (a task, or a result/error that answers the recipient's own
--     outstanding request) is admitted iff fewer than max_size WAKING entries
--     are queued, whatever the total length, so quiet mail never refuses it;
--   * quiet mail (everything else, including entries that decode but are
--     malformed) is admitted iff LLEN < max_size, exactly the rule before
--     waking admission.
-- While classification is stable, ordinary sends bound a queue at about
-- 2 x max_size; a reply turning quiet (its outstanding key expiring or
-- changing) frees a waking slot, so ownership changes alone can grow a queue
-- further, until the ceiling. ADM_CEILING is a pre-send threshold: a send
-- that finds exactly ADM_CEILING entries is still evaluated, so a queue can
-- reach ADM_CEILING + 1. Lossless
-- writers (claim recovery/requeue, DLQ requeue, provisional forwarding,
-- rename migration, sweeper replies) do not run this check and may overshoot;
-- the classification counts the whole actual queue, never a prefix. Above
-- ADM_CEILING entries every ordinary send is refused before any scan.
--
-- adm_entry_wakes is the Lua twin of wakesRecipient/replyOwnerKey/
-- parseQueueEntry in src/core/inbox-events.ts (tests/waking-admission.test.ts
-- checks they agree): a reply wakes only when `from` and `payload.in_reply_to`
-- are non-empty strings and the outstanding owner equals `from`. The twins
-- agree on every entry both decoders accept. An entry cjson cannot decode
-- counts as WAKING: JSON.parse accepts some text cjson rejects (an escaped
-- unpaired surrogate, nesting past cjson's depth limit), and consumers may
-- wake for such an entry, so admission must never count it quiet. No in-repo
-- writer produces one (every ordinary push cjson-decodes its envelope); the
-- cost is that undecodable junk written by hand occupies waking capacity.
-- Refusal reasons: 'waking_full' | 'full' | 'oversize'.
local ADM_CEILING = 1024

-- JavaScript truthiness of a decoded JSON value (cjson.null is JSON null).
local function adm_truthy(value)
  return value ~= nil and value ~= false and value ~= cjson.null and value ~= 0 and value ~= ''
end

-- The value of an outstanding-request key, or false when absent or not a string.
local function adm_owner(key)
  if redis.call('TYPE', key).ok ~= 'string' then return false end
  return redis.call('GET', key)
end

-- Whether a raw queue entry currently wakes `mailbox`. Undecodable counts as
-- waking (never undercount consumers); decodable but malformed is quiet.
local function adm_entry_wakes(raw, mailbox, prefix)
  local ok, entry = pcall(cjson.decode, raw)
  if not ok then return true end
  if type(entry) ~= 'table' then return false end
  if entry.to ~= mailbox or type(entry.id) ~= 'string' or not adm_truthy(entry.payload) then return false end
  if entry.type == 'task' then return true end
  if entry.type ~= 'result' and entry.type ~= 'error' then return false end
  if type(entry.payload) ~= 'table' or type(entry.from) ~= 'string' or entry.from == '' then return false end
  local in_reply_to = entry.payload.in_reply_to
  if type(in_reply_to) ~= 'string' or in_reply_to == '' then return false end
  return adm_owner(prefix .. mailbox .. ':' .. in_reply_to) == entry.from
end

-- {waking, quiet} counts over the WHOLE queue.
local function adm_counts(queue, mailbox, prefix)
  local entries = redis.call('LRANGE', queue, 0, -1)
  local waking = 0
  for _, raw in ipairs(entries) do
    if adm_entry_wakes(raw, mailbox, prefix) then waking = waking + 1 end
  end
  return waking, #entries - waking
end

-- The refusal reason for an ordinary send, or nil when it is admitted. Reads only.
local function adm_refusal(queue, length, max_size, incoming_waking, mailbox, prefix)
  if length > ADM_CEILING then return 'oversize' end
  if not incoming_waking then
    if length >= max_size then return 'full' end
    return nil
  end
  -- waking_count <= length, so a shorter queue cannot hold max_size waking entries.
  if length < max_size then return nil end
  local waking = adm_counts(queue, mailbox, prefix)
  if waking >= max_size then return 'waking_full' end
  return nil
end
