-- Composed before ordinary bounded push. All checks, selection, content and
-- envelope serialization precede the first enqueue write. No retry key: the
-- pending-to-enqueued transition itself is atomic, including on lost replies.
-- Extra KEYS: 9 pending set, 10 overflow, 11 window, 12 registry,
-- 13 reserved sender's sessions, 14 collision diagnostic counter.
local kinds = { [1] = 'list', [2] = 'hash', [4] = 'stream', [5] = 'string',
  [6] = 'string', [7] = 'string', [8] = 'string', [9] = 'set',
  [10] = 'string', [11] = 'string', [12] = 'hash', [13] = 'set', [14] = 'string' }
for i, kind in pairs(kinds) do
  if not dn_holds(KEYS[i], kind) then return {0, ARGV[3], 'bad_key_' .. i} end
end
local overflow = dn_count(KEYS[10])
local window = dn_count(KEYS[11])
local diagnostic = dn_count(KEYS[14])
local bound = dn_number(ARGV[2])
if bound == false or bound < 1 then return {0, ARGV[3], 'bad_numeric_state'} end
local storedBound = redis.call('HGET', KEYS[2], 'max_size')
if storedBound then bound = dn_number(storedBound) end
if overflow == false or window == false or diagnostic == false or bound == false or bound < 1 then
  return {0, ARGV[3], 'bad_numeric_state'}
end
ARGV[2] = tostring(bound)
local ok, envelope = pcall(cjson.decode, ARGV[1])
if not ok or type(envelope) ~= 'table' or envelope.id ~= ARGV[3] or
  envelope.from ~= ARGV[13] or envelope.to ~= ARGV[8] or envelope.type ~= 'task' or
  type(envelope.timestamp) ~= 'string' or type(envelope.payload) ~= 'table' or
  type(envelope.payload.content) ~= 'string' or type(envelope.payload.metadata) ~= 'table' or
  (envelope.payload.in_reply_to ~= nil and type(envelope.payload.in_reply_to) ~= 'string') or
  envelope.payload.metadata.kind ~= 'dlq_notice' or ARGV[6] ~= 'task' or ARGV[7] ~= '1' then
  return {0, ARGV[3], 'bad_envelope'}
end
local count = redis.call('SCARD', KEYS[9])
if count > DN_MAX_COUNT - overflow then return {0, ARGV[3], 'bad_numeric_state'} end
if redis.call('EXISTS', KEYS[11]) == 1 then return {0, ARGV[3], 'window'} end
if count == 0 and overflow == 0 then return {0, ARGV[3], 'empty'} end
-- SEC-F4: never speak as an identity an older server let an agent own.
if redis.call('HEXISTS', KEYS[12], ARGV[13]) == 1 or redis.call('SCARD', KEYS[13]) > 0 then
  if diagnostic >= DN_MAX_COUNT then return {0, ARGV[3], 'bad_numeric_state'} end
  redis.call('INCR', KEYS[14])
  return {0, ARGV[3], 'reserved_sender_collision'}
end
-- A bounded set, deterministically ordered. Keep exact ids; never truncate an
-- id or import its text into trusted instructions. Oversized ids remain
-- pending (observable refusal) rather than silently disappearing.
local candidates = redis.call('SMEMBERS', KEYS[9])
table.sort(candidates)
local sent_ids = {}
local function content(ids)
  -- cjson encodes an empty Lua table as {}, so serialize this array explicitly.
  local encoded = {}
  for _, id in ipairs(ids) do encoded[#encoded + 1] = cjson.encode(id) end
  return envelope.payload.content .. '\nUntrusted data (JSON): {"dead_lettered_ids":[' ..
    table.concat(encoded, ',') .. '],"more":' .. string.format('%.0f', count - #ids + overflow) .. '}'
end
for i = 1, math.min(16, #candidates) do
  sent_ids[#sent_ids + 1] = candidates[i]
  if #content(sent_ids) > 2048 then
    sent_ids[#sent_ids] = nil
    break
  end
end
if count > 0 and #sent_ids == 0 then return {0, ARGV[3], 'content_too_large'} end
envelope.payload.content = content(sent_ids)
if #envelope.payload.content > 2048 then return {0, ARGV[3], 'content_too_large'} end
local encoded, serialized = pcall(cjson.encode, envelope)
if not encoded then return {0, ARGV[3], 'bad_envelope'} end
ARGV[1] = serialized
