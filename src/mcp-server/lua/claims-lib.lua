-- Lua FUNCTIONS (not a standalone script) shared by the claim scripts:
-- claims-batch-claim.lua, claims-renew.lua, claims-recover.lua and
-- claims-requeue.lua. Callers compose this file in front of their own body
-- (see src/core/task-claim-store.ts), like claims-purge.lua.
--
-- wake_plan(payload) decides, without writing anything, whether re-queuing a
-- raw task payload must emit the same header-only inbox event a fresh send
-- does. The rule mirrors bounded-push.lua / bounded-push-idempotent.lua and
-- `eligible` in src/core/mailbox-store.ts: a `task`, or a `result`/`error`
-- reply whose outstanding-request owner (gptq:outstanding:<to>:<in_reply_to>,
-- SESSION_KEYS.outstanding) still names the reply's sender. It returns nil
-- when no event is due, false when the outstanding key holds the wrong type
-- (the caller must refuse before its first write), or the XADD field list.
--
-- wake_emit(events, fields) appends that event with the same MAXLEN as a send.
local function wake_text(value)
  if type(value) == 'string' then return value end
  return ''
end

local function wake_plan(payload)
  local ok, msg = pcall(cjson.decode, payload)
  if not ok or type(msg) ~= 'table' or type(msg.id) ~= 'string' or #msg.id == 0 then
    return nil
  end
  local inReplyTo = ''
  if type(msg.payload) == 'table' then inReplyTo = wake_text(msg.payload.in_reply_to) end
  local due = msg.type == 'task'
  if not due and (msg.type == 'result' or msg.type == 'error') and #inReplyTo > 0
     and type(msg.to) == 'string' and type(msg.from) == 'string' then
    local owner = 'gptq:outstanding:' .. msg.to .. ':' .. inReplyTo
    local kind = redis.call('TYPE', owner).ok
    if kind ~= 'none' and kind ~= 'string' then return false end
    due = redis.call('GET', owner) == msg.from
  end
  if not due then return nil end
  return { 'message_id', msg.id, 'type', msg.type, 'timestamp', wake_text(msg.timestamp),
           'from', wake_text(msg.from), 'to', wake_text(msg.to), 'in_reply_to', inReplyTo }
end

local function wake_emit(events, fields)
  redis.call('XADD', events, 'MAXLEN', '~', 1024, '*', unpack(fields))
end

-- Redis server time in epoch ms. Claim expiry is decided on this one clock so
-- a client process whose wall clock runs fast cannot expire a live claim.
local function server_ms()
  local t = redis.call('TIME')
  return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

-- Howard Hinnant civil-from-days / days-from-civil; pure arithmetic so Lua's
-- lack of a reliable os.time never leaks host clock/timezone into the store.
local function days_from_civil(y, m, d)
  local yy = y
  if m <= 2 then yy = yy - 1 end
  local era = math.floor(yy / 400)
  local yoe = yy - era * 400
  local mp = m + (m > 2 and -3 or 9)
  local doy = math.floor((153 * mp + 2) / 5) + d - 1
  local doe = yoe * 365 + math.floor(yoe / 4) - math.floor(yoe / 100) + doy
  return era * 146097 + doe - 719468
end

local function ms_from_iso(s)
  local y, mo, d, h, mi, se, ms =
    string.match(s, '^(%d+)-(%d+)-(%d+)T(%d+):(%d+):(%d+)%.(%d+)Z$')
  if not y then return nil end
  local days = days_from_civil(tonumber(y), tonumber(mo), tonumber(d))
  local secs = days * 86400 + tonumber(h) * 3600 + tonumber(mi) * 60 + tonumber(se)
  local msv = tonumber(ms)
  local mslen = #ms
  while mslen < 3 do msv = msv * 10; mslen = mslen + 1 end
  return secs * 1000 + msv
end

local function iso_from_ms(ms)
  local totalSecs = math.floor(ms / 1000)
  local msec = ms - totalSecs * 1000
  local days = math.floor(totalSecs / 86400)
  local rem = totalSecs % 86400
  local hour = math.floor(rem / 3600)
  local minute = math.floor((rem % 3600) / 60)
  local sec = rem % 60
  local z = days + 719468
  local era = math.floor(z / 146097)
  local doe = z - era * 146097
  local yoe = math.floor((doe - math.floor(doe / 1460) + math.floor(doe / 36524)
    - math.floor(doe / 146096)) / 365)
  local y = yoe + era * 400
  local doy = doe - (365 * yoe + math.floor(yoe / 4) - math.floor(yoe / 100))
  local mp = math.floor((5 * doy + 2) / 153)
  local d = doy - math.floor((153 * mp + 2) / 5) + 1
  local m = mp + (mp < 10 and 3 or -9)
  local yfinal = y
  if m <= 2 then yfinal = y + 1 end
  return string.format('%04d-%02d-%02dT%02d:%02d:%02d.%03dZ',
    yfinal, m, d, hour, minute, sec, msec)
end

