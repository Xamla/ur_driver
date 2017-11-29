local torch = require 'torch'
local ffi = require 'ffi'
local ros = require 'ros'
local ur5 = require 'ur5_env'
local bigEndianAdapter = require 'BigEndianAdapter'


local MAX_READ_AVAIL_RETRIES = 10
local TIMEOUT = 0.5


local ReverseConnection = torch.class('ReverseConnection')


function ReverseConnection:__init(socket, multJoint, logger)
  self.socket = socket
  self.multJoint = multJoint
  self.logger = logger or ur5.DEFAULT_LOGGER
  socket:setoption('tcp-nodelay', true)
  socket:settimeout(TIMEOUT, 't')
  self.reader = bigEndianAdapter(ros.StorageReader(torch.ByteStorage(4)))
  self.error = false
  self.idleCycles = 0
end


function ReverseConnection:getIdleCycles()
  return self.idleCycles
end


-- read available buffer size on robot
function ReverseConnection:readAvailable()
  local response = self.socket:receive(4)
  if response == nil then
    return nil
  end

  local reader = self.reader
  reader.storage:string(response)
  return reader:readInt32(0)
end


function ReverseConnection:idle(op)
  op = op or 0

  self.idleCycles = self.idleCycles + 1
  for i=1,MAX_READ_AVAIL_RETRIES do
    local avail, err = self:readAvailable()
    if avail ~= nil then
      self.socket:send(ur5.serializeInt32Array({op}))
      return avail
    elseif err ~= 'timeout' then
      self.error = true
      return nil
    end
  end

  self.error = true
  return nil
end


function ReverseConnection:cancel()
  self:idle(-2)
end


function ReverseConnection:close()
  if not self.error then
    self.socket:send(ur5.serializeInt32Array({-1}))   -- send stop message
  end
  self.socket:shutdown('send')
  self.socket:close()
end


local function jointChecksum(q)
  local s = 0   -- sum
  local m = 1   -- left shift multiplier (one bit per joint byte)
  for i,x in ipairs(q) do
    if x < 0 then
      x = -x + 1  -- reflect sign in lsb
    end
    local d = 1   -- divisor for right shift
    for j=1,4 do
      s = s + (math.floor(x / d) % 255) * m    -- shift right, mask out byte and add to sum (left shifted)
      m = m * 2   -- shift one more to left
      d = d * 256
    end
  end
  return s
end


function ReverseConnection:sendPoints(pts)
  self.idleCycles = 0
  local ok = self.socket:send(ur5.serializeInt32Array({#pts}))
  if not ok then
    self.logger.error('[ReverseConnection] Send error')
    self.error = true
    return
  end

  for j=1,#pts do
    local q = pts[j]

    -- convert to integers
    local q_ = {}
    for i=1,6 do
      q_[i] = math.floor(q[i] * self.multJoint)
    end
    q_[7] = jointChecksum(q_)   -- add checksum

    local ok = self.socket:send(ur5.serializeInt32Array(q_))
    if not ok then
      self.logger.error('[ReverseConnection] Send error')
      self.error = true
      return
    end
  end
end