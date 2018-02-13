local torch = require 'torch'
local ros = require 'ros'
local socket = require 'socket'
local convertToBigEndianReader = require 'BigEndianAdapter'
local ur = require 'ur_env'


local RealtimeStream = torch.class('RealtimeStream')


local DEFAULT_REALTIME_PORT = 30003
local STASH_SIZE = 2048
local MAX_PACKAGE_SIZE = 1060
local HEADER_SIZE = 4
local CONNECT_TIMEOUT = 1.0
local RECONNECT_WAIT = 0.1
local READ_TIMEOUT = 0.004
local RECEIVE_BUFFER_SIZE = 4096


local RealtimeStreamState = {
  Disconnected = 1,
  Connected = 2,
  Error = 3
}
ur.RealtimeStreamState = RealtimeStreamState


local ReadState = {
  HEADER = 1,
  PAYLOAD = 2,
  ERROR = 3
}


--- realtimeState is target that receives state updates
function RealtimeStream:__init(realtimeState, logger)
  self.realtimeState = realtimeState
  self.logger = logger or ur.DEFAULT_LOGGER
  self.stash_offset = 0
  self.stash = torch.ByteTensor(STASH_SIZE)
  self.stash_reader = ros.StorageReader(self.stash:storage())
  convertToBigEndianReader(self.stash_reader)
  self:resetStash()
  self.client = socket.tcp()
  self.client:setoption('tcp-nodelay', true)
  self.state = RealtimeStreamState.Disconnected
  self.readTimeouts = 0
end


function RealtimeStream:connect(hostname, port)
  self.client:settimeout(CONNECT_TIMEOUT)
  local ok, err = self.client:connect(hostname, port or DEFAULT_REALTIME_PORT)
  if not ok then
    self.logger.error('Connecting failed, error: ' .. err)
    self.client:close()
    sys.sleep(RECONNECT_WAIT)
    self.client = socket.tcp()
    return false
  end
  self.client:settimeout(READ_TIMEOUT, 't')
  self.state = RealtimeStreamState.Connected
  self.readTimeouts = 0
  return true
end


function RealtimeStream:getState()
  return self.state
end


function RealtimeStream:getSocket()
  return self.client
end


function RealtimeStream:close(abortive)
  if self.client ~= nil then
    if self.state == RealtimeStreamState.Connected then
      self.client:shutdown('send')
      if abortive then
        self.client:setoption('linger', { on = true, timeout = 0 })
      end
    end
    self.client:close()
    self.client = nil
  end
end


local function processReceivedBlock(self, block)
  local updated = false
  local r = self.stash_reader

  local buf = torch.ByteTensor(torch.ByteStorage():string(block), 1, #block)
  local buf_len = buf:size(1)
  local offset, len = 0, 0
  while offset < buf_len do
    local avail = buf_len - offset
    local copy_len = math.min(avail, self.remaining_bytes)
    self.stash[{{self.stash_offset+1, self.stash_offset + copy_len}}] = buf[{{offset+1, offset+copy_len}}]
    self.stash_offset = self.stash_offset + copy_len
    offset = offset + copy_len
    self.remaining_bytes = self.remaining_bytes - copy_len
    if self.remaining_bytes > 0 then
      return
    end

    if self.stream_state == ReadState.HEADER then
      local len = r:readUInt32()
      if len <= MAX_PACKAGE_SIZE then
        self.stream_state = ReadState.PAYLOAD
        self.remaining_bytes = len - 4
      else
        self.logger.error('[RealtimeStream] Received realtime package has invalid size.')
        self.stream_state = ReadState.ERROR
        return false
      end
    elseif self.stream_state == ReadState.PAYLOAD then
      local payload_reader = ros.StorageReader(self.stash:storage())
      convertToBigEndianReader(payload_reader)
      self.realtimeState:read(payload_reader)
      self:resetStash()
      updated = true
    else
      error('[RealtimeStream] Unexpected read state.')   -- should never happen
    end
  end
  return updated
end


function RealtimeStream:read()
  if self.state ~= RealtimeStreamState.Connected then
    return false
  end

  local r, e, p = self.client:receive(RECEIVE_BUFFER_SIZE)
  if e == 'timeout' then
    r = p
  end
  if not r then
    self.logger.error('[RealtimeStream] Receive failure: ' .. e)
    self.state = RealtimeStreamState.Error
    return false
  end

  local updated = false
  if #r > 0 then
    updated = processReceivedBlock(self, r)
    if self.stream_state == ReadState.ERROR then
      self.state = RealtimeStreamState.Error
      return false
    end
  end
  if updated then
    self.readTimeouts = 0
  else
    self.readTimeouts = self.readTimeouts + 1
  end
  return updated
end


function RealtimeStream:resetStash()
  self.stream_state = ReadState.HEADER
  self.remaining_bytes = HEADER_SIZE
  self.stash:fill(0)
  self.stash_offset = 0
  self.stash_reader:setOffset(0)
end


function RealtimeStream:send(msg)
  return self.client:send(msg)
end
