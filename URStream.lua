local torch = require 'torch'
local ros = require 'ros'
local socket = require 'socket'
local convertToBigEndianReader = require 'BigEndianAdapter'
local ur5 = require 'ur5_env'


local URStream = torch.class('URStream')


local STASH_SIZE = 4096
local HEADER_SIZE = 4
local CONNECT_TIMEOUT = 1.0
local RECONNECT_WAIT = 0.1
local READ_TIMEOUT = 0.004
local RECEIVE_BUFFER_SIZE = 4096


local URStreamState = {
  Disconnected = 1,
  Connected = 2,
  Error = 3,

  -- toString helper
  [1] = 'Disconnected',
  [2] = 'Connected',
  [3] = 'Error'
}
ur5.URStreamState = URStreamState


local ReadState = {
  HEADER = 1,
  PAYLOAD = 2,
  ERROR = 3
}


function URStream:__init(packet_handler_fn, max_package_size, logger)
  self.packet_handler_fn = packet_handler_fn
  self.max_package_size = max_package_size
  self.logger = logger or ur5.DEFAULT_LOGGER
  self.stash_offset = 0
  self.stash = torch.ByteTensor(STASH_SIZE)
  self.stash_reader = ros.StorageReader(self.stash:storage())
  convertToBigEndianReader(self.stash_reader)
  self:resetStash()
  self.client = socket.tcp()
  self.client:setoption('tcp-nodelay', true)
  self.state = URStreamState.Disconnected
  self.readTimeouts = 0
end


function URStream:connect(hostname, port)
  self.client:settimeout(CONNECT_TIMEOUT)
  local ok, err = self.client:connect(hostname, port)
  if not ok then
    self.logger.error('Connecting failed, error: ' .. err)
    self.client:close()
    sys.sleep(RECONNECT_WAIT)
    self.client = socket.tcp()
    return false
  end
  self.client:settimeout(READ_TIMEOUT, 't')
  self.state = URStreamState.Connected
  self.readTimeouts = 0
  return true
end


function URStream:getState()
  return self.state
end


function URStream:getSocket()
  return self.client
end


function URStream:close(abortive)
  if self.client ~= nil then
    if self.state == URStreamState.Connected then
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
      if len <= self.max_package_size then
        self.stream_state = ReadState.PAYLOAD
        self.remaining_bytes = len - 4
      else
        self.logger.error('[URStream] Received package has invalid size: %d', len)
        self.stream_state = ReadState.ERROR
        return false
      end
    elseif self.stream_state == ReadState.PAYLOAD then
      local payload_reader = ros.StorageReader(self.stash:storage(), 0, self.stash_offset)
      convertToBigEndianReader(payload_reader)
      self.packet_handler_fn(payload_reader)
      self:resetStash()
      updated = true
    else
      error('[URStream] Unexpected read state.')   -- should never happen
    end
  end
  return updated
end


function URStream:read()
  if self.state ~= URStreamState.Connected then
    return false
  end

  local r, e, p = self.client:receive(RECEIVE_BUFFER_SIZE)
  if e == 'timeout' then
    r = p
  end
  if not r then
    self.logger.error('[URStream] Receive failure: ' .. e)
    self.state = URStreamState.Error
    return false
  end

  local updated = false
  if #r > 0 then
    updated = processReceivedBlock(self, r)
    if self.stream_state == ReadState.ERROR then
      self.state = URStreamState.Error
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


function URStream:resetStash()
  self.stream_state = ReadState.HEADER
  self.remaining_bytes = HEADER_SIZE
  self.stash:fill(0)
  self.stash_offset = 0
  self.stash_reader:setOffset(0)
end


function URStream:send(msg)
  return self.client:send(msg)
end
