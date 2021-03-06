--[[
testClientInterface.lua

Copyright (c) 2018, Xamla and/or its affiliates. All rights reserved.

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License
as published by the Free Software Foundation; either version 2
of the License, or any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
--]]
local ffi = require 'ffi'
local torch = require 'torch'
local ros = require 'ros'
require 'URStream'
local ur = require 'ur_env'
local printf = ur.printf


local logger = {
  debug = ros.DEBUG,
  info = ros.INFO,
  warn = ros.WARN,
  error = ros.ERROR
}


local function readVersionMessage(reader)
  local project_name_length = reader:readUInt8()

  local name_offset = reader.offset
  reader:setOffset(name_offset + project_name_length)
  local project_name = ffi.string(reader.data + name_offset, project_name_length)

  local major = reader:readUInt8()
  local minor = reader:readUInt8()
  local revision = reader:readInt32()

  printf("Version decoded: '%s' %d.%d rev: %d", project_name, major, minor, revision)

end


local function decodePacket(reader)
  local len = reader:readUInt32()
  printf('length: %d', len)
  local packageType = reader:readUInt8()
  printf('package type: %d', packageType)
  if packageType == 20 then
    local time = reader:readUInt64()
    local source = reader:readUInt8()
    local robotMessageType = reader:readUInt8()
    if robotMessageType == 3 then
      readVersionMessage(reader)
    end

  end

end


local URStreamState = ur.URStreamState
local client = URStream(decodePacket, 4096, logger)


local function poll()
  if client:getState() == URStreamState.Disconnected then
    logger.info('Trying to connect...')
    if client:connect('192.168.50.102', 30002) then
      logger.info('Connected.')
    end
  end

  if client:getState() == URStreamState.Connected then
    client:read()
  end
end


local function main()
  ros.init()

  local poll_rate = ros.Rate(200)
  while ros.ok() do
    poll()
    ros.spinOnce()
    poll_rate:sleep()
  end

  if client:getState() ~= URStreamState.Disconnected then
    client:close()
  end

  ros.shutdown()
end

main()
