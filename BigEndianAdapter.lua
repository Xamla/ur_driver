--[[
BigEndianAdapter.lua

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


local function convertToBigEndianReader(reader)

  local function createBigEndianReadFn(buffer_type, read_fn, target_type)
    local buf = ffi.typeof(buffer_type .. '[1]')()
    local target_ptr_ctype = ffi.typeof(target_type .. '*')
    local target_type_size = ffi.sizeof(target_type)
    return function(self,offset)
      local x = read_fn(self, offset)
      local v = bit.bswap(x)
      if target_type_size < 4 then
        v = bit.rshift(v, (4-target_type_size) * 8)
      end
      buf[0] = v
      return ffi.cast(target_ptr_ctype, buf)[0]
    end
  end

  reader.readFloat32 = createBigEndianReadFn('uint32_t', reader.readUInt32, 'float')
  reader.readFloat64 = createBigEndianReadFn('uint64_t', reader.readUInt64, 'double')
  reader.readInt16 = createBigEndianReadFn('uint16_t', reader.readUInt16, 'int16_t')
  reader.readInt32 = createBigEndianReadFn('uint32_t', reader.readUInt32, 'int32_t')
  reader.readInt64 = createBigEndianReadFn('uint64_t', reader.readUInt64, 'int64_t')
  reader.readUInt16 = createBigEndianReadFn('uint16_t', reader.readUInt16, 'uint16_t')
  reader.readUInt32 = createBigEndianReadFn('uint32_t', reader.readUInt32, 'uint32_t')
  reader.readUInt64 = createBigEndianReadFn('uint64_t', reader.readUInt64, 'uint64_t')

  function reader:readChar()
    return string.char(self:readUInt8())
  end

  return reader
end


local function convertToBigEndianWriter(writer)

  local function createBigEndianWriteFn(buffer_type, write_fn, target_type)
    local buf = ffi.typeof(buffer_type .. '[1]')()
    local target_ptr_ctype = ffi.typeof(target_type .. '*')
    local target_type_size = ffi.sizeof(target_type)
    return function(self, value)
      buf[0] = value
      local v = bit.bswap(ffi.cast(target_ptr_ctype, buf)[0])
      if target_type_size < 4 then
        v = bit.rshift(v, (4-target_type_size) * 8)
      end
      write_fn(self, v)
    end
  end

  writer.writeFloat32 = createBigEndianWriteFn('float', writer.writeInt32, 'uint32_t')
  writer.writeFloat64 = createBigEndianWriteFn('double', writer.writeUInt64, 'uint64_t')
  writer.writeInt16 = createBigEndianWriteFn('int16_t', writer.writeUInt16, 'uint16_t')
  writer.writeInt32 = createBigEndianWriteFn('int32_t', writer.writeUInt32, 'uint32_t')
  writer.writeInt64 = createBigEndianWriteFn('int64_t', writer.writeUInt64, 'uint64_t')
  writer.writeUInt16 = createBigEndianWriteFn('uint16_t', writer.writeUInt16, 'uint16_t')
  writer.writeUInt32 = createBigEndianWriteFn('uint32_t', writer.writeUInt32, 'uint32_t')
  writer.writeUInt64 = createBigEndianWriteFn('uint64_t', writer.writeUInt64, 'uint64_t')

  function writer:writChar(c)
    return set:writeUInt8(string.byte(c))
  end

  return writer
end


function bigEndianAdapterFactory(obj)
  if ffi.abi('be') == true then
    return obj
  end

  if torch.isTypeOf(obj, ros.StorageWriter) then
    return convertToBigEndianWriter(obj)
  elseif torch.isTypeOf(obj, ros.StorageReader) then
    return convertToBigEndianReader(obj)
  else
    error('Cannot convert object type:' .. torch.type(obj))
  end
end


return bigEndianAdapterFactory
