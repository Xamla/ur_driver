--[[
ur_env.lua

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
local ros = require 'ros'
local convertToBigEndianReader = require 'BigEndianAdapter'


local ur = {}


ur.JOINT_NAMES = {
  "shoulder_pan_joint",
  "shoulder_lift_joint",
  "elbow_joint",
  "wrist_1_joint",
  "wrist_2_joint",
  "wrist_3_joint"
}


ur.ROBOT_MODE = {
  DISCONNECTED = 0,
  CONFIRM_SAFETY = 1,
  BOOTING = 2,
  POWER_OFF = 3,
  POWER_ON = 4,
  IDLE = 5,
  BACKDRIVE = 6,
  RUNNING = 7,
  UPDATING_FIRMWARE = 8
}


ur.CONTROL_MODE = {
  POSITION = 0,
  TEACH = 1,
  FORCE = 2,
  TORQUE = 3
}


ur.JOINT_MODE = {
  SHUTTING_DOWN = 236,
  PART_D_CALIBRATION = 237,
  BACKDRIVE = 238,
  POWER_OFF = 239,
  NOT_RESPONDING = 245,
  MOTOR_INITIALISATION = 246,
  BOOTING = 247,
  PART_D_CALIBRATION_ERROR = 248,
  BOOTLOADER = 249,
  CALIBRATION = 250,
  FAULT = 252,
  RUNNING = 253,
  IDLE = 255
}


ur.SAFETY_MODE = {
  NORMAL = 1,
  REDUCED = 2,
  PROTECTIVE_STOP = 3,
  RECOVERY = 4,
  SAFEGUARD_STOP = 5,           -- (SI0 + SI1 + SBUS) Physical s-stop interface input
  SYSTEM_EMERGENCY_STOP = 6,    -- (EA + EB + SBUS->Euromap67) Physical e-stop interface input activated
  ROBOT_EMERGENCY_STOP = 7,     -- (EA + EB + SBUS->Screen) Physical e-stop interface input activated
  VIOLATION = 8,
  FAULT = 9
}


local function createToString(obj)
  local reverseMap = {}
  for k,v in pairs(obj) do
    if type(v) == 'number' then
      reverseMap[v] = k
    end
  end
  return function(x)
    local k = reverseMap[x]
    if k == nil then
      return string.format("INVALID_VALUE (%d)", x)
    end
    return k
  end
end


ur.ROBOT_MODE.tostring = createToString(ur.ROBOT_MODE)
ur.CONTROL_MODE.tostring = createToString(ur.CONTROL_MODE)
ur.JOINT_MODE.tostring = createToString(ur.JOINT_MODE)
ur.SAFETY_MODE.tostring = createToString(ur.SAFETY_MODE)


function ur.printf(...)
  return print(string.format(...))
end


function ur.serializeInt32Array(a, writer)
  local w = writer or convertToBigEndianReader(ros.StorageWriter())
  for i,x in ipairs(a) do
    w:writeInt32(x)
  end
  w:shrinkToFit()
  return w.storage:string()
end


ur.DEFAULT_LOGGER = {
  debug = function(...)
    print('[DEBUG] ' .. string.format(...))
  end,
  info = function(...)
    print('[INFO] ' .. string.format(...))
  end,
  warn = function(...)
    print('[WARNING] ' .. string.format(...))
  end,
  error = function(...)
    print('[ERROR] ' .. string.format(...))
  end
}


function ur.findIndex(t, condition)
  for i,v in ipairs(t) do
    if condition(v, i) then
      return i
    end
  end
  return -1
end


function ur.copyMapped(dst, src, map)
  for k,v in pairs(map) do
    dst[k] = src[v]
  end
end


function ur.indexOf(t, v)
  for i=1,#t do
    if t[i] == v then
      return i
    end
  end
  return -1
end



return ur
