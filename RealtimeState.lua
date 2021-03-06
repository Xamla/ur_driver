--[[
RealtimeState.lua

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
local torch = require 'torch'
local ur = require 'ur_env'


local RealtimeState = torch.class('RealtimeState')


function RealtimeState:__init()
  self.time = 0                                           -- Time elapsed since the controller was started
  self.q_target = torch.DoubleTensor(6)                   -- Target joint positions
  self.qd_target = torch.DoubleTensor(6)                  -- Target joint velocities
  self.qdd_target = torch.DoubleTensor(6)                 -- Target joint accelerations
  self.i_target = torch.DoubleTensor(6)                   -- Target joint currents
  self.m_target = torch.DoubleTensor(6)                   -- Target joint moments (torques)
  self.q_actual = torch.DoubleTensor(6)                   -- Actual joint positions
  self.qd_actual = torch.DoubleTensor(6)                  -- Actual joint velocities
  self.i_actual = torch.DoubleTensor(6)                   -- Actual joint currents
  self.i_control = torch.DoubleTensor(6)                  -- Joint control currents
  self.tool_vector_actual = torch.DoubleTensor(6)         -- Actual Cartesian coordinates of the tool: (x,y,z,rx,ry,rz), where rx, ry and rz is a rotation vector representation of the tool orientation
  self.tcp_speed_actual = torch.DoubleTensor(6)           -- Actual speed of the tool given in Cartesian coordinates
  self.tcp_force = torch.DoubleTensor(6)                  -- Generalised forces in the TCP
  self.tool_vector_target = torch.DoubleTensor(6)         -- Target Cartesian coordinates of the tool: (x,y,z,rx,ry,rz), where rx, ry and rz is a rotation vector representation of the tool orientation
  self.tcp_speed_target = torch.DoubleTensor(6)           -- Target speed of the tool given in Cartesian coordinates
  self.digital_input_bits = 0                             -- Current state of the digital inputs. NOTE: these are bits encoded as int64_t, e.g. a value of 5 corresponds to bit 0 and bit 2 set high
  self.motor_temps = torch.DoubleTensor(6)                -- Temperature of each joint in degrees celsius
  self.ctrl_timer = 0                                     -- Controller realtime thread execution time
  self.test_value = 0                                     -- A value used by Universal Robots software only
  self.robot_mode = 0                                     -- Robot mode (see DataStreamFromURController)
  self.joint_control_modes = torch.DoubleTensor(6)        -- Joint control modes see (DataStreamFromURController) (only from software version 1.8 and on)
  self.safety_mode = 0                                    -- Safety mode (see DataStreamFromURController)
  self.ur_internal_data0 = torch.DoubleTensor(6)          -- Used by Universal Robots software only
  self.accelerometer = torch.DoubleTensor(3)              -- Tool x,y and z accelerometer values (software version 1.7)
  self.ur_internal_data1 = torch.DoubleTensor(6)          -- Used by Universal Robots software only
  self.traj_speed_scaling = 0                             -- Speed scaling of the trajectory limiter
  self.linear_momentum_norm = 0                           -- Norm of Cartesian linear momentum
  self.ur_internal_data2 = 0                              -- Used by Universal Robots software only
  self.ur_internal_data3 = 0                              -- Used by Universal Robots software only
  self.masterboard_main_voltage = 0                       -- Masterboard: Main voltage
  self.masterboard_robot_voltage = 0                      -- Masterboard: Robot voltage (48V)
  self.masterboard_robot_current = 0                      -- Masterboard: Robot current
  self.actual_joint_voltage = torch.DoubleTensor(6)       -- Actual joint voltages
  self.digital_output_bits = 0                            -- Digital outputs
  self.program_state = 0                                  -- Program state
  self.unused = torch.DoubleTensor(15)
  self.major = 0
  self.minor = 0
  self:invalidate()
end


local function unpackVector(v, reader, count)
  for i=1,count do
    v[i] = reader:readFloat64()
  end
  return v
end


function RealtimeState:isValid()
  return self.valid
end


function RealtimeState:isRobotReady()
  if self.major < 3 then
    return self.valid
  end

  return self.valid and self.robot_mode == ur.ROBOT_MODE.RUNNING and self.safety_mode == ur.SAFETY_MODE.NORMAL
end


function RealtimeState:invalidate()
  self.valid = false
end


local function decodeCB2(self, reader, major, minor)
  self.safety_mode = ur.SAFETY_MODE.NORMAL
  local len = reader:readUInt32()
  if len < 764 then
    error(string.format("Wrong length of message on RT interface: %i", len))
  end

  local t = reader:readUInt64()
  self.time = t
  unpackVector(self.q_target, reader, 6)
  unpackVector(self.qd_target, reader, 6)
  unpackVector(self.qdd_target, reader, 6)
  unpackVector(self.i_target, reader, 6)
  unpackVector(self.m_target, reader, 6)
  unpackVector(self.q_actual, reader, 6)
  unpackVector(self.qd_actual, reader, 6)
  unpackVector(self.i_actual, reader, 6)
  unpackVector(self.accelerometer, reader, 3)
  unpackVector(self.unused, reader, 15)
  unpackVector(self.tcp_force, reader, 6)
  unpackVector(self.tool_vector_target, reader, 6)
  unpackVector(self.tcp_speed_target, reader, 6)
  self.digital_input_bits = reader:readUInt64()
  unpackVector(self.motor_temps, reader, 6)
  self.ctrl_timer = reader:readFloat64()
  self.test_value = reader:readFloat64()
  self.robot_mode = reader:readFloat64()
  unpackVector(self.joint_control_modes, reader, 6)
  self.valid = true
end


local function decodeCB3(self, reader, major, minor)
  local len = reader:readUInt32()

  if major == 3 and minor < 2 then

    if len < 1044 then
      error(string.format("Wrong length of message on RT interface: %i", len))
    end

  elseif len < 1060 then
    error(string.format("Wrong length of message on RT interface: %i", len))
  end

  local t = reader:readUInt64()
  self.time = t
  unpackVector(self.q_target, reader, 6)
  unpackVector(self.qd_target, reader, 6)
  unpackVector(self.qdd_target, reader, 6)
  unpackVector(self.i_target, reader, 6)
  unpackVector(self.m_target, reader, 6)
  unpackVector(self.q_actual, reader, 6)
  unpackVector(self.qd_actual, reader, 6)
  unpackVector(self.i_actual, reader, 6)
  unpackVector(self.i_control, reader, 6)
  unpackVector(self.tool_vector_actual, reader, 6)
  unpackVector(self.tcp_speed_actual, reader, 6)
  unpackVector(self.tcp_force, reader, 6)
  unpackVector(self.tool_vector_target, reader, 6)
  unpackVector(self.tcp_speed_target, reader, 6)
  self.digital_input_bits = reader:readUInt64()
  unpackVector(self.motor_temps, reader, 6)
  self.ctrl_timer = reader:readFloat64()
  self.test_value = reader:readFloat64()
  self.robot_mode = reader:readFloat64()
  unpackVector(self.joint_control_modes, reader, 6)
  self.safety_mode = reader:readFloat64()
  unpackVector(self.ur_internal_data0, reader, 6)
  unpackVector(self.accelerometer, reader, 3)
  unpackVector(self.ur_internal_data1, reader, 6)
  self.traj_speed_scaling = reader:readFloat64()
  self.linear_momentum_norm = reader:readFloat64()
  self.ur_internal_data2 = reader:readFloat64()
  self.ur_internal_data3 = reader:readFloat64()
  self.masterboard_main_voltage = reader:readFloat64()
  self.masterboard_robot_voltage = reader:readFloat64()
  self.masterboard_robot_current = reader:readFloat64()
  unpackVector(self.actual_joint_voltage, reader, 6)

  -- since 3.2
  if major == 3 and minor >= 2 then
    self.digital_output_bits = reader:readUInt64()
    self.program_state = reader:readFloat64()
  end

  self.valid = true
end


function RealtimeState:read(reader, robot_version)
  if robot_version == nil then
    return  -- wait for robot version to be received first
  end

  local major, minor = robot_version.major, robot_version.minor
  self.major = major
  self.minor = minor

  if major >= 3 then
    decodeCB3(self, reader, major, minor)
  else
    -- pre 3.0
    decodeCB2(self, reader, major, minor)
  end

end
