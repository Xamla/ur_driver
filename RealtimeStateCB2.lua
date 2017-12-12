local torch = require 'torch'
local ur5 = require 'ur5_env'


local RealtimeStateCB2 = torch.class('RealtimeStateCB2')


function RealtimeStateCB2:__init()
  self.time = 0                                           -- Time elapsed since the controller was started
  self.q_target = torch.DoubleTensor(6)                   -- Target joint positions
  self.qd_target = torch.DoubleTensor(6)                  -- Target joint velocities
  self.qdd_target = torch.DoubleTensor(6)                 -- Target joint accelerations
  self.i_target = torch.DoubleTensor(6)                   -- Target joint currents
  self.m_target = torch.DoubleTensor(6)                   -- Target joint moments (torques)
  self.q_actual = torch.DoubleTensor(6)                   -- Actual joint positions
  self.qd_actual = torch.DoubleTensor(6)                  -- Actual joint velocities
  self.i_actual = torch.DoubleTensor(6)                   -- Actual joint currents
  self.tool_acc = torch.DoubleTensor(3)                   -- Tool x,y and z accelerometer values (software version 1.7)

  self.unused = torch.DoubleTensor(15)

  self.tcp_force = torch.DoubleTensor(6)                  -- Generalised forces in the TCP
  self.tool_vector_target = torch.DoubleTensor(6)         -- Target Cartesian coordinates of the tool: (x,y,z,rx,ry,rz), where rx, ry and rz is a rotation vector representation of the tool orientation
  self.tcp_speed_target = torch.DoubleTensor(6)           -- Target speed of the tool given in Cartesian coordinates
  self.digital_input_bits = 0                             -- Current state of the digital inputs. NOTE: these are bits encoded as int64_t, e.g. a value of 5 corresponds to bit 0 and bit 2 set high
  self.motor_temps = torch.DoubleTensor(6)                -- Temperature of each joint in degrees celsius
  self.ctrl_timer = 0                                     -- Controller realtime thread execution time
  self.test_value = 0                                     -- A value used by Universal Robots software only
  self.robot_mode = 0                                     -- Robot mode (see DataStreamFromURController)
  self.joint_control_modes = torch.DoubleTensor(6)        -- Joint control modes see (DataStreamFromURController) (only from software version 1.8 and on)

  self:invalidate()
end


local function unpackVector(v, reader, count)
  for i=1,count do
    v[i] = reader:readFloat64()
  end
  return v
end


function RealtimeStateCB2:isValid()
  return self.valid
end


function RealtimeStateCB2:isRobotReady()
  --return self.valid and self.robot_mode == ur5.ROBOT_MODE.RUNNING and self.safety_mode == ur5.SAFETY_MODE.NORMAL
  return self.valid
end


function RealtimeStateCB2:invalidate()
  self.valid = false
end


function RealtimeStateCB2:read(reader)
  local len = reader:readUInt32()
  if len ~= 812 then
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
  unpackVector(self.tool_acc, reader, 3)

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
