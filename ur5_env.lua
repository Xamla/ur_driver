local ros = require 'ros'
local convertToBigEndianReader = require 'BigEndianAdapter'


local ur5 = {}


ur5.JOINT_NAMES = {
  "shoulder_pan_joint",
  "shoulder_lift_joint",
  "elbow_joint",
  "wrist_1_joint",
  "wrist_2_joint",
  "wrist_3_joint"
}


ur5.ROBOT_MODE = {
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


ur5.CONTROL_MODE = {
  POSITION = 0,
  TEACH = 1,
  FORCE = 2,
  TORQUE = 3
}


ur5.JOINT_MODE = {
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


ur5.SAFETY_MODE = {
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


ur5.ROBOT_MODE.tostring = createToString(ur5.ROBOT_MODE)
ur5.CONTROL_MODE.tostring = createToString(ur5.CONTROL_MODE)
ur5.JOINT_MODE.tostring = createToString(ur5.JOINT_MODE)
ur5.SAFETY_MODE.tostring = createToString(ur5.SAFETY_MODE)


function ur5.printf(...)
  return print(string.format(...))
end


function ur5.serializeInt32Array(a, writer)
  local w = writer or convertToBigEndianReader(ros.StorageWriter())
  for i,x in ipairs(a) do
    w:writeInt32(x)
  end
  w:shrinkToFit()
  return w.storage:string()
end


ur5.DEFAULT_LOGGER = {
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


return ur5
