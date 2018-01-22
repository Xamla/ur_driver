#!/usr/bin/env th
local torch = require 'torch'
local ros = require 'ros'
local ffi = require 'ffi'
require 'URDriver'
require 'ros.actionlib.ActionServer'
local GoalStatus = require 'ros.actionlib.GoalStatus'
local actionlib = ros.actionlib
local ur5 = require 'ur5_env'
local xamal_sysmon = require 'xamla_sysmon'

local nh                        -- ros node handle
local jointStatePublisher       -- joint state publisher
local jointMsg                  -- joint state message
local jointNames = {}
local followTrajectoryServer    -- action server
local trajectoryQueue = {}      -- list of pending trajectories
local currentTrajectory
local driver
local posTrajControllerCommandSub   -- open loop position based trajectory controller listener
local currentPosTraj                -- active trajectory of pos traj controller


-- http://docs.ros.org/fuerte/api/control_msgs/html/msg/FollowJointTrajectoryResult.html
local TrajectoryResultStatus = {
  SUCCESSFUL = 0,
  INVALID_GOAL = -1,
  INVALID_JOINTS = -2,
  OLD_HEADER_TIMESTAMP = -3,
  PATH_TOLERANCE_VIOLATED = -4,
  GOAL_TOLERANCE_VIOLATED = -5
}


local function createJointNames(prefix)
  prefix = prefix or ''
  local jointNames = {}
  for i,n in ipairs(ur5.JOINT_NAMES) do
    table.insert(jointNames, prefix .. n)
  end
  return jointNames
end


local function publishJointStates(driver)
  local state = driver:getRealtimeState()
  jointMsg.header.stamp = ros.Time.now()
  jointMsg.position = state.q_actual
  jointMsg.velocity = state.qd_actual
  jointMsg.effort = state.i_actual
  jointStatePublisher:publish(jointMsg)
end


local function findIndex(t, condition)
  for i,v in ipairs(t) do
    if condition(v, i) then
      return i
    end
  end
  return -1
end


local function copyMapped(dst, src, map)
  for k,v in pairs(map) do
    dst[k] = src[v]
  end
end


local function decodeJointTrajectoryMsg(trajectory)
  -- print('trajectory_msg')
  -- print(trajectory)

  -- get joint names and create mapping
  local jointMapping = {}
  for i,name in ipairs(trajectory.joint_names) do
    local j = findIndex(jointNames, function(x) return x == name end)
    if j > 0 then
      jointMapping[j] = i
    end
  end

  local pointCount = #trajectory.points
  local time = torch.zeros(pointCount)       -- convert trajectory to internal tensor format
  local pos = torch.zeros(pointCount, 6)
  local vel = torch.zeros(pointCount, 6)
  local acc = torch.zeros(pointCount, 6)
  local hasVelocity = true
  local hasAcceleration = true

  for i=1,pointCount do
    local pt = trajectory.points[i]
    time[i] = pt.time_from_start:toSec()
    copyMapped(pos[i], pt.positions, jointMapping)

    if pt.velocities ~= nil and pt.velocities:nElement() > 0 then
      copyMapped(vel[i], pt.velocities, jointMapping)
    else
      hasVelocity = false
    end

    if pt.accelerations ~= nil and pt.accelerations:nElement() > 0 then
      copyMapped(acc[i], pt.accelerations, jointMapping)
    else
      hasAcceleration = false
    end
  end

  if not hasAcceleration then
    acc = nil
  end

  --[[
  print('current joint configuration (q_actual):')
  print(driver:getRealtimeState().q_actual)

  print('time:')
  print(time)

  print('pos:')
  print(pos)

  print('vel:')
  print(vel)
  ]]

  return time, pos, vel, acc
end


-- Called when new follow trajectory action goal is received.
-- http://docs.ros.org/fuerte/api/control_msgs/html/msg/FollowJointTrajectoryActionGoal.html
local function FollowJointTrajectory_Goal(goalHandle)
  local g = goalHandle:getGoal()

  -- decode trajectory
  local time, pos, vel, acc = decodeJointTrajectoryMsg(g.goal.trajectory)

  ros.INFO('FollowJointTrajectory_Goal: Trajectory with %d points received.', time:nElement())

  local traj = {
    time = time, pos = pos, vel = vel, acc = acc,
    goalHandle = goalHandle, goal = g,
    accept = function(self)
      if goalHandle:getGoalStatus().status == GoalStatus.PENDING then
        goalHandle:setAccepted('Starting trajectory execution')
        return true
      else
        ros.WARN('Status of queued trajectory is not pending but %d.', goalHandle:getGoalStatus().status)
        return false
      end
    end,
    proceed = function(self)
      if goalHandle:getGoalStatus().status == GoalStatus.ACTIVE then
        return true
      else
        ros.WARN('Goal status of current trajectory no longer ACTIVE (actual: %d).', goalHandle:getGoalStatus().status)
        return false
      end
    end,
    abort = function(self, msg)
      goalHandle:setAborted(nil, msg or 'Error')
    end,
    completed = function(self)
      local r = goalHandle:createResult()
      r.error_code = TrajectoryResultStatus.SUCCESSFUL
      goalHandle:setSucceeded(r, 'Completed')
    end
  }

  if traj.pos:nElement() == 0 then    -- empty trajectory
    local r = goalHandle:createResult()
    r.error_code = TrajectoryResultStatus.SUCCESSFUL
    goalHandle:setSucceeded(r, 'Completed (nothing to do)')
    ros.WARN('Received empty FollowJointTrajectory request (goal: %s).', goalHandle:getGoalID().id)
  else
    local ok, reason = driver:validateTrajectory(traj)
    if ok then
      driver:doTrajectoryAsync(traj)    -- queue for processing
      ros.INFO('Trajectory queued for execution (goal: %s).', goalHandle:getGoalID().id)
    else
      -- trajectory is not valid, immediately abort it
      ros.WARN('Aborting trajectory processing: ' .. reason)
      local r = goalHandle:createResult()
      r.error_code = TrajectoryResultStatus.INVALID_GOAL
      goalHandle:setRejected(r, 'Validation of trajectory failed')
    end
  end
end


local function FollowJointTrajectory_Cancel(goalHandle)
  ros.INFO('FollowJointTrajectory_Cancel')

  if driver.currentTrajectory ~= nil and driver.currentTrajectory.goalHandle == goalHandle then
    driver:cancelCurrentTrajectory('Canceled')
  else
    -- check if trajectory is in trajectoryQueue
    local i = findIndex(driver.trajectoryQueue, function(x) return x.goalHandle == goalHandle end)
    if i > 0 then
      -- entry found, simply remove from queue
      table.remove(driver.trajectoryQueue, i)
    end
    goalHandle:setCanceled(nil, 'Canceled')
  end
end


local function indexOf(t, v)
  for i=1,#t do
    if t[i] == v then
      return i
    end
  end
  return -1
end


local function posTrajController_Command(msg, header, subscriber)
  -- msg is trajectory_msgs/JointTrajectory, see http://docs.ros.org/kinetic/api/trajectory_msgs/html/msg/JointTrajectory.html

  -- decode trajectory
  local time, pos, vel, acc = decodeJointTrajectoryMsg(msg)

  if time:nElement() == 0 then    -- empty trajectory
    ros.WARN('Received empty JointTrajectory request.')
    return
  end

  ros.DEBUG('posTrajController_Command: Trajectory with %d points received.', time:nElement())

  local traj = {
    maxBuffering = 2,
    flush = true,
    waitConvergence = true,
    time = time, pos = pos, vel = vel, acc = acc,
    abort = function(self, msg)
      currentPosTraj = nil
    end,
    completed = function(self)
      currentPosTraj = nil
    end
  }

  local ok, reason = driver:validateTrajectory(traj)
  if not ok then
    -- trajectory is not valid, ignore request
    ros.WARN('Aborting trajectory processing: ' .. reason)
    return
  end

  if currentPosTraj ~= nil then

    if driver.currentTrajectory ~= nil and driver.currentTrajectory.traj == currentPosTraj then
      currentPosTraj = driver:blendTrajectory(traj)
      ros.DEBUG('Blending existing trajectory to new trajectory (posTrajController).')
    else
      local i = indexOf(driver.trajectoryQueue, currentPosTraj)
      if i > 0 then
        currentPosTraj = traj
        driver.trajectoryQueue[i] = traj    -- replace exsting pending trajectory
      else
        ros.WARN('currentPosTraj not found in driver queue.')
        currentPosTraj = traj
        driver:doTrajectoryAsync(traj)      -- queue for processing
      end
    end

  else

    if #driver.trajectoryQueue == 0 then
      currentPosTraj = traj
      driver:doTrajectoryAsync(traj)    -- queue for processing
      ros.DEBUG("Trajectory queued for execution (posTrajController).")
    else
      ros.WARN('Driver busy. Rejecting posTrajController request.')
    end

  end

end


local function startPosTrajControllerListener(controllerName)
  --local commandTopic = string.format('%s/joint_command', controllerName)
  local commandTopic = 'joint_command'
  posTrajControllerCommandSub = nh:subscribe(commandTopic, 'trajectory_msgs/JointTrajectory', 1)
  posTrajControllerCommandSub:registerCallback(posTrajController_Command)
end


local function printSplash()
  print([[
   _  __                __
  | |/ /___ _____ ___  / /___ _     "I like to move it move it"
  |   / __ `/ __ `__ \/ / __ `/
 /   / /_/ / / / / / / / /_/ /
/_/|_\__,_/_/ /_/ /_/_/\__,_/

Xamla UR5 ROS driver v1.0
]])
end


local function main()
  printSplash()

  local rosArgs = {}

  -- filter special ROS command line args
  for k,v in pairs(arg) do
    if v:sub(1,2) == '__' then
      table.insert(rosArgs, v)
      arg[k] = nil
    end
  end

  -- parse command line arguments
  local cmd = torch.CmdLine()
  cmd:text()
  cmd:text('Xamla UR5 ROS driver')
  cmd:text()
  cmd:option('-hostname',              'ur5',             'hostname of robot to connect to')
  cmd:option('-realtime-port',         30003,             'realtime port')
  cmd:option('-reversename',              '',             'servername. Robot connect to localhost')
  cmd:option('-reverse-realtime-port',    0,              'realtime port')
  cmd:option('-lookahead',             0.01,              'lookahead time (in ms) for servoj')
  cmd:option('-gain',                  1000,              'gain parameter for servoj')
  cmd:option('-servo-time',           0.008,              'servo time (in ms) for servoj')
  cmd:option('-path-tolerance',       math.pi / 10,       'max set point distance to current joint configuration')
  cmd:option('-ring-size',            64,                 'robot side ring buffer ring-size')
  cmd:option('-script-template',      'driverCB3.urscript',  'filename of urscript template executed on robot')
  cmd:option('-max-idle-cycles',      250,                'number of idle cycles before driver_proc shutdown')
  cmd:option('-controller-name',  'ur5', 'Emulation of ROS position controller')
  cmd:option('-joint-name-prefix',  '', 'Name prefix of published joints')
  cmd:option('-use-cb2', false, 'Use CB2 controller instead of CB3')
  local opt = cmd:parse(arg or {})

  -- ros initialization
  ros.init('ur5_driver', nil, rosArgs)
  nh = ros.NodeHandle('~')

  local logger = {
    debug = ros.DEBUG,
    info = ros.INFO,
    warn = ros.WARN,
    error = ros.ERROR
  }

  local controllerName, ok = nh:getParamString('controller_name')
  if (ok == false) then
    controllerName = opt['controller-name']
  end

  if opt['controller-name'] and #opt['controller-name'] > 0 then
    startPosTrajControllerListener(opt['controller-name'])
  end

  -- create driver object
  local driverConfiguration = {
    hostname                = opt['hostname'],
    realtimePort            = opt['realtime-port'],
    reversename             = opt['reversename'],
    reverserealtimePort     = opt['reverse-realtime-port'],
    lookahead               = opt['lookahead'],
    servoTime               = opt['servo-time'],
    gain                    = opt['gain'],
    pathTolerance           = opt['path-tolerance'],
    ringSize                = opt['ring-size'],
    scriptTemplateFilename  = opt['script-template'],
    maxIdleCycles           = opt['max-idle-cycles'],
    maxSinglePointTrajectoryDistance = opt['max-single-point-trajectory-distance'],
    jointNamePrefix           = opt['joint-name-prefix'],
    useCb2           = opt['use-cb2']
  }

  local overrideInputArguments = function (key, value, ok)
    if ok == true  then
      driverConfiguration[key] = value
    end
  end

  overrideInputArguments('hostname', nh:getParamString('hostname'))
  overrideInputArguments('realtimePort', nh:getParamInt('realtime_port'))
  overrideInputArguments('reversename', nh:getParamString('reversename'))
  overrideInputArguments('reverserealtimePort', nh:getParamInt('reverse_realtime_port'))
  overrideInputArguments('lookahead', nh:getParamDouble('lookahead'))
  overrideInputArguments('servoTime', nh:getParamDouble('servo_time'))
  overrideInputArguments('gain', nh:getParamDouble('gain'))
  overrideInputArguments('pathTolerance', nh:getParamDouble('path_tolerance'))
  overrideInputArguments('ringSize', nh:getParamInt('ring_size'))
  overrideInputArguments('scriptTemplateFilename', nh:getParamString('script_template'))
  overrideInputArguments('maxIdleCycles', nh:getParamInt('max_idle_cycles'))
  overrideInputArguments('maxSinglePointTrajectoryDistance', nh:getParamDouble('max_single_point_trajectory_distance'))
  overrideInputArguments('jointNamePrefix', nh:getParamString('joint_name_prefix'))
  overrideInputArguments('useCb2', nh:getParamBool('use_cb2'))

  if driverConfiguration['reversename'] == '' then driverConfiguration['reversename'] = nil end

  -- create joint state publisher
  jointNames = createJointNames(driverConfiguration.jointNamePrefix)
  jointStatePublisher = nh:advertise('/joint_states', 'sensor_msgs/JointState', 1)
  jointMsg = jointStatePublisher:createMessage()
  jointMsg.name = jointNames

  -- print effective options
  print('Effective driver configuration:')
  print(driverConfiguration)

  local heartbeat = xamal_sysmon.Heartbeat()
  heartbeat:start(nh, 5)
  driver = URDriver(driverConfiguration, logger, heartbeat)
  driver:addSyncCallback(publishJointStates)

  -- set up follow trajectory action server
  ros.console.setLoggerLevel('actionlib', ros.console.Level.Debug)
  followTrajectoryServer = actionlib.ActionServer(nh, 'follow_joint_trajectory', 'control_msgs/FollowJointTrajectory')
  followTrajectoryServer:registerGoalCallback(FollowJointTrajectory_Goal)
  followTrajectoryServer:registerCancelCallback(FollowJointTrajectory_Cancel)
  followTrajectoryServer:start()

  -- main driver loop
  local rate = ros.Rate(250)
  while ros.ok() do
    driver:spin()
    heartbeat:publish()
    ros.spinOnce()
    collectgarbage()
    rate:sleep()
  end

  -- tear down components
  if posTrajControllerCommandSub ~= nil then
    posTrajControllerCommandSub:shutdown()
  end

  jointStatePublisher:shutdown()
  followTrajectoryServer:shutdown()
  driver:shutdown()
  nh:shutdown()
  ros.shutdown()
end


main()
