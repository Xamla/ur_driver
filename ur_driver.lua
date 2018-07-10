#!/usr/bin/env th
--[[
ur_driver.lua

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
local ros = require 'ros'
local ffi = require 'ffi'
require 'URDriver'
require 'ros.actionlib.ActionServer'
local GoalStatus = require 'ros.actionlib.GoalStatus'
local actionlib = ros.actionlib
local ur = require 'ur_env'
local findIndex, copyMapped, indexOf  = ur.findIndex, ur.copyMapped, ur.indexOf
local xamal_sysmon = require 'xamla_sysmon'


local nh                        -- ros node handle
local jointStatePublisher       -- joint state publisher
local jointMsg                  -- joint state message
local jointNames = {}
local followTrajectoryServer    -- action server
local trajectoryQueue = {}      -- list of pending trajectories
local driver
local posTrajControllerCommandSub   -- open loop position based trajectory controller listener
local currentPosTraj                -- active trajectory of pos traj controller


-- see http://docs.ros.org/api/control_msgs/html/action/FollowJointTrajectory.html
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
  for i,n in ipairs(ur.JOINT_NAMES) do
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


local function decodeJointTrajectoryMsg(trajectory)
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

  return time, pos, vel, acc
end


local function isTerminalGoalStatus(status)
  return status == GoalStatus.PREEMPTED or
    status == GoalStatus.SUCCEEDED or
    status == GoalStatus.ABORTED or
    status == GoalStatus.REJECTED or
    status == GoalStatus.RECALLED or
    status == GoalStatus.LOST
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
      local status = goalHandle:getGoalStatus().status
      if status == GoalStatus.PENDING then
        goalHandle:setAccepted('Starting trajectory execution')
        return true
      else
        ros.WARN('Status of queued trajectory is not pending but %d.', status)
        return false
      end
    end,
    proceed = function(self)
      local status = goalHandle:getGoalStatus().status
      if status == GoalStatus.ACTIVE then
        return true
      else
        ros.WARN('Goal status of current trajectory no longer ACTIVE (actual: %d).', status)
        return false
      end
    end,
    -- cancel signals the begin of a cancel/stop operation (to enter the preempting action state)
    cancel = function(self)
      local status = goalHandle:getGoalStatus().status
      if status == GoalStatus.PENDING or status == GoalStatus.ACTIVE then
        goalHandle:setCancelRequested()
      end
    end,
    -- abort signals end of trajectory processing
    abort = function(self, msg)
      local status = goalHandle:getGoalStatus().status
      if status == GoalStatus.ACTIVE or status == GoalStatus.PREEMPTING then
        goalHandle:setAborted(nil, msg or 'Error')
      elseif status == GoalStatus.PENDING or status == GoalStatus.RECALLING then
        goalHandle:setCanceled(nil, msg or 'Error')
      elseif not isTerminalGoalStatus(status) then
        ros.ERROR('[abort trajectory] Unexpected goal status: %d', status)
      end
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

  if driver.currentTrajectory ~= nil and driver.currentTrajectory.traj.goalHandle == goalHandle then
    driver:cancelCurrentTrajectory()
  else
    -- check if trajectory is in trajectoryQueue
    local i = findIndex(driver.trajectoryQueue, function(x) return x.goalHandle == goalHandle end)
    if i > 0 then
      -- entry found, simply remove from queue
      table.remove(driver.trajectoryQueue, i)
    else
      ros.WARN('Trajectory to cancel with goal handle \'%s\' not found.', goalHandle:getGoalID().id)
    end
    goalHandle:setCanceled(nil, 'Canceled')
  end
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


local function printSplash()
  print([[
   _  __                __
  | |/ /___ _____ ___  / /___ _     "I like to move it move it"
  |   / __ `/ __ `__ \/ / __ `/
 /   / /_/ / / / / / / / /_/ /
/_/|_\__,_/_/ /_/ /_/_/\__,_/

Xamla UR ROS driver v1.1
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
  cmd:text('Xamla Rosvita driver for Universal Robots (UR3/UR5/UR10)')
  cmd:text()
  cmd:option('-hostname',              'ur5',              'hostname of robot to connect to')
  cmd:option('-reverse-name',             '',              'Hostname used in URScript to connect to this driver')
  cmd:option('-reverse-port',              0,              'Port on which this driver is listening for reversve connections')
  cmd:option('-lookahead',              0.01,              'lookahead time (in ms) for servoj')
  cmd:option('-gain',                   1200,              'gain parameter for servoj')
  cmd:option('-servo-time',            0.008,              'servo time (in ms) for servoj')
  cmd:option('-path-tolerance', math.pi / 10,              'max set point distance to current joint configuration')
  cmd:option('-ring-size',                64,              'robot side ring buffer ring-size')
  cmd:option('-max-idle-cycles',         250,              'number of idle cycles before driver_proc shutdown')
  cmd:option('-joint-name-prefix',        '',              'name prefix of published joints')
  cmd:option('-max-convergence-cycles',  150,              'max number of cycles to wait for goal convergence')
  cmd:option('-goal-position-threshold', 0.005,            'goal convergence position threshold (in rad)')
  cmd:option('-goal-velocity-threshold', 0.01,             'goal convergence velocity threshold (in rad/s)')
  local opt = cmd:parse(arg or {})

  -- ros initialization
  local ros_init_options = 0
  ros.init('ur_driver', ros_init_options, rosArgs)
  nh = ros.NodeHandle('~')

  local logger = {
    debug = ros.DEBUG,
    info = ros.INFO,
    warn = ros.WARN,
    error = ros.ERROR
  }

  -- create driver object
  local driverConfiguration = {
    hostname                = opt['hostname'],
    reverseName             = opt['reverse-name'],
    reversePort             = opt['reverse-port'],
    lookahead               = opt['lookahead'],
    servoTime               = opt['servo-time'],
    gain                    = opt['gain'],
    pathTolerance           = opt['path-tolerance'],
    ringSize                = opt['ring-size'],
    maxIdleCycles           = opt['max-idle-cycles'],
    maxSinglePointTrajectoryDistance = opt['max-single-point-trajectory-distance'],
    maxConvergenceCycles    = opt['max-convergence-cycles'],
    goalPositionThreshold   = opt['goal-position-threshold'],
    goalVelocityThreshold   = opt['goal-velocity-threshold'],
    jointNamePrefix         = opt['joint-name-prefix']
  }

  local overrideInputArguments = function (key, value, ok)
    if ok == true  then
      driverConfiguration[key] = value
    end
  end

  overrideInputArguments('hostname', nh:getParamString('hostname'))
  overrideInputArguments('reverseName', nh:getParamString('reverse_name'))
  overrideInputArguments('reversePort', nh:getParamInt('reverse_port'))
  overrideInputArguments('lookahead', nh:getParamDouble('lookahead'))
  overrideInputArguments('servoTime', nh:getParamDouble('servo_time'))
  overrideInputArguments('gain', nh:getParamDouble('gain'))
  overrideInputArguments('pathTolerance', nh:getParamDouble('path_tolerance'))
  overrideInputArguments('ringSize', nh:getParamInt('ring_size'))
  overrideInputArguments('maxIdleCycles', nh:getParamInt('max_idle_cycles'))
  overrideInputArguments('maxSinglePointTrajectoryDistance', nh:getParamDouble('max_single_point_trajectory_distance'))
  overrideInputArguments('maxConvergenceCycles', nh:getParamInt('max_convergence_cycles'))
  overrideInputArguments('goalPositionThreshold', nh:getParamDouble('goal_position_threshold'))
  overrideInputArguments('goalVelocityThreshold', nh:getParamDouble('goal_velocity_threshold'))
  overrideInputArguments('jointNamePrefix', nh:getParamString('joint_name_prefix'))

  if driverConfiguration['reverseName'] == '' then driverConfiguration['reverseName'] = nil end

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

  posTrajControllerCommandSub = nh:subscribe('joint_command', 'trajectory_msgs/JointTrajectory', 1)
  posTrajControllerCommandSub:registerCallback(posTrajController_Command)

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
  posTrajControllerCommandSub:shutdown()
  jointStatePublisher:shutdown()
  followTrajectoryServer:shutdown()
  driver:shutdown()
  nh:shutdown()
  ros.shutdown()
end


main()
