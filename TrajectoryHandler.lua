local torch = require 'torch'
require 'TrajectorySampler'
local ur = require 'ur_env'


local MAX_NO_RESPONSE = 100   -- number of cycles without avail count message from driver before entering ConnectionLost state
local STOP_CYCLE_COUNT = 3  -- number of cycles with v < threshold before robot is considered stopped


local TrajectoryHandlerStatus = {
  ProtocolError = -3,
  ConnectionLost = -2,
  Canceled = -1,
  Fresh = 0,
  Streaming = 1,
  Flushing = 2,
  Cancelling = 3,     -- stop request has been sent to robot, waiting for zero velocity
  Completed = 1000,
}
ur.TrajectoryHandlerStatus = TrajectoryHandlerStatus


local TrajectoryHandler = torch.class('TrajectoryHandler')


function TrajectoryHandler:__init(ringSize, servoTime, realtimeState, reverseConnection, traj, flush, waitCovergence,
  maxBuffering, maxConvergenceCycles, goalPositionThreshold, goalVelocityThreshold, logger)

  assert(reverseConnection, "Argument 'reverseConnection' must not be nil.")
  assert(maxConvergenceCycles > 0, "Argument 'maxConvergenceCycles' must be greater than zero.")
  assert(goalPositionThreshold > 0, "Argument 'goalPositionThreshold' must be greater than zero.")
  assert(goalVelocityThreshold >= 0, "Argument 'goalVelocityThreshold' must be greater than or equal zero.")

  self.ringSize = ringSize
  self.realtimeState = realtimeState
  self.reverseConnection = reverseConnection
  self.traj = traj
  self.flush = flush
  self.waitCovergence = waitCovergence
  self.maxBuffering = maxBuffering or ringSize
  self.logger = logger or ur.DEFAULT_LOGGER
  self.status = TrajectoryHandlerStatus.Fresh
  self.sampler = TrajectorySampler(traj, servoTime)
  self.maxConvergenceCycles = maxConvergenceCycles
  self.goalPositionThreshold = goalPositionThreshold
  self.goalVelocityThreshold = goalVelocityThreshold
  self.noResponse = 0
  self.convergenceCycle = 0
  self.noMotionCycle = 0
end


function TrajectoryHandler:cancel()
  if self.status > 0 and self.status ~= TrajectoryHandlerStatus.Cancelling then
    self.reverseConnection:cancel()    -- send cancel message to robot
    self.status = TrajectoryHandlerStatus.Cancelling
  end
end


local function isRobotStopped(self)
  return self.noMotionCycle > STOP_CYCLE_COUNT
end


local function checkRobotStopped(self)
  local qd_actual = self.realtimeState.qd_actual
  if qd_actual:norm() < self.goalVelocityThreshold then
    self.noMotionCycle = self.noMotionCycle + 1
  else
    self.noMotionCycle = 0
  end
  return isRobotStopped(self)
end


local function reachedGoal(self)
  local q_goal = self.sampler:getGoalPosition()
  local q_actual = self.realtimeState.q_actual
  local qd_actual = self.realtimeState.qd_actual

  local goal_distance = torch.norm(q_goal - q_actual)

  self.logger.debug('Convergence cycle %d: |qd_actual|: %f; goal_distance (joints): %f;', self.convergenceCycle, qd_actual:norm(), goal_distance)

  self.convergenceCycle = self.convergenceCycle + 1
  if self.convergenceCycle >= self.maxConvergenceCycles then
    error(string.format('[TrajectoryHandler] Did not reach goal after %d convergence cycles. Goal distance: %f; |qd_actual|: %f;', self.maxConvergenceCycles, goal_distance, qd_actual:norm()))
  end

  return isRobotStopped(self) and goal_distance < self.goalPositionThreshold
end


function TrajectoryHandler:update()
  if self.status < 0 or self.status == TrajectoryHandlerStatus.Completed then
    return false
  end

  local driver = self.driver
  if self.sampler:atEnd() and self.flush == false and self.status ~= TrajectoryHandlerStatus.Cancelling then
    self.status = TrajectoryHandlerStatus.Completed
    return false  -- all data sent, nothing to do
  end

  local avail = self.reverseConnection:readAvailable()
  if avail == nil then
    self.noResponse = self.noResponse + 1
    if self.noResponse > MAX_NO_RESPONSE then
      self.status = TrajectoryHandlerStatus.ConnectionLost
      self.logger.error('[TrajectoryHandler] Error: No response from robot!')
      return false
    else
      return true   -- wait for more data from robot
    end
  end

  if avail < 0 or avail > self.ringSize-1 then
    self.status = TrajectoryHandlerStatus.ProtocolError
    self.logger.error('[TrajectoryHandler] Error: Invalid avail count received from robot. %s - %s', tostring(avail), tostring(self.ringSize))
    return false
  end

  self.noResponse = 0

  -- self.logger.debug('avail: %d', avail)
  if self.status == TrajectoryHandlerStatus.Cancelling then
    checkRobotStopped(self)

    self.reverseConnection:sendPoints({})   -- send zero count

    if checkRobotStopped(self) then    -- wait for robot to stop
      self.status = TrajectoryHandlerStatus.Canceled
      return false
    else

      -- check limited number of cycles and generate error when robot does not hold
      self.convergenceCycle = self.convergenceCycle + 1
      if self.convergenceCycle >= self.maxConvergenceCycles then
        error(string.format('[TrajectoryHandler] Error: Robot did not stop after %d convergence cycles. Goal distance: %f; |qd_actual|: %f;', self.maxConvergenceCycles, goal_distance, qd_actual:norm()))
      end

    end

  elseif not self.sampler:atEnd() then    -- if trajectory is not at end
    self.status = TrajectoryHandlerStatus.Streaming

    local pts = self.sampler:generateNextPoints(math.min(self.maxBuffering, avail))    -- send new trajectory points via reverse conncection
    self.reverseConnection:sendPoints(pts)

  else  -- all servo points have been sent, wait for robot to empty its queue

    self.reverseConnection:sendPoints({})   -- send zero count
    checkRobotStopped(self)
    if avail >= self.ringSize-1 and (reachedGoal(self) or not self.waitCovergence) then      -- when buffer is empty we are done
      self.status = TrajectoryHandlerStatus.Completed
      return false
    else
      self.status = TrajectoryHandlerStatus.Flushing
    end
  end

  return true   -- not yet at end of trajectory
end
