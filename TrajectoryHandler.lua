local torch = require 'torch'
require 'TrajectorySampler'
local ur5 = require 'ur5_env'


local GOAL_CONVERGENCE_POSITION_THRESHOLD = 0.00051   -- in rad
local GOAL_CONVERGENCE_VELOCITY_THRESHOLD = 0.001    -- in rad/s
local MAX_CONVERGENCE_CYCLES = 50


local TrajectoryHandlerStatus = {
  ProtocolError = -3,
  ConnectionLost = -2,
  Canceled = -1,
  Fresh = 0,
  Streaming = 1,
  Flushing = 2,
  Completed = 1000,
}
ur5.TrajectoryHandlerStatus = TrajectoryHandlerStatus


local TrajectoryHandler = torch.class('TrajectoryHandler')


function TrajectoryHandler:__init(ringSize, servoTime, realtimeState, reverseConnection, traj, flush, waitCovergence, maxBuffering, logger)
  assert(reverseConnection, "[TrajectoryHandler] Argument 'reverseConnection' must not be nil.")
  self.ringSize = ringSize
  self.realtimeState = realtimeState
  self.reverseConnection = reverseConnection
  self.traj = traj
  self.flush = flush
  self.waitCovergence = waitCovergence
  self.maxBuffering = maxBuffering or ringSize
  self.logger = logger or ur5.DEFAULT_LOGGER
  self.noResponse = 0
  self.status = TrajectoryHandlerStatus.Fresh
  self.sampler = TrajectorySampler(traj, servoTime)
  self.noResponse = 0
  self.convergenceCycle = 0
end


function TrajectoryHandler:cancel()
  if self.status > 0 then
    self.reverseConnection:cancel()    -- send cancel message to robot
    self.status = TrajectoryHandlerStatus.Canceled
  end
end


local function reachedGoal(self)
  local q_goal = self.sampler:getGoalPosition()
  local q_actual = self.realtimeState.q_actual
  local qd_actual = self.realtimeState.qd_actual

  local goal_distance = torch.norm(q_goal - q_actual)

  self.logger.debug('Convergence cycle %d: |qd_actual|: %f; goal_distance (joints): %f;', self.convergenceCycle, qd_actual:norm(), goal_distance)

  self.convergenceCycle = self.convergenceCycle + 1
  if self.convergenceCycle >= MAX_CONVERGENCE_CYCLES then
    error(string.format('Did not reach goal after %d convergence cycles.', MAX_CONVERGENCE_CYCLES))
  end

  return qd_actual:norm() < GOAL_CONVERGENCE_VELOCITY_THRESHOLD and goal_distance < GOAL_CONVERGENCE_POSITION_THRESHOLD
end


function TrajectoryHandler:update()
  if self.status < 0 or self.status == TrajectoryHandlerStatus.Completed then
    return false
  end

  local driver = self.driver
  if self.sampler:atEnd() and self.flush == false then
    self.status = TrajectoryHandlerStatus.Completed
    return false  -- all data sent, nothing to do
  end

  local avail = self.reverseConnection:readAvailable()
  if avail == nil then
    self.noResponse = self.noResponse + 1
    if self.noResponse > 100 then
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

  if not self.sampler:atEnd() then    -- if trajectory is not at end
    self.status = TrajectoryHandlerStatus.Streaming

    local pts = self.sampler:generateNextPoints(math.min(self.maxBuffering, avail))    -- send new trajectory points via reverse conncection
    self.reverseConnection:sendPoints(pts)

  else  -- all servo points have been sent, wait for robot to empty its queue

    self.reverseConnection:sendPoints({})   -- send zero count
    if avail >= self.ringSize-1 and (reachedGoal(self) or not self.waitCovergence) then      -- when buffer is empty we are done
      self.status = TrajectoryHandlerStatus.Completed
      return false
    else
      self.status = TrajectoryHandlerStatus.Flushing
    end
  end

  return true   -- not yet at end of trajectory
end
