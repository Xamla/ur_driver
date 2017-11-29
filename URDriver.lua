local torch = require 'torch'
local sys = require 'sys'
local ffi = require 'ffi'
require 'RealtimeStream'
require 'RealtimeState'
require 'ReverseConnection'
require 'TrajectoryHandler'
local ur5 = require 'ur5_env'


local RealtimeStreamState = ur5.RealtimeStreamState
local TrajectoryHandlerStatus = ur5.TrajectoryHandlerStatus


-- constants
local MULT_JOINT = 1000000
local MAX_SYNC_READ_TRIES = 250
local DEFAULT_MAX_IDLE_CYCLES = 250      -- number of idle sync-cyles before reverse connection shutdown (that re-enables freedrive)
local DEFAULT_HOSTNAME = 'ur5'
local DEFAULT_REALTIME_PORT = 30003
local DEFAULT_REVERSENAME = nil
local DEFAULT_REVERSE_REALTIME_PORT = 0
local DEFAULT_RING_SIZE = 64
local DEFAULT_PATH_TOLERANCE = math.pi / 10
local DEFAULT_LOOKAHEAD = 0.01
local DEFAULT_SERVO_TIME = 0.008
local DEFAULT_GAIN = 1000
local DEFAULT_SCRIPT_TEMPLATE_FILENAME = 'driver.urscript'
local DEFAULT_MAX_SINGLE_POINT_TRAJECTORY_DISTANCE = 0.5      -- max allowed distance of single point trajectory target relative to current joint pos


-- these values are used for a basic trajectory sanity check (in validateTrajectory)
local JOINT_POSITION_LIMITS = torch.Tensor({
  { -math.pi*2, math.pi*2 }, -- shoulder_pan_joint (min, max)
  { -math.pi*2, math.pi*2 }, -- shoulder_lift_joint (min, max)
  { -math.pi*2, math.pi*2 }, -- elbow_joint (min, max)
  { -math.pi*2, math.pi*2 }, -- wrist_1_joint (min, max)
  { -math.pi*2, math.pi*2 }, -- wrist_2_joint (min, max)
  { -math.pi*2, math.pi*2 }, -- wrist_3_joint (min, max)
})


local JOINT_VELOCITY_LIMITS = torch.Tensor({
  { -math.pi*4, math.pi*4 }, -- shoulder_pan_joint (min, max)
  { -math.pi*4, math.pi*4 }, -- shoulder_lift_joint (min, max)
  { -math.pi*4, math.pi*4 }, -- elbow_joint (min, max)
  { -math.pi*4, math.pi*4 }, -- wrist_1_joint (min, max)
  { -math.pi*4, math.pi*4 }, -- wrist_2_joint (min, max)
  { -math.pi*4, math.pi*4 }, -- wrist_3_joint (min, max)
})


local URDriver = torch.class('URDriver')


function URDriver:__init(cfg, logger, heartbeat)
  self.logger = logger or ur5.DEFAULT_LOGGER
  self.heartbeat = heartbeat

  -- apply configuration
  self.hostname = cfg.hostname or DEFAULT_HOSTNAME
  self.realtimePort = cfg.realtimePort or DEFAULT_REALTIME_PORT
  self.reversename = cfg.reversename or DEFAULT_REVERSENAME
  self.reverserealtimePort = cfg.reverserealtimePort or DEFAULT_REVERSE_REALTIME_PORT
  self.lookahead = cfg.lookahead or DEFAULT_LOOKAHEAD
  self.gain = cfg.gain or DEFAULT_GAIN
  self.servoTime = cfg.servoTime or DEFAULT_SERVO_TIME
  self.pathTolerance = cfg.pathTolerance or DEFAULT_PATH_TOLERANCE
  self.ringSize = cfg.ringSize or DEFAULT_RING_SIZE
  self.maxIdleCycles = cfg.maxIdleCycles or DEFAULT_MAX_IDLE_CYCLES
  self.maxSinglePointTrajectoryDistance = cfg.maxSinglePointTrajectoryDistance or DEFAULT_MAX_SINGLE_POINT_TRAJECTORY_DISTANCE
  self.jointNamePrefix = cfg.jointNamePrefix

  -- read script template
  local scriptFilename = cfg.scriptTemplateFileName or DEFAULT_SCRIPT_TEMPLATE_FILENAME
  local f = assert(io.open(scriptFilename, "r"))
  self.scriptTemplate = f:read("*all")
  f:close()

  self.realtimeState = RealtimeState()
  self.realtimeStream = RealtimeStream(self.realtimeState, self.logger)
  self.syncCallbacks = {}
  self.trajectoryQueue = {}      -- list of pending trajectories
end


function URDriver:validateTrajectory(traj)
  local time, pos, vel = traj.time, traj.pos, traj.vel

  if pos == nil or time == nil or pos:nElement() == 0 or time:nElement() == 0 then
    return false, 'Trajectory has no waypoints.'
  end

  if pos:size(1) ~= time:size(1) then
    return false, "Row counts of 'time' and 'pos' fields do not match."
  end

  -- verify that time is strictly increasing
  local last_time = -math.huge
  for i=1,time:size(1) do
    if time[i] < last_time then
      return false, 'time_from_start of trajectory is not strictly increasing.'
    end
    last_time = time[i]

    -- check joints
    for j=1,6 do
      if pos[{i,j}] < JOINT_POSITION_LIMITS[{j,1}] or pos[{i,j}] > JOINT_POSITION_LIMITS[{j,2}] then
        return false, string.format("Waypoint %d: Position %f of joint '%s' lies outside control limits.", i, pos[{i,j}], ur5.JOINT_NAMES[j])
      end
      if vel[{i,j}] < JOINT_VELOCITY_LIMITS[{j,1}] or vel[{i,j}] > JOINT_VELOCITY_LIMITS[{j,2}] then
        return false, string.format("Waypoint %d: Velocity %f of joint '%s' lies outside control limits.", i, vel[{i,j}], ur5.JOINT_NAMES[j])
      end
    end
  end

  return true, 'ok'
end


function URDriver:blendTrajectory(traj)
  assert(traj and traj.time:nElement() > 0)

  if self.currentTrajectory == nil then
    self:doTrajectoryAsync(traj)
    return traj
  else

    local traj_
    if traj.time:size(1) == 1 and traj.time[1] == 0 then
      traj_ = traj    -- special case for single point trajectory with 0 time_from_start (no blending)
    else
      -- get position of last command sent to robot
      local sampler = self.currentTrajectory.handler.sampler
      local q,qd = sampler:evaluateAt(sampler:getCurrentTime())

      local time = torch.Tensor(traj.time:size(1)+1)
      local pos = torch.Tensor(traj.pos:size(1)+1, 6)
      local vel = torch.Tensor(traj.vel:size(1)+1, 6)

      time[1] = 0
      time[{{2,time:size(1)}}] = traj.time + self.servoTime

      pos[1] = q
      pos[{{2,pos:size(1)},{}}] = traj.pos
      vel[1] = qd
      vel[{{2,vel:size(1)},{}}] = traj.vel

      -- create new blended trajectory (do not modify caller's trajectory)
      traj_  = {
        time = time,
        pos = pos,
        vel = vel,
        abort = traj.abort,
        completed = traj.completed,
        flush = traj.flush,
        waitConvergence = traj.waitConvergence,
        maxBuffering = traj.maxBuffering
      }
    end

    local handler_ = self:createTrajectoryHandler(traj_, traj_.flush, traj_.waitConvergence, traj_.maxBuffering)

    self.currentTrajectory = {
      startTime = sys.clock(),     -- debug information
      traj = traj_,
      handler = handler_
    }

    return traj_
  end
end


function URDriver:doTrajectoryAsync(traj)
  table.insert(self.trajectoryQueue, traj)
end


function URDriver:getRealtimeState()
  return self.realtimeState
end


function URDriver:addSyncCallback(fn)
  table.insert(self.syncCallbacks, fn)
end


function URDriver:removeSyncCallback(fn)
  for i,x in ipairs(self.syncCallbacks) do
    if x == fn then
      table.remove(self.syncCallbacks, i)
      return
    end
  end
end


function URDriver:sync()
  for i=1,MAX_SYNC_READ_TRIES do

    if self.realtimeStream:getState() ~= RealtimeStreamState.Connected then
      return false, '[URDriver] Realtime stream not connected.'
    end

    if self.realtimeStream:read() then

      -- execute sync callbacks (e.g. to publish joint states)
      for i,fn in ipairs(self.syncCallbacks) do
        fn(self)
      end

      return true
    end

  end

  return false, '[URDriver] Sync timeout.'
end


local function establishReverseConnection(self)
  if self.reverseConnection ~= nil then
    return
  end

  if self.reverseListener == nil then
    error('[URDriver] Reverse listener not ready.')
  end

  local function fillTemplate(urscript_template, variables)
    local s = urscript_template
    for k,v in pairs(variables) do
      s = string.gsub(s, '${' .. k .. '}', v)
    end
    return s
  end

  local function generateZeros(count)
    return "0" .. string.rep(',0', count-1)
  end

  local ip, port = self.reverseListener:getsockname()

  local variables = {
    IP            = self.reversename or ip,
    Port          = port,
    ServoTime     = self.servoTime,
    LookAhead     = self.lookahead,
    Gain          = self.gain,
    PathTolerance = self.pathTolerance,
    RingSize      = self.ringSize,
    RingZeros     = generateZeros(self.ringSize * 7)
  }

  local s = fillTemplate(self.scriptTemplate, variables)
  local ok, err = self.realtimeStream:send(s)
  if not ok then
    error('[URDriver] Sending driver script failed: ' .. err)
  end
  self.logger.info('[URDriver] Length of script sent to robot: %d characters', #s)

  -- receive UR response
  local reverseConnectionSocket, err = self.reverseListener:accept()
  if reverseConnectionSocket == nil then
    error('[URDriver] Error: No response from robot received.')
  end

  self.reverseConnection = ReverseConnection(reverseConnectionSocket, MULT_JOINT, self.logger)
  self.logger.info('[URDriver] Reverse connection established!')
end


function URDriver:createTrajectoryHandler(traj, flush, waitCovergence, maxBuffering)
  if flush == nil then
    flush = true
  end
  if waitCovergence == nil then
    waitCovergence = true
  end
  establishReverseConnection(self)
  return TrajectoryHandler(
    self.ringSize,
    self.servoTime,
    self.realtimeState,
    self.reverseConnection,
    traj,
    flush,
    waitCovergence,
    maxBuffering,
    self.logger
  )
end


function URDriver:cancelCurrentTrajectory(abortMsg)
  if self.currentTrajectory ~= nil then
    self.logger.info('[URDriver] Cancelling trajectory execution.')
    local handler = self.currentTrajectory.handler
    if callAbortCallback then
      local traj = self.currentTrajectory.traj
      if traj.abort ~= nil then
        traj:abort(abortMsg or 'Canceled')        -- abort callback
      end
    end
    self.currentTrajectory = nil
    handler:cancel()
  end
end


local function dispatchTrajectory(self)
  local robotReady = self.realtimeState:isRobotReady()

  if not robotReady then
    if #self.trajectoryQueue > 0 then
      self.logger.warn('[URDriver] Aborting %d queued trajectories since robot is not ready.', #self.trajectoryQueue)
      -- Cancel all queued trajectories due to robot error.
      -- Otherwise possibly an unexpected execution of old trajectories would start when robot becomes ready again.
      -- Execution of current trajectory will be stopped anyways when robot is not ready (see end of this function)...
      for i,traj in ipairs(self.trajectoryQueue) do
        if traj.abort ~= nil then
          traj:abort('Robot not ready.')        -- abort callback
        end
      end
      self.trajectoryQueue = {}   -- clear
    end
  end

  if self.currentTrajectory == nil then
    if robotReady and #self.trajectoryQueue > 0 then    -- check if new trajectory is available

      establishReverseConnection(self)  -- try to establish reverse connection before accepting trajectory

      while #self.trajectoryQueue > 0 do
        local traj = table.remove(self.trajectoryQueue, 1)
        if traj.accept == nil or traj:accept() then   -- call optional accept callback
          local flush, waitCovergence = true, true
          local maxBuffering = self.ringSize

          if traj.flush ~= nil then
            flush = traj.flush
          end

          if traj.waitCovergence ~= nil then
            waitCovergence = traj.waitCovergence
          end

          if traj.maxBuffering ~= nil then
            maxBuffering = math.max(1, traj.maxBuffering)
          end

          if traj.time:nElement() == 1 then
            local time = torch.Tensor(2)
            time[1] = 0
            time[2] = traj.time[1]

            if not self.realtimeState:isValid() then
              self.logger.error('Dropping single point trajectory execution request since realtime state is not valid.')
              return
            end

            local pos = torch.zeros(2,6)
            pos[1] = self.realtimeState.q_actual
            pos[2] = traj.pos[1]

            local targetDistance = torch.norm(pos[1] - pos[2])
            if targetDistance > self.maxSinglePointTrajectoryDistance then
              self.logger.error(
                'Single point trajectory target lies too far away from current joint configuration (distance: %f; maxSinglePointTrajectoryDistance: %f).',
                targetDistance,
                self.maxSinglePointTrajectoryDistance
              )
              return
            end

            local vel = torch.zeros(2,6)
            -- vel[1] = self.realtimeState.qd_actual

            traj.time = time
            traj.pos = pos
            traj.vel = vel
            traj.acc = nil
          end

          self.currentTrajectory = {
            startTime = sys.clock(),     -- debug information
            traj = traj,
            handler = self:createTrajectoryHandler(traj, flush, waitCovergence, maxBuffering)
          }
          break
        end
      end

    elseif self.reverseConnection ~= nil then

      if robotReady then
        self.reverseConnection:idle()
        if self.reverseConnection:getIdleCycles() >= self.maxIdleCycles then
          self.logger.info('[URDriver] Robot was idle for %d cycles. Closing reverse connection. You can use freedrive now.', self.reverseConnection:getIdleCycles())
          self.reverseConnection:close()
          self.reverseConnection = nil
        end
      else
        self.logger.warn(
          '[URDriver] Robot is not ready (Robot mode: %s; Safety mode: %s). Closing reverse connection.',
          ur5.ROBOT_MODE.tostring(self.realtimeState.robot_mode),
          ur5.SAFETY_MODE.tostring(self.realtimeState.safety_mode)
        )
        self.reverseConnection:close()
        self.reverseConnection = nil
      end
    elseif #self.trajectoryQueue > 0 then
        self.logger.info(
          '[URDriver] Waiting for robot to become ready. (Robot mode: %s; Safety mode: %s)',
          ur5.ROBOT_MODE.tostring(self.realtimeState.robot_mode),
          ur5.SAFETY_MODE.tostring(self.realtimeState.safety_mode)
        )
    end
  end

  -- ensure first points are send to robot immediately after accepting trajectory execution
  if self.currentTrajectory ~= nil then     -- if we have an exsting trajectory

    local traj = self.currentTrajectory.traj
    local handler = self.currentTrajectory.handler

    -- check if trajectory execution is still desired (e.g. not canceled)
    if robotReady and (traj.proceed == nil or traj:proceed()) then

      -- execute main update call
      local ok, err = pcall(function() handler:update() end)
      if not ok then
        self.logger.warn('Exception during handler update: %s', err)
      end

      if not ok or handler.status < 0 then    -- error
        if traj.abort ~= nil then
          traj:abort()        -- abort callback
        end
        self.currentTrajectory = nil
      elseif handler.status == TrajectoryHandlerStatus.Completed then
        if traj.completed ~= nil then
          traj:completed()    -- completed callback
        end
        self.currentTrajectory = nil
      end

    else
      -- robot not ready or proceed callback returned false
      self:cancelCurrentTrajectory('Robot not ready or proceed callback returned false.')
    end
  end

end


local function driverCore(self)
  local ok, err = self:sync()
  if not ok then
    error(err)
  end
  dispatchTrajectory(self)
end


function URDriver:spin()
  if self.realtimeState:isRobotReady() then
    self.heartbeat:updateStatus(self.heartbeat.GO, "")
  else
    self.heartbeat:updateStatus(self.heartbeat.INTERNAL_ERROR, "Robot is not ready.")
  end

  if self.realtimeStream:getState() == RealtimeStreamState.Disconnected then
    self.logger.info('RealtimeStream not connected, trying to connect...')
    if self.realtimeStream:connect(self.hostname, self.realtimePort) then
      self.logger.info('Connected.')

      -- create and bind socket to accept reverse connections
      local reverseListener = socket.tcp()
      local clientIp, err = self.realtimeStream:getSocket():getsockname()    -- get local ip for reverse channel
      if not clientIp then
        error('Could not get local IP address for reverse listener.')
      end
      reverseListener:bind(clientIp, self.reverserealtimePort)
      reverseListener:listen(1)
      reverseListener:settimeout(2, 't')
      self.reverseListener = reverseListener
    end
  end

  local ok, err = true, nil
  if self.realtimeStream:getState() == RealtimeStreamState.Connected then

    -- sync
    ok, err = pcall(function() driverCore(self) end)
    if not ok then
      self.logger.error("[URDRiver] Spin error: " .. err)
    end

    --[[
    local dbgState = string.format('Robot mode: %s; Safety mode: %s;',
      ur5.ROBOT_MODE.tostring(self.realtimeState.robot_mode),
      ur5.SAFETY_MODE.tostring(self.realtimeState.safety_mode)
    )
    self.logger.debug(dbgState)
    ]]
  end

  if not ok or self.realtimeStream:getState() == RealtimeStreamState.Error or
    (self.reverseConnection ~= nil and self.reverseConnection.error) then

    -- abort current trajectory
    if self.currentTrajectory then
      local traj = self.currentTrajectory.traj
      if traj.abort ~= nil then
        traj:abort()
      end
      self.currentTrajectory = nil
    end

    -- realtime stream is in error state, recreate stream object

    self.logger.warn('Closing robot communication sockets and reconnecting. (RT state: %d)', self.realtimeStream:getState())

    if self.reverseConnection ~= nil then
      if self.reverseConnection.error then
        self.logger.warn('ReverseConnection was in error state.')
      end
      self.reverseConnection:close()
      self.reverseConnection = nil
    end

    if self.reverseListener ~= nil then
      self.reverseListener:close()
      self.reverseListener = nil
    end

    self.realtimeStream:close()
    self.realtimeState:invalidate()
    self.realtimeStream = RealtimeStream(self.realtimeState)
  end

end


function URDriver:shutdown()
  self.realtimeStream:close()
end