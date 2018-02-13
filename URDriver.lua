local torch = require 'torch'
local sys = require 'sys'
local ffi = require 'ffi'
require 'URStream'
require 'ReverseConnection'
require 'TrajectoryHandler'
require 'RealtimeState'
local ur = require 'ur_env'
local printf = ur.printf


local URStreamState = ur.URStreamState
local TrajectoryHandlerStatus = ur.TrajectoryHandlerStatus


-- constants
local MULT_JOINT = 1000000
local MAX_SYNC_READ_TRIES = 250
local DEFAULT_MAX_IDLE_CYCLES = 250      -- number of idle sync-cyles before reverse connection shutdown (that re-enables freedrive)
local DEFAULT_HOSTNAME = 'ur5'
local PRIMARY_CLIENT_PORT = 30001
local SECONDARY_CLIENT_PORT = 30002
local DEFAULT_REALTIME_PORT = 30003
local DEFAULT_REVERSENAME = nil
local DEFAULT_REVERSE_PORT = 0
local DEFAULT_RING_SIZE = 64
local DEFAULT_PATH_TOLERANCE = math.pi / 10
local DEFAULT_LOOKAHEAD = 0.01
local DEFAULT_SERVO_TIME = 0.008
local DEFAULT_GAIN = 1200
local DEFAULT_MAX_SINGLE_POINT_TRAJECTORY_DISTANCE = 0.5      -- max allowed distance of single point trajectory target relative to current joint pos
local DEFAULT_MAX_CONVERGENCE_CYCLES = 150
local DEFAULT_GOAL_CONVERGENCE_POSITION_THRESHOLD = 0.005 -- in rad
local DEFAULT_GOAL_CONVERGENCE_VELOCITY_THRESHOLD = 0.01 -- in rad/s

local MAX_CLIENT_INTERFACE_PACKET_SIZE = 4096
local MAX_REALTIME_STREAM_PACKET_SIZE = 1060

local CB2_SCRIPT_TEMPLATE_FILENAME = 'driverCB2.urscript'
local CB3_SCRIPT_TEMPLATE_FILENAME = 'driverCB3.urscript'


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


local function loadScriptTemplate(self)
  assert(self.robot_version ~= nil, 'Robot version is unknown')

  local scriptFilename
  if self.robot_version.major < 3 then
    scriptFilename = CB2_SCRIPT_TEMPLATE_FILENAME
  else
    scriptFilename = CB3_SCRIPT_TEMPLATE_FILENAME
  end

  -- read script template
  local f = assert(io.open(scriptFilename, "r"))
  local scriptTemplate = f:read("*all")
  f:close()

  return scriptTemplate
end


local function createRealtimeStream(self)
  local function handleRealtimeStreamPacket(reader)
    self.realtimeState:read(reader, self.robot_version)
  end
  self.realtimeStream = URStream(handleRealtimeStreamPacket, MAX_REALTIME_STREAM_PACKET_SIZE, self.logger)
end


local function initializeRobotMode(self)
  self.robot_mode = {
    isRobotConnected = false,
    isRealRobotEnabled = false,
    isPowerOnRobot = false,
    isEmergencyStopped = false,
    isSecurityStopped = false,
    isProgramRunning = false,
    isProgramPaused = false,
    robotMode = 0
  }
end


local function readRobotModeData(self, reader)
  local timestamp = reader:readUInt64()
  local mode = self.robot_mode
  mode.isRobotConnected = reader:readUInt8() ~= 0
  mode.isRealRobotEnabled = reader:readUInt8() ~= 0
  mode.isPowerOnRobot = reader:readUInt8() ~= 0
  mode.isEmergencyStopped = reader:readUInt8() ~= 0
  mode.isSecurityStopped = reader:readUInt8() ~= 0
  mode.isProgramRunning = reader:readUInt8() ~= 0
  mode.isProgramPaused = reader:readUInt8() ~= 0
  mode.robotMode = reader:readUInt8()
end


local function readRobotState(self, reader)
  -- decode all packages

  while reader.offset < reader.length do

    local begin_of_package = reader.offset
    local package_size = reader:readInt32()
    if package_size < 5 or begin_of_package + package_size > reader.length then
      self.logger.error('Invalid package size in robot state: %d (begin of package: %d; buffer length: %d)', package_size, begin_of_package, reader.length)
      break
    end
    local end_of_package = begin_of_package + package_size
    local package_type = reader:readUInt8()

    if package_type == 0 then
      readRobotModeData(self, reader)
    end

    reader:setOffset(end_of_package)

  end

end


local function readVersionMessage(self, reader)
  local project_name_length = reader:readUInt8()

  local name_offset = reader.offset
  reader:setOffset(name_offset + project_name_length)
  local project_name = ffi.string(reader.data + name_offset, project_name_length)

  local major = reader:readUInt8()
  local minor = reader:readUInt8()
  local revision = reader:readInt32()

  self.robot_version = {
    major, minor, revision, project_name,
    major = major,
    minor = minor,
    revision = revision,
    project_name = project_name
  }

  self.logger.info("Robot version: %s %d.%d rev. %d", project_name, major, minor, revision)
end


local function readRobotMessage(self, reader)
  local timestamp = reader:readUInt64()
  local source = reader:readUInt8()
  local robotMessageType = reader:readUInt8()
  if robotMessageType == 3 then
    readVersionMessage(self, reader)
  end
end


local function createClientStream(self)
  initializeRobotMode(self)

  local function handleClientInterfacePacket(reader)
    local message_size = reader:readUInt32()
    local message_type = reader:readUInt8()
    if message_type == 16 then
      readRobotState(self, reader)
    elseif message_type == 20 then
      readRobotMessage(self, reader)
    end
  end
  self.clientStream = URStream(handleClientInterfacePacket, MAX_CLIENT_INTERFACE_PACKET_SIZE, self.logger)
end


function URDriver:__init(cfg, logger, heartbeat)
  self.logger = logger or ur.DEFAULT_LOGGER
  self.heartbeat = heartbeat

  -- apply configuration
  self.hostname = cfg.hostname or DEFAULT_HOSTNAME
  self.realtimePort = cfg.realtimePort or DEFAULT_REALTIME_PORT
  self.reverseName = cfg.reverseName or DEFAULT_REVERSENAME
  self.reversePort = cfg.reversePort or DEFAULT_REVERSE_PORT
  self.lookahead = cfg.lookahead or DEFAULT_LOOKAHEAD
  self.gain = cfg.gain or DEFAULT_GAIN
  self.servoTime = cfg.servoTime or DEFAULT_SERVO_TIME
  self.pathTolerance = cfg.pathTolerance or DEFAULT_PATH_TOLERANCE
  self.ringSize = cfg.ringSize or DEFAULT_RING_SIZE
  self.maxIdleCycles = cfg.maxIdleCycles or DEFAULT_MAX_IDLE_CYCLES
  self.maxSinglePointTrajectoryDistance = cfg.maxSinglePointTrajectoryDistance or DEFAULT_MAX_SINGLE_POINT_TRAJECTORY_DISTANCE
  self.jointNamePrefix = cfg.jointNamePrefix
  self.maxConvergenceCycles = cfg.maxConvergenceCycles or DEFAULT_MAX_CONVERGENCE_CYCLES
  self.goalPositionThreshold = cfg.goalPositionThreshold or DEFAULT_GOAL_CONVERGENCE_POSITION_THRESHOLD
  self.goalVelocityThreshold = cfg.goalVelocityThreshold or DEFAULT_GOAL_CONVERGENCE_VELOCITY_THRESHOLD

  self.realtimeState = RealtimeState()

  createClientStream(self)   -- get robot version info
  createRealtimeStream(self)          -- get robot mode and joint details

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
        return false, string.format("Waypoint %d: Position %f of joint '%s' lies outside control limits.", i, pos[{i,j}], ur.JOINT_NAMES[j])
      end
      if vel[{i,j}] < JOINT_VELOCITY_LIMITS[{j,1}] or vel[{i,j}] > JOINT_VELOCITY_LIMITS[{j,2}] then
        return false, string.format("Waypoint %d: Velocity %f of joint '%s' lies outside control limits.", i, vel[{i,j}], ur.JOINT_NAMES[j])
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


function URDriver:poll()
  if self.clientStream:getState() == URStreamState.Connected then
    self.clientStream:read()
  end

  if self.realtimeStream:getState() ~= URStreamState.Connected then
    return nil, '[URDriver] Realtime stream not connected.'
  end

  if self.realtimeStream:read() then

    -- execute sync callbacks (e.g. to publish joint states)
    for j,fn in ipairs(self.syncCallbacks) do
      fn(self)
    end

    return true
  end

  if self.realtimeStream.readTimeouts > MAX_SYNC_READ_TRIES then
    return nil, '[URDriver] Sync timeout.'
  end

  return false
end


function URDriver:sync()
  while true do
    local ok, err = self:poll(true)
    if ok then
      return ok, 'ok'
    elseif ok == nil then
      return ok, err
    end
  end
end


local function establishReverseConnection(self)
  if self.reverseConnection ~= nil then
    return
  end

  if self.reverseListener == nil then
    error('[URDriver] Reverse listener not ready.')
  end

  if self.robot_version == nil then
    error('[URDriver] Robot version unknown')
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
    IP            = self.reverseName or ip,
    Port          = port,
    ServoTime     = self.servoTime,
    LookAhead     = self.lookahead,
    Gain          = self.gain,
    PathTolerance = self.pathTolerance,
    RingSize      = self.ringSize,
    RingZeros     = generateZeros(self.ringSize * 7)
  }

  local scriptTemplate = loadScriptTemplate(self)
  local s = fillTemplate(scriptTemplate, variables)
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
    self.maxConvergenceCycles,
    self.goalPositionThreshold,
    self.goalVelocityThreshold,
    self.logger
  )
end


function URDriver:cancelCurrentTrajectory()
  if self.currentTrajectory == nil then
    return
  end

  self.logger.info('[URDriver] Cancelling trajectory execution.')
  local traj = self.currentTrajectory.traj
  local handler = self.currentTrajectory.handler

  local robotReady = self:isRobotReady()
  if not robotReady then
    -- if robot is not ready we abort the trajectory immediately
    if traj.abort ~= nil then
      traj:abort()        -- abort callback
    end
    self.currentTrajectory = nil
  else
    -- otherwise we try to bring the robot gracefully to a standstill
    handler:cancel()
    if traj.cancel ~= nil then
      traj:cancel()        -- cancel callback (e.g. enter canel requested state)
    end
  end
end


local function dispatchTrajectory(self)
  local robotReady = self:isRobotReady()

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
          ur.ROBOT_MODE.tostring(self.realtimeState.robot_mode),
          ur.SAFETY_MODE.tostring(self.realtimeState.safety_mode)
        )
        self.reverseConnection:close()
        self.reverseConnection = nil
      end
    elseif #self.trajectoryQueue > 0 then
        self.logger.info(
          '[URDriver] Waiting for robot to become ready. (Robot mode: %s; Safety mode: %s)',
          ur.ROBOT_MODE.tostring(self.realtimeState.robot_mode),
          ur.SAFETY_MODE.tostring(self.realtimeState.safety_mode)
        )
    end
  end

  -- ensure first points are send to robot immediately after accepting trajectory execution
  if self.currentTrajectory ~= nil then     -- if we have an exsting trajectory

    local traj = self.currentTrajectory.traj
    local handler = self.currentTrajectory.handler

    -- check if trajectory execution is still desired or if we are cancelling (waiting for robot stop)
    if robotReady and (handler.status == TrajectoryHandlerStatus.Cancelling or traj.proceed == nil or traj:proceed()) then

      -- execute main update call
      local ok, err = pcall(function() handler:update() end)
      if not ok then
        self.logger.warn('Exception during handler update: %s', err)
      end

      if not ok or handler.status < 0 then    -- error
        if traj.abort ~= nil then
          local msg
          if handler.status == TrajectoryHandlerStatus.Canceled then
            msg = 'Robot was stopped due to trajectory cancel request.'
          elseif handler.status == TrajectoryHandlerStatus.ConnectionLost then
            msg = 'Connection to robot was lost.'
          elseif handler.status == TrajectoryHandlerStatus.ProtocolError then
            msg = 'A robot communication error occured (ProtocolError).'
          end
          traj:abort(msg)        -- abort callback
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
      self:cancelCurrentTrajectory()
    end
  end

end


local function driverCore(self)
  local ok, err = self:poll(false)
  if ok == nil then
    error(err)
  end
  dispatchTrajectory(self)
end


function URDriver:isRobotReady()
  local client_state = self.clientStream:getState()
  local realtime_state = self.realtimeStream:getState()

  if client_state ~= URStreamState.Connected then
    return false, string.format('Client network interface not connected (%s).', URStreamState[client_state])
  end

  if realtime_state ~= URStreamState.Connected then
    return false, string.format('Realtime stream not connected (%s).', URStreamState[realtime_state])
  end

  if self.robot_version == nil then
    return false, 'Still waiting for robot version message.'
  end

  if self.robot_mode.isEmergencyStopped then
    return false, 'Emergency stopped'
  end

  if self.robot_mode.isSecurityStopped then
    return false, 'Security stopped'
  end

  if not self.robot_mode.isRobotConnected then
    return false, 'Robot not connected'
  end

  if not self.robot_mode.isPowerOnRobot then
    return false, 'Robot power off'
  end

  if not self.realtimeState:isRobotReady() then
    return false, 'Robot is not ready.'
  end

  return true, string.format("%s %d.%d rev %d",
    self.robot_version.project_name,
    self.robot_version.major,
    self.robot_version.minor,
    self.robot_version.revision)
end


function URDriver:spin()
  local ready, status_text = self:isRobotReady()
  if ready then
    self.heartbeat:updateStatus(self.heartbeat.GO, status_text)
  else
    self.heartbeat:updateStatus(self.heartbeat.INTERNAL_ERROR, status_text)
  end

  if self.clientStream:getState() == URStreamState.Disconnected then
    self.logger.info('client not connected, trying to connect...')
    if self.clientStream:connect(self.hostname, PRIMARY_CLIENT_PORT) then
      self.logger.info('client connected.')
    end
  end

  if self.realtimeStream:getState() == URStreamState.Disconnected then
    self.logger.info('RealtimeStream not connected, trying to connect...')
    if self.realtimeStream:connect(self.hostname, self.realtimePort) then
      self.logger.info('RealtimeStream connected.')

      -- create and bind socket to accept reverse connections
      local reverseListener = socket.tcp()
      local clientIp, err = self.realtimeStream:getSocket():getsockname()    -- get local ip for reverse channel
      if not clientIp then
        error('Could not get local IP address for reverse listener.')
      end
      reverseListener:bind(clientIp, self.reversePort)
      reverseListener:listen(1)
      reverseListener:settimeout(2, 't')
      self.reverseListener = reverseListener
    end
  end

  local ok, err = true, nil
  if self.realtimeStream:getState() == URStreamState.Connected then

    -- sync
    ok, err = pcall(function() driverCore(self) end)
    if not ok then
      self.logger.error("[URDRiver] Spin error: " .. err)
    end

    --[[
    local dbgState = string.format('Robot mode: %s; Safety mode: %s;',
      ur.ROBOT_MODE.tostring(self.realtimeState.robot_mode),
      ur.SAFETY_MODE.tostring(self.realtimeState.safety_mode)
    )
    self.logger.debug(dbgState)
    ]]
  end

  if self.clientStream:getState() == URStreamState.Error then
    self.robot_version = nil
    self.realtimeState:invalidate()
    self.clientStream:close()
    createClientStream(self)
  end

  if not ok or self.realtimeStream:getState() == URStreamState.Error or
    (self.reverseConnection ~= nil and self.reverseConnection.error) then

    -- abort current trajectory
    if self.currentTrajectory ~= nil then
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

    self.realtimeStream = URStream(self.realtimeStream.packet_handler_fn, 1060, self.logger)
  end

end


function URDriver:shutdown()
  self.realtimeStream:close()
  self.clientStream:close()
end
