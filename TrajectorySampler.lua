--[[
TrajectorySampler.lua

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


local TrajectorySampler = torch.class('TrajectorySampler')


local JUMP_ERROR_THRESHOLD = math.pi / 10


-- Returns joints positions at time 't'
local function interpolateCubic(t, t0, t1, p0, p1, v0, v1)
  local dt = t1-t0
  if dt < 1e-6 then
    return p1, p0:clone():zero()
  end
  local pos = p0:clone()
  local vel = p0:clone()
  for i = 1,p0:size(1) do
    local a = p0[i]
    local b = v0[i]
    local c = (-3 * p0[i] + 3 * p1[i] - 2 * dt * v0[i] - dt * v1[i]) / dt^2
    local d = ( 2 * p0[i] - 2 * p1[i] +     dt * v0[i] + dt * v1[i]) / dt^3
    pos[i] = a + b * t +     c * t^2 +     d * t^3
    vel[i] =     b     + 2 * c * t   + 3 * d * t^2
  end
  return pos, vel
end


local function interpolateAt(t, time, pos, vel)
  local j = 1
  for i=1,time:size(1) do
    if time[i] > t then
      break
    end
    j = i
  end
  local k = math.min(j+1, time:size(1))

  local t0,t1 = time[j],time[k]
  local p0,v0 = pos[j],vel[j]
  local p1,v1 = pos[k],vel[k]
  return interpolateCubic(t-t0, t0, t1, p0, p1, v0, v1)
end


function TrajectorySampler:__init(traj, step)
  self.traj = traj
  self.qlast = traj.pos[1]
  self.tend = traj.time[traj.time:size(1)]  -- end time of trajectory
  self.step = step
  self.t = 0
end


function TrajectorySampler:atEnd()
  return self.t >= self.tend + self.step
end


function TrajectorySampler:getCurrentTime(t)
  return self.t
end


function TrajectorySampler:evaluateAt(t)
  t = math.max(0, math.min(t or self.t, self.tend))    -- clamp t to valid range
  local traj = self.traj
  local q,qd = interpolateAt(t, traj.time, traj.pos, traj.vel)
  return q,qd
end


function TrajectorySampler:getGoalPosition()
  return self:evaluateAt(self.tend)
end


function TrajectorySampler:generateNextPoints(maxCount)
  local pts = {}
  for i=1,maxCount do
    local q,qd = self:evaluateAt(self.t)

    local delta = torch.norm(q - self.qlast)
    if delta > JUMP_ERROR_THRESHOLD then
      error(string.format('[TrajectorySampler] Error: Unexpected set point jump of %f rad at time %fs.', delta, self.t))
    end
    self.t = self.t + self.step
    pts[#pts+1] = q
    self.qlast = q
  end
  return pts
end
