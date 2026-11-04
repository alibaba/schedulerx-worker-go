/*
 * Copyright (c) 2023 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package utils

import (
	"sync"
	"sync/atomic"
	"time"
)

var (
	htOnce     sync.Once
	timeHolder *healthTimeHolder
)

type healthTimeHolder struct {
	// latestServerHeartbeatTime is the last successful heartbeat detection time of the server
	latestServerHeartbeatTime atomic.Int64
	// Each discovered group has its own failure window. Entries are retained for
	// the worker lifetime so a failed group's health check cannot renew its grace.
	groupServerHeartbeatTimes sync.Map // map[string]*atomic.Int64
}

func GetHealthTimeHolder() *healthTimeHolder {
	htOnce.Do(func() {
		timeHolder = newHealthTimeHolder()
	})
	return timeHolder
}

func newHealthTimeHolder() *healthTimeHolder {
	h := &healthTimeHolder{}
	h.latestServerHeartbeatTime.Store(time.Now().UnixMilli())
	return h
}

// ResetServerHeartbeatTime reset the last heartbeat detection with server success time
func (h *healthTimeHolder) ResetServerHeartbeatTime() {
	h.latestServerHeartbeatTime.Store(time.Now().UnixMilli())
}

// GetServerHeartbeatMsInterval is the interval between the last successful heartbeat detection on the server and now
func (h *healthTimeHolder) GetServerHeartbeatMsInterval() int64 {
	return time.Now().UnixMilli() - h.latestServerHeartbeatTime.Load()
}

// IsServerHeartbeatHealthTimeout check if the time since the last successful heartbeat detection of the server is greater than the given interval time
func (h *healthTimeHolder) IsServerHeartbeatHealthTimeout(seconds int64) bool {
	return h.GetServerHeartbeatMsInterval() > seconds*1000
}

// InitGroupServerHeartbeatTime starts a new group's grace window without
// renewing an existing group. Groups discovered later do not inherit worker age.
func (h *healthTimeHolder) InitGroupServerHeartbeatTime(groupID string) {
	h.groupServerHeartbeatTime(groupID)
}

// ResetGroupServerHeartbeatTime records a successful heartbeat for only this
// group. Empty group IDs retain the legacy, unscoped health behavior.
func (h *healthTimeHolder) ResetGroupServerHeartbeatTime(groupID string) {
	h.groupServerHeartbeatTime(groupID).Store(time.Now().UnixMilli())
}

// GetGroupServerHeartbeatMsInterval returns milliseconds since this group's
// last success, or since the group was first observed if it has not succeeded.
func (h *healthTimeHolder) GetGroupServerHeartbeatMsInterval(groupID string) int64 {
	return time.Now().UnixMilli() - h.groupServerHeartbeatTime(groupID).Load()
}

// IsGroupServerHeartbeatHealthTimeout checks this group's own failure window.
func (h *healthTimeHolder) IsGroupServerHeartbeatHealthTimeout(groupID string, seconds int64) bool {
	return h.GetGroupServerHeartbeatMsInterval(groupID) > seconds*1000
}

func (h *healthTimeHolder) groupServerHeartbeatTime(groupID string) *atomic.Int64 {
	if groupID == "" {
		return &h.latestServerHeartbeatTime
	}
	if current, ok := h.groupServerHeartbeatTimes.Load(groupID); ok {
		return current.(*atomic.Int64)
	}
	initial := new(atomic.Int64)
	initial.Store(time.Now().UnixMilli())
	current, _ := h.groupServerHeartbeatTimes.LoadOrStore(groupID, initial)
	return current.(*atomic.Int64)
}
