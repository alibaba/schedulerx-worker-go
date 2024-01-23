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
	"time"
)

var (
	htOnce     sync.Once
	timeHolder *healthTimeHolder
)

type healthTimeHolder struct {
	// latestServerHeartbeatTime is the last successful heartbeat detection time of the server
	latestServerHeartbeatTime int64
}

func GetHealthTimeHolder() *healthTimeHolder {
	htOnce.Do(func() {
		timeHolder = newHealthTimeHolder()
	})
	return timeHolder
}

func newHealthTimeHolder() *healthTimeHolder {
	return &healthTimeHolder{
		latestServerHeartbeatTime: time.Now().UnixMilli(),
	}
}

// ResetServerHeartbeatTime reset the last heartbeat detection with server success time
func (h *healthTimeHolder) ResetServerHeartbeatTime() {
	h.latestServerHeartbeatTime = time.Now().UnixMilli()
}

// GetServerHeartbeatMsInterval is the interval between the last successful heartbeat detection on the server and now
func (h *healthTimeHolder) GetServerHeartbeatMsInterval() int64 {
	return time.Now().UnixMilli() - h.latestServerHeartbeatTime
}

// IsServerHeartbeatHealthTimeout check if the time since the last successful heartbeat detection of the server is greater than the given interval time
func (h *healthTimeHolder) IsServerHeartbeatHealthTimeout(seconds int64) bool {
	return (time.Now().UnixMilli() - h.latestServerHeartbeatTime) > seconds*1000
}
