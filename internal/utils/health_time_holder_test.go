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
	"testing"
	"time"
)

func TestHeartbeatExpirationAndSuccessfulReset(t *testing.T) {
	h := newHealthTimeHolder()
	h.latestServerHeartbeatTime.Store(time.Now().Add(-31 * time.Second).UnixMilli())
	if !h.IsServerHeartbeatHealthTimeout(30) {
		t.Fatal("expired server heartbeat treated as healthy")
	}
	if h.GetServerHeartbeatMsInterval() < 30000 {
		t.Fatal("heartbeat interval no longer uses milliseconds")
	}
	h.ResetServerHeartbeatTime()
	if h.IsServerHeartbeatHealthTimeout(30) {
		t.Fatal("successful heartbeat did not restore health")
	}
	if h.GetServerHeartbeatMsInterval() < 0 {
		t.Fatal("reset created future heartbeat")
	}
}

func TestHeartbeatConcurrentResetAndHealthChecks(t *testing.T) {
	h := newHealthTimeHolder()
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(3)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			h.ResetServerHeartbeatTime()
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			h.IsServerHeartbeatHealthTimeout(30)
		}
	}()
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 1000; i++ {
			h.GetServerHeartbeatMsInterval()
		}
	}()
	close(start)
	wg.Wait()
	if h.IsServerHeartbeatHealthTimeout(30) {
		t.Fatal("reset heartbeat is unhealthy after concurrent checks")
	}
}
