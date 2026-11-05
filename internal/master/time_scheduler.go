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

package master

import (
	"sync"
	"time"

	"github.com/alibaba/schedulerx-worker-go/logger"
)

var (
	tsOnce        sync.Once
	timeScheduler *TimeScheduler
)

func GetTimeScheduler() *TimeScheduler {
	tsOnce.Do(func() {
		timeScheduler = newTimeScheduler()
	})
	return timeScheduler
}

// TimeScheduler is the client time scheduler, mainly used for second-level task scheduling.
type TimeScheduler struct {
	timeQueue  *TimeQueue
	once       sync.Once
	dispatchMu sync.Mutex
	dispatches map[*SecondJobUpdateInstanceStatusHandler][]*TimePlanEntry
}

func newTimeScheduler() *TimeScheduler {
	return &TimeScheduler{
		timeQueue:  NewTimeQueue(),
		dispatches: make(map[*SecondJobUpdateInstanceStatusHandler][]*TimePlanEntry),
	}
}

func (s *TimeScheduler) init() {
	s.once.Do(func() {
		go s.timeScan()
		logger.Infof("TimeScanThread started")
	})
}

func (s *TimeScheduler) timeScan() {
	for {
		now := time.Now().UnixMilli()
		for {
			planEntry := s.timeQueue.PopDue(now)
			if planEntry == nil {
				break
			}
			logger.Infof("%s time ready", planEntry)
			// PopDue releases the queue lock before asynchronous lifecycle work.
			s.dispatchPlan(planEntry)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (s *TimeScheduler) add(planEntry *TimePlanEntry) {
	s.timeQueue.Add(planEntry)
}

func (s *TimeScheduler) remove(jobInstanceId int64) {
	s.timeQueue.Remove(jobInstanceId)
}

func (s *TimeScheduler) submitPlan(planEntry *TimePlanEntry) {
	planEntry.Handler().triggerNewCycle()
}

// dispatchPlan admits work without waiting for the selected worker. A single
// runner owns each handler's FIFO: one owner keeps its cycle ordering while
// unrelated jobs and a replacement owner never wait for its remote reply.
func (s *TimeScheduler) dispatchPlan(plan *TimePlanEntry) {
	if plan == nil {
		return
	}
	owner := plan.Handler()
	s.dispatchMu.Lock()
	_, running := s.dispatches[owner]
	s.dispatches[owner] = append(s.dispatches[owner], plan)
	s.dispatchMu.Unlock()
	if !running {
		go s.runDispatch(owner)
	}
}

func (s *TimeScheduler) runDispatch(owner *SecondJobUpdateInstanceStatusHandler) {
	defer func() {
		// Also runs for runtime.Goexit in user hooks. Never leave a dead runner
		// owning this handler's queue or discard another already-admitted plan.
		// Panics retain the existing fail-fast behavior; swallowing one can leave
		// its claimed cycle in the pool without a completion or next plan.
		s.dispatchMu.Lock()
		if len(s.dispatches[owner]) == 0 {
			delete(s.dispatches, owner)
		} else {
			go s.runDispatch(owner)
		}
		s.dispatchMu.Unlock()
	}()
	for {
		s.dispatchMu.Lock()
		plans := s.dispatches[owner]
		if len(plans) == 0 {
			s.dispatchMu.Unlock()
			return
		}
		plan := plans[0]
		plans[0] = nil
		s.dispatches[owner] = plans[1:]
		s.dispatchMu.Unlock()
		s.submitPlan(plan)
	}
}
