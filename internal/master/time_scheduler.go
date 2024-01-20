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
	timeQueue *TimeQueue
	inited    bool // no concurrency safe
	lock      sync.RWMutex
}

func newTimeScheduler() *TimeScheduler {
	return &TimeScheduler{
		timeQueue: NewTimeQueue(),
	}
}

func (s *TimeScheduler) init() {
	if !s.isInited() {
		go s.timeScan()
		logger.Infof("TimeScanThread started")
		s.setInited(true)
	}
}

func (s *TimeScheduler) timeScan() {
	for {
		now := time.Now().UnixMilli()
		for !s.timeQueue.IsEmpty() {
			if planEntry := s.timeQueue.Peek(); planEntry != nil && planEntry.ScheduleTimeStamp() <= now {
				logger.Infof("%+v time ready", planEntry)
				// 1. remove this from planQueue
				s.timeQueue.RemoveHeader()
				// 2. start this time based job
				s.submitPlan(planEntry)
			} else {
				break
			}
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (s *TimeScheduler) isInited() bool {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.inited
}

func (s *TimeScheduler) setInited(flag bool) {
	s.lock.Lock()
	s.inited = flag
	s.lock.Unlock()
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
