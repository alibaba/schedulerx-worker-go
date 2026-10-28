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

	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

// TimeQueue Time queue sorted by scheduling time and task priority
type TimeQueue struct {
	// mu owns both membership and heap operations. Logging and dispatch run outside it.
	mu        sync.Mutex
	timeSet   *utils.ConcurrentSet[string] // Set<TimePlanEntry>
	timeQueue *utils.PriorityQueue         // Queue<TimePlanEntry> priority queue, sorted from small to large by scheduling time
}

func NewTimeQueue() *TimeQueue {
	return &TimeQueue{
		timeSet:   utils.NewConcurrentSet[string](),
		timeQueue: utils.NewPriorityQueue(100),
	}
}

func (q *TimeQueue) Add(timePlanEntry *TimePlanEntry) {
	exists := func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		if q.timeSet.Contains(timePlanEntry.UniqueID()) {
			return true
		}
		q.timeSet.Add(timePlanEntry.UniqueID())
		q.timeQueue.PushItem(timePlanEntry)
		return false
	}()
	if exists {
		logger.Warnf("plan=%s is existed in timeQueue", timePlanEntry)
	} else {
		logger.Infof("timeQueue add plan=%s", timePlanEntry)
	}
}

func (q *TimeQueue) Remove(jobInstanceID int64) {
	removed := func() []*TimePlanEntry {
		q.mu.Lock()
		defer q.mu.Unlock()
		var plans []*TimePlanEntry
		q.timeQueue.RemoveBy(func(item utils.ComparatorItem) bool {
			planEntry := item.(*TimePlanEntry)
			if jobInstanceID == planEntry.jobInstanceId {
				q.timeSet.Remove(planEntry.UniqueID())
				plans = append(plans, planEntry)
				return true
			}
			return false
		})
		return plans
	}()
	for _, planEntry := range removed {
		logger.Infof("planEntry=%s removed, event.getTriggerType() != null", planEntry)
	}
}

// Peek returns the head of this queue, or nil if the queue is empty.
func (q *TimeQueue) Peek() *TimePlanEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	if item := q.timeQueue.Peek(); item != nil {
		return item.(*TimePlanEntry)
	}
	return nil
}

// Pop removes the head of this queue, or returns nil if the queue is empty.
func (q *TimeQueue) Pop() *TimePlanEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.popLocked()
}

// PopDue atomically checks the deadline and removes the earliest due plan.
// Remove wins if it deletes a plan before this claim; a later removal cannot
// revoke a plan that has already been claimed for dispatch.
func (q *TimeQueue) PopDue(now int64) *TimePlanEntry {
	q.mu.Lock()
	defer q.mu.Unlock()
	item := q.timeQueue.Peek()
	if item == nil || item.(*TimePlanEntry).ScheduleTimeStamp() > now {
		return nil
	}
	return q.popLocked()
}

func (q *TimeQueue) popLocked() *TimePlanEntry {
	if q.timeQueue.Len() == 0 {
		return nil
	}
	planEntry := q.timeQueue.PopItem().(*TimePlanEntry)
	q.timeSet.Remove(planEntry.UniqueID())
	return planEntry
}

func (q *TimeQueue) IsEmpty() bool {
	return q.Size() == 0
}

func (q *TimeQueue) Size() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.timeQueue.Len()
}

func (q *TimeQueue) Clear() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.timeSet.Clear()
	q.timeQueue.Clear()
}
