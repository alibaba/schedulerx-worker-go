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
	"github.com/alibaba/schedulerx-worker-go/internal/utils"
	"github.com/alibaba/schedulerx-worker-go/logger"
)

// TimeQueue Time queue sorted by scheduling time and task priority
type TimeQueue struct {
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
	if !q.timeSet.Contains(timePlanEntry.UniqueID()) {
		q.timeSet.Add(timePlanEntry.UniqueID())
		q.timeQueue.PushItem(timePlanEntry)
		logger.Infof("timeQueue add plan=%s", timePlanEntry)
	} else {
		logger.Warnf("plan=%s is existed in timeQueue", timePlanEntry)
	}
}

func (q *TimeQueue) Remove(jobInstanceId int64) {
	q.timeQueue.RemoveBy(func(item utils.ComparatorItem) bool {
		planEntry := item.(*TimePlanEntry)
		if jobInstanceId == planEntry.jobInstanceId {
			// found item matched, remove from timeSet
			q.timeSet.Remove(planEntry.UniqueID())
			logger.Infof("planEntry=%s removed, event.getTriggerType() != null", planEntry)
			return true
		}
		return false
	})
}

// Peek return the head of this queue, or returns null if this queue is empty.
func (q *TimeQueue) Peek() *TimePlanEntry {
	if item := q.timeQueue.Peek(); item != nil {
		return item.(*TimePlanEntry)
	}
	return nil
}

// Pop removes the head of this queue.
func (q *TimeQueue) Pop() *TimePlanEntry {
	planEntry := q.timeQueue.PopItem().(*TimePlanEntry)
	q.timeSet.Remove(planEntry.UniqueID())
	return planEntry
}

func (q *TimeQueue) IsEmpty() bool {
	return q.timeQueue.Len() == 0
}

func (q *TimeQueue) Size() int {
	return q.timeQueue.Len()
}

func (q *TimeQueue) Clear() {
	q.timeSet.Clear()
	q.timeQueue.Clear()
}
