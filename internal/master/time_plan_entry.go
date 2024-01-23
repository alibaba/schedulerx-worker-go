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
)

var _ utils.ComparatorItem = &TimePlanEntry{}

type TimePlanEntry struct {
	jobInstanceId     int64
	scheduleTimeStamp int64
	handler           *secondJobUpdateInstanceStatusHandler
}

func NewTimePlanEntry(jobInstanceId int64, scheduleTimeStamp int64, handler *secondJobUpdateInstanceStatusHandler) *TimePlanEntry {
	return &TimePlanEntry{jobInstanceId: jobInstanceId, scheduleTimeStamp: scheduleTimeStamp, handler: handler}
}

func (t *TimePlanEntry) JobInstanceId() int64 {
	return t.jobInstanceId
}

func (t *TimePlanEntry) SetJobInstanceId(jobInstanceId int64) {
	t.jobInstanceId = jobInstanceId
}

func (t *TimePlanEntry) ScheduleTimeStamp() int64 {
	return t.scheduleTimeStamp
}

func (t *TimePlanEntry) SetScheduleTimeStamp(scheduleTimeStamp int64) {
	t.scheduleTimeStamp = scheduleTimeStamp
}

func (t *TimePlanEntry) Handler() *secondJobUpdateInstanceStatusHandler {
	return t.handler
}

func (t *TimePlanEntry) SetHandler(handler *secondJobUpdateInstanceStatusHandler) {
	t.handler = handler
}

func (t *TimePlanEntry) Value() interface{} {
	return t
}

func (t *TimePlanEntry) Priority() int64 {
	return t.scheduleTimeStamp
}
