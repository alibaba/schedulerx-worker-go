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
	"fmt"

	"github.com/alibaba/schedulerx-worker-go/internal/utils"
)

var _ utils.ComparatorItem = (*TimePlanEntry)(nil)

type TimePlanEntry struct {
	jobInstanceId     int64
	scheduleTimeStamp int64
	handler           *SecondJobUpdateInstanceStatusHandler
}

func NewTimePlanEntry(jobInstanceId int64, scheduleTimeStamp int64, handler *SecondJobUpdateInstanceStatusHandler) *TimePlanEntry {
	return &TimePlanEntry{jobInstanceId: jobInstanceId, scheduleTimeStamp: scheduleTimeStamp, handler: handler}
}

func (t *TimePlanEntry) JobInstanceId() int64 {
	return t.jobInstanceId
}

func (t *TimePlanEntry) ScheduleTimeStamp() int64 {
	return t.scheduleTimeStamp
}

func (t *TimePlanEntry) Handler() *SecondJobUpdateInstanceStatusHandler {
	return t.handler
}

func (t *TimePlanEntry) UniqueID() string {
	return fmt.Sprintf("%d@%d", t.jobInstanceId, t.scheduleTimeStamp)
}

func (t *TimePlanEntry) String() string {
	return fmt.Sprintf("TimePlanEntry [jobInstanceId=%d, scheduleTimeStamp=%d]", t.jobInstanceId, t.scheduleTimeStamp)
}

func (t *TimePlanEntry) Priority() int64 {
	return t.scheduleTimeStamp
}
